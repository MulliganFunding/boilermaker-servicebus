import datetime
import logging
import time
import traceback
import typing
from abc import abstractmethod
from collections.abc import Awaitable, Callable
from functools import cached_property
from json.decoder import JSONDecodeError

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusReceiver
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    ServiceBusError,
    SessionLockLostError,
)
from opentelemetry import trace
from pydantic import ValidationError

from boilermaker import exc, sample
from boilermaker.failure import TaskFailureResult
from boilermaker.retries import RetryException
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus
from boilermaker.types import TaskHandler

tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger("boilermaker.app")


class MessageActions:
    """
    Common message actions for handling ServiceBusReceivedMessage.

    This is a mixin class to provide common message actions such as:
    - task_decoder
    - abandon_current_message
    - complete_message
    - renew_message_lock
    - deadletter_or_complete_task
    """

    @classmethod
    async def task_decoder(
        cls, msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ) -> Task | None:
        """Decode a ServiceBusReceivedMessage into a Task."""
        try:
            task = Task.model_validate_json(str(msg))
            task.msg = msg
            return task
        except (JSONDecodeError, ValidationError):
            # This task is not parseable
            log_err_msg = (
                f"Invalid task sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(log_err_msg)
            await receiver.dead_letter_message(
                msg,
                reason="InvalidTaskFormat",
                error_description=log_err_msg,
            )
            return None

    @staticmethod
    async def abandon_message(
        msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ) -> None:
        """Abandon the message being processed."""
        if msg is not None:
            sequence_number = msg.sequence_number
            try:
                await receiver.abandon_message(msg)
                logger.warning(
                    f"Abandoning message: returned to queue {sequence_number=}"
                )
            except (
                MessageAlreadySettled,
                MessageLockLostError,
                SessionLockLostError,
            ):
                msg = (
                    f"Failed to requeue message {sequence_number=} "
                    f"exc_info={traceback.format_exc()}"
                )
                logger.error(msg)
                raise exc.BoilermakerTaskLeaseLost(msg) from None
            except ServiceBusError as sb_exc:
                msg = (
                    f"ServiceBusError requeuing message {sequence_number=} "
                    f"exc_info={traceback.format_exc()}"
                )
                logger.error(msg)
                raise exc.BoilermakerServiceBusError(msg) from sb_exc
        return None

    @staticmethod
    async def complete_message(
        msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ):
        """Complete the current message being processed."""
        try:
            await receiver.complete_message(msg)
        except (
            MessageAlreadySettled,
            MessageLockLostError,
            SessionLockLostError,
        ):
            logmsg = (
                f"Failed to settle message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerTaskLeaseLost(msg) from None
        except ServiceBusError as sb_exc:
            logmsg = (
                f"ServiceBusError settling message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerServiceBusError(msg) from sb_exc
        return None

    @staticmethod
    async def dead_letter_message(
        msg: ServiceBusReceivedMessage,
        receiver: ServiceBusReceiver,
        reason: str,
        error_description: str = "Task failed",
    ) -> None:
        """Deadletter-store the message being processed."""
        try:
            return await receiver.dead_letter_message(
                msg,
                reason=reason,
                error_description=error_description,
            )
        except (
            MessageAlreadySettled,
            MessageLockLostError,
            SessionLockLostError,
        ):
            logmsg = (
                f"Failed to deadletter message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerTaskLeaseLost(msg) from None
        except ServiceBusError as sb_exc:
            logmsg = (
                f"ServiceBusError deadlettering message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerServiceBusError(msg) from sb_exc

    @staticmethod
    async def renew_message_lock(
        msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ) -> datetime.datetime | None:
        """Renew the lock on the current message being processed."""
        if receiver is None:
            logger.warning("No receiver to renew lock for")
            return None
        if msg is None:
            logger.warning("No current message to renew lock for")
            return None

        try:
            # Returns new expiration time if successful
            return await receiver.renew_message_lock(msg)
        except (MessageLockLostError, MessageAlreadySettled, SessionLockLostError):
            logmsg = (
                f"Failed to renew message lock sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerTaskLeaseLost(logmsg) from None
        except ServiceBusError as sb_exc:
            logmsg = (
                f"ServiceBusError renewing message lock sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            raise exc.BoilermakerServiceBusError(logmsg) from sb_exc

    @classmethod
    async def deadletter_or_complete_task(
        cls,
        task: Task,
        receiver: ServiceBusReceiver,
        reason: str,
        detail: Exception | str | None = None,
    ):
        """Deadletter or complete the current task based on its configuration."""
        if task.msg is None:
            logger.warning(
                "No current message to settle for deadletter_or_complete_task"
            )
            return None

        if task.should_dead_letter:
            await cls.dead_letter_message(
                task.msg,
                receiver,
                reason=reason,
                error_description=detail or "Task failed",
            )
        else:
            await cls.complete_message(task.msg, receiver)
        return None


class MessageHandler:
    """
    Handles messages from the service bus.
    This is a base class to be extended by specific evaluators.
    It provides common functionality for handling messages, including
    decoding tasks, managing message locks, and publishing tasks.
    """

    def __init__(
        self,
        receiver: ServiceBusReceiver,
        task: Task,
        task_publisher: Callable[[Task], Awaitable[None]],
        function_registry: dict[str, TaskHandler],
        state: typing.Any | None = None,
        storage_interface: StorageInterface | None = None,
    ):
        self._receiver: ServiceBusReceiver = receiver
        self.task = task
        self.function_registry = function_registry
        self.state = state if state is not None else {}
        self.task_publisher = task_publisher
        self.storage_interface = storage_interface

    async def __call__(self):
        return await self.message_handler()

    # TO BE IMPLEMENTED BY SUBCLASSES
    @abstractmethod
    async def message_handler(self):
        """Individual message handler"""
        raise NotImplementedError

    # Shared properties and methods
    @cached_property
    def current_msg(self) -> ServiceBusReceivedMessage | None:
        return self.task.msg

    @cached_property
    def sequence_number(self) -> int | None:
        return self.task.sequence_number

    async def publish_task(
        self,
        task: Task,
        delay: int = 0,
        publish_attempts: int = 1,
    ) -> Task:
        await self.task_publisher(task, delay=delay, publish_attempts=publish_attempts)
        return task

    # Message handling actions
    async def abandon_current_message(self) -> None:
        return await MessageActions.abandon_message(self.current_msg, self._receiver)

    async def complete_message(self) -> None:
        return await MessageActions.complete_message(self.current_msg, self._receiver)

    async def renew_message_lock(self) -> datetime.datetime | None:
        """Renew the lock on the current message being processed."""
        return await MessageActions.renew_message_lock(self.current_msg, self._receiver)

    async def deadletter_or_complete_task(
        self,
        reason: str,
        detail: Exception | str | None = None,
    ) -> None:
        return await MessageActions.deadletter_or_complete_task(
            self.task, self._receiver, reason, detail=detail
        )

    async def task_handler(self) -> TaskResult:
        """
        Dynamically look up function requested and then evaluate it.

        Wrap results in TaskResult. If callers return the special TaskFailureResult,
        store result as a failure result with no actual result data.

        Note: special case for `sample.debug_task` which returns 0 and does not log.

        Returns:
            TaskResult instance or 0 for `sample.debug_task`.
        Raises:
            ValueError: If the function is not found in the registry.
        """
        start = time.monotonic()

        # Check if it's a debug task
        if self.task.function_name == sample.TASK_NAME:
            await sample.debug_task(self.state)
            return TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                result=0,
                status=TaskStatus.Success,
            )

        function = self.function_registry.get(self.task.function_name)
        if not function:
            raise ValueError(f"Missing registered function {self.task.function_name}")

        # Look up function associated with the task
        logger.info(f"[{self.task.function_name}] Begin Task {self.sequence_number=}")

        try:
            result = await function(
                self.state,
                *self.task.payload["args"],
                **self.task.payload["kwargs"],
            )
            # Check if result is TaskFailureResult (special failure case)
            if result is TaskFailureResult:
                # Store as failure result (no actual result data to store)
                task_result = TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    result=None,  # No result data for failures
                    status=TaskStatus.Failure,
                    errors=["Task returned TaskFailureResult"],
                )
                logger.warning(
                    f"[{self.task.function_name}] Task {self.sequence_number=} "
                    f"returned TaskFailureResult in {time.monotonic()-start:.3f}s"
                )
            else:
                # Create success result
                task_result = TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    result=result,
                    status=TaskStatus.Success,
                )
                logger.info(
                    f"[{self.task.function_name}] Completed Task {self.sequence_number=} "
                    f"in {time.monotonic()-start:.3f}s"
                )
        except RetryException as retry:
            # A retry has been requested:
            # Calculate next delay and publish retry.
            # Do not run on_failure run until after retries exhausted!
            # The `retry` RetryException may have a policy -> What if it's different from the Task?
            if retry.policy and retry.policy != self.task.policy:
                # This will publish the *next* instance of the task using *this* policy
                self.task.policy = retry.policy
                logger.warning(f"Task policy updated to retry policy {retry.policy}")

            delay = self.task.get_next_delay()
            warn_msg = (
                f"{retry.msg} "
                f"[attempt {self.task.attempts.attempts} of {self.task.policy.max_tries}] "
                f"Publishing retry... {self.sequence_number=} "
                f"<function={self.task.function_name}> with {delay=}"
            )
            logger.warning(warn_msg)
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.Retry,
                errors=[str(retry)],
                formatted_exception=traceback.format_exc(),
            )
        except Exception as exc:
            # Some other exception has been thrown
            err_msg = (
                f"Exception in task sequence_number={self.sequence_number} "
                f"{traceback.format_exc()}"
            )
            logger.error(err_msg)
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.Failure,
                errors=[str(exc)],
                formatted_exception=traceback.format_exc(),
            )

        return task_result
