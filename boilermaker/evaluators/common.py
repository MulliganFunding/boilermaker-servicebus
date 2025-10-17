import datetime
import logging
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
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus

tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger("boilermaker.app")


# Common Types used when evaluating tasks
TaskHandler: typing.TypeAlias = Callable[..., Awaitable[typing.Any]]
TaskHandlerRegistry: typing.TypeAlias = dict[str, TaskHandler]


class TaskPublisher(typing.Protocol):
    # Define types here, as if __call__ were a function (ignore self).
    def __call__(
        self, task: Task, delay: int | None = None, publish_attempts: int | None = None
    ) -> Awaitable[None]: ...


# Message handling actions
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
                err_msg = f"Failed to requeue message {sequence_number=} exc_info={traceback.format_exc()}"
                logger.error(err_msg)
                raise exc.BoilermakerTaskLeaseLost(err_msg) from None
            except ServiceBusError as sb_exc:
                err_msg = (
                    f"ServiceBusError requeuing message {sequence_number=} "
                    f"exc_info={traceback.format_exc()}"
                )
                logger.error(err_msg)
                raise exc.BoilermakerServiceBusError(err_msg) from sb_exc
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
            raise exc.BoilermakerServiceBusError(logmsg) from sb_exc
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
            raise exc.BoilermakerTaskLeaseLost(logmsg) from None
        except ServiceBusError as sb_exc:
            sb_logmsg = (
                f"ServiceBusError deadlettering message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(sb_logmsg)
            raise exc.BoilermakerServiceBusError(sb_logmsg) from sb_exc

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
                error_description=str(detail) if detail else "Task failed",
            )
        else:
            await cls.complete_message(task.msg, receiver)
        return None


# Base class for Task Evaluation
class TaskEvaluatorBase:
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
        task_publisher: TaskPublisher,
        function_registry: TaskHandlerRegistry,
        state: typing.Any | None = None,
        storage_interface: StorageInterface | None = None,
    ):
        self._receiver: ServiceBusReceiver = receiver
        self.task = task
        self.function_registry = function_registry
        self.state = state if state is not None else {}
        self.task_publisher = task_publisher
        self.storage_interface = storage_interface

    # TO BE IMPLEMENTED BY SUBCLASSES
    @abstractmethod
    async def message_handler(self) -> TaskResult:
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
        if self.current_msg is not None:
            return await MessageActions.abandon_message(
                self.current_msg, self._receiver
            )

    async def complete_message(self) -> None:
        if self.current_msg is not None:
            return await MessageActions.complete_message(
                self.current_msg, self._receiver
            )

    async def renew_message_lock(self) -> datetime.datetime | None:
        """Renew the lock on the current message being processed."""
        if self.current_msg is not None:
            return await MessageActions.renew_message_lock(
                self.current_msg, self._receiver
            )
        return None

    async def deadletter_or_complete_task(
        self,
        reason: str,
        detail: Exception | str | None = None,
    ) -> None:
        return await MessageActions.deadletter_or_complete_task(
            self.task, self._receiver, reason, detail=detail
        )

    async def __call__(self) -> TaskResult | None:
        """Call pre-processing hook and then `message_handler`."""

        try:
            if not await self.pre_process():
                return None
        except exc.BoilermakerUnregisteredFunction:
            await self.deadletter_or_complete_task(
                "ExpectationFailed", detail="Pre-processing expectation failed"
            )
            return TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                result=None,
                status=TaskStatus.Failure,
                errors=["Pre-processing expectation failed"],
            )
        except Exception:
            logger.error("Exception in pre_process", exc_info=True)
            await self.deadletter_or_complete_task(
                "ProcessingError", detail="Pre-processing exception"
            )
            return TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                result=None,
                status=TaskStatus.Failure,
                errors=["Pre-processing exception"],
                formatted_exception=traceback.format_exc(),
            )

        return await self.message_handler()

    async def pre_process(self) -> bool:
        """
        Hook for pre-processing before task execution.

        Returns True if processing should continue, False if it should not.
        """

        # Check if it's a debug task
        if self.task.function_name == sample.TASK_NAME:
            await sample.debug_task(self.state)
            try:
                await self.complete_message()
            except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                msg = f"Lost message lease when trying to complete early for task {self.task.function_name}"
                logger.error(msg)
            # Early return - don't create TaskResult here as caller handles it
            return False

        if not self.function_registry.get(self.task.function_name):
            raise exc.BoilermakerUnregisteredFunction(
                f"Missing registered function {self.task.function_name}"
            )
        return True
