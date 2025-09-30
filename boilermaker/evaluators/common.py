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

from boilermaker.failure import TaskFailureResultType
from boilermaker.storage import StorageInterface
from boilermaker.task import Task
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
    async def abandon_current_message(
        msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ) -> None:
        """Abandon the current message being processed."""
        if msg is not None:
            sequence_number = msg.sequence_number
            try:
                await receiver.abandon_message(msg)
                logger.warning(
                    f"Abandoning message: returned to queue {sequence_number=}"
                )
            except (
                MessageLockLostError,
                ServiceBusError,
                SessionLockLostError,
            ):
                msg = (
                    f"Failed to requeue message {sequence_number=} "
                    f"exc_info={traceback.format_exc()}"
                )
                logger.error(msg)
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
            ServiceBusError,
            SessionLockLostError,
        ):
            logmsg = (
                f"Failed to settle message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
        return None

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
            return None

    @classmethod
    async def deadletter_or_complete_task(
        cls,
        msg: ServiceBusReceivedMessage,
        receiver: ServiceBusReceiver,
        reason: str,
        task: Task,
        detail: Exception | str | None = None,
    ):
        """Deadletter or complete the current task based on its configuration."""
        description = f"Task failed: {detail}" if detail else "Task failed"
        if task.should_dead_letter:
            await receiver.dead_letter_message(
                msg,
                reason=reason,
                error_description=description,
            )
        else:
            await cls.complete_message(msg, receiver)
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

    @abstractmethod
    async def task_handler(self) -> typing.Any | TaskFailureResultType:
        """The actual task handler to be implemented by subclasses."""
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
        return await MessageActions.abandon_current_message(self.current_msg, self._receiver)

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
            self.current_msg, self._receiver, reason, self.task, detail=detail
        )
