import logging
import time
import traceback
import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver
from opentelemetry import trace

from boilermaker import sample
from boilermaker.failure import TaskFailureResult, TaskFailureResultType
from boilermaker.retries import RetryException
from boilermaker.task import Task
from boilermaker.types import TaskHandler

from .common import MessageHandler

tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger("boilermaker.app")


class NoStorageEvaluator(MessageHandler):
    """
    Base class for evaluating tasks from Azure Service Bus.

    Use this if no storage interface is provided and no TaskGraph is involved.
    """
    def __init__(
        self,
        receiver: ServiceBusReceiver,
        task: Task,
        task_publisher: Callable[[Task], Awaitable[None]],
        function_registry: dict[str, TaskHandler],
        state: typing.Any | None = None,
    ):
        super().__init__(receiver, task, task_publisher, function_registry, state=state)

    # The main message handler
    async def message_handler(self):
        """Individual message handler"""
        message_settled = False
        # Immediately record an attempt
        self.task.record_attempt()

        # at-most once: "complete" msg even if it fails later
        if self.task.acks_early:
            await self.complete_message()
            message_settled = True

        if not self.task.can_retry:
            logger.error(f"Retries exhausted for event {self.task.function_name}")
            if not message_settled:
                await self.deadletter_or_complete_task(
                    "ProcessingError", detail="Retries exhausted"
                )
            # This task is a failure because it did not succeed and retries are exhausted
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)
            # Early return here: no more processing
            return None

        # Actually handle the task here
        try:
            result = await self.task_handler()

            # Check and handle failure first
            if result is TaskFailureResult:
                # Deadletter or complete the message
                if not message_settled:
                    await self.deadletter_or_complete_task("TaskFailed")
                    message_settled = True
                if self.task.on_failure is not None:
                    # Schedule on_failure task
                    await self.publish_task(self.task.on_failure)
            # Success case: publish the next task (if desired)
            elif self.task.on_success is not None:
                # Success case: publish the next task
                await self.publish_task(self.task.on_success)
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
            await self.publish_task(
                self.task,
                delay=delay,
            )
        except Exception as exc:
            # Some other exception has been thrown
            err_msg = (
                f"Failed processing task sequence_number={self.sequence_number} "
                f"{traceback.format_exc()}"
            )
            logger.error(err_msg)
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)

            if not message_settled:
                await self.deadletter_or_complete_task(
                    "ExceptionThrown", detail=exc
                )
                message_settled = True

        # at-least once: settle at the end
        if self.task.acks_late and not message_settled:
            await self.complete_message()
            message_settled = True

    async def task_handler(self) -> typing.Any | TaskFailureResultType:
        """
        Dynamically look up function requested and then evaluate it.
        """
        start = time.monotonic()
        logger.info(f"[{self.task.function_name}] Begin Task {self.sequence_number=}")

        # Check if it's a debug task
        if self.task.function_name == sample.TASK_NAME:
            return await sample.debug_task(self.state)

        # Look up function associated with the task
        function = self.function_registry.get(self.task.function_name)
        if not function:
            raise ValueError(f"Missing registered function {self.task.function_name}")
        result = await function(
            self.state, *self.task.payload["args"], **self.task.payload["kwargs"]
        )
        logger.info(
            f"[{self.task.function_name}] Completed Task {self.sequence_number=} in {time.monotonic()-start}s"
        )
        return result
