import logging
import traceback
import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import failure
from boilermaker.retries import RetryException
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus
from boilermaker.types import TaskHandler

from .common import MessageHandler

logger = logging.getLogger("boilermaker.app")


class ResultsStorageTaskEvaluator(MessageHandler):
    """Evaluator for standalone tasks (not part of TaskGraph) that stores results."""

    def __init__(
        self,
        receiver: ServiceBusReceiver,
        task: Task,
        task_publisher: Callable[[Task], Awaitable[None]],
        function_registry: dict[str, TaskHandler],
        state: typing.Any | None = None,
        storage_interface: StorageInterface | None = None,
    ):
        if storage_interface is None:
            raise ValueError(
                "Storage interface is required for ResultsStorageTaskEvaluator"
            )

        super().__init__(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
        )

    async def message_handler(self):
        """Handle a standalone task message with result storage."""
        message_settled = False

        # Record attempt
        self.task.record_attempt()

        # Handle early acknowledgment
        if self.task.acks_early:
            await self.complete_message()
            message_settled = True

        # Check retry eligibility
        if not self.task.can_retry:
            logger.error(f"Retries exhausted for task {self.task.function_name}")
            if not message_settled:
                await self.deadletter_or_complete_task(
                    "ProcessingError", detail="Retries exhausted"
                )

            # Store failure result
            failure_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.Failure,
                errors=["Retries exhausted"],
            )
            await self.storage_interface.store_task_result(failure_result)

            # Handle callback
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)
            return None

        # Execute the task
        try:
            result = await self.task_handler()

            # Check if result is TaskFailureResult (special failure case)
            if result is failure.TaskFailureResult:
                # Deadletter or complete the message
                if not message_settled:
                    await self.deadletter_or_complete_task("TaskFailed")
                    message_settled = True
                if self.task.on_failure is not None:
                    # Schedule on_failure task
                    await self.publish_task(self.task.on_failure)
            else:
                # Handle success callback
                if self.task.on_success is not None:
                    await self.publish_task(self.task.on_success)

        except RetryException as retry:
            # Handle retry request
            if retry.policy and retry.policy != self.task.policy:
                self.task.policy = retry.policy
                logger.warning(f"Task policy updated to retry policy {retry.policy}")

            delay = self.task.get_next_delay()
            logger.warning(
                f"{retry.msg} [attempt {self.task.attempts.attempts} of {self.task.policy.max_tries}] "
                f"Publishing retry for task {self.task.task_id} with {delay=}s"
            )
            await self.publish_task(self.task, delay=delay)

        except Exception as exc:
            logger.error(
                f"Failed processing task {self.task.task_id}: {traceback.format_exc()}"
            )

            # Store failure result
            failure_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.Failure,
                errors=[str(exc)],
                formatted_exception=traceback.format_exc(),
            )
            await self.storage_interface.store_task_result(failure_result)

            # Handle callback
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)

            if not message_settled:
                await self.deadletter_or_complete_task("ExceptionThrown", detail=exc)
                message_settled = True

        # Handle late acknowledgment
        if self.task.acks_late and not message_settled:
            await self.complete_message()

    async def task_handler(self) -> TaskResult | typing.Literal[0]:
        """Execute task function and store results."""
        task_result = await super().task_handler()
        await self.storage_interface.store_task_result(task_result)
        return task_result
