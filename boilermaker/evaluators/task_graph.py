import logging
import time
import traceback
import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import sample
from boilermaker.exc import BoilermakerStorageError
from boilermaker.failure import TaskFailureResultType
from boilermaker.retries import RetryException
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskResultSlim, TaskStatus
from boilermaker.types import TaskHandler

from .common import MessageHandler

logger = logging.getLogger("boilermaker.app")


class TaskGraphEvaluator(MessageHandler):
    """Evaluator for tasks that are part of a TaskGraph workflow."""

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
            raise ValueError("Storage interface is required for TaskGraphEvaluator")

        super().__init__(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
        )

    async def message_handler(self):
        """Handle a message containing a task that's part of a TaskGraph."""
        message_settled = False
        # First, mark task as started in the graph to prevent double-scheduling
        if self.task.graph_id:
            started_result = TaskResultSlim.default(
                self.task.task_id, self.task.graph_id
            )
            started_result.status = TaskStatus.Started
            # Explore leases around blob result writes!
            try:
                await self.storage_interface.store_task_result(started_result)
            except BoilermakerStorageError as exc:
                if exc.status_code == 409:
                    logger.warning(
                        f"Task {self.task.task_id} in graph {self.task.graph_id} already started!"
                    )
                    return None

                logger.error(
                    f"Failed to mark task {self.task.task_id} as started in graph {self.task.graph_id}: {exc}"
                )
                return None

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

            # Store failure result for graph processing
            if self.task.graph_id:
                failure_result = TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=["Retries exhausted"],
                )
                await self.storage_interface.store_task_result(failure_result)

            # Handle legacy callbacks (for backward compatibility)
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)
            return None

        # Execute the task
        try:
            await self.task_handler()
            # For TaskGraph, success handling is done in task_handler
            # Legacy callback support
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

            # Store failure result for graph processing
            if self.task.graph_id:
                failure_result = TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=[str(exc)],
                    formatted_exception=traceback.format_exc(),
                )
                await self.storage_interface.store_task_result(failure_result)

            # Legacy callback support
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)

            if not message_settled:
                await self.deadletter_or_complete_task("ExceptionThrown", detail=exc)
                message_settled = True

        # Handle late acknowledgment
        if self.task.acks_late and not message_settled:
            await self.complete_message()

    async def task_handler(self) -> typing.Any | TaskFailureResultType:
        """Execute task function and handle TaskGraph workflow progression."""
        start = time.monotonic()
        logger.info(
            f"[{self.task.function_name}] Begin Task {self.sequence_number=} "
            f"graph={self.task.graph_id}"
        )

        # First, mark task as started in the graph to prevent double-scheduling
        if self.task.graph_id:
            graph = await self.storage_interface.load_graph(self.task.graph_id)
            if graph:
                started_result = graph.start_task(self.task.task_id)
                await self.storage_interface.store_task_result(started_result)

        # Execute the actual task function
        try:
            if self.task.function_name == sample.TASK_NAME:
                result = await sample.debug_task(self.state)
            else:
                function = self.function_registry.get(self.task.function_name)
                if not function:
                    raise ValueError(
                        f"Missing registered function {self.task.function_name}"
                    )

                # Track Failure!
                result = await function(
                    self.state,
                    *self.task.payload["args"],
                    **self.task.payload["kwargs"],
                )

            # Store successful result
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                result=result,
                status=TaskStatus.Success,
            )
            await self.storage_interface.store_task_result(task_result)

            # Handle TaskGraph workflow progression
            if self.task.graph_id:
                await self.continue_graph(task_result)

            logger.info(
                f"[{self.task.function_name}] Completed Task {self.sequence_number=} "
                f"in {time.monotonic()-start:.2f}s"
            )
            return result

        except Exception as exc:
            logger.error(f"Task {self.task.task_id} failed: {exc}")
            # Let the exception bubble up to message_handler for proper error handling
            raise

    async def continue_graph(self, completed_task_result: TaskResult) -> int | None:
        """Continue evaluating TaskGraph workflow after a task completes successfully."""
        graph_id = completed_task_result.graph_id
        if not graph_id:
            return None

        try:
            # Reload graph with latest results
            graph = await self.storage_interface.load_graph(graph_id)
            if not graph:
                logger.error(f"Graph {graph_id} not found after task completion")
                return

            # Find and publish newly ready tasks
            ready_count = 0
            for ready_task in graph.ready_tasks():
                logger.info(
                    f"Publishing ready task {ready_task.task_id} in graph {graph_id}"
                )
                # Publish the task
                await self.publish_task(ready_task)
                ready_count += 1

            if ready_count == 0:
                logger.info(
                    f"No new tasks ready in graph {graph_id} after task {completed_task_result.task_id}"
                )
            else:
                logger.info(f"Published {ready_count} ready tasks in graph {graph_id}")
            return ready_count

        except Exception as exc:
            logger.error(f"Error processing graph workflow {graph_id}: {exc}")
            # Don't re-raise - we don't want graph processing errors to fail the original task

        return None
