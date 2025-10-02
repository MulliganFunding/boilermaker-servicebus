import logging
import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import exc
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus
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
        delete_successful_graphs: bool = False,
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
        # Whether to fire a clean-up task at the end if all tasks completed successfully
        self.delete_successful_graphs = delete_successful_graphs

    # The main message handler
    async def message_handler(self) -> TaskResult:
        """Individual message handler"""
        message_settled = False

        start_result = TaskResult(
            task_id=self.task.task_id,
            graph_id=self.task.graph_id,
            status=TaskStatus.Started,
        )
        # What to do if failure?
        await self.storage_interface.store_task_result(start_result)

        # Immediately record an attempt
        self.task.record_attempt()

        # at-most once: "complete" msg even if it fails later
        if self.task.acks_early:
            try:
                await self.complete_message()
                message_settled = True
            except exc.BoilermakerTaskLeaseLost:
                logger.error(
                    f"Lost message lease when trying to complete early for task {self.task.function_name}"
                )
                return None
            except exc.BoilermakerServiceBusError:
                logger.error("Unknown ServiceBusError", exc_info=True)
                return None

        if not self.task.can_retry:
            logger.error(f"Retries exhausted for event {self.task.function_name}")
            if not message_settled:
                try:
                    await self.deadletter_or_complete_task(
                        "ProcessingError", detail="Retries exhausted"
                    )
                    message_settled = True
                except exc.BoilermakerTaskLeaseLost:
                    logger.error(
                        f"Lost message lease when trying to deadletter/complete "
                        f" for task {self.task.function_name}"
                    )
                    return None
                except exc.BoilermakerServiceBusError:
                    logger.error("Unknown ServiceBusError", exc_info=True)
                    return None

            # This task is a failure because it did not succeed and retries are exhausted
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)

            # Early return here: no more processing
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.RetriesExhausted,
                result=None,
            )
            await self.storage_interface.store_task_result(task_result)
            return task_result

        # Actually invoke the task here
        result: TaskResult = await self.task_handler()
        # We *must* serialize this result before *loading* the graph again
        await self.storage_interface.store_task_result(result)

        if result.status == TaskStatus.Success:
            if self.task.on_success is not None:
                # Success case: continue evaluating the graph
                await self.publish_task(self.task.on_success)
            if self.task.graph_id is not None:
                # Success case: continue evaluating the graph
                await self.continue_graph(result)
        elif result.status == TaskStatus.Failure and self.task.on_failure is not None:
            # Schedule on_failure task
            await self.publish_task(self.task.on_failure)
        elif result.status == TaskStatus.Retry:
            # Retry requested: republish the same task with delay
            delay = self.task.get_next_delay()
            warn_msg = (
                f"{result.errors} "
                f"[attempt {self.task.attempts.attempts} of {self.task.policy.max_tries}] "
                f"Publishing retry... {self.sequence_number=} "
                f"<function={self.task.function_name}> with {delay=}"
            )
            logger.warning(warn_msg)
            await self.publish_task(
                self.task,
                delay=delay,
            )

        # At-least once: settle at the end.
        # IF we have lost the message lease, we *may* have *multiple* copies of this task running.
        # This means, we *may have* multiple `graph_continue` or `on_failure` tasks scheduled.
        if not message_settled:
            try:
                if result.status == TaskStatus.Failure:
                    await self.deadletter_or_complete_task("TaskFailed")
                else:
                    await self.complete_message()
                message_settled = True
            except exc.BoilermakerTaskLeaseLost:
                logger.error(
                    f"Lost message lease when trying to complete late for task {self.task.function_name} "
                    "May result in multiple executions of this task and its callbacks!"
                )
                return result
            except exc.BoilermakerServiceBusError:
                logger.error("Unknown ServiceBusError", exc_info=True)
                return result

        return result

    async def continue_graph(self, completed_task_result: TaskResult) -> int | None:
        """
        Continue evaluating TaskGraph workflow after a task completes successfully.

        We always reload the graph from storage to get the latest state.
        """
        graph_id = completed_task_result.graph_id
        if not graph_id:
            return None

        try:
            # Reload graph with latest results
            graph = await self.storage_interface.load_graph(graph_id)
        except Exception:
            logger.error(
                f"Exception in continue_graph for graph {graph_id}", exc_info=True
            )
            return None

        if not graph:
            logger.error(f"Graph {graph_id} not found after task completion")
            return None

        # Sanity check: did we load the result that was *just* stored?
        loaded_task_status = graph.get_status(completed_task_result.task_id)
        if loaded_task_status != completed_task_result.status:
            logger.error(
                f"Task status mismatch in continue_graph for graph {graph_id}: "
                f"expected {completed_task_result.task_id} to be {completed_task_result.status}, "
                f"but got {loaded_task_status}"
            )
            return None

        # Find and publish newly ready tasks
        ready_count = 0
        for ready_task in graph.ready_tasks():
            # Publish the task
            await self.publish_task(ready_task)
            ready_count += 1
            logger.info(
                f"Publishing ready task {ready_task.task_id} in graph {graph_id} total={ready_count}"
            )
        else:
            logger.info(
                f"No new tasks ready in graph {graph_id} after task {completed_task_result.task_id}"
            )

        return ready_count
