import logging
import traceback
import typing

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import exc
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus

from .common import TaskEvaluatorBase, TaskHandlerRegistry, TaskPublisher
from .eval import eval_task

logger = logging.getLogger("boilermaker.app")


class ResultsStorageTaskEvaluator(TaskEvaluatorBase):
    """Evaluator for standalone tasks (not part of TaskGraph) that stores results."""

    def __init__(
        self,
        receiver: ServiceBusReceiver,
        task: Task,
        task_publisher: TaskPublisher,
        function_registry: TaskHandlerRegistry,
        state: typing.Any | None = None,
        storage_interface: StorageInterface | None = None,
    ):
        if storage_interface is None:
            raise ValueError("Storage interface is required for ResultsStorageTaskEvaluator")

        super().__init__(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
        )
        # Override type to indicate storage_interface is never None after validation
        self.storage_interface: StorageInterface = storage_interface

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
                return TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=["Lost message lease"],
                )
            except exc.BoilermakerServiceBusError:
                logger.error("Unknown ServiceBusError", exc_info=True)
                return TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=["Service Bus error"],
                    formatted_exception=traceback.format_exc(),
                )

        if not self.task.can_retry:
            logger.error(f"Retries exhausted for event {self.task.function_name}")
            if not message_settled:
                try:
                    await self.deadletter_or_complete_task("ProcessingError", detail="Retries exhausted")
                    message_settled = True
                except exc.BoilermakerTaskLeaseLost:
                    logger.error(
                        f"Lost message lease when trying to deadletter/complete "
                        f" for task {self.task.function_name}"
                    )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Failure,
                        errors=["Lost message lease during retry exhaustion"],
                    )
                except exc.BoilermakerServiceBusError:
                    logger.error("Unknown ServiceBusError", exc_info=True)
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Failure,
                        errors=["ServiceBus error during retry exhaustion"],
                        formatted_exception=traceback.format_exc(),
                    )

            # This task is a failure because it did not succeed and retries are exhausted
            if self.task.on_failure is not None:
                await self.publish_task(self.task.on_failure)

            # Early return here: no more processing
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.RetriesExhausted,
            )
            await self.storage_interface.store_task_result(task_result)
            return task_result

        # Actually invoke the task here
        result: TaskResult = await eval_task(
            self.task,
            self.function_registry,
            self.state,
        )
        await self.storage_interface.store_task_result(result)

        if result.status == TaskStatus.Success and self.task.on_success is not None:
            # Success case: publish the next task
            await self.publish_task(self.task.on_success)
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
        # This means, we *may have* multiple `on_success` or `on_failure` tasks scheduled.
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
