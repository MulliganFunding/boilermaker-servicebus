import asyncio
import itertools
import logging
import typing

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import exc, retries
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskResult, TaskStatus

from .common import TaskEvaluatorBase, TaskHandlerRegistry, TaskPublisher
from .eval import eval_task

logger = logging.getLogger("boilermaker.app")

# Retry policy used when load_graph raises a transient exception.
# Up to 3 attempts total (initial + 2 retries) with exponential backoff.
_LOAD_GRAPH_RETRY_POLICY = retries.RetryPolicy(
    max_tries=3,
    delay=1,
    delay_max=16,
    retry_mode=retries.RetryMode.Exponential,
)


class TaskGraphEvaluator(TaskEvaluatorBase):
    """Evaluator for tasks that are part of a TaskGraph workflow.

    At-least-once delivery contract: workers must tolerate at-least-once
    delivery.  ``continue_graph`` uses publish-before-store ordering: each
    ready task is published to Service Bus (with duplicate detection via
    task_id) before writing ``Scheduled`` status to blob storage.  If the
    blob write fails, the task remains ``Pending`` and will be re-discovered
    on redelivery; SB dedup suppresses the duplicate publish.
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
        if storage_interface is None:
            raise ValueError("Storage interface is required for TaskGraphEvaluator")

        if task.acks_early:
            logger.warning(
                f"Task {task.task_id} ({task.function_name}) uses acks_early=True in a "
                "TaskGraph context. If the worker crashes after message settlement but before "
                "the task result is written, this task will be permanently stuck in Started "
                "status with no recovery path. Use acks_late=True (the default) for graph tasks."
            )

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

        # Idempotent redelivery guard — if this task already reached a terminal state
        # (e.g. a prior execution succeeded before the SB lock expired and redelivered), skip
        # re-execution entirely.  Writing Started on top of Success/Failure would regress the
        # blob status and corrupt graph state.
        if self.task.graph_id:
            try:
                _existing = await self.storage_interface.load_task_result(self.task.task_id, self.task.graph_id)
            except exc.BoilermakerStorageError:
                # Transient read failure — proceed normally; do NOT skip execution on a read
                # error, as that would permanently stall the graph.
                logger.warning(
                    f"Failed to read current status for task {self.task.task_id} before "
                    "writing Started; proceeding with execution",
                    exc_info=True,
                )
                _existing = None

            if _existing is not None and _existing.status.finished:
                logger.info(
                    f"Task {self.task.task_id} already in terminal state {_existing.status!r} "
                    "(SB redelivery); skipping re-execution"
                )
                _terminal_result = TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=_existing.status,
                )
                try:
                    await self.continue_graph(_terminal_result)
                except exc.ContinueGraphError:
                    logger.error(
                        f"continue_graph failed on redelivery for task {self.task.task_id}; "
                        "suppressing settlement to allow redelivery",
                        exc_info=True,
                    )
                    return _terminal_result
                try:
                    await self.complete_message()
                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                    logger.warning(
                        f"Failed to complete message on redelivery for task {self.task.task_id}; SB will redeliver",
                        exc_info=True,
                    )
                return _terminal_result

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
                logger.error(f"Lost message lease when trying to complete early for task {self.task.function_name}")
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
                    errors=["ServiceBus error"],
                )

        if not self.task.can_retry:
            logger.error(f"Retries exhausted for event {self.task.function_name}")
            if not message_settled:
                try:
                    await self.deadletter_or_complete_task("ProcessingError", detail="Retries exhausted")
                    message_settled = True
                except exc.BoilermakerTaskLeaseLost:
                    logger.error(
                        f"Lost message lease when trying to deadletter/complete  for task {self.task.function_name}"
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
                    )

            # Early return here: no more processing
            task_result = TaskResult(
                task_id=self.task.task_id,
                graph_id=self.task.graph_id,
                status=TaskStatus.RetriesExhausted,
                result=None,
            )
            await self.storage_interface.store_task_result(task_result)
            # Publish failure tasks which may be ready now.
            # The message is already deadlettered at this point, so suppressing settlement
            # is not possible — log and return gracefully if continue_graph fails.
            try:
                await self.continue_graph(task_result)
            except exc.ContinueGraphError:
                logger.error(
                    f"continue_graph failed after retries exhausted for task {self.task.task_id}; "
                    "failure callbacks may not be dispatched (message already deadlettered)",
                    exc_info=True,
                )
            return task_result

        # Actually invoke the task here
        result: TaskResult = await eval_task(
            self.task,
            self.function_registry,
            self.state,
        )
        # We *must* serialize this result before *loading* the graph again
        await self.storage_interface.store_task_result(result)

        if result.status.finished:
            try:
                await self.continue_graph(result)
            except exc.ContinueGraphError:
                # Transient load_graph failure — do NOT settle the message.
                # Allow Service Bus redelivery so downstream dispatch can be retried.
                logger.error(
                    f"continue_graph failed for task {self.task.task_id}; "
                    "suppressing message settlement to allow redelivery",
                    exc_info=True,
                )
                return result
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

        Transient ``load_graph`` failures (``BoilermakerStorageError`` with a
        non-404 status code) are retried with exponential backoff up to
        ``_LOAD_GRAPH_RETRY_POLICY.max_tries`` attempts.  If all attempts fail,
        ``ContinueGraphError`` is raised so that ``message_handler`` can suppress
        message settlement and allow Service Bus redelivery.

        Permanent failures (``BoilermakerStorageError`` with ``status_code=404``)
        are logged at CRITICAL severity and ``None`` is returned.  Settling the
        message is correct in this case because redelivery will not help — the
        graph blob is gone and downstream tasks cannot be dispatched.

        Note: in practice ``load_graph`` never returns ``None`` for a missing
        blob; the underlying library re-raises all ``HttpResponseError``s
        (including 404) as ``AzureBlobError``, which ``load_graph`` wraps as
        ``BoilermakerStorageError(status_code=404)``.  The ``if not graph`` guard
        below is retained as a defensive fallback only.

        Publish-before-store: each ready task is published to Service Bus
        (with duplicate detection via task_id) before writing ``Scheduled``
        status to blob storage.  If the blob write fails, the task remains
        ``Pending`` and will be re-discovered by ``generate_ready_tasks()``
        on redelivery; SB dedup suppresses the duplicate publish.
        """
        graph_id = completed_task_result.graph_id
        if not graph_id:
            return None

        # Attempt to load the graph, retrying on transient errors.
        last_exc: Exception | None = None
        for attempt in range(_LOAD_GRAPH_RETRY_POLICY.max_tries):
            try:
                graph = await self.storage_interface.load_graph(graph_id)
                break  # success
            except exc.BoilermakerStorageError as e:
                if getattr(e, "status_code", None) == 404:
                    # Permanent: graph blob does not exist. Redelivery will not help.
                    logger.critical(
                        f"Graph {graph_id} not found in storage (404); downstream tasks will not be dispatched. "
                        "This graph may have been deleted.",
                        exc_info=True,
                    )
                    return None
                # Transient error — will retry or raise ContinueGraphError after max_tries
                last_exc = e
                if attempt < _LOAD_GRAPH_RETRY_POLICY.max_tries - 1:
                    delay = _LOAD_GRAPH_RETRY_POLICY.get_delay_interval(attempt)
                    logger.warning(
                        f"load_graph failed for graph {graph_id} "
                        f"(attempt {attempt + 1}/{_LOAD_GRAPH_RETRY_POLICY.max_tries}); "
                        f"retrying in {delay}s",
                        exc_info=True,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"load_graph failed for graph {graph_id} after "
                        f"{_LOAD_GRAPH_RETRY_POLICY.max_tries} attempts; "
                        "raising ContinueGraphError to suppress message settlement",
                        exc_info=True,
                    )
                    raise exc.ContinueGraphError(
                        f"load_graph failed for graph {graph_id} after {_LOAD_GRAPH_RETRY_POLICY.max_tries} attempts"
                    ) from last_exc
        else:
            # Should only be reached if max_tries == 0 (not expected).
            raise exc.ContinueGraphError(f"load_graph not attempted for graph {graph_id}")

        if not graph:
            # Permanent failure: graph blob does not exist.  Redelivery will not help.
            # Settling the upstream message is intentional here.
            logger.critical(
                f"Graph {graph_id} not found after task completion — "
                "downstream tasks will never be dispatched. "
                "This is a permanent data loss; redelivery will not recover it."
            )
            return None

        # Sanity check: did we load the result that was *just* stored?
        loaded_task_status = graph.get_status(completed_task_result.task_id)
        if loaded_task_status != completed_task_result.status:
            logger.error(
                f"Task status mismatch in continue_graph for graph {graph_id}: "
                f"expected {completed_task_result.task_id} to be {completed_task_result.status}, "
                f"but got {loaded_task_status}. Suppressing settlement to allow redelivery."
            )
            raise exc.ContinueGraphError(
                f"Status mismatch for task {completed_task_result.task_id} in graph {graph_id}: "
                f"expected {completed_task_result.status}, got {loaded_task_status}"
            )

        # Find and publish newly ready tasks
        ready_count = 0
        for ready_task in itertools.chain.from_iterable(
            (graph.generate_ready_tasks(), graph.generate_failure_ready_tasks())
        ):
            try:
                # This should publish with a task_id so no double-publish
                await self.publish_task(ready_task)
                ready_count += 1
                logger.info(f"Publishing ready task {ready_task.task_id} in graph {graph_id} total={ready_count}")
            except Exception:
                logger.error(
                    f"Failed to publish ready task {ready_task.task_id} in graph {graph_id}; "
                    "task remains in Pending status in blob and will be retried on redelivery "
                    "via generate_ready_tasks().",
                    exc_info=True,
                )
                continue

            # Write that the task was *scheduled* back to Blob Storage with blob etag!
            try:
                result = graph.schedule_task(ready_task.task_id)
                await self.storage_interface.store_task_result(result, etag=result.etag)
            except exc.BoilermakerStorageError:
                logger.error(
                    f"Failed to store Scheduled status for task {ready_task.task_id} in graph {graph_id}. "
                    "Task was already published; blob remains Pending and will be retried on redelivery.",
                    exc_info=True,
                )
                continue
            except ValueError:
                logger.error(
                    f"schedule_task raised ValueError for task {ready_task.task_id} in graph {graph_id}. "
                    "Task was already published; blob write skipped.",
                    exc_info=True,
                )
                continue

        if ready_count == 0:
            logger.info(f"No new tasks ready in graph {graph_id} after task {completed_task_result.task_id}")

        return ready_count
