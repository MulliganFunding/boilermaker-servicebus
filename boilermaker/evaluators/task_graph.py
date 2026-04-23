import asyncio
import itertools
import logging
import typing

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker import exc, retries
from boilermaker.storage import StorageInterface
from boilermaker.task import GraphId, Task, TaskGraph, TaskResult, TaskStatus

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
            raise ValueError(
                f"Task {task.task_id} ({task.function_name}) uses acks_early=True, which is "
                "incompatible with TaskGraph coordination. If the worker crashes after message "
                "settlement but before the task result is written, the task will be permanently "
                "stuck in Started status with no recovery path. Set acks_early=False (the default) "
                "for all graph tasks."
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

    def _graph_tag(self) -> str:
        return f"[Graph {self.task.graph_id}]" if self.task.graph_id else "[Graph ?]"

    def _task_tag(self, task: Task) -> str:
        return f"{task.function_name}[{task.task_id}]"

    # The main message handler
    async def message_handler(self) -> TaskResult:
        """Individual message handler"""
        message_settled = False
        _graph_tag = self._graph_tag()
        _task_tag = self._task_tag(self.task)

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
                    f"{_graph_tag} Failed to read current status for task {_task_tag} before "
                    "writing Started; proceeding with execution",
                    exc_info=True,
                )
                _existing = None

            _etag: str | None = _existing.etag if _existing else None

            if _existing is not None and _existing.status.finished:
                logger.info(
                    f"{_graph_tag} Task {_task_tag} already in terminal state {_existing.status!r} "
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
                        f"{_graph_tag} continue_graph failed on redelivery for task {_task_tag}; "
                        "abandoning for immediate redelivery",
                        exc_info=True,
                    )
                    try:
                        await self.abandon_current_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to abandon message after continue_graph redelivery failure "
                            f"for task {_task_tag}; SB will redeliver when lock expires",
                            exc_info=True,
                        )
                    return _terminal_result
                try:
                    await self.complete_message()
                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                    logger.warning(
                        (
                            f"{_graph_tag} Failed to complete message on redelivery for task "
                            f"{_task_tag}; Expect redelivery"
                        ),
                        exc_info=True,
                    )
                return _terminal_result
        else:
            _etag = None

        start_result = TaskResult(
            task_id=self.task.task_id,
            graph_id=self.task.graph_id,
            status=TaskStatus.Started,
        )
        # Write Started with an ETag so two concurrent workers racing on the same task
        # cannot both pass the terminal-status guard and overwrite each other's result.
        # When _etag is None (no prior read, or read failed) this degrades to an
        # unconditional write.
        try:
            await self.storage_interface.store_task_result(start_result, etag=_etag)
        except exc.BoilermakerStorageError as _started_err:
            _is_precondition_failure = getattr(_started_err, "status_code", None) == 412
            if not _is_precondition_failure:
                logger.warning(
                    f"{_graph_tag} Non-precondition error writing Started for task {_task_tag}; proceeding...",
                    exc_info=True,
                )
            else:
                # ETag mismatch: another worker wrote to this blob since our read.
                # Re-read to determine the current state.
                # _etag is only non-None when self.task.graph_id is set (see capture above),
                # so this assertion is always true when we reach a 412.
                assert self.task.graph_id is not None, (
                    f"{_graph_tag} 412 on Started write implies _etag was set, which requires graph_id"
                )
                try:
                    _reread = await self.storage_interface.load_task_result(self.task.task_id, self.task.graph_id)
                except exc.BoilermakerStorageError:
                    logger.error(
                        f"{_graph_tag} 412 on Started write and re-read also failed for task {_task_tag}; "
                        "abandoning for immediate redelivery",
                        exc_info=True,
                    )
                    try:
                        await self.abandon_current_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to abandon message after 412+re-read failure for task "
                            f"{_task_tag}; SB will redeliver when lock expires",
                            exc_info=True,
                        )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Scheduled,
                    )

                if _reread is None:
                    # The blob vanished between the 412 and the re-read — an unexpected
                    # state, since a blob must exist once the graph is stored.  Returning
                    # Failure surfaces the anomaly rather than silently dropping the task.
                    logger.error(
                        f"{_graph_tag} 412 on Started write but re-read returned None for task {_task_tag}; "
                        "blob should always exist after graph creation."
                    )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Failure,
                        errors=["412 on Started write; re-read returned None (blob vanished)"],
                    )

                if _reread.status.finished:
                    # Another worker already reached a terminal state — skip execution.
                    logger.info(
                        f"{_graph_tag} Task {_task_tag} reached terminal state {_reread.status!r} "
                        "after 412 on Started write; skipping execution"
                    )
                    try:
                        await self.complete_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to complete message after 412 skip for task {_task_tag}; "
                            "SB will redeliver",
                            exc_info=True,
                        )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=_reread.status,
                    )

                if _reread.status == TaskStatus.Started:
                    # Another worker won the CAS and is executing — yield to that worker.
                    logger.info(
                        f"{_graph_tag} Task {_task_tag} is already Started by another worker "
                        "(412 on Started write); completing message without executing"
                    )
                    try:
                        await self.complete_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to complete message after 412 yield for task {_task_tag}; "
                            "SB will redeliver",
                            exc_info=True,
                        )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Started,
                    )

                if _reread.status in (TaskStatus.Scheduled, TaskStatus.Pending):
                    # No other worker holds Started yet — retry the CAS with the
                    # re-read etag.
                    #
                    # Scheduled: normal publish-before-store window; the scheduler's
                    #   Scheduled content write (E→E+1) raced with this worker's
                    #   initial Started write.
                    # Pending:   the scheduler acquired the lease (Azure does NOT
                    #   advance ETag on lease acquire) and published to SB, but has
                    #   not yet written Scheduled — or WriteScheduledFail left the
                    #   blob Pending.  The content write that bumped the ETag was
                    #   something other than a lease operation.  Either way no other
                    #   worker holds Started, so this worker is the legitimate claimant.
                    #
                    # Spec action: WriteStarted412 (Scheduled/Pending branch) →
                    #              RetryStartedAfterScheduled
                    if _reread.etag is None:
                        logger.warning(
                            f"{_graph_tag} 412 + {_reread.status.value} re-read for task {_task_tag}: "
                            "re-read etag is None — degraded to unconditional retry write"
                        )
                    logger.warning(
                        f"{_graph_tag} 412 + {_reread.status.value} for task {_task_tag}; "
                        "retrying Started write with re-read etag"
                    )
                    try:
                        await self.storage_interface.store_task_result(start_result, etag=_reread.etag)
                        # RetryStartedWriteSuccess: fall through to execution (no return).
                        # This path is identical to WriteStartedSuccess — no special handling.

                    except exc.BoilermakerStorageError as _retry_err:
                        _retry_is_precondition_failure = getattr(_retry_err, "status_code", None) == 412

                        if not _retry_is_precondition_failure:
                            # RetryStartedWriteNon412Error: fail-open, fall through to execution.
                            logger.warning(
                                f"{_graph_tag} Non-412 error on retry Started write for task {_task_tag}; "
                                "proceeding with execution",
                                exc_info=True,
                            )
                            # no return — fall through to execution

                        else:
                            # RetryStartedWrite412: another worker moved the blob between
                            # the re-read and the retry.  Re-read a second time to determine
                            # the current state.
                            # Spec action: RetryStartedWrite412 → RereadAfterRetry412
                            logger.warning(
                                f"{_graph_tag}  Second 412 on retry Started write for task {_task_tag}; "
                                "re-reading to determine action"
                            )
                            try:
                                _reread2 = await self.storage_interface.load_task_result(
                                    self.task.task_id, self.task.graph_id
                                )
                            except exc.BoilermakerStorageError:
                                # Blob state unknown — abandon for immediate redelivery.
                                # Falls through to the else branch below which calls abandon.
                                logger.error(
                                    f"{_graph_tag} Second 412 + re-read also failed for task {_task_tag}; "
                                    "abandoning for immediate redelivery",
                                    exc_info=True,
                                )
                                _reread2 = None

                            if _reread2 is not None and _reread2.status == TaskStatus.Started:
                                # Another worker won the second CAS — settle and yield.
                                # Spec action: RereadAfterRetry412 (Started) → Completing
                                try:
                                    await self.complete_message()
                                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                                    logger.warning(
                                        f"{_graph_tag} Failed to complete message after second 412 (Started) "
                                        f"for task {_task_tag}; SB will redeliver",
                                        exc_info=True,
                                    )
                                return TaskResult(
                                    task_id=self.task.task_id,
                                    graph_id=self.task.graph_id,
                                    status=TaskStatus.Started,
                                )
                            elif _reread2 is not None and _reread2.status.finished:
                                # Task is already terminal — settle and yield.
                                # Spec action: RereadAfterRetry412 (terminal) → Completing
                                try:
                                    await self.complete_message()
                                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                                    logger.warning(
                                        f"{_graph_tag} Failed to complete message after second 412 (terminal) "
                                        f"for task {_task_tag}; SB will redeliver",
                                        exc_info=True,
                                    )
                                return TaskResult(
                                    task_id=self.task.task_id,
                                    graph_id=self.task.graph_id,
                                    status=_reread2.status,
                                )
                            elif _reread2 is not None and _reread2.status == TaskStatus.Retry:
                                # Another worker already ran this task and published a delayed
                                # retry SB message.  Settle our duplicate and yield to the
                                # retry mechanism — the graph will continue via that message.
                                # Spec action: RereadAfterRetry412 (Retry) → Completing
                                # Note: Retry is not modeled as a blob status in the TLA+ spec
                                # (spec only produces Success/Failure from execution).
                                logger.warning(
                                    f"{_graph_tag} Second 412 + Retry re-read for task {_task_tag}; "
                                    "another worker published a retry message — settling our duplicate"
                                )
                                try:
                                    await self.complete_message()
                                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                                    logger.warning(
                                        f"{_graph_tag} Failed to complete message after second 412 (Retry) "
                                        f"for task {_task_tag}; SB will redeliver",
                                        exc_info=True,
                                    )
                                return TaskResult(
                                    task_id=self.task.task_id,
                                    graph_id=self.task.graph_id,
                                    status=TaskStatus.Retry,
                                )
                            else:
                                # Pending, Scheduled, None, or any other unclaimed state:
                                # no other worker holds Started.  Abandon for immediate
                                # redelivery — a fresh delivery will pre-read the current
                                # ETag and succeed without hitting the two-412 window.
                                # Spec action: RereadAfterRetry412 (Pending/Scheduled) → abandon
                                _reread2_status = _reread2.status if _reread2 is not None else None
                                logger.error(
                                    f"{_graph_tag} Two 412s on Started write; second re-read returned "
                                    f"{_reread2_status!r} "
                                    f"for task {_task_tag}. "
                                    "Abandoning for immediate redelivery.",
                                )
                                try:
                                    await self.abandon_current_message()
                                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                                    logger.warning(
                                        f"{_graph_tag} Failed to abandon message after two 412s for task "
                                        f"{_task_tag}; SB will redeliver when lock expires",
                                        exc_info=True,
                                    )
                                return TaskResult(
                                    task_id=self.task.task_id,
                                    graph_id=self.task.graph_id,
                                    status=TaskStatus.Scheduled,
                                )
                elif _reread.status == TaskStatus.Retry:
                    # Another worker executed the task and published a delayed retry
                    # SB message.  Settle our duplicate — the graph will continue via
                    # the retry message.
                    # Spec action: WriteStarted412 (Retry) → Completing
                    logger.warning(
                        f"{_graph_tag} 412 on Started write; re-read returned Retry for task {_task_tag}; "
                        "another worker already retried — settling our duplicate"
                    )
                    try:
                        await self.complete_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to complete message after 412+Retry for task {_task_tag}; "
                            "SB will redeliver",
                            exc_info=True,
                        )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Retry,
                    )

                else:
                    # Truly unexpected status after 412 (not None, finished, Started,
                    # Scheduled, Pending, or Retry) — this branch should be unreachable
                    # for all currently defined statuses.  Abandon for immediate redelivery.
                    logger.error(
                        f"{_graph_tag} 412 on Started write but re-read returned unexpected status "
                        f"{_reread.status!r} for task {_task_tag}; "
                        "abandoning for immediate redelivery."
                    )
                    try:
                        await self.abandon_current_message()
                    except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                        logger.warning(
                            f"{_graph_tag} Failed to abandon message after 412+unexpected status for task "
                            f"{_task_tag}; SB will redeliver when lock expires",
                            exc_info=True,
                        )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=_reread.status,
                    )

        # We successfully wrote Started — we own execution from here.
        # Subsequent writes (RetriesExhausted, final result) are unconditional:
        # no other worker can simultaneously write a terminal result without also
        # passing through the Started ETag write we just won.

        # Immediately record an attempt
        self.task.record_attempt()

        # at-most once: "complete" msg even if it fails later
        if self.task.acks_early:
            try:
                await self.complete_message()
                message_settled = True
            except exc.BoilermakerTaskLeaseLost:
                logger.error(f"{_graph_tag} Lost message lease when trying to complete early for task {_task_tag}")
                return TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=["Lost message lease"],
                )
            except exc.BoilermakerServiceBusError:
                logger.error(f"{_graph_tag} Unknown ServiceBusError", exc_info=True)
                return TaskResult(
                    task_id=self.task.task_id,
                    graph_id=self.task.graph_id,
                    status=TaskStatus.Failure,
                    errors=["ServiceBus error"],
                )

        if not self.task.can_retry:
            logger.error(f"{_graph_tag} Retries exhausted for {_task_tag}")
            if not message_settled:
                try:
                    await self.deadletter_or_complete_task("ProcessingError", detail="Retries exhausted")
                    message_settled = True
                except exc.BoilermakerTaskLeaseLost:
                    logger.error(
                        f"{_graph_tag} Lost message lease when trying to deadletter/complete {_task_tag}"
                        " after retries exhausted"
                    )
                    return TaskResult(
                        task_id=self.task.task_id,
                        graph_id=self.task.graph_id,
                        status=TaskStatus.Failure,
                        errors=["Lost message lease during retry exhaustion"],
                    )
                except exc.BoilermakerServiceBusError:
                    logger.error(f"{_graph_tag} Unknown ServiceBusError", exc_info=True)
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
                    f"{_graph_tag} continue_graph failed after retries exhausted for {_task_tag}; "
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
                # Transient load_graph failure — abandon for immediate redelivery.
                # The task result is already written; a fresh delivery will skip
                # re-execution (idempotency guard) and retry continue_graph directly.
                logger.error(
                    f"{_graph_tag} continue_graph failed for task {_task_tag}; abandoning for immediate redelivery",
                    exc_info=True,
                )
                try:
                    await self.abandon_current_message()
                except (exc.BoilermakerTaskLeaseLost, exc.BoilermakerServiceBusError):
                    logger.warning(
                        f"{_graph_tag} Failed to abandon message after continue_graph failure for task {_task_tag}; "
                        "SB will redeliver when lock expires",
                        exc_info=True,
                    )
                return result
        elif result.status == TaskStatus.Retry:
            # Retry requested: republish the same task with delay
            delay = self.task.get_next_delay()
            retry_msg_id = f"{self.task.task_id}:{self.task.attempts.attempts}"
            warn_msg = (
                f"{_graph_tag} {result.errors} {_task_tag} "
                f"[attempt {self.task.attempts.attempts} of {self.task.policy.max_tries}] "
                f"Publishing retry... {self.sequence_number=} "
                f"<function={self.task.function_name}> with {delay=} {retry_msg_id=}"
            )
            logger.warning(warn_msg)
            await self.publish_task(
                self.task,
                delay=delay,
                unique_msg_id=retry_msg_id,
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
                    f"{_graph_tag} Lost message lease when trying to complete late for task {_task_tag} "
                    "May result in multiple executions of this task and its callbacks!"
                )
                return result
            except exc.BoilermakerServiceBusError:
                logger.error(f"{_graph_tag} Unknown ServiceBusError", exc_info=True)
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
                        f"[Graph {graph_id}] not found in storage (404); downstream tasks will not be dispatched. "
                        "This graph may have been deleted.",
                        exc_info=True,
                    )
                    return None
                # Transient error — will retry or raise ContinueGraphError after max_tries
                last_exc = e
                if attempt < _LOAD_GRAPH_RETRY_POLICY.max_tries - 1:
                    delay = _LOAD_GRAPH_RETRY_POLICY.get_delay_interval(attempt)
                    logger.warning(
                        f"[Graph {graph_id}] load_graph failed "
                        f"(attempt {attempt + 1}/{_LOAD_GRAPH_RETRY_POLICY.max_tries}); "
                        f"retrying in {delay}s",
                        exc_info=True,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"[Graph {graph_id}] load_graph failed after "
                        f"{_LOAD_GRAPH_RETRY_POLICY.max_tries} attempts; "
                        "raising ContinueGraphError to suppress message settlement",
                        exc_info=True,
                    )
                    raise exc.ContinueGraphError(
                        f"[Graph {graph_id}] load_graph failed after {_LOAD_GRAPH_RETRY_POLICY.max_tries} attempts"
                    ) from last_exc
        else:
            # Should only be reached if max_tries == 0 (not expected).
            raise exc.ContinueGraphError(f"[Graph {graph_id}] load_graph not attempted")

        if not graph:
            # Permanent failure: graph blob does not exist.  Redelivery will not help.
            # Settling the upstream message is intentional here.
            logger.critical(
                f"[Graph {graph_id}] not found after task completion — "
                "downstream tasks will never be dispatched. "
                "This is a permanent data loss; redelivery will not recover it."
            )
            return None

        # Sanity check: did we load the result that was *just* stored?
        loaded_task_status = graph.get_status(completed_task_result.task_id)
        if loaded_task_status != completed_task_result.status:

            logger.error(
                f"[Graph {graph_id}] Task status mismatch: "
                f"expected {completed_task_result.task_id} to be {completed_task_result.status}, "
                f"but got {loaded_task_status}. Suppressing settlement to allow redelivery."
            )
            raise exc.ContinueGraphError(
                f"[Graph {graph_id}] Status mismatch for task {completed_task_result.task_id}: "
                f"expected {completed_task_result.status}, got {loaded_task_status}"
            )

        # Find and publish newly ready tasks
        ready_count = 0
        for ready_task in itertools.chain.from_iterable(
            (graph.generate_ready_tasks(), graph.generate_failure_ready_tasks())
        ):
            if await self._lease_publish_schedule(ready_task, graph, graph_id):
                ready_count += 1

        # Dispatch the all_failed_callback if the graph has reached terminal-failed state.
        # This loop yields at most one task.
        for callback_task in graph.generate_all_failed_callback_task():
            if await self._lease_publish_schedule(callback_task, graph, graph_id):
                ready_count += 1

        if ready_count == 0:
            logger.debug(f"[Graph {graph_id}] No new tasks ready after task {completed_task_result.task_id}")

        return ready_count

    async def _lease_publish_schedule(
        self,
        task: Task,
        graph: TaskGraph,
        graph_id: GraphId,
    ) -> bool:
        """Acquire a lease, publish a task to Service Bus, and write Scheduled status.

        Implements the four-step dispatch protocol used by ``continue_graph()``:

        1. Acquire an ETag-guarded blob lease (atomic CAS that prevents double-dispatch).
        2. Publish the task to Service Bus (message_id = task_id for dedup).
        3. Write ``Scheduled`` status to blob storage under the held lease.
        4. Release the lease (always, via ``finally``).

        If the lease cannot be acquired (another worker holds it, or the blob was
        modified since ``load_graph``), the task is silently skipped — it will be
        retried on redelivery.

        Returns:
            True if the task was successfully published, False otherwise.
        """
        task_result_slim = graph.results.get(task.task_id)
        lease_id = None
        try:
            lease_id = await self.storage_interface.try_acquire_lease(
                task.task_id,
                graph_id,
                etag=task_result_slim.etag if task_result_slim else None,
            )
        except exc.BoilermakerStorageError:
            logger.warning(
                f"[Graph {graph_id}] try_acquire_lease raised unexpected error for task "
                f"{task.task_id}; skipping — will be retried on redelivery.",
                exc_info=True,
            )
            return False
        if lease_id is None:
            logger.debug(
                f"[Graph {graph_id}] Skipping task {task.task_id}: "
                "lease not acquired (another worker holds it or blob was modified)."
            )
            return False

        try:
            # Publish task to SB (message_id = task_id for fan-in dedup).
            _task_tag = self._task_tag(task)
            await self.publish_task(task)
            logger.info(f"[Graph {graph_id}] Published task {_task_tag}")

            # Write Scheduled status under the held lease. The lease prevents a
            # racing worker (that picked up the just-published SB message) from
            # writing Started before this Scheduled write completes.
            try:
                result = graph.schedule_task(task.task_id)
                await self.storage_interface.store_task_result(result, lease_id=lease_id)
            except exc.BoilermakerStorageError as err:
                logger.warning(
                    f"[Graph {graph_id}] Failed to write Scheduled for task {_task_tag}. "
                    f"Task published to Service Bus. Error: {err}",
                    exc_info=True,
                )
                return True  # published successfully, blob write failed
            except ValueError:
                logger.error(
                    f"[Graph {graph_id}] schedule_task raised ValueError for task {_task_tag}. "
                    f"Task published; blob write skipped.",
                    exc_info=True,
                )
                return True  # published successfully, schedule_task rejected it

            return True

        except Exception:
            logger.error(
                f"[Graph {graph_id}] Failed to publish task {_task_tag}; remains Pending, will retry on redelivery.",
                exc_info=True,
            )
            return False
        finally:
            if lease_id is not None:
                await self.storage_interface.release_lease(task.task_id, graph_id, lease_id)
