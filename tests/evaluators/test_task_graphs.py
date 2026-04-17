import itertools
from unittest.mock import AsyncMock, MagicMock

import pytest
from boilermaker import exc, retries
from boilermaker.evaluators import TaskGraphEvaluator
from boilermaker.task import Task, TaskGraph, TaskResult, TaskResultSlim, TaskStatus


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler generic Tests
# (Making sure parent-class expectations are not violated)
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_missing_function(evaluator_context, mock_storage):
    """Test that message_handler returns failure for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")
    task.graph_id = "test-graph-id"
    evaluator = evaluator_context.evaluator
    evaluator.task = task

    # Should not raise
    result = await evaluator()
    assert result.status == TaskStatus.Failure
    mock_storage.store_task_result.assert_not_called()


async def test_message_handler_debug_task(evaluator_context, mock_storage):
    """Test that message_handler runs the debug task."""
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    task.graph_id = "test-graph-id"
    evaluator = evaluator_context.evaluator
    evaluator.task = task

    assert await evaluator() is None

    # Should not store result
    mock_storage.store_task_result.assert_not_called()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Initialization Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
def test_task_graph_evaluator_requires_storage(app, mockservicebus, evaluator_context):
    """Test that TaskGraphEvaluator requires a storage interface."""

    with pytest.raises(ValueError, match="Storage interface is required"):
        TaskGraphEvaluator(
            mockservicebus._receiver,
            evaluator_context.current_task,
            evaluator_context.mock_task_publisher,
            app.function_registry,
            state=app.state,
            storage_interface=None,
        )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message Handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_early", [True, False])
async def test_message_handler_no_graph_success(
    acks_early,
    evaluator_context,
):
    """Test successful message handling with early/late acks."""
    evaluator = evaluator_context.evaluator
    evaluator.task.acks_late = not acks_early
    evaluator.task.graph_id = None
    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
        check_graph_loaded=False,
    ) as ctx:
        ctx.assert_graph_not_loaded()


async def test_evaluator_success(success_scenario):
    """Simple success test for TaskGraphEvaluator."""

    async with success_scenario.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # Standard success dependent called
        expected = {"positive", "success_callback"}
        ctx.assert_messages_scheduled(2)
        published = success_scenario.get_scheduled_messages()
        assert len(published) == 2
        pub_funcs = {msg.task.function_name for msg in published}
        msg = f"Expected messages: {expected}, got {pub_funcs}"
        assert pub_funcs == expected, msg

        others = ctx.get_other_storage_calls()
        assert len(others) == 2
        t1, t2 = others
        t1 = t1.args[0]
        t2 = t2.args[0]
        assert all([isinstance(tsk, TaskResultSlim) for tsk in (t1, t2)])
        task1 = success_scenario.get_task(t1.task_id)
        task2 = success_scenario.get_task(t2.task_id)
        stored_funcs = {task1.function_name, task2.function_name}
        msg = f"Expected tasks: {expected}, got {stored_funcs}"
        assert stored_funcs == expected, msg


async def test_message_handler_with_on_success_callback(
    app,
    success_scenario,
):
    """
    Test message_handler with on_success callback, which should *not* be invoked.
    """

    # We are adding a fake on_success callback which we do not expect to be invoked
    async def on_success_fake(state):
        return "fake"

    app.register_async(on_success_fake, policy=retries.NoRetry())
    success_scenario.current_task.on_success = Task.si(on_success_fake)

    async with success_scenario.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # Standard success dependent called
        expected = {"positive", "success_callback"}
        ctx.assert_messages_scheduled(2)
        published = success_scenario.get_scheduled_messages()
        assert len(published) == 2
        pub_funcs = {msg.task.function_name for msg in published}
        msg = f"Expected messages: {expected}, got {pub_funcs}"
        assert pub_funcs == expected, msg

        others = ctx.get_other_storage_calls()
        assert len(others) == 2
        t1, t2 = others
        t1 = t1.args[0]
        t2 = t2.args[0]
        assert all([isinstance(tsk, TaskResultSlim) for tsk in (t1, t2)])
        task1 = success_scenario.get_task(t1.task_id)
        task2 = success_scenario.get_task(t2.task_id)
        stored_funcs = {task1.function_name, task2.function_name}
        msg = f"Expected tasks: {expected}, got {stored_funcs}"
        assert stored_funcs == expected, msg


async def test_message_handler_with_retry_exception(retry_scenario):
    """Test message_handler handling a regular RetryException."""

    async with retry_scenario.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Retry,
        check_graph_loaded=False,
    ) as ctx:
        assert ctx.current_task_result.errors == ["Retry for 100"]
        ctx.assert_messages_scheduled(1)
        ctx.assert_graph_not_loaded()


async def test_message_handler_retries_exhausted(retries_exhausted_scenario, make_message):
    """Test message_handler when retries are exhausted."""
    # Set up task with exhausted retries
    evaluator = retries_exhausted_scenario.evaluator
    evaluator.task.attempts.attempts = evaluator.task.policy.max_tries + 1
    # Can't dead letter without this attrib
    evaluator.task.msg = make_message(evaluator.task, sequence_number=1234)

    async with retries_exhausted_scenario.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.RetriesExhausted,
    ) as ctx:
        # It's okay for this one to have been scheduled: its parent had previously succeeded!
        callback_allowed = ctx.success_callback_single_parent_task.task_id
        # It's *not* okay for this one to have been scheduled because one of its parents failed with exhausted retries
        callback_not_allowed = ctx.success_callback_two_parents_task.task_id
        # We expect this failure to have been called
        failure_callback = ctx.failure_callback_task.task_id
        ctx.assert_messages_scheduled(2)
        sched = set(msg.task.task_id for msg in ctx.get_scheduled_messages())
        assert callback_not_allowed not in sched, "Task with failed parent should not be scheduled!"
        assert {callback_allowed, failure_callback} == sched, "Expect only allowed and failure callbacks scheduled!"

        # Get other stored calls to check what was scheduled after
        others = ctx.get_other_storage_calls()
        assert len(others) == 2
        stored_task_ids = set(t.args[0].task_id for t in others)
        assert stored_task_ids == {callback_allowed, failure_callback}
        # Should be deadleattered
        ctx.assert_msg_dead_lettered()


async def test_message_handler_with_exception(exception_scenario):
    """Test message_handler handling regular exceptions."""

    async with exception_scenario.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Failure,
    ) as ctx:
        assert ctx.current_task_result.errors
        # It's okay for this one to have been scheduled: its parent had previously succeeded!
        callback_allowed = ctx.success_callback_single_parent_task.task_id
        # It's *not* okay for this one to have been scheduled because one of its parents failed with exhausted retries
        callback_not_allowed = ctx.success_callback_two_parents_task.task_id
        # We expect this failure to have been called
        failure_callback = ctx.failure_callback_task.task_id
        ctx.assert_messages_scheduled(2)
        sched = set(msg.task.task_id for msg in ctx.get_scheduled_messages())
        assert callback_not_allowed not in sched, "Task with failed parent should not be scheduled!"
        assert {callback_allowed, failure_callback} == sched, "Expect only allowed and failure callbacks scheduled!"

        # Get other stored calls to check what was scheduled after
        others = ctx.get_other_storage_calls()
        assert len(others) == 2
        stored_task_ids = set(t.args[0].task_id for t in others)
        assert stored_task_ids == {callback_allowed, failure_callback}
        # Should be deadleattered
        ctx.assert_msg_dead_lettered()


async def test_retry_policy_update_with_storage(app, evaluator_context):
    """Test that retry policy can be updated during retry exception handling."""

    async def retrytask2(state):
        new_policy = retries.RetryPolicy(max_tries=10, retry_mode=retries.RetryMode.Exponential)
        raise retries.RetryException("Retry with new policy", policy=new_policy)

    app.register_async(retrytask2, policy=retries.RetryPolicy.default())
    task = app.create_task(retrytask2)

    task.msg = evaluator_context.make_message(task, sequence_number=123)
    evaluator_context.evaluator.task = task
    async with evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Retry,
        check_graph_loaded=False,
    ) as ctx:
        ctx.assert_messages_scheduled(1)

        scheduled_messages = evaluator_context.get_scheduled_messages()
        assert scheduled_messages and scheduled_messages[0].task.function_name == "retrytask2"

        # Task policy should be updated
        assert task.policy.max_tries == 10
        assert task.policy.retry_mode == retries.RetryMode.Exponential


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Exception handling tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_early_ack_task_lease_lost_exception(evaluator_context, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.graph_id = "test-graph-id"
    evaluator_context.current_task = task

    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator_context.evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator_context()

    # Should return None when lease is lost during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]

    evaluator_context.evaluator.complete_message.assert_called_once()

    # Should still store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_early_ack_service_bus_error_exception(evaluator_context, mock_storage, app):
    """Test BoilermakerServiceBusError exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.graph_id = "test-graph-id"
    evaluator_context.current_task = task

    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator_context.evaluator.complete_message = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator_context()

    # Should return None when service bus error occurs during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    evaluator_context.evaluator.complete_message.assert_called_once()
    # Should still store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_retries_exhausted_task_lease_lost_exception(retries_exhausted_scenario, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception when settling message for exhausted retries."""

    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    retries_exhausted_scenario.evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerTaskLeaseLost("Lease lost")
    )

    result = await retries_exhausted_scenario()

    # Should return None when lease is lost during exhausted retries settlement
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]
    retries_exhausted_scenario.evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )

    # Should store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_retries_exhausted_service_bus_error_exception(retries_exhausted_scenario, mock_storage, app):
    """Test BoilermakerServiceBusError exception when settling message for exhausted retries."""

    # Mock deadletter_or_complete_task to raise BoilermakerServiceBusError
    retries_exhausted_scenario.evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await retries_exhausted_scenario()
    # Should return None when service bus error occurs during exhausted retries settlement
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    retries_exhausted_scenario.evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )

    # Should store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_late_settlement_task_lease_lost_exception_success(evaluator_context, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    evaluator_context.current_task = task

    # Mock continue_graph so the test focuses on settlement behavior, not graph loading.
    evaluator_context.evaluator.continue_graph = AsyncMock(return_value=0)
    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator_context.evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator_context()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator_context.evaluator.complete_message.assert_called_once()

    # Should store both start and success results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_task_lease_lost_exception_failure(evaluator_context, mock_storage, app, make_message):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for failed task."""

    async def failtask(state):
        raise ValueError("Test failure")

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    task.msg = make_message(task)
    evaluator_context.current_task = task

    # Mock continue_graph so the test focuses on settlement behavior, not graph loading.
    evaluator_context.evaluator.continue_graph = AsyncMock(return_value=0)
    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    evaluator_context.evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerTaskLeaseLost("Lease lost")
    )

    result = await evaluator_context()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator_context.evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")

    # Should store both start and failure results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_service_bus_error_exception_success(evaluator_context, mock_storage, app):
    """Test BoilermakerServiceBusError exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    evaluator_context.current_task = task

    # Mock continue_graph so the test focuses on settlement behavior, not graph loading.
    evaluator_context.evaluator.continue_graph = AsyncMock(return_value=0)
    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator_context.evaluator.complete_message = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator_context()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator_context.evaluator.complete_message.assert_called_once()

    # Should store both start and success results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_service_bus_error_exception_failure(evaluator_context, mock_storage, app, make_message):
    """Test BoilermakerServiceBusError exception during late message settlement for failed task."""

    async def failtask(state):
        raise ValueError("Test failure")

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    task.msg = make_message(task)
    evaluator_context.current_task = task

    # Mock continue_graph so the test focuses on settlement behavior, not graph loading.
    evaluator_context.evaluator.continue_graph = AsyncMock(return_value=0)
    # Mock deadletter_or_complete_task to raise BoilermakerServiceBusError
    evaluator_context.evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator_context()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator_context.evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")

    # Should store both start and failure results
    assert mock_storage.store_task_result.call_count == 2


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# continue_graph Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


# THIS IS AN EXTREMELY IMPORTANT SAFETY PROPERTY TEST
# DO NOT ALLOW REGRESSIONS FOR THIS TEST. DO NOT SKIP. DO NOT IGNORE.
async def test_safety_prop_publish_before_store_blob_failure_harmless(success_scenario):
    """Safety property: With publish-before-store, tasks are published even when the
    subsequent blob write fails.  This is safe because Service Bus duplicate detection
    (keyed on task_id) prevents genuine duplicate delivery on redelivery, and the blob
    remaining in Pending status means generate_ready_tasks() will re-discover the task."""
    started_stored_effects = [None, None]  # start success, task result success
    # Any successive writes should fail
    side_effecty = itertools.chain(
        started_stored_effects, itertools.repeat(exc.BoilermakerStorageError("Blob write failed"))
    )
    success_scenario.mock_storage.store_task_result.side_effect = side_effecty
    async with success_scenario.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # Tasks ARE published (publish happens before blob write); blob failure is harmless
        ctx.assert_messages_scheduled(2)


async def test_continue_graph_no_graph_id(evaluator_context):
    """Test continue_graph with no graph_id."""
    result = TaskResult(task_id="test-task", graph_id=None, status=TaskStatus.Success, result=42)

    # Should not raise any errors and should return early
    result = await evaluator_context.evaluator.continue_graph(result)
    assert result is None


async def test_graph_workflow_exception_handling(evaluator_context):
    """Test that a transient load_graph exception suppresses message settlement for redelivery."""
    from unittest.mock import patch

    # Mock storage.load_graph to raise a transient BoilermakerStorageError (the exception
    # type that load_graph actually raises in production when a non-404 storage error occurs).
    evaluator_context.mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("Storage error")

    # Patch asyncio.sleep so retry backoff doesn't slow the test
    with patch("asyncio.sleep"):
        result = await evaluator_context()

    # Task itself should have succeeded
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # load_graph was retried _LOAD_GRAPH_RETRY_POLICY.max_tries times before giving up
    from boilermaker.evaluators.task_graph import _LOAD_GRAPH_RETRY_POLICY

    assert evaluator_context.mock_storage.load_graph.call_count == _LOAD_GRAPH_RETRY_POLICY.max_tries

    # Message must NOT be settled — suppress settlement to allow Service Bus redelivery
    assert len(evaluator_context.mockservicebus._receiver.method_calls) == 0, (
        "Message should NOT be settled when load_graph raises (transient error path)"
    )


async def test_continue_graph_graph_not_found(evaluator_context):
    """Test continue_graph when graph is not found."""
    result = TaskResult(
        task_id="test-task",
        graph_id="missing-graph",
        status=TaskStatus.Success,
        result=42,
    )

    evaluator_context.mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("Not found", status_code=404)

    # Should not raise errors, just log CRITICAL and return None
    result = await evaluator_context.evaluator.continue_graph(result)
    assert result is None
    evaluator_context.mock_storage.load_graph.assert_called_with("missing-graph")


async def test_continue_graph_no_ready_tasks(evaluator_context):
    """Test continue_graph when no tasks are ready."""
    # Root task STARTED
    parent_started = TaskResult(
        task_id=next(iter(evaluator_context.graph.edges.keys())),
        graph_id=evaluator_context.graph.graph_id,
        status=TaskStatus.Started,
        result=None,
    )
    evaluator_context.graph.add_result(parent_started)
    evaluator_context.mock_storage.load_graph.return_value = evaluator_context.graph

    published_tasks = []

    async def mock_publish_task(task, *args, **kwargs):
        published_tasks.append(task)

    first_child_task_id = next(iter(next(iter(evaluator_context.graph.edges.values()))))
    child_result = TaskResult(
        task_id=first_child_task_id,
        graph_id=evaluator_context.graph.graph_id,
        status=TaskStatus.Success,
        result=42,
    )
    # continue_graph checks that the loaded graph's status matches completed_task_result.status.
    # Populate the graph's results so get_status() returns Success for the completing task.
    evaluator_context.graph.add_result(child_result)
    evaluator_context.evaluator.task_publisher = mock_publish_task
    await evaluator_context.evaluator.continue_graph(child_result)

    # Should not publish any tasks since no new tasks are ready
    assert len(published_tasks) == 0


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Double-yield in generate_failure_ready_tasks
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_generate_failure_ready_tasks_no_double_yield(app):
    """A shared failure callback with two failed parents must be yielded exactly once."""

    async def task_a(state):
        return "A"

    async def task_b(state):
        return "B"

    async def fail_cb(state):
        return "failure handled"

    app.register_many_async([task_a, task_b, fail_cb])

    a = app.create_task(task_a)
    b = app.create_task(task_b)
    f = app.create_task(fail_cb)

    graph = TaskGraph()
    graph.add_task(a)
    graph.add_task(b)
    # Shared failure callback registered on both A and B
    graph.add_failure_callback(a.task_id, f)
    graph.add_failure_callback(b.task_id, f)

    # Initialize pending results
    list(graph.generate_pending_results())

    # Mark both A and B as failed
    graph.add_result(TaskResult(task_id=a.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))
    graph.add_result(TaskResult(task_id=b.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))

    # The shared failure callback should only appear once
    failure_ready = list(graph.generate_failure_ready_tasks())
    assert len(failure_ready) == 1, (
        f"Expected exactly 1 failure-ready task, got {len(failure_ready)}. "
        "Double-yield bug: same callback yielded once per failed parent."
    )
    assert failure_ready[0].task_id == f.task_id


async def test_continue_graph_shared_failure_callback_no_raise(evaluator_context, app):
    """continue_graph must not raise when a shared failure callback has two failed parents."""

    async def task_a(state):
        return "A"

    async def task_b(state):
        return "B"

    async def shared_fail_cb(state):
        return "shared failure handled"

    app.register_many_async([task_a, task_b, shared_fail_cb])

    a = app.create_task(task_a)
    b = app.create_task(task_b)
    f = app.create_task(shared_fail_cb)

    graph = TaskGraph()
    graph.add_task(a)
    graph.add_task(b)
    graph.add_failure_callback(a.task_id, f)
    graph.add_failure_callback(b.task_id, f)

    list(graph.generate_pending_results())

    # Both parents failed
    graph.add_result(TaskResult(task_id=a.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))
    graph.add_result(TaskResult(task_id=b.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))

    evaluator_context.graph = graph
    evaluator_context.mock_storage.load_graph.return_value = graph

    # Build the completed_task_result as if A just finished (failed)
    completed = TaskResult(task_id=a.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure)

    # Should not raise even though both parents are failed
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    # The shared failure callback must have been dispatched exactly once
    published = evaluator_context.get_scheduled_messages()
    assert len(published) == 1, f"Expected F dispatched exactly once, got {len(published)}"
    assert published[0].task.task_id == f.task_id

    assert ready_count == 1


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# schedule_task ValueError outside try/except
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_schedule_task_value_error_does_not_propagate(evaluator_context):
    """ValueError from schedule_task must be caught; continue_graph must return without raising.

    With publish-before-store, the task IS published before schedule_task is called.
    ValueError from schedule_task only prevents the blob write — the task was already published.
    """
    # Build a mock graph whose schedule_task always raises ValueError.
    # We use a MagicMock(spec=TaskGraph) so we bypass Pydantic's __setattr__ restriction.
    real_graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task

    mock_graph = MagicMock(spec=TaskGraph)
    mock_graph.graph_id = real_graph.graph_id
    mock_graph.results = {}
    # Sanity check: status must match the completed result's status
    mock_graph.get_status.return_value = TaskStatus.Success
    # generate_ready_tasks yields the ok_task (so schedule_task gets called)
    mock_graph.generate_ready_tasks.return_value = iter([ok_task])
    mock_graph.generate_failure_ready_tasks.return_value = iter([])
    # schedule_task always raises ValueError
    mock_graph.schedule_task.side_effect = ValueError("task is not pending")

    evaluator_context.mock_storage.load_graph.return_value = mock_graph

    completed = TaskResult(
        task_id=ok_task.task_id,
        graph_id=real_graph.graph_id,
        status=TaskStatus.Success,
        result="OK",
    )

    # Should not raise
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    # Task IS published (publish-before-store); ValueError only prevents blob write
    published = evaluator_context.get_scheduled_messages()
    assert len(published) == 1, "Task should be published even when schedule_task raises ValueError"
    assert ready_count == 1


async def test_message_settled_even_when_schedule_task_raises(evaluator_context, mock_storage):
    """message_handler must settle its message even if schedule_task raises ValueError inside continue_graph."""
    evaluator_context.prep_task_to_succeed()

    real_graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task

    # Build a mock graph whose schedule_task always raises ValueError
    mock_graph = MagicMock(spec=TaskGraph)
    mock_graph.graph_id = real_graph.graph_id
    mock_graph.results = {}
    mock_graph.get_status.return_value = TaskStatus.Success
    mock_graph.generate_ready_tasks.return_value = iter([ok_task])
    mock_graph.generate_failure_ready_tasks.return_value = iter([])
    mock_graph.schedule_task.side_effect = ValueError("not pending")
    evaluator_context.mock_storage.load_graph.return_value = mock_graph

    # Run the full evaluator (message_handler path)
    result = await evaluator_context()

    # Task itself should have succeeded
    assert result.status == TaskStatus.Success

    # The message must have been settled (complete_message called, not stuck)
    evaluator_context.assert_msg_settled()


async def test_other_tasks_dispatched_when_one_raises_value_error(evaluator_context, app):
    """When schedule_task raises ValueError for one task, remaining tasks in batch are still dispatched."""

    async def sibling_a(state):
        return "A"

    async def sibling_b(state):
        return "B"

    async def root_task(state):
        return "root"

    app.register_many_async([sibling_a, sibling_b, root_task])

    root = app.create_task(root_task)
    sa = app.create_task(sibling_a)
    sb = app.create_task(sibling_b)

    graph = TaskGraph()
    graph.add_task(root)
    graph.add_task(sa, parent_ids=[root.task_id])
    graph.add_task(sb, parent_ids=[root.task_id])

    list(graph.generate_pending_results())

    # Root succeeded
    graph.add_result(TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    # Build a mock graph wrapping the real one so we can selectively raise ValueError for sibling_a
    mock_graph = MagicMock(spec=TaskGraph)
    mock_graph.graph_id = graph.graph_id
    mock_graph.results = {}
    mock_graph.get_status.return_value = TaskStatus.Success
    # Both siblings are ready
    mock_graph.generate_ready_tasks.return_value = iter([sa, sb])
    mock_graph.generate_failure_ready_tasks.return_value = iter([])

    call_count = {"n": 0}

    def selective_schedule_task(task_id):
        call_count["n"] += 1
        if task_id == sa.task_id:
            raise ValueError("already scheduled")
        # For sibling_b, return a minimal scheduled result
        return graph.schedule_task(task_id)

    mock_graph.schedule_task.side_effect = selective_schedule_task
    evaluator_context.mock_storage.load_graph.return_value = mock_graph

    completed = TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success)
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    published = evaluator_context.get_scheduled_messages()
    published_ids = {msg.task.task_id for msg in published}

    # Both siblings are published (publish-before-store); ValueError only prevents blob write
    assert sb.task_id in published_ids, "sibling_b should be dispatched"
    assert sa.task_id in published_ids, (
        "sibling_a should be dispatched (publish happens before schedule_task; ValueError only prevents blob write)"
    )
    assert ready_count == 2


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# load_graph failure must not settle message
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_load_graph_storage_error_no_settlement(evaluator_context, mock_storage):
    """Transient load_graph failure must suppress message settlement and not publish any tasks."""
    from unittest.mock import patch

    from boilermaker.evaluators.task_graph import _LOAD_GRAPH_RETRY_POLICY

    # Simulate a transient storage error on every attempt
    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("503 Service Unavailable")

    # Patch asyncio.sleep so retry backoff doesn't slow the test
    with patch("asyncio.sleep"):
        result = await evaluator_context()

    # Task itself succeeded
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # load_graph was retried the configured number of times
    assert mock_storage.load_graph.call_count == _LOAD_GRAPH_RETRY_POLICY.max_tries

    # Message must NOT be settled — allow Service Bus redelivery
    assert len(evaluator_context.mockservicebus._receiver.method_calls) == 0, (
        "Message must not be settled when load_graph raises a transient error"
    )

    # No downstream tasks must be published
    assert evaluator_context.get_scheduled_messages() == [], "task_publisher must not be called when load_graph fails"


async def test_load_graph_not_found_settles_and_logs_critical(evaluator_context, mock_storage, caplog):
    """Permanent load_graph failure (404 BoilermakerStorageError) must settle message and log CRITICAL.

    In production, the underlying library raises AzureBlobError (wrapping the HTTP 404 response)
    which load_graph re-raises as BoilermakerStorageError(status_code=404).  load_graph never
    returns None for a missing blob; this test reflects the actual production code path.
    """
    import logging

    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("Not found", status_code=404)

    with caplog.at_level(logging.CRITICAL, logger="boilermaker.app"):
        result = await evaluator_context()

    # Task itself succeeded
    assert result.status == TaskStatus.Success

    # Message MUST be settled — redelivery won't help when the graph blob is gone
    evaluator_context.assert_msg_settled()

    # A CRITICAL log entry must be emitted
    critical_msgs = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert critical_msgs, (
        "A CRITICAL log must be emitted when load_graph raises BoilermakerStorageError(status_code=404)"
    )


async def test_no_publish_when_load_graph_fails(evaluator_context, mock_storage):
    """task_publisher must never be called when load_graph raises."""
    from unittest.mock import patch

    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("Transient error")

    with patch("asyncio.sleep"):
        await evaluator_context()

    assert evaluator_context.get_scheduled_messages() == [], "task_publisher must not be called when load_graph raises"


async def test_non404_storage_error_retried_raises_continue_graph_error_no_settlement(
    evaluator_context, mock_storage
):
    """A non-404 BoilermakerStorageError (e.g. 503) must be retried and eventually
    raise ContinueGraphError, leaving the message unsettled for Service Bus redelivery.

    This distinguishes the transient case (status_code=503) from the permanent 404 case.
    """
    from unittest.mock import patch

    from boilermaker.evaluators.task_graph import _LOAD_GRAPH_RETRY_POLICY

    # Simulate a 503 (transient) storage error on every attempt
    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("503 Service Unavailable", status_code=503)

    with patch("asyncio.sleep"):
        result = await evaluator_context()

    # Task itself succeeded
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # load_graph must have been retried the full number of times
    assert mock_storage.load_graph.call_count == _LOAD_GRAPH_RETRY_POLICY.max_tries, (
        f"Expected {_LOAD_GRAPH_RETRY_POLICY.max_tries} load_graph attempts, got {mock_storage.load_graph.call_count}"
    )

    # Message must NOT be settled — Service Bus should redeliver
    assert len(evaluator_context.mockservicebus._receiver.method_calls) == 0, (
        "Message must not be settled when load_graph raises a transient 503 error"
    )

    # No downstream tasks published
    assert evaluator_context.get_scheduled_messages() == [], (
        "task_publisher must not be called when load_graph raises 503"
    )


async def test_retries_exhausted_continue_graph_error_does_not_propagate(
    retries_exhausted_scenario, mock_storage
):
    """ContinueGraphError from continue_graph on the RetriesExhausted path must not propagate.

    The message is already deadlettered before continue_graph is called, so
    suppressing settlement is not possible.  The correct behaviour is to log
    and return the RetriesExhausted result gracefully without raising.
    """
    from unittest.mock import patch

    from boilermaker.evaluators.task_graph import _LOAD_GRAPH_RETRY_POLICY

    # Make load_graph raise a transient storage error on every attempt so that
    # continue_graph exhausts its retry policy and raises ContinueGraphError.
    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("503 Service Unavailable")

    # Patch asyncio.sleep so retry backoff does not slow the test
    with patch("asyncio.sleep"):
        result = await retries_exhausted_scenario()

    # ContinueGraphError must NOT propagate — message_handler must return cleanly
    assert result is not None, "message_handler must return a result, not raise ContinueGraphError"
    assert result.status == TaskStatus.RetriesExhausted, f"Expected RetriesExhausted status, got {result.status}"

    # load_graph should have been attempted (and exhausted the retry policy)
    assert mock_storage.load_graph.call_count == _LOAD_GRAPH_RETRY_POLICY.max_tries, (
        f"Expected {_LOAD_GRAPH_RETRY_POLICY.max_tries} load_graph attempts, got {mock_storage.load_graph.call_count}"
    )

    # The message must have been settled (deadlettered) before continue_graph was called
    retries_exhausted_scenario.assert_msg_dead_lettered()


async def test_pending_task_published_exactly_once_from_ready_loop(evaluator_context, app):
    """A Pending task that becomes ready is published exactly once by the ready-task loop."""

    async def root_task(state):
        return "root"

    async def pending_child(state):
        return "pending_child"

    app.register_many_async([root_task, pending_child])

    root = app.create_task(root_task)
    child = app.create_task(pending_child)

    graph = TaskGraph()
    graph.add_task(root)
    graph.add_task(child, parent_ids=[root.task_id])
    list(graph.generate_pending_results())

    # Root succeeded — child is Pending and will be scheduled by the ready-task loop.
    graph.add_result(TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph

    completed = TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success)
    await evaluator_context.evaluator.continue_graph(completed)

    published = evaluator_context.get_scheduled_messages()
    published_ids = [msg.task.task_id for msg in published]

    # child should appear exactly once (from the ready-task loop).
    child_publish_count = published_ids.count(child.task_id)
    assert child_publish_count == 1, (
        f"Pending->Scheduled task must be published exactly once, got {child_publish_count}."
    )


async def test_validation_error_in_load_graph_raises_continue_graph_error(evaluator_context, mock_storage):
    """ValidationError raised by load_graph must be wrapped as BoilermakerStorageError,
    enter the retry loop, exhaust retries, and surface as ContinueGraphError — not as a raw
    ValidationError that bypasses message_handler's except clause.

    This guards against the regression introduced when `except Exception` was narrowed to
    `except BoilermakerStorageError` in continue_graph: pydantic.ValidationError (raised when
    a corrupt blob fails model_validate_json) is not a BoilermakerStorageError, so it would
    escape the retry loop.  The fix is to wrap it inside load_graph before raising.
    """
    from unittest.mock import patch

    from boilermaker.evaluators.task_graph import _LOAD_GRAPH_RETRY_POLICY

    # Simulate corrupt blob contents that fail Pydantic validation.
    # After the fix, load_graph wraps ValidationError → BoilermakerStorageError, so the
    # retry loop catches it and eventually raises ContinueGraphError.
    mock_storage.load_graph.side_effect = exc.BoilermakerStorageError("Failed to deserialize graph: validation error")

    with patch("asyncio.sleep"):
        result = await evaluator_context()

    # Task itself succeeded
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # load_graph must have been retried the full configured number of times
    assert mock_storage.load_graph.call_count == _LOAD_GRAPH_RETRY_POLICY.max_tries, (
        f"Expected {_LOAD_GRAPH_RETRY_POLICY.max_tries} load_graph attempts, got {mock_storage.load_graph.call_count}"
    )

    # Message must NOT be settled — allow Service Bus redelivery
    assert len(evaluator_context.mockservicebus._receiver.method_calls) == 0, (
        "Message must not be settled when load_graph raises (wrapping ValidationError)"
    )

    # No downstream tasks must be published
    assert evaluator_context.get_scheduled_messages() == [], "task_publisher must not be called when load_graph raises"


async def test_pending_task_rediscovered_on_redelivery(evaluator_context, app):
    """With publish-before-store, if the Scheduled blob write fails after publish,
    the task remains Pending.  On redelivery generate_ready_tasks() re-discovers it;
    SB dedup suppresses the duplicate publish.
    """

    async def downstream(state):
        return "downstream done"

    app.register_async(downstream)
    downstream_task = app.create_task(downstream)

    graph = evaluator_context.graph
    graph.add_task(downstream_task, parent_ids=[evaluator_context.ok_task.task_id])

    # Generate pending results so results map is populated
    list(graph.generate_pending_results())

    # ok_task succeeded (trigger for downstream) — downstream is still Pending
    # (simulates crash after publish but before blob write on a prior invocation)
    graph.add_result(
        TaskResult(
            task_id=evaluator_context.ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
        )
    )

    evaluator_context.mock_storage.load_graph.return_value = graph

    completed = TaskResult(
        task_id=evaluator_context.ok_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )
    await evaluator_context.evaluator.continue_graph(completed)

    published_ids = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert downstream_task.task_id in published_ids, (
        "Pending downstream task must be published on redelivery (generate_ready_tasks re-discovers it)"
    )


async def test_blob_write_after_publish(evaluator_context, app):
    """With publish-before-store, a successful publish is followed by a blob write
    to mark the task as Scheduled.  Verify that store_task_result IS called for newly-ready tasks."""

    async def downstream(state):
        return "done"

    app.register_async(downstream)
    downstream_task = app.create_task(downstream)

    graph = evaluator_context.graph
    graph.add_task(downstream_task, parent_ids=[evaluator_context.ok_task.task_id])
    list(graph.generate_pending_results())

    # ok_task succeeded — downstream is Pending and ready
    graph.add_result(
        TaskResult(
            task_id=evaluator_context.ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
        )
    )

    evaluator_context.mock_storage.load_graph.return_value = graph

    # Reset store_task_result call count so we only see calls from this continue_graph invocation
    evaluator_context.mock_storage.store_task_result.reset_mock()

    completed = TaskResult(
        task_id=evaluator_context.ok_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )
    await evaluator_context.evaluator.continue_graph(completed)

    # store_task_result SHOULD be called for the downstream task (writing Scheduled status after publish)
    stored_ids = {
        call.args[0].task_id for call in evaluator_context.mock_storage.store_task_result.call_args_list if call.args
    }
    assert downstream_task.task_id in stored_ids, (
        "store_task_result must be called after publish to write Scheduled status"
    )


async def test_is_complete_false_while_scheduled(evaluator_context, app):
    """graph.is_complete() must return False when a task is in Scheduled status."""

    async def downstream(state):
        return "done"

    app.register_async(downstream)
    downstream_task = app.create_task(downstream)

    graph = evaluator_context.graph
    graph.add_task(downstream_task, parent_ids=[evaluator_context.ok_task.task_id])
    list(graph.generate_pending_results())

    # Set downstream to Scheduled (not a terminal state)
    graph.results[downstream_task.task_id].status = TaskStatus.Scheduled
    graph.add_result(
        TaskResult(
            task_id=evaluator_context.ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
        )
    )

    assert not graph.is_complete(), (
        "graph.is_complete() must return False when a task is in Scheduled status (Scheduled is not a terminal state)"
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Idempotent redelivery guard
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_redelivery_guard_skips_execution_and_calls_continue_graph(evaluator_context, mock_storage):
    """When load_task_result returns a terminal result, message_handler must skip
    re-execution (eval_task not called) and still call continue_graph to dispatch
    any downstream tasks that may not have been published due to the original crash.

    This is the core safety property of the idempotent redelivery guard.
    """
    from unittest.mock import patch

    from boilermaker.task import GraphId

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    # Arrange: the task already completed successfully in a prior invocation.
    stored_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Success,
    )
    mock_storage.load_task_result.return_value = stored_slim

    # The loaded graph must reflect the terminal status so the status-mismatch check passes
    # when continue_graph is called on the redelivery path.
    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    # eval_task must NOT have been called — execution was skipped.
    mock_eval_task.assert_not_called()

    # continue_graph WAS called — downstream dispatch must still happen.
    mock_storage.load_graph.assert_called_once()

    # The returned result reflects the already-terminal status, not a fresh execution.
    assert result is not None
    assert result.status == TaskStatus.Success

    # store_task_result must NOT have been called for the redelivering task itself
    # (no Started or re-executed result blob was written). It may be called by
    # continue_graph to write Scheduled status for newly-ready downstream tasks.
    stored_statuses = [call.args[0].status for call in mock_storage.store_task_result.call_args_list if call.args]
    assert TaskStatus.Started not in stored_statuses, (
        "Started must not be stored on redelivery — that would overwrite the terminal result"
    )
    stored_task_ids = {call.args[0].task_id for call in mock_storage.store_task_result.call_args_list if call.args}
    assert ok_task.task_id not in stored_task_ids, (
        "store_task_result must not be called for the redelivering task (ok_task) itself"
    )


async def test_failure_callback_publish_before_store(evaluator_context, app):
    """With publish-before-store, a failure callback still in Pending status
    (blob write failed on prior invocation) is re-discovered by generate_failure_ready_tasks()
    and re-published on redelivery.  SB dedup suppresses the duplicate.
    """

    async def fail_cb(state):
        return "failure handled"

    app.register_async(fail_cb)
    fail_task = app.create_task(fail_cb)

    graph = evaluator_context.graph
    graph.add_failure_callback(evaluator_context.ok_task.task_id, fail_task)
    list(graph.generate_pending_results())

    # ok_task has failed (trigger for the failure callback)
    # fail_task remains Pending (simulates crash after publish but before blob write)
    graph.add_result(
        TaskResult(
            task_id=evaluator_context.ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Failure,
        )
    )

    evaluator_context.mock_storage.load_graph.return_value = graph

    completed = TaskResult(
        task_id=evaluator_context.ok_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Failure,
    )
    await evaluator_context.evaluator.continue_graph(completed)

    published_ids = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert fail_task.task_id in published_ids, (
        "Pending failure callback must be published on redelivery "
        "(generate_failure_ready_tasks re-discovers it)"
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Per-task publish_task exception isolation
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_publish_task_exception_on_second_task_does_not_abort_loop(app, evaluator_context):
    """A publish_task failure for one ready task must not abort publishing of remaining tasks.

    Two ready tasks are set up. publish_task raises on the second call.
    Assert: first task is published, second task is also attempted, continue_graph does not raise,
    and the failed task remains in Pending status (publish-before-store: blob write skipped on failure).
    """

    async def task_b(state):
        return "B"

    async def task_c(state):
        return "C"

    app.register_many_async([task_b, task_c])
    t_b = app.create_task(task_b)
    t_c = app.create_task(task_c)

    # Add two more parallel successors to ok_task alongside the existing ones
    # so we have at least three ready tasks when ok_task succeeds.
    # Use a fresh graph with just three parallel tasks to keep the test simple.
    fresh_graph = TaskGraph()
    root_task = evaluator_context.ok_task
    fresh_graph.add_task(root_task)
    fresh_graph.add_task(t_b, parent_ids=[root_task.task_id])
    fresh_graph.add_task(t_c, parent_ids=[root_task.task_id])
    list(fresh_graph.generate_pending_results())

    # ok_task already completed successfully
    fresh_graph.add_result(
        TaskResult(
            task_id=root_task.task_id,
            graph_id=fresh_graph.graph_id,
            status=TaskStatus.Success,
        )
    )
    evaluator_context.mock_storage.load_graph.return_value = fresh_graph

    publish_call_count = 0
    published_task_ids: list[str] = []
    attempted_task_ids: list[str] = []

    async def publish_task_with_second_failure(task, *args, **kwargs):
        nonlocal publish_call_count
        publish_call_count += 1
        attempted_task_ids.append(task.task_id)
        if publish_call_count == 2:
            raise Exception("Simulated transient SB publish failure on second task")
        published_task_ids.append(task.task_id)

    evaluator_context.evaluator.task_publisher = publish_task_with_second_failure

    completed = TaskResult(
        task_id=root_task.task_id,
        graph_id=fresh_graph.graph_id,
        status=TaskStatus.Success,
    )

    # continue_graph must not raise even though publish_task raises on the second call
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    # Both t_b and t_c were attempted (3 attempts including the one that failed)
    assert len(attempted_task_ids) == 2, f"Expected both ready tasks to be attempted, got {attempted_task_ids}"

    # Only one task was successfully published (the one that didn't raise)
    assert len(published_task_ids) == 1, f"Expected exactly one successful publish, got {published_task_ids}"

    # ready_count only counts successful publishes
    assert ready_count == 1, f"Expected ready_count=1 (only successful publish), got {ready_count}"

    # The failed task remains in Pending status (publish-before-store: blob write is skipped on publish failure)
    failed_task_id = next(tid for tid in attempted_task_ids if tid not in published_task_ids)
    assert fresh_graph.results[failed_task_id].status == TaskStatus.Pending, (
        "Failed-publish task must remain in Pending status (publish failed before blob write)"
    )


async def test_continue_graph_does_not_raise_when_publish_fails(evaluator_context):
    """continue_graph must not raise when publish_task raises; message_handler can proceed to settle."""
    graph = evaluator_context.graph
    # ok_task completed successfully
    graph.add_result(
        TaskResult(
            task_id=evaluator_context.ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
        )
    )
    evaluator_context.mock_storage.load_graph.return_value = graph

    async def always_failing_publisher(task, *args, **kwargs):
        raise Exception("Simulated persistent SB publish failure")

    evaluator_context.evaluator.task_publisher = always_failing_publisher

    completed = TaskResult(
        task_id=evaluator_context.ok_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    # Must not raise — exception isolation means the loop continues and returns normally
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    # All publishes failed, so ready_count is 0
    assert ready_count == 0


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# acks_early=True raises ValueError in TaskGraphEvaluator.__init__
# # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_acks_early_true_raises_at_construction(app, mockservicebus, mock_storage, make_message):
    """Constructing TaskGraphEvaluator with acks_early=True must raise ValueError.

    acks_early is a computed property (not acks_late), so setting acks_late=False triggers the error.
    """

    async def acks_early_task_fn(state):
        return "ok"

    app.register_async(acks_early_task_fn)
    early_task = Task.si(acks_early_task_fn, acks_late=False)
    early_task.msg = make_message(early_task)

    assert early_task.acks_early is True, "Precondition: task must have acks_early=True"

    async def noop_publisher(task, *args, **kwargs):
        pass

    with pytest.raises(ValueError, match="acks_early=True"):
        TaskGraphEvaluator(
            mockservicebus._receiver,
            early_task,
            noop_publisher,
            app.function_registry,
            state=app.state,
            storage_interface=mock_storage,
        )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Status mismatch and fail-open error handling in continue_graph
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_status_mismatch_raises_continue_graph_error(evaluator_context, mock_storage):
    """When the loaded graph reports a different status than the completed TaskResult,
    continue_graph must raise ContinueGraphError and must not publish any tasks.

    This guards against the 'stale graph blob' race: a redelivered message writes a new
    terminal result but then loads the pre-write graph blob where the status still shows
    the old value.  Raising rather than silently continuing allows Service Bus to redeliver
    and retry once the blob is consistent.
    """
    from unittest.mock import MagicMock

    from boilermaker.task import GraphId, TaskGraph

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task

    # Build a mock graph whose get_status returns a status that does NOT match the
    # completed_task_result.status we will pass to continue_graph.
    mock_graph = MagicMock(spec=TaskGraph)
    mock_graph.graph_id = graph.graph_id
    mock_graph.results = {}
    # The completing task finished with Success, but the loaded blob reports Pending —
    # a mismatch that should trigger the status guard.
    mock_graph.get_status.return_value = TaskStatus.Pending
    mock_graph.generate_ready_tasks.return_value = iter([])
    mock_graph.generate_failure_ready_tasks.return_value = iter([])

    mock_storage.load_graph.return_value = mock_graph

    completed = TaskResult(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Success,
    )

    with pytest.raises(exc.ContinueGraphError):
        await evaluator_context.evaluator.continue_graph(completed)

    # No tasks must have been published — the mismatch guard fires before scheduling.
    assert evaluator_context.get_scheduled_messages() == [], (
        "publish_task must not be called when continue_graph raises on status mismatch"
    )


async def test_fail_open_on_load_task_result_storage_error(evaluator_context, mock_storage):
    """When load_task_result raises BoilermakerStorageError, message_handler must
    proceed with normal execution (fail-open) rather than skipping the task.

    Fail-closed on a transient read error would permanently stall the graph: the task
    would never run because every redelivery returns an error, so no result is ever
    stored, so continue_graph never fires, so downstream tasks are never dispatched.

    This test guards against someone accidentally changing fail-open to fail-closed.
    """
    from unittest.mock import patch

    # Arrange: load_task_result raises a transient storage error on every call.
    mock_storage.load_task_result.side_effect = exc.BoilermakerStorageError("503 transient error")

    # The graph must have ok_task's result pre-seeded so the status-mismatch check in
    # continue_graph passes — the loaded graph status must match what eval_task produces.
    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph
    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        # eval_task must be awaitable and return a plausible TaskResult so message_handler
        # can continue past execution into the settlement/continue_graph path.

        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        mock_eval_task.side_effect = None

        result = await evaluator_context.evaluator()

    # eval_task MUST have been called — fail-open means we proceed despite the read error.
    (
        mock_eval_task.assert_called_once(),
        (
            "eval_task must be called when load_task_result raises BoilermakerStorageError "
            "(fail-open: do not stall the graph on transient read errors)"
        ),
    )

    # Execution should have produced a result, not silently skipped.
    assert result is not None, "message_handler must return a result on the fail-open path"


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Fix F0: Retry publish uses differentiated message_id
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_retry_publish_uses_differentiated_message_id(retry_scenario):
    """Retry publishes must use message_id '{task_id}:{attempt_number}' so that
    Azure Service Bus duplicate detection does not silently drop retry messages.

    The original scheduling publish uses bare task_id as message_id. A retry
    for the same task_id would be deduped without a differentiated ID.

    record_attempt() is called before the retry branch, so
    task.attempts.attempts is already incremented (first retry = 1).
    """
    async with retry_scenario.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Retry,
        check_graph_loaded=False,
    ) as ctx:
        ctx.assert_messages_scheduled(1)
        scheduled = ctx.get_scheduled_messages()
        retry_msg = scheduled[0]
        task = retry_msg.task
        kwargs = retry_msg.kwargs

        # The retry publish must pass unique_msg_id with the attempt-differentiated format
        expected_msg_id = f"{task.task_id}:{task.attempts.attempts}"
        actual_msg_id = kwargs.get("unique_msg_id")
        assert actual_msg_id == expected_msg_id, (
            f"Retry publish must use differentiated message_id. "
            f"Expected '{expected_msg_id}', got '{actual_msg_id}'"
        )


async def test_continue_graph_publish_does_not_override_message_id(success_scenario):
    """continue_graph publishes must NOT pass unique_msg_id, preserving fan-in
    dedup where two workers racing to schedule the same ready task should
    produce identical message_ids (bare task_id)."""
    async with success_scenario.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()
        for msg in scheduled:
            unique_msg_id = msg.kwargs.get("unique_msg_id")
            assert unique_msg_id is None, (
                f"continue_graph publish for task {msg.task.function_name} must not "
                f"override unique_msg_id, but got '{unique_msg_id}'"
            )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# F3/F4: ETag-based CAS on Started write
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_started_write_passes_etag_from_initial_read(evaluator_context, mock_storage):
    """When load_task_result returns a Pending blob with an ETag, the Started write
    must be called with that ETag so concurrent workers cannot both win the CAS.

    This is the core safety property of F3: the Started write is conditional on the
    blob not having been modified since our read.
    """
    from unittest.mock import patch

    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    observed_etag = "W/\"abc123\""

    # Arrange: the task is Pending with a known ETag.
    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag=observed_etag,
    )
    mock_storage.load_task_result.return_value = pending_slim

    # The loaded graph must reflect a Success result so the status-mismatch check passes.
    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        await evaluator_context.evaluator()

    # The first store_task_result call is the Started write — it must carry the ETag.
    started_call = mock_storage.store_task_result.call_args_list[0]
    actual_etag = started_call.kwargs.get("etag") or (
        started_call.args[1] if len(started_call.args) > 1 else None
    )
    assert actual_etag == observed_etag, (
        f"Started write must pass etag={observed_etag!r} from the initial read, got {actual_etag!r}"
    )


async def test_412_on_started_write_with_terminal_reread_skips_execution(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read shows a terminal status,
    message_handler must skip execution and complete the message.

    This covers the race where worker W1 wins the CAS and writes Success before W2's
    Started write arrives.  W2 gets 412, re-reads Success, and gracefully yields.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    # Initial read returns Pending — task has not started yet.
    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    # After the 412, the re-read returns Success (another worker completed the task).
    success_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Success,
    )
    mock_storage.load_task_result.side_effect = [pending_slim, success_slim]

    # The Started write raises 412.
    mock_storage.store_task_result.side_effect = BoilermakerStorageError(
        "412 Precondition Failed", status_code=412
    )

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    # eval_task must NOT have been called — another worker already finished.
    mock_eval_task.assert_not_called()

    # The returned result must reflect the terminal status from the re-read.
    assert result is not None
    assert result.status == TaskStatus.Success, (
        f"Expected Success (from re-read), got {result.status}"
    )

    # Message must have been completed (not left unsettled).
    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called after 412 + terminal re-read"
    )


async def test_412_on_started_write_with_started_reread_yields_to_other_worker(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read shows Started (another worker
    is executing), message_handler must complete the message and return without executing.

    This covers the race where worker W1 wins the CAS (writes Started) and W2's Started
    write arrives late.  W2 gets 412, re-reads Started, completes its message, and yields.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    # Initial read returns Pending.
    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    # After the 412, the re-read returns Started (another worker won the CAS and is running).
    started_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Started,
    )
    mock_storage.load_task_result.side_effect = [pending_slim, started_slim]

    # The Started write raises 412.
    mock_storage.store_task_result.side_effect = BoilermakerStorageError(
        "412 Precondition Failed", status_code=412
    )

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    # eval_task must NOT have been called — we yielded to the other worker.
    mock_eval_task.assert_not_called()

    # The returned result carries Started status (we deferred to the other worker).
    assert result is not None
    assert result.status == TaskStatus.Started, (
        f"Expected Started (yielded to other worker), got {result.status}"
    )

    # Message must have been completed so it is not redelivered.
    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called after 412 + Started re-read (yield path)"
    )


@pytest.mark.asyncio
async def test_412_on_started_write_with_none_reread_returns_failure(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the subsequent re-read returns None (blob
    vanished), message_handler must return a Failure result and must NOT silently complete
    the message as if another worker is handling the task.

    Returning Failure surfaces the anomaly so the graph is not permanently stalled.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    # Initial read returns Pending with an ETag so the CAS write is conditional.
    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    # After the 412 the re-read returns None — the blob has vanished unexpectedly.
    mock_storage.load_task_result.side_effect = [pending_slim, None]

    # The Started write raises 412.
    mock_storage.store_task_result.side_effect = BoilermakerStorageError(
        "412 Precondition Failed", status_code=412
    )

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    # eval_task must NOT have been called — the 412 path exits before execution.
    mock_eval_task.assert_not_called()

    # The returned result must be Failure, not Started (which would falsely imply
    # another worker is handling the task).
    assert result is not None
    assert result.status == TaskStatus.Failure, (
        f"Expected Failure when re-read returns None after 412, got {result.status}"
    )

    # The message must NOT have been completed — suppressing settlement allows
    # redelivery so the task is not permanently dropped.
    assert not evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must NOT be called when re-read returns None after 412"
    )


async def test_412_scheduled_reread_retries_started_write_with_new_etag(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read shows Scheduled, the retry
    must use the etag from the Scheduled blob (not the original Pending etag, not None).

    Spec action: WriteStarted412 (Scheduled branch) → RetryStartedAfterScheduled.
    The worker captures _reread.etag from the Scheduled blob and passes it to the
    retry store_task_result call.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim]

    # First store raises 412; retry succeeds; result write also succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        None,
        None,
    ]

    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        await evaluator_context.evaluator()

    assert mock_storage.store_task_result.call_count >= 2, (
        "store_task_result must be called at least twice (Started write + retry)"
    )
    retry_call = mock_storage.store_task_result.call_args_list[1]
    actual_etag = retry_call.kwargs.get("etag") or (
        retry_call.args[1] if len(retry_call.args) > 1 else None
    )
    assert actual_etag == "W/\"etag-v2\"", (
        f"Retry Started write must use the Scheduled blob's etag 'W/\"etag-v2\"', got {actual_etag!r}"
    )


async def test_412_scheduled_retry_success_falls_through_to_execution(evaluator_context, mock_storage):
    """When the retry Started write succeeds after a Scheduled re-read, the task executes
    normally: eval_task is called, the result is stored, and complete_message is called.

    Spec action: RetryStartedWriteSuccess → Executing (same path as WriteStartedSuccess).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim]

    # First store raises 412; retry succeeds; result write also succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        None,
        None,
    ]

    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_called_once()

    assert mock_storage.store_task_result.call_count >= 2, (
        "store_task_result must be called at least twice (Started retry + result write)"
    )

    assert result is not None
    assert result.status == TaskStatus.Success, (
        f"Expected Success after retry success + execution, got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called via the normal execution path after retry success"
    )


async def test_412_scheduled_retry_second_412_sees_started_yields_with_settle(evaluator_context, mock_storage):
    """When the retry Started write gets a second 412 and the second re-read shows Started,
    the handler calls complete_message and returns TaskResult(Started) without executing.

    Spec action: RereadAfterRetry (Started branch) → Completing.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )
    started_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Started,
    )
    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim, started_slim]

    # Both store attempts raise 412.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()

    assert mock_storage.store_task_result.call_count == 2, (
        "store_task_result must be called exactly twice (Started write + retry, no result write)"
    )

    assert result is not None
    assert result.status == TaskStatus.Started, (
        f"Expected Started (yielded to other worker after second 412), got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called exactly once in the second-412 path"
    )


async def test_412_scheduled_retry_second_412_sees_terminal_settles(evaluator_context, mock_storage):
    """When the retry Started write gets a second 412 and the second re-read shows a
    terminal status, the handler calls complete_message and returns the terminal result.

    Spec action: RereadAfterRetry (terminal branch) → Completing.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )
    success_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Success,
    )
    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim, success_slim]

    # Both store attempts raise 412.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()

    assert result is not None
    assert result.status == TaskStatus.Success, (
        f"Expected Success (terminal from second re-read), got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called once after second 412 + terminal re-read"
    )


@pytest.mark.parametrize("second_reread_value", [None, "scheduled", "pending"])
async def test_412_scheduled_retry_second_412_sees_unclaimed_does_not_settle(
    evaluator_context, mock_storage, second_reread_value
):
    """When the retry Started write gets a second 412 and the second re-read returns an
    unclaimed status (None, Scheduled, Pending), the handler does NOT call complete_message
    — it lets the SB lock expire for redelivery instead of permanently stalling the graph.

    Spec action: RereadAfterRetry412 (Pending/Scheduled) → RereadAfterRetryNoSettle.
    Parameterized over: None, Scheduled, Pending second re-reads.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )

    if second_reread_value is None:
        third_load_result = None
    elif second_reread_value == "scheduled":
        third_load_result = TaskResultSlim(
            task_id=ok_task.task_id,
            graph_id=GraphId(graph.graph_id),
            status=TaskStatus.Scheduled,
        )
    else:
        third_load_result = TaskResultSlim(
            task_id=ok_task.task_id,
            graph_id=GraphId(graph.graph_id),
            status=TaskStatus.Pending,
        )

    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim, third_load_result]

    # Both store attempts raise 412.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()

    assert result is not None
    assert result.status == TaskStatus.Failure, (
        f"Expected Failure for unclaimed second re-read value {second_reread_value!r}, got {result.status}"
    )

    assert not evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must NOT be called for unclaimed second re-read status — "
        "let SB redeliver rather than permanently stalling the graph"
    )


async def test_412_scheduled_retry_non412_error_proceeds_to_execution(evaluator_context, mock_storage):
    """When the retry Started write raises a non-412 BoilermakerStorageError, the handler
    fails open and proceeds to execution (eval_task is called).

    Spec action: RetryStartedWriteNon412Error → Executing (fail-open).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    scheduled_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Scheduled,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, scheduled_slim]

    # First store raises 412; retry raises a non-412 storage error (fail-open);
    # result write succeeds after execution proceeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        BoilermakerStorageError("503 Service Unavailable", status_code=503),
        None,
    ]

    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_called_once()

    assert result is not None
    assert result.status == TaskStatus.Success, (
        f"Expected Success after fail-open retry + execution, got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called via normal execution path after fail-open retry"
    )


async def test_412_pending_reread_retries_started_write_with_new_etag(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read shows Pending, the retry must use
    the etag from the re-read (not the original etag, not None).

    This models the production race: the scheduler's BlobLeaseClient.acquire() bumped the
    blob ETag without changing the status, so the re-read returns Pending with a new etag.
    The handler must treat this identically to the Scheduled case and retry the CAS.

    Spec action: WriteStarted412 (Pending branch) → RetryStartedAfterScheduled.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    # Re-read after 412 returns Pending with a NEW etag — lease acquire bumped it.
    pending_slim_reread = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, pending_slim_reread]

    # First store raises 412; retry succeeds; result write also succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        None,
        None,
    ]

    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        await evaluator_context.evaluator()

    assert mock_storage.store_task_result.call_count >= 2, (
        "store_task_result must be called at least twice (Started write + retry)"
    )
    retry_call = mock_storage.store_task_result.call_args_list[1]
    actual_etag = retry_call.kwargs.get("etag") or (
        retry_call.args[1] if len(retry_call.args) > 1 else None
    )
    assert actual_etag == "W/\"etag-v2\"", (
        f"Retry Started write must use the re-read Pending blob etag 'W/\"etag-v2\"', got {actual_etag!r}"
    )


async def test_412_pending_retry_success_falls_through_to_execution(evaluator_context, mock_storage):
    """When the retry Started write succeeds after a Pending re-read, the task executes
    normally: eval_task is called, the result is stored, and complete_message is called.

    This is the expected recovery path when the scheduler's lease acquire bumped the ETag
    but the worker can still claim the task by retrying with the new etag.

    Spec action: RetryStartedWriteSuccess → Executing (same path as WriteStartedSuccess).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    pending_slim_reread = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, pending_slim_reread]

    # First store raises 412; retry succeeds; result write also succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412 Precondition Failed", status_code=412),
        None,
        None,
    ]

    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_called_once()

    assert mock_storage.store_task_result.call_count >= 2, (
        "store_task_result must be called at least twice (Started retry + result write)"
    )

    assert result is not None
    assert result.status == TaskStatus.Success, (
        f"Expected Success after Pending retry success + execution, got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called via the normal execution path after retry success"
    )


async def test_412_retry_reread_settles_and_returns_retry(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read returns Retry, the handler
    settles the duplicate message and returns TaskResult(Retry) — another worker already
    ran the task and published a delayed retry SB message.

    Spec action: WriteStarted412 (Retry re-read) → Completing.
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    graph = evaluator_context.graph
    ok_task = evaluator_context.ok_task
    ok_task.graph_id = graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Pending,
        etag="W/\"etag-v1\"",
    )
    retry_slim_reread = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(graph.graph_id),
        status=TaskStatus.Retry,
        etag="W/\"etag-v2\"",
    )
    mock_storage.load_task_result.side_effect = [pending_slim, retry_slim_reread]

    mock_storage.store_task_result.side_effect = BoilermakerStorageError(
        "412 Precondition Failed", status_code=412
    )

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()

    assert result is not None
    assert result.status == TaskStatus.Retry, (
        f"Expected Retry for Retry re-read after 412, got {result.status}"
    )

    assert evaluator_context.mockservicebus._receiver.complete_message.called, (
        "complete_message must be called — another worker already owns the retry"
    )


# ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ #
# Coverage gap tests                                                           #
# ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ ~~~~ #

# ----- Redelivery path (lines 106-116) -----


async def test_redelivery_continue_graph_fails_suppresses_settlement(evaluator_context, mock_storage):
    """On SB redelivery with a terminal blob, if continue_graph raises ContinueGraphError
    the evaluator must return the terminal result WITHOUT settling (lines 106-112).
    Suppressing settlement allows SB to redeliver and downstream dispatch to be retried.
    """
    from unittest.mock import patch

    from boilermaker.exc import ContinueGraphError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.return_value = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(ok_task.graph_id),
        status=TaskStatus.Success,
    )

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch.object(evaluator_context.evaluator, "continue_graph", side_effect=ContinueGraphError("load failed")):
        result = await evaluator_context.evaluator()

    assert result.status == TaskStatus.Success
    assert not evaluator_context.mockservicebus._receiver.complete_message.called, (
        "must not settle when continue_graph fails on redelivery"
    )


async def test_redelivery_complete_message_fails_still_returns_terminal(evaluator_context, mock_storage):
    """On SB redelivery with a terminal blob, if complete_message raises the evaluator
    must still return the terminal result (lines 115-116).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.return_value = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(ok_task.graph_id),
        status=TaskStatus.Success,
    )

    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch.object(evaluator_context.evaluator, "continue_graph", return_value=None):
        result = await evaluator_context.evaluator()

    assert result.status == TaskStatus.Success


# ----- Non-412 error on Started write (line 138) -----


async def test_non_412_started_write_error_falls_through_to_execution(evaluator_context, mock_storage):
    """A non-412 storage error on the Started write must be logged and execution must
    proceed (fail-open, line 138).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.return_value = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(ok_task.graph_id),
        status=TaskStatus.Pending,
        etag='W/"e1"',
    )
    # First store (Started) raises a 500-style error; second store (result) succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("500 Internal Server Error", status_code=500),
        None,
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_called_once()
    assert result.status == TaskStatus.Success


# ----- 412 + re-read also fails (lines 152-158) -----


async def test_412_started_write_reread_also_fails_returns_failure(evaluator_context, mock_storage):
    """When the Started write gets a 412 and the re-read also raises a storage error,
    the evaluator must return Failure without settling (lines 152-158).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    pending_slim = TaskResultSlim(
        task_id=ok_task.task_id,
        graph_id=GraphId(ok_task.graph_id),
        status=TaskStatus.Pending,
        etag='W/"e1"',
    )
    # First call: idempotency read → Pending. Second call: re-read after 412 → raises.
    mock_storage.load_task_result.side_effect = [
        pending_slim,
        BoilermakerStorageError("503 Service Unavailable", status_code=503),
    ]
    mock_storage.store_task_result.side_effect = BoilermakerStorageError("412 Precondition Failed", status_code=412)

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Failure
    assert not evaluator_context.mockservicebus._receiver.complete_message.called


# ----- 412 complete_message failures (lines 189-190, 209-210) -----


async def test_412_terminal_reread_complete_message_failure_still_returns_terminal(evaluator_context, mock_storage):
    """After a 412 + terminal re-read, if complete_message raises the evaluator must
    still return the terminal status (lines 189-190).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Success),
    ]
    mock_storage.store_task_result.side_effect = BoilermakerStorageError("412", status_code=412)
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Success


async def test_412_started_reread_complete_message_failure_still_returns_started(evaluator_context, mock_storage):
    """After a 412 + Started re-read, if complete_message raises the evaluator must
    still return Started (lines 209-210).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Started),
    ]
    mock_storage.store_task_result.side_effect = BoilermakerStorageError("412", status_code=412)
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Started


# ----- 412 + Pending/Scheduled re-read with None etag warning (line 237) -----


async def test_412_reread_with_none_etag_logs_warning_and_retries(evaluator_context, mock_storage):
    """When the re-read after a 412 returns Scheduled with a None etag, the evaluator
    logs a warning and retries the Started write with etag=None (line 237).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        # Re-read has etag=None — triggers the warning log at line 237.
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag=None
        ),
    ]
    # First store: 412; retry store: succeeds; result store: succeeds.
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        None,
        None,
    ]
    evaluator_context.graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = evaluator_context.graph

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()
        assert result.status == TaskStatus.Success

    mock_eval_task.assert_called_once()
    # Verify that the retry write used None as the etag (unconditional).
    retry_call = mock_storage.store_task_result.call_args_list[1]
    assert retry_call.kwargs.get("etag") is None or retry_call.args[1] is None


# ----- Second 412 paths (lines 275-283, 290-291, 306-307, 324-336) -----


async def test_second_412_reread_also_fails_returns_failure_no_settle(evaluator_context, mock_storage):
    """When the second 412 + second re-read also raises a storage error, _reread2 is set
    to None and the evaluator returns Failure without settling (lines 275-283).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag='W/"e2"'
        ),
        BoilermakerStorageError("503", status_code=503),  # re-read2 fails
    ]
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        BoilermakerStorageError("412", status_code=412),
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Failure
    assert not evaluator_context.mockservicebus._receiver.complete_message.called


async def test_second_412_started_reread2_complete_message_failure(evaluator_context, mock_storage):
    """After second 412 + Started re-read2, if complete_message raises the evaluator
    still returns Started (lines 290-291).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag='W/"e2"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Started),
    ]
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        BoilermakerStorageError("412", status_code=412),
    ]
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Started


async def test_second_412_terminal_reread2_complete_message_failure(evaluator_context, mock_storage):
    """After second 412 + terminal re-read2, if complete_message raises the evaluator
    still returns the terminal status (lines 306-307).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag='W/"e2"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Success),
    ]
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        BoilermakerStorageError("412", status_code=412),
    ]
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Success


async def test_second_412_retry_reread2_complete_message_failure(evaluator_context, mock_storage):
    """After second 412 + Retry re-read2, if complete_message raises the evaluator still
    returns Retry (lines 330-331).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag='W/"e2"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Retry),
    ]
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        BoilermakerStorageError("412", status_code=412),
    ]
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Retry


async def test_second_412_retry_reread2_settles_and_returns_retry(evaluator_context, mock_storage):
    """After second 412 + Retry re-read2, the evaluator settles and returns Retry
    (lines 324-336).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Scheduled, etag='W/"e2"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Retry),
    ]
    mock_storage.store_task_result.side_effect = [
        BoilermakerStorageError("412", status_code=412),
        BoilermakerStorageError("412", status_code=412),
    ]

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Retry
    assert evaluator_context.mockservicebus._receiver.complete_message.called


# ----- First 412 Retry, complete_message fails (lines 372-373) -----


async def test_412_retry_reread_complete_message_failure_still_returns_retry(evaluator_context, mock_storage):
    """After first 412 + Retry re-read, if complete_message raises the evaluator still
    returns Retry (lines 372-373).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError, BoilermakerTaskLeaseLost
    from boilermaker.task import GraphId, TaskResultSlim

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    mock_storage.load_task_result.side_effect = [
        TaskResultSlim(
            task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Pending, etag='W/"e1"'
        ),
        TaskResultSlim(task_id=ok_task.task_id, graph_id=GraphId(ok_task.graph_id), status=TaskStatus.Retry),
    ]
    mock_storage.store_task_result.side_effect = BoilermakerStorageError("412", status_code=412)
    evaluator_context.mockservicebus._receiver.complete_message.side_effect = BoilermakerTaskLeaseLost("lost")

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        result = await evaluator_context.evaluator()

    mock_eval_task.assert_not_called()
    assert result.status == TaskStatus.Retry


# ----- continue_graph: graph blob not found (lines 614-619) -----


async def test_continue_graph_graph_not_found_returns_none(evaluator_context, mock_storage):
    """When load_graph returns None (graph blob missing), continue_graph logs critical and
    returns None — the main flow settles normally (lines 614-619).
    """
    from unittest.mock import patch

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    # Override load_graph to return None (graph missing).
    mock_storage.load_graph.return_value = None

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    assert result.status == TaskStatus.Success
    # Message still settled (continue_graph returned None, not raised).
    assert evaluator_context.mockservicebus._receiver.complete_message.called


# ----- continue_graph: try_acquire_lease unexpected error (lines 651-658) -----


async def test_continue_graph_lease_unexpected_error_skips_task(evaluator_context, mock_storage):
    """When try_acquire_lease raises an unexpected BoilermakerStorageError, the task is
    skipped and processing continues (lines 651-658).
    """
    from unittest.mock import patch

    from boilermaker.exc import BoilermakerStorageError

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    # Graph needs a ready task so the lease acquisition code is reached.
    graph = evaluator_context.graph
    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    # Unexpected error on lease acquisition.
    mock_storage.try_acquire_lease.side_effect = BoilermakerStorageError("unexpected", status_code=500)

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    # Evaluator completes normally; the ready task was skipped but no exception raised.
    assert result.status == TaskStatus.Success


async def test_continue_graph_lease_returns_none_skips_task(evaluator_context, mock_storage):
    """When try_acquire_lease returns None (lease already held or etag mismatch),
    the task is skipped silently (lines 660-664).
    """
    from unittest.mock import patch

    ok_task = evaluator_context.ok_task
    ok_task.graph_id = evaluator_context.graph.graph_id

    graph = evaluator_context.graph
    graph.add_result(
        TaskResult(
            task_id=ok_task.task_id,
            graph_id=graph.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
    )
    mock_storage.load_graph.return_value = graph

    # Returning None means lease not acquired — task is skipped.
    mock_storage.try_acquire_lease.return_value = None

    evaluator_context.evaluator.task = ok_task
    ok_task.msg = evaluator_context.make_message(ok_task)

    with patch("boilermaker.evaluators.task_graph.eval_task") as mock_eval_task:
        mock_eval_task.return_value = TaskResult(
            task_id=ok_task.task_id,
            graph_id=ok_task.graph_id,
            status=TaskStatus.Success,
            result="OK",
        )
        result = await evaluator_context.evaluator()

    assert result.status == TaskStatus.Success


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# all_failed_callback integration tests (BMO-57)
# # # # # # # # # # # # # # # # # # # # # # # # # # #


def _build_graph_with_all_failed_callback(app):
    """Helper: build a graph with one main task, one per-task failure callback,
    and one all_failed_callback. Pending results are pre-populated.

    Returns (graph, main_task, per_task_cb, all_failed_cb).
    """

    async def afc_main(state):
        return "main"

    async def afc_per_task_fail(state):
        return "per_task handled"

    async def afc_all_failed(state):
        return "all failed"

    app.register_many_async([afc_main, afc_per_task_fail, afc_all_failed])

    main_task = app.create_task(afc_main)
    per_task_cb = app.create_task(afc_per_task_fail)
    all_failed_cb = app.create_task(afc_all_failed)

    graph = TaskGraph()
    graph.add_task(main_task)
    graph.add_failure_callback(main_task.task_id, per_task_cb)
    graph.add_all_failed_callback(all_failed_cb)
    list(graph.generate_pending_results())

    return graph, main_task, per_task_cb, all_failed_cb


async def test_continue_graph_dispatches_all_failed_callback(evaluator_context, app):
    """After the last main task fails and all per-task failure callbacks finish,
    continue_graph() publishes the all_failed_callback and writes Scheduled."""
    graph, main_task, per_task_cb, all_failed_cb = _build_graph_with_all_failed_callback(app)

    # Main task failed; per-task callback done
    graph.add_result(TaskResult(task_id=main_task.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))
    graph.add_result(TaskResult(task_id=per_task_cb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph

    completed = TaskResult(
        task_id=per_task_cb.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    # The all_failed_callback must have been published
    published = evaluator_context.get_scheduled_messages()
    published_ids = {msg.task.task_id for msg in published}
    assert all_failed_cb.task_id in published_ids, (
        "all_failed_callback must be published after graph reaches terminal-failed state"
    )
    assert ready_count >= 1

    # Scheduled blob must have been written
    store_calls = evaluator_context.mock_storage.store_task_result.mock_calls
    stored_ids_scheduled = {
        c.args[0].task_id
        for c in store_calls
        if hasattr(c.args[0], "status") and c.args[0].status == TaskStatus.Scheduled
    }
    assert all_failed_cb.task_id in stored_ids_scheduled, (
        "all_failed_callback status must be written as Scheduled in blob storage"
    )


async def test_continue_graph_does_not_dispatch_callback_on_success(evaluator_context, app):
    """When all main tasks succeed, continue_graph() must NOT publish the all_failed_callback."""
    graph, main_task, per_task_cb, all_failed_cb = _build_graph_with_all_failed_callback(app)

    graph.add_result(TaskResult(task_id=main_task.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph

    completed = TaskResult(
        task_id=main_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    await evaluator_context.evaluator.continue_graph(completed)

    published_ids = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert all_failed_cb.task_id not in published_ids, (
        "all_failed_callback must NOT be published when graph completes successfully"
    )


async def test_continue_graph_callback_not_double_dispatched(evaluator_context, app):
    """Two concurrent continue_graph() calls (simulated by returning None from
    try_acquire_lease on the second attempt) must result in only one publish."""
    graph, main_task, per_task_cb, all_failed_cb = _build_graph_with_all_failed_callback(app)

    graph.add_result(TaskResult(task_id=main_task.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))
    graph.add_result(TaskResult(task_id=per_task_cb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph

    # First call acquires lease; second call finds it held (returns None).
    evaluator_context.mock_storage.try_acquire_lease.side_effect = itertools.chain(
        ["lease-id-1"],   # first worker acquires
        [None],           # second worker cannot acquire
    )

    completed = TaskResult(
        task_id=per_task_cb.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    await evaluator_context.evaluator.continue_graph(completed)
    first_publishes = list(evaluator_context.get_scheduled_messages())

    evaluator_context._published_messages.clear()

    await evaluator_context.evaluator.continue_graph(completed)
    second_publishes = list(evaluator_context.get_scheduled_messages())

    callback_published_first = sum(1 for m in first_publishes if m.task.task_id == all_failed_cb.task_id)
    callback_published_second = sum(1 for m in second_publishes if m.task.task_id == all_failed_cb.task_id)

    assert callback_published_first == 1, "First worker must dispatch exactly once"
    assert callback_published_second == 0, "Second worker must be blocked by lease and skip"


async def test_continue_graph_callback_dispatched_after_failure_callback_finishes(evaluator_context, app):
    """The all_failed_callback is dispatched only once the per-task failure callback
    completes — not prematurely while it is still Pending/Started."""
    graph, main_task, per_task_cb, all_failed_cb = _build_graph_with_all_failed_callback(app)

    # main_task failed; per-task callback still Pending (not done yet)
    graph.add_result(TaskResult(task_id=main_task.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))

    evaluator_context.graph = graph

    completed_main = TaskResult(
        task_id=main_task.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Failure,
    )
    await evaluator_context.evaluator.continue_graph(completed_main)

    published_after_main = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert all_failed_cb.task_id not in published_after_main, (
        "all_failed_callback must NOT be dispatched while per-task failure callback is still pending"
    )

    # per-task failure callback completes
    graph.add_result(TaskResult(task_id=per_task_cb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    completed_per_task = TaskResult(
        task_id=per_task_cb.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )
    evaluator_context._published_messages.clear()
    await evaluator_context.evaluator.continue_graph(completed_per_task)

    published_after_cb = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert all_failed_cb.task_id in published_after_cb, (
        "all_failed_callback must be dispatched once per-task failure callback finishes"
    )


async def test_continue_graph_callback_crash_recovery(evaluator_context, app):
    """Crash-recovery: the all_failed_callback stays Pending in blob storage after a prior
    crash (publish succeeded but blob write failed). On redelivery, a freshly loaded graph
    still shows the callback as Pending, so continue_graph() must re-publish it."""

    # Build a graph that is already in terminal-failed state with the callback still Pending.
    # This simulates the state stored in blob storage after a crash: the callback was published
    # to Service Bus but Scheduled was never written to the blob.

    async def afc_main_cr(state):
        return "main"

    async def afc_per_task_fail_cr(state):
        return "per_task handled"

    async def afc_all_failed_cr(state):
        return "all failed"

    app.register_many_async([afc_main_cr, afc_per_task_fail_cr, afc_all_failed_cr])

    main_task = app.create_task(afc_main_cr)
    per_task_cb = app.create_task(afc_per_task_fail_cr)
    all_failed_cb = app.create_task(afc_all_failed_cr)

    graph = TaskGraph()
    graph.add_task(main_task)
    graph.add_failure_callback(main_task.task_id, per_task_cb)
    graph.add_all_failed_callback(all_failed_cb)
    list(graph.generate_pending_results())

    # Graph is terminal-failed: main task failed, per-task callback done.
    # all_failed_cb is still Pending — simulates prior crash after SB publish but before blob write.
    graph.add_result(TaskResult(task_id=main_task.task_id, graph_id=graph.graph_id, status=TaskStatus.Failure))
    graph.add_result(TaskResult(task_id=per_task_cb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph

    completed = TaskResult(
        task_id=per_task_cb.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    # On redelivery the graph is loaded from storage with callback still Pending.
    # continue_graph() should re-publish it (SB dedup suppresses genuine duplicate delivery).
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    published_ids = {msg.task.task_id for msg in evaluator_context.get_scheduled_messages()}
    assert all_failed_cb.task_id in published_ids, (
        "Callback must be re-published on redelivery when it remained Pending (crash recovery)"
    )
    assert ready_count >= 1
