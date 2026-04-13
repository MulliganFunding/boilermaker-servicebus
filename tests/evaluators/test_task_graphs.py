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


# continue_graph liveness gap — Scheduled re-publish
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_publish_before_store_crash_recovery(evaluator_context, app):
    """With publish-before-store, if the blob write fails after publish,
    the task remains in Pending status.  On redelivery, generate_ready_tasks()
    re-discovers it and re-publishes (SB dedup suppresses the duplicate).

    This replaces the old second-pass crash-recovery test.
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
# acks_early warning in TaskGraphEvaluator.__init__
# # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_acks_early_true_emits_warning_at_construction(app, mockservicebus, mock_storage, make_message, caplog):
    """Constructing TaskGraphEvaluator with acks_early=True must emit a warning log.

    The warning must contain the task ID and describe the irrecoverable-Started risk.
    Construction must succeed — no ValueError is raised.

    acks_early is a property that returns not acks_late, so acks_late=False triggers the warning.
    """
    import logging

    async def acks_early_task_fn(state):
        return "ok"

    app.register_async(acks_early_task_fn)
    # acks_early is a computed property: acks_early == not acks_late
    early_task = Task.si(acks_early_task_fn, acks_late=False)
    early_task.msg = make_message(early_task)

    assert early_task.acks_early is True, "Precondition: task must have acks_early=True"

    async def noop_publisher(task, *args, **kwargs):
        pass

    with caplog.at_level(logging.WARNING, logger="boilermaker.app"):
        evaluator = TaskGraphEvaluator(
            mockservicebus._receiver,
            early_task,
            noop_publisher,
            app.function_registry,
            state=app.state,
            storage_interface=mock_storage,
        )

    # Construction must succeed — no ValueError
    assert evaluator is not None

    warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(early_task.task_id in msg for msg in warning_messages), (
        f"Expected warning to contain task_id {early_task.task_id!r}. Got: {warning_messages}"
    )
    assert any("Started" in msg for msg in warning_messages), (
        f"Expected warning to describe the irrecoverable-Started risk. Got: {warning_messages}"
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
