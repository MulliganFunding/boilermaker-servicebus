import itertools
from unittest.mock import AsyncMock

import pytest
from boilermaker import exc, retries
from boilermaker.evaluators import TaskGraphEvaluator
from boilermaker.task import Task, TaskResult, TaskResultSlim, TaskStatus


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
async def test_safety_prop_no_schedule_if_blob_write_fails(success_scenario):
    """Safety property: If writing to storage fails, no new tasks should be scheduled."""
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
        # Should not schedule any new tasks due to storage failure!
        ctx.assert_messages_scheduled(0)


async def test_continue_graph_no_graph_id(evaluator_context):
    """Test continue_graph with no graph_id."""
    result = TaskResult(task_id="test-task", graph_id=None, status=TaskStatus.Success, result=42)

    # Should not raise any errors and should return early
    result = await evaluator_context.evaluator.continue_graph(result)
    assert result is None


async def test_graph_workflow_exception_handling(evaluator_context):
    """Test that graph workflow exceptions don't fail the original task."""
    # Mock storage.load_graph to raise an exception
    evaluator_context.mock_storage.load_graph.side_effect = Exception("Storage error")

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ):
        # Should still succeed despite the graph load error
        pass


async def test_continue_graph_graph_not_found(evaluator_context):
    """Test continue_graph when graph is not found."""
    result = TaskResult(
        task_id="test-task",
        graph_id="missing-graph",
        status=TaskStatus.Success,
        result=42,
    )

    evaluator_context.mock_storage.load_graph.return_value = None

    # Should not raise errors, just log and return
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
    evaluator_context.evaluator.task_publisher = mock_publish_task
    await evaluator_context.evaluator.continue_graph(child_result)

    # Should not publish any tasks since no new tasks are ready
    assert len(published_tasks) == 0
