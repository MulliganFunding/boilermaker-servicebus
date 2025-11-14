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
    evaluator = evaluator_context.get_evaluator()
    evaluator.task = task

    # Should not raise
    result = await evaluator.message_handler()
    assert result.status == TaskStatus.Failure
    mock_storage.store_task_result.assert_not_called()


async def test_message_handler_debug_task(evaluator_context, mock_storage):
    """Test that message_handler runs the debug task."""
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    task.graph_id = "test-graph-id"
    evaluator = evaluator_context.get_evaluator()
    evaluator.task = task

    assert await evaluator.message_handler() is None

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


async def test_message_handler_with_on_success_callback(
    app,
    evaluator_context,
):
    """
    Test message_handler with on_success callback.

    Expected behavior: if `on_success` is not None, then
    `on_success` task is published after successful execution.
    """

    # We are adding a fake on_success callback which we do not expect to be invoked
    async def on_success_fake(state):
        return "fake"

    app.register_async(on_success_fake, policy=retries.NoRetry())
    evaluator_context.current_task.on_success = Task.si(on_success_fake)

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        ctx.assert_messages_scheduled(1)

        others = ctx.get_other_storage_calls()
        assert len(others) == 1
        t1 = others[0]
        assert isinstance(t1.args[0], TaskResultSlim)
        t1_slim = t1.args[0]
        task1 = evaluator_context.get_task(t1_slim.task_id)
        assert t1_slim.task_id == task1.task_id
        assert task1.function_name == "failure_callback"


async def test_message_handler_with_retry_exception(evaluator_context, mock_storage, mockservicebus, make_message):
    """Test message_handler handling RetryException."""

    evaluator_context.set_task_to_retry(sample_graph)
    result = await evaluator_context.get_evaluator().message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Retry
    assert "Retry for 100" == result.errors[0]

    # Should store retry result
    _started, stored_result, _ = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Retry

    # Should publish retry task
    assert len(mockservicebus._sender.method_calls) == 1


async def test_message_handler_retries_exhausted(retries_exhausted_scenario, make_message):
    """Test message_handler when retries are exhausted."""
    # Set up task with exhausted retries
    evaluator = retries_exhausted_scenario.get_evaluator()
    evaluator.task.attempts.attempts = evaluator.task.policy.max_tries + 1
    # Can't dead letter without this attrib
    evaluator.task.msg = make_message(evaluator.task, sequence_number=1234)

    async with retries_exhausted_scenario.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.RetriesExhausted,
    ) as ctx:
        ctx.assert_messages_scheduled(1)

        # Get other stored calls to check what was scheduled after
        others = ctx.get_other_storage_calls()
        assert len(others) == 1
        f1 = others[0]
        assert isinstance(f1.args[0], TaskResultSlim)
        f1_slim = f1.args[0]
        fail_task = retries_exhausted_scenario.get_task(f1_slim.task_id)
        assert f1_slim.task_id == fail_task.task_id
        assert fail_task.function_name == "failure_callback"


async def test_message_handler_with_exception(evaluator_context, mock_storage, mockservicebus, app, make_message):
    """Test message_handler handling regular exceptions."""

    async def failing_func(state, x):
        raise ValueError("Test error")

    app.register_async(failing_func, policy=retries.RetryPolicy.default())
    task = app.create_task(failing_func)
    task.payload["args"] = (21,)
    task.msg = make_message(task)
    task.graph_id = "test-graph-id"
    evaluator_context.get_evaluator().task = task
    evaluator_context.get_evaluator().function_registry["failing_func"] = failing_func

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Test error" in result.errors[0]

    # Should store failure result
    _started_result, stored_result, _ = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Failure
    assert "Test error" in stored_result.errors


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Graph Workflow Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_continue_graph_no_graph_id(evaluator):
    """Test continue_graph with no graph_id."""
    result = TaskResult(task_id="test-task", graph_id=None, status=TaskStatus.Success, result=42)

    # Should not raise any errors and should return early
    result = await evaluator.continue_graph(result)
    assert result is None


async def test_continue_graph_graph_not_found(evaluator, mock_storage):
    """Test continue_graph when graph is not found."""
    result = TaskResult(
        task_id="test-task",
        graph_id="missing-graph",
        status=TaskStatus.Success,
        result=42,
    )

    mock_storage.load_graph.return_value = None

    # Should not raise errors, just log and return
    result = await evaluator.continue_graph(result)
    assert result is None
    mock_storage.load_graph.assert_called_with("missing-graph")


async def test_continue_graph_publishes_ready_tasks(evaluator, mock_storage):
    """Test that continue_graph publishes newly ready tasks."""
    # Complete the root task
    root_task_id = list(sample_graph.edges.keys())[0]  # Get first child task
    # mark root task as successful
    result = TaskResult(
        task_id=root_task_id,
        graph_id=sample_graph.graph_id,
        status=TaskStatus.Success,
        result=42,
    )
    sample_graph.add_result(result)
    mock_storage.load_graph.return_value = sample_graph

    # Mock task publisher to track calls
    published_tasks = []

    async def mock_publish_task(task, *args, **kwargs):
        published_tasks.append(task)

    evaluator.task_publisher = mock_publish_task

    await evaluator.continue_graph(result)

    # Should have published the two child tasks that are now ready
    assert len(published_tasks) == len(sample_graph.edges[root_task_id])


async def test_continue_graph_no_ready_tasks(evaluator, mock_storage):
    """Test continue_graph when no tasks are ready."""
    # Root task STARTED
    parent_started = TaskResult(
        task_id=next(iter(sample_graph.edges.keys())),
        graph_id=sample_graph.graph_id,
        status=TaskStatus.Started,
        result=None,
    )
    sample_graph.add_result(parent_started)

    mock_storage.load_graph.return_value = sample_graph

    published_tasks = []

    async def mock_publish_task(task, *args, **kwargs):
        published_tasks.append(task)

    first_child_task_id = next(iter(next(iter(sample_graph.edges.values()))))
    child_result = TaskResult(
        task_id=first_child_task_id,
        graph_id=sample_graph.graph_id,
        status=TaskStatus.Success,
        result=42,
    )
    evaluator.task_publisher = mock_publish_task
    await evaluator.continue_graph(child_result)

    # Should not publish any tasks since no new tasks are ready
    assert len(published_tasks) == 0


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Integration Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_graph_workflow_exception_handling(evaluator, mock_storage):
    """Test that graph workflow exceptions don't fail the original task."""
    # Mock storage.load_graph to raise an exception
    mock_storage.load_graph.side_effect = Exception("Storage error")

    result = TaskResult(task_id="test-task", graph_id="test-graph", status=TaskStatus.Success, result=42)

    # Should not raise exception, just log and continue
    await evaluator.continue_graph(result)


async def test_retries_exhausted_with_storage(app, evaluator, mockservicebus, mock_storage, make_message):
    """Test that retries exhausted scenario stores failure result."""

    evaluator.task.msg = make_message(evaluator.task, sequence_number=321)
    evaluator.task.attempts.attempts = evaluator.task.policy.max_tries + 1

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.RetriesExhausted
    assert result.result is None

    # Verify task started was stored and  task_result was stored
    _started_result, stored_result, _ = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.RetriesExhausted

    # Task should be deadlettered
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[0] == "dead_letter_message"


async def test_retry_policy_update_with_storage(app, evaluator, mockservicebus, mock_storage, make_message):
    """Test that retry policy can be updated during retry exception handling."""

    async def retrytask2(state):
        new_policy = retries.RetryPolicy(max_tries=10, retry_mode=retries.RetryMode.Exponential)
        raise retries.RetryException("Retry with new policy", policy=new_policy)

    app.register_async(retrytask2, policy=retries.RetryPolicy.default())
    task = app.create_task(retrytask2)

    task.msg = make_message(task, sequence_number=123)
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Retry
    assert result.result is None

    # Task policy should be updated
    assert task.policy.max_tries == 10
    assert task.policy.retry_mode == retries.RetryMode.Exponential

    # Should publish retry with new policy
    assert len(mockservicebus._sender.method_calls) == 1
    publish_retry_call = mockservicebus._sender.method_calls[0]
    assert publish_retry_call[0] == "schedule_messages"

    # Should still store result
    # Verify task started was stored and  task_result was stored
    _started_result, stored_result, _ = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Retry


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Exception handling tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_early_ack_task_lease_lost_exception(evaluator, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.graph_id = "test-graph-id"
    evaluator.task = task

    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return None when lease is lost during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]

    evaluator.complete_message.assert_called_once()

    # Should still store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_early_ack_service_bus_error_exception(evaluator, mock_storage, app):
    """Test BoilermakerServiceBusError exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.graph_id = "test-graph-id"
    evaluator.task = task

    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return None when service bus error occurs during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    evaluator.complete_message.assert_called_once()

    # Should still store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_retries_exhausted_task_lease_lost_exception(evaluator, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception when settling message for exhausted retries."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Don't settle early
    task.graph_id = "test-graph-id"
    # Make retries exhausted
    task.attempts.attempts = task.policy.max_tries + 1
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return None when lease is lost during exhausted retries settlement
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]
    evaluator.deadletter_or_complete_task.assert_called_once_with("ProcessingError", detail="Retries exhausted")

    # Should store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_retries_exhausted_service_bus_error_exception(evaluator, mock_storage, app):
    """Test BoilermakerServiceBusError exception when settling message for exhausted retries."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Don't settle early
    task.graph_id = "test-graph-id"
    # Make retries exhausted
    task.attempts.attempts = task.policy.max_tries + 1
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerServiceBusError
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return None when service bus error occurs during exhausted retries settlement
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    evaluator.deadletter_or_complete_task.assert_called_once_with("ProcessingError", detail="Retries exhausted")

    # Should store the start result
    assert mock_storage.store_task_result.call_count == 1
    start_call = mock_storage.store_task_result.call_args_list[0][0][0]
    assert start_call.status == TaskStatus.Started


async def test_late_settlement_task_lease_lost_exception_success(evaluator, mock_storage, app):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    evaluator.task = task

    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator.complete_message.assert_called_once()

    # Should store both start and success results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_task_lease_lost_exception_failure(evaluator, mock_storage, app, make_message):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for failed task."""

    async def failtask(state):
        raise ValueError("Test failure")

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    task.msg = make_message(task)
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")

    # Should store both start and failure results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_service_bus_error_exception_success(evaluator, mock_storage, app):
    """Test BoilermakerServiceBusError exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    evaluator.task = task

    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator.complete_message.assert_called_once()

    # Should store both start and success results
    assert mock_storage.store_task_result.call_count == 2


async def test_late_settlement_service_bus_error_exception_failure(evaluator, mock_storage, app, make_message):
    """Test BoilermakerServiceBusError exception during late message settlement for failed task."""

    async def failtask(state):
        raise ValueError("Test failure")

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
    task.graph_id = "test-graph-id"
    task.msg = make_message(task)
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerServiceBusError
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")

    # Should store both start and failure results
    assert mock_storage.store_task_result.call_count == 2


async def test_evaluator_simple_success(app, mockservicebus, mock_storage, make_message):
    """Simple success test for TaskGraphEvaluator."""

    # Simple async task
    async def simple_task(state):
        return "success!"

    # Register and create clean evaluator
    app.register_async(simple_task)
    task = app.create_task(simple_task)
    task.msg = make_message(task)

    evaluator = TaskGraphEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    # Execute and verify
    result = await evaluator.message_handler()
    assert result.status == TaskStatus.Success
    assert result.result == "success!"

    # Use helper for verification
    verify_storage_started_and_get_result_calls(mock_storage, task)
