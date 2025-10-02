from unittest.mock import AsyncMock

import pytest
from boilermaker import exc, retries
from boilermaker.evaluators import TaskGraphEvaluator
from boilermaker.task import Task, TaskGraph, TaskResult, TaskStatus

# Requires running pytest with `--import-mode importlib`
from .helpers import verify_storage_started_and_get_result_calls


@pytest.fixture
def sample_graph():
    """Create a sample TaskGraph for testing."""
    graph = TaskGraph()

    # Create some tasks
    task1 = Task.default("task1")
    task2 = Task.default("task2")
    task3 = Task.default("task3")
    task3.graph_id = graph.graph_id

    # Add tasks to graph
    graph.add_task(task1)  # Root task
    graph.add_task(task2, parent_id=task1.task_id)  # Depends on task1
    graph.add_task(task3, parent_id=task1.task_id)  # Also depends on task1

    return graph


async def somefunc(state, x):
    return x * 2


@pytest.fixture
def evaluator(app, mockservicebus, mock_storage, make_message):
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    task.payload["args"] = (21,)
    task.msg = make_message(task)

    return TaskGraphEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler generic Tests
# (Making sure parent-class expectations are not violated)
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_missing_function(evaluator, mock_storage):
    """Test that message_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")
    task.graph_id = "test-graph-id"
    evaluator.task = task

    # Should not raise
    result = await evaluator()
    assert result.status == TaskStatus.Failure
    mock_storage.store_task_result.assert_not_called()


async def test_message_handler_debug_task(evaluator, mock_storage):
    """Test that message_handler runs the debug task."""
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    task.graph_id = "test-graph-id"
    evaluator.task = task

    result = await evaluator()
    assert result.result == 0
    assert result.status == TaskStatus.Success

    # Should not store result
    mock_storage.store_task_result.assert_not_called()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Initialization Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
def test_task_graph_evaluator_requires_storage(app, mockservicebus):
    """Test that TaskGraphEvaluator requires a storage interface."""
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    with pytest.raises(ValueError, match="Storage interface is required"):
        TaskGraphEvaluator(
            mockservicebus._receiver,
            task,
            app.publish_task,
            app.function_registry,
            state=app.state,
            storage_interface=None,
        )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message Handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_retries_exhausted(evaluator, mock_storage, mockservicebus):
    """Test message_handler when retries are exhausted."""
    # Set up task with exhausted retries
    evaluator.task.attempts.attempts = evaluator.task.policy.max_tries + 1
    evaluator.task.graph_id = "test-graph-id"

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result is None
    assert result.status == TaskStatus.RetriesExhausted

    # Should store failure result
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.RetriesExhausted

    # No graph loaded
    mock_storage.load_graph.assert_not_called()

    # Should settle message
    assert len(mockservicebus._receiver.method_calls) == 1


@pytest.mark.parametrize("acks_early", [True, False])
async def test_message_handler_no_graph_success(acks_early, evaluator, mock_storage, mockservicebus):
    """Test successful message handling with early/late acks."""
    evaluator.task.acks_late = not acks_early
    evaluator.task.graph_id = None

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == 42
    assert result.status == TaskStatus.Success

    # Should store successful result
    # Should store started and successful result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 42

    # No graph loaded
    mock_storage.load_graph.assert_not_called()

    # Should settle message
    assert len(mockservicebus._receiver.method_calls) == 1


async def test_message_handler_with_on_success_callback(evaluator, mock_storage, mockservicebus, app):
    """
    Test message_handler with on_success callback.

    Expected behavior: if `on_success` is not None, then
    `on_success` task is published after successful execution.
    """

    async def on_success_func(state):
        return "success"

    app.register_async(on_success_func, policy=retries.NoRetry())
    success_task = app.create_task(on_success_func)
    evaluator.task.on_success = success_task
    evaluator.task.graph_id = "test-graph-id"

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == 42
    assert result.status == TaskStatus.Success

    # Should store successful result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 42

    # Graph-id -> Graph loaded
    mock_storage.load_graph.assert_called()

    # Should publish success callback task
    assert len(mockservicebus._sender.method_calls) == 1


async def test_message_handler_with_retry_exception(
    evaluator, mock_storage, mockservicebus, app, make_message
):
    """Test message_handler handling RetryException."""

    async def retry_func(state, x):
        raise retries.RetryException("Retry me")

    app.register_async(retry_func, policy=retries.RetryPolicy.default())
    task = app.create_task(retry_func)
    task.payload["args"] = (21,)
    task.msg = make_message(task)
    task.graph_id = "test-graph-id"
    evaluator.task = task
    evaluator.function_registry["retry_func"] = retry_func

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Retry
    assert "Retry me" == result.errors[0]

    # Should store retry result
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Retry

    # Should publish retry task
    assert len(mockservicebus._sender.method_calls) == 1


async def test_message_handler_with_exception(evaluator, mock_storage, mockservicebus, app, make_message):
    """Test message_handler handling regular exceptions."""

    async def failing_func(state, x):
        raise ValueError("Test error")

    app.register_async(failing_func, policy=retries.RetryPolicy.default())
    task = app.create_task(failing_func)
    task.payload["args"] = (21,)
    task.msg = make_message(task)
    task.graph_id = "test-graph-id"
    evaluator.task = task
    evaluator.function_registry["failing_func"] = failing_func

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Test error" in result.errors[0]

    # Should store failure result
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Failure
    assert "Test error" in stored_result.errors


async def test_message_handler_with_on_failure_callback(
    evaluator, mock_storage, mockservicebus, app, make_message
):
    """Test message_handler with on_failure callback."""

    async def failing_func(state, x):
        raise ValueError("Test error")

    async def on_failure_func(state):
        return "failure handled"

    app.register_async(failing_func, policy=retries.RetryPolicy.default())
    app.register_async(on_failure_func, policy=retries.NoRetry())

    task = app.create_task(failing_func)
    task.payload["args"] = (21,)
    task.msg = make_message(task)
    task.graph_id = "test-graph-id"
    task.on_failure = app.create_task(on_failure_func)

    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Test error" in result.errors[0]

    # Should store failure result
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Failure

    # Should publish failure callback task
    assert len(mockservicebus._sender.method_calls) == 1


@pytest.mark.parametrize("acks_early", [True, False])
async def test_message_handler_success_no_graph(acks_early, evaluator, mock_storage):
    """Test that message_handler executes a registered function and stores result."""
    result = await evaluator.message_handler()
    assert result.result == 42

    # Should store started and successful result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 42

    # Should store failure result
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Success
    assert stored_result.result == 42


async def test_message_handler_with_graph_workflow(evaluator, mock_storage, sample_graph):
    """Test message_handler with graph workflow progression."""
    # Set up the task as part of the graph
    task = list(sample_graph.children.values())[0]  # Get first task
    task.function_name = evaluator.task.function_name
    task.payload = evaluator.task.payload
    evaluator.task = task

    # Mock the storage to return our sample graph
    mock_storage.load_graph.return_value = sample_graph

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == 42
    assert result.status == TaskStatus.Success

    # Should store started and successful result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 42

    # Should have loaded graph
    mock_storage.load_graph.assert_called_with(task.graph_id)


async def test_message_handler_exception_handling(evaluator, mock_storage):
    """Test that message_handler properly handles exceptions."""

    async def failing_func(state, x):
        raise ValueError("Test error")

    evaluator.function_registry["failing_func"] = failing_func
    task = Task.default("failing_func")
    task.graph_id = "test-graph-id"
    task.payload = {"args": (1,), "kwargs": {}}
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result is None
    assert result.status == TaskStatus.Failure
    assert "Test error" in result.errors[0]

    # Should store result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Failure

    # Should not load graph or continue
    mock_storage.load_graph.assert_not_called()


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


async def test_continue_graph_publishes_ready_tasks(evaluator, mock_storage, sample_graph):
    """Test that continue_graph publishes newly ready tasks."""
    # Complete the root task
    root_task_id = list(sample_graph.edges.keys())[0]  # Get child tasks
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


async def test_continue_graph_no_ready_tasks(evaluator, mock_storage, sample_graph):
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
async def test_full_graph_workflow_integration(app, mockservicebus, mock_storage):
    """Test full integration of TaskGraphEvaluator with a complete workflow."""

    # Create functions
    async def task_a(state, x):
        return x * 2

    async def task_b(state, x):
        return x + 10

    async def task_c(state, x):
        return x - 5

    # Register functions
    app.register_async(task_a, policy=retries.RetryPolicy.default())
    app.register_async(task_b, policy=retries.RetryPolicy.default())
    app.register_async(task_c, policy=retries.RetryPolicy.default())

    # Create tasks
    task_a_instance = app.create_task(task_a)
    task_a_instance.payload["args"] = (5,)
    task_b_instance = app.create_task(task_b)
    task_b_instance.payload["args"] = (20,)
    task_c_instance = app.create_task(task_c)
    task_c_instance.payload["args"] = (15,)

    # Create graph
    graph = TaskGraph()

    # Add to graph
    graph.add_task(task_a_instance)
    graph.add_task(task_b_instance, parent_id=task_a_instance.task_id)
    graph.add_task(task_c_instance, parent_id=task_a_instance.task_id)

    # Mock storage to return our graph
    result = TaskResult(
        task_id=task_a_instance.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
        result=42,
    )
    graph.add_result(result)
    mock_storage.load_graph.return_value = graph

    # Create evaluator for task_a
    evaluator = TaskGraphEvaluator(
        mockservicebus._receiver,
        task_a_instance,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    # Track published tasks
    published_tasks = []

    async def mock_publish_task(task, *args, **kwargs):
        published_tasks.append(task)

    evaluator.task_publisher = mock_publish_task

    # Execute the message handler
    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == 10
    assert result.status == TaskStatus.Success
    assert result.task_id == task_a_instance.task_id

    # Should store started and successful result
    stored_result_call = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 10
    assert stored_result_call.task_id == task_a_instance.task_id

    # Should have stored results and published child tasks
    assert mock_storage.store_task_result.call_count >= 2  # Started + Success
    assert len(published_tasks) == 2  # Both child tasks should be published


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Edge Case Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_with_no_graph_id(evaluator, mock_storage):
    """Test task execution when task has no graph_id."""
    evaluator.task.graph_id = None

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == 42
    assert result.status == TaskStatus.Success

    # Should still store result (with graph_id=None)
    mock_storage.store_task_result.assert_called()
    stored_result = mock_storage.store_task_result.call_args[0][0]
    assert stored_result.graph_id is None


async def test_graph_workflow_exception_handling(evaluator, mock_storage):
    """Test that graph workflow exceptions don't fail the original task."""
    # Mock storage.load_graph to raise an exception
    mock_storage.load_graph.side_effect = Exception("Storage error")

    result = TaskResult(task_id="test-task", graph_id="test-graph", status=TaskStatus.Success, result=42)

    # Should not raise exception, just log and continue
    await evaluator.continue_graph(result)


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_storage(
    can_retry,
    has_on_failure,
    should_deadletter,
    evaluator,
    app,
    mockservicebus,
    mock_storage,
    make_message,
):
    """Test retry logic with result storage and on_failure callback."""

    async def retrytask(state):
        raise retries.RetryException("Retry me")

    async def onfail(state, **kwargs):
        return 1

    # Register functions
    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    app.register_async(onfail, policy=retries.NoRetry())

    # Create task
    task = app.create_task(retrytask)
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        task.on_failure = app.create_task(onfail, somekwarg="akwargval")

    if not can_retry:
        task.attempts.attempts = task.policy.max_tries + 1

    task.msg = make_message(task, sequence_number=148)
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Retry if can_retry else TaskStatus.RetriesExhausted
    assert result.result is None

    # Task should be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == 148

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)

    if not can_retry:
        # Retries exhausted - should store failure result
        assert isinstance(stored_result, TaskResult)
        assert stored_result.status == TaskStatus.RetriesExhausted

        if has_on_failure:
            # Should publish failure callback
            assert len(mockservicebus._sender.method_calls) == 1
            publish_fail_call = mockservicebus._sender.method_calls[0]
            assert publish_fail_call[0] == "schedule_messages"
            published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
            assert published_task.function_name == "onfail"
        else:
            # No callbacks published
            assert not mockservicebus._sender.method_calls
    else:
        # Can retry - should publish retry task, no storage calls
        assert len(mockservicebus._sender.method_calls) == 1
        publish_retry_call = mockservicebus._sender.method_calls[0]
        assert publish_retry_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_retry_call[1][0]))
        assert published_task.function_name == "retrytask"


async def test_retries_exhausted_with_storage(app, evaluator, mockservicebus, mock_storage, make_message):
    """Test that retries exhausted scenario stores failure result."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    # Set attempts to exceed max tries
    task.attempts.attempts = task.policy.max_tries + 1

    task.msg = make_message(task, sequence_number=321)
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.RetriesExhausted
    assert result.result is None

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.RetriesExhausted

    # Task should be deadlettered
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[0] == "dead_letter_message"


async def test_retry_policy_update_with_storage(app, evaluator, mockservicebus, mock_storage, make_message):
    """Test that retry policy can be updated during retry exception handling."""

    async def retrytask(state):
        new_policy = retries.RetryPolicy(max_tries=10, retry_mode=retries.RetryMode.Exponential)
        raise retries.RetryException("Retry with new policy", policy=new_policy)

    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    task = app.create_task(retrytask)

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
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Retry


async def test_early_acks_with_storage(app, evaluator, mockservicebus, mock_storage, make_message):
    """Test that early acknowledgment works correctly with storage."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Early ack
    task.msg = make_message(task, sequence_number=456)
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == "OK"
    assert result.status == TaskStatus.Success

    # Should complete message (early ack)
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[0] == "complete_message"

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, evaluator.task)
    assert stored_result.status == TaskStatus.Success


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Exception handling tests for uncovered lines
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
    assert result is None
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
    assert result is None
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
    assert result is None
    evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )

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
    evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator.message_handler()

    # Should return None when service bus error occurs during exhausted retries settlement
    assert result is None
    evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )

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


async def test_late_settlement_service_bus_error_exception_failure(
    evaluator, mock_storage, app, make_message
):
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
    evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator.message_handler()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")

    # Should store both start and failure results
    assert mock_storage.store_task_result.call_count == 2
