import copy
import random
from unittest.mock import AsyncMock, Mock

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from boilermaker import retries
from boilermaker.app import Boilermaker
from boilermaker.evaluators import TaskGraphEvaluator
from boilermaker.storage import StorageInterface
from boilermaker.task import Task, TaskGraph, TaskResult, TaskStatus


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]


DEFAULT_STATE = State({"somekey": "somevalue"})


def make_message(task, sequence_number: int = 123):
    # Example taken from:
    # azure-sdk-for-python/blob/main/sdk/servicebus/azure-servicebus/tests/test_message.py#L233
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[task.model_dump_json().encode("utf-8")],
        message_annotations={SEQUENCENUBMERNAME: sequence_number},
    )
    return ServiceBusReceivedMessage(
        amqp_received_message, receiver=None, frame=my_frame
    )


@pytest.fixture
def mock_storage():
    """Mock storage interface for testing."""
    storage = Mock(spec=StorageInterface)
    storage.store_task_result = AsyncMock()
    storage.load_graph = AsyncMock(return_value=None)
    storage.store_graph = AsyncMock()
    return storage


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


@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


async def somefunc(state, x):
    return x * 2


@pytest.fixture
def evaluator(app, mockservicebus, mock_storage):
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
# task_handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_handler_success(evaluator, mock_storage):
    """Test that task_handler executes a registered function and stores result."""
    result = await evaluator.task_handler()
    assert result == 42

    # Should store successful result
    mock_storage.store_task_result.assert_called()
    stored_result_call = mock_storage.store_task_result.call_args[0][0]
    assert stored_result_call.status == TaskStatus.Success
    assert stored_result_call.result == 42


async def test_task_handler_missing_function(evaluator, mock_storage):
    """Test that task_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")
    task.graph_id = "test-graph-id"
    evaluator.task = task

    with pytest.raises(ValueError) as exc:
        await evaluator.task_handler()
    assert "Missing registered function" in str(exc.value)


async def test_task_handler_debug_task(evaluator, mock_storage):
    """Test that task_handler runs the debug task."""
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    task.graph_id = "test-graph-id"
    evaluator.task = task

    result = await evaluator.task_handler()
    # Should return whatever sample.debug_task returns
    assert result is not None

    # Should store result
    mock_storage.store_task_result.assert_called()


async def test_task_handler_with_graph_workflow(evaluator, mock_storage, sample_graph):
    """Test task_handler with graph workflow progression."""
    # Set up the task as part of the graph
    task = list(sample_graph.children.values())[0]  # Get first task
    task.function_name = evaluator.task.function_name
    task.payload = evaluator.task.payload
    evaluator.task = task

    # Mock the storage to return our sample graph
    mock_storage.load_graph.return_value = sample_graph

    result = await evaluator.task_handler()
    assert result == 42

    # Should load graph and store results
    mock_storage.load_graph.assert_called_with(task.graph_id)
    assert mock_storage.store_task_result.call_count >= 2  # Started + Success results


async def test_task_handler_exception_handling(evaluator, mock_storage):
    """Test that task_handler properly handles exceptions."""

    async def failing_func(state, x):
        raise ValueError("Test error")

    evaluator.function_registry["failing_func"] = failing_func
    task = Task.default("failing_func")
    task.graph_id = "test-graph-id"
    task.payload = {"args": (1,), "kwargs": {}}
    evaluator.task = task

    with pytest.raises(ValueError):
        await evaluator.task_handler()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Graph Workflow Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def testcontinue_graph_no_graph_id(evaluator):
    """Test continue_graph with no graph_id."""
    result = TaskResult(
        task_id="test-task", graph_id=None, status=TaskStatus.Success, result=42
    )

    # Should not raise any errors and should return early
    await evaluator.continue_graph(result)


async def testcontinue_graph_graph_not_found(evaluator, mock_storage):
    """Test continue_graph when graph is not found."""
    result = TaskResult(
        task_id="test-task",
        graph_id="missing-graph",
        status=TaskStatus.Success,
        result=42,
    )

    mock_storage.load_graph.return_value = None

    # Should not raise errors, just log and return
    await evaluator.continue_graph(result)
    mock_storage.load_graph.assert_called_with("missing-graph")


async def testcontinue_graph_publishes_ready_tasks(
    evaluator, mock_storage, sample_graph
):
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


async def testcontinue_graph_no_ready_tasks(evaluator, mock_storage, sample_graph):
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
# Message Handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_retries_exhausted(
    evaluator, mock_storage, mockservicebus
):
    """Test message_handler when retries are exhausted."""
    # Set up task with exhausted retries
    evaluator.task.attempts.attempts = evaluator.task.policy.max_tries + 1
    evaluator.task.graph_id = "test-graph-id"

    result = await evaluator.message_handler()
    assert result is None

    # Should store failure result
    mock_storage.store_task_result.assert_called()
    stored_result = mock_storage.store_task_result.call_args[0][0]
    assert stored_result.status == TaskStatus.Failure
    assert "Retries exhausted" in stored_result.errors

    # Should settle message
    assert len(mockservicebus._receiver.method_calls) == 1


@pytest.mark.parametrize("acks_early", [True, False])
async def test_message_handler_success(
    acks_early, evaluator, mock_storage, mockservicebus
):
    """Test successful message handling with early/late acks."""
    evaluator.task.acks_late = not acks_early
    evaluator.task.graph_id = "test-graph-id"

    result = await evaluator.message_handler()
    assert result is None

    # Should store successful result
    mock_storage.store_task_result.assert_called()

    # Should settle message
    assert len(mockservicebus._receiver.method_calls) == 1


async def test_message_handler_with_on_success_callback(
    evaluator, mock_storage, mockservicebus, app
):
    """Test message_handler with on_success callback."""

    async def on_success_func(state):
        return "success"

    app.register_async(on_success_func, policy=retries.NoRetry())
    success_task = app.create_task(on_success_func)
    evaluator.task.on_success = success_task
    evaluator.task.graph_id = "test-graph-id"

    result = await evaluator.message_handler()
    assert result is None

    # Should publish success callback task
    assert len(mockservicebus._sender.method_calls) == 1


async def test_message_handler_with_retry_exception(
    evaluator, mock_storage, mockservicebus, app
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
    assert result is None

    # Should publish retry task
    assert len(mockservicebus._sender.method_calls) == 1


async def test_message_handler_with_exception(
    evaluator, mock_storage, mockservicebus, app
):
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
    assert result is None

    # Should store failure result for graph processing
    mock_storage.store_task_result.assert_called()
    stored_result = mock_storage.store_task_result.call_args[0][0]
    assert stored_result.status == TaskStatus.Failure
    assert "Test error" in stored_result.errors


async def test_message_handler_with_on_failure_callback(
    evaluator, mock_storage, mockservicebus, app
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
    evaluator.function_registry["failing_func"] = failing_func

    result = await evaluator.message_handler()
    assert result is None

    # Should publish failure callback task
    assert len(mockservicebus._sender.method_calls) == 1


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
    graph_before_success = graph
    graph_after_success = copy.deepcopy(graph)
    result = TaskResult(
        task_id=task_a_instance.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
        result=42,
    )
    graph_after_success.add_result(result)

    mock_storage.load_graph.side_effect = [graph_before_success, graph_after_success]

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
    assert result is None

    # Should have stored results and published child tasks
    assert mock_storage.store_task_result.call_count >= 2  # Started + Success
    assert len(published_tasks) == 2  # Both child tasks should be published


async def test_garbage_message_handling(evaluator, mockservicebus):
    """Test that message_handler handles invalid JSON messages gracefully."""
    message_num = random.randint(100, 1000)
    # Create garbage message
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[b"{{\\])))"],  # Invalid JSON
        message_annotations={SEQUENCENUBMERNAME: message_num},
    )
    msg = ServiceBusReceivedMessage(
        amqp_received_message, receiver=None, frame=my_frame
    )
    evaluator.task.msg = msg

    result = await evaluator.message_handler()
    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Edge Case Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_with_no_graph_id(evaluator, mock_storage):
    """Test task execution when task has no graph_id."""
    evaluator.task.graph_id = None

    result = await evaluator.task_handler()
    assert result == 42

    # Should still store result (with graph_id=None)
    mock_storage.store_task_result.assert_called()
    stored_result = mock_storage.store_task_result.call_args[0][0]
    assert stored_result.graph_id is None


async def test_graph_workflow_exception_handling(evaluator, mock_storage):
    """Test that graph workflow exceptions don't fail the original task."""
    # Mock storage.load_graph to raise an exception
    mock_storage.load_graph.side_effect = Exception("Storage error")

    result = TaskResult(
        task_id="test-task", graph_id="test-graph", status=TaskStatus.Success, result=42
    )

    # Should not raise exception, just log and continue
    await evaluator.continue_graph(result)


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("acks_late", [True, False])
async def test_retry_policy_update(
    should_deadletter, acks_late, evaluator, mock_storage, mockservicebus, app
):
    """Test retry policy update from RetryException."""

    async def retry_func(state, x):
        raise retries.RetryExceptionDefaultExponential(
            "Retry with new policy", delay=800, max_delay=2000, max_tries=99
        )

    app.register_async(retry_func, policy=retries.RetryPolicy.default())
    task = app.create_task(retry_func)
    task.payload["args"] = (21,)
    task.msg = make_message(task)
    task.graph_id = "test-graph-id"
    task.should_dead_letter = should_deadletter
    task.acks_late = acks_late

    evaluator.task = task
    evaluator.function_registry["retry_func"] = retry_func

    result = await evaluator.message_handler()
    assert result is None

    # Should publish retry task with updated policy
    assert len(mockservicebus._sender.method_calls) == 1
    publish_call = mockservicebus._sender.method_calls[0]
    published_task = Task.model_validate_json(str(publish_call[1][0]))
    assert published_task.policy.retry_mode == retries.RetryMode.Exponential
    assert published_task.policy.max_tries == 99
