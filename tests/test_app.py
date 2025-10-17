import asyncio
import os
import random
import signal
from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from anyio import create_task_group, to_thread
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from azure.servicebus.exceptions import ServiceBusError
from boilermaker import retries
from boilermaker.app import Boilermaker, BoilermakerAppException
from boilermaker.evaluators import NoStorageEvaluator
from boilermaker.exc import BoilermakerStorageError
from boilermaker.task import Task, TaskGraph


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]


DEFAULT_STATE = State({"somekey": "somevalue"})


@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


@pytest.fixture
def evaluator(app, mockservicebus):
    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc, 21)

    return NoStorageEvaluator(mockservicebus._receiver, task, app.publish_task, app.function_registry)


def test_app_state(app):
    """Test that app.state is set correctly."""
    assert app.state == DEFAULT_STATE


async def test_task_decorator(app):
    """Test that the task decorator registers and calls a function."""

    @app.task()
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__

    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_task_decorator_with_policy(app):
    """Test that the task decorator registers a function with a custom retry policy."""

    @app.task(policy=retries.RetryPolicy.default())
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert app.task_registry[somefunc.__name__].policy == retries.RetryPolicy.default()
    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_app_register_async(app):
    """Test registering a single async function as a task."""

    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_app_register_many_async(app, mockservicebus, evaluator, make_message):
    """Test registering multiple async functions as tasks and message handling."""

    async def somefunc1(state):
        return state["somekey1"]

    async def somefunc2(state, one_arg):
        return state["somekey2"]

    async def somefunc3(state, one_arg, two_args, one_kwarg=True):
        return state["somekey2"]

    all_funcs = [somefunc1, somefunc2, somefunc3]
    # We do not specify a policy here
    app.register_many_async(all_funcs)
    for func in all_funcs:
        assert func.__name__ in app.task_registry
        assert app.task_registry[func.__name__].function_name == func.__name__
        assert app.task_registry[func.__name__].policy == retries.RetryPolicy.default()

    task1 = app.create_task(somefunc1)
    task2 = app.create_task(somefunc2, "a")
    task3 = app.create_task(somefunc3, "b", 123, one_kwarg="11")
    for task in [task1, task2, task3]:
        await app.message_handler(make_message(task), mockservicebus._receiver)

    # Failures
    def not_a_coroutine(state):
        return state["somekey"]

    with pytest.raises(ValueError):
        app.register_async(not_a_coroutine)

    # Register previously registered function
    with pytest.raises(ValueError):
        app.register_async(somefunc1)


async def test_create_task(app):
    """Test creating a task from a registered function."""

    async def somefunc(state, **kwargs):
        state.inner.update(kwargs)
        return state["somekey"]

    # Function must be registered  first
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    # Now we can create a task out of it
    task = app.create_task(somefunc, somekwarg="akwargval")
    assert task.function_name == "somefunc"
    assert task.payload == {"args": (), "kwargs": {"somekwarg": "akwargval"}}
    assert task.attempts.attempts == 0
    assert task.acks_late
    assert task.acks_early is False
    assert task.can_retry
    assert task.get_next_delay() == retries.RetryPolicy.default().delay
    assert task.record_attempt()
    assert task.attempts.attempts == 1


async def test_create_task_with_policy(app):
    """Test creating a task with a custom retry policy."""

    async def somefunc(state, **kwargs):
        state.inner.update(kwargs)
        return state["somekey"]

    # Function must be registered first: use default policy
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    # Sanity check: default policy should be higher than what we're setting below
    assert app.task_registry["somefunc"].policy.max_tries > 2
    # Now we can create a task out of it
    policy = retries.RetryPolicy(
        max_tries=2, delay=30, delay_max=600, retry_mode=retries.RetryMode.Exponential
    )
    task = app.create_task(somefunc, somekwarg="akwargval", policy=policy)
    assert task.function_name == "somefunc"
    assert task.payload == {"args": (), "kwargs": {"somekwarg": "akwargval"}}
    assert task.policy.max_tries == 2
    assert task.attempts.attempts == 0
    assert task.acks_late
    assert task.acks_early is False
    assert task.can_retry
    assert task.get_next_delay() <= policy.delay
    assert task.record_attempt()
    assert task.attempts.attempts == 1
    assert task.record_attempt()
    assert task.can_retry
    assert task.record_attempt()
    assert not task.can_retry


async def test_create_task_failures(app):
    """Test error handling for unregistered functions and tasks."""

    async def one(state):
        pass

    # Check failures
    with pytest.raises(ValueError) as exc:
        app.create_task(one)
        assert "Unregistered function" in str(exc)

    # Register it but leave from task registry
    app.function_registry[one.__name__] = one
    with pytest.raises(ValueError) as exc:
        app.create_task(one)
        assert "Unregistered task" in str(exc)


async def test_apply_async_with_policy(app, mockservicebus):
    """Test applying a task asynchronously with a custom retry policy."""

    async def somefunc(state, **kwargs):
        state.inner.update(kwargs)
        return state["somekey"]

    # Function must be registered  first
    app.register_async(somefunc, policy=retries.RetryPolicy.default())

    policy = retries.RetryPolicy(max_tries=2, delay=30, delay_max=600, retry_mode=retries.RetryMode.Linear)
    # Now we can try to publish
    await app.apply_async(somefunc, bla="heynow", policy=policy)
    assert len(mockservicebus._sender.method_calls) == 1
    publish_success_call = mockservicebus._sender.method_calls[0]
    assert publish_success_call[0] == "schedule_messages"
    # We should always be able to deserialize the task
    published_task = Task.model_validate_json(str(publish_success_call[1][0]))
    # This task was published with a linear policy
    assert published_task.policy.retry_mode == retries.RetryMode.Linear
    assert published_task.payload["args"] == []
    assert published_task.payload["kwargs"] == {"bla": "heynow"}
    assert published_task.function_name == "somefunc"
    assert published_task.on_success is None
    assert published_task.on_failure is None


async def test_apply_async_no_policy(app, mockservicebus):
    """Test applying a task asynchronously with the default retry policy."""

    async def somefunc(state, **kwargs):
        state.inner.update(kwargs)
        return state["somekey"]

    # Function must be registered  first
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    # Now we can try to publish
    await app.apply_async(somefunc, bla="heynow")
    assert len(mockservicebus._sender.method_calls) == 1
    publish_success_call = mockservicebus._sender.method_calls[0]
    assert publish_success_call[0] == "schedule_messages"
    # We should always be able to deserialize the task
    published_task = Task.model_validate_json(str(publish_success_call[1][0]))
    assert published_task.policy.retry_mode == retries.RetryMode.Fixed
    assert published_task.payload["args"] == []
    assert published_task.payload["kwargs"] == {"bla": "heynow"}
    assert published_task.function_name == "somefunc"
    assert published_task.on_success is None
    assert published_task.on_failure is None


# Chain Tests
async def sample_task(state, number1: int, number2: int = 4):
    if "sample_task_called" not in state.inner:
        state.inner["sample_task_called"] = 0

    state.inner["sample_task_called"] += 1
    if "sample_task_payload" not in state.inner:
        state.inner["sample_task_payload"] = []

    state.inner["sample_task_payload"].append((number1, number2))

    # simulate failure
    if number1 + number2 > 10:
        raise ValueError("Number too big!")

    return number1 + number2


async def failing_task(state) -> int:
    if "fail_count" not in state.inner:
        state.inner["fail_count"] = 0
    state.inner["fail_count"] += 1
    return state.inner["fail_count"]


def test_chain_fail(app):
    # on_failure must be a Task or None
    with pytest.raises(ValueError):
        app.chain(Task.si(sample_task, 1), Task.si(sample_task, 2), on_failure="not_a_task")
    # must have enough tasks
    with pytest.raises(ValueError):
        app.chain(
            Task.si(sample_task, 1),
        )


@pytest.mark.parametrize("has_fail", [True, False])
def test_chain_odd_task_count(app, has_fail):
    fail = Task.si(failing_task) if has_fail else None
    workflow = app.chain(
        Task.si(sample_task, 1, 2),
        Task.si(sample_task, 3),
        Task.si(sample_task, 5, number2=6),
        on_failure=fail,
    )
    assert isinstance(workflow, Task)
    assert workflow.on_failure == fail
    assert workflow.on_success is not None
    # Evaluate the chain
    current = workflow
    count = 0
    while current is not None:
        assert current.on_failure == fail
        count += 1
        current = current.on_success


@pytest.mark.parametrize("has_fail", [True, False])
async def test_chain_even_task_count(app, has_fail):
    fail = Task.si(failing_task) if has_fail else None
    workflow = app.chain(
        Task.si(sample_task, 1, 2),
        Task.si(sample_task, 3),
        Task.si(sample_task, 5, number2=6),
        Task.si(sample_task, 4, number2=9),
        on_failure=fail,
    )
    assert isinstance(workflow, Task)
    assert workflow.on_failure == fail
    assert workflow.on_success is not None
    # Evaluate the chain
    current = workflow
    count = 0
    while current is not None:
        assert current.on_failure == fail
        count += 1
        current = current.on_success
    assert count == 4


async def test_chain_publish(app, mockservicebus):
    app.register_async(failing_task)
    app.register_async(sample_task)

    fail = Task.si(failing_task)
    workflow = app.chain(
        Task.si(sample_task, 1, 2),
        Task.si(sample_task, 3),
        Task.si(sample_task, 5, number2=6),
        on_failure=fail,
    )
    assert isinstance(workflow, Task)
    await app.publish_task(workflow)

    # Publish new task with sender
    assert len(mockservicebus._sender.method_calls) == 1
    publish_success_call = mockservicebus._sender.method_calls[0]
    assert publish_success_call[0] == "schedule_messages"
    # We should always be able to deserialize the task
    published_task = Task.model_validate_json(str(publish_success_call[1][0]))
    assert published_task.payload["args"] == [1, 2]
    assert published_task.payload["kwargs"] == {}
    assert published_task.function_name == "sample_task"
    assert published_task.on_failure.function_name == "failing_task"
    assert published_task.on_success is not None
    published_task.on_success.function_name = "sample_task"
    assert published_task.on_success.payload["args"] == [
        3,
    ]
    assert published_task.on_success.on_success is not None
    published_task.on_success.on_success.function_name = "sample_task"
    published_task.on_success.on_success.payload["args"] = (5,)
    assert published_task.on_success.on_success.payload["kwargs"] == {"number2": 6}


async def test_chain_publish_and_evaluate(app, mockservicebus, make_message):
    app.register_async(failing_task)
    app.register_async(sample_task)
    fail = Task.si(failing_task)
    workflow = app.chain(
        Task.si(sample_task, 1, 2),
        # This one should fail!
        Task.si(sample_task, 5, number2=6),
        Task.si(sample_task, 3),
        on_failure=fail,
    )
    assert isinstance(workflow, Task)
    await app.publish_task(workflow)

    # Publish new task with sender
    assert len(mockservicebus._sender.method_calls) == 1
    publish_success_call1 = mockservicebus._sender.method_calls[0]
    assert publish_success_call1[0] == "schedule_messages"
    # We should always be able to deserialize the task
    published_task = Task.model_validate_json(str(publish_success_call1[1][0]))
    smb_msg1 = make_message(published_task, sequence_number=1)

    # *EVALUATE*: We should be able to run the task based on our published task
    await app.message_handler(
        smb_msg1,
        mockservicebus._receiver,
    )

    # *EVALUATE*: It should have scheduled the next task in the chain!
    assert len(mockservicebus._sender.method_calls) == 2
    publish_success_call2 = mockservicebus._sender.method_calls[1]
    assert publish_success_call2[0] == "schedule_messages"
    # We should always be able to deserialize the automatically-published task
    published_task2 = Task.model_validate_json(str(publish_success_call2[1][0]))
    smb_msg2 = make_message(published_task2, sequence_number=2)

    #  *EVALUATE*: We expect this one to fail! Number too big!
    await app.message_handler(smb_msg2, mockservicebus._receiver)

    # It should have scheduled the failure task now!
    assert len(mockservicebus._sender.method_calls) == 3
    publish_success_call3 = mockservicebus._sender.method_calls[2]
    assert publish_success_call3[0] == "schedule_messages"
    # This should be a failure task now
    published_task3 = Task.model_validate_json(str(publish_success_call3[1][0]))
    assert published_task3.function_name == "failing_task"
    smb_msg3 = make_message(published_task3, sequence_number=3)

    # *EVALUATE*: We can handle the failure task now
    await app.message_handler(smb_msg3, mockservicebus._receiver)

    # Our state has been updated correctly
    assert app.state.inner["sample_task_called"] == 2
    assert app.state.inner["sample_task_payload"] == [(1, 2), (5, 6)]
    assert "fail_count" in app.state.inner and app.state.inner["fail_count"] == 1


async def test_publish_graph_failures(app, mockservicebus, mock_storage):
    """Test that publishing a task graph with no storage raises an error."""
    with pytest.raises(BoilermakerAppException) as exc:
        await app.publish_graph(None)
        assert "TaskGraph workflows require a storage interface" in str(exc)

    # No tasks published
    mockservicebus._sender.assert_not_called()
    # Add a storage interface
    app.results_storage = mock_storage
    # This should fail because the graph is invalid
    graph = TaskGraph()
    graph.add_task(Task.si(sample_task, 1, 2))
    # Fails because this task does not include the graph_id!
    graph.children["non_existent_task"] = Task.si(sample_task, 3, 4)
    with pytest.raises(BoilermakerAppException) as exc:
        await app.publish_graph(graph)
        assert "All tasks must have graph_id matching graph" in str(exc)
    # No tasks published
    mockservicebus._sender.assert_not_called()

    # handle storage failures!
    graph = TaskGraph()
    t1 = Task.si(sample_task, 1, 2)
    t2 = Task.si(sample_task, 3, 4)
    graph.add_task(t1)
    graph.add_task(t2, parent_ids=[t1.task_id])
    mock_storage.store_graph.side_effect = BoilermakerStorageError("Storage failure!")
    with pytest.raises(BoilermakerAppException) as exc:
        await app.publish_graph(graph)
        assert "Failed to store TaskGraph" in str(exc)
    # No tasks published
    mockservicebus._sender.assert_not_called()


async def test_publish_graph_happy_path(app, mockservicebus, mock_storage):
    """Test that publishing a valid task graph works as expected."""
    app.results_storage = mock_storage
    graph = TaskGraph()
    t1 = Task.si(sample_task, 1, 2)
    t2 = Task.si(sample_task, 3, 4)
    graph.add_task(t1)
    graph.add_task(t2, parent_ids=[t1.task_id])

    result = await app.publish_graph(graph)
    # We get the graph back
    assert result is graph

    # One task published
    assert len(mockservicebus._sender.method_calls) == 1
    publish_success_call = mockservicebus._sender.method_calls[0]
    assert publish_success_call[0] == "schedule_messages"
    # Graph stored
    mock_storage.store_graph.assert_called_with(graph)

    # We should always be able to deserialize the task
    published_task = Task.model_validate_json(str(publish_success_call[1][0]))
    assert published_task.payload["args"] == [1, 2]
    assert published_task.payload["kwargs"] == {}
    assert published_task.function_name == "sample_task"
    assert published_task.on_success is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_publish_task_sets_sequence_number(app, mockservicebus):
    """Test that publish_task return  the sequence number after publishing."""

    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    # Simulate ServiceBus returning a sequence number
    mockservicebus._sender.send_message.return_value = [456]
    app.service_bus_client = mockservicebus._sender

    assert await app.publish_task(task) is task
    assert mockservicebus._sender.send_message.call_count == 1


async def test_publish_task_error_handling(app, mockservicebus):
    """Test that publish_task raises an error when publishing fails."""

    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    # Simulate ServiceBus raising an error
    mockservicebus._sender.send_message.side_effect = ServiceBusError(
        task.model_dump_json(), error=ValueError("bad message!")
    )
    app.service_bus_client = mockservicebus._sender

    with pytest.raises(BoilermakerAppException):
        await app.publish_task(task)


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# signal_handler method
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_signal_handler_abandons_message(app, mockservicebus, evaluator):
    """Test that signal_handler abandons the current message on SIGINT."""
    dummy_msg = MagicMock()
    dummy_msg.sequence_number = 123
    receiver = mockservicebus._receiver
    evaluator.task.msg = dummy_msg
    app._message_evaluators[123] = evaluator
    # Test inspired by anyio tests for `open_signal_receiver`
    async with create_task_group() as tg:
        tg.start_soon(app.signal_handler, tg.cancel_scope)
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    receiver.abandon_message.assert_awaited_with(dummy_msg)


async def test_signal_handler_abandons_message_with_error(app, mockservicebus, evaluator):
    """Test that signal_handler handles errors when abandoning a message."""
    dummy_msg = MagicMock()
    dummy_msg.sequence_number = 123
    evaluator.task.msg = dummy_msg
    receiver = mockservicebus._receiver
    receiver.abandon_message.side_effect = ServiceBusError("fail")
    evaluator.task.msg = dummy_msg
    app._message_evaluators[123] = evaluator
    # Test inspired by anyio tests for `open_signal_receiver`
    async with create_task_group() as tg:
        tg.start_soon(app.signal_handler, tg.cancel_scope)
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    receiver.abandon_message.assert_awaited_with(dummy_msg)


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# run method
# # # # # # # # # # # # # # # # # # # # # # # # # # #
class MockReceiver:
    def __init__(self, iter: Sequence[Any]):
        self._iter = iter
        self._index = 0
        self._completed_msgs = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._iter):
            raise StopAsyncIteration
        value = self._iter[self._index]
        self._index += 1
        return value


async def test_run_calls_message_handler(app, mockservicebus):
    """Test that run calls message_handler for each received message."""
    dummy_msgs = [
        MagicMock(sequence_number=321),
        MagicMock(side_effect=ValueError("bad message!")),
    ]
    # Make a generic receiver thing that can do a bunch of fake stuff
    receiver = MockReceiver(dummy_msgs)
    # Hack this in directly to skip the middle layer of ManagedServiceBusClient
    # make sure get_receiver returns an async context manager + receiver
    app.service_bus_client = MagicMock(**{"get_receiver.return_value": receiver})

    # Patch message_handler to track calls
    app.message_handler = AsyncMock()

    async def stop_loop():
        await asyncio.sleep(0.01)
        return to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    async with create_task_group() as tg:
        tg.start_soon(stop_loop)
        tg.start_soon(app.run)
        tg.cancel_scope.cancel()

    assert len(app.message_handler.call_args_list) == 2
    msgs = [call[0][0] for call in app.message_handler.call_args_list]
    assert msgs == dummy_msgs

    # Should never publish
    assert len(mockservicebus._sender.method_calls) == 0


async def test_run_handles_message_handler_exceptions(app, mockservicebus):
    """Test that run calls message_handler and doesn't let exceptions bubble up."""
    dummy_msgs = [MagicMock(sequence_number=321), MagicMock(sequence_number=654)]
    # Make a generic receiver thing that can do a bunch of fake stuff
    receiver = MockReceiver(dummy_msgs)
    # Hack this in directly to skip the middle layer of ManagedServiceBusClient
    # make sure get_receiver returns an async context manager + receiver
    app.service_bus_client = MagicMock(**{"get_receiver.return_value": receiver})

    # Patch message_handler to track calls
    app.message_handler = AsyncMock(side_effect=RuntimeError("Very bad exception"))

    async def stop_loop():
        await asyncio.sleep(0.05)
        return to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    async with create_task_group() as tg:
        tg.start_soon(stop_loop)
        # It called the thing and didn't bubble up the exception
        tg.start_soon(app.run)
        tg.cancel_scope.cancel()

    # It definitely called it twice
    assert len(app.message_handler.call_args_list) == 2


async def test_handler_garbage_message(app, mockservicebus):
    """Test that message_handler handles invalid JSON messages gracefully."""
    message_num = random.randint(100, 1000)
    # We are going to make a custom, totally garbage message
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[b"{{\\]]))"],  # Invalid JSON
        message_annotations={SEQUENCENUBMERNAME: message_num},
    )
    msg = ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)
    with patch("boilermaker.app.evaluator_factory") as mock_eval:
        # *EVALUATE*: We can handle the failure task now
        result = await app.message_handler(msg, mockservicebus._receiver)
        assert result is None
        mock_eval.assert_not_called()

    # Garbage Tasks should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    assert complete_msg_call[0] == "dead_letter_message"


async def test_close(app, mockservicebus):
    app.service_bus_client = AsyncMock()
    await app.close()
    app.service_bus_client.close.assert_called_once()
