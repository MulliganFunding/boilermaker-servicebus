import asyncio
import os
import random
import signal
from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from anyio import create_task_group, to_thread
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from azure.servicebus.exceptions import MessageLockLostError, ServiceBusError
from boilermaker import failure, retries
from boilermaker.app import Boilermaker, BoilermakerAppException
from boilermaker.task import Task


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
    return ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)


@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


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


async def test_app_register_many_async(app, mockservicebus):
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
        await app.message_handler(make_message(task), mockservicebus.get_queue_receiver())

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

    policy = retries.RetryPolicy(
        max_tries=2, delay=30, delay_max=600, retry_mode=retries.RetryMode.Linear
    )
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
    assert published_task.function_name== "somefunc"
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
        app.chain(
            Task.si(sample_task, 1), Task.si(sample_task, 2), on_failure="not_a_task"
        )
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


async def test_chain_publish_and_evaluate(app, mockservicebus):
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
    await app.message_handler(smb_msg1, mockservicebus._receiver)

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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# task_handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_handler_success(app):
    """Test that task_handler executes a registered function and returns the result."""
    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc, 21)
    result = await app.task_handler(task, sequence_number=42)
    assert result == 42


async def test_task_handler_missing_function(app):
    """Test that task_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")

    with pytest.raises(ValueError) as exc:
        await app.task_handler(task, sequence_number=99)
    assert "Missing registered function" in str(exc.value)


async def test_task_handler_debug_task(app):
    """Test that task_handler runs the debug task."""
    # Register the debug task name
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    result = await app.task_handler(task, sequence_number=123)
    # Should return whatever sample.debug_task returns
    # (for now, just check it runs without error)
    assert result is not None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_publish_task_sets_sequence_number(app, mockservicebus):
    """Test that publish_task sets the sequence number after publishing."""
    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    # Simulate ServiceBus returning a sequence number
    mockservicebus._sender.send_message.return_value = [456]
    app.service_bus_client = mockservicebus._sender

    published_task = await app.publish_task(task)
    assert published_task._sequence_number == 456


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
#
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
class DummyMsg:
    sequence_number = 789


async def test_complete_message(app, mockservicebus):
    """Test that complete_message settles a message and clears current message."""
    msg = DummyMsg()
    receiver = mockservicebus._receiver
    app._current_message = object()
    await app.complete_message(msg, receiver)
    # Check that complete_message was called with the correct message
    assert receiver.method_calls
    complete_call = receiver.method_calls[0]
    assert complete_call[0] == "complete_message"
    assert complete_call[1][0] is msg
    assert app._current_message is None


async def test_complete_message_with_error(app, mockservicebus):
    """Test that complete_message handles errors when settling a message."""
    msg = DummyMsg()
    app._current_message = object()
    receiver = mockservicebus._receiver
    receiver.complete_message.side_effect = ServiceBusError("fail")
    await app.complete_message(msg, receiver)
    # Check that complete_message was called with the correct message
    assert receiver.method_calls
    complete_call = receiver.method_calls[0]
    assert complete_call[0] == "complete_message"
    assert complete_call[1][0] is msg
    assert app._current_message is None


async def test_renew_message_lock(app, mockservicebus):
    """Test that renew_message_lock is called with the correct message."""
    msg = DummyMsg()
    receiver = mockservicebus._receiver
    app._current_message = msg
    app._receiver = receiver
    await app.renew_message_lock()
    # Check that renew_message_lock was called with the correct message
    assert receiver.method_calls
    renew_call = receiver.method_calls[0]
    assert renew_call[0] == "renew_message_lock"
    assert renew_call[1][0] is msg
    assert app._current_message is msg


async def test_renew_message_lock_errors(app, mockservicebus):
    """Test that renew_message_lock handles errors gracefully."""
    msg = DummyMsg()
    app._current_message = msg
    receiver = mockservicebus._receiver
    receiver.renew_message_lock.side_effect = MessageLockLostError()
    app._receiver = receiver
    await app.renew_message_lock()
    # Check that renew_message_lock was called with the correct message
    assert receiver.method_calls
    renew_call = receiver.method_calls[0]
    assert renew_call[0] == "renew_message_lock"
    assert renew_call[1][0] is msg
    assert app._current_message is msg


async def test_renew_message_lock_missing(app, mockservicebus):
    """Test that renew_message_lock handles missing receiver or message gracefully."""

    msg = DummyMsg()
    app._current_message = msg
    app._receiver = None

    # Should log warnings but not raise
    await app.renew_message_lock()
    assert not mockservicebus._receiver.method_calls

    receiver = mockservicebus._receiver
    app._current_message = None
    app._receiver = receiver
    await app.renew_message_lock()
    assert not mockservicebus._receiver.method_calls


async def test_task_garbage_message(app, mockservicebus):
    """Test that message_handler handles invalid JSON messages gracefully."""
    message_num = random.randint(100, 1000)
    # We are going to make a custom, totally garbage message
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[b"{{\\]]))"], # Invalid JSON
        message_annotations={SEQUENCENUBMERNAME: message_num},
    )
    msg = ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)

    # Now we can run the task
    result = await app.message_handler(
        msg, mockservicebus.get_queue_receiver()
    )
    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# signal_handler method
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_signal_handler_abandons_message(app, mockservicebus):
    """Test that signal_handler abandons the current message on SIGINT."""
    dummy_msg = MagicMock()
    dummy_msg.sequence_number = 123
    receiver = mockservicebus._receiver
    app._current_message = dummy_msg
    # Test inspired by anyio tests for `open_signal_receiver`
    async with create_task_group() as tg:
        tg.start_soon(app.signal_handler, receiver, tg.cancel_scope)
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    receiver.abandon_message.assert_awaited_with(dummy_msg)
    assert app._current_message is None


async def test_signal_handler_abandons_message_with_error(app, mockservicebus):
    """Test that signal_handler handles errors when abandoning a message."""
    dummy_msg = MagicMock()
    dummy_msg.sequence_number = 123
    receiver = mockservicebus._receiver
    receiver.abandon_message.side_effect = ServiceBusError("fail")
    app._current_message = dummy_msg
    # Test inspired by anyio tests for `open_signal_receiver`
    async with create_task_group() as tg:
        tg.start_soon(app.signal_handler, receiver, tg.cancel_scope)
        await to_thread.run_sync(os.kill, os.getpid(), signal.SIGINT)

    receiver.abandon_message.assert_awaited_with(dummy_msg)
    assert app._current_message is None


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
    dummy_msgs = [MagicMock(sequence_number=321), MagicMock(sequence_number=654)]
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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler with on_success
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success(has_on_success, acks_late, app, mockservicebus):
    """Test successful task execution and optional on_success callback."""
    async def oktask(state):
        return "OK"

    async def onsuccess(state, **kwargs):
        return 1

    # Function must be registered first
    app.register_async(oktask, policy=retries.RetryPolicy.default())
    app.register_async(onsuccess, policy=retries.NoRetry())
    # Now we can create a task out of it
    task = app.create_task(oktask)
    if has_on_success:
        task.on_success = app.create_task(onsuccess, somekwarg="akwargval")
    task.acks_late = acks_late

    # Now we can run the task
    message_num = random.randint(100, 1000)
    result = await app.message_handler(
        make_message(task, sequence_number=message_num), mockservicebus.get_queue_receiver()
    )
    assert result is None
    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    if has_on_success:
        # Publish new task with sender
        assert len(mockservicebus._sender.method_calls) == 1
        publish_success_call = mockservicebus._sender.method_calls[0]
        assert publish_success_call[0] == "schedule_messages"
        # We should always be able to deserialize the task
        published_task = Task.model_validate_json(str(publish_success_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {"somekwarg": "akwargval"}
        assert published_task.function_name == "onsuccess"
        assert published_task.on_success is None
        assert published_task.on_failure is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler with on_failure
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_failure(has_on_failure, should_deadletter, acks_late, app, mockservicebus):
    """Test task failure handling, deadlettering, and on_failure callback."""
    async def failtask(state, **kwargs):
        return failure.TaskFailureResult

    async def onfail(state, **kwargs):
        return 1

    # Function must be registered first -> this function has a retry policy: it should never be retried
    app.register_async(failtask, policy=retries.RetryPolicy.default())
    app.register_async(onfail, policy=retries.NoRetry())
    # Now we can create a task out of it
    task = app.create_task(failtask)
    task.acks_late = acks_late
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        # Set a failure task
        task.on_failure = app.create_task(onfail, somekwarg="akwargval")

    # Now we can run the task
    message_num = random.randint(100, 1000)
    result = await app.message_handler(
        make_message(task, sequence_number=message_num), mockservicebus.get_queue_receiver()
    )

    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    if has_on_failure:
        # Publish new task with sender
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        # We should always be able to deserialize the task
        published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {"somekwarg": "akwargval"}
        assert published_task.function_name == "onfail"
        assert published_task.on_success is None
        assert published_task.on_failure is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    if should_deadletter and acks_late:
        assert complete_msg_call[0] == "dead_letter_message"
    elif not acks_late:
        assert complete_msg_call[0] == "complete_message"
        assert complete_msg_call[1][0].sequence_number == message_num
    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with on_failure
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_onfail(can_retry, has_on_failure, should_deadletter, app, mockservicebus):
    """Test retry logic and on_failure callback for tasks that raise RetryException."""
    async def retrytask(state):
        raise retries.RetryException("Retry me")

    async def onfail(state, **kwargs):
        return 1

    # Function must be registered first
    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    app.register_async(onfail, policy=retries.NoRetry())
    # Now we can create a task out of it
    task = app.create_task(retrytask)
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        # Set a failure task
        task.on_failure = app.create_task(onfail, somekwarg="akwargval")

    if not can_retry:
        task.attempts.attempts = task.policy.max_tries + 1

    # Now we can run the task
    message_num = random.randint(100, 1000)
    result = await app.message_handler(
        make_message(task, sequence_number=message_num), mockservicebus.get_queue_receiver()
    )

    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num

    if has_on_failure and not can_retry:
        # Publish onfail task with sender
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {"somekwarg": "akwargval"}
        assert published_task.function_name == "onfail"
        assert published_task.on_success is None
        assert published_task.on_failure is None
    elif not has_on_failure and can_retry:
        # Publish retry task with sender
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {}
        assert published_task.function_name == "retrytask"
        assert published_task.on_success is None
        assert published_task.on_failure is None
    elif has_on_failure and can_retry:
        # Publish retry task with onfail task
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {}
        assert published_task.function_name == "retrytask"
        assert published_task.on_success is None
        assert published_task.on_failure is not None
        assert published_task.on_failure.function_name == "onfail"
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    if should_deadletter and not can_retry:
        assert complete_msg_call[0] == "dead_letter_message"
    elif not can_retry:
        assert complete_msg_call[0] == "complete_message"
        assert complete_msg_call[1][0].sequence_number == message_num
    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with new retry policy
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_retries_with_new_policy(app, mockservicebus):
    """Test retry logic with a new policy from RetryExceptionDefaultExponential."""
    async def retrytask(state):
        # NEW POLICY!
        raise retries.RetryExceptionDefaultExponential(
            "Retry me", delay=800, max_delay=2000, max_tries=99
        )

    # Function must be registered first
    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    # Now we can create a task out of it
    task = app.create_task(retrytask)
    # Now we can run the task
    result = await app.message_handler(
        make_message(task), mockservicebus.get_queue_receiver()
    )

    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1

    # Published retry task with sender: published with a different policy!
    assert len(mockservicebus._sender.method_calls) == 1
    publish_fail_call = mockservicebus._sender.method_calls[0]
    # Should have used a very large delay based on thrown RetryException
    assert publish_fail_call[0] == "schedule_messages"
    published_task = Task.model_validate_json(str(publish_fail_call[1][0]))
    assert published_task.payload["args"] == []
    assert published_task.payload["kwargs"] == {}
    assert published_task.function_name == "retrytask"
    assert published_task.policy.retry_mode == retries.RetryMode.Exponential
    assert published_task.policy.max_tries == 99
    assert published_task.policy.delay > retries.RetryPolicy.default().delay
    assert published_task.on_success is None
    assert published_task.on_failure is None

    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with acks_late/acks_early
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_acks_late(can_retry, should_deadletter, acks_late, app, mockservicebus):
    """Test retry logic and message settlement for acks_late and deadletter scenarios."""
    async def retrytask(state):
        raise retries.RetryException("Retry me")

    # Function must be registered first
    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    # Now we can create a task out of it
    task = app.create_task(retrytask)
    task.acks_late = acks_late
    task.should_dead_letter = should_deadletter
    if not can_retry:
        # Make it fail early
        task.attempts.attempts = task.policy.max_tries + 1

    # Now we can run the task
    message_num = random.randint(100, 1000)
    result = await app.message_handler(
        make_message(task, sequence_number=message_num), mockservicebus.get_queue_receiver()
    )

    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    if should_deadletter and not acks_late and not can_retry:
        # We should have received the message again
        assert len(mockservicebus._sender.method_calls) == 0
        # acks_early => complete_message (before deadletter)
        assert complete_msg_call[0] == "complete_message"
    elif should_deadletter and acks_late and not can_retry:
        # Publish onfail task with sender
        assert len(mockservicebus._sender.method_calls) == 0
        assert complete_msg_call[0] == "dead_letter_message"
    elif can_retry:
        # Publish retry task with onfail task
        assert len(mockservicebus._sender.method_calls) == 1
        publish_call = mockservicebus._sender.method_calls[0]
        assert publish_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_call[1][0]))
        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {}
        assert published_task.function_name == "retrytask"
        assert published_task.on_success is None
        assert published_task.on_failure is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task-handling with exceptions
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_handle_exception(has_on_failure, should_deadletter, acks_late, app, mockservicebus):
    """Test exception handling, deadlettering, and on_failure callback for tasks."""
    async def except_task(state):
        raise FloatingPointError("Some weird error happened")

    async def onfail(state, **kwargs):
        return 1

    # Function must be registered first
    app.register_async(except_task, policy=retries.RetryPolicy.default())
    app.register_async(onfail, policy=retries.NoRetry())
    # Now we can create a task out of it
    task = app.create_task(except_task)
    task.acks_late = acks_late
    task.should_dead_letter = should_deadletter

    if has_on_failure:
        # Set a failure task
        task.on_failure = app.create_task(onfail, somekwarg="akwargval")

    # Now we can run the task
    message_num = random.randint(100, 1000)
    result = await app.message_handler(
        make_message(task, sequence_number=message_num), mockservicebus.get_queue_receiver()
    )

    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    if not has_on_failure:
        assert len(mockservicebus._sender.method_calls) == 0
        if should_deadletter and not acks_late:
            # We should have received the message again
            # acks_early => complete_message (before deadletter)
            assert complete_msg_call[0] == "complete_message"
        elif should_deadletter and acks_late:
            # Publish onfail task with sender
            assert len(mockservicebus._sender.method_calls) == 0
            assert complete_msg_call[0] == "dead_letter_message"
    else:
        # Publish retry task with onfail task
        assert len(mockservicebus._sender.method_calls) == 1
        publish_call = mockservicebus._sender.method_calls[0]
        assert publish_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_call[1][0]))

        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {'somekwarg': 'akwargval'}
        assert published_task.function_name == "onfail"
        assert published_task.on_success is None
        assert published_task.on_failure is None

    assert result is None
