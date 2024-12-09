import json
import random

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from boilermaker import failure, retries
from boilermaker.app import Boilermaker


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]

DEFAULT_STATE = State({"somekey": "somevalue"})



@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


def test_app_state(app):
    assert app.state == DEFAULT_STATE


async def test_task_decorator(app):
    @app.task()
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__

    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_task_decorator_with_policy(app):
    @app.task(policy=retries.RetryPolicy.default())
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert app.task_registry[somefunc.__name__].policy == retries.RetryPolicy.default()
    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_app_register_async(app):
    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert await somefunc(DEFAULT_STATE) == "somevalue"

async def test_create_task(app):
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


def make_message(task, sequence_number: int = 123):
    # Example taken from:
    # azure-sdk-for-python/blob/main/sdk/servicebus/azure-servicebus/tests/test_message.py#L233
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[task.model_dump_json().encode("utf-8")],
        message_annotations={SEQUENCENUBMERNAME: sequence_number},
    )
    return ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)


async def test_task_garbage_message(app, mockservicebus):
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
    # Task should *always* be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num
    # Should never publish
    assert len(mockservicebus._sender.method_calls) == 0


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success(has_on_success, acks_late, app, mockservicebus):
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
        data = json.loads(str(publish_success_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {"somekwarg": "akwargval"}
        assert data["function_name"] == "onsuccess"
        assert data["on_success"] is None
        assert data["on_failure"] is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_failure(has_on_failure, should_deadletter, acks_late, app, mockservicebus):
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
        data = json.loads(str(publish_fail_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {"somekwarg": "akwargval"}
        assert data["function_name"] == "onfail"
        assert data["on_success"] is None
        assert data["on_failure"] is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    if should_deadletter and acks_late:
        assert complete_msg_call[0] == "dead_letter_message"
    elif not acks_late:
        assert complete_msg_call[0] == "complete_message"
        assert complete_msg_call[1][0].sequence_number == message_num
    assert result is None


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_onfail(can_retry, has_on_failure, should_deadletter, app, mockservicebus):
    async def retrytask(state):
        raise retries.RetryException("Retry me", policy=retries.RetryPolicy.default())

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
        data = json.loads(str(publish_fail_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {"somekwarg": "akwargval"}
        assert data["function_name"] == "onfail"
        assert data["on_success"] is None
        assert data["on_failure"] is None
    elif not has_on_failure and can_retry:
        # Publish retry task with sender
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        data = json.loads(str(publish_fail_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {}
        assert data["function_name"] == "retrytask"
        assert data["on_success"] is None
        assert data["on_failure"] is None
    elif has_on_failure and can_retry:
        # Publish retry task with onfail task
        assert len(mockservicebus._sender.method_calls) == 1
        publish_fail_call = mockservicebus._sender.method_calls[0]
        assert publish_fail_call[0] == "schedule_messages"
        data = json.loads(str(publish_fail_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {}
        assert data["function_name"] == "retrytask"
        assert data["on_success"] is None
        assert data["on_failure"] is not None
        assert data["on_failure"]["function_name"] == "onfail"
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    if should_deadletter and not can_retry:
        assert complete_msg_call[0] == "dead_letter_message"
    elif not can_retry:
        assert complete_msg_call[0] == "complete_message"
        assert complete_msg_call[1][0].sequence_number == message_num
    assert result is None


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_acks_late(can_retry, should_deadletter, acks_late, app, mockservicebus):
    async def retrytask(state):
        raise retries.RetryException("Retry me", policy=retries.RetryPolicy.default())

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
        data = json.loads(str(publish_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {}
        assert data["function_name"] == "retrytask"
        assert data["on_success"] is None
        assert data["on_failure"] is None
    else:
        # No new tasks published
        assert not mockservicebus._sender.method_calls

    assert result is None


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_handle_exception(has_on_failure, should_deadletter, acks_late, app, mockservicebus):
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
        data = json.loads(str(publish_call[1][0]))
        assert data["payload"]["args"] == []
        assert data["payload"]["kwargs"] == {'somekwarg': 'akwargval'}
        assert data["function_name"] == "onfail"
        assert data["on_success"] is None
        assert data["on_failure"] is None

    assert result is None
