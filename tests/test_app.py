import random

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from boilermaker import failure, retries
from boilermaker.app import Boilermaker
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


async def test_app_register_many_async(app, mockservicebus):
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


async def test_create_task_with_policy(app):
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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #

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


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_onfail(can_retry, has_on_failure, should_deadletter, app, mockservicebus):
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


async def test_task_retries_with_new_policy(app, mockservicebus):
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



@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_acks_late(can_retry, should_deadletter, acks_late, app, mockservicebus):
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
        published_task = Task.model_validate_json(str(publish_call[1][0]))

        assert published_task.payload["args"] == []
        assert published_task.payload["kwargs"] == {'somekwarg': 'akwargval'}
        assert published_task.function_name == "onfail"
        assert published_task.on_success is None
        assert published_task.on_failure is None

    assert result is None
