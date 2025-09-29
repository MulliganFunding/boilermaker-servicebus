import random

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from azure.servicebus.exceptions import MessageLockLostError, ServiceBusError
from boilermaker import failure, retries
from boilermaker.app import Boilermaker
from boilermaker.evaluators import BaseTaskEvaluator
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


@pytest.fixture
def evaluator(app, mockservicebus):
    return BaseTaskEvaluator(
        mockservicebus._receiver, app.publish_task, app.function_registry
    )



# # # # # # # # # # # # # # # # # # # # # # # # # # #
# task_handler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_handler_success(app, evaluator):
    """Test that task_handler executes a registered function and returns the result."""
    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc, 21)
    result = await evaluator.task_handler(task, sequence_number=42)
    assert result == 42


async def test_task_handler_missing_function(app, evaluator):
    """Test that task_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")

    with pytest.raises(ValueError) as exc:
        await evaluator.task_handler(task, sequence_number=99)
    assert "Missing registered function" in str(exc.value)


async def test_task_handler_debug_task(app, evaluator):
    """Test that task_handler runs the debug task."""
    # Register the debug task name
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    result = await evaluator.task_handler(task, sequence_number=123)
    # Should return whatever sample.debug_task returns
    # (for now, just check it runs without error)
    assert result is not None



# # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
class DummyMsg:
    sequence_number = 789


async def test_complete_message(evaluator, mockservicebus):
    """Test that complete_message settles a message and clears current message."""
    msg = DummyMsg()
    evaluator._current_message = object()
    await evaluator.complete_message(msg)
    # Check that complete_message was called with the correct message
    assert mockservicebus._receiver.method_calls
    complete_call = mockservicebus._receiver.method_calls[0]
    assert complete_call[0] == "complete_message"
    assert complete_call[1][0] is msg
    assert evaluator._current_message is None


async def test_complete_message_with_error(evaluator, mockservicebus):
    """Test that complete_message handles errors when settling a message."""
    msg = DummyMsg()
    evaluator._current_message = object()
    receiver = mockservicebus._receiver
    receiver.complete_message.side_effect = ServiceBusError("fail")
    await evaluator.complete_message(msg)
    # Check that complete_message was called with the correct message
    assert receiver.method_calls
    complete_call = receiver.method_calls[0]
    assert complete_call[0] == "complete_message"
    assert complete_call[1][0] is msg
    assert evaluator._current_message is None


async def test_renew_message_lock(evaluator, mockservicebus):
    """Test that renew_message_lock is called with the correct message."""
    msg = DummyMsg()
    receiver = mockservicebus._receiver
    evaluator._current_message = msg
    evaluator._receiver = receiver
    await evaluator.renew_message_lock()
    # Check that renew_message_lock was called with the correct message
    assert receiver.method_calls
    renew_call = receiver.method_calls[0]
    assert renew_call[0] == "renew_message_lock"
    assert renew_call[1][0] is msg
    assert evaluator._current_message is msg


async def test_renew_message_lock_errors(evaluator, mockservicebus):
    """Test that renew_message_lock handles errors gracefully."""
    msg = DummyMsg()
    evaluator._current_message = msg
    receiver = mockservicebus._receiver
    receiver.renew_message_lock.side_effect = MessageLockLostError()
    evaluator._receiver = receiver
    await evaluator.renew_message_lock()
    # Check that renew_message_lock was called with the correct message
    assert receiver.method_calls
    renew_call = receiver.method_calls[0]
    assert renew_call[0] == "renew_message_lock"
    assert renew_call[1][0] is msg
    assert evaluator._current_message is msg


async def test_renew_message_lock_missing(evaluator, mockservicebus):
    """Test that renew_message_lock handles missing receiver or message gracefully."""

    msg = DummyMsg()
    evaluator._current_message = msg
    evaluator._receiver = None

    # Should log warnings but not raise
    await evaluator.renew_message_lock()
    assert not mockservicebus._receiver.method_calls

    receiver = mockservicebus._receiver
    evaluator._current_message = None
    evaluator._receiver = receiver
    await evaluator.renew_message_lock()
    assert not mockservicebus._receiver.method_calls


async def test_task_garbage_message(evaluator, mockservicebus):
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
    result = await evaluator.message_handler(msg)
    assert result is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler with on_success
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success(has_on_success, acks_late, app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(make_message(task, sequence_number=message_num))
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
async def test_task_failure(has_on_failure, should_deadletter, acks_late, app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(
        make_message(task, sequence_number=message_num)
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
async def test_task_retries_with_onfail(can_retry, has_on_failure, should_deadletter, app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(
        make_message(task, sequence_number=message_num)
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
async def test_task_retries_with_new_policy(app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(make_message(task))

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
async def test_task_retries_acks_late(can_retry, should_deadletter, acks_late, app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(
        make_message(task, sequence_number=message_num)
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
async def test_task_handle_exception(has_on_failure, should_deadletter, acks_late, app, mockservicebus, evaluator):
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
    result = await evaluator.message_handler(
        make_message(task, sequence_number=message_num)
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
