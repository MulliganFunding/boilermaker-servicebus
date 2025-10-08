import random
from unittest.mock import AsyncMock

import pytest
from boilermaker import exc, failure, retries
from boilermaker.evaluators import NoStorageEvaluator
from boilermaker.task import Task, TaskResult, TaskStatus


@pytest.fixture
def evaluator(app, mockservicebus, make_message):
    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    task.payload["args"] = (21,)
    task.msg = make_message(task)

    return NoStorageEvaluator(
        mockservicebus._receiver, task, app.publish_task, app.function_registry, state=app.state
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_handler_success(evaluator):
    """Test that task_handler executes a registered function and returns the result."""

    result = await evaluator.message_handler()
    assert result.status == TaskStatus.Success
    assert result.result == 42


async def test_task_handler_missing_function(evaluator):
    """Test that task_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")
    evaluator.task = task

    # Should not raise
    result = await evaluator()
    assert result.status == TaskStatus.Failure
    assert "Pre-processing expectation failed" in result.errors


async def test_task_handler_debug_task(evaluator):
    """Test that task_handler runs the debug task."""
    # Register the debug task name
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    evaluator.task = task
    assert await evaluator() is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler with on_success
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success(
    has_on_success, acks_late, app, mockservicebus, evaluator, make_message
):
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
    task.msg = make_message(task, sequence_number=message_num)
    evaluator.task = task

    result = await evaluator()
    # Check result from evaluating the task
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

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
async def test_task_failure(
    has_on_failure,
    should_deadletter,
    acks_late,
    app,
    mockservicebus,
    evaluator,
    make_message,
):
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
    task.msg = make_message(task, sequence_number=message_num)
    evaluator.task = task
    result = await evaluator()
    # Check result from evaluating the task
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert result.result is None

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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with on_failure
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_onfail(
    can_retry,
    has_on_failure,
    should_deadletter,
    app,
    mockservicebus,
    evaluator,
    make_message,
):
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
    task.msg = make_message(task, sequence_number=message_num)
    evaluator.task = task
    result = await evaluator.message_handler()
    # Check result from evaluating the task
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.RetriesExhausted if not can_retry else TaskStatus.Retry

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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with new retry policy
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_task_retries_with_new_policy(
    app, mockservicebus, evaluator, make_message
):
    """Test retry logic with a new policy from RetryExceptionDefaultExponential."""

    async def retrytask(state):
        # NEW POLICY!
        raise retries.RetryExceptionDefaultExponential("Retry me", delay=800, max_delay=2000, max_tries=99)

    # Function must be registered first
    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    # Now we can create a task out of it
    task = app.create_task(retrytask)
    task.msg = make_message(task)
    evaluator.task = task
    # Now we can run the task
    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Retry

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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task retries with acks_late/acks_early
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_acks_late(
    can_retry,
    should_deadletter,
    acks_late,
    app,
    mockservicebus,
    evaluator,
    make_message,
):
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
    task.msg = make_message(task, sequence_number=message_num)
    evaluator.task = task
    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.RetriesExhausted if not can_retry else TaskStatus.Retry

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


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# test task-handling with exceptions
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_handle_exception(
    has_on_failure,
    should_deadletter,
    acks_late,
    app,
    mockservicebus,
    evaluator,
    make_message,
):
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
    task.msg = make_message(task, sequence_number=message_num)
    evaluator.task = task
    result = await evaluator.message_handler()

    # Check result from evaluating the task
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert result.errors
    assert "FloatingPointError" in result.formatted_exception

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
        assert published_task.payload["kwargs"] == {"somekwarg": "akwargval"}
        assert published_task.function_name == "onfail"
        assert published_task.on_success is None
        assert published_task.on_failure is None


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Exception handling tests for uncovered lines
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_early_ack_task_lease_lost_exception(app, make_message, evaluator):
    """Test BoilermakerTaskLeaseLost exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.msg = make_message(task)
    evaluator.task = task

    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return None when lease is lost during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]
    evaluator.complete_message.assert_called_once()


async def test_early_ack_service_bus_error_exception(app, make_message, evaluator):
    """Test BoilermakerServiceBusError exception during early message acknowledgment."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Enable early acks
    task.msg = make_message(task)
    evaluator.task = task

    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return None when service bus error occurs during early ack
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    evaluator.complete_message.assert_called_once()


async def test_retries_exhausted_task_lease_lost_exception(
    app, make_message, evaluator
):
    """Test BoilermakerTaskLeaseLost exception when settling message for exhausted retries."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Don't settle early
    # Make retries exhausted
    task.attempts.attempts = task.policy.max_tries + 1
    task.msg = make_message(task)
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "Lost message lease" in result.errors[0]

    # Should return None when lease is lost during exhausted retries settlement
    evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )


async def test_retries_exhausted_service_bus_error_exception(
    app, make_message, evaluator
):
    """Test BoilermakerServiceBusError exception when settling message for exhausted retries."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Don't settle early
    # Make retries exhausted
    task.attempts.attempts = task.policy.max_tries + 1
    task.msg = make_message(task)
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerServiceBusError
    evaluator.deadletter_or_complete_task = AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("Service bus error")
    )

    result = await evaluator.message_handler()

    # Should return None when service bus error occurs during exhausted retries settlement
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert "ServiceBus error" in result.errors[0]
    evaluator.deadletter_or_complete_task.assert_called_once_with(
        "ProcessingError", detail="Retries exhausted"
    )


async def test_late_settlement_task_lease_lost_exception_success(
    app, make_message, evaluator
):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.msg = make_message(task)
    evaluator.task = task

    # Mock complete_message to raise BoilermakerTaskLeaseLost
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator.complete_message.assert_called_once()


async def test_late_settlement_task_lease_lost_exception_failure(
    app, make_message, evaluator
):
    """Test BoilermakerTaskLeaseLost exception during late message settlement for failed task."""

    async def failtask(state):
        return failure.TaskFailureResult

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
    task.msg = make_message(task)
    evaluator.task = task

    # Mock deadletter_or_complete_task to raise BoilermakerTaskLeaseLost
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("Lease lost"))

    result = await evaluator.message_handler()

    # Should return the task result even when lease is lost during late settlement
    assert result is not None
    assert result.status == TaskStatus.Failure
    evaluator.deadletter_or_complete_task.assert_called_once_with("TaskFailed")


async def test_late_settlement_service_bus_error_exception_success(
    app, make_message, evaluator
):
    """Test BoilermakerServiceBusError exception during late message settlement for successful task."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = True  # Enable late settlement
    task.msg = make_message(task)
    evaluator.task = task

    # Mock complete_message to raise BoilermakerServiceBusError
    evaluator.complete_message = AsyncMock(side_effect=exc.BoilermakerServiceBusError("Service bus error"))

    result = await evaluator.message_handler()

    # Should return the task result even when service bus error occurs during late settlement
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"
    evaluator.complete_message.assert_called_once()


async def test_late_settlement_service_bus_error_exception_failure(
    app, make_message, evaluator
):
    """Test BoilermakerServiceBusError exception during late message settlement for failed task."""

    async def failtask(state):
        return failure.TaskFailureResult

    app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = app.create_task(failtask)
    task.acks_late = True  # Enable late settlement
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
