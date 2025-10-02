import random

import pytest
from boilermaker import failure, retries
from boilermaker.evaluators import ResultsStorageTaskEvaluator
from boilermaker.task import Task, TaskResult, TaskStatus

# Requires running pytest with `--import-mode importlib`
from .helpers import verify_storage_started_and_get_result_calls


@pytest.fixture
def evaluator(app, mockservicebus, mock_storage, make_message):
    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    task = app.create_task(somefunc)
    task.payload["args"] = (21,)
    task.msg = make_message(task)

    return ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Constructor Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_constructor_requires_storage_interface(app, mockservicebus):
    """Test that ResultsStorageTaskEvaluator requires a storage interface."""

    async def somefunc(state, x):
        return x * 2

    app.register_async(somefunc)
    task = app.create_task(somefunc)

    with pytest.raises(ValueError) as exc:
        ResultsStorageTaskEvaluator(
            mockservicebus._receiver,
            task,
            app.publish_task,
            app.function_registry,
            state=app.state,
            storage_interface=None,
        )
    assert "Storage interface is required for ResultsStorageTaskEvaluator" in str(
        exc.value
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# message_handler generic Tests
# (Making sure parent-class expectations are not violated)
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_missing_function(evaluator, mock_storage):
    """Test that message_handler raises an error for missing functions."""
    # Create a task with a function name not in registry
    task = Task.default("not_registered")
    evaluator.task = task

    # Should not raise
    result = await evaluator()
    assert result.status == TaskStatus.Failure
    assert "Pre-processing expectation failed" in result.errors

    # Should not store any result when function is missing
    mock_storage.store_task_result.assert_not_called()


async def test_message_handler_stores_function_exception(
    evaluator, mock_storage, app, make_message
):
    """Test that message_handler handles function exceptions properly."""

    async def failing_func(state):
        raise RuntimeError("Something went wrong")

    app.register_async(failing_func)
    task = app.create_task(failing_func)
    task.msg = make_message(task)
    evaluator.task = task

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert result.result is None
    assert "Something went wrong" in result.formatted_exception

    # Should not store result when function raises exception
    # (storage happens in message_handler for exceptions)

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(mock_storage, task)
    assert stored_result.result is None
    assert stored_result.status == TaskStatus.Failure
    assert "Something went wrong" in stored_result.formatted_exception


async def test_message_handler_debug_task(evaluator, mock_storage, make_message):
    """Test that message_handler runs the debug task and DOES NOT store result."""
    from boilermaker import sample

    task = Task.default(sample.TASK_NAME)
    task.msg = make_message(task)
    evaluator.task = task

    result = await evaluator()
    # Should return whatever sample.debug_task returns
    assert result is not None

    # Verify debug task result was *not* stored
    mock_storage.store_task_result.assert_not_called()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_success(evaluator, mock_storage):
    """Test that message_handler executes a registered function, returns result, and stores it."""
    result = await evaluator.message_handler()
    assert result.result == 42
    assert result.status == TaskStatus.Success

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.result == 42
    assert stored_result.status == TaskStatus.Success
    assert stored_result.errors is None
    assert stored_result.formatted_exception is None


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success_with_storage(
    has_on_success, acks_late, app, mockservicebus, mock_storage, make_message
):
    """Test successful task execution with result storage and optional on_success callback."""

    async def oktask(state):
        return "OK"

    async def onsuccess(state, **kwargs):
        return 1

    # Register functions
    app.register_async(oktask, policy=retries.RetryPolicy.default())
    app.register_async(onsuccess, policy=retries.NoRetry())

    # Create task
    task = app.create_task(oktask)
    if has_on_success:
        task.on_success = app.create_task(onsuccess, somekwarg="akwargval")
    task.acks_late = acks_late

    # Create evaluator with storage
    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == "OK"
    assert result.status == TaskStatus.Success

    # Task should be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.status == TaskStatus.Success
    assert stored_result.result == "OK"
    assert stored_result.errors is None
    assert stored_result.formatted_exception is None

    if has_on_success:
        # Should publish success callback
        assert len(mockservicebus._sender.method_calls) == 1
        publish_success_call = mockservicebus._sender.method_calls[0]
        assert publish_success_call[0] == "schedule_messages"
        published_task = Task.model_validate_json(str(publish_success_call[1][0]))
        assert published_task.function_name == "onsuccess"
    else:
        # No callbacks published
        assert not mockservicebus._sender.method_calls


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_failure_with_storage(
    has_on_failure,
    should_deadletter,
    acks_late,
    app,
    mockservicebus,
    mock_storage,
    make_message,
):
    """Test task failure handling with result storage, deadlettering, and on_failure callback."""

    async def failtask(state, **kwargs):
        return failure.TaskFailureResult

    async def onfail(state, **kwargs):
        return 1

    # Register functions
    app.register_async(failtask, policy=retries.RetryPolicy.default())
    app.register_async(onfail, policy=retries.NoRetry())

    # Create task
    task = app.create_task(failtask)
    task.acks_late = acks_late
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        task.on_failure = app.create_task(onfail, somekwarg="akwargval")

    # Create evaluator with storage
    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.Failure
    assert result.result is None

    # Task should be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.status == TaskStatus.Failure
    assert stored_result.result is None

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


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_storage(
    can_retry,
    has_on_failure,
    should_deadletter,
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

    # Create evaluator with storage
    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert (
        result.status == TaskStatus.Retry if can_retry else TaskStatus.RetriesExhausted
    )
    assert result.result is None

    # Task should be settled
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[1][0].sequence_number == message_num

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )

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


async def test_retries_exhausted_with_storage(
    app, mockservicebus, mock_storage, make_message
):
    """Test that retries exhausted scenario stores failure result."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    # Set attempts to exceed max tries
    task.attempts.attempts = task.policy.max_tries + 1

    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.status == TaskStatus.RetriesExhausted
    assert result.result is None

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.status == TaskStatus.RetriesExhausted

    # Task should be deadlettered
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[0] == "dead_letter_message"


async def test_retry_policy_update_with_storage(
    app, mockservicebus, mock_storage, make_message
):
    """Test that retry policy can be updated during retry exception handling."""

    async def retrytask(state):
        new_policy = retries.RetryPolicy(
            max_tries=10, retry_mode=retries.RetryMode.Exponential
        )
        raise retries.RetryException("Retry with new policy", policy=new_policy)

    app.register_async(retrytask, policy=retries.RetryPolicy.default())
    task = app.create_task(retrytask)

    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

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
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.status == TaskStatus.Retry


async def test_early_acks_with_storage(app, mockservicebus, mock_storage, make_message):
    """Test that early acknowledgment works correctly with storage."""

    async def oktask(state):
        return "OK"

    app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = app.create_task(oktask)
    task.acks_late = False  # Early ack

    evaluator = ResultsStorageTaskEvaluator(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state=app.state,
        storage_interface=mock_storage,
    )

    message_num = random.randint(100, 1000)
    task.msg = make_message(task, sequence_number=message_num)

    result = await evaluator.message_handler()
    assert isinstance(result, TaskResult)
    assert result.result == "OK"
    assert result.status == TaskStatus.Success

    # Should complete message (early ack)
    assert len(mockservicebus._receiver.method_calls) == 1
    complete_msg_call = mockservicebus._receiver.method_calls[0]
    assert complete_msg_call[0] == "complete_message"

    # Verify task started was stored and  task_result was stored
    stored_result = verify_storage_started_and_get_result_calls(
        mock_storage, evaluator.task
    )
    assert stored_result.status == TaskStatus.Success
