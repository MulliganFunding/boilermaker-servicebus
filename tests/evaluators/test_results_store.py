from unittest import mock
from unittest.mock import AsyncMock

import pytest
from boilermaker import exc, failure, retries, sample
from boilermaker.evaluators import ResultsStorageTaskEvaluator
from boilermaker.task import Task, TaskResult, TaskStatus


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
    assert "Storage interface is required for ResultsStorageTaskEvaluator" in str(exc.value)


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


async def test_message_handler_stores_function_exception(store_evaluator_context):
    """Test that message_handler handles function exceptions properly."""

    async def failing_func(state):
        raise RuntimeError("Something went wrong")

    store_evaluator_context.app.register_async(failing_func)
    task = store_evaluator_context.app.create_task(failing_func)
    task.msg = store_evaluator_context.make_message(task)
    store_evaluator_context.evaluator.task = task

    async with store_evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Failure,
        check_graph_loaded=False,
    ) as ctx:
        assert "Something went wrong" in ctx.current_task_result.formatted_exception


async def test_message_handler_debug_task(evaluator, mock_storage, make_message):
    """Test that message_handler runs the debug task and DOES NOT store result."""
    task = Task.default(sample.TASK_NAME)
    task.msg = make_message(task)
    evaluator.task = task

    result = await evaluator()
    # Should return whatever sample.debug_task returns
    assert result is None

    # Verify debug task result was *not* stored
    mock_storage.store_task_result.assert_not_called()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message Handling Logic Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
async def test_message_handler_success(store_evaluator_context):
    """Test that message_handler executes a registered function, returns result, and stores it."""
    async with store_evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
        check_graph_loaded=False,
    ):
        pass


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("has_on_success", [True, False])
async def test_task_success_with_storage(has_on_success, acks_late, store_evaluator_context):
    """Test successful task execution with result storage and optional on_success callback."""

    async def oktask(state):
        return "OK"

    async def onsuccess(state, **kwargs):
        return 1

    # Register functions
    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    store_evaluator_context.app.register_async(onsuccess, policy=retries.NoRetry())
    # Create task
    task = store_evaluator_context.app.create_task(oktask)
    if has_on_success:
        task.on_success = store_evaluator_context.app.create_task(onsuccess, somekwarg="akwargval")
    task.acks_late = acks_late

    store_evaluator_context.current_task = task
    async with store_evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
        check_graph_loaded=False,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()
        if has_on_success:
            assert len(scheduled) == 1
            callback = scheduled[0].task
            assert callback.function_name == "onsuccess"
        else:
            # No callbacks published
            assert not scheduled


@pytest.mark.parametrize("acks_late", [True, False])
@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
async def test_task_failure_with_storage(
    has_on_failure,
    should_deadletter,
    acks_late,
    store_evaluator_context,
):
    """Test task failure handling with result storage, deadlettering, and on_failure callback."""

    async def failtask(state, **kwargs):
        return failure.TaskFailureResult

    async def onfail(state, **kwargs):
        return 1

    # Register functions
    store_evaluator_context.app.register_async(failtask, policy=retries.RetryPolicy.default())
    store_evaluator_context.app.register_async(onfail, policy=retries.NoRetry())

    # Create task
    task = store_evaluator_context.app.create_task(failtask)
    task.acks_late = acks_late
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        task.on_failure = store_evaluator_context.app.create_task(onfail, somekwarg="akwargval")

    store_evaluator_context.current_task = task

    async with store_evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Failure,
        check_graph_loaded=False,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()
        if has_on_failure:
            assert len(scheduled) == 1
            callback = scheduled[0].task
            assert callback.function_name == "onfail"
        else:
            # No callbacks published
            assert not scheduled


@pytest.mark.parametrize("should_deadletter", [True, False])
@pytest.mark.parametrize("has_on_failure", [True, False])
@pytest.mark.parametrize("can_retry", [True, False])
async def test_task_retries_with_storage(
    can_retry,
    has_on_failure,
    should_deadletter,
    store_evaluator_context,
):
    """Test retry logic with result storage and on_failure callback."""

    async def retrytask(state):
        raise retries.RetryException("Retry me")

    async def onfail(state, **kwargs):
        return 1

    # Register functions
    store_evaluator_context.app.register_async(retrytask, policy=retries.RetryPolicy.default())
    store_evaluator_context.app.register_async(onfail, policy=retries.NoRetry())

    # Create task
    task = store_evaluator_context.app.create_task(retrytask)
    task.should_dead_letter = should_deadletter
    if has_on_failure:
        task.on_failure = store_evaluator_context.app.create_task(onfail, somekwarg="akwargval")

    if not can_retry:
        task.attempts.attempts = task.policy.max_tries + 1

    store_evaluator_context.current_task = task

    async with store_evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Retry if can_retry else TaskStatus.RetriesExhausted,
        check_graph_loaded=False,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()

        if not can_retry:
            if has_on_failure:
                assert len(scheduled) == 1
                callback = scheduled[0].task
                assert callback.function_name == "onfail"
            else:
                # No callbacks published
                assert not scheduled
        else:
            assert len(scheduled) == 1
            callback = scheduled[0].task
            assert callback.function_name == "retrytask"


@pytest.mark.parametrize("acks_late", [True, False])
async def test_retries_exhausted_with_storage(acks_late, store_evaluator_context):
    """Test that retries exhausted scenario stores failure result."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    task.acks_late = acks_late
    # Set attempts to exceed max tries
    task.attempts.attempts = task.policy.max_tries + 1
    store_evaluator_context.current_task = task

    async with store_evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.RetriesExhausted,
        check_graph_loaded=False,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()
        # No callbacks published
        assert not scheduled
        ctx.assert_msg_settled()
        if acks_late:
            ctx.assert_msg_dead_lettered()


@pytest.mark.parametrize("acks_late", [True, False])
async def test_retry_policy_update_with_storage(acks_late, store_evaluator_context):
    """Test that retry policy can be updated during retry exception handling."""

    async def retrytask(state):
        new_policy = retries.RetryPolicy(max_tries=10, retry_mode=retries.RetryMode.Exponential)
        raise retries.RetryException("Retry with new policy", policy=new_policy)

    store_evaluator_context.app.register_async(retrytask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(retrytask)
    task.acks_late = acks_late
    store_evaluator_context.current_task = task
    async with store_evaluator_context.with_regular_assertions(
        compare_result=None,
        compare_status=TaskStatus.Retry,
        check_graph_loaded=False,
    ) as ctx:
        scheduled = ctx.get_scheduled_messages()
        assert len(scheduled) == 1
        callback = scheduled[0].task
        assert callback.function_name == "retrytask"
        assert callback.policy.max_tries == 10


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Exception Handling Coverage Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #
@pytest.mark.parametrize("acks_late", [True, False])
async def test_lease_lost_exception(acks_late, store_evaluator_context):
    """Test exception handling when early ack fails with lease lost."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    task.acks_late = acks_late
    store_evaluator_context.current_task = task

    # Mock complete_message to raise lease lost error
    store_evaluator_context.evaluator.complete_message = mock.AsyncMock(
        side_effect=exc.BoilermakerTaskLeaseLost("lease lost")
    )
    result = await store_evaluator_context()

    # Should return TaskResult when lease is lost during early ack
    assert isinstance(result, TaskResult)
    if acks_late:
        assert result.status == TaskStatus.Success
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 2
    else:
        assert result.status == TaskStatus.Failure
        assert "Lost message lease" in result.errors
        # Should still store the started result
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 1

    start_call = store_evaluator_context.mock_storage.store_task_result.call_args_list[0]
    start_result = start_call[0][0]
    assert start_result.status == TaskStatus.Started


@pytest.mark.parametrize("acks_late", [True, False])
async def test_early_ack_service_bus_error_exception(acks_late, store_evaluator_context):
    """Test exception handling when early ack fails with service bus error."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    task.acks_late = acks_late
    store_evaluator_context.current_task = task

    # Mock complete_message to raise service bus error
    store_evaluator_context.evaluator.complete_message = mock.AsyncMock(
        side_effect=exc.BoilermakerServiceBusError("service bus error")
    )

    result = await store_evaluator_context()

    # Should return TaskResult when service bus error occurs during ack
    assert isinstance(result, TaskResult)
    if acks_late:
        # Late ack: task completes successfully, then fails during final settlement
        assert result.status == TaskStatus.Success
        assert result.errors is None
        # Should store both started and success results
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 2
    else:
        # Early ack: fails immediately during early settlement
        assert result.status == TaskStatus.Failure
        assert "Service Bus error" in result.errors
        # Should only store the started result
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 1

    start_call = store_evaluator_context.mock_storage.store_task_result.call_args_list[0]
    start_result = start_call[0][0]
    assert start_result.status == TaskStatus.Started


@pytest.mark.parametrize("acks_late", [True, False])
async def test_retries_exhausted_settle_lease_lost_exception(acks_late, store_evaluator_context):
    """Test exception handling when settling message fails during retries exhausted."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    # Set attempts to exceed max tries
    task.attempts.attempts = task.policy.max_tries + 1
    task.acks_late = acks_late
    store_evaluator_context.current_task = task
    evaluator = store_evaluator_context.evaluator

    # Mock deadletter_or_complete_task to raise lease lost error
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("lease lost"))
    result = await evaluator.message_handler()

    assert isinstance(result, TaskResult)
    if acks_late:
        # Late ack: message not settled early, so deadletter_or_complete_task is called and fails
        assert result.status == TaskStatus.Failure
        assert "Lost message lease during retry exhaustion" in result.errors[0]
        # Should only store started result (no retries exhausted result gets stored)
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 1
    else:
        # Early ack: message already settled early, so deadletter_or_complete_task is not called for retries exhausted
        assert result.status == TaskStatus.RetriesExhausted
        # Should store both started and retries exhausted results
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 2

    start_call = store_evaluator_context.mock_storage.store_task_result.call_args_list[0]
    start_result = start_call[0][0]
    assert start_result.status == TaskStatus.Started


@pytest.mark.parametrize("acks_late", [True, False])
async def test_retries_exhausted_settle_service_bus_error_exception(acks_late, store_evaluator_context):
    """Test exception handling when settling message fails during retries exhausted with service bus error."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    # Set attempts to exceed max tries
    task.attempts.attempts = task.policy.max_tries + 1
    task.acks_late = acks_late
    store_evaluator_context.current_task = task
    evaluator = store_evaluator_context.evaluator

    # Mock deadletter_or_complete_task to raise service bus error
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerServiceBusError("service bus error"))

    result = await evaluator.message_handler()

    assert isinstance(result, TaskResult)
    if acks_late:
        # Late ack: message not settled early, so deadletter_or_complete_task is called and fails
        assert result.status == TaskStatus.Failure
        assert "ServiceBus error during retry exhaustion" in result.errors[0]
        # Should only store started result (no retries exhausted result gets stored)
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 1
    else:
        # Early ack: message already settled early, so deadletter_or_complete_task is not called for retries exhausted
        assert result.status == TaskStatus.RetriesExhausted
        # Should store both started and retries exhausted results
        assert store_evaluator_context.mock_storage.store_task_result.call_count == 2

    start_call = store_evaluator_context.mock_storage.store_task_result.call_args_list[0]
    start_result = start_call[0][0]
    assert start_result.status == TaskStatus.Started


@pytest.mark.parametrize("acks_late", [True, False])
async def test_final_settle_lease_lost_exception(acks_late, store_evaluator_context):
    """Test exception handling when final message settlement fails with lease lost."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    task.acks_late = acks_late  # Late ack so message settlement happens at the end
    store_evaluator_context.current_task = task
    evaluator = store_evaluator_context.evaluator

    # Mock complete_message to raise lease lost error at the end
    store_evaluator_context.mockservicebus._receiver.complete_message.side_effect = exc.BoilermakerTaskLeaseLost(
        "lease lost"
    )

    result = await evaluator.message_handler()

    # Should return the result even when final settlement fails
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # Should store both started and success results
    assert store_evaluator_context.mock_storage.store_task_result.call_count == 2


@pytest.mark.parametrize("acks_late", [True, False])
async def test_final_settle_service_bus_error_exception(acks_late, store_evaluator_context):
    """Test exception handling when final message settlement fails with service bus error."""

    async def oktask(state):
        return "OK"

    store_evaluator_context.app.register_async(oktask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(oktask)
    task.acks_late = acks_late  # Late ack so message settlement happens at the end
    store_evaluator_context.current_task = task
    evaluator = store_evaluator_context.evaluator
    # Mock complete_message to raise service bus error at the end
    store_evaluator_context.mockservicebus._receiver.complete_message.side_effect = exc.BoilermakerServiceBusError(
        "service bus error"
    )

    result = await evaluator.message_handler()

    # Should return the result even when final settlement fails
    assert result is not None
    assert result.status == TaskStatus.Success
    assert result.result == "OK"

    # Should store both started and success results
    assert store_evaluator_context.mock_storage.store_task_result.call_count == 2


async def test_final_settle_failure_deadletter_lease_lost_exception(
    store_evaluator_context,
):
    """Test exception handling when final deadletter settlement fails with lease lost."""

    async def failtask(state):
        raise ValueError("Task failed")

    store_evaluator_context.app.register_async(failtask, policy=retries.RetryPolicy.default())
    task = store_evaluator_context.app.create_task(failtask)
    task.acks_late = True  # Late ack so message settlement happens at the end
    store_evaluator_context.current_task = task
    evaluator = store_evaluator_context.evaluator
    # Mock deadletter_or_complete_task to raise lease lost error at the end
    evaluator.deadletter_or_complete_task = AsyncMock(side_effect=exc.BoilermakerTaskLeaseLost("lease lost"))

    result = await evaluator.message_handler()

    # Should return the result even when final settlement fails
    assert result is not None
    assert result.status == TaskStatus.Failure
    assert result.result is None

    # Should store both started and failure results
    assert store_evaluator_context.mock_storage.store_task_result.call_count == 2
