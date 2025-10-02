from unittest.mock import AsyncMock, Mock

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    ServiceBusError,
    SessionLockLostError,
)
from boilermaker import exc
from boilermaker.app import Boilermaker
from boilermaker.evaluators import NoStorageEvaluator
from boilermaker.evaluators.common import MessageActions, MessageHandler
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
    return ServiceBusReceivedMessage(
        amqp_received_message, receiver=None, frame=my_frame
    )


def make_garbage_message(sequence_number: int = 456):
    """Create a message with invalid JSON data."""
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[b"{{\\])))"],  # Invalid JSON
        message_annotations={SEQUENCENUBMERNAME: sequence_number},
    )
    return ServiceBusReceivedMessage(
        amqp_received_message, receiver=None, frame=my_frame
    )


@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


@pytest.fixture
def dummy_msg():
    """Create a basic ServiceBusReceivedMessage for testing."""
    return make_message(Task.default("test_function"), sequence_number=789)


@pytest.fixture
def dummy_task(dummy_msg):
    """Create a basic task for testing."""
    task = Task.default("test_function")
    task.msg = dummy_msg
    return task


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# MessageActions Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_task_decoder_valid_json(dummy_task, mockservicebus):
    """Test that task_decoder successfully decodes valid Task messages."""
    msg = make_message(dummy_task)

    result = await MessageActions.task_decoder(msg, mockservicebus._receiver)

    assert result is not None
    assert isinstance(result, Task)
    assert result.function_name == "test_function"
    assert result.msg is msg


async def test_task_decoder_invalid_json(mockservicebus):
    """Test that task_decoder handles invalid JSON and dead letters the message."""
    msg = make_garbage_message()

    result = await MessageActions.task_decoder(msg, mockservicebus._receiver)

    assert result is None
    # Should have called dead_letter_message on the receiver
    mockservicebus._receiver.dead_letter_message.assert_called_once()
    call_args = mockservicebus._receiver.dead_letter_message.call_args
    assert call_args[0][0] is msg  # First positional arg is the message
    assert call_args[1]["reason"] == "InvalidTaskFormat"


# Tests moved from test_base.py that test MessageHandler functionality
async def test_message_handler_complete_message_integration(mockservicebus, dummy_task):
    """Test that MessageHandler complete_message works through NoStorageEvaluator interface."""

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )

    await evaluator.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_task.msg)


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_message_handler_complete_message_with_error_integration(
    side_effect, wrapped_exc, mockservicebus, dummy_task
):
    """Test that MessageHandler complete_message wraps appropriate errors through NoStorageEvaluator."""

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )

    mockservicebus._receiver.complete_message.side_effect = side_effect

    # Should raise wrapped exception if applicable, otherwise original
    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await evaluator.complete_message()
    else:
        with pytest.raises(side_effect.__class__):
            await evaluator.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_task.msg)


async def test_message_handler_renew_message_lock_integration(
    mockservicebus, dummy_task
):
    """Test that MessageHandler renew_message_lock works through NoStorageEvaluator interface."""

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )

    expected_time = Mock()
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await evaluator.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(dummy_task.msg)
    assert result is expected_time


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_message_handler_renew_message_lock_errors_integration(
    side_effect, wrapped_exc, mockservicebus, dummy_task
):
    """Test that MessageHandler renew_message_lock handles errors through NoStorageEvaluator."""

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )

    mockservicebus._receiver.renew_message_lock.side_effect = side_effect

    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await evaluator.renew_message_lock()
    else:
        with pytest.raises(side_effect.__class__):
            await evaluator.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(dummy_task.msg)


async def test_message_handler_renew_message_lock_missing_integration(
    mockservicebus, dummy_task
):
    """
    Test that MessageHandler renew_message_lock handles
    missing receiver/message through NoStorageEvaluator.
    """

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )

    # Missing receiver
    evaluator._receiver = None
    # Should log warnings but not raise
    await evaluator.renew_message_lock()

    msg_ptr = dummy_task.msg
    # Missing message
    evaluator._receiver = mockservicebus._receiver
    evaluator.task.msg = None
    # Should log warnings but not raise
    await evaluator.renew_message_lock()

    # With the stuff
    evaluator.task.msg = msg_ptr
    await evaluator.renew_message_lock()
    assert mockservicebus._receiver.renew_message_lock.called


async def test_abandon_message_success(mockservicebus, dummy_msg):
    """Test that abandon_message successfully abandons a message."""

    await MessageActions.abandon_message(dummy_msg, mockservicebus._receiver)

    mockservicebus._receiver.abandon_message.assert_called_once_with(dummy_msg)


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_abandon_message_with_error(
    side_effect, wrapped_exc, mockservicebus, dummy_msg
):
    """Test that abandon_message handles errors gracefully."""
    mockservicebus._receiver.abandon_message.side_effect = side_effect

    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await MessageActions.abandon_message(dummy_msg, mockservicebus._receiver)
    else:
        with pytest.raises(side_effect.__class__):
            await MessageActions.abandon_message(dummy_msg, mockservicebus._receiver)

    mockservicebus._receiver.abandon_message.assert_called_once_with(dummy_msg)


async def test_abandon_message_none_msg(mockservicebus):
    """Test that abandon_message handles None message gracefully."""
    await MessageActions.abandon_message(None, mockservicebus._receiver)

    # Should not have called abandon_message
    mockservicebus._receiver.abandon_message.assert_not_called()


async def test_complete_message_success(mockservicebus, dummy_msg):
    """Test that complete_message successfully completes a message."""
    await MessageActions.complete_message(dummy_msg, mockservicebus._receiver)

    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_msg)


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_complete_message_with_error(
    side_effect, wrapped_exc, mockservicebus, dummy_msg
):
    """Test that complete_message handles errors gracefully."""
    mockservicebus._receiver.complete_message.side_effect = side_effect

    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await MessageActions.complete_message(dummy_msg, mockservicebus._receiver)
    else:
        with pytest.raises(side_effect.__class__):
            await MessageActions.complete_message(dummy_msg, mockservicebus._receiver)

    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_msg)


async def test_renew_message_lock_success(mockservicebus, dummy_msg):
    """Test that renew_message_lock successfully renews lock."""
    expected_time = Mock()  # Mock datetime
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await MessageActions.renew_message_lock(
        dummy_msg, mockservicebus._receiver
    )

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(dummy_msg)
    assert result is expected_time


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_renew_message_lock_with_error(
    side_effect, wrapped_exc, mockservicebus, dummy_msg
):
    """Test that renew_message_lock handles errors gracefully."""

    mockservicebus._receiver.renew_message_lock.side_effect = side_effect

    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await MessageActions.renew_message_lock(dummy_msg, mockservicebus._receiver)
    else:
        with pytest.raises(side_effect.__class__):
            await MessageActions.renew_message_lock(dummy_msg, mockservicebus._receiver)

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(dummy_msg)


async def test_renew_message_lock_none_receiver(dummy_msg):
    """Test that renew_message_lock handles None receiver gracefully."""
    result = await MessageActions.renew_message_lock(dummy_msg, None)

    assert result is None


async def test_renew_message_lock_none_message(mockservicebus):
    """Test that renew_message_lock handles None message gracefully."""
    result = await MessageActions.renew_message_lock(None, mockservicebus._receiver)

    assert result is None
    mockservicebus._receiver.renew_message_lock.assert_not_called()


async def test_dead_letter_message_success(mockservicebus, dummy_msg):
    """Test that dead_letter_message successfully deadletters a message."""
    await MessageActions.dead_letter_message(
        dummy_msg, mockservicebus._receiver, "TestReason", "Test description"
    )

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        dummy_msg,
        reason="TestReason",
        error_description="Test description",
    )


async def test_dead_letter_message_success_default_description(
    mockservicebus, dummy_msg
):
    """Test that dead_letter_message uses default error description when not provided."""
    await MessageActions.dead_letter_message(
        dummy_msg, mockservicebus._receiver, "TestReason"
    )

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        dummy_msg,
        reason="TestReason",
        error_description="Task failed",
    )


@pytest.mark.parametrize(
    "side_effect,wrapped_exc",
    [
        (ServiceBusError("fail"), exc.BoilermakerServiceBusError),
        (MessageLockLostError(), exc.BoilermakerTaskLeaseLost),
        (MessageAlreadySettled(), exc.BoilermakerTaskLeaseLost),
        (SessionLockLostError(), exc.BoilermakerTaskLeaseLost),
        (ValueError("other"), None),
    ],
)
async def test_dead_letter_message_with_error(
    side_effect, wrapped_exc, mockservicebus, dummy_msg
):
    """Test that dead_letter_message handles errors gracefully."""
    mockservicebus._receiver.dead_letter_message.side_effect = side_effect

    if wrapped_exc is not None:
        with pytest.raises(wrapped_exc):
            await MessageActions.dead_letter_message(
                dummy_msg, mockservicebus._receiver, "TestReason"
            )
    else:
        with pytest.raises(side_effect.__class__):
            await MessageActions.dead_letter_message(
                dummy_msg, mockservicebus._receiver, "TestReason"
            )

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        dummy_msg,
        reason="TestReason",
        error_description="Task failed",
    )


async def test_deadletter_or_complete_task_deadletter(mockservicebus, dummy_task):
    """Test deadletter_or_complete_task when task should_dead_letter is True."""
    dummy_task.should_dead_letter = True

    await MessageActions.deadletter_or_complete_task(
        dummy_task,
        mockservicebus._receiver,
        "TestReason",
        detail="Test detail",
    )

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        dummy_task.msg,
        reason="TestReason",
        error_description="Test detail",
    )
    mockservicebus._receiver.complete_message.assert_not_called()


async def test_deadletter_or_complete_task_complete(mockservicebus, dummy_task):
    """Test deadletter_or_complete_task when task should_dead_letter is False."""
    dummy_task.should_dead_letter = False

    await MessageActions.deadletter_or_complete_task(
        dummy_task, mockservicebus._receiver, "TestReason"
    )

    mockservicebus._receiver.dead_letter_message.assert_not_called()
    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_task.msg)


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# MessageHandler Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


class ConcreteMessageHandler(MessageHandler):
    """Concrete implementation of MessageHandler for testing."""

    async def message_handler(self):
        return "message_handler_result"

    async def task_handler(self):
        return "task_handler_result"


@pytest.fixture
def message_handler(dummy_task, mockservicebus):
    """Create a MessageHandler instance for testing."""
    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    handler = ConcreteMessageHandler(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE,
    )
    return handler


async def test_message_handler_init(message_handler, dummy_task, mockservicebus):
    """Test MessageHandler initialization."""
    assert message_handler._receiver is mockservicebus._receiver
    assert message_handler.task is dummy_task
    assert message_handler.state is DEFAULT_STATE
    assert message_handler.function_registry["test_function"] is not None


async def test_message_handler_call(message_handler):
    """Test MessageHandler __call__ method."""
    result = await message_handler()
    assert result == "message_handler_result"


async def test_message_handler_call_with_pre_process_exception(
    message_handler, mockservicebus
):
    """Test MessageHandler __call__ method handles Exception in pre_process."""
    # Mock pre_process to raise a general exception
    message_handler.pre_process = AsyncMock(side_effect=RuntimeError("Test exception"))

    result = await message_handler()

    # Should return TaskResult with failure status
    assert result is not None
    assert result.status.value == "failure"
    assert result.task_id == message_handler.task.task_id
    assert result.graph_id == message_handler.task.graph_id
    assert result.result is None
    assert result.errors == ["Pre-processing exception"]
    assert result.formatted_exception is not None

    # Should have called deadletter_or_complete_task (deadletter since should_dead_letter=True)
    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        message_handler.task.msg,
        reason="ProcessingError",
        error_description="Pre-processing exception",
    )


async def test_message_handler_current_msg_property(message_handler, dummy_task):
    """Test MessageHandler current_msg property."""
    # Set a message on the task
    msg = make_message(dummy_task)
    message_handler.task.msg = msg

    # Clear cached property to force recalculation
    if hasattr(message_handler, "_current_msg"):
        delattr(message_handler, "_current_msg")

    assert message_handler.current_msg is msg


async def test_message_handler_sequence_number_property(message_handler, dummy_task):
    """Test MessageHandler sequence_number property."""
    # Set sequence number on task
    msg = make_message(dummy_task, sequence_number=123)
    message_handler.task.msg = msg

    # Clear cached property to force recalculation
    if hasattr(message_handler, "_sequence_number"):
        delattr(message_handler, "_sequence_number")

    assert message_handler.sequence_number == 123


async def test_message_handler_publish_task(message_handler):
    """Test MessageHandler publish_task method."""
    new_task = Task.default("other_function")

    result = await message_handler.publish_task(new_task, delay=10, publish_attempts=2)

    assert result is new_task
    message_handler.task_publisher.assert_called_once_with(
        new_task, delay=10, publish_attempts=2
    )


async def test_message_handler_abandon_current_message(
    message_handler, mockservicebus, dummy_msg
):
    """Test MessageHandler abandon_current_message delegates to MessageActions."""
    message_handler.task.msg = dummy_msg

    await message_handler.abandon_current_message()

    mockservicebus._receiver.abandon_message.assert_called_once_with(dummy_msg)


async def test_message_handler_complete_message(
    message_handler, mockservicebus, dummy_msg
):
    """Test MessageHandler complete_message delegates to MessageActions."""
    message_handler.task.msg = dummy_msg

    await message_handler.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(dummy_msg)


async def test_message_handler_renew_message_lock(
    message_handler, mockservicebus, dummy_msg
):
    """Test MessageHandler renew_message_lock delegates to MessageActions."""
    message_handler.task.msg = dummy_msg
    expected_time = Mock()
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await message_handler.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(dummy_msg)
    assert result is expected_time


async def test_message_handler_deadletter_or_complete_task(
    message_handler, mockservicebus, dummy_msg
):
    """Test MessageHandler deadletter_or_complete_task delegates to MessageActions."""
    message_handler.task.msg = dummy_msg
    message_handler.task.should_dead_letter = True

    await message_handler.deadletter_or_complete_task("TestReason")

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        dummy_msg, reason="TestReason", error_description="Task failed"
    )


async def test_message_handler_pre_process_debug_task_success(
    message_handler, mockservicebus
):
    """Test MessageHandler pre_process handles debug task successfully."""
    from boilermaker import sample

    # Set task to debug task
    message_handler.task.function_name = sample.TASK_NAME

    # Mock sample.debug_task
    original_debug_task = sample.debug_task
    sample.debug_task = AsyncMock()

    try:
        result = await message_handler.pre_process()

        # Should return TaskResult with success status
        assert result is not None
        assert result.status.value == "success"
        assert result.task_id == message_handler.task.task_id
        assert result.graph_id == message_handler.task.graph_id
        assert result.result == 0
        assert result.errors == []

        # Should have called debug_task and complete_message
        sample.debug_task.assert_called_once_with(message_handler.state)
        mockservicebus._receiver.complete_message.assert_called_once_with(
            message_handler.task.msg
        )
    finally:
        # Restore original function
        sample.debug_task = original_debug_task


async def test_message_handler_pre_process_debug_task_lease_lost(
    message_handler, mockservicebus
):
    """Test MessageHandler pre_process handles debug task with message lease lost."""
    from boilermaker import exc, sample

    # Set task to debug task
    message_handler.task.function_name = sample.TASK_NAME

    # Mock sample.debug_task
    original_debug_task = sample.debug_task
    sample.debug_task = AsyncMock()

    # Mock complete_message to raise lease lost error
    mockservicebus._receiver.complete_message.side_effect = (
        exc.BoilermakerTaskLeaseLost("lease lost")
    )

    try:
        result = await message_handler.pre_process()

        # Should return TaskResult with failure status
        assert result is not None
        assert result.status.value == "failure"
        assert result.task_id == message_handler.task.task_id
        assert result.graph_id == message_handler.task.graph_id
        assert result.result == 0
        assert len(result.errors) == 1
        assert "Lost message lease when trying to complete early" in result.errors[0]

        # Should have called debug_task and complete_message
        sample.debug_task.assert_called_once_with(message_handler.state)
        mockservicebus._receiver.complete_message.assert_called_once_with(
            message_handler.task.msg
        )
    finally:
        # Restore original function
        sample.debug_task = original_debug_task


async def test_message_handler_pre_process_debug_task_service_bus_error(
    message_handler, mockservicebus
):
    """Test MessageHandler pre_process handles debug task with service bus error."""
    from boilermaker import exc, sample

    # Set task to debug task
    message_handler.task.function_name = sample.TASK_NAME

    # Mock sample.debug_task
    original_debug_task = sample.debug_task
    sample.debug_task = AsyncMock()

    # Mock complete_message to raise service bus error
    mockservicebus._receiver.complete_message.side_effect = (
        exc.BoilermakerServiceBusError("service bus error")
    )

    try:
        result = await message_handler.pre_process()

        # Should return TaskResult with failure status
        assert result is not None
        assert result.status.value == "failure"
        assert result.task_id == message_handler.task.task_id
        assert result.graph_id == message_handler.task.graph_id
        assert result.result == 0
        assert len(result.errors) == 1
        assert "Lost message lease when trying to complete early" in result.errors[0]

        # Should have called debug_task and complete_message
        sample.debug_task.assert_called_once_with(message_handler.state)
        mockservicebus._receiver.complete_message.assert_called_once_with(
            message_handler.task.msg
        )
    finally:
        # Restore original function
        sample.debug_task = original_debug_task


async def test_message_handler_pre_process_missing_function(message_handler):
    """Test MessageHandler pre_process raises exception for missing function."""
    from boilermaker import exc

    # Set task to non-existent function
    message_handler.task.function_name = "non_existent_function"

    with pytest.raises(exc.BoilermakerExpectionFailed) as exc_info:
        await message_handler.pre_process()

    assert "Missing registered function non_existent_function" in str(exc_info.value)


async def test_message_handler_pre_process_normal_function(message_handler):
    """Test MessageHandler pre_process returns None for normal functions."""
    # Task already has "test_function" which exists in function_registry
    result = await message_handler.pre_process()

    # Should return None (no early return)
    assert result is None
