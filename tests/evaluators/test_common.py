from unittest.mock import AsyncMock, Mock

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from azure.servicebus.exceptions import MessageLockLostError, ServiceBusError
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
def dummy_task():
    """Create a basic task for testing."""
    return Task.default("test_function")


class DummyMsg:
    sequence_number = 789


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
        state=DEFAULT_STATE
    )

    # Set up a dummy message on the task
    class DummyMsg:
        sequence_number = 789

    msg = DummyMsg()
    evaluator.task.msg = msg

    await evaluator.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


async def test_message_handler_complete_message_with_error_integration(mockservicebus, dummy_task):
    """Test that MessageHandler complete_message handles errors through NoStorageEvaluator."""


    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE
    )

    class DummyMsg:
        sequence_number = 789

    msg = DummyMsg()
    evaluator.task.msg = msg
    mockservicebus._receiver.complete_message.side_effect = ServiceBusError("fail")

    # Should not raise an exception
    await evaluator.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


async def test_message_handler_renew_message_lock_integration(mockservicebus, dummy_task):
    """Test that MessageHandler renew_message_lock works through NoStorageEvaluator interface."""


    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE
    )

    class DummyMsg:
        sequence_number = 789

    msg = DummyMsg()
    evaluator.task.msg = msg
    expected_time = Mock()
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await evaluator.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(msg)
    assert result is expected_time


async def test_message_handler_renew_message_lock_errors_integration(mockservicebus, dummy_task):
    """Test that MessageHandler renew_message_lock handles errors through NoStorageEvaluator."""


    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE
    )

    class DummyMsg:
        sequence_number = 789

    msg = DummyMsg()
    evaluator.task.msg = msg
    mockservicebus._receiver.renew_message_lock.side_effect = MessageLockLostError()

    result = await evaluator.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(msg)
    assert result is None


async def test_message_handler_renew_message_lock_missing_integration(mockservicebus, dummy_task):
    """Test that MessageHandler renew_message_lock handles missing receiver/message through NoStorageEvaluator."""

    mock_task_publisher = AsyncMock()
    function_registry = {"test_function": AsyncMock()}

    evaluator = NoStorageEvaluator(
        receiver=mockservicebus._receiver,
        task=dummy_task,
        task_publisher=mock_task_publisher,
        function_registry=function_registry,
        state=DEFAULT_STATE
    )

    class DummyMsg:
        sequence_number = 789

    msg = DummyMsg()
    # Missing receiver
    evaluator.task.msg = msg
    evaluator._receiver = None
    # Should log warnings but not raise
    await evaluator.renew_message_lock()

    # Missing message
    evaluator._receiver = mockservicebus._receiver
    evaluator.task.msg = None
    # Should log warnings but not raise
    await evaluator.renew_message_lock()

    # With the stuff
    evaluator.task.msg = msg
    await evaluator.renew_message_lock()
    assert mockservicebus._receiver.renew_message_lock.called


async def test_abandon_message_success(mockservicebus):
    """Test that abandon_message successfully abandons a message."""
    msg = DummyMsg()

    await MessageActions.abandon_message(msg, mockservicebus._receiver)

    mockservicebus._receiver.abandon_message.assert_called_once_with(msg)


async def test_abandon_message_with_error(mockservicebus):
    """Test that abandon_message handles errors gracefully."""
    msg = DummyMsg()
    mockservicebus._receiver.abandon_message.side_effect = ServiceBusError("abandon failed")

    # Should not raise an exception
    await MessageActions.abandon_message(msg, mockservicebus._receiver)

    mockservicebus._receiver.abandon_message.assert_called_once_with(msg)


async def test_abandon_message_none_msg(mockservicebus):
    """Test that abandon_message handles None message gracefully."""
    await MessageActions.abandon_message(None, mockservicebus._receiver)

    # Should not have called abandon_message
    mockservicebus._receiver.abandon_message.assert_not_called()


async def test_complete_message_success(mockservicebus):
    """Test that complete_message successfully completes a message."""
    msg = DummyMsg()

    await MessageActions.complete_message(msg, mockservicebus._receiver)

    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


async def test_complete_message_with_error(mockservicebus):
    """Test that complete_message handles errors gracefully."""
    msg = DummyMsg()
    mockservicebus._receiver.complete_message.side_effect = ServiceBusError("complete failed")

    # Should not raise an exception
    await MessageActions.complete_message(msg, mockservicebus._receiver)

    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


async def test_renew_message_lock_success(mockservicebus):
    """Test that renew_message_lock successfully renews lock."""
    msg = DummyMsg()
    expected_time = Mock()  # Mock datetime
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await MessageActions.renew_message_lock(msg, mockservicebus._receiver)

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(msg)
    assert result is expected_time


async def test_renew_message_lock_with_error(mockservicebus):
    """Test that renew_message_lock handles errors gracefully."""
    msg = DummyMsg()
    mockservicebus._receiver.renew_message_lock.side_effect = MessageLockLostError()

    result = await MessageActions.renew_message_lock(msg, mockservicebus._receiver)

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(msg)
    assert result is None


async def test_renew_message_lock_none_receiver():
    """Test that renew_message_lock handles None receiver gracefully."""
    msg = DummyMsg()

    result = await MessageActions.renew_message_lock(msg, None)

    assert result is None


async def test_renew_message_lock_none_message(mockservicebus):
    """Test that renew_message_lock handles None message gracefully."""
    result = await MessageActions.renew_message_lock(None, mockservicebus._receiver)

    assert result is None
    mockservicebus._receiver.renew_message_lock.assert_not_called()


async def test_deadletter_or_complete_task_deadletter(mockservicebus, dummy_task):
    """Test deadletter_or_complete_task when task should_dead_letter is True."""
    msg = DummyMsg()
    dummy_task.should_dead_letter = True

    await MessageActions.deadletter_or_complete_task(
        msg, mockservicebus._receiver, "TestReason", dummy_task, detail="Test detail"
    )

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        msg, reason="TestReason", error_description="Task failed: Test detail"
    )
    mockservicebus._receiver.complete_message.assert_not_called()


async def test_deadletter_or_complete_task_complete(mockservicebus, dummy_task):
    """Test deadletter_or_complete_task when task should_dead_letter is False."""
    msg = DummyMsg()
    dummy_task.should_dead_letter = False

    await MessageActions.deadletter_or_complete_task(
        msg, mockservicebus._receiver, "TestReason", dummy_task
    )

    mockservicebus._receiver.dead_letter_message.assert_not_called()
    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


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
        state=DEFAULT_STATE
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


async def test_message_handler_current_msg_property(message_handler, dummy_task):
    """Test MessageHandler current_msg property."""
    # Set a message on the task
    msg = make_message(dummy_task)
    message_handler.task.msg = msg

    # Clear cached property to force recalculation
    if hasattr(message_handler, '_current_msg'):
        delattr(message_handler, '_current_msg')

    assert message_handler.current_msg is msg


async def test_message_handler_sequence_number_property(message_handler, dummy_task):
    """Test MessageHandler sequence_number property."""
    # Set sequence number on task
    msg = make_message(dummy_task, sequence_number=123)
    message_handler.task.msg = msg

    # Clear cached property to force recalculation
    if hasattr(message_handler, '_sequence_number'):
        delattr(message_handler, '_sequence_number')

    assert message_handler.sequence_number == 123


async def test_message_handler_publish_task(message_handler):
    """Test MessageHandler publish_task method."""
    new_task = Task.default("other_function")

    result = await message_handler.publish_task(new_task, delay=10, publish_attempts=2)

    assert result is new_task
    message_handler.task_publisher.assert_called_once_with(
        new_task, delay=10, publish_attempts=2
    )


async def test_message_handler_abandon_current_message(message_handler, mockservicebus):
    """Test MessageHandler abandon_current_message delegates to MessageActions."""
    msg = DummyMsg()
    message_handler.task.msg = msg

    await message_handler.abandon_current_message()

    mockservicebus._receiver.abandon_message.assert_called_once_with(msg)


async def test_message_handler_complete_message(message_handler, mockservicebus):
    """Test MessageHandler complete_message delegates to MessageActions."""
    msg = DummyMsg()
    message_handler.task.msg = msg

    await message_handler.complete_message()

    mockservicebus._receiver.complete_message.assert_called_once_with(msg)


async def test_message_handler_renew_message_lock(message_handler, mockservicebus):
    """Test MessageHandler renew_message_lock delegates to MessageActions."""
    msg = DummyMsg()
    message_handler.task.msg = msg
    expected_time = Mock()
    mockservicebus._receiver.renew_message_lock.return_value = expected_time

    result = await message_handler.renew_message_lock()

    mockservicebus._receiver.renew_message_lock.assert_called_once_with(msg)
    assert result is expected_time


async def test_message_handler_deadletter_or_complete_task(message_handler, mockservicebus):
    """Test MessageHandler deadletter_or_complete_task delegates to MessageActions."""
    msg = DummyMsg()
    message_handler.task.msg = msg
    message_handler.task.should_dead_letter = True

    await message_handler.deadletter_or_complete_task("TestReason", detail="Test detail")

    mockservicebus._receiver.dead_letter_message.assert_called_once_with(
        msg, reason="TestReason", error_description="Task failed: Test detail"
    )
