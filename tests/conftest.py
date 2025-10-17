from unittest import mock

import pytest
from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from boilermaker import config
from boilermaker.app import Boilermaker
from boilermaker.task import Task

pytest_plugins = [
    "aio_azure_clients_toolbox.testing_utils.fixtures",
]


@pytest.fixture()
@mock.patch.object(config, "DefaultAzureCredential", return_value=mock.AsyncMock(DefaultAzureCredential))
def settings(mock_az_cred):
    mock_az_cred.return_value = mock_az_cred
    return config.Config(
        service_bus_namespace_url="https://example.mulligancloud.com",
        service_bus_queue_name="fake-queue-name",
    )


@pytest.fixture
def make_message():
    def inner(task, sequence_number: int = 123):
        # Example taken from:
        # azure-sdk-for-python/blob/main/sdk/servicebus/azure-servicebus/tests/test_message.py#L233
        my_frame = [0, 0, 0]
        amqp_received_message = Message(
            data=[task.model_dump_json().encode("utf-8")],
            message_annotations={SEQUENCENUBMERNAME: sequence_number},
        )
        return ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)

    return inner


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]


DEFAULT_STATE = State({"somekey": "somevalue"})


@pytest.fixture
def state():
    return DEFAULT_STATE


@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)


@pytest.fixture
def dummy_msg(make_message):
    """Create a basic ServiceBusReceivedMessage for testing."""
    return make_message(Task.default("test_function"), sequence_number=789)


@pytest.fixture
def dummy_task(dummy_msg):
    """Create a basic task for testing."""
    task = Task.default("test_function")
    task.msg = dummy_msg
    return task


@pytest.fixture
def mock_storage():
    """Mock storage interface for testing."""
    from boilermaker.storage import StorageInterface

    storage = mock.Mock(spec=StorageInterface)
    storage.store_task_result = mock.AsyncMock()
    storage.load_graph = mock.AsyncMock(return_value=None)
    storage.store_graph = mock.AsyncMock()
    return storage
