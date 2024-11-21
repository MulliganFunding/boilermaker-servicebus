from unittest import mock

import pytest
from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient
from boilermaker import config, service_bus


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


def make_async_ctx_mgr(mock_thing):
    something = mock_thing

    class WithAsyncContextManager:
        def __getattr__(self, key):
            return getattr(something, key)

        async def __aenter__(self, *args, **kwargs):
            return something

        async def __aexit__(self, *args, **kwargs):
            pass

    return WithAsyncContextManager()


@pytest.fixture(autouse=True)
def mockservicebus(monkeysession):  # type: ignore
    """Mock out Azure Blob Service client"""
    mocksb = mock.MagicMock(ServiceBusClient)

    # Receiver client
    receiver_client = mock.AsyncMock()
    ctx_receiver = make_async_ctx_mgr(receiver_client)
    mocksb.get_queue_receiver = mock.PropertyMock(return_value=ctx_receiver)
    # Sender client reuse mocksb
    sender_client = mock.AsyncMock()
    ctx_sender = make_async_ctx_mgr(sender_client)
    mocksb.get_queue_sender = mock.PropertyMock(return_value=ctx_sender)

    def get_client(*args, **kwargs):
        # Wrap it up so it can be used as a context manager
        return make_async_ctx_mgr(mocksb)

    monkeysession.setattr(
        service_bus,
        "ServiceBusClient",
        get_client,
    )
    mocksb._receiver = receiver_client
    mocksb._sender = sender_client

    return mocksb


@pytest.fixture
def fake_config():
    return config.Config(
        service_bus_namespace_url="https://example.mulligancloud.com",
        service_bus_queue_name="fake-queue-name",
        service_bus_credential=mock.AsyncMock(DefaultAzureCredential)  # fake credential
    )


@pytest.fixture()
def sbus(mockservicebus, fake_config):
    return service_bus.AzureServiceBus(fake_config)
