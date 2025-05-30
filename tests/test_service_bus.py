
import pytest
from boilermaker.service_bus import AzureServiceBus


@pytest.fixture
def boilermaker_servicebus(settings, mockservicebus):
    return AzureServiceBus.from_config(settings)


async def test_get_receiver(boilermaker_servicebus, mockservicebus):
    receiver =  await boilermaker_servicebus.get_receiver_async()
    # it's an asyncmock, so we have to await it
    await receiver.bla()
    assert mockservicebus._receiver.method_calls
    assert await boilermaker_servicebus.get_receiver_async() is receiver


async def test_get_sender(boilermaker_servicebus, mockservicebus):
    sender = await boilermaker_servicebus.get_sender_async()
    # it's an asyncmock, so we have to await it
    await sender.bla()
    assert mockservicebus._sender.method_calls
    assert await boilermaker_servicebus.get_sender_async() is sender


async def test_send_message(boilermaker_servicebus, mockservicebus):
    await boilermaker_servicebus.send_message("hey")
    assert mockservicebus._sender.method_calls
