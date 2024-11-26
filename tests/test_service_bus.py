import pytest


def test_validate_settings(sbus):
    assert sbus._validate_access_settings() is None
    with pytest.raises(ValueError):
        sbus.queue_name = ""
        sbus._validate_access_settings()


async def test_get_receiver(sbus, mockservicebus):
    receiver =  sbus.get_receiver()
    # it's an asyncmock, so we have to await it
    await receiver.bla()
    assert mockservicebus._receiver.method_calls
    assert sbus.get_receiver() is receiver


async def test_get_sender(sbus, mockservicebus):
    sender =  sbus.get_sender()
    # it's an asyncmock, so we have to await it
    await sender.bla()
    assert mockservicebus._sender.method_calls
    assert sbus.get_sender() is sender


async def test_close(sbus):
    # Make sure these things are bootstrapped
    sbus.get_receiver()
    sbus.get_sender()

    await sbus.close()
    assert sbus._receiver_client is None
    assert sbus._sender_client is None
    assert sbus._client is None


async def test_send_message(sbus, mockservicebus):
    await sbus.send_message("hey")
    assert mockservicebus._sender.method_calls
