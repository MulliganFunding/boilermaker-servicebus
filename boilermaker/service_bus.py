"""
    service_bus.py

    Wrapper class around a `ServiceBusClient` which allows sending messages or
    subscribing to a queue.
"""
import datetime
import logging

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver, ServiceBusSender

from .config import Config

servicebus_logger = logging.getLogger("azure.servicebus")
servicebus_logger.setLevel(logging.WARNING)


class AzureServiceBus:
    def __init__(self, settings: Config):
        self.namespace_url = settings.service_bus_namespace_url
        self.queue_name = settings.service_bus_queue_name
        self.credential = settings.az_credential()
        self._client: ServiceBusClient | None = None
        self._receiver_client: ServiceBusReceiver | None = None
        self._sender_client: ServiceBusSender | None = None

    def _validate_access_settings(self):
        if not all((self.namespace_url, self.queue_name, self.credential)):
            raise ValueError("Invalid configuration for AzureServiceBus")
        return None

    @property
    def client(self):
        if self._client is None:
            self._validate_access_settings()
            self._client = ServiceBusClient(self.namespace_url, self.credential)
        return self._client

    def get_receiver(self) -> ServiceBusReceiver:
        if self._receiver_client is not None:
            return self._receiver_client

        self._receiver_client = self.client.get_queue_receiver(queue_name=self.queue_name)
        return self._receiver_client

    def get_sender(self) -> ServiceBusSender:
        if self._sender_client is not None:
            return self._sender_client

        self._sender_client = self.client.get_queue_sender(queue_name=self.queue_name)
        return self._sender_client

    async def close(self):
        if self._receiver_client is not None:
            await self._receiver_client.close()
            self._receiver_client = None

        if self._sender_client is not None:
            await self._sender_client.close()
            self._sender_client = None

        if self._client is not None:
            await self._client.close()
            self._client = None

    async def send_message(self, msg: str, delay: int = 0):
        message = ServiceBusMessage(msg)
        now = datetime.datetime.now(tz=datetime.UTC)
        scheduled_time_utc = now + datetime.timedelta(seconds=delay)
        sender = self.get_sender()
        await sender.schedule_messages(message, scheduled_time_utc)
