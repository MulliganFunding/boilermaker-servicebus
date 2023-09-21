"""
    service_bus.py

    Wrapper class around a `ServiceBusClient` which allows sending messages or
    subscribing to a queue.
"""
import contextlib
import datetime
import logging
from typing import Optional

from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

from .config import Config

servicebus_logger = logging.getLogger("azure.servicebus")
servicebus_logger.setLevel(logging.WARNING)


class AzureServiceBus:
    def __init__(self, settings: Config):
        self.namespace_url = settings.service_bus_namespace_url
        self.queue_name = settings.service_bus_queue_name
        self.credential = settings.az_credential()
        self._client: Optional[ServiceBusClient] = None

    def _validate_access_settings(self):
        if not all((self.namespace_url, self.queue_name, self.credential)):
            raise ValueError("Invalid configuration for AzureServiceBus")
        return None

    @property
    def client(self) -> ServiceBusClient:
        if self._client is not None:
            return self._client

        self._validate_access_settings()
        self._client = ServiceBusClient(self.namespace_url, self.credential)
        return self._client

    @contextlib.asynccontextmanager
    async def get_receiver(self):
        async with self.client as client:
            receiver = client.get_queue_receiver(queue_name=self.queue_name)
            async with receiver:  # type: ignore
                yield receiver

    async def send_message(self, msg: str, delay: int = 0):
        message = ServiceBusMessage(msg)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        scheduled_time_utc = now + datetime.timedelta(seconds=delay)
        async with self.client as client:  # type: ignore
            sender = client.get_queue_sender(queue_name=self.queue_name)
            async with sender:
                await sender.schedule_messages(message, scheduled_time_utc)
