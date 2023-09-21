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

    def get_sbc(self):
        self._validate_access_settings()
        return ServiceBusClient(self.namespace_url, self.credential)

    @contextlib.asynccontextmanager
    async def get_receiver(self):
        async with self.get_sbc() as client:
            receiver = client.get_queue_receiver(queue_name=self.queue_name)
            async with receiver:  # type: ignore
                yield receiver

    @contextlib.asynccontextmanager
    async def get_sender(self):
        async with self.get_sbc() as client:
            sender = client.get_queue_sender(queue_name=self.queue_name)
            async with sender:
                yield sender

    async def send_message(self, msg: str, delay: int = 0):
        message = ServiceBusMessage(msg)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        scheduled_time_utc = now + datetime.timedelta(seconds=delay)
        async with self.get_sender() as sender:  # type: ignore
            await sender.schedule_messages(message, scheduled_time_utc)
