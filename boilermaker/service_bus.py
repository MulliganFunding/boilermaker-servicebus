"""
    service_bus.py

    Wrapper class around a `aio_azure_clients_toolbox.ManagedAzureServiceBusSender` which
    allows sending messages or subscribing to a queue.
"""

from aio_azure_clients_toolbox import ManagedAzureServiceBusSender  # type: ignore
from azure.identity.aio import DefaultAzureCredential

from .config import Config
from .sample import STATIC_DEBUG_TASK


class AzureServiceBus:
    def __init__(
        self,
        service_bus_namespace_url: str,
        service_bus_queue_name,
        az_credential: DefaultAzureCredential,
    ):
        self.client = ManagedAzureServiceBusSender(
            service_bus_namespace_url,
            service_bus_queue_name,
            az_credential,
            ready_message=STATIC_DEBUG_TASK.model_dump_json(),
        )

    def __getattr__(self, key: str):
        return getattr(self.client, key)

    @classmethod
    def from_config(cls, settings: Config):
        return cls(
            settings.service_bus_namespace_url,
            settings.service_bus_queue_name,
            settings.service_bus_credential or settings.az_credential()
        )
