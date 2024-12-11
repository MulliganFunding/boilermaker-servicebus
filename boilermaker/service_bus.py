"""
    service_bus.py

    Wrapper class around a `ServiceBusClient` which allows sending messages or
    subscribing to a queue.
"""
import logging
from aio_azure_clients_toolbox import ManagedAzureServiceBusSender

from .config import Config
from .task import STATIC_DEBUG_TASK

servicebus_logger = logging.getLogger("azure.servicebus")
servicebus_logger.setLevel(logging.WARNING)


class AzureServiceBus:
    def __init__(self, config: Config, credential: DefaultAzureCredential):
        self.client = ManagedAzureServiceBusSender(
            config.service_bus_namespace_url,
            config.service_bus_queue_name,
            credential,
            ready_message=STATIC_DEBUG_TASK.model_dump_json(),
        )
