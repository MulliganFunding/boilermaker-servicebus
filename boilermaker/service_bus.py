"""
service_bus.py

Wrapper class around a `aio_azure_clients_toolbox.ManagedAzureServiceBusSender` which
allows sending messages or subscribing to a queue.
"""

from aio_azure_clients_toolbox import (
    CredentialFactory,
    ManagedAzureServiceBusSender,  # type: ignore
)

from .config import Config
from .sample import STATIC_DEBUG_TASK


class AzureServiceBus:
    """Wrapper around Azure Service Bus client for task queue operations.

    Provides a simplified interface to Azure Service Bus functionality
    using the aio_azure_clients_toolbox.ManagedAzureServiceBusSender.
    Handles connection management and message publishing/receiving.

    This class delegates most operations to the underlying client while
    providing convenient construction from configuration objects.

    Attributes:
        client: The underlying ManagedAzureServiceBusSender instance

    Example:

        config = Config()
        service_bus = AzureServiceBus.from_config(config)

        # Direct construction
        credential_factory = lambda: DefaultAzureCredential()
        service_bus = AzureServiceBus(
            "https://myapp.servicebus.windows.net/",
            "tasks",
            credential_factory
        )
    """

    def __init__(
        self,
        service_bus_namespace_url: str,
        service_bus_queue_name,
        credential_factory: CredentialFactory,
    ):
        self.client = ManagedAzureServiceBusSender(
            service_bus_namespace_url,
            service_bus_queue_name,
            credential_factory,
            ready_message=STATIC_DEBUG_TASK.model_dump_json(),
        )

    def __getattr__(self, key: str):
        return getattr(self.client, key)

    @classmethod
    def from_config(cls, settings: Config):
        """Create AzureServiceBus instance from configuration settings.

        Convenience constructor that extracts Service Bus connection details
        and Azure credentials from a Config object.

        Args:
            settings: Configuration object with Service Bus settings

        Returns:
            AzureServiceBus: New instance configured from settings

        Example:

            config = Config()
            service_bus = AzureServiceBus.from_config(config)
        """
        return cls(
            settings.service_bus_namespace_url,
            settings.service_bus_queue_name,
            settings.service_bus_credential or settings.az_credential,
        )
