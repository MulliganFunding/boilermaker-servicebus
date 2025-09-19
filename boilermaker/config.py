from azure.identity.aio import DefaultAzureCredential
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Configuration settings for Boilermaker Azure Service Bus integration.

    Uses Pydantic settings to load configuration from environment variables
    or direct instantiation. Manages Azure authentication credentials and
    Service Bus connection details.

    Attributes:
        azure_credential_include_msi: Whether to include Managed Service Identity
            for Azure authentication (default: False for local development)
        service_bus_namespace_url: Full URL to Azure Service Bus namespace
            (e.g., "https://myapp.servicebus.windows.net/")
        service_bus_queue_name: Name of the Service Bus queue to use
        service_bus_credential: Optional pre-configured Azure credential

    Environment Variables:
        - SERVICE_BUS_NAMESPACE_URL: Azure Service Bus namespace URL
        - SERVICE_BUS_QUEUE_NAME: Queue name for task messages
        - AZURE_CREDENTIAL_INCLUDE_MSI: Set to "true" for MSI in deployed environments

    Example:
        >>> # From environment variables
        >>> config = Config()
        >>>
        >>> # Direct configuration
        >>> config = Config(
        ...     service_bus_namespace_url="https://myapp.servicebus.windows.net/",
        ...     service_bus_queue_name="tasks",
        ...     azure_credential_include_msi=True
        ... )
    """
    azure_credential_include_msi: bool = False
    # Service Bus ENV vars
    service_bus_namespace_url: str
    service_bus_queue_name: str
    service_bus_credential: DefaultAzureCredential | None = None

    def az_credential(self):
        """Create or return Azure credential for Service Bus authentication.

        Returns a configured DefaultAzureCredential with appropriate settings
        for the deployment environment. For local development, excludes
        Managed Service Identity to avoid timeout delays.

        Returns:
            DefaultAzureCredential: Configured Azure credential object

        Note:
            - Local development: Excludes MSI to prevent 169.254.169.254 timeouts
            - Deployed environments: Includes MSI for automatic authentication
        """
        if self.service_bus_credential:
            return self.service_bus_credential

        if self.azure_credential_include_msi:
            # For deployed envs, this *should* work mostly via MSI
            return DefaultAzureCredential()
        else:
            # For local dev, MSI would try to hit 169.254.169.254:80 and wait a *long* time and fail
            return DefaultAzureCredential(exclude_managed_identity_credential=True)
