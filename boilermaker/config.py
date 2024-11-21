from azure.identity.aio import DefaultAzureCredential
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    azure_credential_include_msi: bool = False
    # Service Bus ENV vars
    service_bus_namespace_url: str
    service_bus_queue_name: str
    service_bus_credential: DefaultAzureCredential | None = None

    def az_credential(self):
        if self.service_bus_credential:
            return self.service_bus_credential

        if self.azure_credential_include_msi:
            # For deployed envs, this *should* work mostly via MSI
            return DefaultAzureCredential()
        else:
            # For local dev, MSI would try to hit 169.254.169.254:80 and wait a *long* time and fail
            return DefaultAzureCredential(exclude_managed_identity_credential=True)
