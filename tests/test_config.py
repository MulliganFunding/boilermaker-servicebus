import os
from unittest import mock

from azure.identity.aio import DefaultAzureCredential

from boilermaker import config


@mock.patch.object(config, "DefaultAzureCredential", return_value=mock.AsyncMock(DefaultAzureCredential))
def test_init(mock_az_cred):
    mock_az_cred.return_value = mock_az_cred
    conf = config.Config(
        service_bus_namespace_url="https://example.mulligancloud.com",
        service_bus_queue_name="fake-queue-name",
    )
    conf.service_bus_credential = None

    conf.az_credential()
    conf.azure_credential_include_msi = True
    conf.az_credential()


@mock.patch.object(config, "DefaultAzureCredential", return_value=mock.AsyncMock(DefaultAzureCredential))
def test_env_vars(mock_az_cred):
    old_environ = os.environ
    mock_az_cred.return_value = mock_az_cred
    os.environ["SERVICE_BUS_NAMESPACE_URL"] = "https://example.mulligancloud.com"
    os.environ["SERVICE_BUS_QUEUE_NAME"] = "fake-queue-name"
    conf = config.Config()
    conf.service_bus_credential = None

    conf.az_credential()
    conf.azure_credential_include_msi = True
    conf.az_credential()
    os.environ = old_environ