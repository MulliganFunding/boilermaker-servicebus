from unittest import mock

import pytest
from azure.identity.aio import DefaultAzureCredential
from boilermaker import config

pytest_plugins = [
    "aio_azure_clients_toolbox.testing_utils.fixtures",
]

@pytest.fixture()
@mock.patch.object(config, "DefaultAzureCredential", return_value=mock.AsyncMock(DefaultAzureCredential))
def settings(mock_az_cred):
    mock_az_cred.return_value = mock_az_cred
    return config.Config(
        service_bus_namespace_url="https://example.mulligancloud.com",
        service_bus_queue_name="fake-queue-name",
    )
