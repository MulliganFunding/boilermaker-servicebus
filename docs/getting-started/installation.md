# Installation

## Prerequisites

Before installing Boilermaker, ensure you have:

- **Python 3.11 or higher**
- **Azure subscription** with ServiceBus namespace
- **pip** or **uv** package manager

## Install Boilermaker

=== "pip"

    ```bash
    pip install "boilermaker-servicebus"
    ```

=== "uv"

    ```bash
    uv add "boilermaker-servicebus"
    ```

=== "Poetry"

    ```bash
    poetry add "boilermaker-servicebus"
    ```

## Verify Installation

Test your installation:

```python
import boilermaker
print(f"Boilermaker version: {boilermaker.__version__}")
```

## Azure ServiceBus Setup

You'll need an Azure ServiceBus namespace and queue. If you don't have these set up yet, see our [Azure Setup Guide](../guides/azure-setup.md).

### Required Environment Variables

Set these environment variables for your application:

```bash
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"
```

### Authentication

Boilermaker uses Azure's DefaultAzureCredential for authentication, which supports:

- **Azure CLI**: `az login` (recommended for development)
- **Environment variables**: `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`
- **Managed Identity**: Automatic in Azure environments
- **Visual Studio Code**: If signed in to Azure

For development, the easiest method is:

```bash
az login
```

## Development Installation

If you plan to contribute to Boilermaker:

```bash
git clone https://github.com/MulliganFunding/boilermaker-servicebus.git
cd boilermaker-servicebus
uv sync
```

See our [Development Guide](../contributing/development.md) for more details.

## Next Steps

- **[Quick Start](quickstart.md)** - Create your first background task
- **[Basic Concepts](basic-concepts.md)** - Understand how Boilermaker works
- **[Azure Setup](../guides/azure-setup.md)** - Set up Azure ServiceBus