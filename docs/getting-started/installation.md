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

```py
import boilermaker
print(f"Boilermaker version: {boilermaker.__version__}")
```

## Azure ServiceBus Setup

You'll need an Azure ServiceBus namespace and queue.

### Environment Variables

You can set these environment variables for your application or specify them elsewhere in your app:

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


## Next Steps

- **[Quick Start](quickstart.md)** - Create your first background task
- **[Basic Concepts](basic-concepts.md)** - Understand how Boilermaker works
