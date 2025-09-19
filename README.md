# Boilermaker

**Async Python background tasks with Azure ServiceBus**

[![Tests](https://github.com/MulliganFunding/boilermaker-servicebus/workflows/Tests/badge.svg)](https://github.com/MulliganFunding/boilermaker-servicebus/actions)
[![PyPI version](https://badge.fury.io/py/boilermaker-servicebus.svg)](https://badge.fury.io/py/boilermaker-servicebus)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-github--pages-blue)](https://mulliganfunding.github.io/boilermaker-servicebus/)

Boilermaker is a lightweight task runner exclusively for **async Python** and Azure Service Bus Queues. If you need a fully-featured task runner with multiple backends, consider [Celery](https://github.com/celery/celery/tree/main) instead.

## Quick Start

### Install

```sh
pip install "boilermaker-servicebus"
```

### Basic Usage

```python
import asyncio
from boilermaker import Boilermaker, retries
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Configure (loads from environment variables)
config = Config()
service_bus = AzureServiceBus.from_config(config)

# Create app with shared state
class AppState:
    def __init__(self):
        self.data = {"count": 0}

app = Boilermaker(AppState(), service_bus)

# Define background task
async def process_data(state, data_id):
    """state is injected automatically as first parameter"""
    print(f"Processing {data_id}")
    state.data["count"] += 1
    return f"Processed {data_id}"

# Register task
app.register_async(process_data, policy=retries.RetryPolicy.default())

# Publish task
async def main():
    await app.apply_async(process_data, "item_123")

# Run worker (separate process)
async def worker():
    await app.run()  # Runs forever

if __name__ == "__main__":
    asyncio.run(main())
```

### Environment Variables

```bash
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net/"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"
```

## Key Features

- **Async-only**: Built for async Python applications
- **Retry Policies**: Exponential, linear, and fixed backoff strategies
- **Task Chaining**: Success/failure callbacks and workflows

## Documentation

📖 **[Complete Documentation](https://mulliganfunding.github.io/boilermaker-servicebus/)**
- **[Getting Started](https://mulliganfunding.github.io/boilermaker-servicebus/)** - Installation and basic usage
- **[Callbacks & Chains](https://mulliganfunding.github.io/boilermaker-servicebus/guides/callbacks-chains/)** - Task workflows
- **[Retry Policies](https://mulliganfunding.github.io/boilermaker-servicebus/guides/retry-policies/)** - Error handling and retries
- **[API Reference](https://mulliganfunding.github.io/boilermaker-servicebus/reference/boilermaker/)** - Complete API docs

## Requirements

- Python 3.11+
- Azure ServiceBus namespace and queue
- All task arguments must be JSON-serializable
