# Boilermaker ServiceBus

**Async Python background tasks with Azure ServiceBus**

[![Tests](https://github.com/MulliganFunding/boilermaker-servicebus/workflows/Tests/badge.svg)](https://github.com/MulliganFunding/boilermaker-servicebus/actions)
[![PyPI version](https://badge.fury.io/py/boilermaker-servicebus.svg)](https://badge.fury.io/py/boilermaker-servicebus)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

Boilermaker is a lightweight task runner exclusively for **async Python** and Azure Service Bus Queues. It's designed for simplicity and focuses specifically on Azure ServiceBus integration.


## Features

- **Async-only**: Built specifically for async Python applications
- **Retry Policies**: Configurable retry strategies with exponential, linear, and fixed backoff
- **Task Chaining**: Chain tasks together with success/failure callbacks
- **Observability**: Built-in OpenTelemetry tracing support

## Tradeoffs

Here are some decisions we made in this library, which may give you pause in considering it for your own services:

- **Azure ServiceBus Queues**: We needed something for Azure ServiceBus Queues so we are deeply integrated with that service (and the features we use: message-scheduling, dead-lettering).
- **JSON Serialization**: All task arguments must be JSON-serializable
- **No DAGs**: Nothing like Chords or Groups (from Celery) present at this time.
- **No ServiceBus Message in Handlers**: We require some `state` as the first arg for all background tasks, but we don't bind the message itself or pass it.

!!! note "Why Not Boilermaker?"
    If you need a fully-featured task runner with multiple backends, consider [Celery](https://github.com/celery/celery/tree/main) instead. Boilermaker is purpose-built for Azure ServiceBus with a focus on simplicity and async Python.


## Quick Start

### Installation

```bash
pip install "boilermaker-servicebus"
```

### Environment Setup

Set your Azure ServiceBus configuration:

```bash
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net/"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"
```

### Basic Usage

Create your first background task:

```python
import asyncio
from boilermaker import Boilermaker, retries
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Configure Azure ServiceBus
config = Config()  # Loads from environment variables
service_bus = AzureServiceBus.from_config(config)

# Create application with shared state
class AppState:
    def __init__(self):
        self.data = {"processed_count": 0}

# Initialize Boilermaker
app = Boilermaker(AppState(), service_bus)

# Define a background task
async def process_data(state, data_id, priority="normal"):
    """Process data in the background.

    Args:
        state: Application state (injected automatically)
        data_id: ID of data to process
        priority: Processing priority level
    """
    print(f"Processing data {data_id} with priority {priority}")

    # Access shared state
    state.data["processed_count"] += 1

    # Simulate processing
    await asyncio.sleep(1)
    return f"Processed {data_id}"

# Register the task with retry policy
app.register_async(process_data, policy=retries.RetryPolicy.default())

# Publish tasks
async def publish_tasks():
    # Simple task
    await app.apply_async(process_data, "data_123")

    # Task with keywords
    await app.apply_async(process_data, "data_456", priority="high")

# Run publisher
if __name__ == "__main__":
    asyncio.run(publish_tasks())
```

### Running Workers

In a separate process, run the worker to process tasks:

```python
import asyncio
from boilermaker import Boilermaker
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Same setup as above...
config = Config()
service_bus = AzureServiceBus.from_config(config)
app = Boilermaker(AppState(), service_bus)
app.register_async(process_data, policy=retries.RetryPolicy.default())

# Start the worker
async def run_worker():
    await app.run()  # Runs indefinitely

if __name__ == "__main__":
    asyncio.run(run_worker())
```

### Task Registration Requirements

All task functions in Boilermaker must:

1. **Be async functions** - `async def my_task(...)`
2. **Take `state` as first parameter** - The application state object
3. **Have JSON-serializable arguments** - All args/kwargs must be JSON-serializable
4. **Be registered before use** - `app.register_async(my_task, policy=...)`

!!! warning "Important Notes"
    - Boilermaker does not store or use task results
    - All task arguments must be JSON-serializable
    - The `state` parameter is injected automatically
    - Tasks run in separate processes from publishers

### Logging Output

When running workers, you'll see logs such as the following from the `boilermaker` logger:

```json
{"event": "Registered background function fn=process_data", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "[process_data] Begin Task sequence_number=19657", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
{"event": "[process_data] Completed Task sequence_number=19657 in 0.00021s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
```

## Learn More

- **[Getting Started](getting-started/installation.md)** - Installation and basic setup
- **[Quick Start Guide](getting-started/quickstart.md)** - Your first task in 5 minutes
- **[User Guide](guides/task-registration.md)** - Comprehensive feature documentation
- **[API Reference](api-reference/app.md)** - Complete API documentation
- **[Examples](examples/basic-usage.md)** - Real-world usage patterns

## Why the Name?

The name comes from a band named [Boilermaker](https://www.discogs.com/artist/632957-Boilermaker).
