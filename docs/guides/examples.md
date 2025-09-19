# Examples and Patterns

This guide provides practical examples demonstrating how to use Boilermaker for various async task scenarios. All examples use Azure Service Bus as the message broker.

## Prerequisites

Before running any examples, you'll need:

1. **Azure Service Bus namespace and queue configured**
2. **Environment variables set:**
   ```bash
   export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net/"
   export SERVICE_BUS_QUEUE_NAME="your-queue-name"
   ```
3. **Azure authentication configured** (Azure CLI, service principal, or managed identity)

## Basic Task Publishing and Processing

### Simple Background Task

The most basic pattern involves creating tasks and publishing them to the queue:

```python
import asyncio
import os
from boilermaker import retries
from boilermaker.app import Boilermaker
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Configuration from environment
conf = Config(
    service_bus_namespace_url=os.environ["SERVICE_BUS_NAMESPACE_URL"],
    service_bus_queue_name=os.environ["SERVICE_BUS_QUEUE_NAME"],
)
service_bus_client = AzureServiceBus.from_config(conf)

# Application state object
class App:
    def __init__(self, data):
        self.data = data

# Background task function
async def background_task(state, message, urgent=False):
    """Process a background task.

    Args:
        state: Application state (injected automatically)
        message: Task message to process
        urgent: Whether this is urgent processing
    """
    print(f"Processing: {message} (urgent: {urgent})")

    # Access application state
    config_value = await state.data.get("key")
    print(f"App config: {config_value}")

    # Simulate some work
    await asyncio.sleep(1)
    print("Task completed!")

# Create worker and register task
worker = Boilermaker(
    App({"key": "value"}),
    service_bus_client=service_bus_client
)
worker.register_async(background_task, policy=retries.RetryPolicy.default())

# Publish tasks
async def publish_tasks():
    # Simple task
    await worker.apply_async(background_task, "Hello World")

    # Task with keyword arguments
    await worker.apply_async(
        background_task,
        "Urgent message",
        urgent=True
    )

# Run the publisher
if __name__ == "__main__":
    asyncio.run(publish_tasks())
```

### Running the Worker

To process the tasks, run the worker in a separate process:

```python
# worker.py
import asyncio
from boilermaker.app import Boilermaker
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Same setup as publisher...
conf = Config()
service_bus_client = AzureServiceBus.from_config(conf)
worker = Boilermaker(App({"key": "value"}), service_bus_client)
worker.register_async(background_task)

# Start processing
async def main():
    await worker.run()  # Runs forever

if __name__ == "__main__":
    asyncio.run(main())
```

## Callbacks and Workflows

### Success and Failure Callbacks

Tasks can have callbacks that execute based on success or failure:

```python
from boilermaker.failure import TaskFailureResult

async def main_task(state, action: str):
    """Main task that can succeed or fail."""
    if action == "success":
        print("Main task completed successfully")
        return "success_result"
    elif action == "fail":
        print("Main task failed gracefully")
        return TaskFailureResult
    else:
        raise ValueError("Unexpected error occurred!")

async def success_callback(state):
    """Called when main task succeeds."""
    print("Success callback executed!")

async def failure_callback(state):
    """Called when main task fails."""
    print("Failure callback executed!")

# Register all tasks
worker.register_async(main_task, policy=retries.NoRetry())
worker.register_async(success_callback, policy=retries.NoRetry())
worker.register_async(failure_callback, policy=retries.NoRetry())

# Create task with callbacks
async def create_workflow():
    # Create main task
    task = worker.create_task(main_task, "success")

    # Add callbacks
    task.on_success = worker.create_task(success_callback)
    task.on_failure = worker.create_task(failure_callback)

    # Publish the workflow
    await worker.publish_task(task)
```

### Task Chaining

Chain multiple tasks together using the `>>` operator:

```python
async def step1(state, data):
    print(f"Step 1: Processing {data}")
    return f"processed_{data}"

async def step2(state, data):
    print(f"Step 2: Validating {data}")
    return f"validated_{data}"

async def step3(state, data):
    print(f"Step 3: Finalizing {data}")
    return f"final_{data}"

# Register all steps
worker.register_async(step1)
worker.register_async(step2)
worker.register_async(step3)

# Create chained workflow
async def create_chain():
    task1 = worker.create_task(step1, "input_data")
    task2 = worker.create_task(step2, "step1_output")
    task3 = worker.create_task(step3, "step2_output")

    # Chain tasks: step1 >> step2 >> step3
    task1 >> task2 >> task3

    # Publish the first task (chain executes automatically)
    await worker.publish_task(task1)
```

## Retry Policies and Error Handling

### Custom Retry Policies

Different tasks may need different retry behaviors:

```python
from boilermaker.retries import RetryPolicy, RetryMode

# Network task with exponential backoff
network_policy = RetryPolicy(
    max_tries=5,
    delay=30,  # Start with 30s
    delay_max=600,  # Cap at 10 minutes
    retry_mode=RetryMode.Exponential
)

# Database task with linear backoff
database_policy = RetryPolicy(
    max_tries=3,
    delay=60,  # 60s intervals
    delay_max=180,  # Cap at 3 minutes
    retry_mode=RetryMode.Linear
)

# Critical task with fixed short intervals
critical_policy = RetryPolicy(
    max_tries=10,
    delay=5,  # Every 5 seconds
    delay_max=5,
    retry_mode=RetryMode.Fixed
)

async def network_task(state, url):
    """Task that makes network requests."""
    # Simulate network operation
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def database_task(state, query):
    """Task that performs database operations."""
    # Simulate database operation
    print(f"Executing query: {query}")
    return "query_result"

# Register with different policies
worker.register_async(network_task, policy=network_policy)
worker.register_async(database_task, policy=database_policy)
```

### Retry Exceptions

Use retry exceptions to dynamically control retry behavior:

```python
from boilermaker.retries import (
    RetryException,
    RetryExceptionDefault,
    RetryExceptionDefaultExponential
)

async def adaptive_task(state, operation_type):
    """Task that chooses retry policy based on error type."""

    if operation_type == "network":
        # For network issues, use exponential backoff
        if network_is_down():
            raise RetryExceptionDefaultExponential("Network temporarily unavailable")

    elif operation_type == "rate_limit":
        # For rate limiting, use custom aggressive retry
        custom_policy = RetryPolicy(
            max_tries=10,
            delay=60,
            retry_mode=RetryMode.Exponential
        )
        raise RetryException("Rate limited", policy=custom_policy)

    elif operation_type == "temporary":
        # For temporary issues, use default retry
        raise RetryExceptionDefault("Temporary issue occurred")

    # Success case
    return f"Completed {operation_type} operation"
```

## Advanced Patterns

### Batch Processing

Process multiple items with error isolation:

```python
async def process_batch(state, items):
    """Process a batch of items with individual error handling."""
    results = []

    for item in items:
        try:
            result = await process_item(state, item)
            results.append({"item": item, "status": "success", "result": result})
        except Exception as e:
            results.append({"item": item, "status": "error", "error": str(e)})

    return results

async def process_item(state, item):
    """Process a single item."""
    # Simulate processing
    await asyncio.sleep(0.1)
    return f"processed_{item}"

# Create batch processing task
async def submit_batch():
    items = ["item1", "item2", "item3", "item4", "item5"]
    await worker.apply_async(process_batch, items)
```

### Conditional Workflows

Create workflows that branch based on conditions:

```python
async def analyze_data(state, data):
    """Analyze data and determine next steps."""
    analysis_result = perform_analysis(data)

    if analysis_result["confidence"] > 0.8:
        return {"next_step": "high_confidence", "data": analysis_result}
    elif analysis_result["confidence"] > 0.5:
        return {"next_step": "medium_confidence", "data": analysis_result}
    else:
        return {"next_step": "low_confidence", "data": analysis_result}

async def high_confidence_task(state, analysis_data):
    """Handle high confidence results."""
    print("Processing with high confidence path")

async def medium_confidence_task(state, analysis_data):
    """Handle medium confidence results."""
    print("Processing with medium confidence path")

async def low_confidence_task(state, analysis_data):
    """Handle low confidence results."""
    print("Processing with low confidence path")

# Register all tasks
worker.register_async(analyze_data)
worker.register_async(high_confidence_task)
worker.register_async(medium_confidence_task)
worker.register_async(low_confidence_task)

# Note: Conditional branching currently requires manual task creation
# based on the analysis result. Future versions may support automatic
# conditional chaining.
```

### Monitoring and Observability

Use the built-in tracing for monitoring:

```python
from opentelemetry import trace

async def monitored_task(state, task_id):
    """Task with custom tracing and monitoring."""
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("custom_operation") as span:
        span.set_attribute("task.id", task_id)
        span.set_attribute("task.type", "monitored")

        try:
            # Simulate work
            await asyncio.sleep(1)
            result = f"completed_{task_id}"

            span.set_attribute("task.result", result)
            span.set_status(trace.Status(trace.StatusCode.OK))

            return result

        except Exception as e:
            span.set_status(trace.Status(
                trace.StatusCode.ERROR,
                str(e)
            ))
            raise
```

## Testing Patterns

### Unit Testing Tasks

Test your task functions independently:

```python
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_background_task():
    """Test a background task function."""
    # Mock application state
    mock_state = AsyncMock()
    mock_state.data = {"key": "test_value"}

    # Test the task function directly
    result = await background_task(mock_state, "test_message", urgent=True)

    # Verify behavior
    assert result is not None
    mock_state.data.get.assert_called_with("key")

@pytest.mark.asyncio
async def test_task_with_callbacks():
    """Test task workflow with callbacks."""
    # Create test worker with mock service bus
    mock_service_bus = AsyncMock()
    test_worker = Boilermaker({"test": "state"}, mock_service_bus)

    # Register test tasks
    test_worker.register_async(success_callback)
    test_worker.register_async(failure_callback)

    # Test task creation and callback assignment
    task = test_worker.create_task(success_callback)
    assert task.function_name == "success_callback"
    assert task.on_success is None
    assert task.on_failure is None
```

## Best Practices

1. **Keep tasks idempotent** - Tasks should produce the same result when run multiple times
2. **Use appropriate retry policies** - Match retry behavior to the failure characteristics
3. **Handle errors gracefully** - Use TaskFailureResult for expected failures
4. **Monitor task performance** - Use the built-in tracing capabilities
5. **Test task functions** - Unit test your task logic independently of the queue
6. **Use application state** - Store shared resources in the application state object
7. **Keep tasks focused** - Each task should have a single responsibility
8. **Plan for failure** - Always consider what happens when tasks fail

## Running Examples

To run the provided examples:

```bash
# Set environment variables
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net/"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"

# Run the basic example
python examples/basic.py

# Run the callbacks example
python examples/callbacks.py

# In separate terminals, run workers to process tasks
python -c "
import asyncio
from examples.basic import worker
asyncio.run(worker.run())
"
```

For more complex scenarios, refer to the [Callbacks and Chains](callbacks-chains.md) and [Retry Policies](retry-policies.md) guides.