# Quick Start

## Installation and Setup

Install Boilermaker:

```bash
pip install "boilermaker-servicebus"
```

Set up your environment variables:

```bash
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"
```

## Create Your App

Create `app.py`:

```py
import asyncio
import os
from boilermaker import Boilermaker, retries
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Configure connection
config = Config(
    service_bus_namespace_url=os.environ["SERVICE_BUS_NAMESPACE_URL"],
    service_bus_queue_name=os.environ["SERVICE_BUS_QUEUE_NAME"],
)
service_bus = AzureServiceBus.from_config(config)

# Application state (shared across tasks)
class AppState:
    def __init__(self):
        self.processed_count = 0
        self.items = []

# Create the Boilermaker app
app = Boilermaker(AppState(), service_bus)

# Define background tasks
@app.task()
async def send_email(state, recipient: str, subject: str, body: str):
    """Send an email."""
    print(f"📧 Sending email to {recipient}: {subject}")
    state.processed_count += 1
    return {"sent": True, "recipient": recipient}

@app.task(policy=retries.RetryPolicy(max_tries=3, delay=30))
async def process_payment(state, user_id: int, amount: float):
    """Process a payment with retries."""
    print(f"💳 Processing ${amount} payment for user {user_id}")
    state.processed_count += 1
    return {"status": "completed", "user_id": user_id}
```

## Schedule Tasks

Run your app to schedule some tasks:

```py
async def schedule_tasks():
    """Schedule some background tasks."""
    # Schedule individual tasks
    await app.apply_async(send_email, "user@example.com", "Welcome!", "Thanks for signing up!")
    await app.apply_async(process_payment, 123, 99.99)
    print("📤 Tasks scheduled!")

if __name__ == "__main__":
    asyncio.run(schedule_tasks())
```

Test it:

```bash
python app.py
```

## Run the Worker

Create `worker.py` to process the queued tasks:

```py
import asyncio
from app import app  # Import your configured app

async def main():
    print("🚀 Starting Boilermaker worker...")
    await app.run()  # This processes tasks from the queue

if __name__ == "__main__":
    asyncio.run(main())
```

Run the worker in a separate terminal:

```bash
python worker.py
```

You should see your tasks being processed:
```
🚀 Starting Boilermaker worker...
📧 Sending email to user@example.com: Welcome!
💳 Processing $99.99 payment for user 123
```

## Task Chaining

Chain tasks using callbacks.

!!! warning "Note"
    - Task args and kwargs must be specified up-front: `Task.si()` creates an _immutable_ signature, binding args and kwargs to a background task before publishing it.
    - Results are not automatically passed between tasks.

```py
from boilermaker.task import Task

@app.task()
async def download_file(state, url: str, job_id: str):
    """Download a file."""
    print(f"📥 Downloading {url}")
    state.items.append({"job_id": job_id, "status": "downloaded", "url": url})
    return {"downloaded": True}

@app.task()
async def process_file(state, job_id: str):
    """Process the downloaded file."""
    job_data = next(item for item in state.items if item["job_id"] == job_id)
    job_data["status"] = "processed"
    print(f"⚙️ Processing file for job {job_id}")
    return {"processed": True}

# Chain tasks together
async def schedule_workflow():
    job_id = "job_123"

    download_task = Task.si(download_file, "https://example.com/file.txt", job_id)
    process_task = Task.si(process_file, job_id)

    # Set up callback chain
    download_task >> process_task

    # Schedule the workflow
    await app.apply_async_task(download_task)
```


## What's Next?

Explore these core concepts and features:

- **[Basic Concepts](basic-concepts.md)** - Understand tasks, state, and policies
- **[Task Registration](../guides/task-registration.md)** - Different ways to register tasks
- **[Error Handling](../guides/error-handling.md)** - Robust error management
- **[Callbacks & Chains](../guides/callbacks-chains.md)** - Chain tasks together

## Common Patterns

=== "Add Retry Logic"
    ```py
    @app.task(policy=retries.RetryPolicy(
        max_tries=3,
        delay=30,
        retry_mode=retries.RetryMode.Exponential
    ))
    async def unreliable_task(state, data: str):
        # This task will retry up to 3 times with exponential backoff
        if data == "fail":
            raise retries.RetryException("Temporary failure")
        return "Success!"
    ```

=== "Handle Failures"
    ```py
    from boilermaker.failure import TaskFailureResult

    @app.task()
    async def validate_data(state, data: dict):
        if not data.get("email"):
            return TaskFailureResult  # Don't retry, permanent failure
        return process_user_data(data)

    @app.task()
    async def handle_failure(state, error_info: str):
        print(f"Task failed: {error_info}")
        # Send alert, log error, etc.

    # Set up failure callback
    main_task = Task.si(validate_data, {"name": "John"})  # Missing email
    failure_task = Task.si(handle_failure, "Validation failed")
    main_task.failure_callback = failure_task

    await app.apply_async_task(main_task)
    ```

=== "Testing Tasks"
    ```py
    import pytest
    from unittest.mock import AsyncMock

    class MockState:
        def __init__(self):
            self.processed_count = 0

    @pytest.mark.asyncio
    async def test_send_email():
        state = MockState()
        result = await send_email(state, "test@example.com", "Test", "Body")

        assert result["sent"] is True
        assert result["recipient"] == "test@example.com"
        assert state.processed_count == 1
    ```