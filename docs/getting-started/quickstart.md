# Quick Start

Get up and running with Boilermaker in under 5 minutes!

## Step 1: Install and Configure

First, install Boilermaker:

```bash
pip install "boilermaker-servicebus"
```

Set up your environment variables:

```bash
export SERVICE_BUS_NAMESPACE_URL="https://your-namespace.servicebus.windows.net"
export SERVICE_BUS_QUEUE_NAME="your-queue-name"
```

## Step 2: Create Your First Task

Create a file called `app.py`:

```python
import asyncio
import os
from boilermaker import Boilermaker, retries
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Configure ServiceBus connection
config = Config(
    service_bus_namespace_url=os.environ["SERVICE_BUS_NAMESPACE_URL"],
    service_bus_queue_name=os.environ["SERVICE_BUS_QUEUE_NAME"],
)
service_bus = AzureServiceBus.from_config(config)

# Application state (shared across all tasks)
class AppState:
    def __init__(self):
        self.processed_items = []
        self.total_processed = 0

# Create the Boilermaker app
app = Boilermaker(AppState(), service_bus)

# Define a background task
@app.task(policy=retries.RetryPolicy.default())
async def send_welcome_email(state, user_id: str, email: str):
    """Send a welcome email to a new user."""
    # Simulate email sending
    print(f"📧 Sending welcome email to {email} for user {user_id}")

    # Update shared state
    state.processed_items.append(f"email:{user_id}")
    state.total_processed += 1

    print(f"✅ Email sent! Total processed: {state.total_processed}")
    return f"Email sent to {email}"

if __name__ == "__main__":
    # This is how you'd schedule tasks in your web app
    async def schedule_task():
        await app.apply_async(
            send_welcome_email,
            user_id="user123",
            email="newuser@example.com"
        )
        print("📤 Task scheduled!")

    asyncio.run(schedule_task())
```

## Step 3: Schedule a Task

Run the scheduler to queue a task:

```bash
python app.py
```

You should see:
```
📤 Task scheduled!
```

## Step 4: Run the Worker

In a **separate terminal** (or process), run the worker to process tasks:

```python
# worker.py
import asyncio
from app import app  # Import your configured app

async def run_worker():
    print("🚀 Starting Boilermaker worker...")
    await app.run()

if __name__ == "__main__":
    asyncio.run(run_worker())
```

Run the worker:

```bash
python worker.py
```

You should see:
```
🚀 Starting Boilermaker worker...
📧 Sending welcome email to newuser@example.com for user user123
✅ Email sent! Total processed: 1
```


## What's Next?

### Learn Core Concepts

- **[Basic Concepts](basic-concepts.md)** - Understand tasks, state, and policies
- **[Task Registration](../guides/task-registration.md)** - Different ways to register tasks
- **[Retry Policies](../guides/retry-policies.md)** - Handle failures gracefully

### Explore Advanced Features

- **[Callbacks & Chains](../guides/callbacks-chains.md)** - Chain tasks together
- **[Error Handling](../guides/error-handling.md)** - Robust error management
- **[Testing](../examples/testing.md)** - Test your background tasks

### Production Ready

- **[Azure Setup](../guides/azure-setup.md)** - Production Azure configuration
- **[Production Deployment](../guides/production-deployment.md)** - Deploy workers at scale
- **[Performance](../troubleshooting/performance.md)** - Optimize performance

## Common Next Steps

=== "Add Retry Logic"

    ```python
    @app.task(policy=retries.RetryPolicy(
        max_tries=3,
        delay=30,
        retry_mode=retries.RetryMode.Exponential
    ))
    async def unreliable_task(state, data):
        # This task will retry up to 3 times with exponential backoff
        if random.random() < 0.5:
            raise retries.RetryException("Temporary failure")
        return "Success!"
    ```

=== "Chain Tasks"

    ```python
    # Chain tasks to run in sequence
    task1 = app.create_task(process_data, "input")
    task2 = app.create_task(send_notification, "processed")

    # task2 runs only if task1 succeeds
    task1 >> task2

    await app.publish_task(task1)
    ```

=== "Handle Failures"

    ```python
    @app.task()
    async def main_task(state, data):
        if data == "bad":
            return TaskFailureResult  # Triggers failure callback
        return "processed"

    @app.task()
    async def failure_handler(state):
        print("Main task failed, sending alert...")

    # Set up failure callback
    main = app.create_task(main_task, "some_data")
    main.on_failure = app.create_task(failure_handler)

    await app.publish_task(main)
    ```