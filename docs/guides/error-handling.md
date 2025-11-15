# Error Handling and Retries

Handle task failures with retries, callbacks, and dead letter queues.

## Retrying Tasks

Configure retry behavior when registering tasks:

```python
from boilermaker import retries

# Exponential backoff
@app.task(policy=retries.RetryPolicy(
    max_tries=5,
    delay=30,
    delay_max=600,
    retry_mode=retries.RetryMode.Exponential
))
async def task_with_retries(state, data: str):
    """Task that retries on failure."""
    # Task logic here
    pass

# No retries for critical operations
@app.task(policy=retries.NoRetry())
async def no_retry_task(state, payment_id: str):
    """Task that should not retry (e.g., payments)."""
    pass
```

### Retry Modes

```python
# Fixed delay (same each time)
retries.RetryMode.Fixed

# Linear backoff (increases steadily)
retries.RetryMode.Linear

# Exponential backoff (doubles each time)
retries.RetryMode.Exponential
```

### Triggering Retries

Tasks only retry when they raise `RetryException`:

```python
from boilermaker.retries import RetryException

async def api_task(state, endpoint: str):
    try:
        response = await state.http_client.get(endpoint)
        return response.json()
    except ConnectionError:
        # This will trigger retry
        raise RetryException("Connection failed, will retry")
    except ValidationError:
        # This won't retry - permanent failure
        raise  # This won't retry

```

## Failure Callbacks

Chain tasks for error handling workflows.

!!! note "Results not propagated
    **Results are not automatically passed between tasks.**

```python
from boilermaker.task import Task

@app.task()
async def process_data(state, job_id: str):
    """Main task that may fail."""
    try:
        # Processing logic
        result = await process_job(job_id)
        state.jobs[job_id] = {"status": "completed", "result": result}
        return result
    except Exception as e:
        state.jobs[job_id] = {"status": "failed", "error": str(e)}
        raise

@app.task()
async def handle_success(state, job_id: str):
    """Success callback using job_id to find results."""
    job_info = state.jobs[job_id]
    # Send success notification
    await notify_user(job_id, "completed")
    return {"notified": True}

@app.task()
async def handle_failure(state, job_id: str, error_msg: str):
    """Failure callback."""
    # Log error and notify admin
    await notify_admin(f"Job {job_id} failed: {error_msg}")
    return {"admin_notified": True}

# Set up task chain with callbacks
job_id = "job_123"
main_task = Task.si(process_data, job_id)
success_task = Task.si(handle_success, job_id)
failure_task = Task.si(handle_failure, job_id, "Processing failed")

# Chain success and failure paths
main_task.success_callback = success_task
main_task.failure_callback = failure_task

await app.apply_async_task(main_task)
```

## Dead Letter Queues

Configure dead letter behavior:

```python
@app.task(
    policy=retries.RetryPolicy(max_tries=3),
    should_dead_letter=True  # Send failed messages to dead letter queue
)
async def important_task(state, data: dict):
    """Task where failures should be investigated."""
    pass

@app.task(
    should_dead_letter=False  # Don't dead letter, just drop
)
async def optional_task(state, data: dict):
    """Task where failures can be ignored."""
    pass
```

## Message Lock Renewal for Long Tasks

For long-running tasks that may exceed the Azure Service Bus message lock duration, renew the lock periodically:

```python
@app.task()
async def long_running_task(state, data_file: str):
    """Process large dataset with lock renewal."""
    items = await load_large_dataset(data_file)

    for i, item in enumerate(items):
        await process_item(item)

        # Renew lock every 100 items to prevent timeout
        if i % 100 == 0:
            await state.app.renew_message_lock()

    return f"Processed {len(items)} items"
```

!!! note "More information"
    Use message lock renewal for tasks that take longer than the message-lease duration for your queue.

    Consult the [Azure documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) for more information.
