# Retry Policies

Retry policies in Boilermaker provide powerful and flexible ways to handle task failures. You can configure how tasks retry, when they retry, and when to give up and run failure callbacks.

## Overview

Boilermaker's retry system supports:

- **Three retry modes**: Fixed, Linear, and Exponential backoff
- **Configurable retry counts and delays**
- **Multiple ways to configure retries**: at registration, scheduling, or runtime
- **Exception-driven retries**: Tasks only retry when they raise `RetryException`

## Retry Policy Structure

```python
from boilermaker import retries

policy = retries.RetryPolicy(
    max_tries=5,                           # Maximum number of attempts
    delay=60,                              # Base delay in seconds
    delay_max=3600,                        # Maximum delay (cap)
    retry_mode=retries.RetryMode.Fixed     # Retry mode
)
```

### Retry Modes

#### Fixed Delay

Same delay between each retry:

```python
policy = retries.RetryPolicy(
    max_tries=3,
    delay=30,                              # Always 30 seconds
    retry_mode=retries.RetryMode.Fixed
)
# Delays: 30s, 30s, 30s
```

#### Linear Backoff

Delay increases linearly with attempt number:

```python
policy = retries.RetryPolicy(
    max_tries=4,
    delay=30,                              # Base delay
    retry_mode=retries.RetryMode.Linear
)
# Delays: 30s, 60s, 90s, 120s
```

#### Exponential Backoff

Delay doubles each time (with jitter to prevent thundering herd):

```python
policy = retries.RetryPolicy(
    max_tries=5,
    delay=30,                              # Starting delay
    delay_max=600,                         # Cap at 10 minutes
    retry_mode=retries.RetryMode.Exponential
)
# Delays: ~30s, ~60s, ~120s, ~240s, ~480s (with random jitter)
```

## How Retries Work

### 1. Tasks Only Retry on RetryException

**Important**: Tasks only retry when they raise a `RetryException`. Other exceptions are treated as permanent failures.

```python
from boilermaker.retries import RetryException

async def api_task(state, endpoint: str):
    try:
        response = await state.http_client.get(endpoint)
        return response.json()
    except ConnectionError:
        # Network issues are usually temporary
        raise RetryException("Connection failed, will retry")
    except ValidationError:
        # Validation errors are permanent
        raise  # Don't retry
```

### 2. Failure Callbacks Run After All Retries

Failure callbacks only execute after **all retries have been exhausted**:

```python
async def unreliable_task(state):
    if random.random() < 0.7:  # 70% failure rate
        raise RetryException("Random failure")
    return "Success!"

async def cleanup_task(state):
    print("Task failed permanently, cleaning up...")

# Set up task with failure callback
task = worker.create_task(unreliable_task)
task.on_failure = worker.create_task(cleanup_task)

await worker.publish_task(task)
```

## Three Ways to Configure Retries

### 1. Default Policy at Registration

Set a default policy when registering the task:

```python
from boilermaker import retries

async def flaky_api_call(state, url: str):
    response = await state.http_client.get(url)
    if response.status_code >= 500:
        raise retries.RetryException("Server error, retrying...")
    return response.json()

# Register with default retry policy
worker.register_async(
    flaky_api_call,
    policy=retries.RetryPolicy(
        max_tries=3,
        delay=60,
        retry_mode=retries.RetryMode.Exponential
    )
)

# All invocations will use this policy by default
await worker.apply_async(flaky_api_call, "https://api.example.com/data")
```

### 2. Override Policy When Scheduling

Override the default policy for a specific task invocation:

```python
# Use the same task but with different retry behavior
critical_policy = retries.RetryPolicy(
    max_tries=10,                          # More attempts for critical tasks
    delay=30,
    delay_max=300,
    retry_mode=retries.RetryMode.Linear
)

# This specific invocation uses the critical policy
await worker.apply_async(
    flaky_api_call,
    "https://critical-api.example.com/data",
    policy=critical_policy
)
```

### 3. Dynamic Policy from Exception

Change the retry policy from within the task:

```python
async def adaptive_task(state, complexity: str):
    try:
        if complexity == "simple":
            return await simple_operation()
        else:
            return await complex_operation()
    except SimpleError:
        # Use default policy for simple errors
        raise retries.RetryException("Simple error occurred")
    except ComplexError:
        # Use aggressive retry for complex operations
        raise retries.RetryExceptionDefaultExponential(
            "Complex operation failed",
            max_tries=8,
            delay=120,
            delay_max=1800  # 30 minutes max
        )
```

## Common Retry Patterns

### API Integration

```python
async def external_api_task(state, endpoint: str, payload: dict):
    """Task for calling external APIs with appropriate retry logic."""
    try:
        response = await state.http_client.post(endpoint, json=payload)

        if response.status_code == 429:  # Rate limited
            raise retries.RetryExceptionDefaultLinear(
                "Rate limited",
                delay=60,  # Wait longer for rate limits
                max_tries=5
            )
        elif response.status_code >= 500:  # Server error
            raise retries.RetryException("Server error")
        elif response.status_code >= 400:  # Client error
            raise ValueError(f"Client error: {response.status_code}")  # Don't retry

        return response.json()

    except ConnectionError:
        raise retries.RetryException("Connection failed")
    except TimeoutError:
        raise retries.RetryException("Request timed out")

# Register with exponential backoff for server errors
worker.register_async(
    external_api_task,
    policy=retries.RetryPolicy(
        max_tries=5,
        delay=30,
        delay_max=600,
        retry_mode=retries.RetryMode.Exponential
    )
)
```

### Database Operations

```python
async def database_operation(state, query: str, params: dict):
    """Database task with retry for transient failures."""
    try:
        async with state.db.transaction():
            result = await state.db.execute(query, params)
            return result

    except DeadlockError:
        # Deadlocks are usually resolved quickly
        raise retries.RetryExceptionDefaultExponential(
            "Database deadlock",
            delay=5,
            delay_max=60,
            max_tries=6
        )
    except ConnectionError:
        # Connection issues might take longer to resolve
        raise retries.RetryException("Database connection failed")
    except IntegrityError:
        # Constraint violations are permanent
        raise  # Don't retry

worker.register_async(
    database_operation,
    policy=retries.RetryPolicy(
        max_tries=3,
        delay=10,
        retry_mode=retries.RetryMode.Linear
    )
)
```

### File Processing

```python
async def process_file(state, file_path: str):
    """Process a file with retry for I/O issues."""
    try:
        content = await state.storage.read_file(file_path)
        processed = await process_content(content)
        await state.storage.write_file(f"{file_path}.processed", processed)
        return len(processed)

    except FileNotFoundError:
        # File doesn't exist - permanent error
        raise ValueError(f"File not found: {file_path}")
    except PermissionError:
        # Permission issues might be temporary (especially in cloud storage)
        raise retries.RetryException("Permission denied")
    except IOError:
        # I/O errors are often temporary
        raise retries.RetryException("I/O error occurred")

# Use linear backoff for I/O operations
worker.register_async(
    process_file,
    policy=retries.RetryPolicy(
        max_tries=4,
        delay=15,
        retry_mode=retries.RetryMode.Linear
    )
)
```

## Pre-configured Policies

Boilermaker provides several pre-configured policies:

### Default Policy

```python
# Moderate retry with fixed delay
policy = retries.RetryPolicy.default()
# Equivalent to:
# RetryPolicy(max_tries=5, delay=120, delay_max=120, retry_mode=Fixed)
```

### No Retry

```python
# Never retry - useful for tasks that should only run once
policy = retries.NoRetry()
# Equivalent to:
# RetryPolicy(max_tries=1, delay=5, delay_max=5, retry_mode=Fixed)

worker.register_async(send_notification, policy=retries.NoRetry())
```

### Convenience Exception Classes

```python
# Default exponential backoff
raise retries.RetryExceptionDefaultExponential("Temporary failure")

# Default linear backoff
raise retries.RetryExceptionDefaultLinear("Temporary failure")

# Use default policy from task registration
raise retries.RetryExceptionDefault("Temporary failure")
```

## Best Practices

### 1. Choose Appropriate Retry Modes

- **Fixed**: Simple scenarios, predictable delays
- **Linear**: I/O operations, database transactions
- **Exponential**: API calls, network operations (prevents overwhelming external services)

### 2. Set Reasonable Limits

```python
# ✅ Good: Reasonable limits
policy = retries.RetryPolicy(
    max_tries=5,        # Don't retry forever
    delay=30,           # Not too frequent
    delay_max=600       # Cap at 10 minutes
)

# ❌ Avoid: Excessive retries
policy = retries.RetryPolicy(
    max_tries=50,       # Too many attempts
    delay=1,            # Too frequent
    delay_max=7200      # 2 hours is too long
)
```

### 3. Distinguish Error Types

```python
async def smart_retry_task(state, data):
    try:
        return await risky_operation(data)
    except ValidationError:
        # Permanent error - don't retry
        raise
    except RateLimitError:
        # Temporary but needs longer delays
        raise retries.RetryExceptionDefaultLinear("Rate limited", delay=120)
    except NetworkError:
        # Temporary, exponential backoff appropriate
        raise retries.RetryException("Network error")
```

### 4. Monitor and Alert

```python
async def monitored_task(state, operation_id: str):
    try:
        result = await perform_operation(operation_id)
        # Track success metrics
        state.metrics.increment("task.success")
        return result
    except Exception as e:
        # Track failure metrics
        state.metrics.increment("task.failure")

        if isinstance(e, CriticalError):
            # Alert on critical errors
            await state.alerts.send_alert(f"Critical error in {operation_id}: {e}")

        # Decide whether to retry
        if isinstance(e, TransientError):
            raise retries.RetryException(f"Transient error: {e}")
        else:
            raise  # Permanent failure
```

### 5. Test Retry Behavior

```python
# Test your retry logic with controlled failures
async def test_retry_behavior():
    failure_count = 0

    async def failing_task(state):
        nonlocal failure_count
        failure_count += 1

        if failure_count < 3:
            raise retries.RetryException(f"Failure {failure_count}")
        return "Success after retries"

    worker.register_async(failing_task, policy=retries.RetryPolicy(max_tries=5))

    result = await worker.apply_async(failing_task)
    # Task should succeed on the 3rd attempt
```

## Troubleshooting

### Common Issues

1. **Tasks not retrying**: Make sure you're raising `RetryException`, not other exceptions
2. **Retries taking too long**: Check your `delay_max` settings
3. **Infinite retries**: Ensure you have reasonable `max_tries` limits
4. **Failure callbacks not running**: They only run after ALL retries are exhausted

### Debugging Retry Behavior

Enable debug logging to see retry behavior:

```python
import logging
logging.getLogger("boilermaker.app").setLevel(logging.DEBUG)

# You'll see logs like:
# "Retry attempt 2 of 5 for task_name, next delay: 60s"
```

## Next Steps

- **[Error Handling](error-handling.md)** - Comprehensive error handling strategies
- **[Callbacks & Chains](callbacks-chains.md)** - Combine retries with workflows
- **[Examples](../examples/advanced-patterns.md)** - See retry patterns in action