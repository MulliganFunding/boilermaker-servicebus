# Retry Policies

Configure how tasks retry on failure.

## Basic Setup

```python
from boilermaker import retries

# Default retry policy
policy = retries.RetryPolicy.default()

# Custom policy
policy = retries.RetryPolicy(
    max_tries=3,
    delay=60,
    retry_mode=retries.RetryMode.Exponential
)

# Register task with policy
app.register_async(my_task, policy=policy)
```

## Retry Modes

```python
# Fixed delay (same each time)
retries.RetryMode.Fixed

# Linear backoff (increases steadily)
retries.RetryMode.Linear

# Exponential backoff (doubles each time)
retries.RetryMode.Exponential
```

## Triggering Retries

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

