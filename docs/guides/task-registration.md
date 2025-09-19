# Task Registration

Register async functions as background tasks using the `@app.task()` decorator or the method `app.register_async()`.

## Basic Registration

```python
from boilermaker import Boilermaker
from boilermaker.config import Config
from boilermaker.service_bus import AzureServiceBus

# Set up your app
config = Config()
service_bus = AzureServiceBus.from_config(config)
app = Boilermaker(state={}, service_bus=service_bus)

# Can also dynamically register with `app.register_async(send_email)
@app.task()
async def send_email(state, recipient: str, subject: str, body: str):
    """Send an email."""
    print(f"Sending email to {recipient}: {subject}")
    return {"status": "sent"}
```

## Registration with Retry Policy

```python
from boilermaker import retries

# Exponential backoff policy
@app.task(policy=retries.RetryPolicy(
    max_tries=5,
    delay=30,
    delay_max=600,
    retry_mode=retries.RetryMode.Exponential
))
async def process_payment(state, payment_id: str, amount: float):
    """Process a payment with retries on failure."""
    # Payment processing logic
    return {"payment_id": payment_id, "processed": True}
```

## Requirements

Task functions must:

- Be `async` functions
- Accept `state` as the first parameter
- Use JSON-serializable parameters (str, int, float, bool, dict, list, None)

## Example

```python
@app.task()
async def send_notification(state, user_id: int, message: str, channel: str = "email"):
    """Send notification to user."""
    print(f"Sending {channel} notification to user {user_id}: {message}")
    return {"sent": True, "channel": channel}
```