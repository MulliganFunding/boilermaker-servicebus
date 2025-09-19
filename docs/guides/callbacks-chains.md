# Callbacks & Chains

Boilermaker supports success or failure callbacks and chains, allowing you to build complex orkflows where tasks run in response to success or failure of other tasks.

## Overview

- **Success callbacks** run when a task completes successfully
- **Failure callbacks** run when a task fails (after all retries are exhausted)
- **Task chains** allow you to sequence multiple tasks together
- **Workflow patterns** can be combined for complex business logic

## Success and Failure Callbacks

### Basic Callback Setup

To use callbacks, you need to:

1. Register all tasks that will be used
2. Create task instances
3. Set up the callback relationships
4. Publish the initial task

```python
from boilermaker import Boilermaker, retries
from boilermaker.failure import TaskFailureResult

# Assume you have your worker set up...
# worker = Boilermaker(app_state, service_bus_client)

async def main_task(state, param: str):
    """A task that can succeed or fail based on input."""
    if param == "success":
        print("Main task completed successfully!")
        return "success_result"
    elif param == "fail":
        print("Main task failed permanently")
        return TaskFailureResult  # Permanent failure
    else:
        raise ValueError("Something went wrong!")  # Will trigger retries

async def success_handler(state):
    """Runs when main_task succeeds."""
    print("🎉 Success callback executed!")
    state.metrics["successful_workflows"] += 1

async def failure_handler(state):
    """Runs when main_task fails permanently."""
    print("😞 Failure callback executed!")
    state.metrics["failed_workflows"] += 1
    # Maybe send an alert, clean up resources, etc.

# Register all tasks
worker.register_async(main_task, policy=retries.NoRetry())
worker.register_async(success_handler, policy=retries.NoRetry())
worker.register_async(failure_handler, policy=retries.NoRetry())
```

### Setting Up Callbacks

There are **three** ways to set up callbacks:

#### Method 1: Direct Assignment

```python
# Create task instances
main = worker.create_task(main_task, "success")
success = worker.create_task(success_handler)
failure = worker.create_task(failure_handler)

# Set up callbacks
main.on_success = success
main.on_failure = failure

# Publish the main task
await worker.publish_task(main)
```

#### Method 2: Using the `>>` Operator

```python
# Create tasks and chain them
main = worker.create_task(main_task, "success")
success = worker.create_task(success_handler)

# Use >> for success callbacks
main >> success  # Same as: main.on_success = success

await worker.publish_task(main)
```


#### Method 2: Using the `chain` method

```python
from Boilermaker import Task
# Create tasks with bound args/kwargs and chain them
worflow = worker.chain(
    Task.si(main_task, "success"),
    Task.si(success_handler)
    on_failure=...
)
await worker.publish_task(workflow)
```

!!! tip "Callback Execution"
    - Success callbacks only run if the main task completes successfully
    - Failure callbacks only run after **all retries have been exhausted**
    - Callbacks are scheduled as new tasks in the queue

### Real-World Example: User Registration

```python
async def create_user(state, email: str, name: str):
    """Create a new user account."""
    try:
        user_id = await state.db.create_user(email, name)
        print(f"✅ User created: {user_id}")
        return user_id
    except DuplicateEmailError:
        return TaskFailureResult  # Don't retry duplicates

async def send_welcome_email(state, user_id: str = None):
    """Send welcome email to new user."""
    if user_id:
        await state.email_service.send_welcome(user_id)
        print(f"📧 Welcome email sent to user {user_id}")

async def handle_signup_failure(state, email: str = None):
    """Handle failed user creation."""
    if email:
        await state.analytics.track_failed_signup(email)
        print(f"❌ Signup failed for {email}")

# Set up the workflow
signup_task = worker.create_task(create_user, "user@example.com", "John Doe")
welcome_task = worker.create_task(send_welcome_email)
failure_task = worker.create_task(handle_signup_failure, email="user@example.com")

signup_task.on_success = welcome_task
signup_task.on_failure = failure_task

await worker.publish_task(signup_task)
```

## Task Chains

Task chains allow you to run multiple tasks in sequence, where each task only runs if the previous one succeeds.

### Basic Chain Syntax

```python
workflow = worker.chain(
    # create a Task from a function with bound args
    worker.create_task(step1, "input_data"),
    # alternately, use the Task.si classmethod
    Task.si(step2),
    worker.create_task(step3),
    # any unhandled exception or retries exhausted will fire this task
    on_failure=Task.si(cleanup_handler)
)

await worker.publish_task(workflow)
```

### Chain Behavior

- Tasks run **sequentially** (step2 only runs if step1 succeeds)
- If any task fails, the chain stops and the `on_failure` task runs
- The `on_failure` task is set for **all** tasks in the chain
- Chains return the first task, which you publish to start the workflow

### Example: Data Processing Pipeline

```python
async def fetch_data(state, source_url: str):
    """Fetch data from external source."""
    data = await state.http_client.get(source_url)
    print(f"📥 Fetched {len(data)} records")
    return data

async def validate_data(state):
    """Validate the fetched data."""
    # Data from previous task is not automatically passed
    # You'll need to store it in shared state or external storage
    print("✅ Data validation passed")

async def transform_data(state):
    """Transform data to target format."""
    print("🔄 Data transformed")

async def save_to_database(state):
    """Save processed data."""
    print("💾 Data saved to database")

async def send_notification(state):
    """Notify users of new data."""
    print("📬 Notification sent")

async def handle_pipeline_failure(state):
    """Clean up on pipeline failure."""
    print("🧹 Cleaning up failed pipeline")
    await state.cleanup_temp_files()

# Register all tasks
tasks = [fetch_data, validate_data, transform_data, save_to_database, send_notification, handle_pipeline_failure]
for task in tasks:
    worker.register_async(task, policy=retries.RetryPolicy.default())

# Create the pipeline
pipeline = worker.chain(
    worker.create_task(fetch_data, "https://api.example.com/data"),
    worker.create_task(validate_data),
    worker.create_task(transform_data),
    worker.create_task(save_to_database),
    worker.create_task(send_notification),
    on_failure=worker.create_task(handle_pipeline_failure)
)

await worker.publish_task(pipeline)
```

## Advanced Patterns

### Conditional Workflows

```python
async def process_order(state, order_id: str):
    """Process an order and determine next steps."""
    order = await state.db.get_order(order_id)

    if order.total > 1000:
        return "high_value"
    elif order.customer.is_premium:
        return "premium"
    else:
        return "standard"

async def high_value_processing(state):
    """Special processing for high-value orders."""
    print("💎 High-value order processing")

async def premium_processing(state):
    """Processing for premium customers."""
    print("⭐ Premium customer processing")

async def standard_processing(state):
    """Standard order processing."""
    print("📦 Standard processing")

# You'd need to implement conditional logic in your tasks
# since Boilermaker doesn't have built-in conditional routing
```

### Parallel Task Execution

While Boilermaker doesn't have built-in parallel execution, you can simulate it by scheduling multiple independent tasks:

```python
# Schedule multiple independent tasks
await worker.apply_async(process_email_batch, batch_1)
await worker.apply_async(process_email_batch, batch_2)
await worker.apply_async(process_email_batch, batch_3)

# These will be processed by different workers in parallel
```

## Best Practices

### 1. Keep Tasks Focused

```python
# ✅ Good: Focused, single responsibility
async def send_email(state, user_id: str, template: str):
    await state.email_service.send(user_id, template)

# ❌ Avoid: Too many responsibilities
async def process_user_signup(state, email, name, send_email=True, create_profile=True):
    # This task does too many things
    pass
```

### 2. Handle Data Passing

```python
# Boilermaker doesn't automatically pass results between chained tasks
# Use shared state or external storage:

async def fetch_data(state, url: str):
    data = await fetch_from_api(url)
    # Store in shared state or database
    state.cache[f"job_{state.current_job_id}"] = data
    return len(data)  # Return metadata, not the full data

async def process_data(state):
    # Retrieve data from shared state
    data = state.cache[f"job_{state.current_job_id}"]
    processed = transform(data)
    # Update the cached data
    state.cache[f"job_{state.current_job_id}"] = processed
```

### 3. Keep Args, Kwargs, and Chains **SMALL**

All args, kwargs, and nested `Tasks` will be serialized into a **single ServiceBus Message**.

Try to keep task args and kwargs small, and try not to build large chains.


### 4. Design for Observability

```python
async def trackable_task(state, operation_id: str):
    """Task with good observability."""
    logger.info(f"Starting operation {operation_id}")

    try:
        result = await perform_operation()
        logger.info(f"Operation {operation_id} completed successfully")
        state.metrics["operations_completed"] += 1
        return result
    except Exception as e:
        logger.error(f"Operation {operation_id} failed: {e}")
        state.metrics["operations_failed"] += 1
        raise
```

### 5. Error Handling

```python
async def robust_task(state, data):
    """Task with proper error handling."""
    try:
        return await risky_operation(data)
    except TemporaryError:
        # Let retry policy handle temporary errors
        raise retries.RetryException("Temporary failure, will retry")
    except PermanentError:
        # Don't retry permanent errors
        return TaskFailureResult
    except ValidationError as e:
        # Log validation errors but don't retry
        logger.warning(f"Validation failed: {e}")
        return TaskFailureResult
```

## Example Logs

When you run callback and chain workflows, you'll see logs like this:

```json
{"event": "Registered background function fn=main_task", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "Registered background function fn=success_handler", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "[main_task] Begin Task sequence_number=19657", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
{"event": "[main_task] Completed Task sequence_number=19657 in 0.00021s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
{"event": "[success_handler] Begin Task sequence_number=19659", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:24"}
{"event": "[success_handler] Completed Task sequence_number=19659 in 0.00014s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:24"}
```

## Next Steps

- **[Retry Policies](retry-policies.md)** - Configure how tasks retry on failure
- **[Error Handling](error-handling.md)** - Robust error handling strategies
- **[Quick Start](../getting-started/quickstart.md)** - See chaining examples in action