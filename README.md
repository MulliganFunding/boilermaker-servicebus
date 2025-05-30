# Boilermaker

This project is an extremely-lightweight task-runner exclusively for __async Python__ and Azure Service Bus Queues. If you need a fully fledged task-runner, you should consider one of these other projects instead:

- [Celery](https://github.com/celery/celery/tree/main)
-

## To Install

```sh
pip install "boilermaker-servicebus"
```

## How to Use

The `Boilermaker` application object requires some state (which will get sent to all handlers as the first argument upon invocation) and an authenticated ServiceBus client.

A task handler for `Boilermaker` is any async function which takes as its first argument application state and which has been registered:

```python
# This is a background task that we'll register
async def background_task1(state, somearg, somekwarg=True):
    """`state` must be first argument."""
    await state.data.get("key")
    print(state)
    print(somearg)
    print(somekwarg)

# Our task must be registered to be invocable.
boilermaker_app.register_async(background_task1, policy=...A retry policy goes here...)
```
__Note__: `Boilermaker` does not currently have a way to store or use the results of tasks, and all arguments must be JSON-serializable.

### Complete Example

For a fuller example, in our application, we may have some code like the following:

```python
from azure.identity.aio import DefaultAzureCredential
from boilermaker.app import Boilermaker
from boilermaker.config import Config
# The boilermaker ServiceBus wrapper is for convenience/example
from boilermaker.service_bus import AzureServiceBus
from boilermaker import retries


# This represents our "App" object which will be passed as `state` to our handlers
class App:
    def __init__(self, data):
        self.data = data


# This is a background task that we'll register
async def background_task1(state, somearg, somekwarg=True):
    """`state` must be first argument."""
    await state.data.get("key")
    print(state)
    print(somearg)
    print(somekwarg)



service_bus_namespace_url = os.environ["SERVICE_BUS_NAMESPACE_URL"]
service_bus_queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]
conf = Config(
    service_bus_namespace_url=service_bus_namespace_url,
    service_bus_queue_name=service_bus_queue_name,
)
# We create a service bus Sender client
service_bus_client = AzureServiceBus.from_config(conf)

# Here, we'll create an arbitrary state payload for every task to receive.
state = App({"key": "value"})
# Next we'll create a worker and register our task
worker = Boilermaker(state, service_bus_client=service_bus_client)
worker.register_async(background_task1, policy=retries.RetryPolicy.default())


# Finally, this will schedule the task
async def publish_task():
    await worker.apply_async(
        background_task1, "first-arg", somekwarg=False
    )

if __name__ == "__main__":
    asyncio.run(publish_task())
```

In another process, we'll need to _run_ a background task worker. That looks like this:

```python
async def run_worker():
    await worker.run()
```

If we look in the logs for our other process, we should be able to see this task registered and run:

```sh
{"event": "Registered background function fn=background_task1", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "[background_task1] Begin Task sequence_number=19657", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
...prints-go-here...
{"event": "[background_task1] Completed Task sequence_number=19657 in 0.00021130498498678207s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
```


## Callbacks

It is possible to register callbacks for tasks, which can run on success or failure. To schedule a task with callbacks, we have to create a task object and then set a success and/or failure callback. Finally, instead of `apply_async` we have to call `publish_task`:

```python
...see worker-instantation example above...

from boilermaker.failure import TaskFailureResult

async def a_background_task(state, param: str):
    if param == "success":
        print("Things seem to be going really well")
        return None
    elif param == "fail":
        return TaskFailureResult
    raise ValueError("Exceptions are a bummer!")


async def happy_path(state):
    print("Everything is great!")


async def sad_path(state):
    print("Everything is sad")


# We need to be sure our tasks are registered
worker.register_async(a_background_task, policy=retries.NoRetry())
worker.register_async(happy_path, policy=retries.NoRetry())
worker.register_async(sad_path, policy=retries.NoRetry())

# Now we can create a happy task and add callbacks
happy_task = worker.create_task(a_background_task, "success")
# This callback should get scheduled
happy_task.on_success = worker.create_task(happy_path)
# This callback will not
happy_task.on_failure = worker.create_task(sad_path)

# For good measure, we'll create a sad task too
sad_task = worker.create_task(a_background_task, "uh oh!")
# This callback should not get scheduled
sad_task.on_success = worker.create_task(happy_path)
# This callback should get scheduled
sad_task.on_failure = worker.create_task(sad_path)

# Finally, this will schedule the tasks
async def publish_task():
    await worker.publish_task(happy_task)
    # If we wait a bit we can see more clearly one task before the other
    await asyncio.sleep(4)
    await worker.publish_task(sad_task)
```

Here are examples of log out from running the above:

```json
// Logs from registering the functions
{"event": "Registered background function fn=callback_task_test", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "Registered background function fn=happy_path", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
{"event": "Registered background function fn=sad_path", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:46:54"}
// Happy path test ->
{"event": "[callback_task_test] Begin Task sequence_number=19657", "version": "pr72-0.25.1", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
{"event": "[callback_task_test] Completed Task sequence_number=19657 in 0.00021130498498678207s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:23"}
{"event": "[happy_path] Begin Task sequence_number=19659", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:24"}
{"event": "[happy_path] Completed Task sequence_number=19659 in 0.0001390039687976241s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:24"}
// Sad path test ->
{"event": "[callback_task_test] Begin Task sequence_number=19661", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:27"}
{"event": "Failed processing task sequence_number=19661  Traceback (most recent call last):\n  File \"/app/.venv/lib/python3.12/site-packages/boilermaker/app.py\", line 238, in message_handler\n    result = await self.task_handler(task, sequence_number)\n             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/app/.venv/lib/python3.12/site-packages/boilermaker/app.py\", line 306, in task_handler\n    result = await function(\n             ^^^^^^^^^^^^^^^\n  File \"/app/boilermaker_example.py\", line 210, in callback_task_test\n    raise ValueError(\"Negative numbers are a bummer\")\nValueError: Negative numbers are a bummer\n", "level": "error", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:27"}
{"event": "[sad_path] Begin Task sequence_number=19663", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:28"}
{"event": "[sad_path] Completed Task sequence_number=19663 in 0.00020030408632010221s", "level": "info", "logger": "boilermaker.app", "timestamp": "2024/12/10 15:52:28"}
```

## Retries

Retries are configured in `Boilermaker` using a `RetryPolicy`:

```python
class RetryPolicy(BaseModel):
    # Capped at this value
    max_tries: int = MAX_REASONABLE_RETRIES
    # Seconds
    delay: int = 0
    # Max seconds
    delay_max: int = MAX_BACKOFF_SECONDS
    # Mode for retrying
    retry_mode: RetryMode = RetryMode.Fixed
```

Retries are __scheduled__ only by raising a `RetryException`:

```python
def sometask(state):
    raise RetryException("Please try again!")
```

__Note__: _all_ tasks __must__ be registered with a `RetryPolicy`, but `Boilermaker` will only retry tasks that have raised a `RetryException`.

If you do not want your tasks to be retried:

- Do not throw a `RetryException` from inside of your task.
- Register them with a `NoRetry` policy: `worker.register_async(..., policy=retries.NoRetry())`

`NoRetry()` is simply a special policy where `max_tries=1`.

__Note__: `on_failure` callbacks will only be run __after all retries have been exhausted__.

### Scheduling Retries

There are three places where retries for a task can be configured:

- When initially registering a task (the task default).
- When scheduling a task (overrides the default for that invocation).
- When raising a `RetryException` with a custom policy (overrides default _if_ different).

Examples of each are included below.

#### Register Task with a Default Retry Policy

When a task has been registered, we can set a retry policy for any invocation of it:

```python
from boilermaker import retries

async def a_background_task(state, param: str):
    if param == "throwing":
        # This will retry using the *default policy* set when the task was registered
        raise retries.RetryException("We are thrown")
    return "OK"

worker.register_async(a_background_task, policy=retries.RetryPolicy.default())
```

This example works like a template for running the task. If `a_background_task` gets retried, `Boilermaker` will use the default policy to calculate its next delay and max attempts allowed.

#### Scheduling a Task with a Different Retry Policy

Sometimes we'd like to publish our task so that its retries follow a different plan than the task was originally registered with.

We can __publish__ (schedule) a particular task with a different retry policy like this:

```python
from boilermaker import retries

async def a_background_task(state, param: str):
    if param == "throwing":
        # This will retry using the RetryPolicy set
        # when *scheduling* the task (see below)
        raise retries.RetryException("We are thrown")
    return "OK"

# First we register the task
worker.register_async(a_background_task, policy=retries.RetryPolicy.default())

async def schedule_task_with_different_policy():
    new_policy=retries.RetryPolicy(
        max_tries=100, delay=2, delay_max=1000, retry_mode=retries.RetryMode.Exponential,
    )
    await worker.apply_async(a_background_task, "throwing", policy=new_policy)
```

In the above example, _this_ invocation of the task has a new retry policy, but all other invocations of this task elsewhere in our codebase will continue to use the default.


#### Raising a `RetryException` with a Different Policy

Lastly, it's also possible to request a different `RetryPolicy` from _inside_ a task:


```python
from boilermaker import retries

async def a_background_task(state, param: str):
    if param == "throwing":
        # This will retry using *THIS* RetryPolicy
        raise retries.RetryExceptionDefaultExponential("We are thrown", max_tries=42)
    return "OK"

# First we register the task
worker.register_async(a_background_task, policy=retries.RetryPolicy.default())

async def schedule_task_with_different_policy():
    new_policy=retries.RetryPolicy(
        max_tries=10, delay=20, delay_max=100, retry_mode=retries.RetryMode.Linear,
    )
    # Because the task raises a different policy, this one will get dropped in face of `RetryExceptionDefaultExponential`
    await worker.apply_async(a_background_task, "throwing", policy=new_policy)
```


## FAQ

**Where does the name come from?**

From [here](https://www.discogs.com/artist/632957-Boilermaker)

**Are You Going to Add Other Queue Services?**

No, not planning to. Consider one of the other background-task libraries.