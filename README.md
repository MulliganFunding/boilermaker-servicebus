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

In our primary app, we may have some code like the following:

```python
from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusSender

from boilermaker.app import Boilermaker
from boilermaker import retries


# This represents our "App" object
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


azure_identity_async_credential = DefaultAzureCredential()
service_bus_namespace_url = os.environ["SERVICE_BUS_NAMESPACE_URL"]
service_bus_queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]

# We create a service bus Sender client
sbus_client = AzureServiceBus(
    service_bus_namespace_url,
    service_bus_queue_name,
    azure_identity_async_credential,  # type: DefaultAzureCredential
)

# Next we'll create a worker and register our task
worker = Boilermaker(App({"key": "value"}), service_bus_client=service_bus_client)
worker.register_async(background_task1, policy=retries.RetryPolicy.default())


# Finally, this will schedule the task
async def publish_task():
    await worker.apply_async(
        background_task1, "first-arg", somekwarg=False
    )

if __name__ == "__main__":
    asyncio.run(publish_task())
```

In another process, we'll need to *run* a background task worker. That looks like this:

```python
async def run_worker():
    await worker.run()
```

If we look in , we should be able to see this task registered and run:

```sh


```


## Callbacks and Retries

It is possible to register callbacks for tasks, which can run on success or failure. To schedule a task with callbacks, we have to create a task object and then set a success and/or failure callback. Finally, instead of `apply_async` we have to call `publish_task`:

```python
...see worker-instantation example above...

async def a_background_task(state, param: int):
    if param > 0:
        print("Things seem to be going really well")
        return None
    raise ValueError("Negative numbers are a bummer")


async def happy_path(state):
    print("Everything is great!")


async def sad_path(state):
    print("Everything is sad")


# We need to be sure our tasks are registered
worker.register_async(a_background_task, policy=retries.NoRetry)
worker.register_async(happy_path, policy=retries.NoRetry())
worker.register_async(sad_path, policy=retries.NoRetry())

# Now we can create a happy task and add callbacks
happy_task = worker.create_task(a_background_task, 11)
# This callback should get scheduled
happy_task.on_success = worker.create_task(happy_path)
# This callback will not
happy_task.on_failure = worker.create_task(sad_path)

# For good measure, we'll create a sad task too
sad_task = worker.create_task(a_background_task, -20)
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

## FAQ

**Where does the name come from?**

From [here](https://www.discogs.com/artist/632957-Boilermaker)

**Are You Going to Add Other Queue Services?**

No, not planning to. Consider one of the other background-task libraries.