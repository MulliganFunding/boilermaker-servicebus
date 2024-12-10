import asyncio
import os

from boilermaker import retries
from boilermaker.app import Boilermaker
from boilermaker.config import Config
from boilermaker.failure import TaskFailureResult
from boilermaker.service_bus import AzureServiceBus

service_bus_namespace_url = os.environ["SERVICE_BUS_NAMESPACE_URL"]
service_bus_queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]
conf = Config(
    service_bus_namespace_url=service_bus_namespace_url,
    service_bus_queue_name=service_bus_queue_name,
)
# We create a service bus Sender client
service_bus_client = AzureServiceBus(conf)


# This represents our "App" object
class App:
    def __init__(self, data):
        self.data = data


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


# Next we'll create a worker and register our task
worker = Boilermaker(App({"key": "value"}), service_bus_client=service_bus_client)

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


if __name__ == "__main__":
    asyncio.run(publish_task())
