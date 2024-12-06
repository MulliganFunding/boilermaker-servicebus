import asyncio
import os

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusSender

from boilermaker.app import Boilermaker
from boilermaker import retries


azure_identity_async_credential = DefaultAzureCredential()
service_bus_namespace_url = os.environ["SERVICE_BUS_NAMESPACE_URL"]
service_bus_queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]


# This represents our "App" object
class App:
    def __init__(self, data):
        self.data = data


async def a_background_task(state, param: int):
    if param > 0:
        print("Things seem to be going really well")
        return None
    raise ValueError("Negative numbers are a bummer")


async def happy_path(state):
    print("Everything is great!")


async def sad_path(state):
    print("Everything is sad")


# We create a service bus Sender client
service_bus_client = ServiceBusSender(
    service_bus_namespace_url,
    azure_identity_async_credential,
    queue_name=service_bus_queue_name,
)

# Next we'll create a worker and register our task
worker = Boilermaker(App({"key": "value"}), service_bus_client=service_bus_client)

# We need to be sure our tasks are registered
worker.register_async(a_background_task, policy=retries.NoRetry)
worker.register_async(happy_path, policy=retries.NoRetry)
worker.register_async(sad_path, policy=retries.NoRetry)

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
    await worker.publish_task(sad_task)


if __name__ == "__main__":
    asyncio.run(publish_task())
