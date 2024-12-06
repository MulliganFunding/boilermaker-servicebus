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


# This is a background task that we'll register
async def background_task1(state, somearg, somekwarg=True):
    """`state` must be first argument."""
    await state.data.get("key")
    print(state)
    print(somearg)
    print(somekwarg)


# We create a service bus Sender client
service_bus_client = ServiceBusSender(
    service_bus_namespace_url,
    azure_identity_async_credential,
    queue_name=service_bus_queue_name,
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
