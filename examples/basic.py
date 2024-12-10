import asyncio
import os

from boilermaker import retries
from boilermaker.app import Boilermaker
from boilermaker.config import Config
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


# This is a background task that we'll register
async def background_task1(state, somearg, somekwarg=True):
    """`state` must be first argument."""
    await state.data.get("key")
    print(state)
    print(somearg)
    print(somekwarg)


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
