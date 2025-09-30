"""
Example of TaskGraph workflows with the boilermaker library.

This example demonstrates how to create complex workflows using TaskGraphs,
which allow for DAG-based task dependencies instead of simple linear chains.
"""

import asyncio

from azure.identity.aio import DefaultAzureCredential
from boilermaker import Boilermaker
from boilermaker.service_bus import AzureServiceBus
from boilermaker.storage import BlobClientStorage


async def fetch_data(state, url: str):
    """Simulate fetching data from a URL."""
    print(f"Fetching data from {url}")
    # Simulate some work
    await asyncio.sleep(1)
    return {"data": f"content from {url}", "size": 1024}


async def process_data(state, data: dict):
    """Process the fetched data."""
    print(f"Processing data of size {data['size']}")
    await asyncio.sleep(0.5)
    return {"processed": True, "result": data["data"].upper()}


async def save_results(state, processed_data: dict):
    """Save the processed results."""
    print(f"Saving results: {processed_data}")
    await asyncio.sleep(0.2)
    return {"saved": True, "id": "result_123"}


async def send_notification(state, result_id: str):
    """Send notification about completion."""
    print(f"Sending notification for result {result_id}")
    return {"notification_sent": True}


async def cleanup_temp_files(state):
    """Clean up temporary files."""
    print("Cleaning up temporary files")
    return {"cleanup_completed": True}


async def main():
    """Demonstrate TaskGraph usage."""

    # Set up the application (you'd configure these with real values)
    service_bus_client = AzureServiceBus.from_config({
        "connection_string": "your-connection-string",
        "queue_name": "your-queue"
    })

    storage_client = BlobClientStorage(
        az_storage_url="https://yourstorage.blob.core.windows.net",
        container_name="task-results",
        credentials=DefaultAzureCredential()
    )

    app = Boilermaker(
        state={},
        service_bus_client=service_bus_client,
        results_storage=storage_client
    )

    # Register task functions
    app.register_async(fetch_data)
    app.register_async(process_data)
    app.register_async(save_results)
    app.register_async(send_notification)
    app.register_async(cleanup_temp_files)

    # Create a TaskGraph workflow
    graph = app.create_graph()

    # Create tasks
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    notify_task = app.create_task(send_notification, "placeholder_id")
    cleanup_task = app.create_task(cleanup_temp_files)

    # Build the graph dependencies
    # fetch_data (root)
    graph.add_task(fetch_task)

    # process_data depends on fetch_data
    graph.add_task(process_task, parent_id=fetch_task.task_id)

    # save_results depends on process_data
    graph.add_task(save_task, parent_id=process_task.task_id)

    # Both notification and cleanup depend on save_results
    graph.add_task(notify_task, parent_id=save_task.task_id)
    graph.add_task(cleanup_task, parent_id=save_task.task_id)

    # Publish the graph - this will start the workflow
    await app.publish_graph(graph)

    print(f"Published TaskGraph {graph.graph_id} with {len(graph.children)} tasks")
    print("Workflow started - check your worker logs to see execution")

    # Clean up
    await app.close()


if __name__ == "__main__":
    asyncio.run(main())
