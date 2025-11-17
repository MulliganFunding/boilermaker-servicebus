"""
Example of TaskGraph workflows using TaskGraphBuilder.

This example demonstrates how to create complex workflows using TaskGraphBuilder,
which provides a simple, readable way to define DAG-based task dependencies.
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


async def handle_fetch_error(state):
    """Handle fetch data errors."""
    print("Handling fetch error - trying backup source")
    return {"fallback_attempted": True}


async def handle_processing_error(state):
    """Handle data processing errors."""
    print("Processing failed - sending alert to administrators")
    return {"alert_sent": True}


async def main():
    """Demonstrate TaskGraph usage."""

    # Set up the application (you'd configure these with real values)
    service_bus_client = AzureServiceBus.from_config(
        {"connection_string": "your-connection-string", "queue_name": "your-queue"}
    )

    storage_client = BlobClientStorage(
        az_storage_url="https://yourstorage.blob.core.windows.net",
        container_name="task-results",
        credentials=DefaultAzureCredential(),
    )

    app = Boilermaker(state={}, service_bus_client=service_bus_client, results_storage=storage_client)

    # Register task functions
    app.register_many_async(
        [
            fetch_data,
            process_data,
            save_results,
            send_notification,
            cleanup_temp_files,
            handle_fetch_error,
            handle_processing_error,
        ]
    )

    # Create tasks
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    notify_task = app.create_task(send_notification, "placeholder_id")
    cleanup_task = app.create_task(cleanup_temp_files)

    # Error handling tasks
    fetch_error_task = app.create_task(handle_fetch_error)
    process_error_task = app.create_task(handle_processing_error)

    # Build workflow using TaskGraphBuilder with failure handling
    from boilermaker.task import TaskGraphBuilder

    graph = (
        TaskGraphBuilder()
        .add(fetch_task)  # Start with fetch_data
        .on_failure(fetch_task.task_id, fetch_error_task)  # Handle fetch failures
        .then(process_task)  # process_data depends on fetch_data
        .on_failure(process_task.task_id, process_error_task)  # Handle processing failures
        .then(save_task)  # save_results depends on process_data
        .parallel([notify_task, cleanup_task])  # Both depend on save_results, run in parallel
        .build()
    )

    # Publish the graph - this will start the workflow
    await app.publish_graph(graph)

    print(f"Published TaskGraph {graph.graph_id} with {len(graph.children)} tasks")
    print("Workflow started - check your worker logs to see execution")

    # Clean up
    await app.close()


if __name__ == "__main__":
    asyncio.run(main())
