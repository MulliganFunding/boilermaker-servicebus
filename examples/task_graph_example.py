"""
Example of TaskGraph workflows using TaskGraphBuilder.

This example demonstrates how to create complex workflows using TaskGraphBuilder,
which provides a simple, readable way to define DAG-based task dependencies.

Patterns covered:
  1. Sequential:                add().then().then()
  2. Fan-out:                   add().parallel(t1, t2, t3)
  3. Fan-in:                    parallel(a, b, c).then(d)
  4. Diamond:                   add(a).parallel(b, c).then(d)
  5. Independent chains + join: add_chain(..., depends_on=None) x2, then join
  6. Inline failure handler:    add(task, on_failure=handler)
  7. TaskChain with shared failure handler
"""

import asyncio

from azure.identity.aio import DefaultAzureCredential
from boilermaker import Boilermaker
from boilermaker.service_bus import AzureServiceBus
from boilermaker.storage import BlobClientStorage
from boilermaker.task import Task, TaskChain, TaskGraphBuilder

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


async def fetch_data(state, url: str):
    """Simulate fetching data from a URL."""
    print(f"Fetching data from {url}")
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


async def aggregate_results(state):
    """Aggregate results from multiple sources."""
    print("Aggregating all results")
    return {"aggregated": True}


async def handle_fetch_error(state):
    """Handle fetch data errors."""
    print("Handling fetch error - trying backup source")
    return {"fallback_attempted": True}


async def handle_processing_error(state):
    """Handle data processing errors."""
    print("Processing failed - sending alert to administrators")
    return {"alert_sent": True}


async def handle_aggregation_error(state):
    """Handle aggregation errors."""
    print("Aggregation failed - running cleanup")
    return {"cleanup_done": True}


# ---------------------------------------------------------------------------
# Workflow patterns
# ---------------------------------------------------------------------------


async def example_sequential(app: Boilermaker) -> None:
    """Pattern 1 — Sequential: fetch → process → save."""
    # Create a Task with an immutable signature (si) so that the args are baked in
    fetch_task = Task.si(fetch_data, "https://api.example.com/data")
    # Alterantively, we can use the Boilermaker app to create the same
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})

    graph = (
        TaskGraphBuilder()
        .add(fetch_task)
        .then(process_task)
        .then(save_task)
        .build()
    )
    await app.publish_graph(graph)
    print(f"[sequential] published graph {graph.graph_id}")


async def example_fan_out(app: Boilermaker) -> None:
    """Pattern 2 — Fan-out: fetch → (notify, cleanup)."""
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    notify_task = app.create_task(send_notification, "placeholder_id")
    cleanup_task = app.create_task(cleanup_temp_files)

    graph = (
        TaskGraphBuilder()
        .add(fetch_task)
        .parallel(notify_task, cleanup_task)  # both depend on fetch; run in parallel
        .build()
    )
    await app.publish_graph(graph)
    print(f"[fan-out] published graph {graph.graph_id}")


async def example_fan_in(app: Boilermaker) -> None:
    """Pattern 3 — Fan-in: (fetch, process, save) → aggregate."""
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    agg_task = app.create_task(aggregate_results)

    graph = (
        TaskGraphBuilder()
        .parallel(fetch_task, process_task, save_task)  # three independent roots; cursor = all three
        .then(agg_task)  # depends on all three; fan-in
        .build()
    )
    await app.publish_graph(graph)
    print(f"[fan-in] published graph {graph.graph_id}")


async def example_diamond(app: Boilermaker) -> None:
    """Pattern 4 — Diamond: fetch → (process, save) → aggregate."""
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    agg_task = app.create_task(aggregate_results)

    graph = (
        TaskGraphBuilder()
        .add(fetch_task)
        .parallel(process_task, save_task)  # both depend on fetch; cursor = [process, save]
        .then(agg_task)  # depends on process AND save; fan-in join
        .build()
    )
    await app.publish_graph(graph)
    print(f"[diamond] published graph {graph.graph_id}")


async def example_independent_chains_join(app: Boilermaker) -> None:
    """Pattern 5 — Independent chains with fan-in join.

    This pattern was previously impossible with the old API.

    Graph:
        fetch → process → save ─┐
                                  ├→ aggregate
        notify → cleanup ────────┘
    """
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    notify_task = app.create_task(send_notification, "placeholder_id")
    cleanup_task = app.create_task(cleanup_temp_files)
    agg_task = app.create_task(aggregate_results)

    pipeline_chain = TaskChain(fetch_task, process_task, save_task)
    notification_chain = TaskChain(notify_task, cleanup_task)

    graph = (
        TaskGraphBuilder()
        .add_chain(pipeline_chain, depends_on=None)  # root; cursor = [save_task]
        .add_chain(notification_chain, depends_on=None)  # root; cursor = [save_task, cleanup_task]
        .then(agg_task)  # depends on BOTH chain lasts
        .build()
    )
    await app.publish_graph(graph)
    print(f"[independent-chains-join] published graph {graph.graph_id}")


async def example_inline_failure_handler(app: Boilermaker) -> None:
    """Pattern 6 — Inline failure handler.

    on_failure= is co-located with the task it guards.
    handle_fetch_error only runs if fetch_task fails.
    process_task only runs if fetch_task succeeds.
    """
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    fetch_error_task = app.create_task(handle_fetch_error)

    graph = (
        TaskGraphBuilder()
        .add(fetch_task, on_failure=fetch_error_task)  # failure handler declared at task site
        .then(process_task)  # only runs if fetch_task succeeds
        .build()
    )
    await app.publish_graph(graph)
    print(f"[inline-failure] published graph {graph.graph_id}")


async def example_taskchain_shared_failure(app: Boilermaker) -> None:
    """Pattern 7 — TaskChain with a shared failure handler.

    process_error_task is registered on EACH task in the chain.
    Whichever task fails first triggers the handler.
    """
    fetch_task = app.create_task(fetch_data, "https://api.example.com/data")
    process_task = app.create_task(process_data, {"data": "placeholder", "size": 0})
    save_task = app.create_task(save_results, {"processed": False})
    process_error_task = app.create_task(handle_processing_error)

    pipeline = TaskChain(fetch_task, process_task, save_task, on_any_failure=process_error_task)

    graph = TaskGraphBuilder().add_chain(pipeline).build()
    await app.publish_graph(graph)
    print(f"[taskchain-shared-failure] published graph {graph.graph_id}")


# ---------------------------------------------------------------------------
# Application setup
# ---------------------------------------------------------------------------


async def main():
    """Run all workflow examples."""
    service_bus_client = AzureServiceBus.from_config(
        {"connection_string": "your-connection-string", "queue_name": "your-queue"}
    )
    storage_client = BlobClientStorage(
        az_storage_url="https://yourstorage.blob.core.windows.net",
        container_name="task-results",
        credentials=DefaultAzureCredential(),
    )

    app = Boilermaker(state={}, service_bus_client=service_bus_client, results_storage=storage_client)

    app.register_many_async(
        [
            fetch_data,
            process_data,
            save_results,
            send_notification,
            cleanup_temp_files,
            aggregate_results,
            handle_fetch_error,
            handle_processing_error,
            handle_aggregation_error,
        ]
    )

    await example_sequential(app)
    await example_fan_out(app)
    await example_fan_in(app)
    await example_diamond(app)
    await example_independent_chains_join(app)
    await example_inline_failure_handler(app)
    await example_taskchain_shared_failure(app)

    await app.close()


if __name__ == "__main__":
    asyncio.run(main())
