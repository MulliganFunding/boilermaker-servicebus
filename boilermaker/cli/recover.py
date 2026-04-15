"""Recovery subcommand handler for the boilermaker CLI."""

import sys
from datetime import datetime, UTC

from azure.identity.aio import DefaultAzureCredential
from rich.console import Console

from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli._output import render_graph_summary, render_task_table
from boilermaker.service_bus import AzureServiceBus
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task.task_id import GraphId


async def run_recover(
    storage: BlobClientStorage,
    graph_id: str,
    sb_namespace_url: str,
    sb_queue_name: str,
    console: Console | None = None,
) -> int:
    """Load a graph, print its status, and re-publish all stalled tasks to Service Bus.

    Args:
        storage: Blob storage client.
        graph_id: The graph ID to recover.
        sb_namespace_url: Service Bus namespace URL.
        sb_queue_name: Service Bus queue name.
        console: Rich Console for output. When None, a default Console is created.

    Returns:
        EXIT_HEALTHY (0) when no stalled tasks are found.
        EXIT_STALLED (1) when stalled tasks are detected (even if recovery succeeds).
        EXIT_ERROR (2) when the graph is not found or required args are missing.
    """
    if console is None:
        console = Console()

    if not sb_namespace_url or not sb_queue_name:
        print("ERROR: --recover requires --sb-namespace-url and --sb-queue-name", file=sys.stderr)
        return EXIT_ERROR

    graph = await storage.load_graph(GraphId(graph_id))
    if graph is None:
        print(f"ERROR: Graph {graph_id} not found in storage.", file=sys.stderr)
        return EXIT_ERROR

    console.print(render_graph_summary(graph))
    console.print(render_task_table(graph))

    stalled = graph.detect_stalled_tasks()
    if not stalled:
        console.print("No stalled tasks found. Graph is healthy.")
        return EXIT_HEALTHY

    console.print("\nRecovering stalled tasks...\n")

    service_bus_client = AzureServiceBus(
        sb_namespace_url,
        sb_queue_name,
        DefaultAzureCredential,
    )
    try:
        for task_id, fn_name, _status in stalled:
            task = graph.children.get(task_id) or graph.fail_children.get(task_id)
            if task is None:
                print(f"  SKIP: Task {task_id} not found in graph definition", file=sys.stderr)
                continue
            timestamp = int(datetime.now(UTC).timestamp())
            recovery_msg_id = f"{task_id}:recovery:{timestamp}"
            try:
                await service_bus_client.send_message(
                    task.model_dump_json(),
                    unique_msg_id=recovery_msg_id,
                )
                console.print(f"  [green]Recovered[/green]  {fn_name} ({task_id}) -> msg_id: {recovery_msg_id}")
            except Exception as exc:
                console.print(f"  [red]Failed[/red]     {fn_name} ({task_id}) -> {exc}")
    finally:
        await service_bus_client.close()

    return EXIT_STALLED
