"""Invoke subcommand handler for the boilermaker CLI."""

import sys
from datetime import datetime, UTC

from azure.identity.aio import DefaultAzureCredential
from rich.console import Console

from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY
from boilermaker.service_bus import AzureServiceBus
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task.result import TaskStatus
from boilermaker.task.task_id import GraphId, TaskId


async def run_invoke(
    storage: BlobClientStorage,
    graph_id: str,
    task_id: str,
    sb_namespace_url: str,
    sb_queue_name: str,
    force: bool = False,
    console: Console | None = None,
) -> int:
    """Load a graph, find a task, and publish it to Service Bus.

    Refuses to re-invoke tasks in a terminal state unless force=True. Creates
    the Service Bus client internally and closes it in a finally block regardless
    of success or failure.

    Args:
        storage: Blob storage client.
        graph_id: The graph ID containing the task.
        task_id: The task ID to invoke.
        sb_namespace_url: Service Bus namespace URL.
        sb_queue_name: Service Bus queue name.
        force: When True, allows re-invocation of tasks in terminal states.
        console: Rich Console for output. When None, a default Console is created.

    Returns:
        EXIT_HEALTHY (0) when the task is published successfully.
        EXIT_ERROR (2) when the graph or task is not found, or when the task is
            in a terminal state without --force.
    """
    if console is None:
        console = Console()

    graph = await storage.load_graph(GraphId(graph_id))
    if graph is None:
        print(f"ERROR: Graph {graph_id} not found in storage.", file=sys.stderr)
        return EXIT_ERROR

    task_id = TaskId(task_id)
    task = graph.children.get(task_id) or graph.fail_children.get(task_id)
    if task is None:
        available = sorted(
            [str(tid) for tid in list(graph.children) + list(graph.fail_children)]
        )
        print(
            f"ERROR: Task {task_id} not found in graph {graph_id}.\n"
            f"Available tasks: {', '.join(available) if available else '(none)'}",
            file=sys.stderr,
        )
        return EXIT_ERROR

    result = graph.results.get(task_id)
    status = result.status if result is not None else "UNKNOWN"
    task_is_terminal = result is not None and result.status in TaskStatus.finished_types()

    if task_is_terminal and not force:
        print(
            f"ERROR: Task {task_id} is in terminal state '{status}'. "
            f"Use --force to re-invoke.",
            file=sys.stderr,
        )
        return EXIT_ERROR

    # DefaultAzureCredential is passed as the class (not an instance) because AzureServiceBus
    # accepts a CredentialFactory: a callable that returns a credential. The class itself
    # satisfies the CredentialFactory Protocol — calling it constructs a new credential instance.
    # recover.py uses the same pattern.
    service_bus_client = AzureServiceBus(
        sb_namespace_url,
        sb_queue_name,
        DefaultAzureCredential,
    )
    try:
        timestamp = int(datetime.now(UTC).timestamp())
        invoke_msg_id = f"{task_id}:invoke:{timestamp}"
        try:
            await service_bus_client.send_message(
                task.model_dump_json(),
                unique_msg_id=invoke_msg_id,
            )
        except Exception as exc:
            print(f"ERROR: Failed to publish task {task_id} to Service Bus: {exc}", file=sys.stderr)
            return EXIT_ERROR
        console.print(
            f"[green]Invoked[/green]  {task.function_name} ({task_id}) -> msg_id: {invoke_msg_id}"
        )
    finally:
        await service_bus_client.close()

    return EXIT_HEALTHY
