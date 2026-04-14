"""Inspect subcommand handler for the boilermaker CLI."""

import sys

from rich.console import Console

from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli._output import (
    format_graph_json,
    render_graph_summary,
    render_task_detail,
    render_task_table,
)
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task.task_id import GraphId


async def run_inspect(
    storage: BlobClientStorage,
    graph_id: str,
    console: Console | None = None,
    output_json: bool = False,
    task_id: str | None = None,
) -> int:
    """Load a graph from blob storage and print its status output.

    When output_json is True, prints a JSON string directly to stdout and skips
    all Rich rendering. When task_id is provided and output_json is False, renders
    a focused single-task detail panel instead of the full graph table.

    Args:
        storage: Blob storage client.
        graph_id: The graph ID to inspect.
        console: Rich Console for output. When None, a default Console is created.
            Ignored when output_json is True.
        output_json: When True, print JSON to stdout instead of Rich output.
        task_id: When provided, show detail for this single task instead of the
            full graph table. Ignored when output_json is True.

    Returns:
        EXIT_HEALTHY (0) when no stalled tasks are found.
        EXIT_STALLED (1) when stalled tasks are detected.
        EXIT_ERROR (2) when the graph or task is not found.
    """
    graph = await storage.load_graph(GraphId(graph_id))
    if graph is None:
        print(f"ERROR: Graph {graph_id} not found in storage.", file=sys.stderr)
        return EXIT_ERROR

    stalled = graph.detect_stalled_tasks()
    exit_code = EXIT_STALLED if stalled else EXIT_HEALTHY

    if output_json:
        print(format_graph_json(graph))
        return exit_code

    if console is None:
        console = Console()

    if task_id is not None:
        return _inspect_single_task(graph, graph_id, task_id, stalled, console)

    console.print(render_graph_summary(graph))
    console.print(render_task_table(graph))
    return exit_code


def _inspect_single_task(graph, graph_id, task_id, stalled, console) -> int:
    """Find a single task in the graph and render its detail panel.

    Args:
        graph: The loaded TaskGraph.
        graph_id: Graph ID string (for error messages).
        task_id: The task ID to inspect.
        stalled: Already-computed stalled task list.
        console: Rich Console for output.

    Returns:
        EXIT_HEALTHY (0) when task is not stalled.
        EXIT_STALLED (1) when task is stalled.
        EXIT_ERROR (2) when the task is not found.
    """
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

    console.print(render_task_detail(graph, task_id))

    stalled_task_ids = {tid for tid, _, _ in stalled}
    return EXIT_STALLED if task_id in stalled_task_ids else EXIT_HEALTHY
