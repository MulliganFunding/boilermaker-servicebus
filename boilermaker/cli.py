"""CLI tool for inspecting and recovering TaskGraph state in Azure Blob Storage."""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, UTC

from azure.identity.aio import DefaultAzureCredential

from boilermaker.service_bus import AzureServiceBus
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task import TaskGraph, TaskStatus
from boilermaker.task.task_id import GraphId, TaskId

logger = logging.getLogger("boilermaker.cli")

EXIT_HEALTHY = 0
EXIT_STALLED = 1
EXIT_ERROR = 2

STALLED_STATUSES = {TaskStatus.Scheduled, TaskStatus.Started, TaskStatus.Retry}

# Table column widths
COL_TASK_ID = 22
COL_FUNCTION = 25
COL_STATUS = 14
COL_TYPE = 12


def _short_task_id(task_id: TaskId) -> str:
    """Return the last segment of a task ID for compact display."""
    full = str(task_id)
    return full[-12:] if len(full) > 12 else full


def _format_header() -> str:
    """Build the table header and separator line."""
    header = (
        f"{'Task ID (short)':<{COL_TASK_ID}}"
        f"  {'Function':<{COL_FUNCTION}}"
        f"  {'Status':<{COL_STATUS}}"
        f"  {'Type':<{COL_TYPE}}"
    )
    separator = (
        f"{'─' * COL_TASK_ID}"
        f"  {'─' * COL_FUNCTION}"
        f"  {'─' * COL_STATUS}"
        f"  {'─' * COL_TYPE}"
    )
    return f"{header}\n{separator}"


def _format_task_row(
    task_id: TaskId,
    function_name: str,
    status: str,
    task_type: str,
    is_stalled: bool,
) -> str:
    """Format a single task row for the table."""
    stalled_marker = "** STALLED **" if is_stalled else ""
    return (
        f"{_short_task_id(task_id):<{COL_TASK_ID}}"
        f"  {function_name:<{COL_FUNCTION}}"
        f"  {status:<{COL_STATUS}}"
        f"  {task_type:<{COL_TYPE}}"
        f"  {stalled_marker}"
    ).rstrip()


def format_graph_table(graph: TaskGraph) -> str:
    """Build the full table output for a graph, including header, rows, and summary.

    Returns the formatted string (no trailing newline).
    """
    stalled = graph.detect_stalled_tasks()
    stalled_task_ids = {tid for tid, _, _ in stalled}

    lines: list[str] = []
    lines.append(f"\nGraph: {graph.graph_id}")
    lines.append(_format_header())

    for task_id, task in graph.children.items():
        result = graph.results.get(task_id)
        status = str(result.status) if result else "NO BLOB"
        is_stalled = task_id in stalled_task_ids
        lines.append(_format_task_row(task_id, task.function_name, status, "child", is_stalled))

    for task_id, task in graph.fail_children.items():
        result = graph.results.get(task_id)
        status = str(result.status) if result else "NO BLOB"
        is_stalled = task_id in stalled_task_ids
        lines.append(_format_task_row(task_id, task.function_name, status, "fail_child", is_stalled))

    is_complete = graph.is_complete()
    has_failures = graph.has_failures()

    lines.append(f"\nComplete: {is_complete} | Has failures: {has_failures} | Stalled tasks: {len(stalled)}")
    return "\n".join(lines)


async def inspect_graph(
    storage: BlobClientStorage,
    graph_id: str,
    recover: bool = False,
    sb_namespace_url: str | None = None,
    sb_queue_name: str | None = None,
) -> int:
    """Load a graph from blob storage, print its status table, and optionally recover stalled tasks.

    Args:
        storage: Blob storage client.
        graph_id: The graph ID to inspect.
        recover: If True, re-publish stalled tasks to Service Bus.
        sb_namespace_url: Service Bus namespace URL, required when recover is True.
        sb_queue_name: Required when recover is True.

    Returns:
        Exit code: 0 = healthy, 1 = stalled tasks found, 2 = error.
    """
    graph = await storage.load_graph(GraphId(graph_id))
    if graph is None:
        print(f"ERROR: Graph {graph_id} not found in storage.", file=sys.stderr)
        return EXIT_ERROR

    print(format_graph_table(graph))

    stalled = graph.detect_stalled_tasks()

    if stalled and recover:
        if not sb_namespace_url or not sb_queue_name:
            print("ERROR: --recover requires --sb-namespace-url and --sb-queue-name", file=sys.stderr)
            return EXIT_ERROR

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
                    print(f"  RECOVERED: {task_id} ({fn_name}) — published with {recovery_msg_id}")
                except Exception as exc:
                    print(f"  FAILED: {task_id} ({fn_name}) — {exc}", file=sys.stderr)
        finally:
            await service_bus_client.close()

    return EXIT_STALLED if stalled else EXIT_HEALTHY


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="boilermaker-graph",
        description="Inspect and recover TaskGraph state in Azure Blob Storage",
    )
    subparsers = parser.add_subparsers(dest="command")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect a TaskGraph")
    inspect_parser.add_argument("graph_id", help="Graph ID to inspect")
    inspect_parser.add_argument("--storage-url", required=True, help="Azure Blob Storage URL")
    inspect_parser.add_argument("--container", required=True, help="Blob container name")
    inspect_parser.add_argument("--recover", action="store_true", help="Re-publish stalled tasks to Service Bus")
    inspect_parser.add_argument(
        "--sb-namespace-url",
        help="Service Bus namespace URL (required for --recover)",
    )
    inspect_parser.add_argument(
        "--sb-queue-name",
        help="Service Bus queue name (required for --recover)",
    )
    inspect_parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(EXIT_ERROR)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    async def run() -> int:
        credentials = DefaultAzureCredential()
        storage = BlobClientStorage(args.storage_url, args.container, credentials)
        try:
            return await inspect_graph(
                storage,
                args.graph_id,
                recover=args.recover,
                sb_namespace_url=args.sb_namespace_url,
                sb_queue_name=args.sb_queue_name,
            )
        finally:
            await credentials.close()

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
