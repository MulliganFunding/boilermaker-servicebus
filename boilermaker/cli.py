"""CLI tool for inspecting and recovering TaskGraph state in Azure Blob Storage."""

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, UTC

from azure.identity.aio import DefaultAzureCredential

from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from boilermaker.exc import BoilermakerStorageError
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


OLDER_THAN_MIN = 1
OLDER_THAN_MAX = 30


def _validate_older_than(value: str) -> int:
    """Validate --older-than argument as an integer in [1, 30].

    Used as an argparse type= validator. Raises argparse.ArgumentTypeError
    when the value is outside the allowed range.
    """
    try:
        days = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"--older-than must be an integer, got: {value!r}")
    if days < OLDER_THAN_MIN or days > OLDER_THAN_MAX:
        raise argparse.ArgumentTypeError(
            f"--older-than must be between {OLDER_THAN_MIN} and {OLDER_THAN_MAX} (inclusive), got: {days}"
        )
    return days


async def purge_old_results(
    storage: BlobClientStorage,
    older_than_days: int,
    dry_run: bool = False,
) -> int:
    """Delete task-result blobs older than the given threshold from Azure Blob Storage.

    Streams all blobs under the task-results/ prefix, groups them by graph ID,
    and deletes blobs for graphs whose most-recently-modified blob is older than
    the cutoff. Graphs with in-progress tasks (Scheduled, Started, Retry) or
    missing graph.json are skipped with a stderr warning.

    Args:
        storage: Blob storage client.
        older_than_days: Number of days; graphs last modified before this cutoff are eligible.
        dry_run: When True, prints the deletion plan but does not delete any blobs.

    Returns:
        EXIT_HEALTHY (0) on clean run or dry-run.
        EXIT_STALLED (1) if any graphs were skipped due to in-progress tasks.
        EXIT_ERROR (2) on unrecoverable error.
    """
    cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
    cutoff_display = cutoff.strftime("%Y-%m-%d")

    # --- Listing pass: group BlobProperties by graph_id ---
    graph_blobs: dict[str, list] = defaultdict(list)
    listing_prefix = f"{storage.task_result_prefix}/"
    try:
        async for blob in storage.list_blobs(prefix=listing_prefix):
            graph_id = blob.name.split("/")[1]
            graph_blobs[graph_id].append(blob)
    except Exception as exc:
        print(f"ERROR: Failed to list blobs: {exc}", file=sys.stderr)
        return EXIT_ERROR

    if not graph_blobs:
        print("Nothing to purge.")
        return EXIT_HEALTHY

    # --- Eligibility pass: age filter then in-progress safety check ---
    graph_path_suffix = f"/{TaskGraph.StorageName}"

    eligible_graphs: list[tuple[str, list]] = []
    had_stalled = False
    skipped_in_progress = 0

    for graph_id, blobs in graph_blobs.items():
        most_recent_modified = max(blob.last_modified for blob in blobs)
        if most_recent_modified >= cutoff:
            continue

        graph_blob_path = f"{storage.task_result_prefix}/{graph_id}{graph_path_suffix}"
        has_graph_json = any(blob.name == graph_blob_path for blob in blobs)
        if not has_graph_json:
            print(
                f"SKIP: Graph {graph_id} has no graph.json — cannot verify in-progress status. Skipping.",
                file=sys.stderr,
            )
            continue

        try:
            graph = await storage.load_graph(GraphId(graph_id))
        except BoilermakerStorageError as exc:
            print(f"SKIP: Graph {graph_id} — failed to load graph: {exc}. Skipping.", file=sys.stderr)
            continue

        if graph is None:
            print(f"SKIP: Graph {graph_id} — graph.json missing or unreadable. Skipping.", file=sys.stderr)
            continue

        in_progress_counts = {status: 0 for status in STALLED_STATUSES}
        for result in graph.results.values():
            if result.status in STALLED_STATUSES:
                in_progress_counts[result.status] += 1

        total_in_progress = sum(in_progress_counts.values())
        if total_in_progress > 0:
            counts_display = ", ".join(
                f"{status.value.capitalize()}: {count}"
                for status, count in in_progress_counts.items()
                if count > 0
            )
            print(
                f"SKIP: Graph {graph_id} has in-progress tasks ({counts_display}). Skipping.",
                file=sys.stderr,
            )
            had_stalled = True
            skipped_in_progress += 1
            continue

        eligible_graphs.append((graph_id, blobs))

    if not eligible_graphs:
        if had_stalled:
            print("No graphs eligible for purge (some skipped due to in-progress tasks).")
            return EXIT_STALLED
        print("Nothing to purge.")
        return EXIT_HEALTHY

    # --- Print deletion plan ---
    total_blob_count = sum(len(blobs) for _, blobs in eligible_graphs)
    print(f"Purge plan: blobs last modified before {cutoff_display} (older than {older_than_days} days)\n")
    for graph_id, blobs in eligible_graphs:
        print(f"Graph: {graph_id}  ({len(blobs)} blobs)")
    print(f"\nTotal: {len(eligible_graphs)} graphs, {total_blob_count} blobs")

    if dry_run:
        print("[DRY RUN] No blobs were deleted.")
        return EXIT_HEALTHY

    # --- Deletion pass: result blobs first, graph.json last ---
    deleted_blob_count = 0
    deleted_graph_count = 0
    had_errors = False

    for graph_id, blobs in eligible_graphs:
        graph_blob_path = f"{storage.task_result_prefix}/{graph_id}{graph_path_suffix}"
        result_blobs = [b for b in blobs if b.name != graph_blob_path]
        graph_json_blobs = [b for b in blobs if b.name == graph_blob_path]
        ordered_blobs = result_blobs + graph_json_blobs

        graph_deletion_failed = False
        for blob in ordered_blobs:
            try:
                await storage.delete_blob(blob.name)
                deleted_blob_count += 1
            except AzureBlobError as exc:
                if exc.status_code == 404:
                    logger.debug("Blob already deleted (404): %s", blob.name)
                    deleted_blob_count += 1
                else:
                    print(
                        f"WARNING: Failed to delete blob {blob.name}: status={exc.status_code}. Continuing.",
                        file=sys.stderr,
                    )
                    had_errors = True
                    graph_deletion_failed = True

        if not graph_deletion_failed:
            deleted_graph_count += 1

    print(f"\nDeleted {deleted_blob_count} blobs across {deleted_graph_count} graphs.")
    if had_stalled:
        print(f"Skipped {skipped_in_progress} graph(s) due to in-progress tasks.")

    if had_errors and deleted_graph_count == 0:
        return EXIT_ERROR
    if had_stalled:
        return EXIT_STALLED
    return EXIT_HEALTHY


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

    purge_parser = subparsers.add_parser(
        "purge",
        help="Delete old task-result blobs from Azure Blob Storage",
    )
    purge_parser.add_argument("--storage-url", required=True, help="Azure Blob Storage account URL")
    purge_parser.add_argument("--container", required=True, help="Blob container name")
    purge_parser.add_argument(
        "--older-than",
        required=True,
        type=_validate_older_than,
        metavar="DAYS",
        help="Delete blobs last modified more than DAYS days ago (1–30 inclusive)",
    )
    purge_parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted without deleting")
    purge_parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

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
            if args.command == "inspect":
                return await inspect_graph(
                    storage,
                    args.graph_id,
                    recover=args.recover,
                    sb_namespace_url=args.sb_namespace_url,
                    sb_queue_name=args.sb_queue_name,
                )
            if args.command == "purge":
                return await purge_old_results(
                    storage,
                    older_than_days=args.older_than,
                    dry_run=args.dry_run,
                )
            print(f"ERROR: Unknown command: {args.command}", file=sys.stderr)
            return EXIT_ERROR
        finally:
            await credentials.close()

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
