"""Purge subcommand handler for the boilermaker CLI."""

import argparse
import logging
import sys
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, UTC

import uuid_utils
from rich.console import Console

from boilermaker.cli._globals import (
    EXIT_ERROR,
    EXIT_HEALTHY,
    EXIT_STALLED,
    OLDER_THAN_MAX,
    OLDER_THAN_MIN,
    STALLED_STATUSES,
)
from boilermaker.cli._output import render_purge_plan
from boilermaker.exc import BoilermakerStorageError
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task import TaskGraph
from boilermaker.task.task_id import GraphId

logger = logging.getLogger("boilermaker.cli")


async def _stream_eligible_graphs(
    storage: BlobClientStorage,
    cutoff: datetime,
) -> AsyncGenerator[tuple[str, list], None]:
    """Yield (graph_id, blobs) tuples for graphs eligible for purge.

    Discovers candidate graph_ids via the blob tag index
    (created_date < cutoff), then lists each graph's full blob set.

    For containers with pre-tag blobs, use _stream_all_graphs instead
    (exposed via --all-graphs).
    """
    listing_prefix = f"{storage.task_result_prefix}/"
    cutoff_date = cutoff.strftime("%Y-%m-%d")
    candidate_graph_ids: set[str] = set()

    filter_expr = f"\"created_date\" < '{cutoff_date}'"
    async for blob in storage.find_blobs_by_tags(filter_expr):
        graph_id = blob.name.split("/")[1]
        candidate_graph_ids.add(graph_id)

    for graph_id in sorted(candidate_graph_ids):
        graph_prefix = f"{listing_prefix}{graph_id}/"
        blobs: list = []
        async for blob in storage.list_blobs(prefix=graph_prefix):
            blobs.append(blob)
        if blobs:
            yield graph_id, blobs


async def _stream_all_graphs(
    storage: BlobClientStorage,
    cutoff: datetime,
) -> AsyncGenerator[tuple[str, list], None]:
    """Yield (graph_id, blobs) for all graphs created before *cutoff*.

    Discovers graphs by listing all blobs under the task-results/ prefix and
    grouping them by graph_id (the second path segment).  Graph creation time
    is derived from the UUID7 graph_id, which encodes a millisecond timestamp.

    Because UUID7 values are temporally monotonic and ``list_blobs`` returns
    results in lexicographic order, the first graph_id whose timestamp is at or
    after *cutoff* signals that all subsequent groups are also too new.
    Iteration stops immediately at that point (early exit).

    Non-UUID7 graph_ids (which would raise on parse) are logged and skipped.
    """
    listing_prefix = f"{storage.task_result_prefix}/"

    current_graph_id: str | None = None
    current_blobs: list = []

    async for blob in storage.list_blobs(prefix=listing_prefix):
        graph_id = blob.name.split("/")[1]

        if graph_id != current_graph_id:
            # Flush the previous group
            if current_graph_id is not None:
                try:
                    uuid_obj = uuid_utils.UUID(current_graph_id)
                    graph_created = datetime.fromtimestamp(
                        uuid_obj.timestamp / 1000, tz=UTC
                    )
                except (ValueError, AttributeError) as exc:
                    logger.warning(
                        "Skipping graph %s: unable to parse UUID7 (%s).",
                        current_graph_id,
                        exc,
                    )
                    current_graph_id = graph_id
                    current_blobs = [blob]
                    continue

                if graph_created >= cutoff:
                    return  # early exit — all subsequent graphs are newer

                yield current_graph_id, current_blobs

            current_graph_id = graph_id
            current_blobs = []

        current_blobs.append(blob)

    # Flush the final group
    if current_graph_id is not None:
        try:
            uuid_obj = uuid_utils.UUID(current_graph_id)
            graph_created = datetime.fromtimestamp(
                uuid_obj.timestamp / 1000, tz=UTC
            )
        except (ValueError, AttributeError) as exc:
            logger.warning(
                "Skipping graph %s: unable to parse UUID7 (%s).",
                current_graph_id,
                exc,
            )
            return

        if graph_created < cutoff:
            yield current_graph_id, current_blobs


def _validate_older_than(value: str) -> int:
    """Validate --older-than argument as an integer in [1, 30].

    Used as an argparse type= validator. Raises argparse.ArgumentTypeError
    when the value is outside the allowed range.
    """
    try:
        days = int(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError(f"--older-than must be an integer, got: {value!r}") from err
    if days < OLDER_THAN_MIN or days > OLDER_THAN_MAX:
        raise argparse.ArgumentTypeError(
            f"--older-than must be between {OLDER_THAN_MIN} and {OLDER_THAN_MAX} (inclusive), got: {days}"
        )
    return days



async def run_purge(
    storage: BlobClientStorage,
    older_than_days: int,
    dry_run: bool = False,
    all_graphs: bool = False,
    force: bool = False,
    console: Console | None = None,
) -> int:
    """Delete task-result blobs older than the given threshold from Azure Blob Storage.

    Discovers eligible graphs via blob tag index (created_date < cutoff) or UUID7
    timestamp ordering (when all_graphs=True), then deletes their blobs. Graphs
    with in-progress tasks (Scheduled, Started, Retry) are skipped unless
    force=True.

    Args:
        storage: Blob storage client.
        older_than_days: Number of days; graphs created before this cutoff are eligible.
        dry_run: When True, prints the deletion plan but does not delete any blobs.
        all_graphs: When True, discovers graphs via UUID7 timestamp ordering
            (_stream_all_graphs). When False, uses tag-based discovery
            (_stream_eligible_graphs).
        force: When True, also delete graphs with in-progress tasks.
        console: Rich Console for output. When None, a default Console is created.

    Returns:
        EXIT_HEALTHY (0) on clean run or dry-run.
        EXIT_STALLED (1) if any graphs were skipped due to in-progress tasks.
        EXIT_ERROR (2) on unrecoverable error.
    """
    if console is None:
        console = Console()

    cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
    cutoff_display = cutoff.strftime("%Y-%m-%d")

    # --- Streaming pass: discover eligible graphs with age + safety checks ---
    graph_path_suffix = f"/{TaskGraph.StorageName}"

    eligible_graphs: list[tuple[str, list]] = []
    in_progress_graphs: list[tuple[str, list]] = []
    had_stalled = False

    try:
        graph_stream = (
            _stream_all_graphs(storage, cutoff)
            if all_graphs
            else _stream_eligible_graphs(storage, cutoff)
        )
        async for graph_id, blobs in graph_stream:
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

            has_in_progress = any(
                result.status in STALLED_STATUSES for result in graph.results.values()
            )
            if has_in_progress:
                in_progress_graphs.append((graph_id, blobs))
            else:
                eligible_graphs.append((graph_id, blobs))
    except Exception as exc:
        print(f"ERROR: Failed to list blobs: {exc}", file=sys.stderr)
        return EXIT_ERROR

    # --- Resolve in-progress graphs ---
    if in_progress_graphs:
        if force:
            eligible_graphs.extend(in_progress_graphs)
        else:
            for graph_id, _ in in_progress_graphs:
                print(
                    f"SKIP: Graph {graph_id} has in-progress tasks. Skipping.",
                    file=sys.stderr,
                )
            had_stalled = True

    if not eligible_graphs:
        if had_stalled:
            console.print("No graphs eligible for purge (some skipped due to in-progress tasks).")
            return EXIT_STALLED
        console.print("Nothing to purge.")
        return EXIT_HEALTHY

    # --- Print deletion plan ---
    total_blob_count = sum(len(blobs) for _, blobs in eligible_graphs)
    console.print(render_purge_plan(eligible_graphs, cutoff_display, older_than_days))
    console.print(f"\nTotal: {len(eligible_graphs)} graphs, {total_blob_count} blobs")

    if dry_run:
        console.print("[DRY RUN] No blobs were deleted.")
        return EXIT_HEALTHY

    # --- Deletion pass: batch delete, result blobs first, then graph.json ---
    had_errors = False

    # Track which graph.json path belongs to which graph, and which result blobs
    # belong to which graph — so we can skip graph.json if result deletion failed.
    result_blob_names: list[str] = []
    graph_json_by_graph: dict[str, str] = {}  # graph_id -> graph.json blob name
    result_blobs_by_graph: dict[str, list[str]] = {}  # graph_id -> result blob names

    for graph_id, blobs in eligible_graphs:
        graph_blob_path = f"{storage.task_result_prefix}/{graph_id}{graph_path_suffix}"
        result_blobs_by_graph[graph_id] = []
        for blob in blobs:
            if blob.name == graph_blob_path:
                graph_json_by_graph[graph_id] = blob.name
            else:
                result_blob_names.append(blob.name)
                result_blobs_by_graph[graph_id].append(blob.name)

    # Delete result blobs first
    failed_results = await storage.delete_blobs_batch(result_blob_names)
    failed_results_set = set(failed_results)

    for name in failed_results:
        print(f"WARNING: Failed to delete blob {name}. Continuing.", file=sys.stderr)
        had_errors = True

    # Only delete graph.json for graphs whose result blobs all succeeded
    safe_graph_json_names = [
        graph_json_by_graph[graph_id]
        for graph_id, _ in eligible_graphs
        if graph_id in graph_json_by_graph
        and not any(b in failed_results_set for b in result_blobs_by_graph[graph_id])
    ]
    failed_graphs = await storage.delete_blobs_batch(safe_graph_json_names)

    for name in failed_graphs:
        print(f"WARNING: Failed to delete blob {name}. Continuing.", file=sys.stderr)
        had_errors = True

    all_failed = failed_results_set | set(failed_graphs)
    total_deleted = (len(result_blob_names) + len(safe_graph_json_names)) - len(all_failed)
    deleted_graph_count = sum(
        1 for graph_id, blobs in eligible_graphs
        if not any(b.name in all_failed for b in blobs)
    )

    console.print(f"\nDeleted {total_deleted} blobs across {deleted_graph_count} graphs.")
    if had_stalled:
        console.print(f"Skipped {len(in_progress_graphs)} graph(s) due to in-progress tasks.")

    if had_errors and deleted_graph_count == 0:
        return EXIT_ERROR
    if had_stalled:
        return EXIT_STALLED
    return EXIT_HEALTHY
