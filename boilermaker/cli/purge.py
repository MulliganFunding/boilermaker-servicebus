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
) -> AsyncGenerator[tuple[str, list]]:
    """Yield (graph_id, blobs) tuples for graphs eligible for purge.

    Uses a dual-path strategy:
    1. Tagged path: find_blobs_by_tags for blobs with created_date < cutoff
    2. Legacy path: list_blobs for untagged blobs with last_modified < cutoff

    For each candidate graph_id from either path, lists all blobs under that
    graph to get the complete set, then applies the age filter.

    Falls back to full legacy listing if find_blobs_by_tags fails.
    """
    listing_prefix = f"{storage.task_result_prefix}/"
    cutoff_date = cutoff.strftime("%Y-%m-%d")
    candidate_graph_ids: set[str] = set()
    tag_query_failed = False

    # --- Tagged path: discover candidate graph_ids via tag index ---
    try:
        filter_expr = f"\"created_date\" < '{cutoff_date}'"
        async for blob in storage.find_blobs_by_tags(filter_expr):
            graph_id = blob.name.split("/")[1]
            candidate_graph_ids.add(graph_id)
    except Exception as exc:
        logger.warning("find_blobs_by_tags failed (%s); falling back to full listing.", exc)
        tag_query_failed = True

    # --- Legacy path: discover untagged blobs via list_blobs ---
    if tag_query_failed:
        # Full fallback: list everything and filter by last_modified
        current_graph_id: str | None = None
        current_blobs: list = []

        async for blob in storage.list_blobs(prefix=listing_prefix):
            graph_id = blob.name.split("/")[1]
            if graph_id != current_graph_id:
                if current_graph_id is not None:
                    most_recent = max(b.last_modified for b in current_blobs)
                    if most_recent < cutoff:
                        yield current_graph_id, current_blobs
                current_graph_id = graph_id
                current_blobs = []
            current_blobs.append(blob)

        if current_graph_id is not None:
            most_recent = max(b.last_modified for b in current_blobs)
            if most_recent < cutoff:
                yield current_graph_id, current_blobs
        return  # Done — full fallback handled everything

    # --- Legacy path (tag query succeeded): find untagged blobs only ---
    async for blob in storage.list_blobs(prefix=listing_prefix):
        tag_count = getattr(blob, "tag_count", None)
        if tag_count is None or tag_count == 0:
            if blob.last_modified < cutoff:
                graph_id = blob.name.split("/")[1]
                candidate_graph_ids.add(graph_id)

    # --- For each candidate: list full blob set, apply age filter, yield ---
    for graph_id in sorted(candidate_graph_ids):
        graph_prefix = f"{listing_prefix}{graph_id}/"
        blobs: list = []
        async for blob in storage.list_blobs(prefix=graph_prefix):
            blobs.append(blob)

        if not blobs:
            continue

        most_recent = max(b.last_modified for b in blobs)
        if most_recent < cutoff:
            yield graph_id, blobs


async def _stream_all_graphs(
    storage: BlobClientStorage,
    cutoff: datetime,
) -> AsyncGenerator[tuple[str, list]]:
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
    console: Console | None = None,
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
        all_graphs: When True, discovers graphs via UUID7 timestamp ordering
            (_stream_all_graphs). When False, uses the dual-path tag+legacy
            strategy (_stream_eligible_graphs).
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
    had_stalled = False
    skipped_in_progress = 0

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
    except Exception as exc:
        print(f"ERROR: Failed to list blobs: {exc}", file=sys.stderr)
        return EXIT_ERROR

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

    result_blob_names: list[str] = []
    graph_json_blob_names: list[str] = []

    for graph_id, blobs in eligible_graphs:
        graph_blob_path = f"{storage.task_result_prefix}/{graph_id}{graph_path_suffix}"
        for blob in blobs:
            if blob.name == graph_blob_path:
                graph_json_blob_names.append(blob.name)
            else:
                result_blob_names.append(blob.name)

    # Delete result blobs first, then graph.json (ordering preserved)
    failed_results = await storage.delete_blobs_batch(result_blob_names)
    failed_graphs = await storage.delete_blobs_batch(graph_json_blob_names)

    # Report failures
    all_failed = failed_results + failed_graphs
    for name in all_failed:
        print(f"WARNING: Failed to delete blob {name}. Continuing.", file=sys.stderr)
        had_errors = True

    total_deleted = (len(result_blob_names) + len(graph_json_blob_names)) - len(all_failed)
    # Count fully-deleted graphs (no failures in their blobs)
    failed_set = set(all_failed)
    deleted_graph_count = sum(
        1 for gid, blobs in eligible_graphs
        if not any(b.name in failed_set for b in blobs)
    )

    console.print(f"\nDeleted {total_deleted} blobs across {deleted_graph_count} graphs.")
    if had_stalled:
        console.print(f"Skipped {skipped_in_progress} graph(s) due to in-progress tasks.")

    if had_errors and deleted_graph_count == 0:
        return EXIT_ERROR
    if had_stalled:
        return EXIT_STALLED
    return EXIT_HEALTHY
