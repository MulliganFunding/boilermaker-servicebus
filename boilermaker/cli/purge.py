"""Purge subcommand handler for the boilermaker CLI."""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, UTC

from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
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
        console.print("Nothing to purge.")
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

    console.print(f"\nDeleted {deleted_blob_count} blobs across {deleted_graph_count} graphs.")
    if had_stalled:
        console.print(f"Skipped {skipped_in_progress} graph(s) due to in-progress tasks.")

    if had_errors and deleted_graph_count == 0:
        return EXIT_ERROR
    if had_stalled:
        return EXIT_STALLED
    return EXIT_HEALTHY
