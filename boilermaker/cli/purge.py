"""Purge subcommand handler for the boilermaker CLI."""

import argparse
import logging
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
from boilermaker.task.task_id import GraphId, TaskId

logger = logging.getLogger("boilermaker.cli")


def _graph_created_at(graph_id: str) -> datetime | None:
    """Return the creation time encoded in a UUID7 *graph_id*, or None.

    UUID7 values embed a millisecond Unix timestamp, so a graph's age can be
    derived from its id alone — no blob tags required. Returns None for anything
    that is not a version-7 UUID (including v1/v6, whose ``.timestamp`` is a
    different epoch and must not be trusted here) or whose timestamp is out of
    range, so a single malformed id can never abort the whole purge.
    """
    try:
        uuid_obj = uuid_utils.UUID(graph_id)
        if uuid_obj.version != 7:
            return None
        return datetime.fromtimestamp(uuid_obj.timestamp / 1000, tz=UTC)
    except (ValueError, AttributeError, OverflowError, OSError):
        return None


async def _stream_eligible_graphs(
    storage: BlobClientStorage,
    cutoff: datetime,
) -> AsyncGenerator[tuple[str, list], None]:
    """Yield (graph_id, blobs) tuples for graphs eligible for purge.

    Discovers candidate graph_ids via the blob tag index
    (``created_date < cutoff``), then lists each graph's full blob set. This is
    the fast, default path: the tag index filters server-side, so no full
    container scan is needed.

    It only sees blobs that carry a ``created_date`` tag. Graphs written before
    tag support (or otherwise untagged) are invisible here — use
    ``_stream_all_graphs`` (exposed via ``--all-graphs``) to sweep those by
    UUID7 timestamp instead.
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

    Non-UUID7 graph_ids (which cannot be dated this way) are logged via the
    ``boilermaker.cli`` logger and skipped.
    """
    listing_prefix = f"{storage.task_result_prefix}/"

    current_graph_id: str | None = None
    current_blobs: list = []

    async for blob in storage.list_blobs(prefix=listing_prefix):
        graph_id = blob.name.split("/")[1]

        if graph_id != current_graph_id:
            # Flush the previous group
            if current_graph_id is not None:
                graph_created = _graph_created_at(current_graph_id)
                if graph_created is None:
                    logger.warning(
                        "Graph %s appears corrupted — graph_id is not a UUID7, so its age "
                        "cannot be determined. Skipping.",
                        current_graph_id,
                    )
                elif graph_created >= cutoff:
                    return  # early exit — all subsequent graphs are newer
                else:
                    yield current_graph_id, current_blobs

            current_graph_id = graph_id
            current_blobs = []

        current_blobs.append(blob)

    # Flush the final group
    if current_graph_id is not None:
        graph_created = _graph_created_at(current_graph_id)
        if graph_created is None:
            logger.warning(
                "Graph %s appears corrupted — graph_id is not a UUID7, so its age "
                "cannot be determined. Skipping.",
                current_graph_id,
            )
        elif graph_created < cutoff:
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

    By default, discovers eligible graphs via the blob tag index
    (created_date < cutoff) — fast, but blind to untagged/pre-tag graphs. When
    all_graphs=True, instead lists every graph-id prefix and dates each by its
    UUID7 timestamp (slower full scan, but finds untagged graphs).
    Graphs with in-progress tasks (Scheduled, Started, Retry) are skipped
    unless force=True.

    Args:
        storage: Blob storage client.
        older_than_days: Number of days; graphs created before this cutoff are eligible.
        dry_run: When True, prints the deletion plan but does not delete any blobs.
        all_graphs: When True, discovers graphs via a full UUID7-timestamp prefix
            scan (_stream_all_graphs) that also finds untagged/pre-tag graphs.
            When False (default), uses fast tag-index discovery
            (_stream_eligible_graphs).
        force: When True, also delete graphs (and standalone tasks) with
            in-progress status, and purge corrupted graph directories that have
            no graph.json but still hold task blobs. Standalone task-results are
            otherwise purged on age unless they are still in progress.
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
                # A standalone task-result (graph_id=null) is written to
                # task-results/{task_id}/{task_id}.json: a single blob named
                # after its own directory, with no graph.json. A directory that
                # is missing graph.json but holds *other* blobs is instead a
                # damaged graph whose state we cannot reconstruct.
                standalone_blob = f"{storage.task_result_prefix}/{graph_id}/{graph_id}.json"
                is_standalone = all(blob.name == standalone_blob for blob in blobs)
                if not is_standalone:
                    # Corrupted: task-result blobs with no graph.json. This is
                    # the "something is weird here" case — flag it so the
                    # operator can investigate. Delete only under --force.
                    if force:
                        eligible_graphs.append((graph_id, blobs))
                    else:
                        logger.warning(
                            "Graph %s appears corrupted — it has task-result blobs but no graph.json, "
                            "so its status cannot be verified. Skipping (use --force to delete).",
                            graph_id,
                        )
                    continue

                # Standalone task: safe to purge on age, but not while it is
                # still in progress. Load its status and protect stuck/retrying
                # tasks exactly as we protect in-progress graphs.
                if force:
                    eligible_graphs.append((graph_id, blobs))
                    continue
                try:
                    task_result = await storage.load_task_result(TaskId(graph_id), GraphId(graph_id))
                except BoilermakerStorageError as exc:
                    logger.warning(
                        "Task %s appears corrupted — its result blob could not be loaded (%s). Skipping.",
                        graph_id,
                        exc,
                    )
                    continue
                if task_result is not None and task_result.status in STALLED_STATUSES:
                    in_progress_graphs.append((graph_id, blobs))
                else:
                    eligible_graphs.append((graph_id, blobs))
                continue

            try:
                graph = await storage.load_graph(GraphId(graph_id))
            except BoilermakerStorageError as exc:
                logger.warning(
                    "Graph %s appears corrupted — graph.json could not be loaded (%s). Skipping.",
                    graph_id,
                    exc,
                )
                continue

            if graph is None:
                logger.warning(
                    "Graph %s appears corrupted — graph.json is missing or unreadable. Skipping.",
                    graph_id,
                )
                continue

            has_in_progress = any(
                result.status in STALLED_STATUSES for result in graph.results.values()
            )
            if has_in_progress:
                in_progress_graphs.append((graph_id, blobs))
            else:
                eligible_graphs.append((graph_id, blobs))
    except Exception as exc:
        logger.error("Failed to list blobs: %s", exc)
        return EXIT_ERROR

    # --- Resolve in-progress graphs ---
    if in_progress_graphs:
        if force:
            eligible_graphs.extend(in_progress_graphs)
        else:
            # In-progress graphs/tasks are expected (still running or retrying),
            # not corrupted. Skip them quietly — the aggregate count is reported
            # in the summary and reflected in the exit code — and reserve the
            # noisy per-graph SKIP lines for genuinely weird/corrupted data.
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
        logger.warning("Failed to delete blob %s. Continuing.", name)
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
        logger.warning("Failed to delete blob %s. Continuing.", name)
        had_errors = True

    all_failed = failed_results_set | set(failed_graphs)
    total_deleted = (len(result_blob_names) + len(safe_graph_json_names)) - len(all_failed)
    deleted_graph_count = sum(
        1 for graph_id, blobs in eligible_graphs
        if not any(b.name in all_failed for b in blobs)
    )

    console.print(f"\nDeleted {total_deleted} blobs across {deleted_graph_count} graphs.")
    if had_stalled:
        console.print(f"Skipped {len(in_progress_graphs)} graph(s)/task(s) with in-progress tasks.")

    if had_errors and deleted_graph_count == 0:
        return EXIT_ERROR
    if had_stalled:
        return EXIT_STALLED
    return EXIT_HEALTHY
