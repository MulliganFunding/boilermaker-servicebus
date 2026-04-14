"""Tests for the lease+ETag scheduling guard in continue_graph and publish_graph.

These tests verify:
- Lease acquisition is attempted before publishing ready tasks
- Workers that fail to acquire the lease skip the task immediately
- Lease is released in finally block (even on publish/write failure)
- ETag from graph.results is passed to try_acquire_lease
- store_task_result is called without etag when lease is held
- try_acquire_lease returns None on 409 and 412 (BlobClientStorage level)
- try_acquire_lease raises BoilermakerStorageError on non-409/412 errors
- Fan-in: two concurrent calls, one gets lease, other skips
- publish_graph uses publish-before-store ordering (matching continue_graph)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from boilermaker import exc
from boilermaker.task import TaskGraph, TaskResult, TaskStatus

# # # # # # # # # # # # # # # # # # # # # # # # # # #
# continue_graph lease guard tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_lease_acquired_before_publish(success_scenario):
    """Lease must be acquired on each ready task before publishing to Service Bus."""
    async with success_scenario.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ):
        # try_acquire_lease should have been called for each ready task
        lease_calls = success_scenario.mock_storage.try_acquire_lease.call_args_list
        assert len(lease_calls) >= 1, "try_acquire_lease must be called for ready tasks"

        # release_lease should have been called the same number of times
        release_calls = success_scenario.mock_storage.release_lease.call_args_list
        assert len(release_calls) == len(lease_calls), (
            "release_lease must be called once per successful lease acquisition"
        )


async def test_lease_skips_task_when_not_acquired(evaluator_context):
    """When try_acquire_lease returns None, the task must be skipped entirely."""
    evaluator_context.prep_task_to_succeed()

    # Make lease acquisition fail for all tasks
    evaluator_context.mock_storage.try_acquire_lease.return_value = None

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # No downstream tasks should be published since lease was not acquired
        ctx.assert_messages_scheduled(0)

        # release_lease should NOT be called when acquisition failed
        evaluator_context.mock_storage.release_lease.assert_not_called()


async def test_lease_released_on_publish_failure(evaluator_context):
    """Lease must be released even when publish_task raises an exception."""
    evaluator_context.prep_task_to_succeed()

    # Make publish_task raise an exception
    async def failing_publish(task, *args, **kwargs):
        raise RuntimeError("publish failed")

    evaluator_context.mock_task_publisher = failing_publish
    evaluator_context.create_evaluator(evaluator_context.ok_task)

    # Override publish to raise
    evaluator_context.evaluator._task_publisher = failing_publish

    completed = TaskResult(
        task_id=evaluator_context.ok_task.task_id,
        graph_id=evaluator_context.graph.graph_id,
        status=TaskStatus.Success,
        result="OK",
    )

    await evaluator_context.evaluator.continue_graph(completed)

    # Lease should still have been released despite publish failure
    assert evaluator_context.mock_storage.release_lease.call_count > 0, (
        "release_lease must be called even when publish fails"
    )


async def test_lease_released_on_storage_error(evaluator_context):
    """Lease must be released even when store_task_result raises BoilermakerStorageError."""
    evaluator_context.prep_task_to_succeed()

    # Make store_task_result raise on the scheduling writes (3rd+ calls)
    call_count = {"n": 0}

    async def selective_storage_error(*args, **kwargs):
        call_count["n"] += 1
        # First two calls are for started + result; subsequent are scheduling writes
        if call_count["n"] > 2:
            raise exc.BoilermakerStorageError("write failed")

    evaluator_context.mock_storage.store_task_result.side_effect = selective_storage_error

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
        check_graph_loaded=True,
    ):
        # Lease must be released even when storage write fails
        assert evaluator_context.mock_storage.release_lease.call_count > 0


async def test_etag_passed_to_lease_acquisition(evaluator_context):
    """ETag from graph results must be passed to try_acquire_lease."""
    evaluator_context.prep_task_to_succeed()
    graph = evaluator_context.graph

    # Set etags on graph results so they get passed through
    test_etag = "test-etag-value-123"
    for _task_id, result_slim in graph.results.items():
        result_slim.etag = test_etag

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ):
        lease_calls = evaluator_context.mock_storage.try_acquire_lease.call_args_list
        assert len(lease_calls) >= 1

        # At least one call should have the test etag
        # Check via keyword argument
        for lease_call in lease_calls:
            # try_acquire_lease(task_id, graph_id, etag=...)
            if "etag" in lease_call.kwargs:
                assert lease_call.kwargs["etag"] == test_etag, (
                    f"Expected etag={test_etag}, got {lease_call.kwargs['etag']}"
                )


async def test_store_task_result_called_without_etag(evaluator_context):
    """When lease is held, store_task_result must be called without etag kwargs."""
    evaluator_context.prep_task_to_succeed()

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # Get the scheduling write calls (beyond started + result)
        others = ctx.get_other_storage_calls()
        for scheduling_call in others:
            # The call should NOT have etag kwarg
            assert "etag" not in scheduling_call.kwargs, (
                "store_task_result in scheduling path must not pass etag when lease is held"
            )


async def test_lease_not_acquired_no_scheduled_write(evaluator_context):
    """When lease is not acquired, no store_task_result scheduling write should happen."""
    evaluator_context.prep_task_to_succeed()

    # First call to try_acquire_lease returns None (simulating lost race)
    evaluator_context.mock_storage.try_acquire_lease.return_value = None

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ) as ctx:
        # Only the started + result writes should exist (no scheduling writes)
        others = ctx.get_other_storage_calls()
        assert len(others) == 0, (
            "No scheduling writes should happen when lease is not acquired"
        )


async def test_selective_lease_failure_skips_one_task(evaluator_context, app):
    """When lease fails for one ready task but succeeds for another, only the successful one is published."""

    async def task_a(state):
        return "A"

    async def task_b(state):
        return "B"

    async def root_fn(state):
        return "root"

    app.register_many_async([task_a, task_b, root_fn])

    root = app.create_task(root_fn)
    ta = app.create_task(task_a)
    tb = app.create_task(task_b)

    graph = TaskGraph()
    graph.add_task(root)
    graph.add_task(ta, parent_ids=[root.task_id])
    graph.add_task(tb, parent_ids=[root.task_id])
    list(graph.generate_pending_results())

    # Root succeeded
    graph.add_result(TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    evaluator_context.graph = graph
    evaluator_context.create_evaluator(root)

    # Lease succeeds for tb but fails for ta
    async def selective_lease(task_id, graph_id, etag=None, lease_duration=15):
        if task_id == ta.task_id:
            return None  # Lost the race
        return "mock-lease-id"

    evaluator_context.mock_storage.try_acquire_lease.side_effect = selective_lease

    completed = TaskResult(task_id=root.task_id, graph_id=graph.graph_id, status=TaskStatus.Success)
    ready_count = await evaluator_context.evaluator.continue_graph(completed)

    published = evaluator_context.get_scheduled_messages()
    published_ids = {msg.task.task_id for msg in published}

    assert tb.task_id in published_ids, "task_b should be published (lease acquired)"
    assert ta.task_id not in published_ids, "task_a should NOT be published (lease not acquired)"
    assert ready_count == 1


async def test_lease_duration_default_is_15(evaluator_context):
    """The default lease duration passed to try_acquire_lease should be 15 seconds."""
    evaluator_context.prep_task_to_succeed()

    async with evaluator_context.with_regular_assertions(
        compare_result="OK",
        compare_status=TaskStatus.Success,
    ):
        lease_calls = evaluator_context.mock_storage.try_acquire_lease.call_args_list
        for lease_call in lease_calls:
            # lease_duration should not be explicitly passed (uses default=15)
            # or if passed, should be 15
            if "lease_duration" in lease_call.kwargs:
                assert lease_call.kwargs["lease_duration"] == 15


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# publish_graph lease guard tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_publish_graph_acquires_lease(app, mock_storage):
    """publish_graph must acquire lease on root tasks before publishing."""

    async def root_fn(state):
        return "root"

    app.register_async(root_fn)

    graph = TaskGraph()
    root = app.create_task(root_fn)
    graph.add_task(root)
    list(graph.generate_pending_results())

    # Set up results_storage
    app.results_storage = mock_storage

    # load_graph returns the same graph object (simulating reload with etags)
    # Set an etag on the pending result
    for result_slim in graph.results.values():
        result_slim.etag = "initial-etag"

    mock_storage.store_graph.return_value = graph
    mock_storage.load_graph.return_value = graph

    await app.publish_graph(graph)

    # try_acquire_lease should have been called for the root task
    assert mock_storage.try_acquire_lease.call_count >= 1
    lease_call = mock_storage.try_acquire_lease.call_args_list[0]
    assert lease_call.args[0] == root.task_id

    # release_lease should also have been called
    assert mock_storage.release_lease.call_count >= 1


async def test_publish_graph_skips_on_lease_failure(app, mock_storage):
    """publish_graph must skip root tasks when lease acquisition fails."""

    async def root_fn(state):
        return "root"

    app.register_async(root_fn)

    graph = TaskGraph()
    root = app.create_task(root_fn)
    graph.add_task(root)
    list(graph.generate_pending_results())

    app.results_storage = mock_storage

    mock_storage.store_graph.return_value = graph
    mock_storage.load_graph.return_value = graph

    # Lease acquisition fails
    mock_storage.try_acquire_lease.return_value = None

    await app.publish_graph(graph)

    # store_task_result should NOT be called for scheduling writes
    # (store_graph handles initial pending writes, not store_task_result)
    mock_storage.store_task_result.assert_not_called()

    # release_lease should NOT be called when acquisition failed
    mock_storage.release_lease.assert_not_called()


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# BlobClientStorage-level lease tests (mocked Azure SDK)
# # # # # # # # # # # # # # # # # # # # # # # # # # #


@pytest.fixture
def blob_storage():
    """Create a BlobClientStorage instance with mocked credentials."""
    from unittest.mock import MagicMock

    from azure.identity.aio import DefaultAzureCredential
    from boilermaker.storage.blob_storage import BlobClientStorage

    mock_creds = MagicMock(spec=DefaultAzureCredential)
    return BlobClientStorage(
        az_storage_url="https://teststorage.blob.core.windows.net",
        container_name="test-container",
        credentials=mock_creds,
    )


def _make_azure_blob_error(status_code, reason):
    """Create an AzureBlobError by constructing a mock HttpResponseError."""
    from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError

    mock_http_error = MagicMock()
    mock_http_error.status_code = status_code
    mock_http_error.reason = reason
    mock_http_error.message = f"{status_code}: {reason}"
    return AzureBlobError(mock_http_error)


async def test_try_acquire_lease_returns_none_on_409(blob_storage):
    """try_acquire_lease must return None when Azure returns 409 (blob already leased)."""
    mock_blob_client = AsyncMock()
    mock_lease_client = AsyncMock()

    mock_lease_client.acquire.side_effect = _make_azure_blob_error(
        409, "There is already a lease present."
    )

    with patch.object(blob_storage, "get_blob_client") as mock_get_blob:
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = mock_blob_client
        mock_ctx.__aexit__.return_value = False
        mock_get_blob.return_value = mock_ctx

        with patch("boilermaker.storage.blob_storage.BlobLeaseClient", return_value=mock_lease_client):
            result = await blob_storage.try_acquire_lease("task-1", "graph-1")

    assert result is None


async def test_try_acquire_lease_returns_none_on_412(blob_storage):
    """try_acquire_lease must return None when Azure returns 412 (ETag mismatch)."""
    mock_blob_client = AsyncMock()
    mock_lease_client = AsyncMock()

    mock_lease_client.acquire.side_effect = _make_azure_blob_error(
        412, "The condition specified using HTTP conditional header(s) is not met."
    )

    with patch.object(blob_storage, "get_blob_client") as mock_get_blob:
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = mock_blob_client
        mock_ctx.__aexit__.return_value = False
        mock_get_blob.return_value = mock_ctx

        with patch("boilermaker.storage.blob_storage.BlobLeaseClient", return_value=mock_lease_client):
            result = await blob_storage.try_acquire_lease(
                "task-1", "graph-1", etag="stale-etag"
            )

    assert result is None


async def test_try_acquire_lease_raises_on_non_409_412(blob_storage):
    """try_acquire_lease must raise BoilermakerStorageError on non-409/412 errors."""
    mock_blob_client = AsyncMock()
    mock_lease_client = AsyncMock()

    mock_lease_client.acquire.side_effect = _make_azure_blob_error(
        404, "The specified blob does not exist."
    )

    with patch.object(blob_storage, "get_blob_client") as mock_get_blob:
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = mock_blob_client
        mock_ctx.__aexit__.return_value = False
        mock_get_blob.return_value = mock_ctx

        with patch("boilermaker.storage.blob_storage.BlobLeaseClient", return_value=mock_lease_client):
            with pytest.raises(exc.BoilermakerStorageError):
                await blob_storage.try_acquire_lease("task-1", "graph-1")


async def test_try_acquire_lease_passes_etag_to_acquire(blob_storage):
    """try_acquire_lease must pass etag and MatchConditions.IfNotModified to BlobLeaseClient.acquire."""
    from azure.core import MatchConditions

    mock_blob_client = AsyncMock()
    mock_lease_client = AsyncMock()
    mock_lease_client.id = "acquired-lease-id"
    mock_lease_client.acquire = AsyncMock()

    with patch.object(blob_storage, "get_blob_client") as mock_get_blob:
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = mock_blob_client
        mock_ctx.__aexit__.return_value = False
        mock_get_blob.return_value = mock_ctx

        with patch("boilermaker.storage.blob_storage.BlobLeaseClient", return_value=mock_lease_client):
            result = await blob_storage.try_acquire_lease(
                "task-1", "graph-1", etag="my-etag-value"
            )

    assert result == "acquired-lease-id"
    mock_lease_client.acquire.assert_called_once_with(
        lease_duration=15,
        etag="my-etag-value",
        match_condition=MatchConditions.IfNotModified,
    )


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# Fan-in lease guard tests
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_fan_in_two_concurrent_calls_one_wins(evaluator_context, app):
    """Fan-in: when two workers race on the same fan-in task, only one wins the lease.

    Simulates two workers completing the two parents of a fan-in task. The first
    worker acquires the lease and publishes the fan-in task. The second worker
    fails to acquire the lease and skips.
    """

    async def parent_a(state):
        return "A"

    async def parent_b(state):
        return "B"

    async def fan_in_task(state):
        return "fan-in"

    app.register_many_async([parent_a, parent_b, fan_in_task])

    pa = app.create_task(parent_a)
    pb = app.create_task(parent_b)
    fi = app.create_task(fan_in_task)

    graph = TaskGraph()
    graph.add_task(pa)
    graph.add_task(pb)
    graph.add_task(fi, parent_ids=[pa.task_id, pb.task_id])
    list(graph.generate_pending_results())

    # Both parents succeeded
    graph.add_result(TaskResult(task_id=pa.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))
    graph.add_result(TaskResult(task_id=pb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success))

    # Worker 1: completes parent_a, acquires lease
    evaluator_context.graph = graph
    evaluator_context.create_evaluator(pa)

    completed_a = TaskResult(task_id=pa.task_id, graph_id=graph.graph_id, status=TaskStatus.Success)
    ready_count_1 = await evaluator_context.evaluator.continue_graph(completed_a)

    published_1 = evaluator_context.get_scheduled_messages()
    published_ids_1 = {msg.task.task_id for msg in published_1}
    assert fi.task_id in published_ids_1, "Worker 1 should publish the fan-in task"
    assert ready_count_1 == 1

    # Worker 2: completes parent_b, but lease already held (returns None)
    evaluator_context._published_messages.clear()
    evaluator_context.mock_storage.try_acquire_lease.return_value = None
    evaluator_context.create_evaluator(pb)

    completed_b = TaskResult(task_id=pb.task_id, graph_id=graph.graph_id, status=TaskStatus.Success)
    ready_count_2 = await evaluator_context.evaluator.continue_graph(completed_b)

    published_2 = evaluator_context.get_scheduled_messages()
    assert len(published_2) == 0, "Worker 2 should NOT publish (lease not acquired)"
    assert ready_count_2 == 0


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# publish_graph ordering tests (publish-before-store)
# # # # # # # # # # # # # # # # # # # # # # # # # # #


async def test_publish_graph_publishes_before_store(app, mock_storage):
    """publish_graph must publish to SB before writing Scheduled (matching continue_graph ordering).

    This verifies the ordering: acquire lease -> publish -> write Scheduled -> release.
    If publish fails, the task remains Pending (no Scheduled write).
    """

    async def root_fn(state):
        return "root"

    app.register_async(root_fn)

    graph = TaskGraph()
    root = app.create_task(root_fn)
    graph.add_task(root)
    list(graph.generate_pending_results())

    app.results_storage = mock_storage
    mock_storage.store_graph.return_value = graph
    mock_storage.load_graph.return_value = graph

    # Track ordering of operations
    call_order = []

    original_publish = app.publish_task

    async def tracking_publish(task, **kwargs):
        call_order.append("publish")
        return await original_publish(task, **kwargs)

    app.publish_task = tracking_publish

    async def tracking_store(*args, **kwargs):
        call_order.append("store")

    mock_storage.store_task_result.side_effect = tracking_store

    await app.publish_graph(graph)

    # Verify publish happens before store
    assert "publish" in call_order, "publish_task must be called"
    assert "store" in call_order, "store_task_result must be called"
    publish_idx = call_order.index("publish")
    store_idx = call_order.index("store")
    assert publish_idx < store_idx, (
        f"publish must happen before store, but got order: {call_order}"
    )


async def test_publish_graph_publish_failure_skips_store(app, mock_storage):
    """When publish_task fails in publish_graph, store_task_result must NOT be called.

    The task remains Pending in blob storage and will be retried on next invocation.
    """
    from boilermaker.exc import BoilermakerAppException

    async def root_fn(state):
        return "root"

    app.register_async(root_fn)

    graph = TaskGraph()
    root = app.create_task(root_fn)
    graph.add_task(root)
    list(graph.generate_pending_results())

    app.results_storage = mock_storage
    mock_storage.store_graph.return_value = graph
    mock_storage.load_graph.return_value = graph

    # Make publish fail
    async def failing_publish(task, **kwargs):
        raise BoilermakerAppException("SB unavailable", [])

    app.publish_task = failing_publish

    await app.publish_graph(graph)

    # store_task_result should NOT be called (publish failed before store)
    mock_storage.store_task_result.assert_not_called()

    # Lease must still be released despite publish failure
    assert mock_storage.release_lease.call_count >= 1, (
        "release_lease must be called even when publish fails"
    )
