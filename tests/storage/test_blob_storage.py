import re
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import BlobProperties
from boilermaker.exc import BoilermakerStorageError
from boilermaker.storage.blob_storage import BlobClientStorage
from boilermaker.task import (
    GraphId,
    Task,
    TaskGraph,
    TaskResult,
    TaskResultSlim,
    TaskStatus,
)


@pytest.fixture
def sample_task():
    """Create a sample task for testing."""
    return Task.default("test_function", args=[1, 2], kwargs={"key": "value"})


@pytest.fixture
def mock_credentials():
    """Mock Azure credentials for testing."""
    return MagicMock(spec=DefaultAzureCredential)


@pytest.fixture
def blob_storage(mock_credentials):
    """Create a BlobClientStorage instance for testing."""
    return BlobClientStorage(
        az_storage_url="https://teststorage.blob.core.windows.net",
        container_name="test-container",
        credentials=mock_credentials,
    )


@pytest.fixture
def sample_task_result(sample_task):
    """Create a sample TaskResult for testing."""
    return TaskResult(
        task_id=sample_task.task_id,
        graph_id=GraphId("test-graph-id"),
        status=TaskStatus.Success,
        result={"output": "test result"},
        errors=None,
        formatted_exception=None,
    )


@pytest.fixture
def sample_task_graph(sample_task):
    """Create a sample TaskGraph for testing."""
    graph = TaskGraph()
    graph.children[sample_task.task_id] = sample_task
    # Set the task's graph_id
    sample_task.graph_id = graph.graph_id
    return graph


@pytest.fixture
def sample_task_result_slim(sample_task):
    """Create a sample TaskResultSlim for testing."""
    return TaskResultSlim(
        task_id=sample_task.task_id,
        graph_id=GraphId("test-graph-id"),
        status=TaskStatus.Success,
    )


# Tests for store_task_result method
async def test_store_task_result_success(mock_azureblob, blob_storage, sample_task_result):
    """Test successful storage of a TaskResult."""
    _container_client, mockblobc, _ = mock_azureblob
    mockblobc.upload_blob.return_value = {"status": "success"}

    # Fire upload call
    await blob_storage.store_task_result(sample_task_result)

    # Verify upload_blob was called with correct parameters
    assert len(mockblobc.mock_calls) == 1
    blob_upload_call = mockblobc.mock_calls[0]

    # Check JSON content
    assert sample_task_result.model_dump_json() in blob_upload_call.args[0]

    # Check tags and overwrite
    assert blob_upload_call.kwargs["overwrite"] is True
    tags = blob_upload_call.kwargs["tags"]
    assert tags["graph_id"] == sample_task_result.graph_id
    assert tags["status"] == sample_task_result.status.value


async def test_store_task_result_with_azure_blob_error(blob_storage, sample_task_result):
    """Test error handling when Azure blob storage fails."""
    with patch.object(blob_storage, "upload_blob", new_callable=AsyncMock) as mock_upload:
        error = AzureBlobError(
            MagicMock(
                **{
                    "message": "Storage error",
                    "status_code": 500,
                    "reason": "Internal Error",
                }
            )
        )
        mock_upload.side_effect = error

        with pytest.raises(BoilermakerStorageError) as exc_info:
            await blob_storage.store_task_result(sample_task_result)

        assert "Failed to store TaskResult" in str(exc_info.value)
        assert exc_info.value.task_id == sample_task_result.task_id
        assert exc_info.value.graph_id == sample_task_result.graph_id
        assert exc_info.value.status_code == 500


# Tests for load_graph method
@pytest.mark.parametrize("child_graph_id_match", [True, False])
async def test_load_graph_success(
    child_graph_id_match,
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
    sample_task_result_slim,
):
    """
    Test successful loading of a TaskGraph: check with and without child graph id."""
    if not child_graph_id_match:
        # Modify the graph_id in the task result to not match
        sample_task_result_slim.graph_id = GraphId("different-graph-id")
        if sample_task_result_slim.task_id in sample_task_graph.results:
            del sample_task_graph.results[sample_task_result_slim.task_id]
        if sample_task_result_slim.task_id in sample_task_graph.children:
            del sample_task_graph.children[sample_task_result_slim.task_id]
    else:
        sample_task_graph.add_task(sample_task)
        sample_task_result_slim.graph_id = sample_task.graph_id

    graph_json = sample_task_graph.model_dump_json()
    task_result_json = sample_task_result_slim.model_dump_json()
    container_client, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/test-task.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )
    result = await blob_storage.load_graph(sample_task_graph.graph_id)
    assert result is not None and isinstance(result, TaskGraph)
    assert result.graph_id == sample_task_graph.graph_id
    assert result.children == sample_task_graph.children
    assert result.edges == sample_task_graph.edges

    # Check that the loaded graph matches the original
    if not child_graph_id_match:
        # Task with wrong graph_id should not be added to results
        assert not result.results
    else:
        assert sample_task_result_slim.task_id in result.results


async def test_load_graph_not_found(blob_storage):
    """Test loading a non-existent TaskGraph."""
    with patch.object(blob_storage, "download_blob", new_callable=AsyncMock) as mock_download:
        error = AzureBlobError(
            MagicMock(
                **{
                    "message": "Not found",
                    "status_code": 404,
                    "reason": "Not found",
                }
            )
        )
        mock_download.side_effect = error

        with pytest.raises(BoilermakerStorageError) as exc_info:
            await blob_storage.load_graph(GraphId("non-existent-graph"))

        assert "Failed to load TaskGraph" in str(exc_info.value)
        assert exc_info.value.status_code == 404


async def test_load_graph_empty_graph_id(blob_storage):
    """Test that empty graph_id raises ValueError."""
    with pytest.raises(ValueError, match="`graph_id` must be provided"):
        await blob_storage.load_graph(GraphId(""))


async def test_load_graph_returns_none_when_no_content(blob_storage):
    """Test loading returns None when download_blob returns None."""
    with patch.object(blob_storage, "download_blob", new_callable=AsyncMock) as mock_download:
        mock_download.return_value = None

        result = await blob_storage.load_graph(GraphId("test-graph"))
        assert result is None


async def test_load_graph_validation_error_on_graph_json_raises_storage_error(
    blob_storage,
):
    """ValidationError from model_validate_json on the graph blob must be wrapped
    as BoilermakerStorageError so the continue_graph retry loop can catch it.

    Corrupt / schema-mismatched graph blobs raise pydantic.ValidationError inside
    load_graph.  Before the fix, that error escaped as a raw ValidationError bypassing
    the `except BoilermakerStorageError` in continue_graph's retry loop.  After the fix,
    load_graph wraps it so callers only need to handle BoilermakerStorageError.
    """
    from pydantic import ValidationError

    with (
        patch.object(blob_storage, "download_blob", new_callable=AsyncMock) as mock_download,
        patch(
            "boilermaker.storage.blob_storage.TaskGraph.model_validate_json",
            side_effect=ValidationError.from_exception_data("TaskGraph", [], input_type="json"),
        ),
    ):
        mock_download.return_value = '{"corrupt": "data"}'

        with pytest.raises(BoilermakerStorageError) as exc_info:
            await blob_storage.load_graph(GraphId("test-graph"))

        assert "Failed to deserialize graph" in str(exc_info.value)
        assert "test-graph" in str(exc_info.value)


async def test_load_graph_validation_error_on_task_result_json_raises_storage_error(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
):
    """ValidationError from model_validate_json on a TaskResultSlim blob must be
    wrapped as BoilermakerStorageError — same contract as for the graph blob itself.
    """
    from pydantic import ValidationError

    graph_json = sample_task_graph.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, '{"corrupt": "data"}'])
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/task-result.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )

    with patch(
        "boilermaker.storage.blob_storage.TaskResultSlim.model_validate_json",
        side_effect=ValidationError.from_exception_data("TaskResultSlim", [], input_type="json"),
    ):
        with pytest.raises(BoilermakerStorageError) as exc_info:
            await blob_storage.load_graph(sample_task_graph.graph_id)

    assert "Failed to deserialize task result" in str(exc_info.value)
    assert str(sample_task_graph.graph_id) in str(exc_info.value)


async def test_load_graph_skips_graph_blob_in_list_results(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
    sample_task_result_slim,
):
    """Regression: graph.json in list_blobs must not be parsed as TaskResultSlim."""
    sample_task_graph.add_task(sample_task)
    sample_task_result_slim.graph_id = sample_task.graph_id

    graph_json = sample_task_graph.model_dump_json()
    task_result_json = sample_task_result_slim.model_dump_json()
    graph_path = f"task-results/{sample_task_graph.storage_path}"
    task_result_path = f"task-results/{sample_task_result_slim.storage_path}"

    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])
    set_return.list_blobs_returns(
        [
            BlobProperties(name=graph_path, last_modified="2023-01-01T00:00:00Z"),
            BlobProperties(name=task_result_path, last_modified="2023-01-01T00:00:00Z"),
        ]
    )

    result = await blob_storage.load_graph(sample_task_graph.graph_id)

    assert result is not None
    assert result.graph_id == sample_task_graph.graph_id
    assert sample_task_result_slim.task_id in result.results
    assert result.results[sample_task_result_slim.task_id].task_id == sample_task_result_slim.task_id


async def test_load_graph_azure_blob_error_in_inner_download_raises_storage_error(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
):
    """AzureBlobError from get_blob_download_stream in the inner task-result loop must
    be wrapped as BoilermakerStorageError.

    The outer download_blob (graph.json) succeeds; the failure occurs only when
    downloading an individual task-result blob inside the `async for` loop.
    Patching download_blob separately ensures the outer graph.json fetch is not
    affected by the get_blob_download_stream replacement.
    """
    graph_json = sample_task_graph.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/task-result.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )

    azure_error = AzureBlobError(
        MagicMock(
            **{
                "message": "Service Unavailable",
                "status_code": 503,
                "reason": "Service Unavailable",
            }
        )
    )

    @asynccontextmanager
    async def failing_download_stream(_blob_name, **_kwargs):
        raise azure_error
        yield  # make this an async generator

    with (
        patch.object(blob_storage, "download_blob", new_callable=AsyncMock) as mock_dl,
        patch.object(blob_storage, "get_blob_download_stream", failing_download_stream),
    ):
        mock_dl.return_value = graph_json
        with pytest.raises(BoilermakerStorageError) as exc_info:
            await blob_storage.load_graph(sample_task_graph.graph_id)

    assert "Failed to load task result blob" in str(exc_info.value)
    assert exc_info.value.status_code == 503


async def test_load_graph_http_response_error_from_list_blobs_raises_storage_error(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
):
    """HttpResponseError raised by list_blobs (after graph.json downloads successfully)
    must be wrapped as BoilermakerStorageError.

    This exercises the outer except HttpResponseError guard added by BMO-40, which
    catches errors that escape from the container_client.list_blobs async iterator
    before any blob is yielded.
    """
    graph_json = sample_task_graph.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(graph_json)
    set_return.list_blobs_returns(
        HttpResponseError(message="The specified resource does not exist.", response=None)
    )

    with pytest.raises(BoilermakerStorageError) as exc_info:
        await blob_storage.load_graph(sample_task_graph.graph_id)

    assert "Failed to list blobs" in str(exc_info.value)


# Tests for store_graph method
async def test_store_graph_success(mock_azureblob, blob_storage, sample_task_graph):
    """Test successful storage of a TaskGraph."""
    container_client, mockblobc, _ = mock_azureblob
    mockblobc.upload_blob.return_value = {"status": "success"}

    result = await blob_storage.store_graph(sample_task_graph)
    assert result == sample_task_graph

    # Verify upload_blob was called with correct parameters
    # Expect 1) acquire lease, 2) upload graph, 3) upload task one result
    assert len(container_client.mock_calls) == 2
    upload_graph_call, task_result_upload_call = container_client.mock_calls
    assert upload_graph_call[0] == "upload_blob"
    assert task_result_upload_call[0] == "upload_blob"

    # Check filenames
    expected_graph_name = f"task-results/{sample_task_graph.storage_path}"
    pending_task_res = next(iter(sample_task_graph.results.values()))
    expected_task_result_name = f"task-results/{pending_task_res.storage_path}"

    assert expected_graph_name == upload_graph_call.args[0]
    assert expected_task_result_name == task_result_upload_call.args[0]

    # Check JSON content
    deserialized_graph = TaskGraph.model_validate_json(upload_graph_call.args[1])
    assert sample_task_graph.children == deserialized_graph.children
    assert sample_task_graph.edges == deserialized_graph.edges
    assert sample_task_graph.graph_id == deserialized_graph.graph_id
    assert pending_task_res.model_dump_json() == task_result_upload_call.args[1]


async def test_store_graph_with_resource_error(mock_azureblob, blob_storage, sample_task_graph):
    """Test error handling when storing TaskGraph fails."""
    container_client, mockblobc, _ = mock_azureblob
    error = ResourceExistsError("YOU FAILED")
    container_client.upload_blob.side_effect = error
    with pytest.raises(BoilermakerStorageError) as exc_info:
        await blob_storage.store_graph(sample_task_graph)

    assert "Failed to store TaskGraph" in str(exc_info.value)
    assert exc_info.value.graph_id == sample_task_graph.graph_id

    # Second one fails...
    container_client.upload_blob.side_effect = [None, error]
    with pytest.raises(BoilermakerStorageError) as exc_info:
        await blob_storage.store_graph(sample_task_graph)

    assert "Failed to store pending TaskResult" in str(exc_info.value)
    assert exc_info.value.graph_id == sample_task_graph.graph_id


# Edge cases and additional tests
async def test_store_task_result_raises_storage_error_on_etag_mismatch(blob_storage, sample_task_result):
    """
    Verifies that store_task_result raises BoilermakerStorageError when the
    underlying blob client raises AzureBlobError (e.g., HTTP 412 ETag mismatch).
    This is the primary guard against concurrent double-scheduling of downstream tasks.

    The concurrency path (etag != None) is exercised explicitly so that the
    concurrency_kwargs are populated before the upload attempt.
    """
    with patch.object(blob_storage, "upload_blob", new_callable=AsyncMock) as mock_upload:
        error = AzureBlobError(
            MagicMock(
                **{
                    "message": "The condition specified using HTTP conditional header(s) is not met.",
                    "status_code": 412,
                    "reason": "Precondition Failed",
                }
            )
        )
        mock_upload.side_effect = error

        with pytest.raises(BoilermakerStorageError) as exc_info:
            # Pass a non-empty etag to exercise the ETag concurrency guard code path
            await blob_storage.store_task_result(sample_task_result, etag='"0x8DBBAF4B8A6017C"')

        assert "Failed to store TaskResult" in str(exc_info.value)
        assert exc_info.value.task_id == sample_task_result.task_id
        assert exc_info.value.graph_id == sample_task_result.graph_id
        assert exc_info.value.status_code == 412
        assert exc_info.value.reason == "Precondition Failed"

        # Confirm etag and if_match were forwarded to upload_blob
        call_kwargs = mock_upload.call_args[1]
        assert "etag" in call_kwargs
        assert "match_condition" in call_kwargs


async def test_store_task_result_with_lease_id_forwards_lease_to_upload(blob_storage, sample_task_result):
    """When lease_id is provided, upload_blob must receive it as the ``lease`` kwarg.

    The Azure SDK uses the ``lease`` kwarg to enforce that only the lease holder
    can write. If it is not forwarded, the blob service ignores the held lease
    and any concurrent writer can overwrite the blob (the bug fixed by BMO-31).
    """
    with patch.object(blob_storage, "upload_blob", new_callable=AsyncMock) as mock_upload:
        await blob_storage.store_task_result(sample_task_result, lease_id="test-lease-id-abc")

        assert mock_upload.call_count == 1
        call_kwargs = mock_upload.call_args[1]
        assert call_kwargs.get("lease") == "test-lease-id-abc"


async def test_store_task_result_without_lease_id_omits_lease_kwarg(blob_storage, sample_task_result):
    """When lease_id is not provided, upload_blob must NOT receive a ``lease`` kwarg.

    Passing lease=None to the Azure SDK may cause unexpected behaviour on blobs
    that have no active lease. The kwarg must be omitted entirely.
    """
    with patch.object(blob_storage, "upload_blob", new_callable=AsyncMock) as mock_upload:
        await blob_storage.store_task_result(sample_task_result)

        assert mock_upload.call_count == 1
        call_kwargs = mock_upload.call_args[1]
        assert "lease" not in call_kwargs


async def test_store_task_result_without_graph_id(blob_storage, sample_task):
    """Test storing TaskResult without graph_id."""
    task_result = TaskResult(
        task_id=sample_task.task_id,
        graph_id=None,
        status=TaskStatus.Success,
        result="test result",
    )

    with patch.object(blob_storage, "upload_blob", new_callable=AsyncMock) as mock_upload:
        await blob_storage.store_task_result(task_result)

        # Verify tags include 'none' for missing graph_id
        call_args = mock_upload.call_args
        tags = call_args[1]["tags"]
        assert tags["graph_id"] == "none"


async def test_store_and_load_task_result_round_trip(mock_azureblob, blob_storage, sample_task_result):
    """Round-trip test: load_task_result reads back what store_task_result wrote.

    Both methods construct the blob path as
    ``{task_result_prefix}/{graph_id}/{task_id}.json``.  This test pins that
    contract so that a future refactor of either path-construction site
    produces a failing test rather than silently disabling the idempotency guard.

    Acceptance criterion from TDD section 7, load_task_result row.
    """
    _container_client, mockblobc, set_return = mock_azureblob
    mockblobc.upload_blob.return_value = {"status": "success"}

    # Write the result through store_task_result.
    await blob_storage.store_task_result(sample_task_result)

    # Capture the JSON that was actually written to blob storage.
    assert mockblobc.upload_blob.call_count == 1
    uploaded_json = mockblobc.upload_blob.call_args.args[0]

    # Configure the download mock to return that same JSON so load_task_result
    # reads exactly what store_task_result wrote.
    set_return.download_blob_returns(uploaded_json)

    loaded = await blob_storage.load_task_result(sample_task_result.task_id, sample_task_result.graph_id)

    assert loaded is not None, "load_task_result returned None — blob path mismatch between store and load"
    assert loaded.status == sample_task_result.status


# Tests for load_graph_slim_from_tags (inspect-only tag-based loading)


def _tagged_blob(name: str, graph_id: str, status: str, etag: str = "etag-abc") -> mock.MagicMock:
    """Create a BlobProperties-like mock with tags set."""
    blob = mock.MagicMock()
    blob.name = name
    blob.etag = etag
    blob.tags = {"graph_id": graph_id, "status": status, "created_date": "2026-01-01"}
    return blob


async def test_load_graph_slim_from_tags_builds_from_tags_without_download(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
):
    """load_graph_slim_from_tags must build TaskResultSlim from blob tags without
    downloading blob content when the 'status' tag is present."""
    sample_task_graph.add_task(sample_task)
    graph_json = sample_task_graph.model_dump_json()
    _, mockblobc, set_return = mock_azureblob
    set_return.download_blob_returns(graph_json)

    blob_name = f"task-results/{sample_task_graph.graph_id}/{sample_task.task_id}.json"
    set_return.list_blobs_returns(
        [_tagged_blob(blob_name, str(sample_task_graph.graph_id), "success", etag="etag-123")]
    )

    result = await blob_storage.load_graph_slim_from_tags(sample_task_graph.graph_id)

    assert result is not None
    assert sample_task.task_id in result.results
    tr = result.results[sample_task.task_id]
    assert isinstance(tr, TaskResultSlim)
    assert tr.status == TaskStatus.Success
    assert tr.etag == "etag-123"
    # No blob content download should have occurred — only graph.json
    assert mockblobc.download_blob.call_count == 1


async def test_load_graph_slim_from_tags_falls_back_to_download_for_untagged_blob(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
):
    """load_graph_slim_from_tags must fall back to content download for blobs that
    have no 'status' tag (written before tag support was added)."""
    sample_task_graph.add_task(sample_task)
    slim_result = TaskResultSlim(
        task_id=sample_task.task_id,
        graph_id=sample_task.graph_id,
        status=TaskStatus.Success,
    )
    graph_json = sample_task_graph.model_dump_json()
    task_result_json = slim_result.model_dump_json()
    _, mockblobc, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])

    blob = mock.MagicMock()
    blob.name = f"task-results/{sample_task_graph.graph_id}/{sample_task.task_id}.json"
    blob.etag = "etag-old"
    blob.tags = None
    set_return.list_blobs_returns([blob])

    result = await blob_storage.load_graph_slim_from_tags(sample_task_graph.graph_id)

    assert result is not None
    assert sample_task.task_id in result.results
    # download_blob called twice: graph.json + task result fallback
    assert mockblobc.download_blob.call_count == 2


async def test_load_graph_slim_from_tags_passes_include_tags_to_list_blobs(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
):
    """load_graph_slim_from_tags must pass include=['tags'] to list_blobs."""
    graph_json = sample_task_graph.model_dump_json()
    container_client, _, set_return = mock_azureblob
    set_return.download_blob_returns(graph_json)
    set_return.list_blobs_returns([])

    await blob_storage.load_graph_slim_from_tags(sample_task_graph.graph_id)

    container_client.list_blobs.assert_called_once()
    assert container_client.list_blobs.call_args.kwargs.get("include") == ["tags"]


async def test_load_graph_unchanged_does_not_use_tags(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
):
    """load_graph must NOT pass include=['tags'] — evaluator path is unchanged."""
    sample_task_graph.add_task(sample_task)
    slim_result = TaskResultSlim(
        task_id=sample_task.task_id,
        graph_id=sample_task.graph_id,
        status=TaskStatus.Success,
    )
    graph_json = sample_task_graph.model_dump_json()
    task_result_json = slim_result.model_dump_json()
    container_client, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/{sample_task.task_id}.json",
                last_modified="2023-01-01T00:00:00Z",
            )
        ]
    )

    await blob_storage.load_graph(sample_task_graph.graph_id)

    call_kwargs = container_client.list_blobs.call_args.kwargs
    assert "include" not in call_kwargs


# Tests for load_graph full=True parameter


async def test_load_graph_full_true_returns_task_result_instances(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
):
    """load_graph(full=True) must deserialize task result blobs as TaskResult
    (not TaskResultSlim), so that rich fields are available to callers.
    """
    sample_task_graph.add_task(sample_task)
    full_result = TaskResult(
        task_id=sample_task.task_id,
        graph_id=sample_task.graph_id,
        status=TaskStatus.Success,
        result={"key": "value"},
        errors=None,
        formatted_exception=None,
    )

    graph_json = sample_task_graph.model_dump_json()
    task_result_json = full_result.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/test-task.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )

    result = await blob_storage.load_graph(sample_task_graph.graph_id, full=True)

    assert result is not None
    assert sample_task.task_id in result.results
    task_result = result.results[sample_task.task_id]
    assert isinstance(task_result, TaskResult)
    assert task_result.result == {"key": "value"}


async def test_load_graph_full_true_includes_rich_fields(
    mock_azureblob,
    blob_storage,
    sample_task,
):
    """load_graph(full=True) must round-trip all rich TaskResult fields:
    result, errors, and formatted_exception for both successful and failed tasks.
    """
    # Build a graph with two tasks: one successful, one failed.
    success_task = sample_task
    failed_task = Task.default("failing_function", args=[], kwargs={})

    graph = TaskGraph()
    graph.add_task(success_task)
    graph.add_task(failed_task)

    success_result = TaskResult(
        task_id=success_task.task_id,
        graph_id=success_task.graph_id,
        status=TaskStatus.Success,
        result={"key": "value"},
        errors=None,
        formatted_exception=None,
    )
    failed_result = TaskResult(
        task_id=failed_task.task_id,
        graph_id=failed_task.graph_id,
        status=TaskStatus.Failure,
        result=None,
        errors=["something went wrong"],
        formatted_exception="Traceback (most recent call last):\n  File ...\nValueError: bad",
    )

    graph_json = graph.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(
        None,
        side_effect=[graph_json, success_result.model_dump_json(), failed_result.model_dump_json()],
    )
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{graph.graph_id}/{success_task.task_id}.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
            BlobProperties(
                name=f"task-results/{graph.graph_id}/{failed_task.task_id}.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )

    loaded = await blob_storage.load_graph(graph.graph_id, full=True)
    assert loaded is not None

    # Verify successful task round-trips correctly
    sr = loaded.results[success_task.task_id]
    assert isinstance(sr, TaskResult)
    assert sr.result == {"key": "value"}
    assert sr.errors is None
    assert sr.formatted_exception is None

    # Verify failed task round-trips correctly
    fr = loaded.results[failed_task.task_id]
    assert isinstance(fr, TaskResult)
    assert fr.result is None
    assert fr.errors == ["something went wrong"]
    assert fr.formatted_exception == "Traceback (most recent call last):\n  File ...\nValueError: bad"


async def test_load_graph_default_still_returns_task_result_slim(
    mock_azureblob,
    blob_storage,
    sample_task_graph,
    sample_task,
):
    """load_graph() without the full parameter (default False) must continue to
    return TaskResultSlim instances, confirming backward compatibility.
    The rich fields (result, errors, formatted_exception) must NOT be populated.
    """
    sample_task_graph.add_task(sample_task)
    # Provide a full TaskResult JSON blob -- the default path should still
    # parse it as TaskResultSlim, discarding the extra fields.
    full_result = TaskResult(
        task_id=sample_task.task_id,
        graph_id=sample_task.graph_id,
        status=TaskStatus.Success,
        result={"key": "value"},
        errors=["some error"],
        formatted_exception="Traceback ...",
    )

    graph_json = sample_task_graph.model_dump_json()
    task_result_json = full_result.model_dump_json()
    _, _, set_return = mock_azureblob
    set_return.download_blob_returns(None, side_effect=[graph_json, task_result_json])
    set_return.list_blobs_returns(
        [
            BlobProperties(
                name=f"task-results/{sample_task_graph.graph_id}/test-task.json",
                last_modified="2023-01-01T00:00:00Z",
            ),
        ]
    )

    result = await blob_storage.load_graph(sample_task_graph.graph_id)

    assert result is not None
    assert sample_task.task_id in result.results
    task_result = result.results[sample_task.task_id]
    # Must be TaskResultSlim, not TaskResult
    assert type(task_result) is TaskResultSlim
    assert not hasattr(task_result, "result") or task_result.__class__ is TaskResultSlim
    # TaskResultSlim does not have result/errors/formatted_exception fields
    assert not isinstance(task_result, TaskResult)


# Tests for created_date tag presence


async def test_store_task_result_includes_created_date_tag(mock_azureblob, blob_storage, sample_task_result):
    """store_task_result must include a created_date tag in YYYY-MM-DD format
    matching today's UTC date on every blob write."""
    _container_client, mockblobc, _ = mock_azureblob
    mockblobc.upload_blob.return_value = {"status": "success"}

    await blob_storage.store_task_result(sample_task_result)

    assert mockblobc.upload_blob.call_count == 1
    tags = mockblobc.upload_blob.call_args.kwargs["tags"]

    assert "created_date" in tags
    # Must be YYYY-MM-DD format
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", tags["created_date"])
    # Must match today's UTC date
    assert tags["created_date"] == datetime.now(UTC).strftime("%Y-%m-%d")


async def test_store_graph_graph_json_includes_created_date_tag(mock_azureblob, blob_storage, sample_task_graph):
    """store_graph must tag the graph.json upload with both graph_id and created_date."""
    container_client, _mockblobc, _ = mock_azureblob

    await blob_storage.store_graph(sample_task_graph)

    # First call is the graph.json upload
    upload_graph_call = container_client.mock_calls[0]
    assert upload_graph_call[0] == "upload_blob"

    tags = upload_graph_call.kwargs["tags"]
    assert "graph_id" in tags
    assert tags["graph_id"] == str(sample_task_graph.graph_id)
    assert "created_date" in tags
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", tags["created_date"])
    assert tags["created_date"] == datetime.now(UTC).strftime("%Y-%m-%d")


async def test_store_graph_pending_results_include_created_date_tag(
    mock_azureblob, blob_storage, sample_task_graph
):
    """Pending result uploads in store_graph must include graph_id, status, and
    created_date tags."""
    container_client, _mockblobc, _ = mock_azureblob

    await blob_storage.store_graph(sample_task_graph)

    # The graph has one child, so we expect: graph.json upload + 1 pending result upload
    assert len(container_client.mock_calls) == 2
    pending_result_call = container_client.mock_calls[1]
    assert pending_result_call[0] == "upload_blob"

    tags = pending_result_call.kwargs["tags"]
    assert "graph_id" in tags
    assert "status" in tags
    assert "created_date" in tags
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", tags["created_date"])
    assert tags["created_date"] == datetime.now(UTC).strftime("%Y-%m-%d")


async def test_store_graph_created_date_consistent_across_all_uploads(
    mock_azureblob, blob_storage, sample_task_graph
):
    """All created_date values within a single store_graph call must be identical,
    because the date is computed once at the top of the method."""
    container_client, _mockblobc, _ = mock_azureblob

    await blob_storage.store_graph(sample_task_graph)

    # Collect created_date from every upload_blob call on the container_client
    created_dates = []
    for call in container_client.mock_calls:
        if call[0] == "upload_blob" and "tags" in call.kwargs:
            created_dates.append(call.kwargs["tags"]["created_date"])

    # At least the graph.json + 1 pending result = 2 uploads
    assert len(created_dates) >= 2
    # All dates must be the same value
    assert len(set(created_dates)) == 1, (
        f"Expected all created_date tags to be identical, got: {created_dates}"
    )


# ---------------------------------------------------------------------------
# Tests for delete_blobs_batch method
# ---------------------------------------------------------------------------


class _DeleteResult:
    """Minimal response object matching what the Azure SDK returns from delete_blobs."""
    def __init__(self, status_code: int):
        self.status_code = status_code


class _AsyncDeleteResults:
    """Helper that wraps a list of _DeleteResult objects as an async iterator,
    returned by an awaitable (to match container_client.delete_blobs() semantics)."""

    def __init__(self, results: list):
        self._results = results

    def __await__(self):
        async def _noop():
            return self
        return _noop().__await__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._results:
            raise StopAsyncIteration
        return self._results.pop(0)


class TestDeleteBlobsBatch:
    async def test_successful_batch_delete(self, mock_azureblob, blob_storage):
        """Pass a few blob names, mock delete_blobs to return 202 success responses.
        Verify returned failure list is empty."""
        container_client, _, _ = mock_azureblob
        blob_names = ["blob-a", "blob-b", "blob-c"]
        container_client.delete_blobs.return_value = _AsyncDeleteResults(
            [_DeleteResult(202)] * 3
        )

        failures = await blob_storage.delete_blobs_batch(blob_names)

        assert failures == []
        container_client.delete_blobs.assert_called_once_with(*blob_names)

    async def test_batch_chunking_at_256(self, mock_azureblob, blob_storage):
        """Pass 300 blob names. Verify delete_blobs is called twice:
        first with 256, second with 44."""
        container_client, _, _ = mock_azureblob
        blob_names = [f"blob-{i}" for i in range(300)]

        container_client.delete_blobs.side_effect = [
            _AsyncDeleteResults([_DeleteResult(202)] * 256),
            _AsyncDeleteResults([_DeleteResult(202)] * 44),
        ]

        failures = await blob_storage.delete_blobs_batch(blob_names)

        assert failures == []
        assert container_client.delete_blobs.call_count == 2

        first_call_args = container_client.delete_blobs.call_args_list[0].args
        second_call_args = container_client.delete_blobs.call_args_list[1].args
        assert len(first_call_args) == 256
        assert len(second_call_args) == 44

    async def test_404_treated_as_success(self, mock_azureblob, blob_storage):
        """404 response (blob already gone) must NOT appear in the failure list."""
        container_client, _, _ = mock_azureblob
        blob_names = ["blob-gone"]
        container_client.delete_blobs.return_value = _AsyncDeleteResults(
            [_DeleteResult(404)]
        )

        failures = await blob_storage.delete_blobs_batch(blob_names)

        assert failures == []

    async def test_non_404_failure_returned(self, mock_azureblob, blob_storage):
        """A non-202/404 status code means failure; blob name must appear in failure list."""
        container_client, _, _ = mock_azureblob
        blob_names = ["blob-broken"]
        container_client.delete_blobs.return_value = _AsyncDeleteResults(
            [_DeleteResult(500)]
        )

        failures = await blob_storage.delete_blobs_batch(blob_names)

        assert failures == ["blob-broken"]

    async def test_empty_input(self, mock_azureblob, blob_storage):
        """Pass empty list. Verify no calls and empty result."""
        container_client, _, _ = mock_azureblob

        failures = await blob_storage.delete_blobs_batch([])

        assert failures == []
        container_client.delete_blobs.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for find_blobs_by_tags method
# ---------------------------------------------------------------------------


class _AsyncTagResults:
    """Helper that wraps a list of BlobProperties as an async iterator,
    matching the signature of container_client.find_blobs_by_tags()."""

    def __init__(self, blobs: list):
        self._blobs = list(blobs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._blobs:
            raise StopAsyncIteration
        return self._blobs.pop(0)


class TestFindBlobsByTags:
    async def test_yields_matching_blobs(self, mock_azureblob, blob_storage):
        """find_blobs_by_tags yields blobs returned by the container client."""
        container_client, _, _ = mock_azureblob
        blob_a = BlobProperties(name="task-results/graph-1/task.json")
        blob_b = BlobProperties(name="task-results/graph-2/task.json")
        container_client.find_blobs_by_tags.return_value = _AsyncTagResults(
            [blob_a, blob_b]
        )

        filter_expr = "\"created_date\" < '2026-04-10'"
        results = []
        async for blob in blob_storage.find_blobs_by_tags(filter_expr):
            results.append(blob)

        assert len(results) == 2
        assert results[0].name == "task-results/graph-1/task.json"
        assert results[1].name == "task-results/graph-2/task.json"

    async def test_filter_expression_passed_through(self, mock_azureblob, blob_storage):
        """Verify the OData filter expression reaches the Azure SDK call."""
        container_client, _, _ = mock_azureblob
        container_client.find_blobs_by_tags.return_value = _AsyncTagResults([])

        filter_expr = "\"created_date\" < '2026-04-10'"
        async for _ in blob_storage.find_blobs_by_tags(filter_expr):
            pass

        container_client.find_blobs_by_tags.assert_called_once_with(
            filter_expression=filter_expr
        )

    async def test_empty_result_set(self, mock_azureblob, blob_storage):
        """Empty result from tag query completes without error."""
        container_client, _, _ = mock_azureblob
        container_client.find_blobs_by_tags.return_value = _AsyncTagResults([])

        results = []
        async for blob in blob_storage.find_blobs_by_tags("\"created_date\" < '2026-01-01'"):
            results.append(blob)

        assert results == []
