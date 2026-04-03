from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from azure.core.exceptions import ResourceExistsError
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
async def test_store_task_result_success(
    mock_azureblob, blob_storage, sample_task_result
):
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


async def test_store_task_result_with_azure_blob_error(
    blob_storage, sample_task_result
):
    """Test error handling when Azure blob storage fails."""
    with patch.object(
        blob_storage, "upload_blob", new_callable=AsyncMock
    ) as mock_upload:
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
    with patch.object(
        blob_storage, "download_blob", new_callable=AsyncMock
    ) as mock_download:
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
    with patch.object(
        blob_storage, "download_blob", new_callable=AsyncMock
    ) as mock_download:
        mock_download.return_value = None

        result = await blob_storage.load_graph(GraphId("test-graph"))
        assert result is None


async def test_load_graph_validation_error_on_graph_json_raises_storage_error(
    blob_storage,
):
    """BMO-10: ValidationError from model_validate_json on the graph blob must be wrapped
    as BoilermakerStorageError so the continue_graph retry loop can catch it.

    Corrupt / schema-mismatched graph blobs raise pydantic.ValidationError inside
    load_graph.  Before the fix, that error escaped as a raw ValidationError bypassing
    the `except BoilermakerStorageError` in continue_graph's retry loop.  After the fix,
    load_graph wraps it so callers only need to handle BoilermakerStorageError.
    """
    from pydantic import ValidationError

    with (
        patch.object(
            blob_storage, "download_blob", new_callable=AsyncMock
        ) as mock_download,
        patch(
            "boilermaker.storage.blob_storage.TaskGraph.model_validate_json",
            side_effect=ValidationError.from_exception_data(
                "TaskGraph", [], input_type="json"
            ),
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
    """BMO-10: ValidationError from model_validate_json on a TaskResultSlim blob must be
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
        side_effect=ValidationError.from_exception_data(
            "TaskResultSlim", [], input_type="json"
        ),
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


async def test_store_graph_with_resource_error(
    mock_azureblob, blob_storage, sample_task_graph
):
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
async def test_store_task_result_raises_storage_error_on_etag_mismatch(
    blob_storage, sample_task_result
):
    """
    Verifies that store_task_result raises BoilermakerStorageError when the
    underlying blob client raises AzureBlobError (e.g., HTTP 412 ETag mismatch).
    This is the primary guard against concurrent double-scheduling of downstream tasks.

    The concurrency path (etag != None) is exercised explicitly so that the
    concurrency_kwargs are populated before the upload attempt.
    """
    with patch.object(
        blob_storage, "upload_blob", new_callable=AsyncMock
    ) as mock_upload:
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
            await blob_storage.store_task_result(
                sample_task_result, etag='"0x8DBBAF4B8A6017C"'
            )

        assert "Failed to store TaskResult" in str(exc_info.value)
        assert exc_info.value.task_id == sample_task_result.task_id
        assert exc_info.value.graph_id == sample_task_result.graph_id
        assert exc_info.value.status_code == 412
        assert exc_info.value.reason == "Precondition Failed"

        # Confirm etag and if_match were forwarded to upload_blob
        call_kwargs = mock_upload.call_args[1]
        assert "etag" in call_kwargs
        assert "if_match" in call_kwargs


async def test_store_task_result_without_graph_id(blob_storage, sample_task):
    """Test storing TaskResult without graph_id."""
    task_result = TaskResult(
        task_id=sample_task.task_id,
        graph_id=None,
        status=TaskStatus.Success,
        result="test result",
    )

    with patch.object(
        blob_storage, "upload_blob", new_callable=AsyncMock
    ) as mock_upload:
        await blob_storage.store_task_result(task_result)

        # Verify tags include 'none' for missing graph_id
        call_args = mock_upload.call_args
        tags = call_args[1]["tags"]
        assert tags["graph_id"] == "none"
