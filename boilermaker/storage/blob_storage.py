import logging
import traceback
from functools import partial

from aio_azure_clients_toolbox import AzureBlobStorageClient
from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from anyio import create_task_group
from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.identity.aio import DefaultAzureCredential
from pydantic import ValidationError

from boilermaker.exc import BoilermakerStorageError
from boilermaker.storage import StorageInterface
from boilermaker.task import GraphId, TaskGraph, TaskResult, TaskResultSlim

logger = logging.getLogger(__name__)


class BlobClientStorage(AzureBlobStorageClient, StorageInterface):
    """Client for uploading TaskResult instances to Azure Blob Storage.
    This client extends the AzureBlobStorageClient to provide functionality
    specifically for handling TaskResult objects.
    """

    task_result_prefix = "task-results"

    def __init__(
        self,
        az_storage_url: str,
        container_name: str,
        credentials: DefaultAzureCredential,
    ):
        super().__init__(
            az_storage_url=az_storage_url,
            container_name=container_name,
            credentials=credentials,
        )

    async def load_graph(self, graph_id: GraphId) -> TaskGraph | None:
        """Loads a TaskGraph from Azure Blob Storage.

        Args:
            graph_id: The GraphId to filter TaskResult instances by.
        Returns:
            The loaded TaskGraph instance, or None if not found.
        Raises:
            BoilermakerStorageError: If the blob cannot be loaded or if TaskGraph/TaskResultSlim
                data cannot be validated.
        """
        if not graph_id:
            raise ValueError("`graph_id` must be provided to load a TaskGraph.")

        graph_path = f"{self.task_result_prefix}/{TaskGraph.graph_path(graph_id)}"
        graph_dir = f"{self.task_result_prefix}/{graph_id}"
        try:
            graph_contents = await self.download_blob(graph_path)
        except AzureBlobError as exc:
            raise BoilermakerStorageError(
                f"Failed to load TaskGraph {graph_id}",
                task_id=None,
                graph_id=graph_id,
                status_code=exc.status_code,
                reason=exc.reason,
            ) from exc
        if graph_contents is None:
            return None

        try:
            graph = TaskGraph.model_validate_json(graph_contents)
        except ValidationError as e:
            raise BoilermakerStorageError(
                f"Failed to deserialize graph {graph_id}: {e}",
                status_code=None,
            ) from e

        # Load all TaskResultSlim instances associated with this graph
        # We don't want to load *all* return values into memory. Just the statuses.
        async for blob in self.list_blobs(prefix=graph_dir):
            # DO NOT REDOWNLOAD GRAPH
            if blob.name == graph_path:
                continue
            try:
                tr = TaskResultSlim.model_validate_json(await self.download_blob(blob.name))
            except ValidationError as e:
                raise BoilermakerStorageError(
                    f"Failed to deserialize task result in graph {graph_id}: {e}",
                    status_code=None,
                ) from e
            tr.etag = blob.etag
            if tr.graph_id == graph_id:
                graph.results[tr.task_id] = tr
            else:
                logger.warning(f"TaskResult {tr.task_id} in graph {graph_dir} with wrong graph_id {tr.graph_id}!")
        return graph

    async def store_graph(self, graph: TaskGraph) -> TaskGraph:
        """
        Stores a TaskGraph to Azure Blob Storage and stores all children as pending tasks as well.

        We expect the *written graph* to be **immutable** (see the ImmutabilityPolicy below).

        Args:
            graph: The TaskGraph instance to store.
        """
        lease = None
        async with self.get_blob_service_client() as blob_service_client:
            container_client = blob_service_client.get_container_client(self.container_name)
            # Store the graph itself first
            fname = f"{self.task_result_prefix}/{graph.storage_path}"
            try:
                _result = await container_client.upload_blob(
                    fname,
                    graph.model_dump_json(),
                    blob_type="BlockBlob",
                )
            except (
                ResourceNotFoundError,
                HttpResponseError,
                ResourceExistsError,
            ) as exc:
                logger.error(f"Error occurred while storing TaskGraph {graph.graph_id}: {exc}")
                raise BoilermakerStorageError(
                    f"Failed to store TaskGraph {graph.graph_id}",
                    task_id=None,
                    graph_id=graph.graph_id,
                    status_code=500,
                    reason="Unknown",
                ) from exc
            # Store pending results for *all* tasks in the graph
            pending_result = None
            try:
                async with create_task_group() as tg:
                    # don't let any tasks that get ahead accidentally clobber us
                    for pending_result in graph.generate_pending_results():
                        fname_pr = f"{self.task_result_prefix}/{pending_result.storage_path}"

                        uploader = partial(
                            container_client.upload_blob,
                            fname_pr,
                            pending_result.model_dump_json(),
                            blob_type="BlockBlob",
                        )
                        tg.start_soon(uploader)
            except* Exception as excgroup:
                formatted_traceback = traceback.format_exception_only(excgroup)
                logger.error(f"Error occurred while storing pending TaskResults:\n {formatted_traceback}")
                raise BoilermakerStorageError(
                    f"Failed to store pending TaskResults for graph {graph.graph_id}",
                    task_id=None,
                    graph_id=graph.graph_id,
                    status_code=500,
                    reason="Unknown",
                ) from excgroup
            finally:
                if lease is not None:
                    await lease.release()
        return graph

    async def store_task_result(self, task_result: TaskResult | TaskResultSlim, etag: str | None = None) -> None:
        """Stores a TaskResult to Azure Blob Storage.

        Args:
            task_result: The TaskResult instance to store.
        """
        fname = str(task_result.storage_path)
        if self.task_result_prefix:
            fname = f"{self.task_result_prefix}/{fname}"

        # add tags:
        blob_tags = {
            "graph_id": task_result.graph_id or "none",
            "status": task_result.status,
        }
        concurrency_kwargs: dict[str, str | int] = {}
        if etag:
            concurrency_kwargs["etag"] = etag
            concurrency_kwargs["if_match"] = MatchConditions.IfNotModified.value

        try:
            await self.upload_blob(
                fname, task_result.model_dump_json(), tags=blob_tags, overwrite=True, **concurrency_kwargs
            )
        # SAFETY: This catch assumes aio_azure_clients_toolbox raises AzureBlobError
        # (wrapping HTTP 412 Precondition Failed) when an ETag mismatch occurs.
        # This is the primary guard against concurrent double-scheduling of downstream
        # tasks. Verified against aio-azure-clients-toolbox v1.0.4 (see uv.lock):
        # get_blob_client() catches all HttpResponseError (including 412) and re-raises
        # as AzureBlobError. If the library behavior changes, this guard will silently break.
        except AzureBlobError as exc:
            raise BoilermakerStorageError(
                f"Failed to store TaskResult {task_result.task_id}",
                task_id=task_result.task_id,
                graph_id=task_result.graph_id,
                status_code=exc.status_code,
                reason=exc.reason,
            ) from exc
