import datetime
import logging

from aio_azure_clients_toolbox import AzureBlobStorageClient as MFBlobClient
from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from anyio import create_task_group
from azure.identity.aio import DefaultAzureCredential

from boilermaker.exc import BoilermakerStorageError
from boilermaker.storage import StorageInterface
from boilermaker.task import GraphId, TaskGraph, TaskResult, TaskResultSlim

logger = logging.getLogger(__name__)


class BlobClientStorage(MFBlobClient, StorageInterface):
    """Client for uploading TaskResult instances to Azure Blob Storage.
    This client extends the AzureBlobStorageClient to provide functionality
    specifically for handling TaskResult objects.
    """

    task_result_prefix = "task-results/"

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
            ValidationError: If TaskGraph or TaskResultSlim data cannot be validated.
        """
        if not graph_id:
            raise ValueError("`graph_id` must be provided to load a TaskGraph.")

        graph_path = f"{self.task_result_prefix}/{TaskGraph.graph_path(graph_id)}"
        graph_dir = f"{self.task_result_prefix}/{graph_id}"
        graph_contents = await self.download_blob(graph_path)
        if graph_contents is None:
            return None

        graph = TaskGraph.model_validate_json(graph_contents)

        # Load all TaskResultSlim instances associated with this graph
        # We don't want to load *all* return values into memory. Just the statuses.
        async for blob in self.list_blobs(prefix=graph_dir):
            tr = TaskResultSlim.model_validate_json(await self.download_blob(blob.name))
            if tr.graph_id == graph_id:
                graph.results[tr.task_id] = tr
            else:
                logger.warning(
                    f"TaskResult {tr.task_id} in graph {graph_dir} with wrong graph_id {tr.graph_id}!"
                )
        return graph

    async def store_graph(self, graph: TaskGraph) -> str:
        """
        Stores a TaskGraph to Azure Blob Storage.

        Stores all children as pending tasks as well.

        Args:
            graph: The TaskGraph instance to store.
        """
        fname = f"{self.task_result_prefix}/{graph.storage_path()}"
        graph_stored, _res = await self.upload_blob(fname, graph.model_dump_json())
        if not graph_stored:
            raise BoilermakerStorageError(
                f"Failed to store TaskGraph {graph.graph_id}",
                task_id=None,
                graph_id=graph.graph_id,
                status_code=500,
                reason="Unknown",
            )
        # Store pending results for *all* tasks in the graph
        try:
            async with create_task_group() as tg:
                for pending_result in graph.generate_pending_results():
                    tg.start_soon(
                        self.upload_blob,
                        pending_result.storage_path(),
                        pending_result.model_dump_json(),
                        overwrite=True,
                    )
                if not pending_result:
                    raise BoilermakerStorageError(
                        f"Failed to store pending TaskResult {pending_result.task_id}",
                        task_id=pending_result.task_id,
                        graph_id=pending_result.graph_id,
                        status_code=500,
                        reason="Unknown",
                    )
        except* Exception as excgroup:
            for exc in excgroup.exceptions:
                logger.error(f"Error occurred while storing pending TaskResults: {exc}")
            raise BoilermakerStorageError(
                f"Failed to store pending TaskResults for graph {graph.graph_id}",
                task_id=None,
                graph_id=graph.graph_id,
                status_code=500,
                reason="Unknown",
            ) from excgroup
        return fname

    async def store_task_result(self, task_result: TaskResult) -> None:
        """Stores a TaskResult to Azure Blob Storage.

        Args:
            task_result: The TaskResult instance to store.
        """
        fname = str(task_result.storage_path())
        if self.task_result_prefix:
            fname = f"{self.task_result_prefix}{fname}"

        # add tags:
        blob_tags = {
            "graph_id": task_result.graph_id or "none",
            "status": task_result.status,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        }
        try:
            return await self.upload_blob(
                fname, task_result.model_dump_json(), tags=blob_tags, overwrite=True
            )
        except AzureBlobError as exc:
            raise BoilermakerStorageError(
                f"Failed to store TaskResult {task_result.task_id}",
                task_id=task_result.task_id,
                graph_id=task_result.graph_id,
                status_code=exc.status_code,
                reason=exc.reason,
            ) from exc
