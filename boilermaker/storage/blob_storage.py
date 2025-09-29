import datetime
import logging

from aio_azure_clients_toolbox import AzureBlobStorageClient as MFBlobClient
from azure.identity.aio import DefaultAzureCredential

from boilermaker.storage.base import StorageInterface
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

    async def store_graph(self, graph: TaskGraph) -> None:
        """Stores a TaskGraph to Azure Blob Storage.

        Args:
            graph: The TaskGraph instance to store.
        """
        fname = f"{self.task_result_prefix}/{graph.storage_path()}"
        return await self.upload_blob(fname, graph.model_dump_json())

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
        return await self.upload_blob(fname, task_result.model_dump_json(), tags=blob_tags)
