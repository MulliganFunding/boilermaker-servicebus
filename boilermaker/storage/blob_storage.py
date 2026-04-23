import logging
import traceback
from collections.abc import AsyncGenerator
from datetime import datetime, UTC
from functools import partial

from aio_azure_clients_toolbox import AzureBlobStorageClient
from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError
from anyio import CapacityLimiter, create_task_group
from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import BlobProperties
from azure.storage.blob.aio import BlobLeaseClient
from opentelemetry import trace
from pydantic import ValidationError

from boilermaker.exc import BoilermakerStorageError
from boilermaker.storage import StorageInterface
from boilermaker.task import GraphId, TaskGraph, TaskId, TaskResult, TaskResultSlim, TaskStatus

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

BATCH_DELETE_MAX = 256


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

    async def _download_result_with_limiter(
        self,
        graph_id: GraphId,
        blob_name: str,
        limiter: CapacityLimiter,
        model: type[TaskResultSlim] | type[TaskResult] = TaskResultSlim,
    ) -> TaskResultSlim | TaskResult:
        """Download a single task-result blob, parse it, and return it.

        Raises BoilermakerStorageError on network or deserialization failure.
        Does NOT check graph_id — callers are responsible for filtering.
        """
        async with limiter:
            try:
                async with self.get_blob_download_stream(blob_name) as stream:
                    blob_etag = stream.properties.etag
                    contents = await stream.readall()
                tr = model.model_validate_json(contents)
                tr.etag = blob_etag
                return tr
            except AzureBlobError as exc:
                raise BoilermakerStorageError(
                    f"Failed to load task result blob {blob_name} in graph {graph_id}",
                    name=blob_name,
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=exc.reason,
                ) from exc
            except ValidationError as e:
                raise BoilermakerStorageError(
                    f"Failed to deserialize task result in graph {graph_id}: {e}",
                    name=blob_name,
                    graph_id=graph_id,
                    status_code=None,
                    reason="DeserializationError",
                ) from e

    async def load_graph(self, graph_id: GraphId, full: bool = False) -> TaskGraph | None:
        """Loads a TaskGraph from Azure Blob Storage.

        Args:
            graph_id: The GraphId to filter TaskResult instances by.
            full: When True, task result blobs are deserialized as TaskResult
                (with result, errors, formatted_exception fields) instead of
                TaskResultSlim. Defaults to False for memory-efficient
                status-only loading.
        Returns:
            The loaded TaskGraph instance, or None if not found.
        Raises:
            BoilermakerStorageError: If the blob cannot be loaded or if TaskGraph/TaskResultSlim
                data cannot be validated.
        """
        if not graph_id:
            raise ValueError("`graph_id` must be provided to load a TaskGraph.")
        with tracer.start_as_current_span("load_graph"):
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

            # Collect blob names first (sequential), then download concurrently.
            # This keeps list_blobs errors outside the task group so they are
            # caught by the existing AzureBlobError/HttpResponseError handlers.
            try:
                blob_names: list[str] = []
                async for blob in self.list_blobs(prefix=graph_dir):
                    if blob.name != graph_path:
                        blob_names.append(blob.name)
            except AzureBlobError as exc:
                raise BoilermakerStorageError(
                    f"Failed to list blobs for graph {graph_id}",
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=exc.reason,
                ) from exc
            except HttpResponseError as exc:
                raise BoilermakerStorageError(
                    f"Failed to list blobs for graph {graph_id}",
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=str(exc),
                ) from exc

            # Download result blobs concurrently (up to 10 at a time).
            model = TaskResult if full else TaskResultSlim
            limiter = CapacityLimiter(10)

            async def _download_and_store(blob_name: str) -> None:
                tr = await self._download_result_with_limiter(graph_id, blob_name, limiter, model=model)
                if tr.graph_id == graph_id:
                    graph.results[tr.task_id] = tr
                else:
                    logger.warning(f"TaskResult {tr.task_id} in graph {graph_dir} with wrong graph_id {tr.graph_id}!")

            try:
                async with create_task_group() as tg:
                    for blob_name in blob_names:
                        tg.start_soon(_download_and_store, blob_name)
            except* BoilermakerStorageError as exc_group:
                # Unwrap the ExceptionGroup — raise the first storage error
                # directly to preserve the existing API contract.
                raise exc_group.exceptions[0] from exc_group.exceptions[0].__cause__

            return graph

    async def try_acquire_lease(
        self,
        task_id: TaskId,
        graph_id: GraphId,
        etag: str | None = None,
        lease_duration: int = 15,
    ) -> str | None:
        """Attempt to acquire a lease on a task result blob.

        Uses Azure Blob Storage's built-in lease mechanism. The lease is
        per-blob and exclusive — only one caller can hold it at a time.
        If another worker already holds the lease, this returns None
        immediately (no blocking, no retry).

        When ``etag`` is provided, the lease acquire uses an If-Match
        conditional header. This atomically checks "blob unchanged since
        I read it" AND "no other worker holds the lease" in one call.
        """
        fname = f"{self.task_result_prefix}/{graph_id}/{task_id}.json"
        with tracer.start_as_current_span("try_acquire_lease"):
            try:
                async with self.get_blob_client(fname) as blob_client:
                    lease_client = BlobLeaseClient(blob_client)
                    acquire_kwargs: dict = {}
                    if etag:
                        acquire_kwargs["etag"] = etag
                        acquire_kwargs["match_condition"] = MatchConditions.IfNotModified
                    await lease_client.acquire(lease_duration=lease_duration, **acquire_kwargs)
                    return lease_client.id
            except AzureBlobError as exc:
                # 409 Conflict = blob is already leased by another worker
                # 412 Precondition Failed = ETag mismatch (blob modified since load_graph)
                if exc.status_code in (409, 412):
                    return None
                raise BoilermakerStorageError(
                    f"Failed to acquire lease on task {task_id} in graph {graph_id}",
                    task_id=task_id,
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=exc.reason,
                ) from exc

    async def release_lease(self, task_id: TaskId, graph_id: GraphId, lease_id: str) -> None:
        """Release a previously acquired lease. Best-effort — logs warning on failure."""
        fname = f"{self.task_result_prefix}/{graph_id}/{task_id}.json"
        with tracer.start_as_current_span("release_lease"):
            try:
                async with self.get_blob_client(fname) as blob_client:
                    lease_client = BlobLeaseClient(blob_client, lease_id=lease_id)
                    await lease_client.release()
            except AzureBlobError as exc:
                # Lease may have expired or been broken — this is not fatal.
                logger.warning(
                    f"Failed to release lease {lease_id} on task {task_id} in graph {graph_id}: "
                    f"status={exc.status_code} reason={exc.reason}. "
                    "Lease will auto-expire after the configured duration."
                )

    async def store_graph(self, graph: TaskGraph) -> TaskGraph:
        """
        Stores a TaskGraph to Azure Blob Storage and stores all children as pending tasks as well.

        We expect the *written graph* to be **immutable** (see the ImmutabilityPolicy below).

        Args:
            graph: The TaskGraph instance to store.
        """
        with tracer.start_as_current_span("store_graph"):
            created_date_tag = datetime.now(UTC).strftime("%Y-%m-%d")
            async with self.get_blob_service_client() as blob_service_client:
                container_client = blob_service_client.get_container_client(self.container_name)
                # Store the graph itself first
                fname = f"{self.task_result_prefix}/{graph.storage_path}"
                try:
                    _result = await container_client.upload_blob(
                        fname,
                        graph.model_dump_json(),
                        blob_type="BlockBlob",
                        tags={"graph_id": str(graph.graph_id), "created_date": created_date_tag},
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
                                tags={
                                    "graph_id": pending_result.graph_id or "none",
                                    "status": pending_result.status,
                                    "created_date": created_date_tag,
                                },
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
            return graph

    async def load_task_result(self, task_id: TaskId, graph_id: GraphId) -> TaskResultSlim | None:
        """Load a single task result from Azure Blob Storage.

        Returns None if the blob does not exist (404). Raises BoilermakerStorageError
        for any other failure. Used by the idempotent redelivery guard in
        TaskGraphEvaluator to check terminal status before writing Started.

        Args:
            task_id: The TaskId of the task result to load.
            graph_id: The GraphId the task belongs to.
        """
        with tracer.start_as_current_span("load_task_result"):
            fname = f"{self.task_result_prefix}/{graph_id}/{task_id}.json"
            try:
                return await self._download_result_with_limiter(graph_id, fname, CapacityLimiter(1))
            except BoilermakerStorageError as exc:
                if exc.status_code == 404:
                    return None
                raise

    async def load_graph_slim_from_tags(self, graph_id: GraphId) -> TaskGraph | None:
        """Load a TaskGraph for display only, using blob tags to avoid content downloads.

        Builds TaskResultSlim from the 'status' tag on each blob instead of
        downloading the blob body. Falls back to content download for any blob
        that is missing 'status' tag (older blobs that pre-date tag support).

        Args:
            graph_id: The GraphId to load.
        Returns:
            The loaded TaskGraph, or None if graph.json is not found.
        Raises:
            BoilermakerStorageError: On storage or deserialization failures.
        """
        if not graph_id:
            raise ValueError("`graph_id` must be provided to load a TaskGraph.")
        with tracer.start_as_current_span("load_graph_slim_from_tags"):
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

            blob_names_to_download: list[str] = []
            try:
                async for blob in self.list_blobs(prefix=graph_dir, include=["tags"]):
                    if blob.name == graph_path:
                        continue
                    tags = blob.tags or {}
                    status_tag = tags.get("status")
                    if status_tag:
                        raw_graph_id = tags.get("graph_id")
                        blob_graph_id = GraphId(raw_graph_id) if raw_graph_id and raw_graph_id != "none" else None
                        task_id = TaskId(blob.name.split("/")[-1].removesuffix(".json"))
                        tr = TaskResultSlim(
                            task_id=task_id,
                            graph_id=blob_graph_id,
                            status=TaskStatus(status_tag),
                            etag=blob.etag,
                        )
                        if tr.graph_id == graph_id:
                            graph.results[tr.task_id] = tr
                        else:
                            logger.warning(
                                f"TaskResult {tr.task_id} in graph {graph_dir} with wrong graph_id {tr.graph_id}!"
                            )
                    else:
                        blob_names_to_download.append(blob.name)
            except AzureBlobError as exc:
                raise BoilermakerStorageError(
                    f"Failed to list blobs for graph {graph_id}",
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=exc.reason,
                ) from exc
            except HttpResponseError as exc:
                raise BoilermakerStorageError(
                    f"Failed to list blobs for graph {graph_id}",
                    graph_id=graph_id,
                    status_code=exc.status_code,
                    reason=str(exc),
                ) from exc

            limiter = CapacityLimiter(10)

            async def _download_and_store(blob_name: str) -> None:
                tr = await self._download_result_with_limiter(graph_id, blob_name, limiter)
                if tr.graph_id == graph_id:
                    graph.results[tr.task_id] = tr
                else:
                    logger.warning(f"TaskResult {tr.task_id} in graph {graph_dir} with wrong graph_id {tr.graph_id}!")

            try:
                async with create_task_group() as tg:
                    for blob_name in blob_names_to_download:
                        tg.start_soon(_download_and_store, blob_name)
            except* BoilermakerStorageError as exc_group:
                raise exc_group.exceptions[0] from exc_group.exceptions[0].__cause__

            return graph

    async def store_task_result(
        self,
        task_result: TaskResult | TaskResultSlim,
        etag: str | None = None,
        lease_id: str | None = None,
    ) -> None:
        """Stores a TaskResult to Azure Blob Storage.

        Args:
            task_result: The TaskResult instance to store.
            etag: Optional ETag for conditional write (If-Match). When provided,
                the write is rejected (412) if the blob has been modified since
                the ETag was observed.
            lease_id: Optional lease ID. When provided, the write is forwarded to
                the Azure SDK as the ``lease`` kwarg so the service rejects the
                upload if the caller is not the current lease holder.
        """
        with tracer.start_as_current_span("store_task_result"):
            fname = str(task_result.storage_path)
            if self.task_result_prefix:
                fname = f"{self.task_result_prefix}/{fname}"

            # add tags:
            blob_tags = {
                "graph_id": task_result.graph_id or "none",
                "status": task_result.status,
                "created_date": datetime.now(UTC).strftime("%Y-%m-%d"),
            }
            concurrency_kwargs: dict[str, str | int | MatchConditions] = {}
            if etag:
                concurrency_kwargs["etag"] = etag
                concurrency_kwargs["match_condition"] = MatchConditions.IfNotModified
            if lease_id:
                concurrency_kwargs["lease"] = lease_id

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

    async def delete_blobs_batch(self, blob_names: list[str]) -> list[str]:
        """Delete blobs in batches of up to 256. Returns list of blob names
        that failed to delete (non-404 errors). 404s are treated as success.
        """
        failed: list[str] = []
        async with self.get_blob_service_client() as blob_service_client:
            container_client = blob_service_client.get_container_client(self.container_name)
            for i in range(0, len(blob_names), BATCH_DELETE_MAX):
                chunk = blob_names[i : i + BATCH_DELETE_MAX]
                idx = 0
                async for result in await container_client.delete_blobs(*chunk):
                    status_code = result.status_code
                    if status_code not in (202, 404):
                        failed.append(chunk[idx])
                    idx += 1
                if idx != len(chunk):
                    logger.warning(
                        "Batch delete returned %d results for %d blobs; "
                        "failure attribution may be inaccurate.",
                        idx, len(chunk),
                    )
        return failed

    async def find_blobs_by_tags(
        self, filter_expression: str
    ) -> AsyncGenerator[BlobProperties, None]:
        """Query blobs using Azure Blob Storage tag index.

        Args:
            filter_expression: OData-style filter string, e.g.
                '"created_date" < '2026-04-13''
        Yields:
            BlobProperties for each matching blob.
        """
        async with self.get_blob_service_client() as blob_service_client:
            container_client = blob_service_client.get_container_client(
                self.container_name
            )
            async for blob in container_client.find_blobs_by_tags(
                filter_expression=filter_expression
            ):
                yield blob
