import logging
from abc import ABC, abstractmethod

from boilermaker.task import GraphId, TaskGraph, TaskId, TaskResult, TaskResultSlim

logger = logging.getLogger(__name__)


class StorageInterface(ABC):
    """Interface for storage operations related to TaskGraph and TaskResult objects."""

    @abstractmethod
    async def load_graph(self, graph_id: GraphId) -> TaskGraph | None:
        """
        Loads a TaskGraph from storage.

        Args:
            graph_id: The GraphId of the TaskGraph to load.

        Returns:
            The loaded TaskGraph instance, or None if not found.
        """
        raise NotImplementedError

    @abstractmethod
    async def store_graph(self, graph: TaskGraph) -> TaskGraph:
        """
        Stores a TaskGraph to storage.

        Args:
            graph: The TaskGraph instance to store.
        """
        raise NotImplementedError

    @abstractmethod
    async def store_task_result(self, task_result: TaskResult | TaskResultSlim, etag: str | None = None) -> None:
        """Stores a TaskResult to storage.

        Args:
            task_result: The TaskResult instance to storage.
        """
        raise NotImplementedError

    @abstractmethod
    async def try_acquire_lease(
        self,
        task_id: TaskId,
        graph_id: GraphId,
        etag: str | None = None,
        lease_duration: int = 15,
    ) -> str | None:
        """Attempt to acquire a lease on a task result blob.

        This is a non-blocking operation. If the blob already has an active lease
        (another worker is scheduling this task), returns None immediately.

        When ``etag`` is provided, the lease acquire uses an If-Match conditional
        header. If the blob's current ETag does not match (i.e., the blob was
        modified since it was last read), the acquire fails with 412 and this
        method returns None. This allows the caller to atomically verify "the
        blob hasn't changed since I read it" AND "no other worker is scheduling
        it" in a single round-trip.

        Args:
            task_id: The TaskId of the task to lease.
            graph_id: The GraphId the task belongs to.
            etag: Optional ETag to use as a precondition on the lease acquire.
                If provided, the acquire will fail (return None) when the blob's
                current ETag does not match.
            lease_duration: Lease duration in seconds (15-60, or -1 for infinite).
                Default 15 seconds — short enough that a crashed worker's lease
                expires quickly, long enough for publish + write.

        Returns:
            The lease_id string if the lease was acquired, or None if the blob
            is already leased (409) or the ETag precondition failed (412).

        Raises:
            BoilermakerStorageError: If the lease fails for reasons other than
                an existing lease or ETag mismatch (e.g., blob not found, auth error).
        """
        raise NotImplementedError

    @abstractmethod
    async def release_lease(
        self, task_id: TaskId, graph_id: GraphId, lease_id: str
    ) -> None:
        """Release a previously acquired lease on a task result blob.

        This is a best-effort operation. If the lease has already expired
        or been broken, this should log a warning and return (not raise).

        Args:
            task_id: The TaskId of the task to release.
            graph_id: The GraphId the task belongs to.
            lease_id: The lease_id returned by try_acquire_lease.
        """
        raise NotImplementedError

    @abstractmethod
    async def load_task_result(self, task_id: TaskId, graph_id: GraphId) -> TaskResultSlim | None:
        """Load a single task result from storage.

        Used by the idempotent redelivery guard in TaskGraphEvaluator to check whether
        a task has already reached a terminal state before writing Started on redelivery.

        Args:
            task_id: The TaskId of the task result to load.
            graph_id: The GraphId the task belongs to.

        Returns:
            The TaskResultSlim instance, or None if not found.

        Raises:
            BoilermakerStorageError: If the result cannot be loaded for reasons other than not found.
        """
        raise NotImplementedError
