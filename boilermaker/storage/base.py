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
