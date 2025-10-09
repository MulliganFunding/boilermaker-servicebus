import logging
from abc import ABC, abstractmethod

from boilermaker.task import GraphId, TaskGraph, TaskResult, TaskResultSlim

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
