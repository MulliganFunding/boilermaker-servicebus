import enum
import typing
from pathlib import Path

from pydantic import BaseModel

from .task_id import GraphId, TaskId


class TaskStatus(enum.StrEnum):
    """Enumeration of possible task execution statuses."""

    Pending = "pending"
    Scheduled = "scheduled"
    Started = "started"
    Success = "success"
    Failure = "failure"
    Retry = "retry"
    RetriesExhausted = "retries_exhausted"
    Deadlettered = "deadlettered"


    @classmethod
    def default(cls) -> "TaskStatus":
        """Get the default task status.

        Returns:
            TaskStatus: The default status, which is Pending
        """
        return cls.Pending

    @property
    def succeeded(self) -> bool:
        """Check if the task status represents a successful completion.

        Returns:
            bool: True if the status is Success
        """
        return self == TaskStatus.Success

    @classmethod
    def finished_types(cls) -> set["TaskStatus"]:
        return {
            TaskStatus.Success,
            TaskStatus.Failure,
            TaskStatus.RetriesExhausted,
            TaskStatus.Deadlettered,
        }

    @property
    def finished(self) -> bool:
        """Check if the task status represents a finished state.

        Returns:
            bool: True if the status is one of Success, Failure, RetriesExhausted, or Deadlettered
        """
        return self in self.finished_types()

    @property
    def failed(self) -> bool:
        """Check if the task status represents a failure.

        Returns:
            bool: True if the status is one of Failure, RetriesExhausted, or Deadlettered
        """
        return self in self.failure_types()

    @classmethod
    def failure_types(cls) -> set["TaskStatus"]:
        return {
            TaskStatus.Failure,
            TaskStatus.RetriesExhausted,
            TaskStatus.Deadlettered,
        }


class TaskResultSlim(BaseModel):
    """Slim representation of a task result for lightweight status checks.

    This class provides a minimal view of a task's execution result,
    focusing on essential attributes like task ID, status, and error messages.
    It is useful for scenarios where full task result details (return value, exception, errors)
    are not required.

    Attributes:
        task_id: Identifier of the task.
        graph_id: Optional identifier of the task graph this task belongs to.
        status: Execution status of the task.
    """

    etag: str | None = None
    task_id: TaskId
    graph_id: GraphId | None = None
    status: TaskStatus

    @classmethod
    def default(cls, task_id: TaskId, graph_id: GraphId | None = None) -> "TaskResultSlim":
        """Create a default TaskResultSlim with Pending status.

        Returns:
            TaskResultSlim: New instance with status set to Pending
        """
        return TaskResultSlim(task_id=task_id, graph_id=graph_id, status=TaskStatus.default())

    @property
    def directory_path(self) -> Path:
        """Returns the directory path for storing this task result.

        The directory is based on the root and parent task IDs to group related tasks.

        Returns:
            A string representing the directory path.
        """
        if self.graph_id:
            return Path(self.graph_id)
        return Path(self.task_id)

    @property
    def storage_path(self) -> Path:
        """Returns the storage path for this task result.

        The storage path is based on the root and parent task IDs to group related tasks.

        Returns:
            A string representing the storage path.
        """
        directory = self.directory_path
        return directory / f"{self.task_id}.json"

    @property
    def finished(self) -> bool:
        """Check if the task status represents a finished state.

        Returns:
            bool: True if the status is one of Success, Failure, RetriesExhausted, or Deadlettered
        """
        return self.status.finished

    @property
    def succeeded(self) -> bool:
        """Check if the task status represents a successful completion.

        Returns:
            bool: True if the status is Success
        """
        return self.status.succeeded

    @property
    def failed(self) -> bool:
        """Check if the task status represents a failure.

        Returns:
            bool: True if the status is one of Failure, RetriesExhausted, or Deadlettered
        """
        return self.status.failed


class TaskResult(TaskResultSlim):
    """Represents the result of a task execution.

    Encapsulates the outcome of a task, including success status,
    return value, and any exception raised during execution.

    Attributes:
        task_id: Identifier of the task.
        status: Execution status of the task.
        result: The return value of the task if successful.
        errors: List of error messages if any occurred.
        exception: Formatted exception (as a string) raised if the task failed.
    """

    # Inherits task_id, graph_id, and status from TaskResultSlim

    # Out from evaluation of Task: must be jsonable!
    result: typing.Any | None = None
    # Custom error messages, if any.
    errors: list[str] | None = None
    # String-formatted exception, if any.
    formatted_exception: str | None = None
