import datetime
import enum
import logging
import typing
from collections import defaultdict
from functools import cached_property
from pathlib import Path

import uuid_utils as uuid
from azure.servicebus import ServiceBusReceivedMessage
from pydantic import BaseModel, ConfigDict, Field

from boilermaker import tracing

from . import retries

logger = logging.getLogger(__name__)
DEFAULT_RETRY_ATTEMPTS = 1
TaskId = typing.NewType("TaskId", str)
GraphId = typing.NewType("GraphId", str)


class CallbackType(enum.IntEnum):
    """Enumeration of callback types for task execution."""

    Success = 1
    Failure = 2


class Task(BaseModel):
    """Represents a serializable task with retry policies and callback chains.

    A Task encapsulates a function call with its arguments, retry configuration,
    and optional success/failure callbacks. Tasks are JSON-serializable and can
    be published to Azure Service Bus for asynchronous execution.

    Attributes:
        task_id: Unique UUID7 identifier for timestamp-ordered task identification
        should_dead_letter: Whether failed tasks should be dead-lettered (default: True)
        acks_late: Whether to acknowledge messages after processing (default: True)
        function_name: Name of the registered function to execute
        attempts: Retry attempt tracking and metadata
        policy: Retry policy governing backoff and max attempts
        payload: Function arguments and keyword arguments (must be JSON-serializable)
        _sequence_number: Service Bus sequence number (set after publishing)
        on_success: Optional callback task to run on successful completion
        on_failure: Optional callback task to run on failure

    Example:
        >>> # Create task with default settings
        >>> task = Task.default("my_function", args=[1, 2], kwargs={"key": "value"})
        >>>
        >>> # Create task with custom retry policy
        >>> policy = RetryPolicy(max_tries=5, backoff_mode="exponential")
        >>> task = Task.si(my_function, arg1, kwarg=value, policy=policy)
        >>>
        >>> # Chain tasks with callbacks
        >>> task1 >> task2 >> task3  # Success chain
        >>> task1.on_failure = error_handler_task
    """

    # Unique identifier for this task: UUID7 for timestamp ordered identifiers.
    task_id: TaskId = Field(default_factory=lambda: TaskId(str(uuid.uuid7())))
    # Whether we should dead-letter a failing message
    should_dead_letter: bool = True
    # At-most-once vs at-least-once (default)
    acks_late: bool = True
    # function name for this task
    function_name: str
    # Records how many attempts for this task (if previous)
    attempts: retries.RetryAttempts
    # For retries, we want a policy to govern how we retry this task
    policy: retries.RetryPolicy
    # Represents actual arguments: must be jsonable!
    payload: dict[str, typing.Any]

    # Can set this once we receive the message
    _msg: ServiceBusReceivedMessage | None = None

    # If this task is a member of a TaskGraph, this will identify the root node.
    graph_id: GraphId | None = None

    # Callbacks for success and failure (used for chains)
    on_success: typing.Optional["Task"] = None
    on_failure: typing.Optional["Task"] = None

    # Required for the uuid.UUID type annotation
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def default(cls, function_name: str, **kwargs):
        """Create a Task with default retry settings.

        Convenience method to create a task with sensible defaults for
        retry attempts and policy. Additional keyword arguments override
        default values.

        Args:
            function_name: Name of the function to execute
            **kwargs: Additional task attributes to override defaults

        Returns:
            Task: New task instance with default retry configuration

        Example:
            >>> task = Task.default("process_data", payload={"data": [1, 2, 3]})
        """
        attempts = retries.RetryAttempts.default()
        policy = retries.RetryPolicy.default()
        if "policy" in kwargs:
            policy = kwargs.pop("policy")
        if "payload" in kwargs:
            payload = kwargs.pop("payload")
            if "args" not in payload:
                payload["args"] = []
            if "kwargs" not in payload:
                payload["kwargs"] = {}
        else:
            payload = {}
        return cls(
            attempts=attempts,
            function_name=function_name,
            policy=policy,
            payload=payload,
            **kwargs,
        )

    @cached_property
    def diagnostic_id(self) -> str | None:
        """Get the OpenTelemetry diagnostic ID if available.

        Returns:
            str | None: The diagnostic ID for tracing, or None if not set
        """
        if self._msg:
            return tracing.get_traceparent(self._msg)
        return None

    @cached_property
    def sequence_number(self) -> int | None:
        """Get the Service Bus sequence number if available.

        Returns:
            int | None: The sequence number of the associated Service Bus message, or None if not set
        """
        if self._msg:
            return self._msg.sequence_number
        return None

    @property
    def msg(self) -> ServiceBusReceivedMessage | None:
        """Get the associated Service Bus message if available.

        Returns:
            ServiceBusReceivedMessage | None: The associated Service Bus message, or None if not set
        """
        return self._msg

    @msg.setter
    def msg(self, value: ServiceBusReceivedMessage) -> None:
        """Set the associated Service Bus message.

        Args:
            value: The Service Bus message to associate with this task
        """
        self._msg = value

    @property
    def acks_early(self):
        """Whether the task acknowledges messages before processing.

        Returns:
            bool: True if acks_late is False, meaning messages are
                  acknowledged immediately upon receipt
        """
        return not self.acks_late

    @property
    def can_retry(self):
        """Whether the task can be retried based on current attempts.

        Returns:
            bool: True if current attempts are less than or equal to
                  the maximum tries allowed by the retry policy
        """
        return self.attempts.attempts <= self.policy.max_tries

    def get_next_delay(self):
        """Calculate the delay before the next retry attempt.

        Uses the task's retry policy to determine the appropriate
        delay based on the current number of attempts.

        Returns:
            int: Delay in seconds before next retry attempt
        """
        return self.policy.get_delay_interval(self.attempts.attempts)

    def record_attempt(self):
        """Record a new execution attempt with current timestamp.

        Increments the attempt counter and records the current UTC
        timestamp for tracking retry intervals and debugging.

        Returns:
            RetryAttempts: Updated attempts object with incremented count
        """
        now = datetime.datetime.now(datetime.UTC)
        return self.attempts.inc(now)

    def __hash__(self) -> int:
        """Hash based on unique task_id for use in sets and dicts."""
        return hash(self.task_id)

    def __rshift__(self, other: "Task") -> "Task":
        """Set success callback using >> operator (right-shift chaining).

        Creates a success callback chain where the right-hand task
        executes if this task completes successfully.

        Args:
            other: Task to execute on success

        Returns:
            Task: The other task, allowing for continued chaining

        Example:
            >>> task1 >> task2 >> task3
            # If task1 succeeds, run task2
            # If task2 succeeds, run task3
        """
        self.on_success = other
        return other

    def __lshift__(self, other: "Task") -> "Task":
        """Set success callback using << operator (left-shift chaining).

        Creates a success callback chain where this task executes
        if the right-hand task completes successfully.

        Args:
            other: Task that will trigger this task on success

        Returns:
            Task: This task, allowing for continued chaining

        Example:
            >>> task1 << task2 << task3
            # If task3 succeeds, run task2
            # If task2 succeeds, run task1
        """
        other.on_success = self
        return self

    @classmethod
    def si(
        cls,
        fn: typing.Callable,
        *fn_args,
        should_dead_letter: bool = True,
        acks_late: bool = True,
        policy: retries.RetryPolicy | None = None,
        **fn_kwargs,
    ) -> "Task":
        """Create an immutable signature task from a function and arguments.

        Creates a task bound to specific function arguments, useful for
        preparing tasks with callbacks or custom settings before publishing.
        The function arguments are captured at creation time.

        Note: This implementation creates immutable signatures only.
        Future versions may support mutable signatures where task outputs
        are passed to subsequent tasks in a chain.

        Args:
            fn: The function to be executed
            *fn_args: Positional arguments for the function
            should_dead_letter: Whether to dead-letter failed tasks (default: True)
            acks_late: Whether to acknowledge after processing (default: True)
            policy: Custom retry policy (uses default if None)
            **fn_kwargs: Keyword arguments for the function

        Returns:
            Task: New task with bound function signature

        Example:
            >>> def process_data(data, format="json"):
            ...     return f"Processed {data} as {format}"
            >>>
            >>> task = Task.si(process_data, [1, 2, 3], format="xml")
            >>> # Arguments are bound to the task
        """
        attempts = retries.RetryAttempts.default()
        policy = policy or retries.RetryPolicy.default()

        return cls(
            should_dead_letter=should_dead_letter,
            acks_late=acks_late,
            attempts=attempts,
            function_name=fn.__name__,
            policy=policy,
            payload={
                "args": fn_args,
                "kwargs": fn_kwargs,
            },
        )


class TaskStatus(enum.StrEnum):
    """Enumeration of possible task execution statuses."""

    Pending = "pending"
    Started = "started"
    Success = "success"
    Failure = "failure"
    Retry = "retry"
    RetriesExhausted = "retries_exhausted"
    Deadlettered = "deadlettered"
    # In case we want to explicitly abandon a message.
    # We do not currently offer this functionality aside from shutdown.
    Abandoned = "abandoned"

    @classmethod
    def default(cls) -> "TaskStatus":
        """Get the default task status.

        Returns:
            TaskStatus: The default status, which is Pending
        """
        return cls.Pending


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


class TaskGraph(BaseModel):
    """
    Represents a Directed Acyclic Graph (DAG) of tasks.

    A TaskGraph encapsulates a collection of tasks with defined dependencies ("antecedents").
    Each task can have multiple child tasks that depend on its successful completion.

    Each task in the graph is represented as a node, and edges define the
    parent-child relationships between tasks. When all antecedent tasks
    of a given task are completed successfully, that task becomes eligible
    for execution.

    Every task eligible for execution can be immediately published to the task queue
    (which allows for parallel execution of independent tasks).

    In short, if all parent tasks have succeeded, we can immediately
    *schedule* their children.

    We expect the graph to be a DAG: no cycles are allowed.

    Each completed task will have its result stored in persistent storage,
    and checking whether the *next* set of tasks is "ready" means deserializing
    their antecedents and checking their statuses. In other words,
    we expect the graph to be serialized to storage when a TaskGraph is *first* published
    but we also expect it to be loaded into memory from storage at the conclusion of each task execution.

    The order of operations is like this:
    - [Send]: Task published -> Graph serialized to storage. We do not write it again!
    - [Receive]: Task invoked
        -> Evaluation -> TaskResult stored to storage.
        -> Graph loaded from storage (includes latest TaskResultSlim instances).
        -> Check which tasks are ready.
        -> Publish ready tasks.

    Attributes:
        root_id: Unique identifier for the root of the DAG
        children: Mapping of task IDs to Task instances
        edges: Mapping of parent task IDs to lists of child task IDs
        # On write -> TaskResult; on read -> TaskResultSlim
        results: Mapping of task IDs to their TaskResult or TaskResultSlim
    """

    StorageName: typing.ClassVar[str] = "graph.json"

    # The graph has an ID
    graph_id: GraphId = Field(default_factory=lambda: GraphId(str(uuid.uuid7())))
    # Children is a mapping of task IDs to tasks
    children: dict[TaskId, Task] = Field(default_factory=dict)
    # Edges is a mapping of parent task IDs to sets of child task IDs
    edges: dict[TaskId, set[TaskId]] = Field(
        default_factory=lambda: defaultdict(set[TaskId])
    )

    # Task results go here; these get loaded from JSON files on deserialization.
    # We do not write these back because we write only one time: when first publishing this TaskGraph.
    results: dict[TaskId, TaskResultSlim | TaskResult] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def graph_path(cls, graph_id: GraphId) -> Path:
        """Returns the storage path for an arbitrary GraphId."""
        return Path(graph_id) / cls.StorageName

    @property
    def storage_path(self) -> Path:
        """Returns the storage path for this task graph."""
        return self.graph_path(self.graph_id)

    def _has_cycle_dfs(self, start_node: TaskId, visited: set[TaskId], rec_stack: set[TaskId]) -> bool:
        """Check for cycles using DFS traversal.

        Args:
            start_node: Current node being visited
            visited: Set of all visited nodes
            rec_stack: Set of nodes in current recursion stack

        Returns:
            bool: True if cycle is detected, False otherwise
        """
        visited.add(start_node)
        rec_stack.add(start_node)

        # Check all children of current node
        for child_id in self.edges.get(start_node, set()):
            # If child not visited, recurse
            if child_id not in visited:
                if self._has_cycle_dfs(child_id, visited, rec_stack):
                    return True
            # If child is in recursion stack, we found a back edge (cycle)
            elif child_id in rec_stack:
                return True

        # Remove from recursion stack before returning
        rec_stack.remove(start_node)
        return False

    def _detect_cycles(self) -> bool:
        """Detect if the graph contains any cycles.

        Returns:
            bool: True if any cycle is detected, False if DAG is valid
        """
        visited: set[TaskId] = set()
        rec_stack: set[TaskId] = set()

        # Check each unvisited node as potential start of cycle
        for task_id in self.children.keys():
            if task_id not in visited:
                if self._has_cycle_dfs(task_id, visited, rec_stack):
                    return True

        return False

    def add_task(self, task: Task, parent_id: TaskId | None = None) -> None:
        """Add a task to the graph.

        Args:
            task: The Task instance to add to the graph.
            parent_id: Optional parent task ID to create dependency

        Raises:
            ValueError: If adding this task would create a cycle in the DAG
        """
        # Ensure task is part of this graph
        task.graph_id = self.graph_id
        self.children[task.task_id] = task

        if parent_id:
            if parent_id not in self.edges:
                self.edges[parent_id] = set()
            self.edges[parent_id].add(task.task_id)

            # Check for cycles after adding the edge
            if self._detect_cycles():
                # Rollback the changes
                self.edges[parent_id].remove(task.task_id)
                if not self.edges[parent_id]:  # Remove empty set
                    del self.edges[parent_id]
                del self.children[task.task_id]
                raise ValueError(
                    f"Adding task {task.task_id} with parent {parent_id} would create a cycle in the DAG"
                )

    def start_task(self, task_id: TaskId) -> TaskResultSlim:
        """Mark a task as started to prevent double-scheduling."""
        result = TaskResultSlim(task_id=task_id, graph_id=self.graph_id, status=TaskStatus.Started)
        self.results[task_id] = result
        return result

    def add_result(self, result: TaskResult) -> TaskResult:
        """Mark a task as completed with result."""
        if result.task_id not in self.children:
            raise ValueError(f"Task {result.task_id} not found in graph")

        self.results[result.task_id] = result
        return result

    def task_is_ready(self, task_id: TaskId) -> bool:
        """Check if a task is ready to be executed (antecedents succeeded and not yet started)."""
        return self.all_antecedents_succeeded(task_id)

    def all_antecedents_succeeded(self, task_id: TaskId) -> bool:
        """Check if all antecedent tasks of a given task are completed with `Success` result."""
        for parent_id, children_ids in self.edges.items():
            if task_id in children_ids:
                if parent_id not in self.results:
                    return False
                if not self.results[parent_id].status == TaskStatus.Success:
                    return False
        # If we get here, all antecedents succeeded *OR* there are no antecedents.
        return True

    def ready_tasks(self) -> typing.Generator[Task, None, None]:
        """Get a list of tasks that are ready to be executed (not started and all antecedents succeeded)."""
        for task_id in self.children.keys():
            # Task is ready if:
            # 1. It has no result yet (never started) OR it has Pending status
            # 2. All its antecedents have succeeded
            task_result = self.results.get(task_id)
            is_not_started = task_result is None or task_result.status == TaskStatus.Pending
            if is_not_started and self.task_is_ready(task_id):
                yield self.children[task_id]

    def get_result(self, task_id: TaskId) -> TaskResultSlim | TaskResult | None:
        """Get the result of a completed task."""
        return self.results.get(task_id)

    def get_status(self, task_id: TaskId) -> TaskStatus | None:
        """Check if a task is completed."""
        if tr := self.get_result(task_id):
            return tr.status
        return None

    def generate_pending_results(self) -> typing.Generator[TaskResultSlim]:
        """
        Generate pending TaskResultSlim entries for all tasks.
        """
        for task_id in self.children.keys():
            if self.get_result(task_id) is None:
                yield TaskResultSlim(
                    task_id=task_id,
                    graph_id=self.graph_id,
                    status=TaskStatus.Pending,
                )

    def completed_successfully(self) -> bool:
        """Check if all tasks in the graph have completed successfully."""
        return all(
            map(
                lambda task_id: self.get_status(task_id) == TaskStatus.Success,
                self.children.keys(),
            )
        )
