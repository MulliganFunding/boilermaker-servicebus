import datetime
import enum
import logging
import typing
from collections import defaultdict
from collections.abc import Generator
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
    # After publishing, we can set this to the sequence number
    _sequence_number: int | None = None

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
        if self._sequence_number:
            return self._sequence_number
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

    def mark_published(self, sequence_number: int) -> None:
        """Mark the task as published by setting the sequence number.

        Args:
            sequence_number: The Service Bus sequence number assigned upon publishing
        """
        self._sequence_number = sequence_number

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
    Scheduled = "scheduled"
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
    # Failure children is a mapping of task IDs to tasks
    fail_children: dict[TaskId, Task] = Field(default_factory=dict)
    # Edges is a mapping of parent task IDs to sets of child task IDs
    edges: dict[TaskId, set[TaskId]] = Field(
        default_factory=lambda: defaultdict(set[TaskId])
    )
    # fail_edges is a mapping of parent task IDs to sets of child task IDs for failure callbacks
    fail_edges: dict[TaskId, set[TaskId]] = Field(default_factory=lambda: defaultdict(set[TaskId]))

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

        # Check all success children of current node
        for child_id in self.edges.get(start_node, set()):
            # If child not visited, recurse
            if child_id not in visited:
                if self._has_cycle_dfs(child_id, visited, rec_stack):
                    return True
            # If child is in recursion stack, we found a back edge (cycle)
            elif child_id in rec_stack:
                return True

        # Check all failure children of current node
        for child_id in self.fail_edges.get(start_node, set()):
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

        # Get all task IDs from both success and failure children
        all_task_ids = set(self.children.keys()) | set(self.fail_children.keys())

        # Check each unvisited node as potential start of cycle
        for task_id in all_task_ids:
            if task_id not in visited:
                if self._has_cycle_dfs(task_id, visited, rec_stack):
                    return True

        return False

    def add_failure_callback(self, parent_id: TaskId, callback_task: Task) -> None:
        """Add an failure callback task to the graph.

        Args:
            parent_id: The task ID that will trigger the error callback
            callback_task: The Task instance to add as an error callback

        Raises:
            ValueError: If adding this callback would create a cycle in the DAG
        """
        if parent_id not in self.edges:
            self.edges[parent_id] = set()
        self.fail_edges[parent_id].add(callback_task.task_id)
        callback_task.graph_id = self.graph_id
        self.fail_children[callback_task.task_id] = callback_task

        # Check for cycles after adding the edge
        if self._detect_cycles():
            # Rollback the changes
            self.fail_edges[parent_id].remove(callback_task.task_id)
            if not self.fail_edges[parent_id]:  # Remove empty set
                del self.fail_edges[parent_id]
            del self.fail_children[callback_task.task_id]
            raise ValueError(f"Adding failure callback for task {parent_id} would create a cycle in the DAG")

    def add_task(self, task: Task, parent_ids: list[TaskId] | None = None) -> None:
        """Add a task to the graph.

        Args:
            task: The Task instance to add to the graph.
            parent_ids: Optional list of parent task IDs to create dependencies

        Raises:
            ValueError: If adding this task would create a cycle in the DAG
        """
        # Ensure task is part of this graph
        task.graph_id = self.graph_id
        self.children[task.task_id] = task

        if parent_ids:
            for parent_id in parent_ids:
                if parent_id not in self.edges:
                    self.edges[parent_id] = set()
                self.edges[parent_id].add(task.task_id)

            # Check for cycles after adding the edges
            if self._detect_cycles():
                # Rollback the changes
                for parent_id in parent_ids:
                    self.edges[parent_id].remove(task.task_id)
                    if not self.edges[parent_id]:  # Remove empty set
                        del self.edges[parent_id]
                del self.children[task.task_id]
                raise ValueError(
                    f"Adding task {task.task_id} with parent {parent_id} would create a cycle in the DAG"
                )

        # If we leave `on_success` and `on_failure` it's potentially confusing for both callers
        # and our own evaluation. It also has the potential to create cycles inadvertently, so we
        # dynamically add on_success and on_failure into edges here and remove these as individual task callbacks.
        if task.on_success:
            self.add_task(task.on_success, parent_ids=[task.task_id])
            task.on_success = None  # Clear to avoid duplication

        if task.on_failure:
            self.add_failure_callback(task.task_id, task.on_failure)
            task.on_failure = None  # Clear to avoid duplication

    def schedule_task(self, task_id: TaskId) -> TaskResult | TaskResultSlim:
        """Mark a task as scheduled to prevent double-scheduling."""
        if task_id not in self.children:
            raise ValueError(f"Task {task_id} not found in graph")
        if task_id not in self.results or self.results[task_id].status != TaskStatus.Pending:
            raise ValueError(f"Task {task_id} is not pending and cannot be scheduled")

        result = self.results[task_id]
        result.status = TaskStatus.Scheduled
        self.results[result.task_id] = result
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
                if not self.results[parent_id].suceeded:
                    return False
        # If we get here, all antecedents succeeded *OR* there are no antecedents.
        return True

    def all_antecedents_finished(self, task_id: TaskId) -> bool:
        """Check if all antecedent tasks of a given task are completed with `Success` or `Failure` result."""
        for parent_id, children_ids in self.edges.items():
            if task_id in children_ids:
                if parent_id not in self.results:
                    return False
                # Check if the parent has actually finished (not just exists in results)
                if not self.results[parent_id].status.finished:
                    return False
        # If we get here, all antecedents have finished *OR* there are no antecedents.
        return True

    def ready_tasks(self) -> Generator[Task]:
        """Get a list of tasks that are ready to be executed (not started and all antecedents succeeded)."""
        for task_id in self.children.keys():
            # Task is ready if:
            # 1. It has no result yet (never started) OR it has Pending status
            # 2. All its antecedents have succeeded
            task_result = self.results.get(task_id)
            is_not_started = task_result is None or task_result.status == TaskStatus.Pending
            if is_not_started and self.task_is_ready(task_id):
                yield self.children[task_id]

    def failure_ready_tasks(self) -> Generator[Task]:
        """Get a list of failure callback tasks that are ready to be executed.

        A failure task is ready if:
        1. It hasn't started yet (no result or Pending status)
        2. At least one of its triggering parent tasks has failed
        3. All other dependencies (if any) have finished
        """
        for task_id in self.fail_children.keys():
            # Check if this failure task has already started
            task_result = self.results.get(task_id)
            is_not_started = task_result is None or task_result.status == TaskStatus.Pending

            if not is_not_started:
                continue

            # Find which parent task(s) would trigger this failure callback
            triggering_parents = []
            for parent_id, fail_child_ids in self.fail_edges.items():
                if task_id in fail_child_ids:
                    triggering_parents.append(parent_id)

            # Check if any triggering parent has failed
            has_failed_parent = False
            for parent_id in triggering_parents:
                if parent_id in self.results and self.results[parent_id].status.failed:
                    has_failed_parent = True
                    break

            # Only yield if we have at least one failed parent
            if has_failed_parent:
                yield self.fail_children[task_id]

    def get_result(self, task_id: TaskId) -> TaskResultSlim | TaskResult | None:
        """Get the result of a completed task."""
        return self.results.get(task_id)

    def get_status(self, task_id: TaskId) -> TaskStatus | None:
        """Check if a task is completed."""
        if tr := self.get_result(task_id):
            return tr.status
        return None

    def generate_pending_results(self) -> Generator[TaskResultSlim]:
        """
        Generate pending TaskResultSlim entries for all tasks.

        This should probably only be run when the graph is first created and stored
        """
        for task_id in self.children.keys():
            # Create a pending result if it doesn't exist
            if self.get_result(task_id) is None:
                pending_result = TaskResultSlim(
                    task_id=task_id,
                    graph_id=self.graph_id,
                    status=TaskStatus.Pending,
                )
                self.results[pending_result.task_id] = pending_result

            task_result = self.results[task_id]
            if task_result.status == TaskStatus.Pending:
                yield task_result

    def completed_successfully(self) -> bool:
        """Check if all tasks in the graph have completed successfully."""
        return all(
            map(
                lambda task_id: self.get_status(task_id) == TaskStatus.Success,
                self.children.keys(),
            )
        )

    def has_failures(self) -> bool:
        """Check if any tasks in the graph have failed."""
        failure_statuses = TaskStatus.failure_types()
        return any(
            self.get_status(task_id) in failure_statuses
            for task_id in self.children.keys()
            if self.get_status(task_id) is not None
        )

    def is_complete(self) -> bool:
        """Check if the graph has finished executing (either all success or has failures)."""
        all_tasks = set(self.children.keys()) | set(self.fail_children.keys())
        complete_statuses = TaskStatus.finished_types()
        return all(
            self.get_status(task_id) in complete_statuses
            for task_id in all_tasks
            if self.get_status(task_id) is not None
        )


class TaskGraphBuilder:
    """
    Builder class for constructing TaskGraph instances with flexible dependency management.

    Supports multiple patterns:
    1. Layer-based building for simple sequential/parallel workflows
    2. Explicit dependency management for complex DAGs
    3. Success and failure callback chaining

    Examples:
        # Simple chain: A -> B -> C
        builder = TaskGraphBuilder().add(taskA).then(taskB).then(taskC)

        # Parallel execution: A, B, C all run in parallel
        builder = TaskGraphBuilder().parallel([taskA, taskB, taskC])

        # Complex dependencies: D depends on A and B, but C runs independently
        builder = (TaskGraphBuilder()
            .add(taskA)
            .add(taskB)
            .add(taskC)
            .add(taskD, depends_on=[taskA.task_id, taskB.task_id]))

        # With failure handling
        builder = (TaskGraphBuilder()
            .add(taskA)
            .then(taskB)
            .on_failure(taskA.task_id, error_handler))
    """

    def __init__(self) -> None:
        self._tasks: dict[TaskId, Task] = {}
        self._dependencies: dict[TaskId, set[TaskId]] = {}
        self._failure_callbacks: dict[TaskId, list[Task]] = {}
        self._last_added: list[TaskId] = []  # Track recently added tasks for chaining

    def add(self, task: Task, depends_on: list[TaskId] | None = None) -> "TaskGraphBuilder":
        """Add a task with optional explicit dependencies.

        Args:
            task: Task to add
            depends_on: Optional list of task IDs this task depends on

        Returns:
            Self for method chaining
        """
        self._tasks[task.task_id] = task
        self._dependencies[task.task_id] = set(depends_on or [])
        self._last_added = [task.task_id]
        return self

    def parallel(self, tasks: list[Task], depends_on: list[TaskId] | None = None) -> "TaskGraphBuilder":
        """Add multiple tasks to run in parallel.

        Args:
            tasks: List of tasks to run in parallel
            depends_on: Optional list of task IDs all these tasks depend on

        Returns:
            Self for method chaining
        """
        task_ids = []
        for task in tasks:
            self._tasks[task.task_id] = task
            self._dependencies[task.task_id] = set(depends_on or [])
            task_ids.append(task.task_id)

        self._last_added = task_ids
        return self

    def then(self, task: Task) -> "TaskGraphBuilder":
        """Add a task that depends on the previously added task(s).

        Args:
            task: Task to add that depends on last added tasks

        Returns:
            Self for method chaining
        """
        if not self._last_added:
            raise ValueError("No previous tasks to depend on. Use add() first.")

        return self.add(task, depends_on=self._last_added)

    def then_parallel(self, tasks: list[Task]) -> "TaskGraphBuilder":
        """Add multiple tasks that all depend on the previously added task(s).

        Args:
            tasks: Tasks to add that depend on last added tasks

        Returns:
            Self for method chaining
        """
        if not self._last_added:
            raise ValueError("No previous tasks to depend on. Use add() or parallel() first.")

        return self.parallel(tasks, depends_on=self._last_added)

    def on_failure(self, parent_task_id: TaskId, callback_task: Task) -> "TaskGraphBuilder":
        """Add a failure callback for a specific task.

        Args:
            parent_task_id: Task ID that triggers the callback on failure
            callback_task: Task to execute on failure

        Returns:
            Self for method chaining
        """
        if parent_task_id not in self._tasks:
            raise ValueError(f"Parent task {parent_task_id} not found. Add it first.")

        if parent_task_id not in self._failure_callbacks:
            self._failure_callbacks[parent_task_id] = []

        self._failure_callbacks[parent_task_id].append(callback_task)
        return self

    def chain(self, *tasks: Task) -> "TaskGraphBuilder":
        """Convenience method to chain tasks in sequence: A -> B -> C -> ...

        Args:
            *tasks: Tasks to chain in order

        Returns:
            Self for method chaining
        """
        if not tasks:
            return self

        # Add first task
        self.add(tasks[0])

        # Chain the rest
        for task in tasks[1:]:
            self.then(task)

        return self

    def fan_out(self, tasks: list[Task]) -> "TaskGraphBuilder":
        """Fan out: make all tasks depend on the last added task(s).

        Alias for then_parallel for more descriptive naming.
        """
        return self.then_parallel(tasks)

    def fan_in(self, task: Task, from_tasks: list[TaskId] | None = None) -> "TaskGraphBuilder":
        """Fan in: make one task depend on multiple specific tasks.

        Args:
            task: Task that depends on multiple parents
            from_tasks: Optional specific task IDs to depend on.
                       If None, depends on all previously added tasks.
        """
        if from_tasks is None:
            from_tasks = self._last_added

        return self.add(task, depends_on=from_tasks)

    def diamond(self, middle_tasks: list[Task], final_task: Task) -> "TaskGraphBuilder":
        """Create diamond pattern: last -> middle_tasks (parallel) -> final.

        Args:
            middle_tasks: Tasks that run in parallel after last added task(s)
            final_task: Task that waits for all middle tasks to complete
        """
        middle_ids = []
        for task in middle_tasks:
            self.then(task)
            middle_ids.append(task.task_id)

        return self.add(final_task, depends_on=middle_ids)

    def conditional_branch(
        self, success_task: Task, failure_task: Task, condition_task_id: TaskId | None = None
    ) -> "TaskGraphBuilder":
        """Add success and failure branches for a task.

        Args:
            success_task: Task to run on success
            failure_task: Task to run on failure
            condition_task_id: Task to branch from (defaults to last added)
        """
        if condition_task_id is None:
            if not self._last_added:
                raise ValueError("No task to branch from")
            condition_task_id = self._last_added[0]

        self.add(success_task, depends_on=[condition_task_id])
        self.on_failure(condition_task_id, failure_task)
        return self

    def build(self) -> TaskGraph:
        """Build and return the final TaskGraph.

        Returns:
            TaskGraph: Constructed graph with all tasks and dependencies

        Raises:
            ValueError: If the graph would contain cycles
        """
        if not self._tasks:
            raise ValueError("Cannot build empty graph. Add at least one task.")

        tg = TaskGraph()

        # Add all tasks with their dependencies
        for task_id, task in self._tasks.items():
            dependencies = list(self._dependencies.get(task_id, set()))
            if dependencies:
                tg.add_task(task, parent_ids=dependencies)
            else:
                tg.add_task(task)

        # Add failure callbacks
        for parent_task_id, callback_tasks in self._failure_callbacks.items():
            for callback_task in callback_tasks:
                tg.add_failure_callback(parent_task_id, callback_task)

        return tg
