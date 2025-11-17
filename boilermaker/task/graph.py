import itertools
import typing
from collections import defaultdict
from collections.abc import Generator
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from .result import TaskResult, TaskResultSlim, TaskStatus
from .task import Task
from .task_id import GraphId, ident_field, TaskId


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
        - Evaluation -> TaskResult stored to storage.
        - Graph loaded from storage (includes latest TaskResultSlim instances).
        - Check which tasks are ready.
        - Publish ready tasks.

    Attributes:

        root_id: Unique identifier for the root of the DAG
        children: Mapping of task IDs to Task instances
        edges: Mapping of parent task IDs to lists of child task IDs
        # On write -> TaskResult; on read -> TaskResultSlim
        results: Mapping of task IDs to their TaskResult or TaskResultSlim
    """

    StorageName: typing.ClassVar[str] = "graph.json"

    # The graph has an ID
    graph_id: GraphId = ident_field()
    # Children is a mapping of task IDs to tasks
    children: dict[TaskId, Task] = Field(default_factory=dict)
    # Failure children is a mapping of task IDs to tasks
    fail_children: dict[TaskId, Task] = Field(default_factory=dict)
    # Edges is a mapping of parent task IDs to sets of child task IDs
    edges: dict[TaskId, set[TaskId]] = Field(default_factory=lambda: defaultdict(set[TaskId]))
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
                raise ValueError(f"Adding task {task.task_id} with parent {parent_id} would create a cycle in the DAG")

        # If we leave `on_success` and `on_failure` it's potentially confusing for both callers
        # and our own evaluation. It also has the potential to create cycles inadvertently, so we
        # dynamically add on_success and on_failure into edges here and remove these as individual task callbacks.
        if task.on_success:
            self.add_task(task.on_success, parent_ids=[task.task_id])
            task.on_success = None  # Clear to avoid duplication

        if task.on_failure:
            self.add_failure_callback(task.task_id, task.on_failure)
            task.on_failure = None

    def add_failure_callback(self, parent_id: TaskId, callback_task: Task) -> None:
        """Add an failure callback task to the graph.

        Args:

            parent_id: The task ID that will trigger the error callback
            callback_task: The Task instance to add as an error callback

        Raises:

            ValueError: If adding this callback would create a cycle in the DAG
        """

        callback_task.graph_id = self.graph_id

        if callback_task.task_id not in self.fail_children:
            self.fail_children[callback_task.task_id] = callback_task

        if parent_id not in self.fail_edges:
            self.fail_edges[parent_id] = set()
        self.fail_edges[parent_id].add(callback_task.task_id)

        # Check for cycles after adding the edge
        if self._detect_cycles():
            # Rollback the changes
            self.fail_edges[parent_id].remove(callback_task.task_id)
            if not self.fail_edges[parent_id]:  # Remove empty set
                del self.fail_edges[parent_id]
            del self.fail_children[callback_task.task_id]
            raise ValueError(f"Adding failure callback for task {parent_id} would create a cycle in the DAG")

        # handle recursion
        if callback_task.on_success:
            self.add_task(callback_task.on_success, parent_ids=[callback_task.task_id])
            callback_task.on_success = None

        if callback_task.on_failure:
            self.add_failure_callback(callback_task.task_id, callback_task.on_failure)
            callback_task.on_failure = None

    def schedule_task(self, task_id: TaskId) -> TaskResult | TaskResultSlim:
        """Mark a task as scheduled to prevent double-scheduling."""
        if not (task_id in self.children or task_id in self.fail_children):
            raise ValueError(f"Task {task_id} not found in graph")
        if task_id not in self.results or self.results[task_id].status != TaskStatus.Pending:
            raise ValueError(f"Task {task_id} is not pending and cannot be scheduled")

        result = self.results[task_id]
        result.status = TaskStatus.Scheduled
        self.results[result.task_id] = result
        return result

    def add_result(self, result: TaskResult) -> TaskResult:
        """Mark a task as completed with result."""
        if result.task_id not in self.children and result.task_id not in self.fail_children:
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
                if not self.results[parent_id].succeeded:
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

    def generate_pending_results(self) -> Generator[TaskResultSlim]:
        """
        Generate pending TaskResultSlim entries for all tasks.

        This should probably only be run when the graph is first created and stored
        """
        for task_id in itertools.chain.from_iterable((self.children.keys(), self.fail_children.keys())):
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

    def generate_ready_tasks(self) -> Generator[Task]:
        """Get a list of tasks that are ready to be executed (not started and all antecedents succeeded)."""
        for task_id in self.children.keys():
            # Task is ready if:
            # 1. It has no result yet (never started) OR it has Pending status
            # 2. All its antecedents have succeeded
            task_result = self.results.get(task_id)
            is_not_started = task_result is None or task_result.status == TaskStatus.Pending
            if is_not_started and self.task_is_ready(task_id):
                yield self.children[task_id]

    def generate_failure_ready_tasks(self) -> Generator[Task]:
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
            for parent_id in triggering_parents:
                # Check if parent has failed status in results
                if parent_id in self.results and self.results[parent_id].status.failed:
                    yield self.fail_children[task_id]

    def get_result(self, task_id: TaskId) -> TaskResultSlim | TaskResult | None:
        """Get the result of a completed task."""
        return self.results.get(task_id)

    def get_status(self, task_id: TaskId) -> TaskStatus | None:
        """Check if a task is completed."""
        if tr := self.get_result(task_id):
            return tr.status
        return None

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
        """Check if the graph has finished executing (reached a terminal state).

        A graph is complete when:
        1. All main tasks (children) are in finished states, AND
        2. All reachable failure callback tasks are in finished states

        We don't expect ALL failure children to be invoked, only those reachable
        from actual failures. The graph is in a terminal state when we've processed
        as far as we can in both the main graph and the failure graph.
        """
        # If there are no main tasks, the graph is not complete
        if not self.children:
            return False

        finished_statuses = TaskStatus.finished_types()

        # Check that all main tasks are either finished OR cannot run due to failed dependencies
        for task_id in self.children.keys():
            status = self.get_status(task_id)
            if status is None:
                # Task hasn't run - check if it CAN run (all antecedents finished successfully)
                # If it can't run due to failed dependencies, that's OK for completion
                if self.all_antecedents_finished(task_id) and not self.all_antecedents_succeeded(task_id):
                    # All antecedents finished but at least one failed - this task will never run
                    continue
                else:
                    # Task could still run but hasn't - graph not complete
                    return False
            elif status not in finished_statuses:
                return False

        # Find all reachable failure callback tasks
        reachable_failure_tasks = self._get_reachable_failure_tasks()

        # Check that all reachable failure tasks are also finished
        for task_id in reachable_failure_tasks:
            status = self.get_status(task_id)
            if status is None or status not in finished_statuses:
                return False

        return True

    def _get_reachable_failure_tasks(self) -> set[TaskId]:
        """Get all failure callback tasks that are reachable from actual failures.

        A failure task is reachable if:
        1. At least one of its triggering parent tasks has failed, AND
        2. It can be reached through the failure callback chain
        """
        reachable: set[TaskId] = set()
        to_visit = []

        # Find initial failure tasks triggered by failed main tasks
        for parent_id, fail_child_ids in self.fail_edges.items():
            parent_status = self.get_status(parent_id)
            if parent_status is not None and parent_status.failed:
                for child_id in fail_child_ids:
                    if child_id not in reachable:
                        reachable.add(child_id)
                        to_visit.append(child_id)

        # Follow the failure callback chain to find all reachable tasks
        while to_visit:
            current_task_id = to_visit.pop()

            # Check for failure callbacks from this failure task
            if current_task_id in self.fail_edges:
                for child_id in self.fail_edges[current_task_id]:
                    if child_id not in reachable:
                        reachable.add(child_id)
                        to_visit.append(child_id)

            # Also check for success callbacks from this failure task
            # (failure tasks can have success callbacks too)
            if current_task_id in self.edges:
                current_status = self.get_status(current_task_id)
                if current_status is not None and current_status.succeeded:
                    for child_id in self.edges[current_task_id]:
                        if child_id not in reachable:
                            reachable.add(child_id)
                            to_visit.append(child_id)

        return reachable


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
        self._failure_callbacks: dict[TaskId, set[Task]] = {}
        self._last_added: list[TaskId] = []  # Track recently added tasks for chaining

    def add(self, task: Task, depends_on: list[TaskId] | None = None) -> "TaskGraphBuilder":
        """Add a task with optional explicit dependencies.

        Args:
            task: Task to add
            depends_on: Optional list of task IDs this task depends on (will run after self._last_added if None)

        Returns:
            Self for method chaining
        """
        self._tasks[task.task_id] = task
        self._dependencies[task.task_id] = set(depends_on or self._last_added)
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
        parents = depends_on or self._last_added
        for task in tasks:
            self._tasks[task.task_id] = task
            self._dependencies[task.task_id] = set(parents)

        self._last_added = [t.task_id for t in tasks]
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
            self._failure_callbacks[parent_task_id] = set()

        self._failure_callbacks[parent_task_id].add(callback_task)
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

    def add_success_fail_branch(
        self,
        branching_task_id: TaskId,
        success_task: Task,
        failure_task: Task,
    ) -> "TaskGraphBuilder":
        """Add success and failure branches for a task.

        Args:
            branching_task_id: TaskId to branch from
            success_task: Task to run on success
            failure_task: Task to run on failure
        """
        self.add(success_task, depends_on=[branching_task_id])
        self.on_failure(branching_task_id, failure_task)
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
