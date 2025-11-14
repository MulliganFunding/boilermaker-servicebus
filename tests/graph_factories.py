"""
Test factories for common test patterns.

This module provides factory functions to create common test scenarios,
reducing repetitive setup code and making tests more readable.
"""

from boilermaker.task import Task, TaskGraph, TaskResult, TaskStatus


class GraphBuilder:
    """Builder for creating TaskGraphs with common patterns."""

    def __init__(self):
        self.graph = TaskGraph()
        self.tasks = {}

    def add_task(self, name: str, parents=None) -> Task:
        """Add a task to the graph with optional parent dependencies."""
        t = Task.default(f"func_{name}")
        parent_ids = [self.tasks[p].task_id for p in (parents or [])]
        self.graph.add_task(t, parent_ids=parent_ids)
        self.tasks[name] = t
        return t

    def complete_task(self, name: str, result="success"):
        """Mark a task as completed with given result."""
        task_obj = self.tasks[name]
        task_result = TaskResult(
            task_id=task_obj.task_id,
            graph_id=self.graph.graph_id,
            status=TaskStatus.Success,
            result=result
        )
        self.graph.add_result(task_result)
        return task_result

    def fail_task(self, name: str, error="test error"):
        """Mark a task as failed."""
        task_obj = self.tasks[name]
        task_result = TaskResult(
            task_id=task_obj.task_id,
            graph_id=self.graph.graph_id,
            status=TaskStatus.Failure,
            errors=[error]
        )
        self.graph.add_result(task_result)
        return task_result


def simple_graph() -> tuple[TaskGraph, Task]:
    """Create a simple graph with one task."""
    builder = GraphBuilder()
    task1 = builder.add_task("single")
    return builder.graph, task1


def linear_graph(num_tasks: int = 3) -> tuple[TaskGraph, list[Task]]:
    """Create a linear chain of tasks: t1 -> t2 -> t3 -> ..."""
    builder = GraphBuilder()
    tasks = []

    # First task has no parents
    first_task = builder.add_task("task_0")
    tasks.append(first_task)

    # Each subsequent task depends on the previous one
    for i in range(1, num_tasks):
        task_name = f"task_{i}"
        parent_name = f"task_{i-1}"
        t = builder.add_task(task_name, parents=[parent_name])
        tasks.append(t)

    return builder.graph, tasks


def diamond_graph() -> tuple[TaskGraph, dict[str, Task]]:
    r"""
    Create a diamond dependency pattern:
        t1
       /  \
      t2  t3
       \  /
        t4
    """
    builder = GraphBuilder()

    t1 = builder.add_task("root")
    t2 = builder.add_task("left", parents=["root"])
    t3 = builder.add_task("right", parents=["root"])
    t4 = builder.add_task("merge", parents=["left", "right"])

    return builder.graph, {
        "root": t1,
        "left": t2,
        "right": t3,
        "merge": t4
    }


def parallel_graph(num_parallel: int = 3) -> tuple[TaskGraph, Task, list[Task]]:
    """Create a graph with one root and multiple parallel branches."""
    builder = GraphBuilder()

    root = builder.add_task("root")
    parallel_tasks = []

    for i in range(num_parallel):
        t = builder.add_task(f"parallel_{i}", parents=["root"])
        parallel_tasks.append(t)

    return builder.graph, root, parallel_tasks


def complex_graph() -> tuple[TaskGraph, dict[str, Task]]:
    r"""
    Create a more complex graph:
         A
        /|\
       B C D
      /| |\
     E F G H
      \|/|/
       I J
        \|/
         K
    """
    builder = GraphBuilder()

    # Level 1
    builder.add_task("A")

    # Level 2
    builder.add_task("B", parents=["A"])
    builder.add_task("C", parents=["A"])
    builder.add_task("D", parents=["A"])

    # Level 3
    builder.add_task("E", parents=["B"])
    builder.add_task("F", parents=["B"])
    builder.add_task("G", parents=["C"])
    builder.add_task("H", parents=["D"])

    # Level 4 - convergence
    builder.add_task("I", parents=["E", "F", "G"])
    builder.add_task("J", parents=["G", "H"])

    # Level 5 - final convergence
    builder.add_task("K", parents=["I", "J"])

    return builder.graph, builder.tasks


def ready_task_scenario() -> tuple[TaskGraph, dict[str, Task], callable]:
    """Create a scenario for testing ready task detection."""
    graph, tasks = diamond_graph()

    def assert_ready_tasks(expected_names: list[str]):
        """Helper to assert which tasks are ready."""
        ready = list(graph.generate_ready_tasks())
        ready_names = [next(name for name, t in tasks.items()
                           if t.task_id == r.task_id) for r in ready]
        assert set(ready_names) == set(expected_names), f"Expected {expected_names}, got {ready_names}"

    return graph, tasks, assert_ready_tasks
