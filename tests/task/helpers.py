import pytest
from boilermaker.task import Task, TaskGraph, TaskResult, TaskResultSlim, TaskStatus


def assert_graph_structure(graph: TaskGraph, expected_children: int, expected_edges: int = None):
    """Assert basic graph structure properties."""
    assert len(graph.children) == expected_children
    if expected_edges is not None:
        assert len(graph.edges) == expected_edges


def assert_task_in_graph(graph: TaskGraph, task: Task):
    """Assert that a task is properly added to the graph."""
    assert task.task_id in graph.children
    assert graph.children[task.task_id] is task
    assert task.graph_id == graph.graph_id


def assert_dependency_exists(graph: TaskGraph, parent_task: Task, child_task: Task):
    """Assert that a dependency relationship exists between two tasks."""
    assert parent_task.task_id in graph.edges
    assert child_task.task_id in graph.edges[parent_task.task_id]


def assert_ready_tasks(graph: TaskGraph, expected_task_ids: list):
    """Assert which tasks are ready in the graph."""
    ready_tasks = list(graph.generate_ready_tasks())
    ready_ids = [t.task_id for t in ready_tasks]
    assert set(ready_ids) == set(expected_task_ids), f"Expected {expected_task_ids}, got {ready_ids}"


def assert_task_status(graph: TaskGraph, task: Task, expected_status: TaskStatus):
    """Assert that a task has the expected status."""
    assert graph.get_status(task.task_id) == expected_status


def assert_graph_complete(graph: TaskGraph, should_be_complete: bool = True):
    """Assert whether the graph is complete or not."""
    if should_be_complete:
        assert graph.completed_successfully()
    else:
        assert not graph.completed_successfully()


def assert_cycle_detection_error(graph: TaskGraph, task: Task, parent_ids: list):
    """Assert that adding a task with given parents would create a cycle."""

    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task, parent_ids=parent_ids)


def create_mock_task_result(task: Task, status: TaskStatus, result=None, error=None) -> TaskResult:
    """Create a TaskResult for testing purposes."""
    kwargs = {
        "task_id": task.task_id,
        "graph_id": task.graph_id,
        "status": status,
    }
    if result is not None:
        kwargs["result"] = result
    if error is not None:
        kwargs["errors"] = [error] if isinstance(error, str) else error
    return TaskResult(**kwargs)


def create_mock_task_result_slim(task: Task, status: TaskStatus) -> TaskResultSlim:
    """Create a TaskResultSlim for testing purposes."""
    return TaskResultSlim(
        task_id=task.task_id,
        graph_id=task.graph_id,
        status=status,
    )
