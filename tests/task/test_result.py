from pathlib import Path

from boilermaker import task


def test_task_status_enum():
    assert task.TaskStatus.Pending == "pending"
    assert task.TaskStatus.Started == "started"
    assert task.TaskStatus.Success == "success"
    assert task.TaskStatus.Failure == "failure"
    assert task.TaskStatus.Retry == "retry"
    assert task.TaskStatus.Deadlettered == "deadlettered"


def test_task_result_slim():
    task_id = task.TaskId("test-task-id")
    graph_id = task.GraphId("test-graph-id")

    result = task.TaskResultSlim(task_id=task_id, graph_id=graph_id, status=task.TaskStatus.Success)

    assert result.task_id == task_id
    assert result.graph_id == graph_id
    assert result.status == task.TaskStatus.Success


def test_task_result_slim_paths():
    task_id = task.TaskId("test-task-id")
    graph_id = task.GraphId("test-graph-id")

    # With graph_id
    result = task.TaskResultSlim(task_id=task_id, graph_id=graph_id, status=task.TaskStatus.Success)

    assert result.directory_path == Path(graph_id)
    assert result.storage_path == Path(graph_id) / "test-task-id.json"

    # Without graph_id
    result_no_graph = task.TaskResultSlim(
        task_id=task_id, status=task.TaskStatus.Success
    )

    assert result_no_graph.directory_path == Path(task_id)
    assert result_no_graph.storage_path == Path(task_id) / "test-task-id.json"


def test_task_result():
    task_id = task.TaskId("test-task-id")

    result = task.TaskResult(
        task_id=task_id,
        status=task.TaskStatus.Success,
        result={"output": "test"},
        errors=["warning: something"],
        formatted_exception="ValueError: test error",
    )

    assert result.task_id == task_id
    assert result.status == task.TaskStatus.Success
    assert result.result == {"output": "test"}
    assert result.errors == ["warning: something"]
    assert result.formatted_exception == "ValueError: test error"


def test_task_status_default():
    """Test TaskStatus.default() class method returns Pending (line 326)."""
    default_status = task.TaskStatus.default()
    assert default_status == task.TaskStatus.Pending


def test_task_result_slim_default():
    """Test TaskResultSlim.default() class method creation (line 356)."""
    task_id = task.TaskId("test-task-id")
    graph_id = task.GraphId("test-graph-id")

    # Test with graph_id
    result = task.TaskResultSlim.default(task_id=task_id, graph_id=graph_id)
    assert result.task_id == task_id
    assert result.graph_id == graph_id
    assert result.status == task.TaskStatus.Pending

    # Test without graph_id
    result_no_graph = task.TaskResultSlim.default(task_id=task_id)
    assert result_no_graph.task_id == task_id
    assert result_no_graph.graph_id is None
    assert result_no_graph.status == task.TaskStatus.Pending


def test_task_status_new_properties():
    """Test the new TaskStatus properties for failure handling."""
    # Test succeeded property
    assert task.TaskStatus.Success.succeeded is True
    assert task.TaskStatus.Failure.succeeded is False
    assert task.TaskStatus.Pending.succeeded is False

    # Test finished property
    assert task.TaskStatus.Success.finished is True
    assert task.TaskStatus.Failure.finished is True
    assert task.TaskStatus.RetriesExhausted.finished is True
    assert task.TaskStatus.Deadlettered.finished is True
    assert task.TaskStatus.Pending.finished is False
    assert task.TaskStatus.Started.finished is False

    # Test failed property
    assert task.TaskStatus.Failure.failed is True
    assert task.TaskStatus.RetriesExhausted.failed is True
    assert task.TaskStatus.Deadlettered.failed is True
    assert task.TaskStatus.Success.failed is False
    assert task.TaskStatus.Pending.failed is False

    # Test class methods
    failure_types = task.TaskStatus.failure_types()
    expected_failure_types = {
        task.TaskStatus.Failure,
        task.TaskStatus.RetriesExhausted,
        task.TaskStatus.Deadlettered,
    }
    assert failure_types == expected_failure_types

    finished_types = task.TaskStatus.finished_types()
    expected_finished_types = {
        task.TaskStatus.Success,
        task.TaskStatus.Failure,
        task.TaskStatus.RetriesExhausted,
        task.TaskStatus.Deadlettered,
    }
    assert finished_types == expected_finished_types


def test_task_result_slim_new_properties():
    """Test the new properties added to TaskResultSlim."""
    task_id = task.TaskId("test-task")

    # Test finished property
    result_success = task.TaskResultSlim(task_id=task_id, status=task.TaskStatus.Success)
    assert result_success.finished is True
    assert result_success.succeeded is True
    assert result_success.failed is False

    result_failure = task.TaskResultSlim(task_id=task_id, status=task.TaskStatus.Failure)
    assert result_failure.finished is True
    assert result_failure.succeeded is False
    assert result_failure.failed is True

    result_pending = task.TaskResultSlim(task_id=task_id, status=task.TaskStatus.Pending)
    assert result_pending.finished is False
    assert result_pending.succeeded is False
    assert result_pending.failed is False
