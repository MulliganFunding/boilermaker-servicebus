from boilermaker.task import Task, TaskResult, TaskStatus


def verify_storage_started_and_get_result_calls(mock_storage, task: Task) -> TaskResult:
    """
    Helper to verify that store_task_result was called for Started
    and then get the stored result.

    The asserts in here had been duplicated for all of our tests, so
    pulled them into this helper.
    """
    # Verify task started was stored
    assert mock_storage.store_task_result.call_count == 2  # Started + Success
    started_call, result_call = mock_storage.store_task_result.mock_calls
    assert isinstance(started_call.args[0], TaskResult)
    started_result = started_call.args[0]
    assert started_result.task_id == task.task_id
    assert started_result.graph_id == task.graph_id
    assert started_result.status == TaskStatus.Started

    # Verify task result was stored
    assert isinstance(result_call.args[0], TaskResult)
    stored_result = result_call.args[0]
    assert stored_result.task_id == task.task_id
    assert stored_result.graph_id == task.graph_id
    return stored_result
