"""Tests for TaskGraph.detect_stalled_tasks method."""

from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus


def _make_graph_with_result(status: TaskStatus) -> tuple[TaskGraph, Task]:
    """Create a graph with one task that has a result in the given status."""
    graph = TaskGraph()
    t = Task.default("my_func")
    graph.add_task(t)
    graph.results[t.task_id] = TaskResultSlim(
        task_id=t.task_id,
        graph_id=graph.graph_id,
        status=status,
    )
    return graph, t


def test_detect_stalled_tasks_returns_empty_when_no_stalled():
    """When all tasks are in terminal or pending status, no stalled tasks are returned."""
    graph = TaskGraph()

    t_pending = Task.default("pending_func")
    graph.add_task(t_pending)
    graph.results[t_pending.task_id] = TaskResultSlim(
        task_id=t_pending.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Pending,
    )

    t_success = Task.default("success_func")
    graph.add_task(t_success)
    graph.results[t_success.task_id] = TaskResultSlim(
        task_id=t_success.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    t_failure = Task.default("failure_func")
    graph.add_task(t_failure)
    graph.results[t_failure.task_id] = TaskResultSlim(
        task_id=t_failure.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Failure,
    )

    assert graph.detect_stalled_tasks() == []


def test_detect_stalled_tasks_detects_scheduled():
    """A task in Scheduled status is detected as stalled."""
    graph, t = _make_graph_with_result(TaskStatus.Scheduled)

    stalled = graph.detect_stalled_tasks()

    assert len(stalled) == 1
    task_id, function_name, status = stalled[0]
    assert task_id == t.task_id
    assert function_name == t.function_name
    assert status == TaskStatus.Scheduled


def test_detect_stalled_tasks_detects_started():
    """A task in Started status is detected as stalled."""
    graph, t = _make_graph_with_result(TaskStatus.Started)

    stalled = graph.detect_stalled_tasks()

    assert len(stalled) == 1
    task_id, function_name, status = stalled[0]
    assert task_id == t.task_id
    assert function_name == t.function_name
    assert status == TaskStatus.Started


def test_detect_stalled_tasks_detects_retry():
    """A task in Retry status is detected as stalled."""
    graph, t = _make_graph_with_result(TaskStatus.Retry)

    stalled = graph.detect_stalled_tasks()

    assert len(stalled) == 1
    task_id, function_name, status = stalled[0]
    assert task_id == t.task_id
    assert function_name == t.function_name
    assert status == TaskStatus.Retry


def test_detect_stalled_tasks_detects_in_fail_children():
    """Stalled tasks in fail_children are also detected."""
    graph = TaskGraph()

    # Add a main task and fail it
    parent = Task.default("parent_func")
    graph.add_task(parent)

    # Add a failure callback task
    callback = Task.default("callback_func")
    graph.add_failure_callback(parent.task_id, callback)

    # Set the callback task's result to Started (stalled)
    graph.results[callback.task_id] = TaskResultSlim(
        task_id=callback.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Started,
    )

    stalled = graph.detect_stalled_tasks()

    assert len(stalled) == 1
    task_id, function_name, status = stalled[0]
    assert task_id == callback.task_id
    assert function_name == callback.function_name
    assert status == TaskStatus.Started


def test_detect_stalled_tasks_mix_of_stalled_and_non_stalled():
    """Only stalled tasks are returned when the graph has a mix of statuses."""
    graph = TaskGraph()

    t_pending = Task.default("pending_func")
    graph.add_task(t_pending)
    graph.results[t_pending.task_id] = TaskResultSlim(
        task_id=t_pending.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Pending,
    )

    t_success = Task.default("done_func")
    graph.add_task(t_success)
    graph.results[t_success.task_id] = TaskResultSlim(
        task_id=t_success.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Success,
    )

    t_scheduled = Task.default("scheduled_func")
    graph.add_task(t_scheduled)
    graph.results[t_scheduled.task_id] = TaskResultSlim(
        task_id=t_scheduled.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Scheduled,
    )

    t_retry = Task.default("retry_func")
    graph.add_task(t_retry)
    graph.results[t_retry.task_id] = TaskResultSlim(
        task_id=t_retry.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Retry,
    )

    t_retries_exhausted = Task.default("exhausted_func")
    graph.add_task(t_retries_exhausted)
    graph.results[t_retries_exhausted.task_id] = TaskResultSlim(
        task_id=t_retries_exhausted.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.RetriesExhausted,
    )

    stalled = graph.detect_stalled_tasks()

    stalled_ids = {s[0] for s in stalled}
    assert len(stalled) == 2
    assert t_scheduled.task_id in stalled_ids
    assert t_retry.task_id in stalled_ids
    assert t_pending.task_id not in stalled_ids
    assert t_success.task_id not in stalled_ids
    assert t_retries_exhausted.task_id not in stalled_ids


def test_detect_stalled_tasks_does_not_flag_pending():
    """Pending tasks are not stalled — they are waiting for antecedents."""
    graph, _ = _make_graph_with_result(TaskStatus.Pending)
    assert graph.detect_stalled_tasks() == []


def test_detect_stalled_tasks_does_not_flag_terminal_statuses():
    """Terminal statuses (Success, Failure, RetriesExhausted, Deadlettered) are not stalled."""
    for terminal_status in (
        TaskStatus.Success,
        TaskStatus.Failure,
        TaskStatus.RetriesExhausted,
        TaskStatus.Deadlettered,
    ):
        graph, _ = _make_graph_with_result(terminal_status)
        assert graph.detect_stalled_tasks() == [], (
            f"Expected no stalled tasks for terminal status {terminal_status}"
        )


def test_detect_stalled_tasks_skips_tasks_without_results():
    """Tasks with no result entry in graph.results are not reported as stalled."""
    graph = TaskGraph()
    t = Task.default("no_result_func")
    graph.add_task(t)
    # Intentionally do NOT add a result for this task

    assert graph.detect_stalled_tasks() == []


def test_detect_stalled_tasks_returns_correct_function_names():
    """The returned tuples include the correct function_name for each stalled task."""
    graph = TaskGraph()

    t1 = Task.default("alpha_processor")
    graph.add_task(t1)
    graph.results[t1.task_id] = TaskResultSlim(
        task_id=t1.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Scheduled,
    )

    t2 = Task.default("beta_handler")
    graph.add_task(t2)
    graph.results[t2.task_id] = TaskResultSlim(
        task_id=t2.task_id,
        graph_id=graph.graph_id,
        status=TaskStatus.Started,
    )

    stalled = graph.detect_stalled_tasks()
    stalled_by_id = {s[0]: s for s in stalled}

    assert stalled_by_id[t1.task_id][1] == "alpha_processor"
    assert stalled_by_id[t2.task_id][1] == "beta_handler"
