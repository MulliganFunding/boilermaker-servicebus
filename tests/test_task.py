import pytest
from boilermaker import retries, task


def test_can_retry():
    atask = task.Task.default("somefunc")
    assert atask.policy.max_tries > 1
    for _ in range(atask.policy.max_tries):
        atask.record_attempt()
        assert atask.can_retry

    # can no longer retry
    atask.record_attempt()
    assert not atask.can_retry


def test_get_next_delay():
    atask = task.Task.default("somefunc")
    first_delay = atask.get_next_delay()
    atask.record_attempt()
    second_delay = atask.get_next_delay()

    assert first_delay <= atask.policy.delay_max
    assert second_delay <= atask.policy.delay_max
    assert second_delay >= first_delay


def test_rightwise_bitshift_on_failure():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    result = t1 >> t2
    assert t1.on_success is t2
    assert result is t2


def test_leftwise_bitshift_on_failure():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    result = t1 << t2
    assert t2.on_success is t1
    assert result is t1


async def sample_task(state, number1, number2: int = 4):
    if hasattr(state, "sample_task_called"):
        state.sample_task_called += number1
    return number1 + number2


def test_task_signature():
    t = task.Task.si(sample_task, 3, number2=5)
    assert t.function_name == "sample_task"
    assert t.payload == {"args": (3,), "kwargs": {"number2": 5}}


# Additional Task tests


def test_task_default_creation():
    t = task.Task.default("test_func")
    assert t.function_name == "test_func"
    assert t.should_dead_letter is True
    assert t.acks_late is True
    assert t.payload == {}
    assert t.attempts.attempts == 0
    assert t.policy.max_tries > 1
    assert t.task_id is not None
    assert t.graph_id is None
    assert t.on_success is None
    assert t.on_failure is None


def test_task_si_with_custom_policy():
    custom_policy = retries.RetryPolicy(max_tries=10, delay=30)
    t = task.Task.si(sample_task, 1, 2, arg3="test", policy=custom_policy)
    assert t.function_name == "sample_task"
    assert t.payload == {"args": (1, 2), "kwargs": {"arg3": "test"}}
    assert t.policy.max_tries == 10
    assert t.policy.delay == 30


def test_task_si_with_flags():
    t = task.Task.si(sample_task, should_dead_letter=False, acks_late=False)
    assert t.should_dead_letter is False
    assert t.acks_late is False
    assert t.acks_early is True


def test_task_hash():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func1")

    # Hash should be based on task_id
    assert hash(t1) == hash(t1.task_id)
    assert hash(t1) != hash(t2)
    assert hash(t1) != hash(t3)  # Different task_ids even with same function


def test_task_properties():
    t = task.Task.default("test_func")

    # Test acks_early property
    assert t.acks_early is False  # Default acks_late is True
    t.acks_late = False
    assert t.acks_early is True


def test_task_message_properties():
    from unittest.mock import Mock

    t = task.Task.default("test_func")

    # Initially no message
    assert t.msg is None
    assert t.sequence_number is None
    assert t.diagnostic_id is None

    # Set a mock message using the setter
    mock_msg = Mock()
    mock_msg.sequence_number = 12345
    t.msg = mock_msg  # Use the setter which sets _msg

    assert t.msg is mock_msg
    # Clear the cached property to make it re-evaluate
    if hasattr(t, "__dict__") and "sequence_number" in t.__dict__:
        del t.__dict__["sequence_number"]
    assert t.sequence_number == 12345


def test_task_record_attempt():
    t = task.Task.default("test_func")
    initial_attempts = t.attempts.attempts

    result = t.record_attempt()

    assert t.attempts.attempts == initial_attempts + 1
    assert result == initial_attempts + 1  # inc() returns the new attempt count


def test_task_chaining_operations():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func3")

    # Test right-shift chaining (>>)
    result = t1 >> t2
    assert t1.on_success is t2
    assert result is t2

    # Test chaining multiple tasks
    t1 >> t2 >> t3
    assert t1.on_success is t2
    assert t2.on_success is t3

    # Test left-shift chaining (<<)
    t4 = task.Task.default("func4")
    t5 = task.Task.default("func5")
    result = t4 << t5
    assert t5.on_success is t4
    assert result is t4


def test_task_serialization():
    t = task.Task.default("test_func")
    t.payload = {"key": "value"}  # Set payload after creation

    # Should be serializable to dict
    task_dict = t.model_dump()
    assert task_dict["function_name"] == "test_func"
    assert task_dict["payload"] == {"key": "value"}

    # Should be deserializable from dict
    t2 = task.Task.model_validate(task_dict)
    assert t2.function_name == "test_func"
    assert t2.payload == {"key": "value"}
    assert t2.task_id == t.task_id


# TaskStatus, TaskResult, and TaskResultSlim tests


def test_task_status_enum():
    assert task.TaskStatus.Pending == "pending"
    assert task.TaskStatus.Started == "started"
    assert task.TaskStatus.Success == "success"
    assert task.TaskStatus.Failure == "failure"
    assert task.TaskStatus.Retry == "retry"
    assert task.TaskStatus.Deadlettered == "deadlettered"
    assert task.TaskStatus.Abandoned == "abandoned"


def test_task_result_slim():
    task_id = task.TaskId("test-task-id")
    graph_id = task.GraphId("test-graph-id")

    result = task.TaskResultSlim(
        task_id=task_id, graph_id=graph_id, status=task.TaskStatus.Success
    )

    assert result.task_id == task_id
    assert result.graph_id == graph_id
    assert result.status == task.TaskStatus.Success


def test_task_result_slim_paths():
    from pathlib import Path

    task_id = task.TaskId("test-task-id")
    graph_id = task.GraphId("test-graph-id")

    # With graph_id
    result = task.TaskResultSlim(
        task_id=task_id, graph_id=graph_id, status=task.TaskStatus.Success
    )

    assert result.directory_path() == Path(graph_id)
    assert result.storage_path() == Path(graph_id) / "test-task-id.json"

    # Without graph_id
    result_no_graph = task.TaskResultSlim(
        task_id=task_id, status=task.TaskStatus.Success
    )

    assert result_no_graph.directory_path() == Path(task_id)
    assert result_no_graph.storage_path() == Path(task_id) / "test-task-id.json"


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


# TaskGraph tests


def test_task_graph_creation():
    graph = task.TaskGraph()

    assert graph.graph_id is not None
    assert isinstance(graph.children, dict)
    assert isinstance(graph.edges, dict)
    assert isinstance(graph.results, dict)
    assert len(graph.children) == 0
    assert len(graph.edges) == 0
    assert len(graph.results) == 0


def test_task_graph_add_task():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")

    graph.add_task(t1)

    assert t1.task_id in graph.children
    assert graph.children[t1.task_id] is t1
    assert t1.graph_id == graph.graph_id


def test_task_graph_add_task_with_parent():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")

    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)

    assert t1.task_id in graph.children
    assert t2.task_id in graph.children
    assert t1.task_id in graph.edges
    assert t2.task_id in graph.edges[t1.task_id]
    assert t2.graph_id == graph.graph_id


def test_task_graph_start_task():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    result = graph.start_task(t1.task_id)

    assert isinstance(result, task.TaskResultSlim)
    assert result.task_id == t1.task_id
    assert result.graph_id == graph.graph_id
    assert result.status == task.TaskStatus.Started
    assert graph.results[t1.task_id] is result


def test_task_graph_add_result():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    result = task.TaskResult(
        task_id=t1.task_id,
        graph_id=graph.graph_id,
        status=task.TaskStatus.Success,
        result="completed",
    )

    returned_result = graph.add_result(result)

    assert returned_result is result
    assert graph.results[t1.task_id] is result


def test_task_graph_add_result_invalid_task():
    graph = task.TaskGraph()
    invalid_task_id = task.TaskId("nonexistent")

    result = task.TaskResult(task_id=invalid_task_id, status=task.TaskStatus.Success)

    try:
        graph.add_result(result)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "not found in graph" in str(e)


def test_task_graph_all_antecedents_succeeded():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func3")

    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)
    graph.add_task(t3, parent_id=t2.task_id)

    # t1 has no antecedents, should be ready
    assert graph.all_antecedents_succeeded(t1.task_id) is True

    # t2 has t1 as antecedent, but t1 hasn't succeeded yet
    assert graph.all_antecedents_succeeded(t2.task_id) is False

    # Mark t1 as successful
    graph.add_result(
        task.TaskResult(
            task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
        )
    )

    # Now t2 should be ready
    assert graph.all_antecedents_succeeded(t2.task_id) is True

    # t3 still not ready (t2 not succeeded)
    assert graph.all_antecedents_succeeded(t3.task_id) is False


def test_task_graph_task_is_ready():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # Task with no antecedents should be ready
    assert graph.task_is_ready(t1.task_id) is True


def test_task_graph_ready_tasks():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func3")

    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)
    graph.add_task(t3)  # Independent task

    ready_tasks = list(graph.ready_tasks())
    ready_task_ids = [t.task_id for t in ready_tasks]

    # t1 and t3 should be ready (no antecedents)
    assert t1.task_id in ready_task_ids
    assert t3.task_id in ready_task_ids
    assert t2.task_id not in ready_task_ids

    # Mark t1 as successful
    graph.add_result(
        task.TaskResult(
            task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
        )
    )

    # Now t2 should also be ready
    ready_tasks = list(graph.ready_tasks())
    ready_task_ids = [t.task_id for t in ready_tasks]
    assert t2.task_id in ready_task_ids
    assert t3.task_id in ready_task_ids


def test_task_graph_ready_tasks_excludes_started():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # Initially ready
    ready_tasks = list(graph.ready_tasks())
    assert len(ready_tasks) == 1

    # Mark as started
    graph.start_task(t1.task_id)

    # Should no longer be ready
    ready_tasks = list(graph.ready_tasks())
    assert len(ready_tasks) == 0


def test_task_graph_get_result_and_status():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # No result initially
    assert graph.get_result(t1.task_id) is None
    assert graph.get_status(t1.task_id) is None

    # Add result
    result = task.TaskResult(
        task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )
    graph.add_result(result)

    assert graph.get_result(t1.task_id) is result
    assert graph.get_status(t1.task_id) == task.TaskStatus.Success


def test_task_graph_storage_path():
    from pathlib import Path

    graph = task.TaskGraph()
    expected_path = Path(graph.graph_id) / task.TaskGraph.StorageName
    assert graph.storage_path == expected_path

    # Test class method
    graph_id = task.GraphId("test-graph-id")
    expected_path = Path(graph_id) / task.TaskGraph.StorageName
    assert task.TaskGraph.graph_path(graph_id) == expected_path


def test_task_graph_complex_dependency():
    """Test a more complex graph with multiple dependencies."""
    graph = task.TaskGraph()

    # Create a diamond dependency pattern:
    #    t1
    #   /  \
    #  t2  t3
    #   \  /
    #    t4

    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func3")
    t4 = task.Task.default("func4")

    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)
    graph.add_task(t3, parent_id=t1.task_id)
    graph.add_task(t4, parent_id=t2.task_id)

    # Add t4 as child of t3 as well (multiple parents)
    graph.edges[t3.task_id].add(t4.task_id)

    # Initially only t1 should be ready
    ready_tasks = list(graph.ready_tasks())
    assert len(ready_tasks) == 1
    assert ready_tasks[0].task_id == t1.task_id

    # Complete t1
    graph.add_result(
        task.TaskResult(
            task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
        )
    )

    # Now t2 and t3 should be ready
    ready_tasks = list(graph.ready_tasks())
    ready_task_ids = [t.task_id for t in ready_tasks]
    assert len(ready_tasks) == 2
    assert t2.task_id in ready_task_ids
    assert t3.task_id in ready_task_ids
    assert t4.task_id not in ready_task_ids  # Still needs both t2 and t3

    # Complete t2 only
    graph.add_result(
        task.TaskResult(
            task_id=t2.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
        )
    )

    # t4 still not ready (needs t3 too)
    ready_tasks = list(graph.ready_tasks())
    ready_task_ids = [t.task_id for t in ready_tasks]
    assert t4.task_id not in ready_task_ids

    # Complete t3
    graph.add_result(
        task.TaskResult(
            task_id=t3.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
        )
    )

    # Now t4 should be ready
    ready_tasks = list(graph.ready_tasks())
    assert len(ready_tasks) == 1
    assert ready_tasks[0].task_id == t4.task_id


def test_task_graph_cycle_detection_simple():
    """Test that simple cycles are detected and rejected."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")

    # Add tasks: t1 -> t2
    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)

    # Try to create cycle by manually adding edge t2 -> t1, then adding a task
    # This simulates creating a cycle in the graph
    graph.edges[t2.task_id] = {t1.task_id}

    # Now any add_task operation should detect the cycle
    t3 = task.Task.default("task3")
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_task(t3, parent_id=t1.task_id)


def test_task_graph_cycle_detection_complex():
    """Test that complex cycles (A->B->C->A) are detected."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")
    t3 = task.Task.default("task3")

    # Build: t1 -> t2 -> t3
    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)
    graph.add_task(t3, parent_id=t2.task_id)

    # Manually create cycle: t3 -> t1 (completing the cycle t1->t2->t3->t1)
    graph.edges[t3.task_id] = {t1.task_id}

    # Now any add_task should detect the cycle
    t4 = task.Task.default("task4")
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_task(t4, parent_id=t1.task_id)


def test_task_graph_cycle_detection_self_loop():
    """Test that self-loops are detected."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")

    graph.add_task(t1)

    # Manually create self-loop
    graph.edges[t1.task_id] = {t1.task_id}

    # Now any add_task should detect the self-loop cycle
    t2 = task.Task.default("task2")
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_task(t2, parent_id=t1.task_id)


def test_task_graph_no_false_positive_cycles():
    """Test that valid DAG structures don't trigger false positive cycle detection."""
    graph = task.TaskGraph()

    # Create diamond pattern: t1 -> (t2, t3) -> t4
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")
    t3 = task.Task.default("task3")
    t4 = task.Task.default("task4")

    # This should all work without raising cycle detection errors
    graph.add_task(t1)
    graph.add_task(t2, parent_id=t1.task_id)
    graph.add_task(t3, parent_id=t1.task_id)
    graph.add_task(t4, parent_id=t2.task_id)

    # Add t4 as child of t3 as well (multiple parents, but no cycle)
    graph.edges[t3.task_id].add(t4.task_id)

    # Verify the structure is correct
    assert t2.task_id in graph.edges[t1.task_id]
    assert t3.task_id in graph.edges[t1.task_id]
    assert t4.task_id in graph.edges[t2.task_id]
    assert t4.task_id in graph.edges[t3.task_id]

    # Should be able to add more tasks without cycle detection issues
    t5 = task.Task.default("task5")
    graph.add_task(t5, parent_id=t4.task_id)
    assert t5.task_id in graph.edges[t4.task_id]
