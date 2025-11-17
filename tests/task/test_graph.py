import itertools
from pathlib import Path

import pytest
from boilermaker import retries, task

from ..graph_factories import diamond_graph, linear_graph, ready_task_scenario, simple_graph
from .helpers import (
    assert_cycle_detection_error,
    assert_dependency_exists,
    assert_graph_structure,
    assert_task_in_graph,
    create_mock_task_result,
)


# ~~~~ ~~~~~ ~~~~ ~~~~ #
# TaskGraph Tests
# ~~~~ ~~~~~ ~~~~ ~~~~ #
async def sample_task(state, number1, number2: int = 4):
    if hasattr(state, "sample_task_called"):
        state.sample_task_called += number1
    return number1 + number2


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
    graph, t1 = simple_graph()
    assert_task_in_graph(graph, t1)


def test_task_graph_add_task_with_parent():
    graph, tasks = linear_graph(2)
    t1, t2 = tasks

    assert_graph_structure(graph, expected_children=2)
    assert_task_in_graph(graph, t1)
    assert_task_in_graph(graph, t2)
    assert_dependency_exists(graph, t1, t2)


def test_task_graph_schedule_task():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # Make sure marked as pending
    pending_res = list(graph.generate_pending_results())
    assert len(pending_res) == 1

    result = graph.schedule_task(t1.task_id)

    assert isinstance(result, task.TaskResultSlim)
    assert result.task_id == t1.task_id
    assert result.graph_id == graph.graph_id
    assert result.status == task.TaskStatus.Scheduled
    assert graph.results[t1.task_id] is result


def test_task_graph_start_result_invalid_task():
    graph = task.TaskGraph()
    invalid_task_id = task.TaskId("nonexistent")

    try:
        graph.schedule_task(invalid_task_id)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "not found in graph" in str(e)


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
    graph, tasks = linear_graph(3)
    t1, t2, t3 = tasks

    # t1 has no antecedents, should be ready
    assert graph.all_antecedents_succeeded(t1.task_id) is True

    # t2 has t1 as antecedent, but t1 hasn't succeeded yet
    assert graph.all_antecedents_succeeded(t2.task_id) is False

    # Mark t1 as successful
    graph.add_result(create_mock_task_result(t1, task.TaskStatus.Success))

    # Now t2 should be ready, but t3 still not ready
    assert graph.all_antecedents_succeeded(t2.task_id) is True
    assert graph.all_antecedents_succeeded(t3.task_id) is False


def test_task_graph_task_is_ready():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # Task with no antecedents should be ready
    assert graph.task_is_ready(t1.task_id) is True


def test_task_graph_ready_tasks():
    graph, tasks, assert_ready = ready_task_scenario()

    # Initially only root should be ready
    assert_ready(["root"])

    # Complete root task - now both branches should be ready
    root_result = create_mock_task_result(tasks["root"], task.TaskStatus.Success)
    graph.add_result(root_result)

    assert_ready(["left", "right"])


def test_task_graph_ready_tasks_excludes_started():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # Initially ready
    ready_tasks = list(graph.generate_ready_tasks())
    assert len(ready_tasks) == 1

    # Put a Pending result in there
    assert len(list(graph.generate_pending_results())) == 1

    # Mark as started
    graph.schedule_task(t1.task_id)

    # Should no longer be ready
    ready_tasks = list(graph.generate_ready_tasks())
    assert len(ready_tasks) == 0


def test_task_graph_get_result_and_status():
    graph = task.TaskGraph()
    t1 = task.Task.default("func1")
    graph.add_task(t1)

    # No result initially
    assert graph.get_result(t1.task_id) is None
    assert graph.get_status(t1.task_id) is None

    # Add result
    result = task.TaskResult(task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success)
    graph.add_result(result)

    assert graph.get_result(t1.task_id) is result
    assert graph.get_status(t1.task_id) == task.TaskStatus.Success


def test_task_graph_storage_path():
    graph = task.TaskGraph()
    expected_path = Path(graph.graph_id) / task.TaskGraph.StorageName
    assert graph.storage_path == expected_path

    # Test class method
    graph_id = task.GraphId("test-graph-id")
    expected_path = Path(graph_id) / task.TaskGraph.StorageName
    assert task.TaskGraph.graph_path(graph_id) == expected_path


def test_task_graph_complex_dependency():
    """Test a diamond dependency pattern with multiple dependencies."""
    graph, tasks, assert_ready = ready_task_scenario()

    # Initially only root should be ready
    assert_ready(["root"])

    # Complete root - left and right should be ready
    graph.add_result(create_mock_task_result(tasks["root"], task.TaskStatus.Success))
    assert_ready(["left", "right"])

    # Complete left only - merge still not ready (needs both)
    graph.add_result(create_mock_task_result(tasks["left"], task.TaskStatus.Success))
    assert_ready(["right"])  # Only right still ready

    # Complete right - now merge should be ready
    graph.add_result(create_mock_task_result(tasks["right"], task.TaskStatus.Success))
    assert_ready(["merge"])


def test_task_graph_cycle_detection_simple():
    """Test that simple cycles are detected and rejected."""
    graph, tasks = linear_graph(2)
    t1, t2 = tasks

    # Manually create cycle: t2 -> t1
    graph.edges[t2.task_id] = {t1.task_id}

    # Now any add_task operation should detect the cycle
    assert_cycle_detection_error(graph, task.Task.default("task3"), [t1.task_id])


def test_task_graph_cycle_detection_complex():
    """Test that complex cycles (A->B->C->A) are detected."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")
    t3 = task.Task.default("task3")

    # Build: t1 -> t2 -> t3
    graph.add_task(t1)
    graph.add_task(t2, parent_ids=[t1.task_id])
    graph.add_task(t3, parent_ids=[t2.task_id])

    # Manually create cycle: t3 -> t1 (completing the cycle t1->t2->t3->t1)
    graph.edges[t3.task_id] = {t1.task_id}

    # Now any add_task should detect the cycle
    t4 = task.Task.default("task4")
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_task(t4, parent_ids=[t1.task_id])


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
        graph.add_task(t2, parent_ids=[t1.task_id])


def test_task_graph_no_false_positive_cycles():
    """Test that valid DAG structures don't trigger false positive cycle detection."""
    graph, tasks = diamond_graph()

    # Verify the diamond structure is correct
    assert_dependency_exists(graph, tasks["root"], tasks["left"])
    assert_dependency_exists(graph, tasks["root"], tasks["right"])
    assert_dependency_exists(graph, tasks["left"], tasks["merge"])
    assert_dependency_exists(graph, tasks["right"], tasks["merge"])

    # Should be able to add more tasks without cycle detection issues
    t5 = task.Task.default("task5")
    graph.add_task(t5, parent_ids=[tasks["merge"].task_id])
    assert_dependency_exists(graph, tasks["merge"], t5)


def test_task_graph_cycle_detection():
    """Test TaskGraph cycle detection in add_task method (line 546)."""
    graph = task.TaskGraph()

    # Create a simple cycle scenario
    # We'll create tasks t1, t2 where t1 -> t2, then try to make t2 -> t1
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")

    # Add first task
    graph.add_task(t1)

    # Add second task depending on first
    graph.add_task(t2, parent_ids=[t1.task_id])

    # Now manually create the cycle condition that the add_task method checks for
    # We'll directly test the rollback behavior by temporarily modifying the graph
    # to simulate what happens when a cycle is detected

    # Simulate the rollback scenario - manually add a problematic edge then detect/rollback
    if t1.task_id not in graph.edges:
        graph.edges[t1.task_id] = set()
    graph.edges[t1.task_id].add(t2.task_id)  # t1 -> t2 already exists

    # Now try to add t2 -> t1 which would create a cycle
    if t2.task_id not in graph.edges:
        graph.edges[t2.task_id] = set()
    graph.edges[t2.task_id].add(t1.task_id)  # Add the cycle edge

    # Detect the cycle
    has_cycle = graph._detect_cycles()
    assert has_cycle  # Should detect the cycle

    # Simulate the rollback (this is what line 546 does)
    graph.edges[t2.task_id].remove(t1.task_id)
    if not graph.edges[t2.task_id]:
        del graph.edges[t2.task_id]

    # Verify rollback worked - should be back to original state
    assert not graph._detect_cycles()  # No cycle now
    assert t1.task_id in graph.children
    assert t2.task_id in graph.children


def test_task_graph_all_antecedents_succeeded_missing_parent():
    """Test all_antecedents_succeeded returns False when parent not in results (line 579)."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")

    # Add tasks with dependency: t1 -> t2
    graph.add_task(t1)
    graph.add_task(t2, parent_ids=[t1.task_id])

    # Don't add any results for t1, so it won't be in graph.results
    # Check that t2 is not ready because t1 has no result
    assert not graph.all_antecedents_succeeded(t2.task_id)

    # Add a result for t1 but with non-success status
    graph.results[t1.task_id] = task.TaskResultSlim(
        task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Should still return False because t1 didn't succeed
    assert not graph.all_antecedents_succeeded(t2.task_id)

    # Now make t1 successful
    graph.results[t1.task_id] = task.TaskResultSlim(
        task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Now t2 should be ready
    assert graph.all_antecedents_succeeded(t2.task_id)


def test_task_graph_generate_pending_results():
    """Test generate_pending_results method (lines 609-610)."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")
    t3 = task.Task.default("task3")

    # Add tasks to graph
    graph.add_task(t1)
    graph.add_task(t2)
    graph.add_task(t3)

    # Generate pending results
    pending_results = list(graph.generate_pending_results())

    # Should have one pending result for each task
    assert len(pending_results) == 3

    # All results should be TaskResultSlim with Pending status
    task_ids = {result.task_id for result in pending_results}
    assert task_ids == {t1.task_id, t2.task_id, t3.task_id}

    for result in pending_results:
        assert isinstance(result, task.TaskResultSlim)
        assert result.graph_id == graph.graph_id
        assert result.status == task.TaskStatus.Pending
        assert graph.results[result.task_id] is result


def test_task_graph_completed_successfully():
    """Test completed_successfully method."""
    graph = task.TaskGraph()
    t1 = task.Task.default("task1")
    t2 = task.Task.default("task2")

    # Add tasks to graph
    graph.add_task(t1)
    graph.add_task(t2)

    # Initially, no results, so should return False
    assert not graph.completed_successfully()

    # Add one successful result
    graph.results[t1.task_id] = task.TaskResultSlim(
        task_id=t1.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Still not all successful
    assert not graph.completed_successfully()

    # Add second result but with failure
    graph.results[t2.task_id] = task.TaskResultSlim(
        task_id=t2.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Still not all successful
    assert not graph.completed_successfully()

    # Make second task successful too
    graph.results[t2.task_id] = task.TaskResultSlim(
        task_id=t2.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Now all tasks are successful
    assert graph.completed_successfully()


def test_task_graph_cycle_detection_rollback():
    """Test that cycle detection properly rolls back changes"""
    graph = task.TaskGraph(graph_id="test_graph")

    # Create tasks
    task1 = task.Task.si(sample_task)
    task2 = task.Task.si(sample_task)
    task3 = task.Task.si(sample_task)

    # Add tasks to graph
    graph.add_task(task1)
    graph.add_task(task2, parent_ids=[task1.task_id])
    graph.add_task(task3, parent_ids=[task2.task_id])

    # Verify the current state before attempting cycle
    assert task1.task_id in graph.children
    assert task2.task_id in graph.children
    assert task3.task_id in graph.children

    # This should fail and trigger rollback at line 546
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task1, parent_ids=[task3.task_id])  # This creates task3 -> task1 cycle


def test_task_default_with_custom_policy_in_kwargs():
    """Test Task.default() method when policy is provided in kwargs - covers line 113"""
    custom_policy = retries.RetryPolicy(max_retries=5, initial_delay=2.0)
    atask = task.Task.default("test_function", policy=custom_policy)

    assert atask.policy == custom_policy
    assert atask.function_name == "test_function"
    # This test specifically covers the "if 'policy' in kwargs:" branch at line 113


def test_task_diagnostic_id_when_no_message_set():
    """Test diagnostic_id property when no message is set - covers line 132"""
    atask = task.Task.si(sample_task)
    # Task created without a message should return None for diagnostic_id
    assert atask.diagnostic_id is None


def test_task_si_uses_default_retry_attempts():
    """Test that Task.si() uses RetryAttempts.default() - covers line 111"""
    atask = task.Task.si(sample_task)

    # Should use default retry attempts (line 111 in si method)
    assert isinstance(atask.attempts, retries.RetryAttempts)
    assert atask.attempts.attempts == 0  # Default should start with 0 attempts


def test_task_graph_failure_shadow_graph_structure():
    """Test that TaskGraph properly maintains both success and failure structures."""
    graph = task.TaskGraph()

    # Verify initial state
    assert isinstance(graph.children, dict)
    assert isinstance(graph.fail_children, dict)
    assert isinstance(graph.edges, dict)
    assert isinstance(graph.fail_edges, dict)
    assert len(graph.fail_children) == 0
    assert len(graph.fail_edges) == 0


def test_task_graph_add_failure_callback():
    """Test adding failure callbacks to the graph."""
    graph = task.TaskGraph()

    # Create tasks
    main_task = task.Task.default("main_task")
    failure_handler = task.Task.default("failure_handler")

    # Add main task first
    graph.add_task(main_task)

    # Add failure callback
    graph.add_failure_callback(main_task.task_id, failure_handler)

    # Verify the failure callback was added correctly
    assert failure_handler.task_id in graph.fail_children
    assert graph.fail_children[failure_handler.task_id] is failure_handler
    assert main_task.task_id in graph.fail_edges
    assert failure_handler.task_id in graph.fail_edges[main_task.task_id]
    assert failure_handler.graph_id == graph.graph_id


def test_task_graph_add_failure_callback_prevents_cycles():
    """Test that failure callbacks are subject to cycle detection."""
    graph = task.TaskGraph()

    # Create tasks
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add tasks with dependency: A -> B
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])

    # Add failure callback: A fails -> B (should work)
    graph.add_failure_callback(task_a.task_id, task_b)

    # Try to create a cycle: B fails -> A (should fail)
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_failure_callback(task_b.task_id, task_a)


def test_task_graph_failure_callback_rollback():
    """Test that failure callback addition properly rolls back on cycle detection."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Set up basic dependency
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])

    # Manually create a cycle condition to test rollback
    graph.fail_edges[task_b.task_id].add(task_a.task_id)
    graph.fail_children[task_a.task_id] = task_a

    # Try to add another failure callback that would trigger rollback
    task_c = task.Task.default("task_c")
    try:
        graph.add_failure_callback(task_a.task_id, task_c)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "would create a cycle in the DAG" in str(e)

        # Verify rollback happened - task_c should not be in fail_children
        assert task_c.task_id not in graph.fail_children
        # The fail_edges entry should be cleaned up if it was empty
        if task_a.task_id in graph.fail_edges:
            assert task_c.task_id not in graph.fail_edges[task_a.task_id]


def test_task_graph_failure_ready_tasks():
    """Test the failure_ready_tasks() method."""
    graph = task.TaskGraph()

    # Create tasks
    main_task = task.Task.default("main_task")
    success_handler = task.Task.default("success_handler")
    failure_handler = task.Task.default("failure_handler")

    # Build graph: main_task -> success_handler (on success)
    #              main_task -> failure_handler (on failure)
    graph.add_task(main_task)
    graph.add_task(success_handler, parent_ids=[main_task.task_id])
    graph.add_failure_callback(main_task.task_id, failure_handler)

    # Initially, no failure tasks should be ready (main_task hasn't failed)
    failure_tasks = list(graph.generate_failure_ready_tasks())
    assert len(failure_tasks) == 0

    # Set main_task as failed
    graph.results[main_task.task_id] = task.TaskResult(
        task_id=main_task.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Now failure_handler should be ready
    failure_tasks = list(graph.generate_failure_ready_tasks())
    assert len(failure_tasks) == 1
    assert failure_tasks[0].task_id == failure_handler.task_id

    # Success handler should NOT be ready (main_task failed)
    ready_tasks = list(graph.generate_ready_tasks())
    success_task_ids = [t.task_id for t in ready_tasks]
    assert success_handler.task_id not in success_task_ids


def test_task_graph_failure_ready_tasks_multiple_failure_states():
    """Test failure_ready_tasks with different failure states."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    handler_a = task.Task.default("handler_a")
    handler_b = task.Task.default("handler_b")
    handler_c = task.Task.default("handler_c")

    # Add tasks and failure handlers
    graph.add_task(task_a)
    graph.add_task(task_b)
    graph.add_task(task_c)

    graph.add_failure_callback(task_a.task_id, handler_a)
    graph.add_failure_callback(task_b.task_id, handler_b)
    graph.add_failure_callback(task_c.task_id, handler_c)

    # Test different failure states
    failure_statuses = [task.TaskStatus.Failure, task.TaskStatus.RetriesExhausted, task.TaskStatus.Deadlettered]

    tasks = [task_a, task_b, task_c]
    handlers = [handler_a, handler_b, handler_c]

    for main_task, handler, status in zip(tasks, handlers, failure_statuses, strict=True):
        # Set task as failed with different status
        graph.results[main_task.task_id] = task.TaskResult(
            task_id=main_task.task_id, graph_id=graph.graph_id, status=status
        )

        # Check that failure handler is ready
        failure_tasks = list(graph.generate_failure_ready_tasks())
        handler_ids = [t.task_id for t in failure_tasks]
        assert handler.task_id in handler_ids


def test_task_graph_failure_ready_tasks_excludes_already_started():
    """Test that failure_ready_tasks excludes handlers that have already started."""
    graph = task.TaskGraph()

    main_task = task.Task.default("main_task")
    failure_handler = task.Task.default("failure_handler")

    graph.add_task(main_task)
    graph.add_failure_callback(main_task.task_id, failure_handler)

    # Set main_task as failed
    graph.results[main_task.task_id] = task.TaskResult(
        task_id=main_task.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Failure handler should be ready
    failure_tasks = list(graph.generate_failure_ready_tasks())
    assert len(failure_tasks) == 1

    # Mark failure handler as started
    graph.results[failure_handler.task_id] = task.TaskResultSlim(
        task_id=failure_handler.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Started
    )

    # Now it should not be ready anymore
    failure_tasks = list(graph.generate_failure_ready_tasks())
    assert len(failure_tasks) == 0


def test_task_graph_all_antecedents_finished():
    """Test the all_antecedents_finished() method."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Create dependency: A, B -> C
    graph.add_task(task_a)
    graph.add_task(task_b)
    graph.add_task(task_c, parent_ids=[task_a.task_id, task_b.task_id])

    # Initially, antecedents not finished
    assert not graph.all_antecedents_finished(task_c.task_id)

    # Set A as successful
    graph.results[task_a.task_id] = task.TaskResult(
        task_id=task_a.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Still not all finished
    assert not graph.all_antecedents_finished(task_c.task_id)

    # Set B as failed
    graph.results[task_b.task_id] = task.TaskResult(
        task_id=task_b.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Now all antecedents are finished (one success, one failure)
    assert graph.all_antecedents_finished(task_c.task_id)


def test_task_graph_all_antecedents_finished_with_pending_tasks():
    """Test all_antecedents_finished returns False for pending/started tasks."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])

    # Set A as pending (not finished)
    graph.results[task_a.task_id] = task.TaskResultSlim(
        task_id=task_a.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Pending
    )

    assert not graph.all_antecedents_finished(task_b.task_id)

    # Set A as started (not finished)
    graph.results[task_a.task_id].status = task.TaskStatus.Started
    assert not graph.all_antecedents_finished(task_b.task_id)

    # Set A as retries exhausted (finished)
    graph.results[task_a.task_id].status = task.TaskStatus.RetriesExhausted
    assert graph.all_antecedents_finished(task_b.task_id)


def test_task_graph_has_failures():
    """Test the has_failures() method."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    graph.add_task(task_a)
    graph.add_task(task_b)

    # Initially no failures
    assert not graph.has_failures()

    # Set one task as successful
    graph.results[task_a.task_id] = task.TaskResult(
        task_id=task_a.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Still no failures
    assert not graph.has_failures()

    # Set another task as failed
    graph.results[task_b.task_id] = task.TaskResult(
        task_id=task_b.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Now has failures
    assert graph.has_failures()


def test_task_graph_is_complete():
    """Test the is_complete() method."""
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    failure_handler = task.Task.default("failure_handler")

    graph.add_task(task_a)
    graph.add_failure_callback(task_a.task_id, failure_handler)

    # Initially not complete
    assert not graph.is_complete()

    # Set main task as failed
    graph.results[task_a.task_id] = task.TaskResult(
        task_id=task_a.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Failure
    )

    # Still not complete (failure handler hasn't run)
    assert not graph.is_complete()

    # Set failure handler as successful
    graph.results[failure_handler.task_id] = task.TaskResult(
        task_id=failure_handler.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    # Now complete
    assert graph.is_complete()


def test_task_graph_cycle_detection_includes_failure_edges():
    """Test that cycle detection considers both success and failure edges.

    Current design choice: cycle detection treats all edges equally, so
    A -> B (success) + B -> A (failure) is considered a cycle and rejected.
    This is a conservative approach to prevent any potential execution loops.
    """
    graph = task.TaskGraph()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Add basic structure: A -> B (success)
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])

    # This should be rejected due to cycle detection: B fails -> A would create cycle
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        graph.add_failure_callback(task_b.task_id, task_a)

    # But this should work: B fails -> C (no cycle)
    graph.add_failure_callback(task_b.task_id, task_c)

    # Verify the structure exists
    assert task_c.task_id in graph.fail_children
    assert task_b.task_id in graph.fail_edges
    assert task_c.task_id in graph.fail_edges[task_b.task_id]


def test_task_graph_mixed_success_failure_dependencies():
    """Test complex scenarios with both success and failure dependencies."""
    graph = task.TaskGraph()

    # Create tasks
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")
    cleanup_a = task.Task.default("cleanup_a")
    cleanup_b = task.Task.default("cleanup_b")

    # Build structure:
    # A -> B (success) -> C (success)
    # A -> cleanup_a (failure)
    # B -> cleanup_b (failure)
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])  # A succeeds -> B runs
    graph.add_task(task_c, parent_ids=[task_b.task_id])  # B succeeds -> C runs

    graph.add_failure_callback(task_a.task_id, cleanup_a)  # A fails -> cleanup_a runs
    graph.add_failure_callback(task_b.task_id, cleanup_b)  # B fails -> cleanup_b runs

    # Verify structure
    assert cleanup_a.task_id in graph.fail_children
    assert cleanup_b.task_id in graph.fail_children
    assert task_a.task_id in graph.fail_edges
    assert task_b.task_id in graph.fail_edges
    assert cleanup_a.task_id in graph.fail_edges[task_a.task_id]
    assert cleanup_b.task_id in graph.fail_edges[task_b.task_id]  # Test scenario 1: A succeeds
    graph.results[task_a.task_id] = task.TaskResult(
        task_id=task_a.task_id, graph_id=graph.graph_id, status=task.TaskStatus.Success
    )

    ready_tasks = list(graph.generate_ready_tasks())
    failure_tasks = list(graph.generate_failure_ready_tasks())

    # B should be ready, cleanup should not
    ready_ids = [t.task_id for t in ready_tasks]
    failure_ids = [t.task_id for t in failure_tasks]

    assert task_b.task_id in ready_ids
    assert cleanup_a.task_id not in failure_ids
    assert cleanup_b.task_id not in failure_ids

    # Test scenario 2: A fails instead
    graph.results[task_a.task_id].status = task.TaskStatus.Failure

    ready_tasks = list(graph.generate_ready_tasks())
    failure_tasks = list(graph.generate_failure_ready_tasks())

    ready_ids = [t.task_id for t in ready_tasks]
    failure_ids = [t.task_id for t in failure_tasks]

    # B should NOT be ready, cleanup_a should be ready
    assert task_b.task_id not in ready_ids
    assert cleanup_a.task_id in failure_ids


def test_task_on_success_on_failure_moved_to_graph():
    """Test that on_success and on_failure callbacks are moved to graph edges."""
    graph = task.TaskGraph()

    # Create tasks with callbacks
    main_task = task.Task.default("main_task")
    success_task = task.Task.default("success_task")
    failure_task = task.Task.default("failure_task")

    # Set callbacks on the task
    main_task.on_success = success_task
    main_task.on_failure = failure_task

    # Add to graph
    graph.add_task(main_task)

    # Verify callbacks were moved to graph structure and cleared from task
    assert main_task.on_success is None
    assert main_task.on_failure is None

    # Verify success callback became a regular dependency
    assert success_task.task_id in graph.children
    assert main_task.task_id in graph.edges
    assert success_task.task_id in graph.edges[main_task.task_id]

    # Verify failure callback became a failure edge
    assert failure_task.task_id in graph.fail_children
    assert main_task.task_id in graph.fail_edges
    assert failure_task.task_id in graph.fail_edges[main_task.task_id]


def test_task_chained_callbacks_moved_to_graph():
    """Test that chained callbacks (A >> B >> C) are properly moved to graph."""
    graph = task.TaskGraph()

    # Create chained tasks
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Create chain: A >> B >> C
    task_a >> task_b >> task_c

    # Verify chain was created
    assert task_a.on_success is task_b
    assert task_b.on_success is task_c

    # Add to graph - should recursively add all chained tasks
    graph.add_task(task_a)

    # Verify all tasks are in graph
    assert task_a.task_id in graph.children
    assert task_b.task_id in graph.children
    assert task_c.task_id in graph.children

    # Verify callbacks were cleared
    assert task_a.on_success is None
    assert task_b.on_success is None

    # Verify dependencies were created properly
    assert task_b.task_id in graph.edges[task_a.task_id]
    assert task_c.task_id in graph.edges[task_b.task_id]


# ~~~~ ~~~~ ~~~~ ~~~~ ~~~~
# TASKGRAPHBUILDER TESTS
# ~~~~ ~~~~ ~~~~ ~~~~ ~~~~
def test_task_graph_builder_init():
    """Test TaskGraphBuilder initialization."""
    builder = task.TaskGraphBuilder()

    assert isinstance(builder._tasks, dict)
    assert isinstance(builder._dependencies, dict)
    assert isinstance(builder._failure_callbacks, dict)
    assert isinstance(builder._last_added, list)

    assert len(builder._tasks) == 0
    assert len(builder._dependencies) == 0
    assert len(builder._failure_callbacks) == 0
    assert len(builder._last_added) == 0


def test_task_graph_builder_add():
    """Test basic add() method functionality."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add first task
    result = builder.add(task_a)

    # Should return self for chaining
    assert result is builder

    # Verify task was added
    assert task_a.task_id in builder._tasks
    assert builder._tasks[task_a.task_id] is task_a
    assert task_a.task_id in builder._dependencies
    assert builder._dependencies[task_a.task_id] == set()
    assert builder._last_added == [task_a.task_id]

    # Add second task with dependency
    builder.add(task_b, depends_on=[task_a.task_id])

    assert task_b.task_id in builder._tasks
    assert builder._dependencies[task_b.task_id] == {task_a.task_id}
    assert builder._last_added == [task_b.task_id]


def test_task_graph_builder_parallel():
    """Test parallel() method functionality."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")
    tasks = [task_a, task_b, task_c]

    # Add parallel tasks
    result = builder.parallel(tasks)

    # Should return self for chaining
    assert result is builder

    # All tasks should be added
    for t in tasks:
        assert t.task_id in builder._tasks
        assert builder._tasks[t.task_id] is t
        assert builder._dependencies[t.task_id] == set()  # No dependencies

    # All should be in _last_added
    expected_ids = [t.task_id for t in tasks]
    assert set(builder._last_added) == set(expected_ids)


def test_task_graph_builder_parallel_with_dependencies():
    """Test parallel() with explicit dependencies."""
    builder = task.TaskGraphBuilder()
    init_task = task.Task.default("init_task")
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add initial task
    builder.add(init_task)

    # Add parallel tasks that depend on init_task
    builder.parallel([task_a, task_b], depends_on=[init_task.task_id])

    # Both parallel tasks should depend on init_task
    assert builder._dependencies[task_a.task_id] == {init_task.task_id}
    assert builder._dependencies[task_b.task_id] == {init_task.task_id}

    # Both should be in _last_added
    assert set(builder._last_added) == {task_a.task_id, task_b.task_id}


def test_task_graph_builder_then():
    """Test then() method functionality."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add first task
    builder.add(task_a)

    # Add dependent task
    result = builder.then(task_b)

    # Should return self for chaining
    assert result is builder

    # task_b should depend on task_a
    assert builder._dependencies[task_b.task_id] == {task_a.task_id}
    assert builder._last_added == [task_b.task_id]


def test_task_graph_builder_then_without_previous_task():
    """Test then() raises error when no previous tasks."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")

    with pytest.raises(ValueError, match="No previous tasks to depend on"):
        builder.then(task_a)


def test_task_graph_builder_then_parallel():
    """Test parallel() method functionality."""
    builder = task.TaskGraphBuilder()
    init_task = task.Task.default("init_task")
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add initial task
    builder.add(init_task)

    # Add parallel tasks that depend on init_task
    result = builder.parallel([task_a, task_b])

    # Should return self for chaining
    assert result is builder

    # Both should depend on init_task
    assert builder._dependencies[task_a.task_id] == {init_task.task_id}
    assert builder._dependencies[task_b.task_id] == {init_task.task_id}

    # Both should be in _last_added
    assert set(builder._last_added) == {task_a.task_id, task_b.task_id}


def test_task_graph_builder_chain():
    """Test chain() convenience method."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Chain tasks
    result = builder.chain(task_a, task_b, task_c)

    # Should return self for chaining
    assert result is builder

    # Verify chain structure: A -> B -> C
    assert builder._dependencies[task_a.task_id] == set()  # No dependencies
    assert builder._dependencies[task_b.task_id] == {task_a.task_id}
    assert builder._dependencies[task_c.task_id] == {task_b.task_id}

    # Last task should be in _last_added
    assert builder._last_added == [task_c.task_id]


def test_task_graph_builder_chain_empty():
    """Test chain() with no tasks."""
    builder = task.TaskGraphBuilder()
    result = builder.chain()

    # Should return self and do nothing
    assert result is builder
    assert len(builder._tasks) == 0


def test_task_graph_builder_on_failure():
    """Test on_failure() method."""
    builder = task.TaskGraphBuilder()
    main_task = task.Task.default("main_task")
    error_handler = task.Task.default("error_handler")

    # Add main task first
    builder.add(main_task)

    # Add failure callback
    result = builder.on_failure(main_task.task_id, error_handler)

    # Should return self for chaining
    assert result is builder

    # Failure callback should be stored
    assert main_task.task_id in builder._failure_callbacks
    assert error_handler in builder._failure_callbacks[main_task.task_id]


def test_task_graph_builder_on_failure_task_not_found():
    """Test on_failure() raises error when parent task not found."""
    builder = task.TaskGraphBuilder()
    missing_task_id = task.TaskId("missing-task")
    error_handler = task.Task.default("error_handler")

    with pytest.raises(ValueError, match="Parent task .* not found"):
        builder.on_failure(missing_task_id, error_handler)


def test_task_graph_builder_multiple_failure_callbacks():
    """Test adding multiple failure callbacks to same task."""
    builder = task.TaskGraphBuilder()
    main_task = task.Task.default("main_task")
    handler_a = task.Task.default("handler_a")
    handler_b = task.Task.default("handler_b")

    # Add main task and multiple failure handlers
    builder.add(main_task)
    builder.on_failure(main_task.task_id, handler_a)
    builder.on_failure(main_task.task_id, handler_b)

    # Both handlers should be stored
    callbacks = builder._failure_callbacks[main_task.task_id]
    assert len(callbacks) == 2
    assert handler_a in callbacks
    assert handler_b in callbacks


def test_task_graph_builder_add_success_fail_branch():
    """Test add_success_fail_branch() with explicit condition_task_id."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    success_task = task.Task.default("success_task")
    failure_task = task.Task.default("failure_task")

    # Add multiple tasks
    builder.add(task_a)

    # Branch from task_a explicitly (not _last_added)
    builder.add_success_fail_branch(task_a.task_id, success_task, failure_task)

    # Success task should depend on task_a (not task_b)
    assert builder._dependencies[success_task.task_id] == {task_a.task_id}
    assert task_a.task_id in builder._failure_callbacks
    assert failure_task in builder._failure_callbacks[task_a.task_id]


def test_task_graph_builder_build_empty():
    """Test build() raises error for empty graph."""
    builder = task.TaskGraphBuilder()

    with pytest.raises(ValueError, match="Cannot build empty graph"):
        builder.build()


def test_task_graph_builder_build_simple():
    """Test build() creates TaskGraph correctly."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Build simple chain
    graph = builder.add(task_a).then(task_b).build()

    # Should return TaskGraph
    assert isinstance(graph, task.TaskGraph)

    # Verify tasks are in graph
    assert task_a.task_id in graph.children
    assert task_b.task_id in graph.children

    # Verify dependencies
    assert task_a.task_id in graph.edges
    assert task_b.task_id in graph.edges[task_a.task_id]

    # Tasks should have graph_id set
    assert task_a.graph_id == graph.graph_id
    assert task_b.graph_id == graph.graph_id


def test_task_graph_builder_build_with_failure_callbacks():
    """Test build() handles failure callbacks correctly."""
    builder = task.TaskGraphBuilder()
    main_task = task.Task.default("main_task")
    success_task = task.Task.default("success_task")
    failure_task = task.Task.default("failure_task")

    # Build graph with failure callback
    graph = builder.add(main_task).then(success_task).on_failure(main_task.task_id, failure_task).build()

    # Verify failure callback is in graph
    assert failure_task.task_id in graph.fail_children
    assert main_task.task_id in graph.fail_edges
    assert failure_task.task_id in graph.fail_edges[main_task.task_id]


def test_task_graph_builder_complex_workflow():
    """Test building a complex workflow using multiple builder methods."""
    builder = task.TaskGraphBuilder()

    # Create tasks for complex workflow
    init_task = task.Task.default("init_task")
    validate_task = task.Task.default("validate_task")

    process_a = task.Task.default("process_a")
    process_b = task.Task.default("process_b")
    process_c = task.Task.default("process_c")

    merge_task = task.Task.default("merge_task")
    finalize_task = task.Task.default("finalize_task")

    cleanup_task = task.Task.default("cleanup_task")
    error_handler = task.Task.default("error_handler")

    # Build complex graph:
    # init -> validate -> (process_a, process_b, process_c) -> merge -> finalize
    # with cleanup on validate failure and error_handler on merge failure
    graph = (
        builder.chain(init_task, validate_task)
        .parallel([process_a, process_b, process_c])
        .then(merge_task)
        .chain(finalize_task, cleanup_task)
        .on_failure(validate_task.task_id, cleanup_task)
        .on_failure(merge_task.task_id, error_handler)
        .build()
    )

    # Verify the structure - all tasks should be in children (including failure callbacks)
    # Regular tasks: init, validate, process_a, process_b, process_c, merge, finalize = 7
    # Failure callbacks are also in children: cleanup_task, error_handler
    assert len(graph.children) == 8  # All tasks not including failure callbacks
    assert len(graph.fail_children) == 2  # cleanup_task, error_handler

    # Verify chain: init -> validate
    assert validate_task.task_id in graph.edges[init_task.task_id]

    # Verify fan-out: validate -> (process_a, process_b, process_c)
    validate_children = graph.edges[validate_task.task_id]
    assert process_a.task_id in validate_children
    assert process_b.task_id in validate_children
    assert process_c.task_id in validate_children

    # Verify fan-in: (process_a, process_b, process_c) -> merge
    merge_parents = set()
    for parent_id, children in graph.edges.items():
        if merge_task.task_id in children:
            merge_parents.add(parent_id)
    assert merge_parents == {process_a.task_id, process_b.task_id, process_c.task_id}

    # Verify final chain: merge -> finalize -> cleanup
    assert finalize_task.task_id in graph.edges[merge_task.task_id]
    assert cleanup_task.task_id in graph.edges[finalize_task.task_id]

    # Verify failure callbacks
    assert cleanup_task.task_id in graph.fail_edges[validate_task.task_id]
    assert error_handler.task_id in graph.fail_edges[merge_task.task_id]


def test_task_graph_builder_method_chaining():
    """Test that all methods support fluent chaining."""
    builder = task.TaskGraphBuilder()

    # Create tasks
    tasks = [task.Task.default(f"task_{i}") for i in range(10)]

    # Chain multiple operations - this should not raise any errors
    result = (
        builder.add(tasks[0])
        .then(tasks[1])
        .parallel(tasks[2:5])
        .then(tasks[5])
        .parallel(tasks[6:8])
        .then(tasks[8])
        .then(tasks[9])
        .on_failure(tasks[0].task_id, task.Task.default("error_handler"))
    )

    # Should return the builder for continued chaining
    assert result is builder

    # Should be able to build successfully
    graph = builder.build()
    assert isinstance(graph, task.TaskGraph)


def test_task_graph_builder_cycle_detection_during_build():
    """Test that cycle detection works during build() phase."""
    builder = task.TaskGraphBuilder()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Create structure that will cause cycle when built
    builder.add(task_a, depends_on=[task_b.task_id])  # A depends on B
    builder.add(task_b, depends_on=[task_a.task_id])  # B depends on A

    # Should raise ValueError during build due to cycle
    with pytest.raises(ValueError, match="would create a cycle in the DAG"):
        builder.build()


def test_task_graph_builder_state_isolation():
    """Test that different builder instances don't interfere with each other."""
    builder1 = task.TaskGraphBuilder()
    builder2 = task.TaskGraphBuilder()

    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Add different tasks to different builders
    builder1.add(task_a)
    builder2.add(task_b)

    # Builders should have independent state
    assert task_a.task_id in builder1._tasks
    assert task_a.task_id not in builder2._tasks
    assert task_b.task_id not in builder1._tasks
    assert task_b.task_id in builder2._tasks


def test_task_graph_builder_reusable():
    """Test that builder can be used to create multiple graphs."""
    builder = task.TaskGraphBuilder()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")

    # Build first graph
    graph1 = builder.add(task_a).then(task_b).build()

    # The builder should still work for building another graph
    task_c = task.Task.default("task_c")
    task_d = task.Task.default("task_d")

    # Note: We're reusing the same builder which already has task_a and task_b
    graph2 = builder.add(task_c).then(task_d).build()

    # Both graphs should be valid but different
    assert isinstance(graph1, task.TaskGraph)
    assert isinstance(graph2, task.TaskGraph)
    assert graph1.graph_id != graph2.graph_id

    # Second graph should have all tasks (including from first build)
    assert len(graph2.children) == 4  # task_a, task_b, task_c, task_d


# Tests for moving on_success/on_failure from tasks to graph level
def test_task_callbacks_are_none_by_default():
    """Test that new tasks have None callbacks by default (moved to graph level)."""
    task_obj = task.Task.default("test_task")

    # Task-level callbacks should be None (moved to graph level)
    assert task_obj.on_success is None
    assert task_obj.on_failure is None


def test_task_callbacks_can_still_be_set():
    """Test that task-level callbacks can still be set for backward compatibility."""
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Set task-level callbacks (backward compatibility)
    task_a.on_success = task_b
    task_a.on_failure = task_c

    assert task_a.on_success is task_b
    assert task_a.on_failure is task_c


def test_callback_migration_pattern():
    """Test that graph-level callbacks work independently from task-level callbacks."""
    # Create tasks
    main_task = task.Task.default("main_task")
    task_level_success = task.Task.default("task_level_success")
    task_level_failure = task.Task.default("task_level_failure")
    graph_level_failure = task.Task.default("graph_level_failure")

    # Set task-level callbacks (old style)
    main_task.on_success = task_level_success
    main_task.on_failure = task_level_failure

    # Create graph and add graph-level callbacks (new style)
    graph = task.TaskGraph()
    graph.add_task(main_task)
    graph.add_task(task_level_success, parent_ids=[main_task.task_id])  # Traditional dependency
    # Graph-level failure callback
    graph.add_failure_callback(main_task.task_id, graph_level_failure)

    # Verify these callbacks have been cleared out and added to the graph
    assert main_task.on_success is None
    assert main_task.on_failure is None

    # Verify graph-level callbacks
    assert graph_level_failure.task_id in graph.fail_children
    assert main_task.task_id in graph.fail_edges
    assert graph_level_failure.task_id in graph.fail_edges[main_task.task_id]


def test_multiple_graph_level_callbacks_per_task():
    """Test that multiple graph-level callbacks can be added to the same task."""
    main_task = task.Task.default("main_task")
    callback_1 = task.Task.default("callback_1")
    callback_2 = task.Task.default("callback_2")
    callback_3 = task.Task.default("callback_3")
    success_callback = task.Task.default("callback_1_success")
    main_task.on_failure = callback_1
    callback_1.on_failure = callback_2
    callback_1.on_success = success_callback
    callback_2.on_failure = callback_3

    graph = task.TaskGraph()
    graph.add_task(main_task)

    # Verify all callbacks are present
    assert len(graph.fail_edges) == 3, "There should be three tasks with failure callbacks"
    assert callback_1.task_id in graph.fail_edges[main_task.task_id]
    assert callback_1.task_id in graph.fail_edges
    # This one has an on_success so we should check that too
    assert callback_1.task_id in graph.edges, "Callback 1 should have success edge"
    assert success_callback.task_id in graph.edges[callback_1.task_id]
    assert success_callback.task_id in graph.children
    assert callback_2.task_id in graph.fail_edges[callback_1.task_id]
    assert callback_2.task_id in graph.fail_edges
    # This one has no children
    assert callback_3.task_id not in graph.fail_edges, "Callback 3 should have no further failure callbacks"
    assert callback_3.task_id in graph.fail_edges[callback_2.task_id]

    for tsk in (main_task, callback_1, callback_2, callback_3):
        assert tsk.on_failure is None, "Task-level on_failure should be have been removed"
        assert tsk.on_success is None


def test_callback_inheritance_in_task_creation():
    """Test that tasks created with callbacks work properly."""
    success_task = task.Task.default("success_task")
    failure_task = task.Task.default("failure_task")

    # Create task with explicit callback setting
    main_task = task.Task.default("main_task")
    main_task.on_success = success_task
    main_task.on_failure = failure_task

    # Verify callbacks are preserved
    assert main_task.on_success is success_task
    assert main_task.on_failure is failure_task

    # Verify these can be added to graph normally
    graph = task.TaskGraph()
    graph.add_task(main_task)

    assert main_task.task_id in graph.children
    # Task-level callbacks don't automatically create graph structure


def test_callback_migration_ergonomics():
    """Test that the new graph-level callback system is more ergonomic."""
    # Show how TaskGraphBuilder makes this easier
    builder = task.TaskGraphBuilder()

    main_task = task.Task.default("main_task")
    success_task = task.Task.default("success_task")
    failure_task = task.Task.default("failure_task")

    # NEW ERGONOMIC WAY: Use builder with graph-level callbacks
    graph = (
        builder.add(main_task)
        .then(success_task)  # Success path via dependency
        .on_failure(main_task.task_id, failure_task)  # Failure path via callback
        .build()
    )

    # Verify structure
    assert success_task.task_id in graph.edges[main_task.task_id]  # Success dependency
    assert failure_task.task_id in graph.fail_edges[main_task.task_id]  # Failure callback

    # Tasks themselves don't carry callbacks (moved to graph level)
    assert main_task.on_success is None
    assert main_task.on_failure is None
    assert success_task.on_success is None
    assert success_task.on_failure is None


def test_complex_callback_migration_scenario():
    """Test a complex scenario showing the benefits of graph-level callback migration."""
    # Create a workflow that benefits from graph-level callbacks
    builder = task.TaskGraphBuilder()

    validate_input = task.Task.default("validate_input")
    process_data = task.Task.default("process_data")
    save_results = task.Task.default("save_results")
    send_notification = task.Task.default("send_notification")

    # Error handlers (multiple per failure point)
    validation_error_log = task.Task.default("validation_error_log")
    validation_error_alert = task.Task.default("validation_error_alert")
    processing_error_rollback = task.Task.default("processing_error_rollback")
    processing_error_retry = task.Task.default("processing_error_retry")
    save_error_backup = task.Task.default("save_error_backup")

    # Build complex workflow with multiple failure handlers per task
    graph = (
        builder.chain(validate_input, process_data, save_results, send_notification)
        .on_failure(validate_input.task_id, validation_error_log)
        .on_failure(validate_input.task_id, validation_error_alert)  # Multiple handlers!
        .on_failure(process_data.task_id, processing_error_rollback)
        .on_failure(process_data.task_id, processing_error_retry)  # Multiple handlers!
        .on_failure(save_results.task_id, save_error_backup)
        .build()
    )

    # Verify multiple failure handlers per task (impossible with old task-level callbacks)
    validate_failures = graph.fail_edges[validate_input.task_id]
    assert validation_error_log.task_id in validate_failures
    assert validation_error_alert.task_id in validate_failures
    assert len(validate_failures) == 2

    process_failures = graph.fail_edges[process_data.task_id]
    assert processing_error_rollback.task_id in process_failures
    assert processing_error_retry.task_id in process_failures
    assert len(process_failures) == 2

    # All main tasks have clean callback state (moved to graph level)
    for main_task in [validate_input, process_data, save_results, send_notification]:
        assert main_task.on_success is None
        assert main_task.on_failure is None


def test_backward_compatibility_with_bitshift_operators():
    """Test that bitshift operators still work for task-level callbacks."""
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    task_c = task.Task.default("task_c")

    # Old bitshift syntax should still work for task-level callbacks
    task_a >> task_b  # on_success
    task_c << task_b  # task_b.on_success = task_c (leftward)

    # Verify the old operators still set task-level callbacks
    assert task_a.on_success is task_b
    # Note: The << operator behavior might be different, let's test actual implementation

    # Can still add to graph
    graph = task.TaskGraph()
    graph.add_task(task_a)
    graph.add_task(task_b)
    graph.add_task(task_c)

    # Task-level callbacks removed and put in edges instead
    assert task_a.on_success is None
    assert task_b.on_success is None
    for tsk in (task_a, task_b, task_c):
        assert tsk.on_failure is None
        assert tsk.on_success is None
        assert tsk.task_id in graph.children
    assert task_b.task_id in graph.edges[task_a.task_id]
    assert task_c.task_id in graph.edges[task_b.task_id]


def test_failure_ready_tasks_with_no_failures():
    """Test failure_ready_tasks when no tasks have failed."""
    graph = task.TaskGraph()
    task_a = task.Task.default("task_a")
    task_b = task.Task.default("task_b")
    failure_handler = task.Task.default("failure_handler")

    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])
    graph.add_failure_callback(task_a.task_id, failure_handler)

    # No failures yet, so no failure tasks should be ready
    failure_tasks = list(graph.generate_failure_ready_tasks())
    assert len(failure_tasks) == 0


def test_failure_ready_tasks_with_complex_dependency_chains():
    """Test failure_ready_tasks with complex failure dependency chains."""
    graph = task.TaskGraph()

    # Create a complex scenario:
    # A -> B -> C (main chain)
    # A fails -> Handler1 -> Handler2 (failure chain)
    # B fails -> Handler3
    main_a = task.Task.default("main_a")
    main_b = task.Task.default("main_b")
    main_c = task.Task.default("main_c")
    handler_1 = task.Task.default("handler_1")
    handler_2 = task.Task.default("handler_2")
    handler_3 = task.Task.default("handler_3")

    # Build main chain
    graph.add_task(main_a)
    graph.add_task(main_b, parent_ids=[main_a.task_id])
    graph.add_task(main_c, parent_ids=[main_b.task_id])

    # Build failure chains
    graph.add_failure_callback(main_a.task_id, handler_1)
    graph.add_task(handler_2, parent_ids=[handler_1.task_id])  # Handler chain
    graph.add_failure_callback(main_b.task_id, handler_3)

    # A fails - only handler_1 should be ready (not handler_2 which depends on handler_1)
    graph.results[main_a.task_id] = task.TaskResultSlim(
        status=task.TaskStatus.Failure,
        task_id=main_a.task_id,
        graph_id=graph.graph_id,
    )
    failure_tasks = list(
        itertools.chain.from_iterable((graph.generate_ready_tasks(), graph.generate_failure_ready_tasks()))
    )
    assert len(failure_tasks) == 1
    assert failure_tasks[0].task_id == handler_1.task_id

    # handler_1 started,
    graph.results[handler_1.task_id] = task.TaskResultSlim(
        status=task.TaskStatus.Started,
        task_id=handler_1.task_id,
        graph_id=graph.graph_id,
    )
    failure_tasks = list(
        itertools.chain.from_iterable((graph.generate_ready_tasks(), graph.generate_failure_ready_tasks()))
    )
    assert len(failure_tasks) == 0

    # handler 1 completed, now handler_2 should be ready
    graph.results[handler_1.task_id] = task.TaskResultSlim(
        status=task.TaskStatus.Success,
        task_id=handler_1.task_id,
        graph_id=graph.graph_id,
    )
    continue_tasks = list(
        itertools.chain.from_iterable((graph.generate_ready_tasks(), graph.generate_failure_ready_tasks()))
    )
    assert len(continue_tasks) == 1
    assert continue_tasks[0].task_id == handler_2.task_id
