import pytest
from boilermaker import task


async def sample_task(state, number1, number2: int = 4):
    if hasattr(state, "sample_task_called"):
        state.sample_task_called += number1
    return number1 + number2

# ~~~~ ~~~~~ ~~~~ ~~~~ #
# Cycle Detection Tests
# ~~~~ ~~~~~ ~~~~ ~~~~ #
def test_cycle_detection_algorithm_disconnected_components():
    """Test cycle detection with disconnected graph components.

    This tests if the algorithm correctly handles graphs with multiple
    disconnected components, some with cycles and some without.
    """
    graph = task.TaskGraph(graph_id="disconnect_test")

    # Component 1: A -> B -> C (no cycle)
    task_a = task.Task.si(sample_task)
    task_b = task.Task.si(sample_task)
    task_c = task.Task.si(sample_task)

    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])
    graph.add_task(task_c, parent_ids=[task_b.task_id])

    # Component 2: D -> E (no cycle, disconnected)
    task_d = task.Task.si(sample_task)
    task_e = task.Task.si(sample_task)

    graph.add_task(task_d)
    graph.add_task(task_e, parent_ids=[task_d.task_id])

    # Should be no cycles yet
    assert not graph._detect_cycles()

    # Now try to create a cycle in component 1: C -> A
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task_a, parent_ids=[task_c.task_id])


def test_cycle_detection_algorithm_self_loop_edge_case():
    """Test if algorithm handles self-loops correctly.

    Edge case: What happens if we try to create a task that depends on itself?
    """
    graph = task.TaskGraph(graph_id="self_loop_test")

    task_a = task.Task.si(sample_task)
    graph.add_task(task_a)

    # Try to create a self-loop: A -> A
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task_a, parent_ids=[task_a.task_id])


def test_cycle_detection_algorithm_complex_diamond_with_cycle():
    """Test diamond dependency pattern that could create subtle cycle.

    Pattern:    A
               / \\
              B   C
               \\ /
                D
    Then try: D -> A (should detect cycle)
    """
    graph = task.TaskGraph(graph_id="diamond_test")

    # Create diamond pattern
    task_a = task.Task.si(sample_task)  # Root
    task_b = task.Task.si(sample_task)  # Left branch
    task_c = task.Task.si(sample_task)  # Right branch
    task_d = task.Task.si(sample_task)  # Convergence

    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])  # A -> B
    graph.add_task(task_c, parent_ids=[task_a.task_id])  # A -> C
    graph.add_task(task_d, parent_ids=[task_b.task_id])  # B -> D

    # This should work - adding second parent to D
    # This tests if the algorithm handles multiple parents correctly
    graph.add_task(task_d, parent_ids=[task_c.task_id])  # C -> D (second parent)

    # Now try to close the loop: D -> A
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task_a, parent_ids=[task_d.task_id])


def test_cycle_detection_algorithm_long_cycle():
    """Test detection of cycles in long chains.

    Create: A -> B -> C -> D -> E -> F
    Then try: F -> A (should detect 6-node cycle)
    """
    graph = task.TaskGraph(graph_id="long_cycle_test")

    tasks = [task.Task.si(sample_task) for _ in range(6)]  # A, B, C, D, E, F

    # Add first task
    graph.add_task(tasks[0])

    # Create chain: A -> B -> C -> D -> E -> F
    for i in range(1, 6):
        graph.add_task(tasks[i], parent_ids=[tasks[i - 1].task_id])

    # Should be no cycle yet
    assert not graph._detect_cycles()

    # Try to close the long cycle: F -> A
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(tasks[0], parent_ids=[tasks[5].task_id])


def test_cycle_detection_algorithm_multiple_entry_points():
    """Test cycle detection with multiple possible entry points.

    Create complex graph:
         A -> B -> D
         |    |
         v    v
         C -> E -> F

    Then try: F -> B (should create cycle through B->D path and B->E->F path)
    """
    graph = task.TaskGraph(graph_id="multi_entry_test")

    task_a = task.Task.si(sample_task)
    task_b = task.Task.si(sample_task)
    task_c = task.Task.si(sample_task)
    task_d = task.Task.si(sample_task)
    task_e = task.Task.si(sample_task)
    task_f = task.Task.si(sample_task)

    # Build the graph structure
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])  # A -> B
    graph.add_task(task_c, parent_ids=[task_a.task_id])  # A -> C
    graph.add_task(task_d, parent_ids=[task_b.task_id])  # B -> D
    graph.add_task(task_e, parent_ids=[task_b.task_id])  # B -> E
    graph.add_task(task_e, parent_ids=[task_c.task_id])  # C -> E (E has multiple parents)
    graph.add_task(task_f, parent_ids=[task_e.task_id])  # E -> F

    # Try to create cycle: F -> B
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task_b, parent_ids=[task_f.task_id])


def test_cycle_detection_stress_test_false_positive():
    """Stress test: Ensure algorithm doesn't have false positives.

    Create a complex DAG that's valid and ensure no false cycle detection.
    This tests the algorithmic correctness under complex valid scenarios.
    """
    graph = task.TaskGraph(graph_id="stress_test")

    # Create a complex but valid DAG (tree-like with convergence)
    #       A
    #      /|\
    #     B C D
    #    /| ||\
    #   E F G H I
    #    \|/  \|/
    #     J    K
    #      \  /
    #       L

    tasks = {name: task.Task.si(sample_task) for name in "ABCDEFGHIJKL"}

    # Build the structure
    graph.add_task(tasks["A"])

    # Level 2: A -> B, C, D
    for child in "BCD":
        graph.add_task(tasks[child], parent_ids=[tasks["A"].task_id])

    # Level 3: B -> E,F; C -> G; D -> H,I
    graph.add_task(tasks["E"], parent_ids=[tasks["B"].task_id])
    graph.add_task(tasks["F"], parent_ids=[tasks["B"].task_id])
    graph.add_task(tasks["G"], parent_ids=[tasks["C"].task_id])
    graph.add_task(tasks["H"], parent_ids=[tasks["D"].task_id])
    graph.add_task(tasks["I"], parent_ids=[tasks["D"].task_id])

    # Multiple parents
    # Level 4: E,F,G -> J; H,I -> K
    graph.add_task(
        tasks["J"],
        parent_ids=[
            tasks["E"].task_id,
            tasks["F"].task_id,
            tasks["G"].task_id,
        ],
    )

    graph.add_task(tasks["K"], parent_ids=[tasks["H"].task_id, tasks["I"].task_id])

    # Level 5: J,K -> L
    graph.add_task(tasks["L"], parent_ids=[tasks["J"].task_id, tasks["K"].task_id])

    # This complex DAG should be valid - no cycles
    assert not graph._detect_cycles()

    # The stress test: this should still be valid
    assert len(graph.children) == 12  # All tasks added

    # Only tasks with no parents should be ready initially (just A)
    ready_tasks = list(graph.generate_ready_tasks())
    assert len(ready_tasks) == 1
    assert tasks["A"] in ready_tasks  # Only A has no dependencies initially


def test_cycle_detection_algorithm_edge_case_empty_graph():
    """Test cycle detection on empty graph."""
    graph = task.TaskGraph(graph_id="empty_test")
    assert not graph._detect_cycles()  # Empty graph has no cycles


def test_cycle_detection_algorithm_single_node():
    """Test cycle detection with single isolated node."""
    graph = task.TaskGraph(graph_id="single_test")

    task_a = task.Task.si(sample_task)
    graph.add_task(task_a)

    assert not graph._detect_cycles()  # Single node can't have cycle


def test_cycle_detection_potential_algorithm_bug_duplicate_add():
    """Test potential bug: What if we try to add the same task twice?

    This might reveal edge cases in how the algorithm handles existing nodes.
    """
    graph = task.TaskGraph(graph_id="duplicate_test")

    task_a = task.Task.si(sample_task)
    task_b = task.Task.si(sample_task)

    # Add tasks normally
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])

    # What happens if we try to add the same relationship again?
    # This might reveal if the algorithm handles duplicate edges correctly
    graph.add_task(task_b, parent_ids=[task_a.task_id])  # Same relationship again

    # Should still be no cycle
    assert not graph._detect_cycles()

    # But the edges set should not have duplicates
    assert len(graph.edges[task_a.task_id]) == 1


def test_cycle_detection_race_condition_simulation():
    """Test if cycle detection has issues with edge ordering.

    Tests whether the order of adding edges affects cycle detection correctness.
    """
    # Test case 1: Add A->B then B->C then C->A
    graph1 = task.TaskGraph(graph_id="race1")
    tasks1 = [task.Task.si(sample_task) for _ in range(3)]

    graph1.add_task(tasks1[0])  # A
    graph1.add_task(tasks1[1], parent_ids=[tasks1[0].task_id])  # A -> B
    graph1.add_task(tasks1[2], parent_ids=[tasks1[1].task_id])  # B -> C

    # This should create a cycle
    with pytest.raises(ValueError, match="would create a cycle"):
        graph1.add_task(tasks1[0], parent_ids=[tasks1[2].task_id])  # C -> A

    # Test case 2: Same graph, different order
    graph2 = task.TaskGraph(graph_id="race2")
    tasks2 = [task.Task.si(sample_task) for _ in range(3)]

    graph2.add_task(tasks2[0])  # A
    graph2.add_task(tasks2[2])  # C (add C first)
    graph2.add_task(tasks2[1], parent_ids=[tasks2[0].task_id])  # A -> B
    graph2.add_task(tasks2[2], parent_ids=[tasks2[1].task_id])  # B -> C

    # Should also detect the same cycle
    with pytest.raises(ValueError, match="would create a cycle"):
        graph2.add_task(tasks2[0], parent_ids=[tasks2[2].task_id])  # C -> A


def test_cycle_detection_algorithm_subtle_bug_attempt():
    """Attempt to find a subtle bug in cycle detection.

    This test tries to exploit potential issues with:
    1. Adding same task multiple times with different parents
    2. Complex path structures that might confuse the DFS
    3. Edge cases in recursion stack management
    """
    graph = task.TaskGraph(graph_id="subtle_bug_test")

    # Create tasks
    task_a = task.Task.si(sample_task)
    task_b = task.Task.si(sample_task)
    task_c = task.Task.si(sample_task)
    task_d = task.Task.si(sample_task)

    # Build a complex structure step by step
    graph.add_task(task_a)  # A (root)
    graph.add_task(task_b, parent_ids=[task_a.task_id])  # A -> B
    graph.add_task(task_c, parent_ids=[task_b.task_id])  # B -> C
    graph.add_task(task_d, parent_ids=[task_c.task_id])  # C -> D

    # Now add A as a child of B (should be allowed, creates A -> B -> ... -> B)
    # Wait, this wouldn't work because A already exists...
    # Let me try a different approach

    # Add D as additional child of A (creates: A -> B -> C -> D and A -> D)
    graph.add_task(task_d, parent_ids=[task_a.task_id])  # A -> D (second parent for D)

    # Now we have: A -> B -> C -> D and A -> D
    # This should be fine, no cycle
    assert not graph._detect_cycles()

    # Now try to create a cycle: D -> A
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(task_a, parent_ids=[task_d.task_id])  # D -> A (would create cycle)


def test_cycle_detection_algorithm_really_try_to_break_it():
    """Final attempt to break the cycle detection algorithm.

    This creates the most complex scenario I can think of to try to break
    the DFS-based cycle detection.
    """
    graph = task.TaskGraph(graph_id="break_it_test")

    # Create a graph that has many interconnections
    tasks = [task.Task.si(sample_task) for _ in range(10)]  # 0-9

    # Add all tasks first
    for t in tasks:
        graph.add_task(t)

    # Create complex dependencies:
    # 0 -> 1 -> 2 -> 3
    # 0 -> 4 -> 5 -> 3
    # 0 -> 6 -> 7 -> 8 -> 9
    # 4 -> 9
    # 5 -> 8
    graph.add_task(tasks[1], parent_ids=[tasks[0].task_id])  # 0 -> 1
    graph.add_task(tasks[2], parent_ids=[tasks[1].task_id])  # 1 -> 2
    graph.add_task(tasks[3], parent_ids=[tasks[2].task_id])  # 2 -> 3

    graph.add_task(tasks[4], parent_ids=[tasks[0].task_id])  # 0 -> 4
    graph.add_task(tasks[5], parent_ids=[tasks[4].task_id])  # 4 -> 5
    graph.add_task(tasks[3], parent_ids=[tasks[5].task_id])  # 5 -> 3 (multiple parents for 3)

    graph.add_task(tasks[6], parent_ids=[tasks[0].task_id])  # 0 -> 6
    graph.add_task(tasks[7], parent_ids=[tasks[6].task_id])  # 6 -> 7
    graph.add_task(tasks[8], parent_ids=[tasks[7].task_id])  # 7 -> 8
    graph.add_task(tasks[9], parent_ids=[tasks[8].task_id])  # 8 -> 9

    graph.add_task(tasks[9], parent_ids=[tasks[4].task_id])  # 4 -> 9 (multiple parents for 9)
    graph.add_task(tasks[8], parent_ids=[tasks[5].task_id])  # 5 -> 8 (multiple parents for 8)

    # This complex structure should still be a valid DAG
    assert not graph._detect_cycles()

    # Now try various ways to create cycles:

    # Try: 3 -> 0 (would create multiple cycles)
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(tasks[0], parent_ids=[tasks[3].task_id])

    # Try: 9 -> 4 (would create cycle through 4 -> 9 path)
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(tasks[4], parent_ids=[tasks[9].task_id])

    # Try: 8 -> 5 (would create cycle through 5 -> 8 path)
    with pytest.raises(ValueError, match="would create a cycle"):
        graph.add_task(tasks[5], parent_ids=[tasks[8].task_id])
