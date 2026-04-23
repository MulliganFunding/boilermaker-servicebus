"""Tests for boilermaker.cli.inspect — run_inspect() with JSON output mode and single-task path."""

import json
from io import StringIO
from unittest import mock

import pytest
from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli.inspect import run_inspect
from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus
from rich.console import Console

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_graph_with_tasks() -> tuple[TaskGraph, Task, Task, Task]:
    """Build a graph with three children: A -> B -> C."""
    graph = TaskGraph()
    task_a = Task.default("fetch_data")
    task_b = Task.default("process_report")
    task_c = Task.default("send_notification")
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])
    graph.add_task(task_c, parent_ids=[task_b.task_id])
    return graph, task_a, task_b, task_c


def _set_result(graph: TaskGraph, task: Task, status: TaskStatus) -> None:
    """Set a result on a graph for the given task and status."""
    result = TaskResultSlim(
        task_id=task.task_id,
        graph_id=graph.graph_id,
        status=status,
    )
    graph.results[task.task_id] = result


def _mock_storage(graph: TaskGraph | None = None) -> mock.AsyncMock:
    """Return a mock storage whose load_graph_slim_from_tags returns the given graph."""
    storage = mock.AsyncMock()
    storage.load_graph = mock.AsyncMock(return_value=graph)
    storage.load_graph_slim_from_tags = mock.AsyncMock(return_value=graph)
    return storage


def _no_color_console() -> Console:
    """Return a Rich Console that writes to a StringIO buffer with no color."""
    return Console(file=StringIO(), no_color=True, width=120)


# ---------------------------------------------------------------------------
# TestRunInspectExitCodes
# ---------------------------------------------------------------------------


class TestRunInspectExitCodes:
    @pytest.mark.asyncio
    async def test_returns_exit_healthy_when_all_tasks_succeed(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        exit_code = await run_inspect(storage, str(graph.graph_id), console=_no_color_console())
        assert exit_code == EXIT_HEALTHY

    @pytest.mark.asyncio
    async def test_returns_exit_stalled_when_task_is_stalled(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)
        exit_code = await run_inspect(storage, str(graph.graph_id), console=_no_color_console())
        assert exit_code == EXIT_STALLED

    @pytest.mark.asyncio
    async def test_returns_exit_error_when_graph_not_found(self):
        storage = _mock_storage(None)
        exit_code = await run_inspect(storage, "nonexistent-graph-id", console=_no_color_console())
        assert exit_code == EXIT_ERROR


# ---------------------------------------------------------------------------
# TestRunInspectJsonOutputValidity
# ---------------------------------------------------------------------------


class TestRunInspectJsonOutputValidity:
    @pytest.mark.asyncio
    async def test_json_output_is_valid_json(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert isinstance(parsed, dict)

    @pytest.mark.asyncio
    async def test_json_output_contains_no_ansi_codes(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        captured = capsys.readouterr()
        assert "\x1b[" not in captured.out

    @pytest.mark.asyncio
    async def test_json_output_contains_no_rich_markup(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        captured = capsys.readouterr()
        # Rich markup uses [bold], [red], etc. — should not appear in JSON output
        assert "[bold" not in captured.out
        assert "[red" not in captured.out
        assert "[green" not in captured.out


# ---------------------------------------------------------------------------
# TestRunInspectJsonSchema
# ---------------------------------------------------------------------------


class TestRunInspectJsonSchema:
    @pytest.mark.asyncio
    async def test_json_contains_graph_id(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["graph_id"] == str(graph.graph_id)

    @pytest.mark.asyncio
    async def test_json_contains_is_complete_field(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert "is_complete" in parsed
        assert parsed["is_complete"] is True

    @pytest.mark.asyncio
    async def test_json_is_complete_false_for_in_progress_graph(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Started)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["is_complete"] is False

    @pytest.mark.asyncio
    async def test_json_contains_has_failures_field(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        _set_result(graph, task_b, TaskStatus.Pending)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert "has_failures" in parsed
        assert parsed["has_failures"] is True

    @pytest.mark.asyncio
    async def test_json_has_failures_false_when_healthy(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["has_failures"] is False

    @pytest.mark.asyncio
    async def test_json_contains_stalled_count_field(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert "stalled_count" in parsed
        assert parsed["stalled_count"] == 1

    @pytest.mark.asyncio
    async def test_json_stalled_count_zero_for_healthy_graph(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["stalled_count"] == 0


# ---------------------------------------------------------------------------
# TestRunInspectJsonTaskIds
# ---------------------------------------------------------------------------


class TestRunInspectJsonTaskIds:
    @pytest.mark.asyncio
    async def test_json_task_ids_are_full_not_truncated(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        task_ids_in_json = {t["task_id"] for t in parsed["tasks"]}
        expected_task_ids = {str(task_a.task_id), str(task_b.task_id), str(task_c.task_id)}
        assert task_ids_in_json == expected_task_ids

    @pytest.mark.asyncio
    async def test_json_task_ids_match_full_length(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        for task_entry in parsed["tasks"]:
            # Full UUIDs are 36 chars; task IDs are at least 12 chars when full
            assert len(task_entry["task_id"]) > 12


# ---------------------------------------------------------------------------
# TestRunInspectJsonTasksArray
# ---------------------------------------------------------------------------


class TestRunInspectJsonTasksArray:
    @pytest.mark.asyncio
    async def test_json_tasks_array_contains_all_children(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed["tasks"]) == 3

    @pytest.mark.asyncio
    async def test_json_task_entry_has_required_fields(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        required_fields = {"task_id", "function_name", "status", "type", "is_stalled", "depends_on"}
        for task_entry in parsed["tasks"]:
            assert required_fields.issubset(task_entry.keys())

    @pytest.mark.asyncio
    async def test_json_task_type_is_child_for_regular_tasks(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        for task_entry in parsed["tasks"]:
            assert task_entry["type"] == "child"

    @pytest.mark.asyncio
    async def test_json_task_function_names_are_present(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        function_names = {t["function_name"] for t in parsed["tasks"]}
        assert "fetch_data" in function_names
        assert "process_report" in function_names
        assert "send_notification" in function_names

    @pytest.mark.asyncio
    async def test_json_task_status_reflects_result_status(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Failure)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        task_statuses = {t["function_name"]: t["status"] for t in parsed["tasks"]}
        assert task_statuses["fetch_data"] == str(TaskStatus.Success)
        assert task_statuses["process_report"] == str(TaskStatus.Failure)
        assert task_statuses["send_notification"] == str(TaskStatus.Pending)

    @pytest.mark.asyncio
    async def test_json_stalled_task_has_is_stalled_true(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        stalled_entry = next(t for t in parsed["tasks"] if t["function_name"] == "send_notification")
        assert stalled_entry["is_stalled"] is True

    @pytest.mark.asyncio
    async def test_json_healthy_task_has_is_stalled_false(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        for task_entry in parsed["tasks"]:
            assert task_entry["is_stalled"] is False


# ---------------------------------------------------------------------------
# TestRunInspectJsonDependsOn
# ---------------------------------------------------------------------------


class TestRunInspectJsonDependsOn:
    @pytest.mark.asyncio
    async def test_json_root_task_has_empty_depends_on(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        fetch_entry = next(t for t in parsed["tasks"] if t["function_name"] == "fetch_data")
        assert fetch_entry["depends_on"] == []

    @pytest.mark.asyncio
    async def test_json_dependent_task_lists_parent_in_depends_on(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        process_entry = next(t for t in parsed["tasks"] if t["function_name"] == "process_report")
        assert str(task_a.task_id) in process_entry["depends_on"]

    @pytest.mark.asyncio
    async def test_json_depends_on_lists_all_parents_for_multi_parent_task(self, capsys):
        graph = TaskGraph()
        task_a = Task.default("step_a")
        task_b = Task.default("step_b")
        task_c = Task.default("merge")
        graph.add_task(task_a)
        graph.add_task(task_b)
        graph.add_task(task_c, parent_ids=[task_a.task_id, task_b.task_id])
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        merge_entry = next(t for t in parsed["tasks"] if t["function_name"] == "merge")
        assert str(task_a.task_id) in merge_entry["depends_on"]
        assert str(task_b.task_id) in merge_entry["depends_on"]
        assert len(merge_entry["depends_on"]) == 2


# ---------------------------------------------------------------------------
# TestRunInspectJsonFailTasks
# ---------------------------------------------------------------------------


class TestRunInspectJsonFailTasks:
    @pytest.mark.asyncio
    async def test_json_fail_tasks_array_contains_fail_children(self, capsys):
        graph = TaskGraph()
        parent_task = Task.default("do_work")
        fail_task = Task.default("handle_failure")
        graph.add_task(parent_task)
        graph.add_failure_callback(parent_task.task_id, fail_task)
        _set_result(graph, parent_task, TaskStatus.Failure)
        _set_result(graph, fail_task, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed["fail_tasks"]) == 1
        assert parsed["fail_tasks"][0]["function_name"] == "handle_failure"

    @pytest.mark.asyncio
    async def test_json_fail_tasks_type_is_fail_child(self, capsys):
        graph = TaskGraph()
        parent_task = Task.default("do_work")
        fail_task = Task.default("handle_failure")
        graph.add_task(parent_task)
        graph.add_failure_callback(parent_task.task_id, fail_task)
        _set_result(graph, parent_task, TaskStatus.Failure)
        _set_result(graph, fail_task, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["fail_tasks"][0]["type"] == "fail_child"

    @pytest.mark.asyncio
    async def test_json_fail_tasks_empty_when_no_fail_children(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id), output_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["fail_tasks"] == []


# ---------------------------------------------------------------------------
# TestRunInspectJsonExitCodes
# ---------------------------------------------------------------------------


class TestRunInspectJsonExitCodes:
    @pytest.mark.asyncio
    async def test_json_exit_code_healthy_matches_non_json_mode(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)

        json_exit_code = await run_inspect(storage, str(graph.graph_id), output_json=True)
        capsys.readouterr()  # flush captured output

        storage.load_graph_slim_from_tags.return_value = graph
        rich_exit_code = await run_inspect(storage, str(graph.graph_id), console=_no_color_console())
        assert json_exit_code == rich_exit_code == EXIT_HEALTHY

    @pytest.mark.asyncio
    async def test_json_exit_code_stalled_matches_non_json_mode(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)

        json_exit_code = await run_inspect(storage, str(graph.graph_id), output_json=True)
        capsys.readouterr()  # flush captured output

        storage.load_graph_slim_from_tags.return_value = graph
        rich_exit_code = await run_inspect(storage, str(graph.graph_id), console=_no_color_console())
        assert json_exit_code == rich_exit_code == EXIT_STALLED

    @pytest.mark.asyncio
    async def test_json_exit_code_error_when_graph_not_found(self, capsys):
        storage = _mock_storage(None)
        exit_code = await run_inspect(storage, "nonexistent-graph", output_json=True)
        assert exit_code == EXIT_ERROR


# ---------------------------------------------------------------------------
# TestRunInspectSingleTaskGraphNotFound
# ---------------------------------------------------------------------------


class TestRunInspectSingleTaskGraphNotFound:
    @pytest.mark.asyncio
    async def test_returns_exit_error_when_graph_not_found(self):
        storage = _mock_storage(None)
        exit_code = await run_inspect(
            storage, "nonexistent-graph",
            console=_no_color_console(),
            task_id="some-task-id",
        )
        assert exit_code == EXIT_ERROR


# ---------------------------------------------------------------------------
# TestRunInspectSingleTaskNotFound
# ---------------------------------------------------------------------------


class TestRunInspectSingleTaskNotFound:
    @pytest.mark.asyncio
    async def test_returns_exit_error_when_task_not_in_graph(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        exit_code = await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id="nonexistent-task-id",
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_error_message_includes_task_id(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id="bad-task-xyz",
        )
        captured = capsys.readouterr()
        assert "bad-task-xyz" in captured.err

    @pytest.mark.asyncio
    async def test_error_message_lists_available_task_ids(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id="bad-task-id",
        )
        captured = capsys.readouterr()
        assert str(task_a.task_id) in captured.err or str(task_b.task_id) in captured.err


# ---------------------------------------------------------------------------
# TestRunInspectSingleTaskExitCodes
# ---------------------------------------------------------------------------


class TestRunInspectSingleTaskExitCodes:
    @pytest.mark.asyncio
    async def test_returns_exit_healthy_for_non_stalled_task(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        exit_code = await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id=str(task_a.task_id),
        )
        assert exit_code == EXIT_HEALTHY

    @pytest.mark.asyncio
    async def test_returns_exit_stalled_for_stalled_task(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)
        exit_code = await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id=str(task_c.task_id),
        )
        assert exit_code == EXIT_STALLED

    @pytest.mark.asyncio
    async def test_returns_exit_healthy_for_pending_task(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        # No results set — task_a has no blob (counts as not stalled)
        storage = _mock_storage(graph)
        exit_code = await run_inspect(
            storage, str(graph.graph_id),
            console=_no_color_console(),
            task_id=str(task_a.task_id),
        )
        assert exit_code == EXIT_HEALTHY


# ---------------------------------------------------------------------------
# TestRunInspectSingleTaskOutput
# ---------------------------------------------------------------------------


class TestRunInspectSingleTaskOutput:
    @pytest.mark.asyncio
    async def test_output_contains_task_id(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        await run_inspect(
            storage, str(graph.graph_id),
            console=console,
            task_id=str(task_a.task_id),
        )
        output = buf.getvalue()
        # Full task ID or last 12 chars should appear
        assert str(task_a.task_id)[-12:] in output or str(task_a.task_id) in output

    @pytest.mark.asyncio
    async def test_output_contains_function_name(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        await run_inspect(
            storage, str(graph.graph_id),
            console=console,
            task_id=str(task_a.task_id),
        )
        output = buf.getvalue()
        assert task_a.function_name in output

    @pytest.mark.asyncio
    async def test_output_contains_graph_id(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        await run_inspect(
            storage, str(graph.graph_id),
            console=console,
            task_id=str(task_a.task_id),
        )
        output = buf.getvalue()
        assert str(graph.graph_id) in output

    @pytest.mark.asyncio
    async def test_output_shows_dependencies_for_non_root_task(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        storage = _mock_storage(graph)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        await run_inspect(
            storage, str(graph.graph_id),
            console=console,
            task_id=str(task_b.task_id),
        )
        output = buf.getvalue()
        # task_b depends on task_a, so task_a's ID should appear in "Depends on"
        assert str(task_a.task_id) in output

    @pytest.mark.asyncio
    async def test_output_shows_none_for_root_task_dependencies(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        await run_inspect(
            storage, str(graph.graph_id),
            console=console,
            task_id=str(task_a.task_id),
        )
        output = buf.getvalue()
        assert "(none)" in output
