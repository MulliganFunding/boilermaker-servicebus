"""Tests for boilermaker.cli — TaskGraph inspection CLI."""

import re
from unittest import mock

import pytest
from boilermaker.cli import (
    _short_task_id,
    build_parser,
    EXIT_ERROR,
    EXIT_HEALTHY,
    EXIT_STALLED,
    format_graph_table,
    inspect_graph,
)
from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus
from boilermaker.task.task_id import TaskId

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_graph_with_tasks() -> tuple[TaskGraph, Task, Task, Task]:
    """Build a graph with three children: A -> B -> C."""
    graph = TaskGraph()
    task_a = Task.default("business_search")
    task_b = Task.default("business_callback")
    task_c = Task.default("business_report")
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
    """Return a mock storage whose load_graph returns the given graph."""
    storage = mock.AsyncMock()
    storage.load_graph = mock.AsyncMock(return_value=graph)
    return storage


# ---------------------------------------------------------------------------
# _short_task_id
# ---------------------------------------------------------------------------


class TestShortTaskId:
    def test_truncates_long_id(self):
        tid = TaskId("019750a3-bfac-7e3a-b4cd-4c3102c9f3f2")
        assert _short_task_id(tid) == "4c3102c9f3f2"

    def test_preserves_short_id(self):
        tid = TaskId("abcd1234")
        assert _short_task_id(tid) == "abcd1234"


# ---------------------------------------------------------------------------
# format_graph_table
# ---------------------------------------------------------------------------


class TestFormatGraphTable:
    def test_table_contains_header(self):
        graph, task_a, _, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "Task ID (short)" in output
        assert "Function" in output
        assert "Status" in output
        assert "Type" in output

    def test_table_shows_child_tasks(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "business_search" in output
        assert "business_callback" in output
        assert "business_report" in output
        assert "child" in output

    def test_table_shows_fail_children(self):
        graph = TaskGraph()
        parent_task = Task.default("do_work")
        fail_task = Task.default("handle_failure")
        graph.add_task(parent_task)
        graph.add_failure_callback(parent_task.task_id, fail_task)
        _set_result(graph, parent_task, TaskStatus.Failure)
        _set_result(graph, fail_task, TaskStatus.Pending)
        output = format_graph_table(graph)
        assert "fail_child" in output
        assert "handle_failure" in output

    def test_stalled_tasks_marked(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Retry)
        output = format_graph_table(graph)
        assert "** STALLED **" in output

    def test_no_stalled_marker_for_healthy_graph(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "** STALLED **" not in output

    def test_summary_line_present(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "Complete: True" in output
        assert "Has failures: False" in output
        assert "Stalled tasks: 0" in output

    def test_summary_shows_stalled_count(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = format_graph_table(graph)
        assert "Stalled tasks: 1" in output

    def test_no_blob_status_shown_for_missing_result(self):
        graph, task_a, _, _ = _make_graph_with_tasks()
        # Only set result for task_a; task_b and task_c have no result blobs
        _set_result(graph, task_a, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "NO BLOB" in output


# ---------------------------------------------------------------------------
# inspect_graph — exit codes
# ---------------------------------------------------------------------------


class TestInspectGraphExitCodes:
    async def test_returns_error_when_graph_not_found(self):
        storage = _mock_storage(graph=None)
        code = await inspect_graph(storage, "nonexistent-graph-id")
        assert code == EXIT_ERROR

    async def test_returns_healthy_for_completed_graph(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        code = await inspect_graph(storage, str(graph.graph_id))
        assert code == EXIT_HEALTHY

    async def test_returns_stalled_for_stuck_tasks(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Started)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        code = await inspect_graph(storage, str(graph.graph_id))
        assert code == EXIT_STALLED

    async def test_returns_error_when_recover_missing_sb_args(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        code = await inspect_graph(
            storage,
            str(graph.graph_id),
            recover=True,
            sb_connection_string=None,
            sb_queue_name=None,
        )
        assert code == EXIT_ERROR


# ---------------------------------------------------------------------------
# inspect_graph — error output
# ---------------------------------------------------------------------------


class TestInspectGraphErrorOutput:
    async def test_graph_not_found_prints_to_stderr(self, capsys):
        storage = _mock_storage(graph=None)
        await inspect_graph(storage, "missing-graph")
        captured = capsys.readouterr()
        assert "ERROR: Graph missing-graph not found in storage." in captured.err

    async def test_recover_missing_args_prints_to_stderr(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Retry)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await inspect_graph(storage, str(graph.graph_id), recover=True)
        captured = capsys.readouterr()
        assert "--recover requires --sb-connection-string and --sb-queue-name" in captured.err


# ---------------------------------------------------------------------------
# inspect_graph — table output verification
# ---------------------------------------------------------------------------


class TestInspectGraphOutput:
    async def test_prints_table_to_stdout(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await inspect_graph(storage, str(graph.graph_id))
        captured = capsys.readouterr()
        assert "business_search" in captured.out
        assert "Complete: True" in captured.out


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgumentParsing:
    def test_inspect_parses_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            "inspect", "my-graph-id",
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
        ])
        assert args.command == "inspect"
        assert args.graph_id == "my-graph-id"
        assert args.storage_url == "https://example.blob.core.windows.net"
        assert args.container == "my-container"
        assert args.recover is False
        assert args.verbose is False

    def test_inspect_parses_recover_flags(self):
        parser = build_parser()
        args = parser.parse_args([
            "inspect", "my-graph-id",
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "--recover",
            "--sb-connection-string", "Endpoint=sb://test",
            "--sb-queue-name", "my-queue",
        ])
        assert args.recover is True
        assert args.sb_connection_string == "Endpoint=sb://test"
        assert args.sb_queue_name == "my-queue"

    def test_inspect_parses_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args([
            "inspect", "my-graph-id",
            "--storage-url", "https://x.blob.core.windows.net",
            "--container", "c",
            "-v",
        ])
        assert args.verbose is True

    def test_no_command_returns_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_missing_required_args_exits(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["inspect"])


# ---------------------------------------------------------------------------
# inspect_graph — recovery code path
# ---------------------------------------------------------------------------


def _mock_service_bus() -> mock.AsyncMock:
    """Return a mock AzureServiceBus with send_message and close."""
    sb = mock.AsyncMock()
    sb.send_message = mock.AsyncMock(return_value=[1])
    sb.close = mock.AsyncMock()
    return sb


class TestInspectGraphRecovery:
    async def test_recovery_publishes_stalled_tasks_with_correct_message_id(self):
        """Recovery sends messages for stalled tasks with task_id:recovery:timestamp format."""
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Pending)

        storage = _mock_storage(graph)
        mock_sb = _mock_service_bus()

        with mock.patch("boilermaker.cli.AzureServiceBus", return_value=mock_sb):
            code = await inspect_graph(
                storage,
                str(graph.graph_id),
                recover=True,
                sb_connection_string="Endpoint=sb://test",
                sb_queue_name="test-queue",
            )

        assert code == EXIT_STALLED
        mock_sb.send_message.assert_called_once()
        call_kwargs = mock_sb.send_message.call_args
        # Verify the message body is the stalled task's serialized model
        assert call_kwargs[0][0] == task_b.model_dump_json()
        # Verify message_id matches task_id:recovery:<timestamp> format
        recovery_msg_id = call_kwargs[1]["unique_msg_id"]
        assert re.match(rf"^{re.escape(str(task_b.task_id))}:recovery:\d+$", recovery_msg_id)

    async def test_recovery_publishes_multiple_stalled_tasks(self):
        """Recovery sends messages for all stalled tasks, not just the first."""
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Started)

        storage = _mock_storage(graph)
        mock_sb = _mock_service_bus()

        with mock.patch("boilermaker.cli.AzureServiceBus", return_value=mock_sb):
            code = await inspect_graph(
                storage,
                str(graph.graph_id),
                recover=True,
                sb_connection_string="Endpoint=sb://test",
                sb_queue_name="test-queue",
            )

        assert code == EXIT_STALLED
        assert mock_sb.send_message.call_count == 2

    async def test_recovery_skips_terminal_tasks(self):
        """Tasks in terminal states (Success, Failure, Pending) are not recovered."""
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Failure)
        _set_result(graph, task_c, TaskStatus.Pending)

        storage = _mock_storage(graph)
        mock_sb = _mock_service_bus()

        with mock.patch("boilermaker.cli.AzureServiceBus", return_value=mock_sb):
            code = await inspect_graph(
                storage,
                str(graph.graph_id),
                recover=True,
                sb_connection_string="Endpoint=sb://test",
                sb_queue_name="test-queue",
            )

        # No stalled tasks means EXIT_HEALTHY and no send_message calls
        assert code == EXIT_HEALTHY
        mock_sb.send_message.assert_not_called()

    async def test_recovery_closes_service_bus_client(self):
        """Service bus client is closed even if a send fails."""
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Retry)
        _set_result(graph, task_c, TaskStatus.Pending)

        storage = _mock_storage(graph)
        mock_sb = _mock_service_bus()
        mock_sb.send_message.side_effect = RuntimeError("Connection lost")

        with mock.patch("boilermaker.cli.AzureServiceBus", return_value=mock_sb):
            code = await inspect_graph(
                storage,
                str(graph.graph_id),
                recover=True,
                sb_connection_string="Endpoint=sb://test",
                sb_queue_name="test-queue",
            )

        assert code == EXIT_STALLED
        mock_sb.close.assert_awaited_once()
