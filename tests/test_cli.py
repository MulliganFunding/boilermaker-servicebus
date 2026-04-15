"""Tests for boilermaker.cli — TaskGraph inspection CLI."""

import re
from argparse import ArgumentTypeError
from datetime import datetime, timedelta, UTC
from unittest import mock

import pytest
from azure.core.exceptions import HttpResponseError
from boilermaker.cli import build_parser
from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli._output import _short_task_id, format_graph_table
from boilermaker.cli.inspect import run_inspect
from boilermaker.cli.purge import _validate_older_than, run_purge
from boilermaker.cli.recover import run_recover
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
        assert "Task ID" in output
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
        assert "STALLED" in output

    def test_no_stalled_marker_for_healthy_graph(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = format_graph_table(graph)
        assert "STALLED" not in output

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
# run_inspect — exit codes
# ---------------------------------------------------------------------------


class TestInspectGraphExitCodes:
    async def test_returns_error_when_graph_not_found(self):
        storage = _mock_storage(graph=None)
        code = await run_inspect(storage, "nonexistent-graph-id")
        assert code == EXIT_ERROR

    async def test_returns_healthy_for_completed_graph(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        code = await run_inspect(storage, str(graph.graph_id))
        assert code == EXIT_HEALTHY

    async def test_returns_stalled_for_stuck_tasks(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Started)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        code = await run_inspect(storage, str(graph.graph_id))
        assert code == EXIT_STALLED

    async def test_returns_error_when_recover_missing_sb_args(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        code = await run_recover(
            storage,
            str(graph.graph_id),
            sb_namespace_url=None,
            sb_queue_name=None,
        )
        assert code == EXIT_ERROR


# ---------------------------------------------------------------------------
# run_inspect — error output
# ---------------------------------------------------------------------------


class TestInspectGraphErrorOutput:
    async def test_graph_not_found_prints_to_stderr(self, capsys):
        storage = _mock_storage(graph=None)
        await run_inspect(storage, "missing-graph")
        captured = capsys.readouterr()
        assert "ERROR: Graph missing-graph not found in storage." in captured.err

    async def test_recover_missing_args_prints_to_stderr(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Retry)
        _set_result(graph, task_c, TaskStatus.Pending)
        storage = _mock_storage(graph)
        await run_recover(storage, str(graph.graph_id), sb_namespace_url=None, sb_queue_name=None)
        captured = capsys.readouterr()
        assert "--recover requires --sb-namespace-url and --sb-queue-name" in captured.err


# ---------------------------------------------------------------------------
# run_inspect — table output verification
# ---------------------------------------------------------------------------


class TestInspectGraphOutput:
    async def test_prints_table_to_stdout(self, capsys):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_inspect(storage, str(graph.graph_id))
        captured = capsys.readouterr()
        assert "business_search" in captured.out
        assert "Complete" in captured.out


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgumentParsing:
    def test_inspect_parses_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "inspect", "--graph", "my-graph-id",
        ])
        assert args.command == "inspect"
        assert args.graph == "my-graph-id"
        assert args.storage_url == "https://example.blob.core.windows.net"
        assert args.container == "my-container"
        assert args.verbose is False

    def test_recover_parses_sb_flags(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "recover",
            "--graph", "my-graph-id",
            "--sb-namespace-url", "https://test.servicebus.windows.net",
            "--sb-queue-name", "my-queue",
        ])
        assert args.command == "recover"
        assert args.sb_namespace_url == "https://test.servicebus.windows.net"
        assert args.sb_queue_name == "my-queue"

    def test_inspect_parses_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://x.blob.core.windows.net",
            "--container", "c",
            "-v",
            "inspect", "--graph", "my-graph-id",
        ])
        assert args.verbose is True

    def test_no_command_returns_none(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_missing_graph_flag_does_not_exit_at_parse_time(self):
        # With the new parser, inspect without --graph is validated post-parse
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://x.blob.core.windows.net",
            "--container", "c",
            "inspect",
        ])
        assert args.command == "inspect"
        assert args.graph is None


# ---------------------------------------------------------------------------
# run_recover — recovery code path
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

        with mock.patch("boilermaker.cli.recover.AzureServiceBus", return_value=mock_sb):
            code = await run_recover(
                storage,
                str(graph.graph_id),
                sb_namespace_url="https://test.servicebus.windows.net",
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

        with mock.patch("boilermaker.cli.recover.AzureServiceBus", return_value=mock_sb):
            code = await run_recover(
                storage,
                str(graph.graph_id),
                sb_namespace_url="https://test.servicebus.windows.net",
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

        with mock.patch("boilermaker.cli.recover.AzureServiceBus", return_value=mock_sb):
            code = await run_recover(
                storage,
                str(graph.graph_id),
                sb_namespace_url="https://test.servicebus.windows.net",
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

        with mock.patch("boilermaker.cli.recover.AzureServiceBus", return_value=mock_sb):
            code = await run_recover(
                storage,
                str(graph.graph_id),
                sb_namespace_url="https://test.servicebus.windows.net",
                sb_queue_name="test-queue",
            )

        assert code == EXIT_STALLED
        mock_sb.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Purge: helpers
# ---------------------------------------------------------------------------


def _make_blob(name: str, last_modified: datetime) -> mock.MagicMock:
    """Build a minimal BlobProperties-like mock."""
    blob = mock.MagicMock()
    blob.name = name
    blob.last_modified = last_modified
    return blob


def _make_azure_blob_error(status_code: int) -> mock.MagicMock:
    """Build an AzureBlobError mock with the given HTTP status code."""
    from aio_azure_clients_toolbox.clients.azure_blobs import AzureBlobError

    http_error = mock.MagicMock(spec=HttpResponseError)
    http_error.reason = "Not Found"
    http_error.status_code = status_code
    http_error.message = f"HTTP {status_code}"
    return AzureBlobError(http_error)


async def _async_gen(items):
    for item in items:
        yield item


def _make_purge_storage(
    blob_list: list,
    graph: TaskGraph | None = None,
) -> mock.AsyncMock:
    """Build a storage mock suitable for run_purge tests.

    list_blobs returns the blob_list as an async generator.
    load_graph returns the graph (or None).
    delete_blob does nothing by default.
    """
    storage = mock.AsyncMock()
    storage.task_result_prefix = "task-results"
    storage.list_blobs = lambda prefix: _async_gen(blob_list)
    storage.load_graph = mock.AsyncMock(return_value=graph)
    storage.delete_blob = mock.AsyncMock()
    return storage


def _old(days: int = 10) -> datetime:
    """Return a UTC datetime that is `days` days in the past."""
    return datetime.now(UTC) - timedelta(days=days)


def _new(hours: int = 1) -> datetime:
    """Return a UTC datetime that is `hours` hours in the past (recent)."""
    return datetime.now(UTC) - timedelta(hours=hours)


# ---------------------------------------------------------------------------
# _validate_older_than
# ---------------------------------------------------------------------------


class TestValidateOlderThan:
    def test_accepts_minimum_value(self):
        assert _validate_older_than("1") == 1

    def test_accepts_maximum_value(self):
        assert _validate_older_than("30") == 30

    def test_accepts_midrange_value(self):
        assert _validate_older_than("15") == 15

    @pytest.mark.parametrize("invalid_value", ["0", "31", "-1", "seven", "7.5"])
    def test_rejects_invalid_values(self, invalid_value):
        with pytest.raises(ArgumentTypeError):
            _validate_older_than(invalid_value)


# ---------------------------------------------------------------------------
# Purge: argument parsing
# ---------------------------------------------------------------------------
class TestPurgeArgumentParsing:
    def test_parses_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "purge",
            "--task-results",
            "--older-than", "7",
        ])
        assert args.command == "purge"
        assert args.storage_url == "https://example.blob.core.windows.net"
        assert args.container == "my-container"
        assert args.older_than == 7
        assert args.dry_run is False
        assert args.verbose is False

    def test_parses_dry_run_flag(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "purge",
            "--task-results",
            "--older-than", "7",
            "--dry-run",
        ])
        assert args.dry_run is True

    def test_parses_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "-v",
            "purge",
            "--task-results",
            "--older-than", "7",
        ])
        assert args.verbose is True

    def test_older_than_zero_causes_parse_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--storage-url", "https://example.blob.core.windows.net",
                "--container", "my-container",
                "purge",
                "--task-results",
                "--older-than", "0",
            ])

    def test_older_than_thirty_one_causes_parse_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--storage-url", "https://example.blob.core.windows.net",
                "--container", "my-container",
                "purge",
                "--task-results",
                "--older-than", "31",
            ])

    def test_older_than_non_integer_causes_parse_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--storage-url", "https://example.blob.core.windows.net",
                "--container", "my-container",
                "purge",
                "--task-results",
                "--older-than", "abc",
            ])


# ---------------------------------------------------------------------------
# run_purge: empty container
# ---------------------------------------------------------------------------


class TestPurgeEmptyContainer:
    async def test_empty_container_returns_healthy(self, capsys):
        storage = _make_purge_storage(blob_list=[])
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY

    async def test_empty_container_prints_nothing_to_purge(self, capsys):
        storage = _make_purge_storage(blob_list=[])
        await run_purge(storage, older_than_days=7)
        captured = capsys.readouterr()
        assert "Nothing to purge." in captured.out


# ---------------------------------------------------------------------------
# run_purge: age eligibility
# ---------------------------------------------------------------------------


class TestPurgeAgeEligibility:
    def _make_complete_graph(self) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("do_work")
        graph.add_task(task)
        _set_result(graph, task, TaskStatus.Success)
        return graph

    async def test_graphs_newer_than_cutoff_are_skipped(self, capsys):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _new()),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY
        # load_graph should NOT have been called (age filter excludes it)
        storage.load_graph.assert_not_called()

    async def test_graphs_older_than_cutoff_are_eligible(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7)
        storage.load_graph.assert_called_once()

    async def test_graph_with_any_recent_blob_is_skipped(self):
        """If max(last_modified) is newer than cutoff, skip the whole graph."""
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(15)),
            _make_blob(f"task-results/{graph_id}/task-1.json", _new(1)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7)
        # load_graph should NOT be called because max(last_modified) is recent
        storage.load_graph.assert_not_called()


# ---------------------------------------------------------------------------
# run_purge: missing graph.json
# ---------------------------------------------------------------------------


class TestPurgeMissingGraphJson:
    async def test_graph_without_graph_json_is_skipped(self, capsys):
        graph_id = "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4"
        blobs = [
            _make_blob(f"task-results/{graph_id}/task-1.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY
        captured = capsys.readouterr()
        assert "no graph.json" in captured.err
        storage.load_graph.assert_not_called()
        storage.delete_blob.assert_not_called()


# ---------------------------------------------------------------------------
# run_purge: in-progress safety check
# ---------------------------------------------------------------------------


class TestPurgeInProgressSafetyCheck:
    def _make_graph_with_status(self, status: TaskStatus) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("work")
        graph.add_task(task)
        _set_result(graph, task, status)
        return graph

    async def test_scheduled_task_causes_skip(self, capsys):
        graph = self._make_graph_with_status(TaskStatus.Scheduled)
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_STALLED
        captured = capsys.readouterr()
        assert "in-progress tasks" in captured.err
        storage.delete_blob.assert_not_called()

    async def test_started_task_causes_skip(self, capsys):
        graph = self._make_graph_with_status(TaskStatus.Started)
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_STALLED
        storage.delete_blob.assert_not_called()

    async def test_retry_task_causes_skip(self, capsys):
        graph = self._make_graph_with_status(TaskStatus.Retry)
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_STALLED
        storage.delete_blob.assert_not_called()

    async def test_in_progress_skip_message_sent_to_stderr(self, capsys):
        graph = self._make_graph_with_status(TaskStatus.Scheduled)
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7)
        captured = capsys.readouterr()
        assert graph_id in captured.err

    async def test_skipped_in_progress_count_excludes_age_filtered_graphs(self, capsys):
        """Summary line counts only graphs skipped for in-progress tasks, not age-filtered ones."""
        # Graph A: old and eligible for deletion (all tasks complete)
        graph_eligible = TaskGraph()
        task_eligible = Task.default("completed_work")
        graph_eligible.add_task(task_eligible)
        _set_result(graph_eligible, task_eligible, TaskStatus.Success)
        graph_eligible_id = str(graph_eligible.graph_id)

        # Graph B: old but has in-progress tasks — must be skipped
        graph_in_progress = self._make_graph_with_status(TaskStatus.Started)
        graph_in_progress_id = str(graph_in_progress.graph_id)

        blobs = [
            _make_blob(f"task-results/{graph_eligible_id}/graph.json", _old(10)),
            _make_blob(f"task-results/{graph_in_progress_id}/graph.json", _old(10)),
        ]

        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"
        storage.list_blobs = lambda prefix: _async_gen(blobs)
        storage.load_graph = mock.AsyncMock(
            side_effect=lambda gid: graph_eligible if str(gid) == graph_eligible_id else graph_in_progress
        )
        storage.delete_blob = mock.AsyncMock()

        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_STALLED
        captured = capsys.readouterr()
        assert "Skipped 1 graph(s) due to in-progress tasks." in captured.out


# ---------------------------------------------------------------------------
# run_purge: dry-run
# ---------------------------------------------------------------------------


class TestPurgeDryRun:
    def _make_complete_graph(self) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("do_work")
        graph.add_task(task)
        _set_result(graph, task, TaskStatus.Success)
        return graph

    async def test_dry_run_does_not_call_delete_blob(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
            _make_blob(f"task-results/{graph_id}/task-1.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7, dry_run=True)
        assert code == EXIT_HEALTHY
        storage.delete_blob.assert_not_called()

    async def test_dry_run_output_contains_dry_run_marker(self, capsys):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7, dry_run=True)
        captured = capsys.readouterr()
        assert "[DRY RUN]" in captured.out

    async def test_dry_run_output_lists_eligible_graph(self, capsys):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7, dry_run=True)
        captured = capsys.readouterr()
        assert graph_id in captured.out


# ---------------------------------------------------------------------------
# run_purge: deletion order and success summary
# ---------------------------------------------------------------------------


class TestPurgeDeletionOrder:
    def _make_complete_graph(self) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("do_work")
        graph.add_task(task)
        _set_result(graph, task, TaskStatus.Success)
        return graph

    async def test_result_blobs_deleted_before_graph_json(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        graph_json_path = f"task-results/{graph_id}/graph.json"
        task_blob_path = f"task-results/{graph_id}/task-1.json"
        blobs = [
            _make_blob(graph_json_path, _old(10)),
            _make_blob(task_blob_path, _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7)

        delete_calls = [call.args[0] for call in storage.delete_blob.call_args_list]
        assert delete_calls[-1] == graph_json_path
        assert task_blob_path in delete_calls[:-1]

    async def test_successful_deletion_summary_on_stdout(self, capsys):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
            _make_blob(f"task-results/{graph_id}/task-1.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY
        captured = capsys.readouterr()
        assert "Deleted" in captured.out
        assert "blobs" in captured.out
        assert "graphs" in captured.out

    async def test_successful_deletion_returns_healthy(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY


# ---------------------------------------------------------------------------
# run_purge: 404 AzureBlobError treated as no-op
# ---------------------------------------------------------------------------


class TestPurge404NoOp:
    def _make_complete_graph(self) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("do_work")
        graph.add_task(task)
        _set_result(graph, task, TaskStatus.Success)
        return graph

    async def test_404_on_delete_is_treated_as_success(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        storage.delete_blob.side_effect = _make_azure_blob_error(404)
        code = await run_purge(storage, older_than_days=7)
        # 404 is a no-op; the graph still counts as successfully handled
        assert code == EXIT_HEALTHY

    async def test_non_404_error_logged_as_warning(self, capsys):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        storage.delete_blob.side_effect = _make_azure_blob_error(500)
        code = await run_purge(storage, older_than_days=7)
        captured = capsys.readouterr()
        assert "WARNING" in captured.err or "Failed" in captured.err
        # All deletions failed for the only graph, so EXIT_ERROR
        assert code == EXIT_ERROR


# ---------------------------------------------------------------------------
# run_purge: listing error
# ---------------------------------------------------------------------------


class TestPurgeListingError:
    async def test_listing_error_returns_exit_error(self, capsys):
        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"

        async def _failing_gen(prefix):
            raise RuntimeError("network failure")
            yield  # make it a generator

        storage.list_blobs = _failing_gen
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_ERROR
        captured = capsys.readouterr()
        assert "ERROR" in captured.err
