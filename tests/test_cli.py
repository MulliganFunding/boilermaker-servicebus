"""Tests for boilermaker.cli — TaskGraph inspection CLI."""

import re
from argparse import ArgumentTypeError
from datetime import datetime, timedelta, UTC
from unittest import mock

import pytest
from boilermaker.cli import build_parser
from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli._output import _short_task_id, format_graph_table
from boilermaker.cli.inspect import run_inspect
from boilermaker.cli.purge import _stream_all_graphs, _stream_eligible_graphs, _validate_older_than, run_purge
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
    storage.load_graph_slim_from_tags = mock.AsyncMock(return_value=graph)
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


async def _async_gen(items):
    for item in items:
        yield item


def _make_purge_storage(
    blob_list: list,
    graph: TaskGraph | None = None,
) -> mock.AsyncMock:
    """Build a storage mock suitable for run_purge tests.

    find_blobs_by_tags yields all blobs in blob_list (simulates all matching the filter).
    list_blobs filters blob_list by the requested prefix.
    load_graph returns the graph (or None).
    delete_blobs_batch returns empty list (no failures) by default.
    """
    storage = mock.AsyncMock()
    storage.task_result_prefix = "task-results"

    async def _list_blobs(prefix):
        for b in blob_list:
            if b.name.startswith(prefix):
                yield b

    async def _find_blobs_by_tags(filter_expression):
        for b in blob_list:
            yield b

    storage.list_blobs = _list_blobs
    storage.find_blobs_by_tags = _find_blobs_by_tags
    storage.load_graph = mock.AsyncMock(return_value=graph)
    storage.delete_blob = mock.AsyncMock()
    storage.delete_blobs_batch = mock.AsyncMock(return_value=[])
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
                    "--older-than", "0",
            ])

    def test_older_than_thirty_one_causes_parse_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--storage-url", "https://example.blob.core.windows.net",
                "--container", "my-container",
                "purge",
                    "--older-than", "31",
            ])

    def test_older_than_non_integer_causes_parse_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--storage-url", "https://example.blob.core.windows.net",
                "--container", "my-container",
                "purge",
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

    async def test_graphs_older_than_cutoff_are_eligible(self):
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        await run_purge(storage, older_than_days=7)
        storage.load_graph.assert_called_once()


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

    async def test_orphaned_blobs_purged_when_force(self, capsys):
        """--force must purge task-result blobs that have no graph.json."""
        graph_id = "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4"
        blob_name = f"task-results/{graph_id}/task-1.json"
        blobs = [_make_blob(blob_name, _old(10))]
        storage = _make_purge_storage(blob_list=blobs)
        code = await run_purge(storage, older_than_days=7, force=True)
        assert code == EXIT_HEALTHY
        storage.load_graph.assert_not_called()
        storage.delete_blobs_batch.assert_called()
        deleted = [name for call in storage.delete_blobs_batch.call_args_list for name in call.args[0]]
        assert blob_name in deleted

    async def test_orphaned_blobs_not_purged_without_force(self, capsys):
        """Without --force orphaned blobs (no graph.json) must still be skipped."""
        graph_id = "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4"
        blobs = [_make_blob(f"task-results/{graph_id}/task-1.json", _old(10))]
        storage = _make_purge_storage(blob_list=blobs)
        code = await run_purge(storage, older_than_days=7, force=False)
        assert code == EXIT_HEALTHY
        storage.delete_blobs_batch.assert_not_called()


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

        storage = _make_purge_storage(blob_list=blobs)
        storage.load_graph = mock.AsyncMock(
            side_effect=lambda gid: graph_eligible if str(gid) == graph_eligible_id else graph_in_progress
        )

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
        storage.delete_blobs_batch.assert_not_called()

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

        # Batch deletion: first call deletes result blobs, second call deletes graph.json
        calls = storage.delete_blobs_batch.call_args_list
        assert len(calls) == 2
        result_batch = calls[0].args[0]
        graph_batch = calls[1].args[0]
        assert task_blob_path in result_batch
        assert graph_json_path in graph_batch

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
        """404 errors are handled by delete_blobs_batch (returns empty list for 404s).
        When batch returns no failures, the graph counts as successfully handled."""
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        # delete_blobs_batch returns [] by default (no failures, 404s handled internally)
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_HEALTHY

    async def test_non_404_error_logged_as_warning(self, capsys):
        """Non-404 batch failures are returned by delete_blobs_batch and reported as warnings."""
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        graph_json_path = f"task-results/{graph_id}/graph.json"
        blobs = [
            _make_blob(graph_json_path, _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        # Simulate batch failure: delete_blobs_batch returns the failed blob names
        # First call is for result blobs (empty list), second call is for graph.json
        storage.delete_blobs_batch = mock.AsyncMock(
            side_effect=[[], [graph_json_path]]
        )
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

        async def _failing_tag_query(filter_expression):
            raise RuntimeError("network failure")
            yield  # make it a generator

        storage.find_blobs_by_tags = _failing_tag_query
        code = await run_purge(storage, older_than_days=7)
        assert code == EXIT_ERROR
        captured = capsys.readouterr()
        assert "ERROR" in captured.err


# ---------------------------------------------------------------------------
# _stream_eligible_graphs: streaming generator tests
# ---------------------------------------------------------------------------


class TestStreamEligibleGraphs:
    async def test_stream_eligible_graphs_yields_old_graphs(self):
        """Tag query returns only old graph's blobs; new graph is absent from
        tag results (the tag filter excluded it). Verify only the old group is yielded."""
        old_graph_id = "aaaa-old-graph"
        new_graph_id = "bbbb-new-graph"
        old_blobs = [
            _make_blob(f"task-results/{old_graph_id}/graph.json", _old(10)),
            _make_blob(f"task-results/{old_graph_id}/task-1.json", _old(10)),
        ]
        all_blobs = old_blobs + [
            _make_blob(f"task-results/{new_graph_id}/graph.json", _new(1)),
            _make_blob(f"task-results/{new_graph_id}/task-1.json", _new(1)),
        ]
        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"
        storage.find_blobs_by_tags = lambda fe: _tag_gen(old_blobs)
        storage.list_blobs = _make_list_blobs(all_blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, group_blobs in _stream_eligible_graphs(storage, cutoff):
            yielded.append((graph_id, group_blobs))

        assert len(yielded) == 1
        assert yielded[0][0] == old_graph_id
        assert len(yielded[0][1]) == 2

    async def test_stream_eligible_graphs_empty_listing(self):
        """Empty async gen. Verify no yields."""
        storage = _make_purge_storage(blob_list=[])
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, group_blobs in _stream_eligible_graphs(storage, cutoff):
            yielded.append((graph_id, group_blobs))

        assert yielded == []

    async def test_stream_eligible_graphs_final_group_emitted(self):
        """Single graph_id, all old. Verify it's yielded (tests the
        final-group logic after the loop)."""
        graph_id = "cccc-only-graph"
        blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(15)),
            _make_blob(f"task-results/{graph_id}/task-1.json", _old(15)),
        ]
        storage = _make_purge_storage(blob_list=blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for gid, group_blobs in _stream_eligible_graphs(storage, cutoff):
            yielded.append((gid, group_blobs))

        assert len(yielded) == 1
        assert yielded[0][0] == graph_id
        assert len(yielded[0][1]) == 2


# ---------------------------------------------------------------------------
# run_purge: batch deletion ordering and failure warnings
# ---------------------------------------------------------------------------


class TestPurgeBatchDeletion:
    def _make_complete_graph(self) -> TaskGraph:
        graph = TaskGraph()
        task = Task.default("do_work")
        graph.add_task(task)
        _set_result(graph, task, TaskStatus.Success)
        return graph

    async def test_batch_deletion_ordering(self):
        """Verify delete_blobs_batch is called twice: first with result blob
        names, second with graph.json names."""
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

        calls = storage.delete_blobs_batch.call_args_list
        assert len(calls) == 2
        # First call: result blobs (non-graph.json)
        result_batch = calls[0].args[0]
        assert task_blob_path in result_batch
        assert graph_json_path not in result_batch
        # Second call: graph.json blobs
        graph_batch = calls[1].args[0]
        assert graph_json_path in graph_batch
        assert task_blob_path not in graph_batch

    async def test_batch_failure_warnings(self, capsys):
        """Mock delete_blobs_batch to return failed names. Verify warnings
        printed to stderr."""
        graph = self._make_complete_graph()
        graph_id = str(graph.graph_id)
        graph_json_path = f"task-results/{graph_id}/graph.json"
        task_blob_path = f"task-results/{graph_id}/task-1.json"
        blobs = [
            _make_blob(graph_json_path, _old(10)),
            _make_blob(task_blob_path, _old(10)),
        ]
        storage = _make_purge_storage(blob_list=blobs, graph=graph)
        # First batch (result blobs) returns a failure; second batch succeeds
        storage.delete_blobs_batch = mock.AsyncMock(
            side_effect=[[task_blob_path], []]
        )

        await run_purge(storage, older_than_days=7)

        captured = capsys.readouterr()
        assert "WARNING" in captured.err
        assert task_blob_path in captured.err


# ---------------------------------------------------------------------------
# Dual-path purge: tag-based discovery, legacy fallback, deduplication
# ---------------------------------------------------------------------------


async def _tag_gen(blobs):
    """Async generator that yields blobs — simulates find_blobs_by_tags success."""
    for b in blobs:
        yield b


def _make_list_blobs(all_blobs):
    """Return a list_blobs mock that filters by prefix."""
    async def _list_blobs(prefix):
        for b in all_blobs:
            if b.name.startswith(prefix):
                yield b
    return _list_blobs


class TestTagBasedPurge:
    """Tests for tag-based candidate discovery in _stream_eligible_graphs."""

    async def test_tag_query_discovers_candidates(self):
        """find_blobs_by_tags yields blobs from 2 old graph_ids.
        Verify both graphs are yielded."""
        graph_id_1 = "graph-1"
        graph_id_2 = "graph-2"

        tagged_blob_1 = _make_blob(f"task-results/{graph_id_1}/task.json", _old(10))
        tagged_blob_2 = _make_blob(f"task-results/{graph_id_2}/task.json", _old(10))

        full_blobs = [
            _make_blob(f"task-results/{graph_id_1}/graph.json", _old(10)),
            _make_blob(f"task-results/{graph_id_1}/task.json", _old(10)),
            _make_blob(f"task-results/{graph_id_2}/graph.json", _old(10)),
            _make_blob(f"task-results/{graph_id_2}/task.json", _old(10)),
        ]

        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"
        storage.find_blobs_by_tags = lambda fe: _tag_gen([tagged_blob_1, tagged_blob_2])
        storage.list_blobs = _make_list_blobs(full_blobs)

        cutoff = datetime.now(UTC) - timedelta(days=7)
        yielded = []
        async for graph_id, _blobs in _stream_eligible_graphs(storage, cutoff):
            yielded.append(graph_id)

        assert sorted(yielded) == ["graph-1", "graph-2"]

    async def test_deduplication_of_candidates(self):
        """find_blobs_by_tags yields multiple blobs from the same graph_id.
        Verify the graph is only listed and yielded once."""
        graph_id = "dup-graph"

        tagged_blob_1 = _make_blob(f"task-results/{graph_id}/task-1.json", _old(10))
        tagged_blob_2 = _make_blob(f"task-results/{graph_id}/task-2.json", _old(10))
        all_blobs = [
            _make_blob(f"task-results/{graph_id}/graph.json", _old(10)),
            tagged_blob_1,
            tagged_blob_2,
        ]

        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"
        storage.find_blobs_by_tags = lambda fe: _tag_gen([tagged_blob_1, tagged_blob_2])
        storage.list_blobs = _make_list_blobs(all_blobs)

        cutoff = datetime.now(UTC) - timedelta(days=7)
        yielded = []
        async for graph_id_out, _blobs in _stream_eligible_graphs(storage, cutoff):
            yielded.append(graph_id_out)

        assert yielded == ["dup-graph"]


# ---------------------------------------------------------------------------
# _stream_all_graphs: UUID7-based graph discovery
# ---------------------------------------------------------------------------


def _make_uuid7_str(ts_ms: int) -> str:
    """Construct a valid UUID7 string with a specific millisecond timestamp.

    UUID7 layout: 48-bit ms timestamp | 4-bit version (0x7) | 12-bit rand
                  | 2-bit variant (0b10) | 62-bit rand.
    """
    import random as _rng

    rand_a = _rng.getrandbits(12)
    rand_b = _rng.getrandbits(62)
    uuid_int = (ts_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    h = f"{uuid_int:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


class TestStreamAllGraphs:
    """Tests for the _stream_all_graphs async generator."""

    def _make_storage_with_blobs(self, blob_list: list) -> mock.AsyncMock:
        """Build a minimal storage mock for _stream_all_graphs."""
        storage = mock.AsyncMock()
        storage.task_result_prefix = "task-results"
        storage.list_blobs = lambda prefix: _async_gen(blob_list)
        return storage

    async def test_old_uuid7_graphs_are_yielded(self):
        """Blobs whose graph_id is a UUID7 with a timestamp before cutoff are yielded."""
        old_ts_ms = int((datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000)
        old_uuid = _make_uuid7_str(old_ts_ms)

        blobs = [
            _make_blob(f"task-results/{old_uuid}/graph.json", _old(10)),
            _make_blob(f"task-results/{old_uuid}/task-1.json", _old(10)),
        ]
        storage = self._make_storage_with_blobs(blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, group_blobs in _stream_all_graphs(storage, cutoff):
            yielded.append((graph_id, group_blobs))

        assert len(yielded) == 1
        assert yielded[0][0] == old_uuid
        assert len(yielded[0][1]) == 2

    async def test_early_exit_on_recent_uuid7(self):
        """When a recent UUID7 graph_id is encountered, the generator stops early."""
        old_ts_ms = int((datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000)
        old_uuid = _make_uuid7_str(old_ts_ms)

        new_ts_ms = int(datetime.now(UTC).timestamp() * 1000)
        new_uuid = _make_uuid7_str(new_ts_ms)

        # UUID7s are lexicographically ordered by time, so old comes first
        blobs = [
            _make_blob(f"task-results/{old_uuid}/graph.json", _old(10)),
            _make_blob(f"task-results/{old_uuid}/task-1.json", _old(10)),
            _make_blob(f"task-results/{new_uuid}/graph.json", _new(0)),
            _make_blob(f"task-results/{new_uuid}/task-1.json", _new(0)),
        ]
        storage = self._make_storage_with_blobs(blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, _group_blobs in _stream_all_graphs(storage, cutoff):
            yielded.append(graph_id)

        # Only the old graph should be yielded; the new one triggers early exit
        assert len(yielded) == 1
        assert yielded[0] == old_uuid

    async def test_empty_container(self):
        """No blobs yields nothing."""
        storage = self._make_storage_with_blobs([])
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, _group_blobs in _stream_all_graphs(storage, cutoff):
            yielded.append(graph_id)

        assert yielded == []

    async def test_non_uuid_graph_id_is_skipped(self, caplog):
        """A non-UUID graph_id is skipped with a warning, not a crash."""
        import logging

        old_ts_ms = int((datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000)
        old_uuid = _make_uuid7_str(old_ts_ms)

        # "not-a-uuid" sorts lexicographically before UUID7 strings starting with "0"
        blobs = [
            _make_blob("task-results/not-a-uuid/graph.json", _old(10)),
            _make_blob(f"task-results/{old_uuid}/graph.json", _old(10)),
        ]
        storage = self._make_storage_with_blobs(blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        with caplog.at_level(logging.WARNING, logger="boilermaker.cli"):
            yielded = []
            async for graph_id, _group_blobs in _stream_all_graphs(storage, cutoff):
                yielded.append(graph_id)

        # The non-UUID graph is skipped; the old UUID graph is yielded
        assert old_uuid in yielded
        assert "not-a-uuid" not in yielded
        # Warning should be logged about the unparseable graph_id
        assert any("not-a-uuid" in record.message for record in caplog.records)

    async def test_all_graphs_newer_than_cutoff(self):
        """All graph_ids have UUID7 timestamps >= cutoff. Nothing is yielded."""
        new_ts_ms = int(datetime.now(UTC).timestamp() * 1000)
        new_uuid = _make_uuid7_str(new_ts_ms)

        blobs = [
            _make_blob(f"task-results/{new_uuid}/graph.json", _new(0)),
            _make_blob(f"task-results/{new_uuid}/task-1.json", _new(0)),
        ]
        storage = self._make_storage_with_blobs(blobs)
        cutoff = datetime.now(UTC) - timedelta(days=7)

        yielded = []
        async for graph_id, _group_blobs in _stream_all_graphs(storage, cutoff):
            yielded.append(graph_id)

        assert yielded == []


# ---------------------------------------------------------------------------
# Purge: --all-graphs argument parsing
# ---------------------------------------------------------------------------


class TestPurgeAllGraphsArgumentParsing:
    def test_all_graphs_flag_is_parsed(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "purge",
            "--older-than", "7",
            "--all-graphs",
        ])
        assert args.all_graphs is True

    def test_all_graphs_default_is_false(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "purge",
            "--older-than", "7",
        ])
        assert args.all_graphs is False

    def test_all_graphs_combines_with_dry_run(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", "https://example.blob.core.windows.net",
            "--container", "my-container",
            "purge",
            "--older-than", "7",
            "--all-graphs",
            "--dry-run",
        ])
        assert args.all_graphs is True
        assert args.dry_run is True


# ---------------------------------------------------------------------------
# run_purge: --all-graphs routing
# ---------------------------------------------------------------------------


class TestPurgeAllGraphsRouting:
    """Verify run_purge selects the correct graph stream based on all_graphs."""

    async def test_all_graphs_true_uses_stream_all_graphs(self):
        """When all_graphs=True, _stream_all_graphs is called."""
        storage = _make_purge_storage(blob_list=[])

        async def _empty_gen(*args, **kwargs):
            return
            yield  # make it an async generator

        with (
            mock.patch("boilermaker.cli.purge._stream_all_graphs", side_effect=_empty_gen) as mock_all,
            mock.patch("boilermaker.cli.purge._stream_eligible_graphs", side_effect=_empty_gen) as mock_eligible,
        ):
            await run_purge(storage, older_than_days=7, all_graphs=True)

        mock_all.assert_called_once()
        mock_eligible.assert_not_called()

    async def test_all_graphs_false_uses_stream_eligible_graphs(self):
        """When all_graphs=False (default), _stream_eligible_graphs is called."""
        storage = _make_purge_storage(blob_list=[])

        async def _empty_gen(*args, **kwargs):
            return
            yield  # make it an async generator

        with (
            mock.patch("boilermaker.cli.purge._stream_all_graphs", side_effect=_empty_gen) as mock_all,
            mock.patch("boilermaker.cli.purge._stream_eligible_graphs", side_effect=_empty_gen) as mock_eligible,
        ):
            await run_purge(storage, older_than_days=7, all_graphs=False)

        mock_eligible.assert_called_once()
        mock_all.assert_not_called()
