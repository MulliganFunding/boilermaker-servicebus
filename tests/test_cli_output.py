"""Tests for Rich output rendering functions in boilermaker.cli._output."""

from io import StringIO

from boilermaker.cli._output import (
    render_graph_summary,
    render_purge_plan,
    render_task_table,
    status_style,
)
from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus
from boilermaker.task.task_id import truncate_task_id
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

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


def _render_to_str(renderable, width: int = 120) -> str:
    """Render a Rich renderable to a plain-text string without color codes."""
    buf = StringIO()
    console = Console(file=buf, no_color=True, width=width)
    console.print(renderable)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# TestStatusStyle
# ---------------------------------------------------------------------------


class TestStatusStyle:
    def test_success_returns_green_with_star_prefix(self):
        style, prefix = status_style(TaskStatus.Success, is_stalled=False)
        assert style == "green"
        assert prefix == "*"

    def test_failure_returns_red_with_bang_prefix(self):
        style, prefix = status_style(TaskStatus.Failure, is_stalled=False)
        assert style == "red"
        assert prefix == "!"

    def test_retries_exhausted_returns_red(self):
        style, prefix = status_style(TaskStatus.RetriesExhausted, is_stalled=False)
        assert style == "red"
        assert prefix == "!"

    def test_deadlettered_returns_red(self):
        style, prefix = status_style(TaskStatus.Deadlettered, is_stalled=False)
        assert style == "red"
        assert prefix == "!"

    def test_pending_returns_yellow_with_space_prefix(self):
        style, prefix = status_style(TaskStatus.Pending, is_stalled=False)
        assert style == "yellow"
        assert prefix == " "

    def test_scheduled_returns_yellow(self):
        style, prefix = status_style(TaskStatus.Scheduled, is_stalled=False)
        assert style == "yellow"

    def test_stalled_returns_bold_red_with_bang_prefix(self):
        style, prefix = status_style(TaskStatus.Retry, is_stalled=True)
        assert style == "bold red"
        assert prefix == "!"

    def test_stalled_overrides_success_style(self):
        # A task that is both stalled and Success (edge case) still shows stalled style
        style, prefix = status_style(TaskStatus.Success, is_stalled=True)
        assert style == "bold red"
        assert prefix == "!"


# ---------------------------------------------------------------------------
# TestRenderGraphSummary
# ---------------------------------------------------------------------------


class TestRenderGraphSummary:
    def test_returns_rich_panel(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        panel = render_graph_summary(graph)
        assert isinstance(panel, Panel)

    def test_panel_title_contains_graph_id(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        panel = render_graph_summary(graph)
        output = _render_to_str(panel)
        assert str(graph.graph_id) in output

    def test_complete_graph_shows_complete_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = _render_to_str(render_graph_summary(graph))
        assert "Complete" in output

    def test_in_progress_graph_shows_in_progress_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Started)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = _render_to_str(render_graph_summary(graph))
        assert "In Progress" in output

    def test_graph_with_failures_shows_has_failures_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        _set_result(graph, task_b, TaskStatus.Pending)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = _render_to_str(render_graph_summary(graph))
        assert "Has Failures" in output

    def test_completion_fraction_shown(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = _render_to_str(render_graph_summary(graph))
        assert "2/3" in output

    def test_stalled_count_shown(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Scheduled)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = _render_to_str(render_graph_summary(graph))
        assert "Stalled: 1" in output

    def test_failure_count_shown(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        _set_result(graph, task_b, TaskStatus.Pending)
        _set_result(graph, task_c, TaskStatus.Pending)
        output = _render_to_str(render_graph_summary(graph))
        assert "Failures: 1" in output


# ---------------------------------------------------------------------------
# TestRenderTaskTable
# ---------------------------------------------------------------------------


class TestRenderTaskTable:
    def test_returns_rich_table(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        table = render_task_table(graph)
        assert isinstance(table, Table)

    def test_table_has_expected_columns(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        table = render_task_table(graph)
        column_names = [col.header for col in table.columns]
        assert "Task ID" in column_names
        assert "Function" in column_names
        assert "Status" in column_names
        assert "Type" in column_names

    def test_table_shows_child_task_function_names(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = _render_to_str(render_task_table(graph))
        assert "fetch_data" in output
        assert "process_report" in output
        assert "send_notification" in output

    def test_table_shows_child_type(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        output = _render_to_str(render_task_table(graph))
        assert "child" in output

    def test_table_shows_fail_child_type(self):
        graph = TaskGraph()
        parent_task = Task.default("do_work")
        fail_task = Task.default("handle_failure")
        graph.add_task(parent_task)
        graph.add_failure_callback(parent_task.task_id, fail_task)
        _set_result(graph, parent_task, TaskStatus.Failure)
        _set_result(graph, fail_task, TaskStatus.Pending)
        output = _render_to_str(render_task_table(graph))
        assert "fail_child" in output
        assert "handle_failure" in output

    def test_stalled_task_shows_stalled_marker(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Retry)
        output = _render_to_str(render_task_table(graph))
        assert "STALLED" in output

    def test_healthy_graph_has_no_stalled_marker(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        output = _render_to_str(render_task_table(graph))
        assert "STALLED" not in output

    def test_missing_result_blob_shows_no_blob(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        # task_b and task_c have no results
        output = _render_to_str(render_task_table(graph))
        assert "NO BLOB" in output

    def test_task_id_is_truncated_to_last_12_chars(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        short_id = truncate_task_id(task_a.task_id)
        output = _render_to_str(render_task_table(graph))
        assert short_id in output

    def test_row_count_matches_total_tasks(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        table = render_task_table(graph)
        assert table.row_count == 3


# ---------------------------------------------------------------------------
# TestRenderPurgePlan
# ---------------------------------------------------------------------------


class TestRenderPurgePlan:
    def test_returns_rich_table(self):
        eligible = [("graph-id-1", ["b1", "b2"]), ("graph-id-2", ["b3"])]
        table = render_purge_plan(eligible, "2026-04-07", 7)
        assert isinstance(table, Table)

    def test_table_contains_graph_ids(self):
        eligible = [("graph-id-abc", ["b1", "b2"]), ("graph-id-xyz", ["b3"])]
        output = _render_to_str(render_purge_plan(eligible, "2026-04-07", 7))
        assert "graph-id-abc" in output
        assert "graph-id-xyz" in output

    def test_table_contains_blob_counts(self):
        eligible = [("graph-id-1", ["b1", "b2"]), ("graph-id-2", ["b3"])]
        output = _render_to_str(render_purge_plan(eligible, "2026-04-07", 7))
        assert "2" in output
        assert "1" in output

    def test_table_title_contains_cutoff_date(self):
        eligible = [("graph-id-1", ["b1"])]
        output = _render_to_str(render_purge_plan(eligible, "2026-04-07", 7))
        assert "2026-04-07" in output

    def test_table_title_contains_older_than_days(self):
        eligible = [("graph-id-1", ["b1"])]
        output = _render_to_str(render_purge_plan(eligible, "2026-04-07", 7))
        assert "7" in output

    def test_row_count_matches_eligible_graph_count(self):
        eligible = [
            ("graph-id-1", ["b1", "b2"]),
            ("graph-id-2", ["b3"]),
            ("graph-id-3", ["b4", "b5", "b6"]),
        ]
        table = render_purge_plan(eligible, "2026-04-07", 7)
        assert table.row_count == 3

    def test_empty_eligible_list_produces_empty_table(self):
        table = render_purge_plan([], "2026-04-07", 7)
        assert table.row_count == 0


# ---------------------------------------------------------------------------
# TestNoColorOutput
# ---------------------------------------------------------------------------


class TestNoColorOutput:
    def test_no_color_console_produces_plain_text(self):
        """A no-color Console renders tables without ANSI escape codes."""
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Retry)  # stalled
        _set_result(graph, task_c, TaskStatus.Pending)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)
        console.print(render_task_table(graph))
        output = buf.getvalue()
        # No ANSI escape codes
        assert "\x1b[" not in output
        # Content is still present
        assert "fetch_data" in output
        assert "STALLED" in output
