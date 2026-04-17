"""Tests for boilermaker.cli._visual — HTML template, browser integration, and CLI wiring."""

import json
from io import StringIO
from unittest import mock

import pytest
from rich.console import Console

from boilermaker.cli import build_parser, _validate_inspect_args
from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY, EXIT_STALLED
from boilermaker.cli._visual import (
    MERMAID_CDN_URL,
    render_visual_html,
    open_visual,
)
from boilermaker.cli.inspect import run_inspect
from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus
from boilermaker.task.task_id import TaskId


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


def _no_color_console() -> Console:
    """Return a Rich Console that writes to a StringIO buffer with no color."""
    return Console(file=StringIO(), no_color=True, width=120)


def _sample_mermaid_text() -> str:
    """Return a minimal Mermaid flowchart string for testing."""
    return 'flowchart LR\n    id_aaa["fetch_data\\n...aaa"]:::success\n    id_bbb["process\\n...bbb"]:::pending'


def _sample_task_data_json(
    include_result: bool = False,
    include_errors: bool = False,
    include_exception: bool = False,
) -> str:
    """Return a JSON string of task data for testing.

    By default returns minimal fields. Optional flags add rich TaskResult fields.
    """
    entry_a: dict = {
        "task_id": "aaaa-bbbb-cccc-dddd",
        "function_name": "fetch_data",
        "status": "success",
        "type": "child",
        "is_stalled": False,
    }
    entry_b: dict = {
        "task_id": "eeee-ffff-gggg-hhhh",
        "function_name": "process_report",
        "status": "failure",
        "type": "child",
        "is_stalled": False,
    }

    if include_result:
        entry_a["result"] = '{"key": "value"}'
    if include_errors:
        entry_b["errors"] = ["Connection timeout", "Retry limit exceeded"]
    if include_exception:
        entry_b["formatted_exception"] = "Traceback (most recent call last):\n  File ...\nValueError: bad input"

    data = {
        "id_aaaa_bbbb_cccc_dddd": entry_a,
        "id_eeee_ffff_gggg_hhhh": entry_b,
    }
    return json.dumps(data)


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlStructure
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlStructure:
    def test_html_contains_html_tag(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "<html" in result

    def test_html_contains_script_tag(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "<script" in result

    def test_html_contains_mermaid_div(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert '<div class="mermaid">' in result

    def test_html_contains_legend_div(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert '<div class="legend">' in result

    def test_html_contains_mermaid_cdn_url(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert MERMAID_CDN_URL in result

    def test_html_contains_mermaid_initialize(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "mermaid.initialize" in result
        assert "startOnLoad: false" in result
        assert "securityLevel: 'loose'" in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlSecurity
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlSecurity:
    def test_script_close_tag_in_task_data_is_escaped(self):
        """A task result containing </script> must not break out of the script tag."""
        graph, *_ = _make_graph_with_tasks()
        # Build task data JSON with a </script> payload in a result field
        malicious_data = {
            "id_aaaa_bbbb_cccc_dddd": {
                "task_id": "aaaa-bbbb-cccc-dddd",
                "function_name": "fetch_data",
                "status": "success",
                "type": "child",
                "is_stalled": False,
                "result": 'payload</script><script>alert("xss")</script>',
            },
        }
        task_json = json.dumps(malicious_data)
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        # The literal </script> must not appear in the output — it should be escaped
        assert "</script><script>" not in result
        # The escaped form <\/ should be present instead
        assert r"<\/" in result

    def test_mermaid_run_then_used_instead_of_set_timeout(self):
        """Click handlers should be attached via mermaid.run().then(), not setTimeout."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "mermaid.run().then(" in result
        assert "setTimeout" not in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlMermaidContent
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlMermaidContent:
    def test_mermaid_text_embedded_in_div(self):
        graph, *_ = _make_graph_with_tasks()
        mermaid_text = _sample_mermaid_text()
        result = render_visual_html(mermaid_text, _sample_task_data_json(), graph)
        # The mermaid text should appear inside the mermaid div
        assert "flowchart LR" in result
        assert "fetch_data" in result

    def test_mermaid_text_is_html_escaped(self):
        """Mermaid text containing HTML-special chars is escaped."""
        graph, *_ = _make_graph_with_tasks()
        mermaid_text = 'flowchart LR\n    id_a["data<br>test"]'
        result = render_visual_html(mermaid_text, _sample_task_data_json(), graph)
        # The < should be escaped in the HTML output
        assert "&lt;br&gt;" in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlTaskData
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlTaskData:
    def test_task_data_json_embedded_in_script(self):
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json()
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        assert "const taskData =" in result
        assert "fetch_data" in result
        assert "process_report" in result

    def test_task_data_is_parseable_json_in_html(self):
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json()
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        # Extract the JSON blob assigned to taskData
        start = result.index("const taskData = ") + len("const taskData = ")
        end = result.index(";\n", start)
        embedded_json = result[start:end]
        parsed = json.loads(embedded_json)
        assert isinstance(parsed, dict)


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlTitle
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlTitle:
    def test_title_contains_graph_id(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert f"<title>Boilermaker DAG: {graph.graph_id}</title>" in result

    def test_header_contains_graph_id(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert f"Graph: {graph.graph_id}" in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlLegend
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlLegend:
    def test_legend_contains_success(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Success" in result

    def test_legend_contains_failure(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Failure" in result

    def test_legend_contains_pending(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Pending" in result

    def test_legend_contains_scheduled(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Scheduled" in result

    def test_legend_contains_started(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Started" in result

    def test_legend_contains_retry(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Retry" in result

    def test_legend_contains_retries_exhausted(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Retries Exhausted" in result

    def test_legend_contains_deadlettered(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Deadlettered" in result

    def test_legend_contains_no_blob(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "No Blob" in result

    def test_legend_contains_stalled(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Stalled" in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlDetailPanel
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlDetailPanel:
    def test_detail_panel_div_exists(self):
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert 'id="detail-panel"' in result

    def test_detail_panel_has_pre_element_for_exception(self):
        """The JS showDetail function renders formatted_exception inside a <pre> block."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        # The JS template creates <pre> when formatted_exception is present
        assert "formatted_exception" in result
        assert "<pre>" in result

    def test_detail_panel_js_renders_errors_as_list(self):
        """The JS showDetail function renders errors as <ul><li> elements."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "<ul>" in result
        assert "<li>" in result

    def test_detail_panel_js_renders_result_field(self):
        """The JS showDetail function renders the result field as text."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        # The JS checks for task.result and displays it
        assert "task.result" in result

    def test_detail_panel_js_omits_absent_fields(self):
        """The JS only renders sections when fields are present in the JSON entry."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        # The JS uses conditional checks (if task.result, if task.errors, etc.)
        # to skip absent fields -- no "Not available" placeholders
        assert "Not available" not in result
        # The JS conditionally checks before rendering
        assert "if (task.result !== undefined && task.result !== null)" in result
        assert "if (task.errors && task.errors.length > 0)" in result
        assert "if (task.formatted_exception)" in result

    def test_escape_key_closes_panel(self):
        """The JS includes an Escape key handler to close the detail panel."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Escape" in result

    def test_click_outside_closes_panel(self):
        """The JS includes a click-outside handler to close the detail panel."""
        graph, *_ = _make_graph_with_tasks()
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "closePanel" in result


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlDetailPanelRichFields
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlDetailPanelRichFields:
    """Verify that task data JSON with rich fields is correctly embedded."""

    def test_result_field_embedded_in_task_data(self):
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json(include_result=True)
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        # The JSON is embedded verbatim as a JS constant; check the value is present
        assert "key" in result
        assert "value" in result

    def test_errors_field_embedded_in_task_data(self):
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json(include_errors=True)
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        assert "Connection timeout" in result
        assert "Retry limit exceeded" in result

    def test_formatted_exception_field_embedded_in_task_data(self):
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json(include_exception=True)
        result = render_visual_html(_sample_mermaid_text(), task_json, graph)
        assert "Traceback (most recent call last)" in result

    def test_absent_fields_not_in_task_data(self):
        """When rich fields are not included, they do not appear in the JSON."""
        graph, *_ = _make_graph_with_tasks()
        task_json = _sample_task_data_json()  # no rich fields
        parsed = json.loads(task_json)
        for entry in parsed.values():
            assert "result" not in entry
            assert "errors" not in entry
            assert "formatted_exception" not in entry


# ---------------------------------------------------------------------------
# TestRenderVisualHtmlSummary
# ---------------------------------------------------------------------------


class TestRenderVisualHtmlSummary:
    def test_summary_shows_task_completion_fraction(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Pending)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Tasks: 2/3" in result

    def test_summary_shows_failure_count(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Failure)
        _set_result(graph, task_c, TaskStatus.Pending)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Failures: 1" in result

    def test_summary_shows_stalled_count(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Stalled: 1" in result

    def test_summary_shows_complete_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Status: Complete" in result

    def test_summary_shows_has_failures_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        _set_result(graph, task_b, TaskStatus.Pending)
        _set_result(graph, task_c, TaskStatus.Pending)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Status: Has Failures" in result

    def test_summary_shows_in_progress_status(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Started)
        _set_result(graph, task_c, TaskStatus.Pending)
        result = render_visual_html(_sample_mermaid_text(), _sample_task_data_json(), graph)
        assert "Status: In Progress" in result


# ---------------------------------------------------------------------------
# TestOpenVisual
# ---------------------------------------------------------------------------


class TestOpenVisual:
    @mock.patch("webbrowser.open")
    @mock.patch("boilermaker.cli._mermaid.generate_mermaid", return_value="flowchart LR\n    A-->B")
    @mock.patch("boilermaker.cli._mermaid.build_task_data_json", return_value="{}")
    def test_writes_temp_file_and_calls_browser(
        self, mock_build_json, mock_gen_mermaid, mock_browser_open
    ):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        console = _no_color_console()

        path = open_visual(graph, set(), console)

        assert path.endswith(".html")
        assert "boilermaker-dag-" in path
        mock_browser_open.assert_called_once()
        call_url = mock_browser_open.call_args[0][0]
        assert call_url.startswith("file://")
        assert path in call_url

    @mock.patch("webbrowser.open")
    @mock.patch("boilermaker.cli._mermaid.generate_mermaid", return_value="flowchart LR\n    A-->B")
    @mock.patch("boilermaker.cli._mermaid.build_task_data_json", return_value="{}")
    def test_prints_file_path_to_console(
        self, mock_build_json, mock_gen_mermaid, mock_browser_open
    ):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        buf = StringIO()
        console = Console(file=buf, no_color=True, width=120)

        path = open_visual(graph, set(), console)

        output = buf.getvalue()
        assert "Opening DAG visualization in browser..." in output
        assert path in output
        assert "HTML file:" in output

    @mock.patch("webbrowser.open")
    @mock.patch("boilermaker.cli._mermaid.generate_mermaid", return_value="flowchart LR\n    A-->B")
    @mock.patch("boilermaker.cli._mermaid.build_task_data_json", return_value="{}")
    def test_temp_file_contains_valid_html(
        self, mock_build_json, mock_gen_mermaid, mock_browser_open
    ):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        console = _no_color_console()

        path = open_visual(graph, set(), console)

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "<html" in content
        assert "mermaid" in content

    @mock.patch("webbrowser.open")
    @mock.patch("boilermaker.cli._mermaid.generate_mermaid", return_value="flowchart LR\n    A-->B")
    @mock.patch("boilermaker.cli._mermaid.build_task_data_json", return_value="{}")
    def test_returns_file_path_string(
        self, mock_build_json, mock_gen_mermaid, mock_browser_open
    ):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        console = _no_color_console()

        path = open_visual(graph, set(), console)

        assert isinstance(path, str)
        assert len(path) > 0


# ---------------------------------------------------------------------------
# Helpers for CLI wiring tests
# ---------------------------------------------------------------------------

_STORAGE_URL = "https://example.blob.core.windows.net"
_CONTAINER = "my-container"
_GRAPH_ID = "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4"
_TASK_ID = "019d8c0c-be84-4c5388b8de6c"

_GLOBAL_OPTS = [
    "--storage-url", _STORAGE_URL,
    "--container", _CONTAINER,
]


def _mock_storage(graph: TaskGraph | None = None) -> mock.AsyncMock:
    """Return a mock storage whose load_graph returns the given graph."""
    storage = mock.AsyncMock()
    storage.load_graph = mock.AsyncMock(return_value=graph)
    return storage


# ---------------------------------------------------------------------------
# TestVisualParserFlag
# ---------------------------------------------------------------------------


class TestVisualParserFlag:
    def test_visual_flag_parses_correctly(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID, "--visual"])
        assert args.visual is True

    def test_visual_defaults_false(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
        assert args.visual is False


# ---------------------------------------------------------------------------
# TestVisualValidation
# ---------------------------------------------------------------------------


class TestVisualValidation:
    def test_visual_without_graph_is_error(self):
        """--visual without --graph should fail post-parse validation."""
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--visual"])
        with pytest.raises(SystemExit) as exc_info:
            _validate_inspect_args(args, parser)
        assert exc_info.value.code == 2

    def test_visual_with_json_is_error(self):
        """--visual and --json are mutually exclusive."""
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID, "--visual", "--json"])
        with pytest.raises(SystemExit) as exc_info:
            _validate_inspect_args(args, parser)
        assert exc_info.value.code == 2

    def test_visual_with_task_is_error(self):
        """--visual and --task are mutually exclusive."""
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID, "--visual", "--task", _TASK_ID,
        ])
        with pytest.raises(SystemExit) as exc_info:
            _validate_inspect_args(args, parser)
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# TestVisualCallsOpenVisual
# ---------------------------------------------------------------------------


class TestVisualCallsOpenVisual:
    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_visual_flag_calls_open_visual(self, mock_open_visual):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)
        console = _no_color_console()

        await run_inspect(storage, str(graph.graph_id), console=console, visual=True)

        mock_open_visual.assert_called_once()
        call_args = mock_open_visual.call_args
        assert call_args[0][0] is graph  # first positional arg is the graph
        assert call_args[0][1] == set()  # no stalled tasks
        assert call_args.kwargs["console"] is console

    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_visual_passes_stalled_task_ids(self, mock_open_visual):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)

        await run_inspect(storage, str(graph.graph_id), console=_no_color_console(), visual=True)

        mock_open_visual.assert_called_once()
        stalled_ids = mock_open_visual.call_args[0][1]
        assert task_c.task_id in stalled_ids


# ---------------------------------------------------------------------------
# TestVisualExitCodes
# ---------------------------------------------------------------------------


class TestVisualExitCodes:
    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_exit_healthy_when_no_stalled_tasks(self, mock_open_visual):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)

        exit_code = await run_inspect(
            storage, str(graph.graph_id), console=_no_color_console(), visual=True,
        )
        assert exit_code == EXIT_HEALTHY

    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_exit_stalled_when_stalled_tasks_exist(self, mock_open_visual):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Scheduled)
        storage = _mock_storage(graph)

        exit_code = await run_inspect(
            storage, str(graph.graph_id), console=_no_color_console(), visual=True,
        )
        assert exit_code == EXIT_STALLED

    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_exit_error_when_graph_not_found(self, mock_open_visual):
        storage = _mock_storage(None)

        exit_code = await run_inspect(
            storage, "nonexistent-graph-id", console=_no_color_console(), visual=True,
        )
        assert exit_code == EXIT_ERROR
        mock_open_visual.assert_not_called()


# ---------------------------------------------------------------------------
# TestVisualLoadGraphFullTrue
# ---------------------------------------------------------------------------


class TestVisualLoadGraphFullTrue:
    @pytest.mark.asyncio
    @mock.patch("boilermaker.cli.inspect.open_visual")
    async def test_visual_calls_load_graph_with_full_true(self, mock_open_visual):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)

        await run_inspect(
            storage, str(graph.graph_id), console=_no_color_console(), visual=True,
        )

        storage.load_graph.assert_called_once()
        call_kwargs = storage.load_graph.call_args
        assert call_kwargs.kwargs.get("full") is True or (
            len(call_kwargs.args) > 1 and call_kwargs.args[1] is True
        )

    @pytest.mark.asyncio
    async def test_non_visual_calls_load_graph_without_full(self):
        graph, task_a, task_b, task_c = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        _set_result(graph, task_b, TaskStatus.Success)
        _set_result(graph, task_c, TaskStatus.Success)
        storage = _mock_storage(graph)

        await run_inspect(
            storage, str(graph.graph_id), console=_no_color_console(),
        )

        storage.load_graph.assert_called_once()
        call_kwargs = storage.load_graph.call_args
        # Non-visual path should not pass full=True
        assert call_kwargs.kwargs.get("full", False) is False
