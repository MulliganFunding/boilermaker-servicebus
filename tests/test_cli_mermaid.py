"""Tests for boilermaker.cli._mermaid — Mermaid flowchart generator."""

import json

import pytest
from boilermaker.cli._mermaid import (
    _node_class,
    _sanitize_id,
    build_task_data_json,
    generate_mermaid,
)
from boilermaker.task import Task, TaskGraph, TaskStatus
from boilermaker.task.result import TaskResult, TaskResultSlim
from boilermaker.task.task_id import TaskId, truncate_task_id


# Helper to look up an entry in the dict-keyed task data JSON by function name.
def _find_entry_by_func(parsed: dict, func_name: str) -> dict:
    """Find a task data entry by function_name in the dict-keyed JSON output."""
    for entry in parsed.values():
        if entry["function_name"] == func_name:
            return entry
    raise KeyError(f"No entry with function_name={func_name!r}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_result(graph: TaskGraph, task: Task, status: TaskStatus) -> None:
    """Set a slim result on a graph for the given task and status."""
    result = TaskResultSlim(
        task_id=task.task_id,
        graph_id=graph.graph_id,
        status=status,
    )
    graph.results[task.task_id] = result


def _set_full_result(
    graph: TaskGraph,
    task: Task,
    status: TaskStatus,
    *,
    result_value: object = None,
    errors: list[str] | None = None,
    formatted_exception: str | None = None,
) -> None:
    """Set a full TaskResult on a graph for the given task."""
    result = TaskResult(
        task_id=task.task_id,
        graph_id=graph.graph_id,
        status=status,
        result=result_value,
        errors=errors,
        formatted_exception=formatted_exception,
    )
    graph.results[task.task_id] = result


# ---------------------------------------------------------------------------
# TestSanitizeId
# ---------------------------------------------------------------------------


class TestSanitizeId:
    def test_replaces_hyphens_with_underscores(self):
        tid = TaskId("abc-def-123")
        assert _sanitize_id(tid) == "abc_def_123"

    def test_no_hyphens_unchanged(self):
        tid = TaskId("abcdef123")
        assert _sanitize_id(tid) == "abcdef123"


# ---------------------------------------------------------------------------
# TestNodeClass
# ---------------------------------------------------------------------------


class TestNodeClass:
    def test_no_blob_returns_no_blob(self):
        assert _node_class(None, False, False) == "noBlob"

    def test_stalled_returns_composite_class(self):
        assert _node_class(TaskStatus.Scheduled, True, True) == "stalledScheduled"

    def test_success_returns_success(self):
        assert _node_class(TaskStatus.Success, False, True) == "success"

    def test_failure_returns_failure(self):
        assert _node_class(TaskStatus.Failure, False, True) == "failure"

    def test_retries_exhausted_returns_retries_exhausted(self):
        assert _node_class(TaskStatus.RetriesExhausted, False, True) == "retriesExhausted"

    def test_deadlettered_returns_deadlettered(self):
        assert _node_class(TaskStatus.Deadlettered, False, True) == "deadlettered"

    def test_pending_returns_pending(self):
        assert _node_class(TaskStatus.Pending, False, True) == "pending"

    def test_scheduled_returns_scheduled(self):
        assert _node_class(TaskStatus.Scheduled, False, True) == "scheduled"

    def test_started_returns_started(self):
        assert _node_class(TaskStatus.Started, False, True) == "started"

    def test_retry_returns_retry(self):
        assert _node_class(TaskStatus.Retry, False, True) == "retry"

    def test_has_blob_none_status_returns_no_blob(self):
        # blob exists but status is None (edge case)
        assert _node_class(None, False, True) == "noBlob"


# ---------------------------------------------------------------------------
# TestStatusToClassMappingAllValues
# ---------------------------------------------------------------------------


class TestStatusToClassMappingAllValues:
    """Ensure every TaskStatus value has a corresponding classDef in the generated output."""

    @pytest.mark.parametrize("status", list(TaskStatus))
    def test_every_status_maps_to_a_class(self, status: TaskStatus):
        cls = _node_class(status, False, True)
        assert cls != "noBlob", f"TaskStatus.{status.name} should map to a real class, not noBlob"


# ---------------------------------------------------------------------------
# TestSequentialChain
# ---------------------------------------------------------------------------


class TestSequentialChain:
    """A -> B -> C: correct node IDs, solid edges, class assignments."""

    def test_sequential_chain(self):
        graph = TaskGraph()
        a = Task.default("fetch_data")
        b = Task.default("process")
        c = Task.default("save")
        graph.add_task(a)
        graph.add_task(b, parent_ids=[a.task_id])
        graph.add_task(c, parent_ids=[b.task_id])
        _set_result(graph, a, TaskStatus.Success)
        _set_result(graph, b, TaskStatus.Success)
        _set_result(graph, c, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        assert "flowchart LR" in mermaid
        # Nodes present
        assert _sanitize_id(a.task_id) in mermaid
        assert _sanitize_id(b.task_id) in mermaid
        assert _sanitize_id(c.task_id) in mermaid
        # Solid edges
        assert f"{_sanitize_id(a.task_id)} --> {_sanitize_id(b.task_id)}" in mermaid
        assert f"{_sanitize_id(b.task_id)} --> {_sanitize_id(c.task_id)}" in mermaid
        # Class assignments
        assert ":::success" in mermaid
        assert ":::pending" in mermaid


# ---------------------------------------------------------------------------
# TestFanOut
# ---------------------------------------------------------------------------


class TestFanOut:
    """A -> B, A -> C: single parent with multiple children."""

    def test_fan_out(self):
        graph = TaskGraph()
        a = Task.default("start")
        b = Task.default("branch_one")
        c = Task.default("branch_two")
        graph.add_task(a)
        graph.add_task(b, parent_ids=[a.task_id])
        graph.add_task(c, parent_ids=[a.task_id])
        _set_result(graph, a, TaskStatus.Success)
        _set_result(graph, b, TaskStatus.Pending)
        _set_result(graph, c, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        a_san = _sanitize_id(a.task_id)
        b_san = _sanitize_id(b.task_id)
        c_san = _sanitize_id(c.task_id)
        assert f"{a_san} --> {b_san}" in mermaid or f"{a_san} --> {c_san}" in mermaid
        # Both edges must exist
        edges_from_a = [line for line in mermaid.splitlines() if f"{a_san} -->" in line]
        assert len(edges_from_a) == 2


# ---------------------------------------------------------------------------
# TestFanIn
# ---------------------------------------------------------------------------


class TestFanIn:
    """A, B -> C: multiple parents converging."""

    def test_fan_in(self):
        graph = TaskGraph()
        a = Task.default("step_a")
        b = Task.default("step_b")
        c = Task.default("merge")
        graph.add_task(a)
        graph.add_task(b)
        graph.add_task(c, parent_ids=[a.task_id, b.task_id])
        _set_result(graph, a, TaskStatus.Success)
        _set_result(graph, b, TaskStatus.Success)
        _set_result(graph, c, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        c_san = _sanitize_id(c.task_id)
        edges_to_c = [line for line in mermaid.splitlines() if f"--> {c_san}" in line]
        assert len(edges_to_c) == 2


# ---------------------------------------------------------------------------
# TestDiamond
# ---------------------------------------------------------------------------


class TestDiamond:
    """A -> B, A -> C, B -> D, C -> D."""

    def test_diamond(self):
        graph = TaskGraph()
        a = Task.default("start")
        b = Task.default("left")
        c = Task.default("right")
        d = Task.default("join")
        graph.add_task(a)
        graph.add_task(b, parent_ids=[a.task_id])
        graph.add_task(c, parent_ids=[a.task_id])
        graph.add_task(d, parent_ids=[b.task_id, c.task_id])
        for t in [a, b, c, d]:
            _set_result(graph, t, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        a_san = _sanitize_id(a.task_id)
        b_san = _sanitize_id(b.task_id)
        c_san = _sanitize_id(c.task_id)
        d_san = _sanitize_id(d.task_id)
        assert f"{a_san} --> {b_san}" in mermaid or f"{a_san} --> {c_san}" in mermaid
        assert f"{b_san} --> {d_san}" in mermaid or f"{c_san} --> {d_san}" in mermaid
        # 4 edges total
        edge_lines = [ln for ln in mermaid.splitlines() if " --> " in ln]
        assert len(edge_lines) == 4


# ---------------------------------------------------------------------------
# TestFailureCallback
# ---------------------------------------------------------------------------


class TestFailureCallback:
    """A --fail--> E: dashed edge, hexagon shape."""

    def test_failure_callback_dashed_edge_and_hexagon(self):
        graph = TaskGraph()
        a = Task.default("do_work")
        e = Task.default("handle_error")
        graph.add_task(a)
        graph.add_failure_callback(a.task_id, e)
        _set_result(graph, a, TaskStatus.Failure)
        _set_result(graph, e, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        a_san = _sanitize_id(a.task_id)
        e_san = _sanitize_id(e.task_id)
        # Dashed edge
        assert f"{a_san} -.-> {e_san}" in mermaid
        # Hexagon shape (double curly braces)
        assert f'{e_san}{{{{' in mermaid
        # linkStyle declaration exists for the failure edge
        assert "linkStyle" in mermaid


# ---------------------------------------------------------------------------
# TestSharedFailureCallback
# ---------------------------------------------------------------------------


class TestSharedFailureCallback:
    """Multiple parents -> one failure handler."""

    def test_shared_failure_callback(self):
        graph = TaskGraph()
        a = Task.default("step_a")
        b = Task.default("step_b")
        handler = Task.default("shared_handler")
        graph.add_task(a)
        graph.add_task(b)
        graph.add_failure_callback(a.task_id, handler)
        graph.add_failure_callback(b.task_id, handler)
        _set_result(graph, a, TaskStatus.Failure)
        _set_result(graph, b, TaskStatus.Failure)
        _set_result(graph, handler, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        handler_san = _sanitize_id(handler.task_id)
        # Two dashed edges to the same handler
        dashed_to_handler = [ln for ln in mermaid.splitlines() if f"-.-> {handler_san}" in ln]
        assert len(dashed_to_handler) == 2
        # Only one node definition for the handler
        node_defs = [ln for ln in mermaid.splitlines() if ln.strip().startswith(handler_san + "{{")]
        assert len(node_defs) == 1


# ---------------------------------------------------------------------------
# TestAllFailedCallbackPresent
# ---------------------------------------------------------------------------


class TestAllFailedCallbackPresent:
    """all_failed_callback: hexagon, 'on_all_failed' prefix, no incoming edges."""

    def test_all_failed_callback_rendering(self):
        graph = TaskGraph()
        a = Task.default("do_work")
        cb = Task.default("notify_admins")
        graph.add_task(a)
        graph.add_all_failed_callback(cb)
        _set_result(graph, a, TaskStatus.Failure)
        _set_result(graph, cb, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        cb_san = _sanitize_id(cb.task_id)
        # Hexagon shape
        assert f'{cb_san}{{{{' in mermaid
        # "on_all_failed" prefix in label
        assert "on_all_failed" in mermaid
        # Function name in label
        assert "notify_admins" in mermaid
        # No incoming edges to the callback
        edges_to_cb = [ln for ln in mermaid.splitlines() if f"--> {cb_san}" in ln or f"-.-> {cb_san}" in ln]
        assert len(edges_to_cb) == 0


# ---------------------------------------------------------------------------
# TestAllFailedCallbackAbsent
# ---------------------------------------------------------------------------


class TestAllFailedCallbackAbsent:
    """No all_failed_callback registered."""

    def test_no_all_failed_node(self):
        graph = TaskGraph()
        a = Task.default("do_work")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        mermaid = generate_mermaid(graph, set())

        assert "on_all_failed" not in mermaid


# ---------------------------------------------------------------------------
# TestNoBlobNode
# ---------------------------------------------------------------------------


class TestNoBlobNode:
    """Node with no result blob gets noBlob class and 'NO BLOB' in label."""

    def test_no_blob_node(self):
        graph = TaskGraph()
        a = Task.default("missing_result")
        graph.add_task(a)
        # No result set — simulates missing blob

        mermaid = generate_mermaid(graph, set())

        assert ":::noBlob" in mermaid
        assert "NO BLOB" in mermaid


# ---------------------------------------------------------------------------
# TestStalledTask
# ---------------------------------------------------------------------------


class TestStalledTask:
    """Stalled task gets composite stalled class preserving status color."""

    def test_stalled_class_preserves_status(self):
        graph = TaskGraph()
        a = Task.default("stuck_task")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Scheduled)

        stalled = {a.task_id}
        mermaid = generate_mermaid(graph, stalled)

        assert ":::stalledScheduled" in mermaid

    def test_stalled_success_class(self):
        graph = TaskGraph()
        a = Task.default("stuck_success")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        mermaid = generate_mermaid(graph, {a.task_id})

        assert ":::stalledSuccess" in mermaid

    def test_stalled_failure_class(self):
        graph = TaskGraph()
        a = Task.default("stuck_failure")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Failure)

        mermaid = generate_mermaid(graph, {a.task_id})

        assert ":::stalledFailure" in mermaid


# ---------------------------------------------------------------------------
# TestTaskIdSanitization
# ---------------------------------------------------------------------------


class TestTaskIdSanitization:
    """Hyphens replaced in Mermaid IDs, original preserved in label."""

    def test_sanitization_in_output(self):
        graph = TaskGraph()
        a = Task.default("my_func")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        mermaid = generate_mermaid(graph, set())

        sanitized = _sanitize_id(a.task_id)
        # Sanitized ID used as Mermaid node ID (no hyphens)
        assert "-" not in sanitized
        assert sanitized in mermaid
        # Last 12 chars of original ID are in the label
        assert truncate_task_id(a.task_id) in mermaid


# ---------------------------------------------------------------------------
# TestEmptyGraph
# ---------------------------------------------------------------------------


class TestEmptyGraph:
    """Empty graph produces minimal valid Mermaid."""

    def test_empty_graph(self):
        graph = TaskGraph()
        mermaid = generate_mermaid(graph, set())
        assert "flowchart LR" in mermaid
        # No edges
        assert "-->" not in mermaid
        assert "-.->" not in mermaid


# ---------------------------------------------------------------------------
# TestClassDefDeclarations
# ---------------------------------------------------------------------------


class TestClassDefDeclarations:
    """All expected classDef declarations are present."""

    def test_all_status_class_defs_present(self):
        graph = TaskGraph()
        a = Task.default("x")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        for cls_name in [
            "success", "failure", "retriesExhausted", "deadlettered",
            "pending", "scheduled", "started", "retry", "noBlob",
        ]:
            assert f"classDef {cls_name}" in mermaid

    def test_stalled_composite_class_defs_present(self):
        graph = TaskGraph()
        a = Task.default("x")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        for cls_name in [
            "stalledSuccess", "stalledFailure", "stalledRetriesExhausted",
            "stalledDeadlettered", "stalledPending", "stalledScheduled",
            "stalledStarted", "stalledRetry", "stalledNoBlob",
        ]:
            assert f"classDef {cls_name}" in mermaid

    def test_stalled_class_defs_have_dashed_stroke(self):
        graph = TaskGraph()
        a = Task.default("x")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        # Every stalledXxx classDef should have stroke-dasharray
        for line in mermaid.splitlines():
            if "classDef stalled" in line:
                assert "stroke-dasharray:5" in line
                assert "stroke-width:3px" in line


# ---------------------------------------------------------------------------
# TestLinkStyleIndexTracking
# ---------------------------------------------------------------------------


class TestLinkStyleIndexTracking:
    """linkStyle targets the correct edge indices."""

    def test_link_style_indices(self):
        graph = TaskGraph()
        a = Task.default("step_a")
        b = Task.default("step_b")
        handler = Task.default("err_handler")
        graph.add_task(a)
        graph.add_task(b, parent_ids=[a.task_id])
        graph.add_failure_callback(a.task_id, handler)
        _set_result(graph, a, TaskStatus.Failure)
        _set_result(graph, b, TaskStatus.Pending)
        _set_result(graph, handler, TaskStatus.Pending)

        mermaid = generate_mermaid(graph, set())

        # One success edge (index 0: a -> b), one fail edge (index 1: a -.-> handler)
        # linkStyle should target index 1
        assert "linkStyle 1" in mermaid
        assert "linkStyle 0" not in mermaid or "linkStyle 0 stroke" not in mermaid


# ---------------------------------------------------------------------------
# TestBuildTaskDataJson
# ---------------------------------------------------------------------------


class TestBuildTaskDataJson:
    """build_task_data_json returns valid JSON dict keyed by sanitized node ID."""

    def test_valid_json_dict_output(self):
        graph = TaskGraph()
        a = Task.default("fetch")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        result = build_task_data_json(graph, set())
        parsed = json.loads(result)

        assert isinstance(parsed, dict)
        assert len(parsed) == 1

    def test_keyed_by_sanitized_id(self):
        graph = TaskGraph()
        a = Task.default("fetch")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        parsed = json.loads(build_task_data_json(graph, set()))
        expected_key = _sanitize_id(a.task_id)
        assert expected_key in parsed

    def test_expected_keys_present(self):
        graph = TaskGraph()
        a = Task.default("fetch")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]

        assert "task_id" in entry
        assert "function_name" in entry
        assert "status" in entry
        assert "type" in entry
        assert "is_stalled" in entry

    def test_child_type(self):
        graph = TaskGraph()
        a = Task.default("do_work")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        parsed = json.loads(build_task_data_json(graph, set()))
        assert parsed[_sanitize_id(a.task_id)]["type"] == "child"

    def test_fail_child_type(self):
        graph = TaskGraph()
        parent = Task.default("work")
        handler = Task.default("handle")
        graph.add_task(parent)
        graph.add_failure_callback(parent.task_id, handler)
        _set_result(graph, parent, TaskStatus.Failure)
        _set_result(graph, handler, TaskStatus.Pending)

        parsed = json.loads(build_task_data_json(graph, set()))
        fail_entry = _find_entry_by_func(parsed, "handle")
        assert fail_entry["type"] == "fail_child"

    def test_all_failed_callback_type(self):
        graph = TaskGraph()
        a = Task.default("work")
        cb = Task.default("on_fail_all")
        graph.add_task(a)
        graph.add_all_failed_callback(cb)
        _set_result(graph, a, TaskStatus.Failure)
        _set_result(graph, cb, TaskStatus.Pending)

        parsed = json.loads(build_task_data_json(graph, set()))
        cb_entry = _find_entry_by_func(parsed, "on_fail_all")
        assert cb_entry["type"] == "all_failed_callback"

    def test_stalled_flag(self):
        graph = TaskGraph()
        a = Task.default("stuck")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Scheduled)

        parsed = json.loads(build_task_data_json(graph, {a.task_id}))
        assert parsed[_sanitize_id(a.task_id)]["is_stalled"] is True

    def test_not_stalled_flag(self):
        graph = TaskGraph()
        a = Task.default("ok")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        parsed = json.loads(build_task_data_json(graph, set()))
        assert parsed[_sanitize_id(a.task_id)]["is_stalled"] is False


# ---------------------------------------------------------------------------
# TestBuildTaskDataJsonFullResult
# ---------------------------------------------------------------------------


class TestBuildTaskDataJsonFullResult:
    """build_task_data_json with full TaskResult includes rich fields."""

    def test_full_result_includes_result_field(self):
        graph = TaskGraph()
        a = Task.default("compute")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value={"answer": 42})

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "result" in entry
        # Non-string result should be JSON-serialized
        inner = json.loads(entry["result"])
        assert inner == {"answer": 42}

    def test_full_result_string_value_not_double_encoded(self):
        graph = TaskGraph()
        a = Task.default("greet")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value="hello world")

        parsed = json.loads(build_task_data_json(graph, set()))
        assert parsed[_sanitize_id(a.task_id)]["result"] == "hello world"

    def test_full_result_includes_errors(self):
        graph = TaskGraph()
        a = Task.default("fail_task")
        graph.add_task(a)
        _set_full_result(
            graph, a, TaskStatus.Failure,
            errors=["connection timeout", "retry failed"],
        )

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "errors" in entry
        assert entry["errors"] == ["connection timeout", "retry failed"]

    def test_full_result_includes_formatted_exception(self):
        graph = TaskGraph()
        a = Task.default("crash_task")
        graph.add_task(a)
        _set_full_result(
            graph, a, TaskStatus.Failure,
            formatted_exception="Traceback (most recent call last):\n  ValueError: boom",
        )

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "formatted_exception" in entry
        assert "ValueError: boom" in entry["formatted_exception"]


# ---------------------------------------------------------------------------
# TestBuildTaskDataJsonOmitsNoneFields
# ---------------------------------------------------------------------------


class TestBuildTaskDataJsonOmitsNoneFields:
    """Fields that are None or empty are omitted from the JSON entry."""

    def test_successful_task_omits_errors_and_exception(self):
        graph = TaskGraph()
        a = Task.default("ok_task")
        graph.add_task(a)
        _set_full_result(
            graph, a, TaskStatus.Success,
            result_value="done",
            errors=None,
            formatted_exception=None,
        )

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "errors" not in entry
        assert "formatted_exception" not in entry
        assert entry["result"] == "done"

    def test_empty_errors_list_omitted(self):
        graph = TaskGraph()
        a = Task.default("ok_task")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, errors=[])

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "errors" not in entry

    def test_empty_string_exception_omitted(self):
        graph = TaskGraph()
        a = Task.default("ok_task")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, formatted_exception="")

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "formatted_exception" not in entry

    def test_none_result_omitted(self):
        graph = TaskGraph()
        a = Task.default("void_task")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value=None)

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "result" not in entry


# ---------------------------------------------------------------------------
# TestBuildTaskDataJsonSlimResult
# ---------------------------------------------------------------------------


class TestBuildTaskDataJsonSlimResult:
    """TaskResultSlim instances do not include rich fields."""

    def test_slim_result_omits_rich_fields(self):
        graph = TaskGraph()
        a = Task.default("slim_task")
        graph.add_task(a)
        _set_result(graph, a, TaskStatus.Success)

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert "result" not in entry
        assert "errors" not in entry
        assert "formatted_exception" not in entry


# ---------------------------------------------------------------------------
# TestBuildTaskDataJsonNonStringResult
# ---------------------------------------------------------------------------


class TestBuildTaskDataJsonNonStringResult:
    """Non-string result values are JSON-serialized with json.dumps(default=str)."""

    def test_dict_result_serialized(self):
        graph = TaskGraph()
        a = Task.default("compute")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value={"count": 5, "items": [1, 2, 3]})

        parsed = json.loads(build_task_data_json(graph, set()))
        inner = json.loads(parsed[_sanitize_id(a.task_id)]["result"])
        assert inner["count"] == 5
        assert inner["items"] == [1, 2, 3]

    def test_list_result_serialized(self):
        graph = TaskGraph()
        a = Task.default("list_task")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value=[1, 2, 3])

        parsed = json.loads(build_task_data_json(graph, set()))
        inner = json.loads(parsed[_sanitize_id(a.task_id)]["result"])
        assert inner == [1, 2, 3]

    def test_numeric_result_serialized(self):
        graph = TaskGraph()
        a = Task.default("num_task")
        graph.add_task(a)
        _set_full_result(graph, a, TaskStatus.Success, result_value=42)

        parsed = json.loads(build_task_data_json(graph, set()))
        # json.dumps(42) => "42" — a string containing the number
        inner = json.loads(parsed[_sanitize_id(a.task_id)]["result"])
        assert inner == 42


# ---------------------------------------------------------------------------
# TestBuildTaskDataJsonFailedTask
# ---------------------------------------------------------------------------


class TestBuildTaskDataJsonFailedTask:
    """Failed task: formatted_exception as raw string, errors as list."""

    def test_failed_task_has_exception_and_errors(self):
        graph = TaskGraph()
        a = Task.default("boom_task")
        graph.add_task(a)
        _set_full_result(
            graph, a, TaskStatus.Failure,
            errors=["step 1 failed", "step 2 failed"],
            formatted_exception="Traceback:\n  RuntimeError: kaboom",
        )

        parsed = json.loads(build_task_data_json(graph, set()))
        entry = parsed[_sanitize_id(a.task_id)]
        assert isinstance(entry["errors"], list)
        assert len(entry["errors"]) == 2
        assert isinstance(entry["formatted_exception"], str)
        assert "RuntimeError: kaboom" in entry["formatted_exception"]
