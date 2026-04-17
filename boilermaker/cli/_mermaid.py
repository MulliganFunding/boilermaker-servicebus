"""Pure-function Mermaid flowchart generator for TaskGraph DAG visualization."""

from __future__ import annotations

import json
from typing import Any

from boilermaker.task import TaskGraph, TaskStatus
from boilermaker.task.result import TaskResult, TaskResultSlim
from boilermaker.task.task_id import TaskId


# ---------------------------------------------------------------------------
# Color mapping (from UX spec docs/ux/dag-visualization.md)
# ---------------------------------------------------------------------------

_STATUS_COLORS: dict[str, tuple[str, str, str]] = {
    # status_key: (fill, stroke, text_color)
    "success": ("#28a745", "#1e7e34", "#fff"),
    "failure": ("#dc3545", "#bd2130", "#fff"),
    "retriesExhausted": ("#dc3545", "#bd2130", "#fff"),
    "deadlettered": ("#851923", "#5a1018", "#fff"),
    "pending": ("#6c757d", "#545b62", "#fff"),
    "scheduled": ("#ffc107", "#d39e00", "#000"),
    "started": ("#fd7e14", "#d36b0f", "#fff"),
    "retry": ("#e0a800", "#c69500", "#000"),
    "noBlob": ("#343a40", "#1d2124", "#fff"),
}

_STATUS_TO_CLASS: dict[TaskStatus, str] = {
    TaskStatus.Success: "success",
    TaskStatus.Failure: "failure",
    TaskStatus.RetriesExhausted: "retriesExhausted",
    TaskStatus.Deadlettered: "deadlettered",
    TaskStatus.Pending: "pending",
    TaskStatus.Scheduled: "scheduled",
    TaskStatus.Started: "started",
    TaskStatus.Retry: "retry",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sanitize_id(task_id: TaskId) -> str:
    """Replace hyphens with underscores for valid Mermaid node IDs."""
    return str(task_id).replace("-", "_")


def _short_task_id(task_id: TaskId) -> str:
    """Return the last 12 characters of a task ID for compact display."""
    full = str(task_id)
    return full[-12:] if len(full) > 12 else full


def _node_class(status: TaskStatus | None, is_stalled: bool, has_blob: bool) -> str:
    """Map task state to the Mermaid classDef name.

    Stalled nodes receive a composite class (e.g. ``stalledScheduled``) that
    preserves the status fill color while adding a thick dashed stroke overlay.
    """
    if not has_blob:
        return "noBlob"
    base_cls = _STATUS_TO_CLASS.get(status, "pending") if status is not None else "noBlob"
    if is_stalled:
        return f"stalled{base_cls[0].upper()}{base_cls[1:]}"
    return base_cls


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_mermaid(graph: TaskGraph, stalled_task_ids: set[TaskId]) -> str:
    """Generate a Mermaid flowchart string from a TaskGraph.

    Returns a complete ``flowchart LR`` string including node definitions,
    edge definitions, classDef declarations, and linkStyle declarations.

    Pure function: no I/O, no side effects.
    """
    lines: list[str] = ["flowchart LR"]

    # -- Node definitions --------------------------------------------------

    all_task_ids: list[TaskId] = []

    # Children (rectangle shape)
    for task_id, task in graph.children.items():
        all_task_ids.append(task_id)
        sanitized = _sanitize_id(task_id)
        result = graph.results.get(task_id)
        has_blob = result is not None
        status = result.status if result else None
        is_stalled = task_id in stalled_task_ids
        cls = _node_class(status, is_stalled, has_blob)

        label_parts = [task.function_name]
        if not has_blob:
            label_parts.append("NO BLOB")
        label_parts.append(f"...{_short_task_id(task_id)}")
        label = "\\n".join(label_parts)

        lines.append(f'    {sanitized}["{label}"]:::{cls}')

    # Fail children (hexagon shape)
    for task_id, task in graph.fail_children.items():
        all_task_ids.append(task_id)
        sanitized = _sanitize_id(task_id)
        result = graph.results.get(task_id)
        has_blob = result is not None
        status = result.status if result else None
        is_stalled = task_id in stalled_task_ids
        cls = _node_class(status, is_stalled, has_blob)

        if task_id == graph.all_failed_callback_id:
            label_parts = ["on_all_failed", task.function_name]
        else:
            label_parts = [task.function_name]

        if not has_blob:
            label_parts.append("NO BLOB")
        label_parts.append(f"...{_short_task_id(task_id)}")
        label = "\\n".join(label_parts)

        lines.append(f'    {sanitized}{{{{"{label}"}}}}:::{cls}')

    # -- Edge definitions (track indices for linkStyle) --------------------

    edge_index = 0
    fail_edge_indices: list[int] = []

    # Success-path edges (solid arrows)
    for parent_id in sorted(graph.edges, key=str):
        child_ids = graph.edges[parent_id]
        parent_san = _sanitize_id(parent_id)
        for child_id in sorted(child_ids, key=str):
            child_san = _sanitize_id(child_id)
            lines.append(f"    {parent_san} --> {child_san}")
            edge_index += 1

    # Failure-path edges (dashed arrows)
    for parent_id in sorted(graph.fail_edges, key=str):
        child_ids = graph.fail_edges[parent_id]
        parent_san = _sanitize_id(parent_id)
        for child_id in sorted(child_ids, key=str):
            child_san = _sanitize_id(child_id)
            lines.append(f"    {parent_san} -.-> {child_san}")
            fail_edge_indices.append(edge_index)
            edge_index += 1

    # -- linkStyle for failure edges (dashed, red) -------------------------

    for idx in fail_edge_indices:
        lines.append(f"    linkStyle {idx} stroke:#dc3545,stroke-width:2px")

    # -- Click callbacks (Mermaid native click directive) --------------------

    for task_id in all_task_ids:
        sanitized = _sanitize_id(task_id)
        lines.append(f"    click {sanitized} showDetail")

    # -- classDef declarations ---------------------------------------------

    for cls_name, (fill, stroke, color) in _STATUS_COLORS.items():
        lines.append(f"    classDef {cls_name} fill:{fill},stroke:{stroke},color:{color}")

    # Stalled composite classes: preserve status fill color, add dashed stroke overlay
    _STALLED_OVERLAY = "stroke-dasharray:5,stroke-width:3px,stroke:#333"
    for cls_name, (fill, stroke, color) in _STATUS_COLORS.items():
        composite = f"stalled{cls_name[0].upper()}{cls_name[1:]}"
        lines.append(
            f"    classDef {composite} fill:{fill},stroke:{stroke},color:{color},"
            f"{_STALLED_OVERLAY}"
        )

    return "\n".join(lines)


def build_task_data_json(graph: TaskGraph, stalled_task_ids: set[TaskId]) -> str:
    """Build a JSON string of task metadata for the HTML detail panel.

    Each task entry includes: task_id, function_name, status, type
    (child/fail_child/all_failed_callback), is_stalled. When the graph
    contains full ``TaskResult`` instances (loaded via ``load_graph(full=True)``),
    entries also include ``result``, ``errors``, and ``formatted_exception``
    where present and non-empty.

    Fields that are ``None`` or empty are omitted from the JSON entry.
    """
    entries: dict[str, dict[str, Any]] = {}

    def _build_entry(
        task_id: TaskId,
        function_name: str,
        task_type: str,
    ) -> dict[str, Any]:
        result = graph.results.get(task_id)
        status = str(result.status) if result is not None else None
        is_stalled = task_id in stalled_task_ids

        entry: dict[str, Any] = {
            "task_id": str(task_id),
            "function_name": function_name,
            "status": status,
            "type": task_type,
            "is_stalled": is_stalled,
        }

        # Include rich fields when we have a full TaskResult
        if isinstance(result, TaskResult):
            if result.result is not None:
                if isinstance(result.result, str):
                    entry["result"] = result.result
                else:
                    entry["result"] = json.dumps(result.result, indent=2, default=str)

            if result.errors is not None and len(result.errors) > 0:
                entry["errors"] = result.errors

            if result.formatted_exception is not None and result.formatted_exception != "":
                entry["formatted_exception"] = result.formatted_exception

        return entry

    # Children
    for task_id, task in graph.children.items():
        entries[_sanitize_id(task_id)] = _build_entry(task_id, task.function_name, "child")

    # Fail children
    for task_id, task in graph.fail_children.items():
        if task_id == graph.all_failed_callback_id:
            task_type = "all_failed_callback"
        else:
            task_type = "fail_child"
        entries[_sanitize_id(task_id)] = _build_entry(task_id, task.function_name, task_type)

    return json.dumps(entries, indent=2)
