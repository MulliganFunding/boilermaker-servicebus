"""Rich rendering functions for boilermaker CLI output."""

import json
from io import StringIO

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from boilermaker.task import TaskGraph, TaskStatus
from boilermaker.task.task_id import TaskId


def _short_task_id(task_id: TaskId) -> str:
    """Return the last 12 characters of a task ID for compact display."""
    full = str(task_id)
    return full[-12:] if len(full) > 12 else full


def status_style(status: TaskStatus, is_stalled: bool) -> tuple[str, str]:
    """Return (Rich style string, prefix character) for the given task status.

    The prefix character provides the same signal as color for accessible/piped output:
      '*' for Success, '!' for stalled or failed states, ' ' otherwise.
    """
    if is_stalled:
        return "bold red", "!"
    if status == TaskStatus.Success:
        return "green", "*"
    if status in (TaskStatus.Failure, TaskStatus.RetriesExhausted, TaskStatus.Deadlettered):
        return "red", "!"
    if status in (TaskStatus.Pending, TaskStatus.Scheduled, TaskStatus.Retry, TaskStatus.Started):
        return "yellow", " "
    return "", " "


def render_graph_summary(graph: TaskGraph) -> Panel:
    """Build a Rich Panel summary banner for the graph.

    The panel shows graph ID, overall status (Complete / In Progress / Has Failures),
    completion fraction, failure count, and stalled task count.
    """
    stalled = graph.detect_stalled_tasks()
    stalled_count = len(stalled)
    is_complete = graph.is_complete()
    has_failures = graph.has_failures()

    total_tasks = len(graph.children) + len(graph.fail_children)
    complete_tasks = sum(
        1
        for task_id in list(graph.children) + list(graph.fail_children)
        if (r := graph.results.get(task_id)) and r.status == TaskStatus.Success
    )
    failure_count = sum(
        1
        for r in graph.results.values()
        if r.status in (TaskStatus.Failure, TaskStatus.RetriesExhausted, TaskStatus.Deadlettered)
    )

    if is_complete and not has_failures:
        overall_status = Text("Complete", style="green")
    elif has_failures:
        overall_status = Text("Has Failures", style="red")
    else:
        overall_status = Text("In Progress", style="yellow")

    summary = Text()
    summary.append("Status: ")
    summary.append_text(overall_status)
    summary.append(f" | Tasks: {complete_tasks}/{total_tasks} complete")
    summary.append(f" | Failures: {failure_count}")
    summary.append(f" | Stalled: {stalled_count}")

    return Panel(summary, title=f"Graph: {graph.graph_id}")


def render_task_table(graph: TaskGraph) -> Table:
    """Build a Rich Table showing task status with colored badges.

    Columns: Task ID (last 12 chars), Function, Status (colored badge), Type.
    Stalled tasks get a 'STALLED' marker appended to the Type column.
    Missing results show 'NO BLOB' in red in the Status column.
    """
    table = Table(title="Task Status")
    table.add_column("Task ID", style="dim")
    table.add_column("Function")
    table.add_column("Status")
    table.add_column("Type")

    stalled = graph.detect_stalled_tasks()
    stalled_task_ids = {tid for tid, _, _ in stalled}

    def _task_rows(tasks: dict, task_type: str) -> None:
        for task_id, task in tasks.items():
            result = graph.results.get(task_id)
            is_stalled = task_id in stalled_task_ids

            if result is None:
                status_text = Text("NO BLOB", style="red")
            else:
                style, prefix = status_style(result.status, is_stalled)
                status_text = Text(f"{prefix} {result.status}", style=style)

            if is_stalled:
                type_text = Text(f"{task_type}  STALLED", style="bold red")
            else:
                type_text = Text(task_type)

            table.add_row(
                f"...{_short_task_id(task_id)}",
                task.function_name,
                status_text,
                type_text,
            )

    _task_rows(graph.children, "child")
    _task_rows(graph.fail_children, "fail_child")
    return table


def render_purge_plan(
    eligible_graphs: list,
    cutoff_display: str,
    older_than_days: int,
) -> Table:
    """Build a Rich Table showing the purge deletion plan.

    Each row shows a graph ID and the number of blobs to be deleted.
    The eligible_graphs list contains (graph_id, blobs) tuples.
    """
    table = Table(
        title=f"Purge Plan\nBlobs last modified before {cutoff_display} (older than {older_than_days} days)"
    )
    table.add_column("Graph", style="dim")
    table.add_column("Blobs", justify="right")

    for graph_id, blobs in eligible_graphs:
        table.add_row(str(graph_id), str(len(blobs)))

    return table


def format_graph_json(graph: TaskGraph) -> str:
    """Serialize a TaskGraph to a JSON string for machine-readable output.

    The JSON object contains top-level graph metadata and a list of all tasks
    (both children and fail_children) with their status, type, stall state, and
    dependency list. Task IDs are full (not truncated). Dependency lists are built
    by inverting graph.edges so each task lists the tasks it depends on.

    Returns a JSON string serialized with two-space indentation.
    """
    stalled = graph.detect_stalled_tasks()
    stalled_task_ids = {task_id for task_id, _, _ in stalled}

    # Build a reverse-edge map: child_id -> [parent_id, ...] from both graph.edges
    # (success-path dependencies) and graph.fail_edges (failure-callback dependencies).
    # fail_child tasks wire their triggering parents through fail_edges, not edges.
    depends_on_map: dict[TaskId, list[str]] = {}
    for parent_id, child_ids in graph.edges.items():
        for child_id in child_ids:
            depends_on_map.setdefault(child_id, []).append(str(parent_id))
    for parent_id, child_ids in graph.fail_edges.items():
        for child_id in child_ids:
            depends_on_map.setdefault(child_id, []).append(str(parent_id))

    def _task_entry(task_id: TaskId, task, task_type: str) -> dict:
        result = graph.results.get(task_id)
        status = result.status if result is not None else None
        return {
            "task_id": str(task_id),
            "function_name": task.function_name,
            "status": str(status) if status is not None else None,
            "type": task_type,
            "is_stalled": task_id in stalled_task_ids,
            "depends_on": sorted(depends_on_map.get(task_id, [])),
        }

    tasks = [_task_entry(tid, task, "child") for tid, task in graph.children.items()]
    fail_tasks = [_task_entry(tid, task, "fail_child") for tid, task in graph.fail_children.items()]

    output = {
        "graph_id": str(graph.graph_id),
        "is_complete": graph.is_complete(),
        "has_failures": graph.has_failures(),
        "stalled_count": len(stalled),
        "tasks": tasks,
        "fail_tasks": fail_tasks,
    }
    return json.dumps(output, indent=2)


def render_task_detail(graph: TaskGraph, task_id: TaskId) -> Panel:
    """Build a Rich Panel showing detail for a single task.

    The panel shows task ID, function name, status (colored), type (child or
    fail_child), graph ID, the tasks this task depends on (edges where this task
    is a target), and the tasks that depend on this task (edges where this task
    is a source).

    Args:
        graph: The TaskGraph containing the task.
        task_id: The ID of the task to render.

    Returns:
        A Rich Panel with task detail.
    """
    task = graph.children.get(task_id) or graph.fail_children.get(task_id)
    task_type = "child" if task_id in graph.children else "fail_child"
    function_name = task.function_name if task else "(unknown function)"

    result = graph.results.get(task_id)
    stalled = graph.detect_stalled_tasks()
    stalled_task_ids = {tid for tid, _, _ in stalled}
    is_stalled = task_id in stalled_task_ids

    if result is None:
        status_text = Text("NO BLOB", style="red")
    else:
        style, prefix = status_style(result.status, is_stalled)
        status_text = Text(f"{prefix} {result.status}", style=style)

    # Dependencies: tasks this task depends on (edges where this task is a target).
    # fail_child tasks are wired through fail_edges, not edges, so both are checked.
    depends_on = [
        str(parent_id)
        for parent_id, children_ids in graph.edges.items()
        if task_id in children_ids
    ] + [
        str(parent_id)
        for parent_id, children_ids in graph.fail_edges.items()
        if task_id in children_ids
    ]

    # Dependents: tasks that depend on this task (edges where this task is a source).
    # fail_edges holds failure-callback children; both edge sets are checked.
    dependents = [str(child_id) for child_id in graph.edges.get(task_id, set())] + [
        str(child_id) for child_id in graph.fail_edges.get(task_id, set())
    ]

    detail = Text()
    detail.append("Task ID:      ", style="bold")
    detail.append(f"{task_id}\n")
    detail.append("Function:     ", style="bold")
    detail.append(f"{function_name}\n")
    detail.append("Status:       ", style="bold")
    detail.append_text(status_text)
    detail.append("\n")
    detail.append("Type:         ", style="bold")
    detail.append(task_type)
    detail.append("\n")
    detail.append("Graph ID:     ", style="bold")
    detail.append(f"{graph.graph_id}\n")
    detail.append("Depends on:   ", style="bold")
    detail.append(", ".join(sorted(depends_on)) if depends_on else "(none)")
    detail.append("\n")
    detail.append("Dependents:   ", style="bold")
    detail.append(", ".join(sorted(dependents)) if dependents else "(none)")

    return Panel(detail, title=f"Task: ...{_short_task_id(task_id)}")


def format_graph_table(graph: TaskGraph) -> str:
    """Render the graph summary and task table as a plain-text string.

    Compatibility shim for tests and callers that expect a string return value.
    Renders via a no-color Console to a StringIO buffer.
    """
    buf = StringIO()
    console = Console(file=buf, no_color=True, width=120)

    stalled = graph.detect_stalled_tasks()
    stalled_count = len(stalled)
    is_complete = graph.is_complete()
    has_failures = graph.has_failures()

    console.print(f"\nGraph: {graph.graph_id}")
    console.print(render_task_table(graph))
    console.print(
        f"\nComplete: {is_complete} | Has failures: {has_failures} | Stalled tasks: {stalled_count}"
    )

    return buf.getvalue()
