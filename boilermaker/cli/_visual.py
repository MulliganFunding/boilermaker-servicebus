"""HTML assembly, temp file creation, and browser opening for DAG visualization."""

from __future__ import annotations

import html
import tempfile
import webbrowser

from rich.console import Console

from boilermaker.cli._mermaid import build_task_data_json, generate_mermaid
from boilermaker.task import TaskGraph, TaskStatus
from boilermaker.task.task_id import TaskId

MERMAID_CDN_URL = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"

# Color mapping matching the UX spec and _output.py semantics.
STATUS_COLORS: dict[str, tuple[str, str]] = {
    # status_name -> (fill, label)
    "Success": ("#28a745", "Success"),
    "Failure": ("#dc3545", "Failure"),
    "RetriesExhausted": ("#dc3545", "Retries Exhausted"),
    "Deadlettered": ("#851923", "Deadlettered"),
    "Pending": ("#6c757d", "Pending"),
    "Scheduled": ("#ffc107", "Scheduled"),
    "Started": ("#fd7e14", "Started"),
    "Retry": ("#e0a800", "Retry"),
    "No Blob": ("#343a40", "No Blob"),
    "Stalled": ("stroke overlay", "Stalled (dashed border)"),
}


def _compute_summary(graph: TaskGraph) -> dict[str, str | int]:
    """Compute summary statistics from a TaskGraph for the HTML header."""
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
        overall = "Complete"
    elif has_failures:
        overall = "Has Failures"
    else:
        overall = "In Progress"

    return {
        "overall": overall,
        "complete_tasks": complete_tasks,
        "total_tasks": total_tasks,
        "failure_count": failure_count,
        "stalled_count": stalled_count,
    }


def _build_legend_html() -> str:
    """Build the color legend HTML with swatches for each status."""
    items: list[str] = []
    for name, (color, label) in STATUS_COLORS.items():
        if name == "Stalled":
            # Stalled uses a stroke overlay, not a fill color
            swatch_style = (
                "display:inline-block;width:18px;height:18px;border:3px dashed #ffc107;"
                "background:#6c757d;border-radius:3px;vertical-align:middle;margin-right:6px;"
            )
        else:
            swatch_style = (
                f"display:inline-block;width:18px;height:18px;background:{color};"
                "border-radius:3px;vertical-align:middle;margin-right:6px;"
            )
        items.append(
            f'<span style="margin-right:16px;">'
            f'<span style="{swatch_style}"></span>{html.escape(label)}</span>'
        )
    return '<div class="legend">' + "\n".join(items) + "</div>"


def render_visual_html(
    mermaid_text: str,
    task_data_json: str,
    graph: TaskGraph,
) -> str:
    """Assemble a self-contained HTML string with the Mermaid diagram.

    The HTML includes a header with summary stats, a color legend, the Mermaid
    flowchart, a click-to-reveal detail panel, and inline JS for interaction.

    Args:
        mermaid_text: A complete Mermaid flowchart string (e.g. ``flowchart LR ...``).
        task_data_json: JSON string mapping sanitized task IDs to task metadata.
        graph: The TaskGraph used to compute summary statistics.

    Returns:
        A complete HTML document string.
    """
    graph_id = html.escape(str(graph.graph_id))
    summary = _compute_summary(graph)
    legend_html = _build_legend_html()

    # Escape "</script>" sequences to prevent breaking out of the <script> tag.
    # This is the standard mitigation for inline JSON in script elements.
    safe_task_data_json = task_data_json.replace("</", r"<\/")

    status_line = (
        f"Status: {html.escape(str(summary['overall']))} "
        f"| Tasks: {summary['complete_tasks']}/{summary['total_tasks']} "
        f"| Failures: {summary['failure_count']} | Stalled: {summary['stalled_count']}"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Boilermaker DAG: {graph_id}</title>
  <script src="{MERMAID_CDN_URL}"></script>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      margin: 0; padding: 20px; background: #f8f9fa;
    }}
    header {{ margin-bottom: 16px; }}
    h1 {{ margin: 0 0 8px 0; font-size: 1.4rem; }}
    .summary {{ color: #495057; font-size: 0.95rem; }}
    .legend {{
      margin: 12px 0; padding: 10px 14px; background: #fff;
      border: 1px solid #dee2e6; border-radius: 6px; font-size: 0.85rem;
    }}
    .mermaid {{ background: #fff; padding: 16px; border-radius: 6px; border: 1px solid #dee2e6; }}
    #detail-panel {{
      display: none; position: fixed; top: 0; right: 0; width: 400px; height: 100%;
      background: #fff; border-left: 2px solid #dee2e6; padding: 20px;
      overflow-y: auto; box-shadow: -4px 0 12px rgba(0,0,0,0.1); z-index: 1000;
    }}
    #detail-panel.visible {{ display: block; }}
    #detail-panel h2 {{ margin-top: 0; font-size: 1.1rem; }}
    #detail-panel .field {{ margin-bottom: 12px; }}
    #detail-panel .field-label {{ font-weight: 600; color: #495057; font-size: 0.85rem; }}
    #detail-panel .field-value {{ margin-top: 2px; }}
    #detail-panel pre {{
      background: #f1f3f5; padding: 12px; border-radius: 4px;
      overflow-x: auto; font-size: 0.82rem; white-space: pre-wrap;
      word-wrap: break-word; font-family: 'SF Mono', Monaco, Consolas, monospace;
    }}
    #detail-panel ul {{ margin: 4px 0; padding-left: 20px; }}
    #detail-panel li {{ margin-bottom: 4px; }}
    #detail-close {{
      position: absolute; top: 12px; right: 12px; cursor: pointer;
      background: none; border: none; font-size: 1.2rem; color: #495057;
    }}
  </style>
</head>
<body>
  <header>
    <h1>Graph: {graph_id}</h1>
    <div class="summary">
      Status: {status_line}
    </div>
  </header>
  {legend_html}
  <div class="mermaid">
{html.escape(mermaid_text)}
  </div>
  <div id="detail-panel">
    <button id="detail-close" aria-label="Close">&times;</button>
    <div id="detail-content"></div>
  </div>
  <script>
    const taskData = {safe_task_data_json};

    mermaid.initialize({{ startOnLoad: false, securityLevel: 'loose' }});

    function showDetail(nodeId) {{
      var task = taskData[nodeId];
      if (!task) return;
      var h = '<h2>' + escapeHtml(task.function_name) + '</h2>';
      h += field('Task ID', escapeHtml(task.task_id));
      h += field('Function', escapeHtml(task.function_name));
      h += field('Status', escapeHtml(task.status || 'unknown'));
      h += field('Type', escapeHtml(task.type));
      if (task.is_stalled) {{
        h += field('Stalled', '<strong style="color:#dc3545;">Yes</strong>');
      }}
      if (task.result !== undefined && task.result !== null) {{
        h += field('Result', '<pre>' + escapeHtml(String(task.result)) + '</pre>');
      }}
      if (task.errors && task.errors.length > 0) {{
        var items = task.errors.map(function(e) {{ return '<li>' + escapeHtml(e) + '</li>'; }}).join('');
        h += field('Errors', '<ul>' + items + '</ul>');
      }}
      if (task.formatted_exception) {{
        h += field('Exception', '<pre>' + escapeHtml(task.formatted_exception) + '</pre>');
      }}
      document.getElementById('detail-content').innerHTML = h;
      document.getElementById('detail-panel').classList.add('visible');
    }}

    function field(label, value) {{
      return '<div class="field"><div class="field-label">' + label + '</div><div class="field-value">' + value + '</div></div>';
    }}

    function escapeHtml(text) {{
      var d = document.createElement('div');
      d.appendChild(document.createTextNode(text));
      return d.innerHTML;
    }}

    function closePanel() {{
      document.getElementById('detail-panel').classList.remove('visible');
    }}

    document.getElementById('detail-close').addEventListener('click', closePanel);

    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') closePanel();
    }});

    document.addEventListener('click', function(e) {{
      var panel = document.getElementById('detail-panel');
      if (panel.classList.contains('visible') && !panel.contains(e.target)) {{
        var node = e.target.closest('.node');
        if (!node) closePanel();
      }}
    }});

    // Render Mermaid explicitly; click callbacks are handled via Mermaid's
    // native "click nodeId showDetail" directives in the flowchart definition.
    // securityLevel: 'loose' is required for these callbacks to fire.
    mermaid.run().then(function() {{
      // Style all nodes as clickable after rendering completes.
      var nodes = document.querySelectorAll('.node');
      nodes.forEach(function(node) {{ node.style.cursor = 'pointer'; }});
    }});
  </script>
</body>
</html>"""


def open_visual(
    graph: TaskGraph,
    stalled_task_ids: set[TaskId],
    console: Console,
) -> str:
    """Generate a visual DAG HTML file and open it in the default browser.

    Orchestrates the full visual rendering pipeline: generates Mermaid text
    and task data JSON via ``_mermaid.py``, assembles the HTML, writes it
    to a temp file, and opens the browser.

    Args:
        graph: The TaskGraph to visualize.
        stalled_task_ids: Pre-computed set of stalled task IDs.
        console: Rich Console for printing status messages.

    Returns:
        The absolute path to the generated HTML file.
    """
    mermaid_text = generate_mermaid(graph, stalled_task_ids)
    task_data_json = build_task_data_json(graph, stalled_task_ids)
    html_content = render_visual_html(mermaid_text, task_data_json, graph)

    with tempfile.NamedTemporaryFile(
        suffix=".html",
        prefix="boilermaker-dag-",
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as f:
        f.write(html_content)
        path = f.name

    console.print("Opening DAG visualization in browser...")
    console.print(f"HTML file: {path}")
    webbrowser.open(f"file://{path}")

    return path
