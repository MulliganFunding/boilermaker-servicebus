# Task Graph API Reference

API documentation for TaskGraph, TaskGraphBuilder, and TaskChain classes used to build complex workflows with dependencies.

## Module-level Constants

### `LAST_ADDED`

```python
from boilermaker.task import LAST_ADDED
```

Sentinel value used as the default for `depends_on` parameters. When passed (or omitted,
since it is the default), the method resolves dependencies using the builder's internal
cursor — the last task(s) added. Use `None` to declare an explicit root task with no
parents.

## TaskChain

::: boilermaker.task.graph.TaskChain
  options:
    show_source: true
    show_root_heading: true
    members:
      - __init__
      - head
      - last

## TaskGraphBuilder

::: boilermaker.task.graph.TaskGraphBuilder
  options:
    show_source: true
    show_root_heading: true
    members:
      - __init__
      - add
      - then
      - parallel
      - add_chain
      - chain
      - on_all_failed
      - build

## TaskGraph

::: boilermaker.task.graph.TaskGraph
  options:
    show_source: true
    show_root_heading: true
    members:
      - graph_id
      - children
      - fail_children
      - edges
      - fail_edges
      - all_failed_callback_id
      - results
      - add_task
      - add_failure_callback
      - add_all_failed_callback
      - schedule_task
      - add_result
      - task_is_ready
      - generate_ready_tasks
      - generate_failure_ready_tasks
      - generate_all_failed_callback_task
      - completed_successfully
      - has_failures
      - is_terminal_failed
      - is_complete

## All-Failed Callback API

### `TaskGraphBuilder.on_all_failed(task: Task) -> TaskGraphBuilder`

Registers a graph-level fan-in error callback. Returns `self` for chaining. Raises `ValueError`
if called more than once on the same builder.

### `TaskGraphBuilder.build(on_all_failed: Task | None = None) -> TaskGraph`

Equivalent to calling `.on_all_failed(task)` before `.build()`. Raises `ValueError` if both
the method and the kwarg are used on the same builder instance.

### `TaskGraph.all_failed_callback_id: TaskId | None`

`None` when no graph-level error callback is registered; otherwise the `TaskId` of the
callback task. The callback task is stored in `fail_children` but has no entry in
`fail_edges`.

### `TaskGraph.add_all_failed_callback(callback_task: Task) -> None`

Registers `callback_task` as the graph-level fan-in error callback. Stores it in
`fail_children` and sets `all_failed_callback_id`. Does **not** add it to `fail_edges`.

Raises `ValueError` if:

- `all_failed_callback_id` is already set (only one per graph is supported), or
- `callback_task.task_id` is already registered as a per-task failure callback.

If the callback task has `on_success` or `on_failure` set, those are resolved into the graph
exactly as they are for tasks added via `add_task()`.

### `TaskGraph.is_terminal_failed() -> bool`

Returns `True` when all main tasks and all reachable per-task failure callbacks have reached
a finished state, and at least one main task has a failure-type status (`Failure`,
`RetriesExhausted`, or `Deadlettered`). This is the trigger condition for dispatching the
`all_failed_callback`.

### `TaskGraph.generate_all_failed_callback_task() -> Generator[Task]`

Yields the callback task at most once, when:

1. `all_failed_callback_id` is set.
2. `is_terminal_failed()` is `True`.
3. The callback's result status is `Pending` (not yet scheduled or completed).

Yields nothing if no callback is registered, if the graph has not yet settled, if the graph
completed without failures, or if the callback has already been scheduled or finished.

### `is_complete()` interaction

When a graph-level callback is registered and the graph reaches terminal-failed state,
`is_complete()` returns `False` until the callback itself has reached a finished state. If the
graph completes without any failures, `is_complete()` behaves identically to the pre-callback
behavior — the registered-but-untriggered callback has no effect on completion.

## Quick Examples

```py
from boilermaker.task import LAST_ADDED, TaskChain, TaskGraphBuilder

# Sequential workflow: A → B → C
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .then(task_b)
    .then(task_c)
    .build()
)

# Fan-out: A → (B, C, D)
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .parallel(task_b, task_c, task_d)
    .build()
)

# Diamond: A → (B, C) → D
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .parallel(task_b, task_c)
    .then(task_d)
    .build()
)

# Inline failure handler
graph = (
    TaskGraphBuilder()
    .add(main_task, on_failure=cleanup_task)
    .then(success_task)  # only runs if main_task succeeds
    .build()
)

# Independent chains with fan-in join
chain_abc = TaskChain(task_a, task_b, task_c)
chain_de  = TaskChain(task_d, task_e)

graph = (
    TaskGraphBuilder()
    .add_chain(chain_abc, depends_on=None)  # root; cursor accumulates
    .add_chain(chain_de,  depends_on=None)  # root; cursor accumulates
    .then(task_f)                           # depends on both chain lasts
    .build()
)

# TaskChain with shared failure handler
pipeline = TaskChain(task_a, task_b, task_c, on_any_failure=error_handler)
graph = TaskGraphBuilder().add_chain(pipeline).build()

# Graph-level error callback (fires once after the whole graph settles with failures)
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .add(task_b)
    .build(on_all_failed=handle_graph_failure)
)

# Equivalent using the chainable method
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .add(task_b)
    .on_all_failed(handle_graph_failure)
    .build()
)
```
