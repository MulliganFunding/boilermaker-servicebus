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
      - results
      - add_task
      - add_failure_callback
      - schedule_task
      - add_result
      - task_is_ready
      - generate_ready_tasks
      - generate_failure_ready_tasks
      - completed_successfully
      - has_failures
      - is_complete

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

# Independent chains with fan-in join (previously impossible)
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
pipeline = TaskChain(task_a, task_b, task_c, on_failure=error_handler)
graph = TaskGraphBuilder().add_chain(pipeline).build()
```
