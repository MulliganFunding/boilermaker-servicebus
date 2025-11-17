# Task Graph API Reference

API documentation for TaskGraph and TaskGraphBuilder classes used to build complex workflows with dependencies.

## TaskGraphBuilder

::: boilermaker.task.graph.TaskGraphBuilder
  options:
    show_source: true
    show_root_heading: true
    members:
      - __init__
      - add
      - parallel
      - then
      - chain
      - on_failure
      - add_success_fail_branch
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
# Sequential workflow
graph = TaskGraphBuilder().chain(task_a, task_b, task_c).build()

# Parallel execution
graph = TaskGraphBuilder().parallel([task_a, task_b, task_c]).build()

# With failure handling
graph = (TaskGraphBuilder()
    .add(main_task)
    .on_failure(main_task.task_id, cleanup_task)
    .build())
```
