# Callbacks & Chains

Simple task chaining for linear workflows.

!!! tip "Use TaskGraphs Instead"
    For complex workflows, use [TaskGraphs](task-graphs.md) which provide better control, parallel execution, and dependency management.

## Basic Usage

```python
# Create tasks
main = app.create_task(process_data, "input")
success = app.create_task(send_notification)
failure = app.create_task(cleanup_error)

# Set callbacks
main.on_success = success
main.on_failure = failure

# Publish
await app.publish_task(main)
```

## Chain Operator

```python
# Use >> for chaining
taskA >> taskB >> taskC
await app.publish_task(taskA)
```

## Migration to TaskGraphs

When your workflow grows complex, consider migrating to TaskGraphs:

```python
# Simple chain (current approach)
main = app.create_task(process_data, "input")
success = app.create_task(send_notification)
main.on_success = success
await app.publish_task(main)

# TaskGraph equivalent (recommended for complex workflows)
from boilermaker.task import TaskGraphBuilder

graph = (TaskGraphBuilder()
    .chain(
        app.create_task(process_data, "input"),
        app.create_task(send_notification)
    )
    .build())
await app.publish_graph(graph)
