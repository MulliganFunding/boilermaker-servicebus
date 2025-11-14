# Task Graphs

Build complex workflows with dependencies using TaskGraphBuilder. Task graphs allow you to create Directed Acyclic Graphs (DAGs) where tasks run in parallel when possible and wait for their dependencies to complete.

!!! tip "When to Use Task Graphs"
    Use TaskGraphBuilder for workflows with:

    - Multiple parallel tasks
    - Complex dependencies between tasks
    - Fan-out/fan-in patterns
    - Conditional execution paths

    For simple sequential tasks, consider using [task chains](callbacks-chains.md) instead.

## Quick Start

```python
from boilermaker.task import TaskGraphBuilder

# Register your tasks first
app.register_async(fetch_data, policy=retries.RetryPolicy.default())
app.register_async(process_data, policy=retries.RetryPolicy.default())
app.register_async(save_results, policy=retries.RetryPolicy.default())

# Create and publish a workflow
async def create_workflow():
    fetch_task = app.create_task(fetch_data, "https://api.example.com")
    process_task = app.create_task(process_data)
    save_task = app.create_task(save_results)

    # Build: fetch → process → save
    graph = (TaskGraphBuilder()
        .add(fetch_task)
        .then(process_task)
        .then(save_task)
        .build())

    await app.publish_graph(graph)
```

## Common Patterns

```python
# Sequential: A → B → C
graph = TaskGraphBuilder().chain(task_a, task_b, task_c).build()

# Parallel: A, B, C run together
graph = TaskGraphBuilder().parallel([task_a, task_b, task_c]).build()

# Fan-out: A → (B, C, D) → E
graph = (TaskGraphBuilder()
    .add(task_a)
    .parallel([task_b, task_c, task_d])
    .then(task_e)
    .build())
```

## Failure Handling

```python
# Add failure callbacks with on_failure()
graph = (TaskGraphBuilder()
    .add(main_task)
    .on_failure(main_task.task_id, cleanup_task)
    .then(success_task)  # Only runs if main succeeds
    .build())
```

## Storage Setup

TaskGraphs need storage to track execution:

```python
from boilermaker.storage import BlobClientStorage

storage = BlobClientStorage(
    az_storage_url="https://yourstorage.blob.core.windows.net",
    container_name="task-graphs",
    credentials=DefaultAzureCredential()
)

app = Boilermaker(state=your_state, service_bus_client=service_bus, results_storage=storage)
```
