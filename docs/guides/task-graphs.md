# Task Graphs

Build complex workflows with dependencies (DAGs) using the `TaskGraphBuilder`.

Task graphs allow you to create Directed Acyclic Graphs (DAGs) where tasks can chain, run in
parallel, trigger failure callbacks, or converge in a fan-in join. All dependent children in
the DAG wait for their parents to complete.

!!! tip "When to Use Task Graphs"
    Use TaskGraphBuilder for workflows with:

    - Multiple parallel tasks
    - Complex dependencies between tasks
    - Fan-out/fan-in patterns
    - Failure handling scoped to individual tasks

    For simple sequential tasks, consider using [task chains](callbacks-chains.md) instead.

!!! tip "DAGs Only"
    They must be DAGs: accidentally creating a cycle will raise an exception.

## Quick Start

```py
from boilermaker.task import LAST_ADDED, TaskChain, TaskGraphBuilder

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
    graph = (
        TaskGraphBuilder()
        .add(fetch_task)
        .then(process_task)
        .then(save_task)
        .build()
    )

    await app.publish_graph(graph)
```

## Common Patterns

### Sequential: A → B → C

```py
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .then(task_b)
    .then(task_c)
    .build()
)
```

### Fan-out: A → (B, C, D)

```py
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .parallel(task_b, task_c, task_d)  # all depend on task_a; run in parallel
    .build()
)
```

### Fan-in: (A, B, C) → D

```py
graph = (
    TaskGraphBuilder()
    .parallel(task_a, task_b, task_c)  # three independent roots; cursor = [a, b, c]
    .then(task_d)                      # depends on all three; fan-in join
    .build()
)
```

### Diamond: A → (B, C) → D

```py
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .parallel(task_b, task_c)   # both depend on task_a; cursor = [b, c]
    .then(task_d)               # depends on task_b AND task_c; fan-in join
    .build()
)
```

## Independent Chains with Fan-In

The `TaskChain` class lets you build sequential sub-graphs independently and then compose
them in a `TaskGraphBuilder`. The key is `depends_on=None`, which marks a chain as a root
(no parents) while **accumulating** its last into the cursor rather than replacing it.

This pattern was previously impossible with the old API.

```py
from boilermaker.task import TaskChain, TaskGraphBuilder

# Two independent pipelines that converge on a single join task
#
#   task_a → task_b → task_c ─┐
#                               ├─→ task_f
#   task_d → task_e ───────────┘

chain_abc = TaskChain(task_a, task_b, task_c)
chain_de  = TaskChain(task_d, task_e)

graph = (
    TaskGraphBuilder()
    .add_chain(chain_abc, depends_on=None)   # root chain; cursor = [task_c]
    .add_chain(chain_de,  depends_on=None)   # root chain; cursor = [task_c, task_e]
    .then(task_f)                            # depends on task_c AND task_e; fan-in join
    .build()
)
```

!!! note "Cursor accumulation"
    `add_chain(chain, depends_on=None)` **appends** the chain's last to the cursor
    instead of replacing it. This is what makes the fan-in join possible. Using
    `add(task, depends_on=None)` instead **replaces** the cursor, which would lose
    the reference to the previous chain's last.

You can also be fully explicit using `depends_on` with `TaskChain` objects directly:

```py
graph = (
    TaskGraphBuilder()
    .add_chain(chain_abc, depends_on=None)
    .add_chain(chain_de,  depends_on=None)
    .add(task_f, depends_on=[chain_abc, chain_de])  # TaskChain resolves to its .last
    .build()
)
```

## Failure Handling

Failure callbacks are declared inline with `on_failure=` at the task they guard. The handler
is scoped to **that single task** — it does not cascade to downstream tasks.

### Single-task failure handler

```py
graph = (
    TaskGraphBuilder()
    .add(fetch_task, on_failure=fetch_error_handler)  # handler runs if fetch_task fails
    .then(process_task)                               # only runs if fetch_task succeeds
    .build()
)
```

- If `fetch_task` **succeeds**: `process_task` is scheduled. `fetch_error_handler` is never
  triggered.
- If `fetch_task` **fails**: `fetch_error_handler` is published. `process_task` is **never
  executed** — it will never receive the signal it is waiting for.

`then()` and `parallel()` also accept `on_failure=`:

```py
graph = (
    TaskGraphBuilder()
    .add(task_a)
    .then(task_b, on_failure=error_handler)
    .parallel(task_c, task_d, on_failure=parallel_error_handler)
    .build()
)
```

### Shared failure handler with `TaskChain`

If you want a **single error handler that triggers if any task in a sequence fails**, use
`TaskChain(on_any_failure=handler)`. The handler is registered on **each** task in the chain:

```py
pipeline = TaskChain(task_a, task_b, task_c, on_any_failure=pipeline_error_handler)

graph = TaskGraphBuilder().add_chain(pipeline).build()
```

Whichever task fails first, `pipeline_error_handler` runs. Subsequent tasks in the chain
(whose parent failed) never execute.

### Complex example: parallel chains with failure handling

```py
ingest_chain  = TaskChain(validate_task, ingest_task,  on_any_failure=ingest_error_handler)
process_chain = TaskChain(transform_task, enrich_task, on_any_failure=process_error_handler)

graph = (
    TaskGraphBuilder()
    .add_chain(ingest_chain,  depends_on=None)  # root; cursor = [ingest_task]
    .add_chain(process_chain, depends_on=None)  # root; cursor = [ingest_task, enrich_task]
    .then(aggregate_task, on_failure=cleanup_task)  # waits for both chains
    .build()
)
```

## Cursor Semantics

The builder maintains an internal cursor tracking the most recently added task(s). Methods
that omit `depends_on` implicitly depend on the cursor:

| Call | Cursor after |
|------|--------------|
| `add(task)` | `[task]` |
| `then(task)` | `[task]` |
| `parallel(t1, t2, t3)` | `[t1, t2, t3]` |
| `add_chain(chain)` | `[chain.last]` |
| `add_chain(chain, depends_on=None)` | `existing_cursor + [chain.last]` ← accumulates |
| `add(task, depends_on=None)` | `[task]` ← replaces (use `add_chain` for accumulation) |

## Storage Setup

TaskGraphs need storage to track execution:

```py
from boilermaker.storage import BlobClientStorage

storage = BlobClientStorage(
    az_storage_url="https://yourstorage.blob.core.windows.net",
    container_name="task-graphs",
    credentials=DefaultAzureCredential()
)

app = Boilermaker(state=your_state, service_bus_client=service_bus, results_storage=storage)
```
