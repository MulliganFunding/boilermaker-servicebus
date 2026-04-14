# CLI Reference

Boilermaker ships a `boilermaker` command for inspecting and managing TaskGraph state directly from Azure Blob Storage and Azure Service Bus. It is useful for diagnosing stalled graphs in production without touching application code.

## Installation

The CLI is included when you install the package:

```sh
pip install "boilermaker-servicebus"
```

## Global options

`--storage-url` and `--container` are required by all commands and go **before** the subcommand name. They can also be provided via environment variables:

| Option | Env var | Description |
|---|---|---|
| `--storage-url URL` | `BOILERMAKER_STORAGE_URL` | Azure Blob Storage account URL |
| `--container NAME` | `BOILERMAKER_CONTAINER` | Blob container name |
| `-v` / `--verbose` | — | Enable debug logging |
| `--no-color` | `NO_COLOR` | Disable colored output |

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    <subcommand> [subcommand options]
```

Setting `BOILERMAKER_STORAGE_URL` and `BOILERMAKER_CONTAINER` in your environment lets you omit those flags entirely.

## Authentication

The CLI uses [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) for blob storage access. Make sure your environment is authenticated (e.g. via `az login`, a managed identity, or environment variables).

Commands that publish to Service Bus (`recover`, `invoke`) also use `DefaultAzureCredential` for Service Bus access.

## Commands

### `inspect`

Print a rich status panel and task table for a TaskGraph. Pass `--task` to drill into a single task.

```sh
boilermaker --storage-url <url> --container <name> inspect \
    --graph <graph_id> \
    [--task <task_id>] \
    [--json]
```

**Options**

| Option | Required | Description |
|---|---|---|
| `--graph GRAPH_ID` | Yes | The ID of the graph to inspect |
| `--task TASK_ID` | No | Inspect a single task (requires `--graph`) |
| `--json` | No | Output machine-readable JSON instead of formatted output |

**Example — inspect a graph**

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    inspect --graph "$GRAPH_ID"
```

Output (color-coded in a real terminal):

```
╭─────────────────────────────────────────────╮
│ Graph: 019d8c0c-bd9b-7c23-be84-4d0799d7ecd4 │
│ Status: In Progress                         │
│ Complete: 2/3                               │
│ Failures: 0  Stalled: 1                     │
╰─────────────────────────────────────────────╯

 Task ID (short)   Function            Status      Type
 ──────────────    ──────────────────  ──────────  ──────────
 be84-4c416d85     fetch_data          ✓ Success   child
 be84-4c5388b8     process_report      ⚠ Retry     child  STALLED
 be84-4c6a1b2c     send_notification   · Pending   child
```

A task is considered **stalled** if its status is `Scheduled`, `Started`, or `Retry` — states that indicate the task was dispatched but no worker has written a terminal result.

**Example — inspect a single task**

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    inspect --graph "$GRAPH_ID" --task "$TASK_ID"
```

Output:

```
╭─── Task Detail ────────────────────────────────────────────╮
│ Task ID:       019d8c0c-bd9b-7c23-be84-4c5388b8de6c        │
│ Function:      process_report                              │
│ Status:        ⚠ Retry  STALLED                            │
│ Type:          child                                       │
│ Graph:         019d8c0c-bd9b-7c23-be84-4d0799d7ecd4        │
│                                                            │
│ Dependencies:  fetch_data                                  │
│ Dependents:    send_notification                           │
╰────────────────────────────────────────────────────────────╯
```

**Example — JSON output**

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    inspect --graph "$GRAPH_ID" --json
```

```json
{
  "graph_id": "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4",
  "is_complete": false,
  "has_failures": false,
  "stalled_count": 1,
  "tasks": [
    {
      "task_id": "019d8c0c-bd9b-7c23-be84-4c416d85779d",
      "function_name": "fetch_data",
      "status": "Success",
      "type": "child",
      "is_stalled": false,
      "depends_on": []
    }
  ],
  "fail_tasks": []
}
```

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Graph is healthy — no stalled tasks |
| `1` | One or more stalled tasks found |
| `2` | Error (graph not found, missing arguments, etc.) |

`inspect` is safe to use in scripts and health checks:

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    inspect --graph "$GRAPH_ID"

if [ $? -eq 1 ]; then
    echo "Graph has stalled tasks — consider running recover"
fi
```

---

### `recover`

Re-publish all stalled tasks for a graph to Service Bus. Each recovery message uses a unique message ID (`<task_id>:recovery:<timestamp>`) so it bypasses Service Bus duplicate detection.

```sh
boilermaker --storage-url <url> --container <name> recover \
    --graph <graph_id> \
    --sb-namespace-url <url> \
    --sb-queue-name <name>
```

**Options**

| Option | Required | Description |
|---|---|---|
| `--graph GRAPH_ID` | Yes | The ID of the graph to recover |
| `--sb-namespace-url URL` | Yes | Service Bus namespace URL (e.g. `https://myns.servicebus.windows.net`) |
| `--sb-queue-name NAME` | Yes | Service Bus queue name |

**Example**

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    recover \
    --graph "$GRAPH_ID" \
    --sb-namespace-url "$SERVICE_BUS_NAMESPACE_URL" \
    --sb-queue-name "$QUEUE_NAME"
```

Output:

```
  ✓ Recovered: be84-4c5388b8 (process_report) — msg_id: 019d...:recovery:1713095548
  ✗ Failed:    be84-4c6a1b2c (send_notification) — <error message>
```

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | All stalled tasks recovered successfully |
| `1` | One or more tasks failed to recover |
| `2` | Error (graph not found, missing arguments, etc.) |

!!! warning "Recovery re-executes tasks"
    Recovery publishes the task again to the Service Bus queue. If the original task execution is still running (e.g. a very slow worker), both copies may run concurrently. Ensure your task handlers are idempotent before using `recover`.

---

### `purge`

Delete old task-result blobs from Azure Blob Storage. Graphs with in-progress tasks are automatically skipped.

```sh
boilermaker --storage-url <url> --container <name> purge \
    --task-results \
    --older-than <days> \
    [--dry-run]
```

**Options**

| Option | Required | Description |
|---|---|---|
| `--task-results` | Yes | Required flag to confirm intent to purge task results |
| `--older-than DAYS` | Yes | Delete blobs last modified more than `DAYS` days ago (1–30 inclusive) |
| `--dry-run` | No | Print what would be deleted without deleting any blobs |

**Example — dry run first, then execute**

```sh
# Step 1: preview what will be deleted
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    purge --task-results --older-than 7 --dry-run

# Step 2: execute after confirming the plan
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    purge --task-results --older-than 7
```

**Dry-run output**

```
Purge plan: blobs last modified before 2026-04-07 (older than 7 days)

 Graph ID                                    Blobs
 ──────────────────────────────────────────  ─────
 019d8c0c-bd9b-7c23-be84-4d0799d7ecd4        12
 019d8c0c-bd9b-7c23-be84-4d0799d7ecd5        3

[DRY RUN] No blobs were deleted.
```

Graphs with in-progress tasks are printed to stderr and excluded from the plan:

```
SKIP: Graph 019d8c0c-... has in-progress tasks (Scheduled: 1, Started: 0, Retry: 2). Skipping.
```

**Post-deletion output**

```
Deleted 15 blobs across 2 graphs.
Skipped 1 graph(s) due to in-progress tasks.
```

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Success — no errors, no skipped graphs (or dry-run completed) |
| `1` | One or more graphs skipped due to in-progress tasks |
| `2` | Unrecoverable error (auth failure, container not found, all deletions failed) |

!!! warning "Deletion is irreversible"
    Deleted blobs cannot be recovered unless Azure soft-delete is enabled on the storage account. Always run with `--dry-run` first to confirm the scope. Concurrent `purge` invocations against the same container are safe — any blob already deleted by a concurrent process returns a 404, which is treated as a no-op.

---

### `invoke`

Publish a single task to Service Bus. Useful for manually triggering a specific task without re-running the whole graph.

```sh
boilermaker --storage-url <url> --container <name> invoke <task_id> \
    --graph <graph_id> \
    --sb-namespace-url <url> \
    --sb-queue-name <name> \
    [--force]
```

**Options**

| Option | Required | Description |
|---|---|---|
| `TASK_ID` | Yes | Positional — the task ID to invoke |
| `--graph GRAPH_ID` | Yes | The graph the task belongs to |
| `--sb-namespace-url URL` | Yes | Service Bus namespace URL |
| `--sb-queue-name NAME` | Yes | Service Bus queue name |
| `--force` | No | Allow re-invocation of tasks already in a terminal state |

**Example**

```sh
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    invoke "$TASK_ID" \
    --graph "$GRAPH_ID" \
    --sb-namespace-url "$SERVICE_BUS_NAMESPACE_URL" \
    --sb-queue-name "$QUEUE_NAME"
```

Output:

```
Invoking task be84-4c5388b8 (process_report)
  Published -> msg_id: 019d...:invoke:1713095548
```

If the task is already in a terminal state (`Success`, `Failure`, `RetriesExhausted`, etc.) without `--force`:

```
Error: Task 019d8c0c-... is already in status "Success". Use --force to re-invoke a completed task.
```

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Task published successfully |
| `2` | Error (graph/task not found, terminal state without `--force`, publish failed) |

!!! warning "Invoke re-executes a task"
    `invoke` publishes the task to the queue without resetting its result blob. If the worker picks it up, it will overwrite the existing result. Use `--force` intentionally and ensure your task handler is idempotent.
