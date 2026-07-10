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

Delete old task-result blobs from Azure Blob Storage.

```sh
boilermaker --storage-url <url> --container <name> purge \
    --older-than <days> \
    [--dry-run] \
    [--force] \
    [--all-graphs]
```

**How "old" is decided**

`purge` groups blobs by graph and keeps or deletes each group as a unit. A group is eligible when its **creation time** is more than `--older-than` days ago.

By default, discovery uses the Azure blob **tag index** (`created_date < cutoff`): fast, server-side, no full container scan. It only sees graphs that carry a `created_date` tag — graphs written before tagging existed (or otherwise untagged) are invisible to it. To sweep those, use **`--all-graphs`**, which instead lists every graph-id prefix and dates each graph by the millisecond timestamp embedded in its UUID7 `graph_id`. That path is slower (it scans prefixes) but finds untagged/legacy graphs. `purge` uses one path or the other — never both.

!!! tip "Cleaning up legacy (pre-tag) graphs"
    If your container has graphs created before `created_date` tagging was in use, the default run will not find them. Run `purge --older-than <days> --all-graphs` (start with `--dry-run`) to reach them.

**What gets deleted vs. protected**

| Data | Default behavior | With `--force` |
|---|---|---|
| Graph, all tasks finished | Deleted when old | Deleted when old |
| Graph with in-progress tasks (`scheduled`, `started`, `retry`) | **Skipped** quietly (counted in the summary, exit code `1`) | Deleted when old |
| Standalone task-result (a task run outside a graph — no `graph.json`), finished | Deleted when old | Deleted when old |
| Standalone task-result still in progress | **Skipped** quietly (counted in the summary, exit code `1`) | Deleted when old |
| Corrupted graph — missing `graph.json` but still holds task blobs, so its status can't be verified | **Skipped** and logged as a warning | Deleted when old |

In-progress work is a normal, expected state, so it is skipped without noise — you see it in the run summary and the exit code, not as a per-item message. Diagnostics are emitted through the standard `logging` module (logger `boilermaker.cli`), so you can route or suppress them like any other logs; a warning is logged **only** for genuinely **corrupted** data (a directory with task-result blobs but no `graph.json`, or a `graph.json` that fails to load), so a warning always means "something here needs a look."

Within each eligible group, task-result blobs are deleted first and `graph.json` last; if any task-result deletion fails, that group's `graph.json` is left in place so the group is retried on the next run rather than orphaned.

**Options**

| Option | Required | Description |
|---|---|---|
| `--older-than DAYS` | Yes | Delete graphs whose creation time is more than `DAYS` days ago (1–30 inclusive). By default, age comes from the `created_date` tag; with `--all-graphs` it comes from the UUID7 `graph_id` timestamp. |
| `--dry-run` | No | Print what would be deleted without deleting any blobs |
| `--force` | No | Also delete old graphs/tasks that are still in progress, and old corrupted graphs that are missing `graph.json`. Use with care — this removes work whose status could not be verified. |
| `--all-graphs` | No | Slower, thorough discovery: skip the tag index and list every graph-id prefix, dating each graph by its UUID7 timestamp. Use this to find untagged/pre-tag graphs the default (tag-based) discovery cannot see. |

**Example — dry run first, then execute**

```sh
# Step 1: preview what will be deleted
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    purge --older-than 7 --dry-run

# Step 2: execute after confirming the plan
boilermaker \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    purge --older-than 7
```

**Dry-run output**

```
                    Purge Plan
   Blobs last modified before 2026-04-07 (older than 7 days)

 Graph                                        Blobs
 ──────────────────────────────────────────  ─────
 019d8c0c-bd9b-7c23-be84-4d0799d7ecd4        12
 019d8c0c-bd9b-7c23-be84-4d0799d7ecd5        3

Total: 2 graphs, 15 blobs
[DRY RUN] No blobs were deleted.
```

Only **corrupted** data is logged (as a `WARNING` on the `boilermaker.cli` logger) and excluded from the plan; in-progress work is skipped silently — see the summary and exit code instead:

```
WARNING  boilermaker.cli  Graph 019d8c0c-... appears corrupted — it has task-result blobs but no graph.json, so its status cannot be verified. Skipping (use --force to delete).
```

Pass `--force` to delete corrupted and in-progress groups anyway.

**Post-deletion output**

```
Deleted 15 blobs across 2 graphs.
Skipped 1 graph(s)/task(s) with in-progress tasks.
```

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Success — no errors, nothing skipped for in-progress tasks (or dry-run completed) |
| `1` | One or more graphs/tasks skipped because they are still in progress |
| `2` | Unrecoverable error (auth failure, container not found, all deletions failed) |

!!! note "Corrupted data does not change the exit code"
    A logged warning for a corrupted group is there for a human to investigate; it does not on its own make the run exit non-zero. Only in-progress skips set exit code `1`.

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
