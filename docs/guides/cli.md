# CLI Reference

Boilermaker ships a `boilermaker-graph` command for inspecting and recovering TaskGraph state directly from Azure Blob Storage. It is useful for diagnosing stalled graphs in production without touching the application code.

## Installation

The CLI is included when you install the package:

```sh
pip install "boilermaker-servicebus"
```

## Authentication

The CLI uses [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) for blob storage access. Make sure your environment is authenticated (e.g. via `az login`, a managed identity, or environment variables).

## Commands

### `inspect`

Print a status table for a TaskGraph and optionally re-publish stalled tasks.

```sh
boilermaker-graph inspect <graph_id> \
    --storage-url <url> \
    --container <name> \
    [--recover] \
    [--sb-namespace-url <url>] \
    [--sb-queue-name <name>] \
    [-v]
```

**Arguments**

| Argument | Required | Description |
|---|---|---|
| `graph_id` | Yes | The ID of the graph to inspect |
| `--storage-url` | Yes | Azure Blob Storage account URL (e.g. `https://myaccount.blob.core.windows.net`) |
| `--container` | Yes | Blob container name |
| `--recover` | No | Re-publish stalled tasks to Service Bus |
| `--sb-namespace-url` | With `--recover` | Service Bus namespace URL (e.g. `https://myns.servicebus.windows.net`) |
| `--sb-queue-name` | With `--recover` | Service Bus queue name |
| `-v` / `--verbose` | No | Enable debug logging |

**Output**

```
Graph: 019d8c0c-bd9b-7c23-be84-4d0799d7ecd4
Task ID (short)         Function                   Status          Type
──────────────────────  ─────────────────────────  ──────────────  ────────────
be84-4c416d85779d       fetch_data                 success         child
be84-4c5388b8de6c       process_report             retry           child        ** STALLED **
be84-4c6a1b2c3d4e       send_notification          pending         child

Complete: False | Has failures: False | Stalled tasks: 1
```

A task is considered **stalled** if its status is `Scheduled`, `Started`, or `Retry` — states that indicate the task was dispatched but no worker has written a terminal result.

**Exit codes**

| Code | Meaning |
|---|---|
| `0` | Graph is healthy — no stalled tasks |
| `1` | One or more stalled tasks found |
| `2` | Error (graph not found, missing arguments, etc.) |

This makes `inspect` suitable for use in scripts and health checks:

```sh
boilermaker-graph inspect "$GRAPH_ID" \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME"

if [ $? -eq 1 ]; then
    echo "Graph has stalled tasks — manual recovery may be needed"
fi
```

**Recovery**

Pass `--recover` to re-publish all stalled tasks to Service Bus. Each recovery message uses a unique message ID (`<task_id>:recovery:<timestamp>`) so it bypasses Service Bus duplicate detection and is guaranteed to be delivered even if the original message is still in the dedup window.

```sh
boilermaker-graph inspect "$GRAPH_ID" \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    --recover \
    --sb-namespace-url "$SERVICE_BUS_NAMESPACE_URL" \
    --sb-queue-name "$QUEUE_NAME"
```

Recovery output:

```
  RECOVERED: 019d8c0c-be84-4c5388b8de6c (process_report) — published with 019d...:recovery:1713095548
  FAILED: 019d8c0c-be84-4c6a1b2c3d4e (send_notification) — <error>
```

!!! warning "Recovery re-executes tasks"
    Recovery publishes the task again with `acks_late` semantics. If the original task execution is still running (e.g. a very slow worker), both copies may run concurrently. Ensure your task handlers are idempotent before using `--recover`.

### `purge`

Delete old task-result blobs from Azure Blob Storage. Graphs with in-progress tasks are automatically skipped.

```sh
boilermaker-graph purge \
    --storage-url <url> \
    --container <name> \
    --older-than DAYS \
    [--dry-run] \
    [-v]
```

**Arguments**

| Argument | Required | Description |
|---|---|---|
| `--storage-url` | Yes | Azure Blob Storage account URL (e.g. `https://myaccount.blob.core.windows.net`) |
| `--container` | Yes | Blob container name |
| `--older-than DAYS` | Yes | Delete blobs last modified more than `DAYS` days ago (1–30 inclusive) |
| `--dry-run` | No | Print what would be deleted without deleting any blobs |
| `-v` / `--verbose` | No | Enable debug logging |

**Dry-run output**

Use `--dry-run` to preview what will be deleted before committing:

```
Purge plan: blobs last modified before 2026-04-07 (older than 7 days)

Graph: 019d8c0c-bd9b-7c23-be84-4d0799d7ecd4  (12 blobs)
Graph: 019d8c0c-bd9b-7c23-be84-4d0799d7ecd5  (3 blobs)

Total: 2 graphs, 15 blobs
[DRY RUN] No blobs were deleted.
```

Graphs with in-progress tasks are printed to stderr and excluded from the plan:

```
SKIP: Graph 019d8c0c-... has in-progress tasks (Scheduled: 1, Started: 0, Retry: 2). Skipping.
```

**Post-deletion output**

After a successful (non-dry-run) run:

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

**Recommended usage pattern**

Run `--dry-run` first to validate the deletion scope, then execute without it:

```sh
# Step 1: preview what will be deleted
boilermaker-graph purge \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    --older-than 7 \
    --dry-run

# Step 2: execute deletion after confirming the plan
boilermaker-graph purge \
    --storage-url "$AZURE_STORAGE_URL" \
    --container "$CONTAINER_NAME" \
    --older-than 7
```
