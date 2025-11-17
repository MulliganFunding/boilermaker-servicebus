# Callbacks & Chains

Simple task chaining for linear workflows.

## Basic Usage

```py title="Basic callbacks"
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

!!! important "No state passed!"
    No state is passed *between* the elements of the chain.
    In other words, all task signatures are *immutable* from the time that they're published!

## Chain Method

Another, more convenient, way to chain `on_success` callbacks is with the `chain` method:

```py title="Boilermaker.chain method"
def grows_only(state, one: int, two: int=3) -> int:
    if two <= 0:
        raise ValueError("FAILING ON NEGATIVES")
    return one + two

def fail_task(state):
    print("This indicates a failure")

workflow = app.chain(
    Task.si(grows_only, 1, 2),
    Task.si(grows_only, 3),
    Task.si(grows_only, 2, two=-4)
    Task.si(grows_only, 5, two=6),
    on_failure=Task.si(fail_task),
)
await app.publish_task(workflow)
```

In this case `fail_task` will be added on as `on_failure` callback for **each** task in the chain, so if *any* task fails, it will be invoked.

## Chain Method as an Operator

Finally, a binary operator has also been created for visually simplifying chains:

```py title="chain operator"
# Use >> for chaining
taskA >> taskB >> taskC
await app.publish_task(taskA)
```

!!! note "No failures here"
    There is no binary operator for adding a failure callback.

## TaskGraphs Can Also Chain

TaskGraphs also offer chaining, but there they can be part of larger DAGs:

```py title="Task Graphs"
# TaskGraph equivalent (for DAGs)
from boilermaker.task import TaskGraphBuilder

# Simple chain (current approach)
main = app.create_task(process_data, "input")
success = app.create_task(send_notification)
main.on_success = success
await app.publish_task(main)


graph = (TaskGraphBuilder()
    .chain(
        app.create_task(process_data, "input"),
        app.create_task(send_notification)
    ).then(Task.si(some_other_task))
    .build())
await app.publish_graph(graph)
```
