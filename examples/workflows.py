"""
Examples of DAG-like workflows using Boilermaker

This example demonstrates how to use chains, chords, groups, and complex workflows
to build sophisticated task execution patterns.
"""
import asyncio
import os
import time

from boilermaker import retries
from boilermaker.app import Boilermaker
from boilermaker.config import Config
from boilermaker.failure import TaskFailureResult
from boilermaker.service_bus import AzureServiceBus

# Configuration
service_bus_namespace_url = os.environ["SERVICE_BUS_NAMESPACE_URL"]
service_bus_queue_name = os.environ["SERVICE_BUS_QUEUE_NAME"]
conf = Config(
    service_bus_namespace_url=service_bus_namespace_url,
    service_bus_queue_name=service_bus_queue_name,
)
service_bus_client = AzureServiceBus.from_config(conf)


# Application state
class App:
    def __init__(self, data):
        self.data = data
        self.results = {}


# Task definitions
async def fetch_data(state, source: str):
    """Simulate fetching data from a source"""
    print(f"Fetching data from {source}")
    await asyncio.sleep(1)  # Simulate work
    result = f"data_from_{source}"
    state.results[f"fetch_{source}"] = result
    return result


async def process_data(state, data_key: str):
    """Simulate processing data"""
    print(f"Processing {data_key}")
    await asyncio.sleep(1)  # Simulate work
    data = state.results.get(data_key, "no_data")
    result = f"processed_{data}"
    state.results[f"process_{data_key}"] = result
    return result


async def validate_data(state, data_key: str):
    """Simulate data validation"""
    print(f"Validating {data_key}")
    await asyncio.sleep(0.5)  # Simulate work
    data = state.results.get(data_key, "no_data")
    result = f"validated_{data}"
    state.results[f"validate_{data_key}"] = result
    return result


async def aggregate_results(state, *data_keys):
    """Aggregate multiple results"""
    print(f"Aggregating results from {data_keys}")
    await asyncio.sleep(1)  # Simulate work
    results = [state.results.get(key, "no_data") for key in data_keys]
    result = f"aggregated_{'_'.join(results)}"
    state.results["final_result"] = result
    return result


async def send_notification(state, message: str):
    """Simulate sending a notification"""
    print(f"Sending notification: {message}")
    await asyncio.sleep(0.5)  # Simulate work
    return f"notification_sent: {message}"


async def cleanup(state, *keys):
    """Simulate cleanup operations"""
    print(f"Cleaning up {keys}")
    await asyncio.sleep(0.5)  # Simulate work
    for key in keys:
        if key in state.results:
            del state.results[key]
    return "cleanup_complete"


async def error_handler(state, error_msg: str):
    """Handle errors in workflows"""
    print(f"Error handler: {error_msg}")
    return f"error_handled: {error_msg}"


# Create worker and register tasks
worker = Boilermaker(App({"key": "value"}), service_bus_client=service_bus_client)

# Register all tasks
worker.register_async(fetch_data, policy=retries.RetryPolicy.default())
worker.register_async(process_data, policy=retries.RetryPolicy.default())
worker.register_async(validate_data, policy=retries.RetryPolicy.default())
worker.register_async(aggregate_results, policy=retries.RetryPolicy.default())
worker.register_async(send_notification, policy=retries.NoRetry())
worker.register_async(cleanup, policy=retries.NoRetry())
worker.register_async(error_handler, policy=retries.NoRetry())


async def example_chain():
    """Example 1: Simple chain of tasks"""
    print("\n=== Example 1: Chain ===")
    print("A -> B -> C")

    # Create tasks
    task_a = worker.create_task(fetch_data, "database")
    task_b = worker.create_task(process_data, "fetch_database")
    task_c = worker.create_task(send_notification, "Processing complete")

    # Execute chain
    result = await worker.execute_chain(task_a, task_b, task_c)
    print(f"Chain result: {result}")


async def example_chord():
    """Example 2: Chord pattern (parallel tasks + callback)"""
    print("\n=== Example 2: Chord ===")
    print("A, B, C -> D (parallel execution, then callback)")

    # Create header tasks (parallel)
    header_tasks = [
        worker.create_task(fetch_data, "api1"),
        worker.create_task(fetch_data, "api2"),
        worker.create_task(fetch_data, "api3"),
    ]

    # Create callback task
    callback_task = worker.create_task(aggregate_results, "fetch_api1", "fetch_api2", "fetch_api3")

    # Execute chord
    result = await worker.execute_chord(header_tasks, callback_task)
    print(f"Chord result: {result}")


async def example_group():
    """Example 3: Group of parallel tasks"""
    print("\n=== Example 3: Group ===")
    print("A, B, C (all run in parallel)")

    # Create parallel tasks
    tasks = [
        worker.create_task(validate_data, "fetch_database"),
        worker.create_task(validate_data, "fetch_api1"),
        worker.create_task(validate_data, "fetch_api2"),
    ]

    # Execute group
    results = await worker.execute_group(*tasks)
    print(f"Group results: {results}")


async def example_complex_workflow():
    """Example 4: Complex workflow with builder"""
    print("\n=== Example 4: Complex Workflow ===")
    print("Complex DAG with multiple dependencies")

    # Create tasks
    fetch_db = worker.create_task(fetch_data, "database")
    fetch_api = worker.create_task(fetch_data, "api")

    process_db = worker.create_task(process_data, "fetch_database")
    process_api = worker.create_task(process_data, "fetch_api")

    validate_db = worker.create_task(validate_data, "process_fetch_database")
    validate_api = worker.create_task(validate_data, "process_fetch_api")

    aggregate = worker.create_task(aggregate_results, "validate_process_fetch_database", "validate_process_fetch_api")
    notify = worker.create_task(send_notification, "Workflow completed successfully")
    cleanup_task = worker.create_task(cleanup, "fetch_database", "fetch_api")

    # Build complex workflow
    wf = worker.workflow()

    # Add nodes
    db_node = wf.task(fetch_db, "fetch_db")
    api_node = wf.task(fetch_api, "fetch_api")

    process_db_node = wf.task(process_db, "process_db")
    process_api_node = wf.task(process_api, "process_api")

    validate_db_node = wf.task(validate_db, "validate_db")
    validate_api_node = wf.task(validate_api, "validate_api")

    aggregate_node = wf.task(aggregate, "aggregate")
    notify_node = wf.task(notify, "notify")
    cleanup_node = wf.task(cleanup_task, "cleanup")

    # Add dependencies
    wf.depends_on(process_db_node, db_node)
    wf.depends_on(process_api_node, api_node)

    wf.depends_on(validate_db_node, process_db_node)
    wf.depends_on(validate_api_node, process_api_node)

    wf.depends_on(aggregate_node, validate_db_node, validate_api_node)
    wf.depends_on(notify_node, aggregate_node)
    wf.depends_on(cleanup_node, aggregate_node)

    # Build and execute workflow
    workflow = wf.build()
    result = await worker.execute_workflow(workflow)
    print(f"Complex workflow result: {result}")


async def example_workflow_with_error_handling():
    """Example 5: Workflow with error handling"""
    print("\n=== Example 5: Workflow with Error Handling ===")

    # Create a task that might fail
    async def risky_task(state, should_fail: bool):
        if should_fail:
            return TaskFailureResult
        return "success"

    worker.register_async(risky_task, policy=retries.NoRetry())

    # Create tasks
    risky = worker.create_task(risky_task, True)  # This will fail
    error_handler_task = worker.create_task(error_handler, "Risky task failed")
    success_task = worker.create_task(send_notification, "All good!")

    # Build workflow with error handling
    wf = worker.workflow()

    risky_node = wf.task(risky, "risky")
    error_node = wf.task(error_handler_task, "error_handler")
    success_node = wf.task(success_task, "success")

    # The error handler runs if risky task fails
    # In a real implementation, you'd need more sophisticated error handling
    # This is a simplified example

    workflow = wf.build()

    try:
        result = await worker.execute_workflow(workflow)
        print(f"Workflow result: {result}")
    except Exception as e:
        print(f"Workflow failed as expected: {e}")


async def main():
    """Run all workflow examples"""
    print("Boilermaker Workflow Examples")
    print("=" * 50)

    # Enable workflow support
    worker.enable_workflows()

    # Run examples
    await example_chain()
    await example_chord()
    await example_group()
    await example_complex_workflow()
    await example_workflow_with_error_handling()

    print("\nAll examples completed!")


if __name__ == "__main__":
    asyncio.run(main())