"""
Workflow execution engine for coordinating DAG-like workflows

This module provides the execution engine that coordinates the execution
of complex workflows using the existing task infrastructure.
"""
import asyncio
import copy
import logging
from typing import Any, TYPE_CHECKING

from . import retries
from .task import Task
from .workflow import Workflow, WorkflowNode, WorkflowNodeType

if TYPE_CHECKING:
    from .app import Boilermaker

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """
    Executes DAG-like workflows by coordinating task execution
    and managing dependencies.
    """

    def __init__(self, boilermaker: "Boilermaker"):
        self.boilermaker = boilermaker
        self.active_workflows: dict[str, Workflow] = {}
        self.workflow_results: dict[str, Any] = {}
        self.workflow_errors: dict[str, Exception] = {}

    async def execute_workflow(self, workflow: Workflow) -> Any:
        """
        Execute a workflow and return the final result

        This method coordinates the execution of all tasks in the workflow,
        respecting dependencies and handling failures appropriately.
        """
        workflow_id = workflow.workflow_id
        self.active_workflows[workflow_id] = workflow

        try:
            logger.info(f"Starting workflow execution: {workflow_id}")

            # Execute the workflow
            result = await self._execute_workflow_internal(workflow)

            # Store the result
            self.workflow_results[workflow_id] = result
            logger.info(f"Workflow completed successfully: {workflow_id}")

            return result

        except Exception as e:
            logger.error(f"Workflow failed: {workflow_id}, error: {e}")
            self.workflow_errors[workflow_id] = e
            raise
        finally:
            # Clean up
            if workflow_id in self.active_workflows:
                del self.active_workflows[workflow_id]

    async def _execute_workflow_internal(self, workflow: Workflow) -> Any:
        """Internal method to execute a workflow"""
        # Start with nodes that have no dependencies
        ready_nodes = workflow.get_ready_nodes()

        while ready_nodes:
            # Execute all ready nodes in parallel
            tasks = []
            for node_id in ready_nodes:
                task = asyncio.create_task(self._execute_node(workflow, node_id))
                tasks.append((node_id, task))

            # Wait for all tasks to complete
            for node_id, task in tasks:
                try:
                    result = await task
                    workflow.mark_completed(node_id, result)
                except Exception as e:
                    workflow.mark_failed(node_id, e)
                    # If any node fails, we might want to handle this differently
                    # For now, we'll continue with other nodes
                    logger.error(f"Node {node_id} failed: {e}")

            # Get next batch of ready nodes
            ready_nodes = workflow.get_ready_nodes()

        # Check if workflow completed successfully
        if workflow.is_completed():
            return workflow.get_final_result()
        elif workflow.is_failed():
            # Get the first error for now
            failed_nodes = list(workflow.failed_nodes)
            if failed_nodes:
                raise workflow.errors[failed_nodes[0]]
            else:
                raise Exception("Workflow failed but no specific error found")
        else:
            raise Exception("Workflow did not complete or fail properly")

    async def _execute_node(self, workflow: Workflow, node_id: str) -> Any:
        """Execute a single workflow node"""
        node = workflow.nodes[node_id]

        if node.node_type == WorkflowNodeType.TASK:
            return await self._execute_task_node(workflow, node)
        else:
            raise ValueError(f"Unsupported node type: {node.node_type}")

    async def _execute_task_node(self, workflow: Workflow, node: WorkflowNode) -> Any:
        """Execute a task node"""
        if not node.task:
            raise ValueError(f"Task node {node.id} has no task")

        # Create a copy of the task to avoid modifying the original
        task = copy.deepcopy(node.task)

        # Add workflow context to the task payload
        if "workflow_context" not in task.payload:
            task.payload["workflow_context"] = {}
        task.payload["workflow_context"].update({
            "workflow_id": workflow.workflow_id,
            "node_id": node.id,
            "parent_results": self._get_parent_results(workflow, node.id)
        })

        # Execute the task using the boilermaker
        result = await self.boilermaker.task_handler(task, None)
        return result

    def _get_parent_results(self, workflow: Workflow, node_id: str) -> dict[str, Any]:
        """Get results from parent nodes"""
        parent_results = {}
        node = workflow.nodes[node_id]

        for parent_id in node.parents:
            if parent_id in workflow.results:
                parent_results[parent_id] = workflow.results[parent_id]

        return parent_results

    async def execute_chain(self, tasks: list[Task]) -> Any:
        """Execute a chain of tasks sequentially"""
        from .workflow import Chain

        chain = Chain(tasks)
        workflow = chain.build_workflow()
        return await self.execute_workflow(workflow)

    async def execute_chord(self, header_tasks: list[Task], callback_task: Task) -> Any:
        """Execute a chord pattern"""
        from .workflow import Chord

        chord = Chord(header_tasks, callback_task)
        workflow = chord.build_workflow()
        return await self.execute_workflow(workflow)

    async def execute_group(self, tasks: list[Task]) -> list[Any]:
        """Execute a group of tasks in parallel"""
        from .workflow import Group

        group = Group(tasks)
        workflow = group.build_workflow()
        await self.execute_workflow(workflow)

        # For groups, we need to collect all results
        results = []
        for node_id in workflow.execution_order:
            if node_id in workflow.results:
                results.append(workflow.results[node_id])
        return results


class WorkflowTaskHandler:
    """
    Special task handler for workflow coordination tasks

    This allows workflows to be executed as part of the normal task queue
    by creating special coordination tasks.
    """

    def __init__(self, executor: WorkflowExecutor):
        self.executor = executor

    async def handle_workflow_start(self, state: Any, workflow_data: dict[str, Any]) -> Any:
        """Handle starting a workflow"""
        # This would need to reconstruct the workflow from the data
        # For now, this is a placeholder
        workflow_id = workflow_data.get("workflow_id")
        logger.info(f"Starting workflow: {workflow_id}")
        # Implementation would depend on how workflows are serialized
        return {"status": "started", "workflow_id": workflow_id}

    async def handle_workflow_complete(self, state: Any, workflow_data: dict[str, Any]) -> Any:
        """Handle workflow completion"""
        workflow_id = workflow_data.get("workflow_id")
        result = workflow_data.get("result")
        logger.info(f"Workflow completed: {workflow_id}")
        return result


# Extension methods for Boilermaker
def add_workflow_support(boilermaker: "Boilermaker") -> WorkflowExecutor:
    """
    Add workflow support to a Boilermaker instance

    This creates a workflow executor and registers necessary task handlers.
    """
    executor = WorkflowExecutor(boilermaker)

    # Register workflow coordination tasks
    handler = WorkflowTaskHandler(executor)
    boilermaker.register_async(handler.handle_workflow_start, policy=retries.NoRetry())
    boilermaker.register_async(handler.handle_workflow_complete, policy=retries.NoRetry())

    return executor
