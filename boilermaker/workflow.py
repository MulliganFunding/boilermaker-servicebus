"""
Workflow abstractions for DAG-like task execution

This module provides higher-level abstractions for building complex workflows
including chains, chords, and arbitrary DAGs.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .task import Task


class WorkflowNodeType(Enum):
    """Types of workflow nodes"""
    TASK = "task"
    CHAIN = "chain"
    CHORD = "chord"
    GROUP = "group"


@dataclass
class WorkflowNode:
    """Represents a node in a workflow DAG"""
    id: str
    node_type: WorkflowNodeType
    task: Task | None = None
    children: list[str] = field(default_factory=list)
    parents: list[str] = field(default_factory=list)
    dependencies: set[str] = field(default_factory=set)
    completed: bool = False
    failed: bool = False
    result: Any = None
    error: Exception | None = None


class Workflow:
    """
    Represents a DAG-like workflow of tasks

    Supports chains, chords, and arbitrary DAGs with proper dependency management
    and execution coordination.
    """

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        self.nodes: dict[str, WorkflowNode] = {}
        self.execution_order: list[str] = []
        self.completed_nodes: set[str] = set()
        self.failed_nodes: set[str] = set()
        self.results: dict[str, Any] = {}
        self.errors: dict[str, Exception] = {}

    def add_node(self, node_id: str, node_type: WorkflowNodeType, task: Task | None = None) -> "Workflow":
        """Add a node to the workflow"""
        if node_id in self.nodes:
            raise ValueError(f"Node {node_id} already exists in workflow")

        self.nodes[node_id] = WorkflowNode(
            id=node_id,
            node_type=node_type,
            task=task
        )
        return self

    def add_dependency(self, from_node: str, to_node: str) -> "Workflow":
        """Add a dependency between nodes"""
        if from_node not in self.nodes or to_node not in self.nodes:
            raise ValueError(f"One or both nodes not found: {from_node}, {to_node}")

        self.nodes[from_node].children.append(to_node)
        self.nodes[to_node].parents.append(from_node)
        self.nodes[to_node].dependencies.add(from_node)
        return self

    def _calculate_execution_order(self) -> list[str]:
        """Calculate topological sort for execution order"""
        # Kahn's algorithm for topological sorting
        in_degree = {node_id: len(node.dependencies) for node_id, node in self.nodes.items()}
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order = []

        while queue:
            node_id = queue.pop(0)
            order.append(node_id)

            for child_id in self.nodes[node_id].children:
                in_degree[child_id] -= 1
                if in_degree[child_id] == 0:
                    queue.append(child_id)

        if len(order) != len(self.nodes):
            raise ValueError("Workflow contains cycles")

        return order

    def get_ready_nodes(self) -> list[str]:
        """Get nodes that are ready to execute (all dependencies satisfied)"""
        ready = []
        for node_id in self.nodes:
            if (node_id not in self.completed_nodes and
                node_id not in self.failed_nodes and
                self.nodes[node_id].dependencies.issubset(self.completed_nodes)):
                ready.append(node_id)
        return ready

    def mark_completed(self, node_id: str, result: Any = None):
        """Mark a node as completed"""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")

        self.completed_nodes.add(node_id)
        self.results[node_id] = result
        self.nodes[node_id].completed = True
        self.nodes[node_id].result = result

    def mark_failed(self, node_id: str, error: Exception):
        """Mark a node as failed"""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")

        self.failed_nodes.add(node_id)
        self.errors[node_id] = error
        self.nodes[node_id].failed = True
        self.nodes[node_id].error = error

    def is_completed(self) -> bool:
        """Check if the entire workflow is completed"""
        return len(self.completed_nodes) == len(self.nodes)

    def is_failed(self) -> bool:
        """Check if the workflow has any failed nodes"""
        return len(self.failed_nodes) > 0

    def get_final_result(self) -> Any:
        """Get the final result of the workflow (last node in execution order)"""
        if not self.is_completed():
            raise ValueError("Workflow not completed")
        final_node_id = self.execution_order[-1]
        return self.results.get(final_node_id)


class Chain:
    """
    Represents a linear chain of tasks that execute sequentially

    A -> B -> C -> D
    """

    def __init__(self, tasks: list[Task]):
        self.tasks = tasks
        self.workflow_id = f"chain_{id(self)}"

    def build_workflow(self) -> Workflow:
        """Build a workflow from this chain"""
        workflow = Workflow(self.workflow_id)

        for i, task in enumerate(self.tasks):
            node_id = f"task_{i}"
            workflow.add_node(node_id, WorkflowNodeType.TASK, task)

            if i > 0:
                # Add dependency from previous task
                workflow.add_dependency(f"task_{i-1}", node_id)

        workflow.execution_order = workflow._calculate_execution_order()
        return workflow


class Chord:
    """
    Represents a chord pattern: multiple tasks execute in parallel,
    then a callback task executes when all complete

    A, B, C -> D (where A, B, C run in parallel, then D runs)
    """

    def __init__(self, header_tasks: list[Task], callback_task: Task):
        self.header_tasks = header_tasks
        self.callback_task = callback_task
        self.workflow_id = f"chord_{id(self)}"

    def build_workflow(self) -> Workflow:
        """Build a workflow from this chord"""
        workflow = Workflow(self.workflow_id)

        # Add header tasks
        header_node_ids = []
        for i, task in enumerate(self.header_tasks):
            node_id = f"header_{i}"
            workflow.add_node(node_id, WorkflowNodeType.TASK, task)
            header_node_ids.append(node_id)

        # Add callback task
        callback_node_id = "callback"
        workflow.add_node(callback_node_id, WorkflowNodeType.TASK, self.callback_task)

        # Add dependencies from all header tasks to callback
        for header_id in header_node_ids:
            workflow.add_dependency(header_id, callback_node_id)

        workflow.execution_order = workflow._calculate_execution_order()
        return workflow


class Group:
    """
    Represents a group of tasks that execute in parallel

    A, B, C (all run in parallel)
    """

    def __init__(self, tasks: list[Task]):
        self.tasks = tasks
        self.workflow_id = f"group_{id(self)}"

    def build_workflow(self) -> Workflow:
        """Build a workflow from this group"""
        workflow = Workflow(self.workflow_id)

        for i, task in enumerate(self.tasks):
            node_id = f"task_{i}"
            workflow.add_node(node_id, WorkflowNodeType.TASK, task)
            # No dependencies - all run in parallel

        workflow.execution_order = workflow._calculate_execution_order()
        return workflow


class WorkflowBuilder:
    """
    Builder class for creating complex workflows with a fluent API
    """

    def __init__(self):
        self.workflow = Workflow(f"workflow_{id(self)}")
        self.node_counter = 0

    def task(self, task: Task, node_id: str | None = None) -> str:
        """Add a task node"""
        if node_id is None:
            node_id = f"task_{self.node_counter}"
            self.node_counter += 1

        self.workflow.add_node(node_id, WorkflowNodeType.TASK, task)
        return node_id

    def depends_on(self, dependent_node: str, *dependency_nodes: str) -> "WorkflowBuilder":
        """Add dependencies between nodes"""
        for dep_node in dependency_nodes:
            self.workflow.add_dependency(dep_node, dependent_node)
        return self

    def chain(self, *tasks: Task) -> list[str]:
        """Add a chain of tasks"""
        node_ids: list[str] = []
        for task in tasks:
            node_id = self.task(task)
            if node_ids:
                self.depends_on(node_id, node_ids[-1])
            node_ids.append(node_id)
        return node_ids

    def parallel(self, *tasks: Task) -> list[str]:
        """Add tasks that run in parallel"""
        return [self.task(task) for task in tasks]

    def chord(self, header_tasks: list[Task], callback_task: Task) -> list[str]:
        """Add a chord pattern"""
        header_ids = self.parallel(*header_tasks)
        callback_id = self.task(callback_task)
        self.depends_on(callback_id, *header_ids)
        return header_ids + [callback_id]

    def build(self) -> Workflow:
        """Build the final workflow"""
        self.workflow.execution_order = self.workflow._calculate_execution_order()
        return self.workflow


# Convenience functions for creating workflows
def chain(*tasks: Task) -> Chain:
    """Create a chain of tasks"""
    return Chain(list(tasks))


def chord(header_tasks: list[Task], callback_task: Task) -> Chord:
    """Create a chord pattern"""
    return Chord(header_tasks, callback_task)


def group(*tasks: Task) -> Group:
    """Create a group of parallel tasks"""
    return Group(list(tasks))


def workflow() -> WorkflowBuilder:
    """Create a workflow builder for complex DAGs"""
    return WorkflowBuilder()
