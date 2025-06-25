"""
Unit tests for WorkflowExecutor

Tests for workflow execution coordination and task handling.
"""
from unittest.mock import AsyncMock, Mock
import datetime
import pytest
import asyncio

from boilermaker.task import Task
from boilermaker.workflow import Workflow, WorkflowNodeType
from boilermaker.workflow_executor import WorkflowExecutor, WorkflowTaskHandler
from boilermaker.retries import RetryPolicy, RetryAttempts


class TestWorkflowExecutor:
    """Test WorkflowExecutor class"""

    @pytest.fixture
    def mock_boilermaker(self):
        """Create a mock Boilermaker instance"""
        mock = Mock()
        mock.task_handler = AsyncMock()
        return mock

    @pytest.fixture
    def executor(self, mock_boilermaker):
        """Create a WorkflowExecutor instance"""
        return WorkflowExecutor(mock_boilermaker)

    @pytest.fixture
    def simple_workflow(self):
        """Create a simple workflow for testing"""
        workflow = Workflow("test_workflow")
        task_a = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_b = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task_a)
        workflow.add_node("B", WorkflowNodeType.TASK, task_b)
        workflow.add_dependency("A", "B")
        workflow.execution_order = workflow._calculate_execution_order()
        return workflow

    def test_executor_creation(self, mock_boilermaker):
        """Test creating a workflow executor"""
        executor = WorkflowExecutor(mock_boilermaker)

        assert executor.boilermaker == mock_boilermaker
        assert executor.active_workflows == {}
        assert executor.workflow_results == {}
        assert executor.workflow_errors == {}

    async def test_execute_workflow_success(self, executor, simple_workflow):
        """Test successful workflow execution"""
        executor.boilermaker.task_handler.side_effect = ["result_a", "result_b"]
        result = await executor.execute_workflow(simple_workflow)
        assert result == "result_b"  # Final result from the last task (B)
        assert executor.boilermaker.task_handler.call_count == 2

    async def test_execute_workflow_failure(self, executor, simple_workflow):
        """Test workflow execution with task failure"""
        executor.boilermaker.task_handler.side_effect = Exception("Task failed")
        with pytest.raises(Exception, match="Task failed"):
            await executor.execute_workflow(simple_workflow)

    async def test_execute_workflow_parallel_tasks(self, executor):
        """Test workflow execution with parallel tasks"""
        workflow = Workflow("parallel_workflow")
        task_a = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_b = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_c = Task(
            function_name="C",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task_a)
        workflow.add_node("B", WorkflowNodeType.TASK, task_b)
        workflow.add_node("C", WorkflowNodeType.TASK, task_c)
        workflow.execution_order = workflow._calculate_execution_order()
        executor.boilermaker.task_handler.side_effect = ["result_a", "result_b", "result_c"]
        result = await executor.execute_workflow(workflow)
        assert result in ["result_a", "result_b", "result_c"]

    async def test_execute_node_task(self, executor):
        """Test executing a task node"""
        workflow = Workflow("test_workflow")
        task = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task)
        executor.boilermaker.task_handler.return_value = "task_result"

        # Capture the task that was actually executed
        executed_task = None
        original_task_handler = executor.boilermaker.task_handler

        async def capture_task_handler(task, sequence_number):
            nonlocal executed_task
            executed_task = task
            return await original_task_handler(task, sequence_number)

        executor.boilermaker.task_handler = capture_task_handler

        result = await executor._execute_node(workflow, "A")
        assert result == "task_result"
        assert executed_task is not None
        assert "workflow_context" in executed_task.payload

    async def test_execute_node_unsupported_type(self, executor):
        """Test executing unsupported node type"""
        workflow = Workflow("test_workflow")
        workflow.add_node("A", WorkflowNodeType.CHAIN)  # Not TASK

        with pytest.raises(ValueError, match="Unsupported node type"):
            await executor._execute_node(workflow, "A")

    async def test_execute_node_with_parent_results(self, executor):
        """Test executing node with parent results"""
        workflow = Workflow("test_workflow")
        parent_task = Task(
            function_name="parent",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        child_task = Task(
            function_name="child",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("parent", WorkflowNodeType.TASK, parent_task)
        workflow.add_node("child", WorkflowNodeType.TASK, child_task)
        workflow.add_dependency("parent", "child")
        workflow.mark_completed("parent", "parent_result")
        executor.boilermaker.task_handler.return_value = "child_result"

        # Capture the task that was actually executed
        executed_task = None
        original_task_handler = executor.boilermaker.task_handler

        async def capture_task_handler(task, sequence_number):
            nonlocal executed_task
            executed_task = task
            return await original_task_handler(task, sequence_number)

        executor.boilermaker.task_handler = capture_task_handler

        result = await executor._execute_node(workflow, "child")
        assert result == "child_result"
        assert executed_task is not None
        assert executed_task.payload["workflow_context"]["parent_results"]["parent"] == "parent_result"

    async def test_execute_chain(self, executor):
        """Test executing a chain of tasks"""
        task1 = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task2 = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task3 = Task(
            function_name="C",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        executor.boilermaker.task_handler.side_effect = ["result1", "result2", "result3"]
        result = await executor.execute_chain([task1, task2, task3])
        assert result == "result3"
        assert executor.boilermaker.task_handler.call_count == 3

    async def test_execute_chord(self, executor):
        """Test executing a chord pattern"""
        header_tasks = [
            Task(
                function_name="A",
                attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
                policy=RetryPolicy.default(),
                payload={},
                diagnostic_id=None,
            ),
            Task(
                function_name="B",
                attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
                policy=RetryPolicy.default(),
                payload={},
                diagnostic_id=None,
            ),
        ]
        callback_task = Task(
            function_name="cb",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        executor.boilermaker.task_handler.side_effect = ["header1", "header2", "callback_result"]
        result = await executor.execute_chord(header_tasks, callback_task)
        assert result == "callback_result"
        assert executor.boilermaker.task_handler.call_count == 3

    async def test_execute_group(self, executor):
        """Test executing a group of parallel tasks"""
        tasks = [
            Task(
                function_name="A",
                attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
                policy=RetryPolicy.default(),
                payload={},
                diagnostic_id=None,
            ),
            Task(
                function_name="B",
                attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
                policy=RetryPolicy.default(),
                payload={},
                diagnostic_id=None,
            ),
            Task(
                function_name="C",
                attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
                policy=RetryPolicy.default(),
                payload={},
                diagnostic_id=None,
            ),
        ]
        executor.boilermaker.task_handler.side_effect = ["result1", "result2", "result3"]
        results = await executor.execute_group(tasks)
        assert len(results) == 3
        assert "result1" in results
        assert "result2" in results
        assert "result3" in results
        assert executor.boilermaker.task_handler.call_count == 3

    def test_get_parent_results(self, executor):
        """Test getting parent results"""
        workflow = Workflow("test_workflow")
        workflow.add_node("parent1", WorkflowNodeType.TASK)
        workflow.add_node("parent2", WorkflowNodeType.TASK)
        workflow.add_node("child", WorkflowNodeType.TASK)

        workflow.add_dependency("parent1", "child")
        workflow.add_dependency("parent2", "child")

        workflow.mark_completed("parent1", "result1")
        workflow.mark_completed("parent2", "result2")

        parent_results = executor._get_parent_results(workflow, "child")

        assert parent_results["parent1"] == "result1"
        assert parent_results["parent2"] == "result2"


class TestWorkflowTaskHandler:
    """Test WorkflowTaskHandler class"""

    @pytest.fixture
    def mock_executor(self):
        """Create a mock WorkflowExecutor"""
        return Mock()

    @pytest.fixture
    def handler(self, mock_executor):
        """Create a WorkflowTaskHandler instance"""
        return WorkflowTaskHandler(mock_executor)

    async def test_handle_workflow_start(self, handler):
        """Test handling workflow start"""
        workflow_data = {"workflow_id": "test_workflow"}
        state = Mock()

        result = await handler.handle_workflow_start(state, workflow_data)

        assert result["status"] == "started"
        assert result["workflow_id"] == "test_workflow"

    async def test_handle_workflow_complete(self, handler):
        """Test handling workflow completion"""
        workflow_data = {
            "workflow_id": "test_workflow",
            "result": "final_result"
        }
        state = Mock()

        result = await handler.handle_workflow_complete(state, workflow_data)

        assert result == "final_result"


class TestWorkflowExecutionIntegration:
    """Integration tests for workflow execution"""

    @pytest.fixture
    def mock_boilermaker(self):
        """Create a mock Boilermaker with realistic behavior"""
        mock = Mock()
        mock.task_handler = AsyncMock()
        return mock

    @pytest.fixture
    def executor(self, mock_boilermaker):
        """Create a WorkflowExecutor instance"""
        return WorkflowExecutor(mock_boilermaker)

    async def test_complex_workflow_execution(self, executor):
        """Test execution of a complex workflow with multiple dependencies"""
        workflow = Workflow("complex_workflow")
        task_a = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_b = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_c = Task(
            function_name="C",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_d = Task(
            function_name="D",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task_a)
        workflow.add_node("B", WorkflowNodeType.TASK, task_b)
        workflow.add_node("C", WorkflowNodeType.TASK, task_c)
        workflow.add_node("D", WorkflowNodeType.TASK, task_d)
        workflow.add_dependency("A", "B")
        workflow.add_dependency("A", "C")
        workflow.add_dependency("B", "D")
        workflow.add_dependency("C", "D")
        workflow.execution_order = workflow._calculate_execution_order()
        executor.boilermaker.task_handler.side_effect = ["result_a", "result_b", "result_c", "result_d"]
        result = await executor.execute_workflow(workflow)
        assert result == "result_d"
        assert executor.boilermaker.task_handler.call_count == 4

        # Verify execution order
        call_args = [call[0][0] for call in executor.boilermaker.task_handler.call_args_list]
        assert call_args[0].payload["workflow_context"]["node_id"] == "A"
        assert call_args[1].payload["workflow_context"]["node_id"] in ["B", "C"]
        assert call_args[2].payload["workflow_context"]["node_id"] in ["B", "C"]
        assert call_args[3].payload["workflow_context"]["node_id"] == "D"

    async def test_workflow_with_failures(self, executor):
        """Test workflow execution with task failures"""
        workflow = Workflow("failure_workflow")
        task_a = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_b = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_c = Task(
            function_name="C",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task_a)
        workflow.add_node("B", WorkflowNodeType.TASK, task_b)
        workflow.add_node("C", WorkflowNodeType.TASK, task_c)
        workflow.add_dependency("A", "B")
        workflow.add_dependency("B", "C")
        workflow.execution_order = workflow._calculate_execution_order()
        executor.boilermaker.task_handler.side_effect = ["result_a", Exception("Task B failed"), "result_c"]
        with pytest.raises(Exception, match="Task B failed"):
            await executor.execute_workflow(workflow)
        assert "A" in workflow.completed_nodes
        assert "B" in workflow.failed_nodes
        assert "C" not in workflow.completed_nodes

    async def test_workflow_context_preservation(self, executor):
        """Test that workflow context is properly preserved and passed"""
        workflow = Workflow("context_workflow")
        task_a = Task(
            function_name="A",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        task_b = Task(
            function_name="B",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )
        workflow.add_node("A", WorkflowNodeType.TASK, task_a)
        workflow.add_node("B", WorkflowNodeType.TASK, task_b)
        workflow.add_dependency("A", "B")
        workflow.execution_order = workflow._calculate_execution_order()
        executor.boilermaker.task_handler.side_effect = ["result_a", "result_b"]

        await executor.execute_workflow(workflow)

        # Check the tasks that were actually executed
        call_args = [call[0][0] for call in executor.boilermaker.task_handler.call_args_list]
        executed_task_a = call_args[0]
        executed_task_b = call_args[1]

        assert "workflow_context" in executed_task_a.payload
        assert "workflow_context" in executed_task_b.payload
        assert executed_task_a.payload["workflow_context"]["workflow_id"] == "context_workflow"
        assert executed_task_a.payload["workflow_context"]["node_id"] == "A"
        assert executed_task_b.payload["workflow_context"]["workflow_id"] == "context_workflow"
        assert executed_task_b.payload["workflow_context"]["node_id"] == "B"
        assert "parent_results" in executed_task_b.payload["workflow_context"]
