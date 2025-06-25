"""
Unit tests for Boilermaker app workflow integration

Tests for workflow support methods added to the Boilermaker class.
"""
from unittest.mock import AsyncMock, Mock, patch

import pytest
from boilermaker.app import Boilermaker
from boilermaker.retries import RetryPolicy
from boilermaker.task import Task
from boilermaker.workflow import Chain, Chord, Group, Workflow, WorkflowBuilder
from boilermaker.workflow_executor import WorkflowExecutor


class TestBoilermakerWorkflowSupport:
    """Test workflow support methods in Boilermaker class"""

    @pytest.fixture
    def mock_service_bus(self):
        """Create a mock service bus client"""
        mock = Mock()
        mock.send_message = AsyncMock()
        mock.get_receiver = AsyncMock()
        return mock

    @pytest.fixture
    def app_state(self):
        """Create a mock app state"""
        return Mock()

    @pytest.fixture
    def boilermaker(self, app_state, mock_service_bus):
        """Create a Boilermaker instance with workflow support"""
        app = Boilermaker(app_state, service_bus_client=mock_service_bus)
        return app

    @pytest.fixture
    def mock_task(self):
        """Create a mock task"""
        task = Mock(spec=Task)
        task.function_name = "test_task"
        task.payload = {}
        return task

    def test_enable_workflows(self, boilermaker):
        """Test enabling workflow support"""
        executor = boilermaker.enable_workflows()

        assert isinstance(executor, WorkflowExecutor)
        assert boilermaker.workflow_executor is executor

        # Calling again should return the same executor
        executor2 = boilermaker.enable_workflows()
        assert executor2 is executor

    def test_chain_creation(self, boilermaker, mock_task):
        """Test creating a chain"""
        chain_obj = boilermaker.chain(mock_task, mock_task, mock_task)

        assert isinstance(chain_obj, Chain)
        assert len(chain_obj.tasks) == 3
        assert all(task == mock_task for task in chain_obj.tasks)

    def test_chord_creation(self, boilermaker, mock_task):
        """Test creating a chord"""
        header_tasks = [mock_task, mock_task]
        callback_task = mock_task

        chord_obj = boilermaker.chord(header_tasks, callback_task)

        assert isinstance(chord_obj, Chord)
        assert chord_obj.header_tasks == header_tasks
        assert chord_obj.callback_task == callback_task

    def test_group_creation(self, boilermaker, mock_task):
        """Test creating a group"""
        group_obj = boilermaker.group(mock_task, mock_task, mock_task)

        assert isinstance(group_obj, Group)
        assert len(group_obj.tasks) == 3
        assert all(task == mock_task for task in group_obj.tasks)

    def test_workflow_builder_creation(self, boilermaker):
        """Test creating a workflow builder"""
        builder = boilermaker.workflow()

        assert isinstance(builder, WorkflowBuilder)

    async def test_execute_chain(self, boilermaker, mock_task):
        """Test executing a chain"""
        # Mock the workflow executor
        mock_executor = Mock()
        mock_executor.execute_chain = AsyncMock(return_value="chain_result")
        boilermaker.workflow_executor = mock_executor

        result = await boilermaker.execute_chain(mock_task, mock_task, mock_task)

        assert result == "chain_result"
        mock_executor.execute_chain.assert_called_once_with([mock_task, mock_task, mock_task])

    async def test_execute_chain_auto_enable_workflows(self, boilermaker, mock_task):
        """Test that execute_chain automatically enables workflows"""
        assert boilermaker.workflow_executor is None

        # Create real Task objects instead of using Mock
        from boilermaker.task import Task
        from boilermaker.retries import RetryAttempts, RetryPolicy
        import datetime

        real_task = Task(
            function_name="test_task",
            attempts=RetryAttempts(attempts=0, last_retry=datetime.datetime.now(datetime.UTC)),
            policy=RetryPolicy.default(),
            payload={},
            diagnostic_id=None,
        )

        # Mock the task handler to avoid "Missing registered function" error
        boilermaker.task_handler = AsyncMock(return_value="task_result")

        # Patch WorkflowExecutor and ensure enable_workflows sets workflow_executor
        with patch('boilermaker.app.WorkflowExecutor') as mock_executor_class:
            mock_executor = Mock()
            mock_executor.execute_chain = AsyncMock(return_value="chain_result")
            mock_executor_class.return_value = mock_executor

            # Patch enable_workflows to set workflow_executor and return the mock
            def enable_workflows_patch():
                boilermaker.workflow_executor = mock_executor
                return mock_executor
            boilermaker.enable_workflows = enable_workflows_patch

            result = await boilermaker.execute_chain(real_task, real_task)

            assert result == "chain_result"
            assert boilermaker.workflow_executor is mock_executor

    async def test_execute_chord(self, boilermaker, mock_task):
        """Test executing a chord"""
        # Mock the workflow executor
        mock_executor = Mock()
        mock_executor.execute_chord = AsyncMock(return_value="chord_result")
        boilermaker.workflow_executor = mock_executor

        header_tasks = [mock_task, mock_task]
        callback_task = mock_task

        result = await boilermaker.execute_chord(header_tasks, callback_task)

        assert result == "chord_result"
        mock_executor.execute_chord.assert_called_once_with(header_tasks, callback_task)

    async def test_execute_group(self, boilermaker, mock_task):
        """Test executing a group"""
        # Mock the workflow executor
        mock_executor = Mock()
        mock_executor.execute_group = AsyncMock(return_value=["result1", "result2"])
        boilermaker.workflow_executor = mock_executor

        result = await boilermaker.execute_group(mock_task, mock_task)

        assert result == ["result1", "result2"]
        mock_executor.execute_group.assert_called_once_with([mock_task, mock_task])

    async def test_execute_workflow(self, boilermaker):
        """Test executing a workflow"""
        # Mock the workflow executor
        mock_executor = Mock()
        mock_executor.execute_workflow = AsyncMock(return_value="workflow_result")
        boilermaker.workflow_executor = mock_executor

        workflow = Mock(spec=Workflow)

        result = await boilermaker.execute_workflow(workflow)

        assert result == "workflow_result"
        mock_executor.execute_workflow.assert_called_once_with(workflow)


class TestBoilermakerWorkflowIntegration:
    """Integration tests for Boilermaker workflow functionality"""

    @pytest.fixture
    def mock_service_bus(self):
        """Create a mock service bus client"""
        mock = Mock()
        mock.send_message = AsyncMock()
        mock.get_receiver = AsyncMock()
        return mock

    @pytest.fixture
    def app_state(self):
        """Create a mock app state"""
        return Mock()

    @pytest.fixture
    def boilermaker(self, app_state, mock_service_bus):
        """Create a Boilermaker instance"""
        app = Boilermaker(app_state, service_bus_client=mock_service_bus)
        return app

    @pytest.fixture
    def mock_task_function(self):
        """Create a mock task function"""
        async def task_func(state, *args, **kwargs):
            return f"task_result_{args[0] if args else 'default'}"
        return task_func

    async def test_workflow_with_registered_tasks(self, boilermaker, mock_task_function):
        """Test workflow execution with registered tasks"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create tasks
        task1 = boilermaker.create_task(mock_task_function, "task1")
        task2 = boilermaker.create_task(mock_task_function, "task2")
        task3 = boilermaker.create_task(mock_task_function, "task3")

        # Mock the task handler to return expected results
        boilermaker.task_handler = AsyncMock()
        boilermaker.task_handler.side_effect = ["result1", "result2", "result3"]

        # Execute chain
        result = await boilermaker.execute_chain(task1, task2, task3)

        assert result == "result3"
        assert boilermaker.task_handler.call_count == 3

    async def test_complex_workflow_builder(self, boilermaker, mock_task_function):
        """Test building and executing a complex workflow"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create tasks
        task_a = boilermaker.create_task(mock_task_function, "A")
        task_b = boilermaker.create_task(mock_task_function, "B")
        task_c = boilermaker.create_task(mock_task_function, "C")
        task_d = boilermaker.create_task(mock_task_function, "D")

        # Mock the task handler
        boilermaker.task_handler = AsyncMock()
        boilermaker.task_handler.side_effect = ["result_a", "result_b", "result_c", "result_d"]

        # Build complex workflow: A -> B, A -> C, B -> D, C -> D
        wf = boilermaker.workflow()

        node_a = wf.task(task_a, "A")
        node_b = wf.task(task_b, "B")
        node_c = wf.task(task_c, "C")
        node_d = wf.task(task_d, "D")

        wf.depends_on(node_b, node_a)
        wf.depends_on(node_c, node_a)
        wf.depends_on(node_d, node_b, node_c)

        workflow = wf.build()

        # Execute workflow
        result = await boilermaker.execute_workflow(workflow)

        assert result == "result_d"
        assert boilermaker.task_handler.call_count == 4

    async def test_workflow_with_task_failures(self, boilermaker, mock_task_function):
        """Test workflow execution with task failures"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create tasks
        task_a = boilermaker.create_task(mock_task_function, "A")
        task_b = boilermaker.create_task(mock_task_function, "B")
        task_c = boilermaker.create_task(mock_task_function, "C")

        # Mock the task handler to fail on task B
        boilermaker.task_handler = AsyncMock()
        boilermaker.task_handler.side_effect = ["result_a", Exception("Task B failed"), "result_c"]

        # Execute chain
        with pytest.raises(Exception, match="Task B failed"):
            await boilermaker.execute_chain(task_a, task_b, task_c)

    async def test_workflow_context_in_tasks(self, boilermaker, mock_task_function):
        """Test that workflow context is properly passed to tasks"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create tasks
        task_a = boilermaker.create_task(mock_task_function, "A")
        task_b = boilermaker.create_task(mock_task_function, "B")

        # Mock the task handler to capture task payloads
        captured_tasks = []

        async def mock_task_handler(task, sequence_number):
            captured_tasks.append(task)
            return f"result_{task.payload.get('args', ['default'])[0]}"

        boilermaker.task_handler = mock_task_handler

        # Execute chain
        result = await boilermaker.execute_chain(task_a, task_b)

        assert result == "result_B"
        assert len(captured_tasks) == 2

        # Check that workflow context was added to both tasks
        for task in captured_tasks:
            assert "workflow_context" in task.payload
            assert "workflow_id" in task.payload["workflow_context"]
            assert "node_id" in task.payload["workflow_context"]

        # Check that second task has parent results
        second_task = captured_tasks[1]
        assert "parent_results" in second_task.payload["workflow_context"]


class TestBoilermakerWorkflowErrorHandling:
    """Test error handling in workflow execution"""

    @pytest.fixture
    def mock_service_bus(self):
        """Create a mock service bus client"""
        mock = Mock()
        mock.send_message = AsyncMock()
        mock.get_receiver = AsyncMock()
        return mock

    @pytest.fixture
    def app_state(self):
        """Create a mock app state"""
        return Mock()

    @pytest.fixture
    def boilermaker(self, app_state, mock_service_bus):
        """Create a Boilermaker instance"""
        app = Boilermaker(app_state, service_bus_client=mock_service_bus)
        return app

    @pytest.fixture
    def mock_task_function(self):
        async def task_func(state, *args, **kwargs):
            return f"task_result_{args[0] if args else 'default'}"
        return task_func

    async def test_workflow_executor_none_assertion(self, boilermaker, mock_task_function):
        """Test that workflow methods assert executor is not None"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create a task
        task = boilermaker.create_task(mock_task_function, "test")

        # Mock enable_workflows to return None (shouldn't happen in practice)
        with patch.object(boilermaker, 'enable_workflows', return_value=None):
            with pytest.raises(AssertionError):
                await boilermaker.execute_chain(task)

    async def test_workflow_with_invalid_dependencies(self, boilermaker, mock_task_function):
        """Test workflow with invalid dependency structure"""
        # Register a task
        boilermaker.register_async(mock_task_function, policy=RetryPolicy.default())

        # Create tasks
        task_a = boilermaker.create_task(mock_task_function, "A")
        task_b = boilermaker.create_task(mock_task_function, "B")

        # Build workflow with cycle (should fail)
        wf = boilermaker.workflow()

        node_a = wf.task(task_a, "A")
        node_b = wf.task(task_b, "B")

        wf.depends_on(node_b, node_a)
        wf.depends_on(node_a, node_b)  # This creates a cycle

        with pytest.raises(ValueError, match="Workflow contains cycles"):
            wf.build()
