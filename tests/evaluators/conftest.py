"""
Evaluator test fixtures.

This module provides a dedicated evaluator context for testing.

We found that for testing our TaskGraphEvaluator, we needed a rich context
that encapsulated the application, mock service bus, mock storage, and
various tasks and graphs.

We also needed helper methods to set up common scenarios (success, failure,
retry, exception) and to assert expected behaviors (task started stored,
task result stored, message settled, graph loaded, etc).

This module encapsulates all that context into a reusable fixture,
`EvaluatorTestContext`, which can be used in multiple test cases.
"""
from collections import namedtuple
import random
from contextlib import asynccontextmanager
from typing import Any


import pytest
from boilermaker import retries
from boilermaker.evaluators import TaskGraphEvaluator
from boilermaker.failure import TaskFailureResult
from boilermaker.task import Task, TaskGraph, TaskResult, TaskStatus


# Simple test functions - these replace the module-level functions
async def simple_ok_task(state):
    return "OK"


async def simple_retry_task(state):
    raise retries.RetryException("Retry me")


async def simple_positive_task(state, x):
    """Task that behaves differently based on input."""
    if x < 0:
        raise ValueError("x must be non-negative")
    if x == 0:
        return TaskFailureResult
    if x == 100:
        raise retries.RetryException("Retry for 100")
    return x * 2


async def success_callback(state):
    return "Success handled"


async def failure_callback(state):
    return "Failure handled"


ScheduledMessage = namedtuple("ScheduledMessage", ["task", "args", "kwargs"])


class EvaluatorTestContext:
    """
    Encapsulates all the context needed for evaluator tests.
    Provides methods to set up scenarios, invoke the evaluator,
    and assert expected behaviors.
    """

    def __init__(self, app, mockservicebus, mock_storage, make_message):
        self.app = app
        self.mockservicebus = mockservicebus
        self.mock_storage = mock_storage
        self.make_message = make_message

        self._published_messages = []

        async def mock_task_publisher(task: Task, *args, **kwargs):
            self._published_messages.append(ScheduledMessage(task, args, kwargs))
            return

        self.mock_task_publisher = mock_task_publisher

        # Register test functions
        self.app.register_many_async(
            [
                simple_ok_task,
                simple_positive_task,
                simple_retry_task,
                success_callback,
                failure_callback,
            ]
        )

        # Create clean tasks for each test
        self._reset_tasks()

        self._graph = self.create_simple_graph()
        self.create_evaluator()
        self._evaluation_result: TaskResult | None = None

    # Pre-execution methods: graph and evaluator setup
    async def __call__(self) -> TaskResult:
        """Invoke the evaluator's message handler."""
        return await self.evaluator.message_handler()

    def _reset_tasks(self) -> None:
        """Reset all tasks to clean state."""
        self.ok_task = Task.si(simple_ok_task)
        self.positive_task = Task.si(simple_positive_task, 21)
        self.retry_task = Task.si(simple_retry_task)
        self.failure_callback_task = Task.si(failure_callback)
        self.success_callback_task = Task.si(success_callback)

    def create_evaluator(self, task=None) -> "EvaluatorTestContext":
        """Create a TaskGraphEvaluator with the given task."""
        if task is None:
            task = self.ok_task

        # Ensure task has a message
        if not hasattr(task, "msg") or task.msg is None:
            task.msg = self.make_message(task)

        self._evaluator = TaskGraphEvaluator(
            self.mockservicebus._receiver,
            task,
            self.mock_task_publisher,
            self.app.function_registry,
            state=self.app.state,
            storage_interface=self.mock_storage,
        )
        return self

    @property
    def evaluator(self) -> TaskGraphEvaluator:
        """Get the current evaluator."""
        return self.get_evaluator()

    def get_evaluator(self) -> TaskGraphEvaluator:
        """Get the current evaluator."""
        if self._evaluator is None:
            self.create_evaluator()
        return self._evaluator

    def create_simple_graph(self) -> TaskGraph:
        """Create a simple test graph."""
        self._graph = TaskGraph()

        # Reset tasks to ensure clean state
        self._reset_tasks()

        # Add root
        self._graph.add_task(self.ok_task)  # Root
        # Basic success callback
        self._graph.add_task(self.success_callback_task, parent_ids=[self.ok_task.task_id])
        # Basic failure callback
        self._graph.add_failure_callback(self.ok_task.task_id, self.failure_callback_task)

        # positive_task allows dynamic behavior based on input
        # it is dependent on ok_task and also has a failure callback and a success callback
        self._graph.add_task(self.positive_task, parent_ids=[self.ok_task.task_id])
        self._graph.add_task(self.success_callback_task, parent_ids=[self.positive_task.task_id])
        self._graph.add_failure_callback(self.positive_task.task_id, self.failure_callback_task)

        # Generate pending results
        list(self._graph.generate_pending_results())

        return self._graph

    @property
    def graph(self) -> TaskGraph:
        """Get the current graph."""
        if self._graph is None:
            raise ValueError("Graph has not been created yet.")
        return self._graph

    @graph.setter
    def graph(self, value: TaskGraph) -> None:
        """Set the current graph."""
        if not isinstance(value, TaskGraph):
            raise TypeError("value must be a TaskGraph instance.")

        # Must ensure that the mock storage returns this graph
        self._graph = value
        self.mock_storage.load_graph.return_value = self._graph

    @property
    def current_task(self) -> Task:
        """Get the current task from the evaluator."""
        return self.evaluator.task

    def get_task(self, task_id: str) -> Task | None:
        """Get a task from the graph by ID."""
        return self.graph.children.get(task_id) or self.graph.fail_children.get(task_id)

    def set_task(self, task: Task, *args, **kwargs) -> "EvaluatorTestContext":
        """Set the task for the evaluator."""
        task.msg = self.make_message(task, sequence_number=random.randint(1, 1000))
        task.payload["args"] = args
        task.payload["kwargs"] = kwargs
        self.evaluator.task = task
        return self

    def prep_task_to_raise(self) -> "EvaluatorTestContext":
        """Configure positive_task to raise an exception."""
        self.set_task(self.positive_task, -5)

        self._graph.add_result(TaskResult(task_id=self.ok_task.task_id, status=TaskStatus.Success, output="OK"))
        # Because load_graph is called *once* at the end, we have to preseed the result from calling the failing task
        self._graph.add_result(TaskResult(task_id=self.positive_task.task_id, status=TaskStatus.Failure))
        # Set the failure_callback to pending
        self._graph.add_result(
            TaskResult(task_id=self.failure_callback_task.task_id, status=TaskStatus.Pending)
        )

        self.mock_storage.load_graph.return_value = self._graph
        return self

    def prep_task_to_fail(self) -> "EvaluatorTestContext":
        """Configure positive_task to return failure."""
        self.set_task(self.positive_task, 0)

        # Manipulate the graph to reflect one completed and one failure
        self._graph.add_result(TaskResult(task_id=self.ok_task.task_id, status=TaskStatus.Success, output="OK"))
        # Because load_graph is called *once* at the end, we have to preseed the result from calling the failing task
        self._graph.add_result(TaskResult(task_id=self.positive_task.task_id, status=TaskStatus.Failure))

        # Set the failure_callback to pending
        self._graph.add_result(
            TaskResult(task_id=self.failure_callback_task.task_id, status=TaskStatus.Pending)
        )

        self.mock_storage.load_graph.return_value = self._graph

        return self

    def prep_task_to_retry(self) -> "EvaluatorTestContext":
        """Configure positive_task to retry."""
        self.set_task(self.positive_task, 100)

        # Manipulate the graph to reflect one completed and one retry
        self._graph.add_result(TaskResult(task_id=self.ok_task.task_id, status=TaskStatus.Success, output="OK"))
        # Because load_graph is called *once* at the end, we have to preseed the result from calling the failing task
        self._graph.add_result(TaskResult(task_id=self.positive_task.task_id, status=TaskStatus.Retry))
        self.mock_storage.load_graph.return_value = self._graph

        return self

    def prep_task_to_exhaust_retries(self) -> "EvaluatorTestContext":
        """Configure positive_task to exhaust retries."""
        self.positive_task.attempts.attempts = self.positive_task.policy.max_tries + 1
        self.set_task(self.positive_task, 100)

        # Manipulate the graph to reflect one completed and exhausted retries
        self._graph.add_result(TaskResult(task_id=self.ok_task.task_id, status=TaskStatus.Success, output="OK"))
        # Because load_graph is called *once* at the end, we have to preseed the result from calling the failing task
        self._graph.add_result(TaskResult(task_id=self.positive_task.task_id, status=TaskStatus.RetriesExhausted))
        # Set the failure_callback to pending
        self._graph.add_result(
            TaskResult(task_id=self.failure_callback_task.task_id, status=TaskStatus.Pending)
        )
        self.mock_storage.load_graph.return_value = self._graph

        return self

    # # Post-Execution method: assertions and other helpers
    def assert_storage_task_started(self, task: Task) -> TaskResult:
        """Assert that a task started result was stored."""
        calls = self.mock_storage.store_task_result.mock_calls
        started_call = calls[0]

        msg = f"Expected a TaskResult to be stored but got {started_call.args[0]}."
        assert isinstance(started_call.args[0], TaskResult), msg

        started_result = started_call.args[0]
        msg = f"Expected task_id {task.task_id}, got {started_result.task_id}"
        assert started_result.task_id == task.task_id, msg

        msg = f"Expected graph_id {task.graph_id}, got {started_result.graph_id}"
        assert started_result.graph_id == task.graph_id, msg

        msg = f"Expected status 'started', got {started_result.status}"
        assert started_result.status == TaskStatus.Started, msg
        return started_result

    def assert_storage_task_result_stored(self, task: Task) -> TaskResult:
        """Assert that a task result was stored."""
        calls = self.mock_storage.store_task_result.mock_calls
        if len(calls) < 2:
            raise AssertionError("Expected at least two store_task_result calls.")

        result_call = calls[1]
        msg = f"Expected a TaskResult to be stored but got {result_call.args[0]}."
        assert isinstance(result_call.args[0], TaskResult), msg
        stored_result = result_call.args[0]

        msg = f"Expected task_id {task.task_id}, got {stored_result.task_id}"
        assert stored_result.task_id == task.task_id, msg

        msg = f"Expected graph_id {task.graph_id}, got {stored_result.graph_id}"
        assert stored_result.graph_id == task.graph_id, msg
        return stored_result

    def get_other_storage_calls(self) -> list:
        """Get any additional storage calls beyond started and result."""
        calls = self.mock_storage.store_task_result.mock_calls
        return calls[2:]

    def assert_load_graph_called(self):
        """Assert that load_graph was called."""
        assert self.mock_storage.load_graph.called, "Expected load_graph to be called."
        return self.mock_storage.load_graph.call_args

    def assert_graph_not_loaded(self):
        """Assert that load_graph was not called."""
        assert not self.mock_storage.load_graph.called, "Expected load_graph NOT to be called."

    def assert_msg_settled(self) -> None:
        """Assert that the message was settled."""
        assert len(self.mockservicebus._receiver.method_calls) == 1, "Expected one message settlement call."

    def assert_msg_dead_lettered(self) -> None:
        """Assert that the message was dead-lettered."""
        self.assert_msg_settled()
        complete_msg_call = self.mockservicebus._receiver.method_calls[0]
        assert complete_msg_call[0] == "dead_letter_message"

    def assert_messages_scheduled(self, expected_count: int) -> None:
        """Assert that the expected number of messages were scheduled."""
        actual_count = len(self._published_messages)
        msg = f"Expected {expected_count} messages to be scheduled, got {actual_count}."
        assert actual_count == expected_count, msg

    def get_scheduled_messages(self) -> list[tuple[Task, int]]:
        """Get the list of scheduled messages."""
        return self._published_messages

    @asynccontextmanager
    async def with_regular_assertions(
        self,
        compare_result: Any | None = None,
        compare_status: TaskStatus | None = None,
        check_graph_loaded: bool = True,
    ):
        """Enable regular assertions on the mock storage."""
        self.evaluator.task.msg = self.make_message(self.evaluator.task)
        result = await self()
        self._evaluation_result = result

        yield self

        if compare_result is not None:
            assert result.result == compare_result, f"Expected result {compare_result}, got {result.result}"
        if compare_status is not None:
            assert result.status == compare_status, f"Expected status {compare_status}, got {result.status}"

        # Started stored
        self.assert_storage_task_started(self.evaluator.task)

        # Result stored: should match above
        stored_result = self.assert_storage_task_result_stored(self.evaluator.task)
        if compare_result is not None:
            assert stored_result.result == compare_result, (
                f"Expected result {compare_result}, got {stored_result.result}"
            )
        if compare_status is not None:
            assert stored_result.status == compare_status, (
                f"Expected status {compare_status}, got {stored_result.status}"
            )

        # Message settled
        self.assert_msg_settled()
        # Graph loaded check
        if check_graph_loaded:
            # Graph loaded
            self.assert_load_graph_called()

        self._evaluation_result = None


@pytest.fixture
def evaluator_context(app, mockservicebus, mock_storage, make_message):
    """Provide a clean evaluator test context."""
    return EvaluatorTestContext(app, mockservicebus, mock_storage, make_message)


@pytest.fixture
def failure_scenario(evaluator_context: EvaluatorTestContext) -> EvaluatorTestContext:
    """Scenario where task fails."""
    evaluator_context.prep_task_to_fail()
    return evaluator_context


@pytest.fixture
def retry_scenario(evaluator_context: EvaluatorTestContext) -> EvaluatorTestContext:
    """Scenario where task retries."""
    return evaluator_context.prep_task_to_retry()


@pytest.fixture
def retries_exhausted_scenario(evaluator_context: EvaluatorTestContext) -> EvaluatorTestContext:
    """Scenario where task retries are exhausted."""
    evaluator_context.prep_task_to_exhaust_retries()
    return evaluator_context


@pytest.fixture
def exception_scenario(evaluator_context: EvaluatorTestContext) -> EvaluatorTestContext:
    """Scenario where task raises an exception."""
    return evaluator_context.prep_task_to_raise()
