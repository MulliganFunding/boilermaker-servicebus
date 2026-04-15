"""Tests for boilermaker.cli.invoke — run_invoke() handler."""

from io import StringIO
from unittest import mock

import pytest
from boilermaker.cli._globals import EXIT_ERROR, EXIT_HEALTHY
from boilermaker.cli.invoke import run_invoke
from boilermaker.task import Task, TaskGraph, TaskResultSlim, TaskStatus
from rich.console import Console

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_graph_with_tasks() -> tuple[TaskGraph, Task, Task]:
    """Build a graph with two children: A -> B."""
    graph = TaskGraph()
    task_a = Task.default("prepare_data")
    task_b = Task.default("process_data")
    graph.add_task(task_a)
    graph.add_task(task_b, parent_ids=[task_a.task_id])
    return graph, task_a, task_b


def _set_result(graph: TaskGraph, task: Task, status: TaskStatus) -> None:
    """Set a result on a graph for the given task and status."""
    result = TaskResultSlim(
        task_id=task.task_id,
        graph_id=graph.graph_id,
        status=status,
    )
    graph.results[task.task_id] = result


def _mock_storage(graph: TaskGraph | None = None) -> mock.AsyncMock:
    """Return a mock storage whose load_graph returns the given graph."""
    storage = mock.AsyncMock()
    storage.load_graph = mock.AsyncMock(return_value=graph)
    return storage


def _no_color_console() -> Console:
    """Return a Rich Console that writes to a StringIO buffer with no color."""
    return Console(file=StringIO(), no_color=True, width=120)


def _mock_service_bus() -> mock.AsyncMock:
    """Return a mock AzureServiceBus that records send_message calls."""
    sb = mock.AsyncMock()
    sb.send_message = mock.AsyncMock()
    sb.close = mock.AsyncMock()
    return sb


# ---------------------------------------------------------------------------
# TestRunInvokeGraphNotFound
# ---------------------------------------------------------------------------


class TestRunInvokeGraphNotFound:
    @pytest.mark.asyncio
    async def test_returns_exit_error_when_graph_not_found(self):
        storage = _mock_storage(None)
        exit_code = await run_invoke(
            storage, "nonexistent-graph", "some-task-id",
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_error_message_includes_graph_id(self, capsys):
        storage = _mock_storage(None)
        await run_invoke(
            storage, "missing-graph-abc", "some-task-id",
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        captured = capsys.readouterr()
        assert "missing-graph-abc" in captured.err


# ---------------------------------------------------------------------------
# TestRunInvokeTaskNotFound
# ---------------------------------------------------------------------------


class TestRunInvokeTaskNotFound:
    @pytest.mark.asyncio
    async def test_returns_exit_error_when_task_not_in_graph(self):
        graph, _, _ = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        exit_code = await run_invoke(
            storage, str(graph.graph_id), "nonexistent-task-id",
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_error_message_includes_task_id(self, capsys):
        graph, _, _ = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        await run_invoke(
            storage, str(graph.graph_id), "bad-task-id",
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        captured = capsys.readouterr()
        assert "bad-task-id" in captured.err

    @pytest.mark.asyncio
    async def test_error_message_lists_available_tasks(self, capsys):
        graph, task_a, task_b = _make_graph_with_tasks()
        storage = _mock_storage(graph)
        await run_invoke(
            storage, str(graph.graph_id), "bad-task-id",
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        captured = capsys.readouterr()
        # Available tasks should be listed
        assert str(task_a.task_id) in captured.err or str(task_b.task_id) in captured.err


# ---------------------------------------------------------------------------
# TestRunInvokeTerminalStateGuard
# ---------------------------------------------------------------------------


class TestRunInvokeTerminalStateGuard:
    @pytest.mark.asyncio
    async def test_returns_exit_error_for_success_task_without_force(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        exit_code = await run_invoke(
            storage, str(graph.graph_id), str(task_a.task_id),
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_returns_exit_error_for_failure_task_without_force(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        storage = _mock_storage(graph)
        exit_code = await run_invoke(
            storage, str(graph.graph_id), str(task_a.task_id),
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_returns_exit_error_for_retries_exhausted_without_force(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.RetriesExhausted)
        storage = _mock_storage(graph)
        exit_code = await run_invoke(
            storage, str(graph.graph_id), str(task_a.task_id),
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        assert exit_code == EXIT_ERROR

    @pytest.mark.asyncio
    async def test_error_message_mentions_terminal_state(self, capsys):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)
        await run_invoke(
            storage, str(graph.graph_id), str(task_a.task_id),
            sb_namespace_url="https://example.servicebus.windows.net",
            sb_queue_name="tasks",
            console=_no_color_console(),
        )
        captured = capsys.readouterr()
        assert "terminal" in captured.err.lower() or "force" in captured.err.lower()


# ---------------------------------------------------------------------------
# TestRunInvokeForceFlag
# ---------------------------------------------------------------------------


class TestRunInvokeForceFlag:
    @pytest.mark.asyncio
    async def test_force_allows_reinvoke_of_success_task(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                force=True,
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY
        sb.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_force_allows_reinvoke_of_failure_task(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Failure)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                force=True,
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY


# ---------------------------------------------------------------------------
# TestRunInvokePublish
# ---------------------------------------------------------------------------


class TestRunInvokePublish:
    @pytest.mark.asyncio
    async def test_publishes_task_with_invoke_message_id(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Pending)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY
        sb.send_message.assert_called_once()
        _, call_kwargs = sb.send_message.call_args
        msg_id = call_kwargs["unique_msg_id"]
        assert str(task_a.task_id) in msg_id
        assert ":invoke:" in msg_id

    @pytest.mark.asyncio
    async def test_publishes_task_for_non_terminal_state(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Started)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY
        sb.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_publishes_task_with_no_result_blob(self):
        graph, task_a, _ = _make_graph_with_tasks()
        # No result set — task has no blob
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY
        sb.send_message.assert_called_once()


# ---------------------------------------------------------------------------
# TestRunInvokeServiceBusLifecycle
# ---------------------------------------------------------------------------


class TestRunInvokeServiceBusLifecycle:
    @pytest.mark.asyncio
    async def test_service_bus_is_closed_on_success(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Pending)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        sb.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_bus_is_closed_when_send_raises(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Pending)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        sb.send_message.side_effect = RuntimeError("Service Bus unavailable")
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        assert exit_code == EXIT_ERROR
        sb.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_service_bus_not_created_when_graph_not_found(self):
        storage = _mock_storage(None)

        with mock.patch("boilermaker.cli.invoke.AzureServiceBus") as sb_cls:
            await run_invoke(
                storage, "missing-graph", "some-task-id",
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        sb_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_service_bus_not_created_when_task_not_found(self):
        graph, _, _ = _make_graph_with_tasks()
        storage = _mock_storage(graph)

        with mock.patch("boilermaker.cli.invoke.AzureServiceBus") as sb_cls:
            await run_invoke(
                storage, str(graph.graph_id), "nonexistent-task-id",
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        sb_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_service_bus_not_created_when_terminal_without_force(self):
        graph, task_a, _ = _make_graph_with_tasks()
        _set_result(graph, task_a, TaskStatus.Success)
        storage = _mock_storage(graph)

        with mock.patch("boilermaker.cli.invoke.AzureServiceBus") as sb_cls:
            await run_invoke(
                storage, str(graph.graph_id), str(task_a.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        sb_cls.assert_not_called()


# ---------------------------------------------------------------------------
# TestRunInvokeFailChild
# ---------------------------------------------------------------------------


class TestRunInvokeFailChild:
    @pytest.mark.asyncio
    async def test_can_invoke_fail_child_task(self):
        graph = TaskGraph()
        parent_task = Task.default("do_work")
        fail_task = Task.default("handle_failure")
        graph.add_task(parent_task)
        graph.add_failure_callback(parent_task.task_id, fail_task)
        _set_result(graph, parent_task, TaskStatus.Failure)
        _set_result(graph, fail_task, TaskStatus.Pending)
        storage = _mock_storage(graph)

        sb = _mock_service_bus()
        with mock.patch("boilermaker.cli.invoke.AzureServiceBus", return_value=sb):
            exit_code = await run_invoke(
                storage, str(graph.graph_id), str(fail_task.task_id),
                sb_namespace_url="https://example.servicebus.windows.net",
                sb_queue_name="tasks",
                console=_no_color_console(),
            )

        assert exit_code == EXIT_HEALTHY
        sb.send_message.assert_called_once()
