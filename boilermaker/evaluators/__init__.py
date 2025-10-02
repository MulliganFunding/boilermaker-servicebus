import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker.storage.base import StorageInterface
from boilermaker.task import Task
from boilermaker.types import TaskHandler

from .common import MessageActions, MessageHandler
from .results_store import ResultsStorageTaskEvaluator
from .simple import NoStorageEvaluator
from .task_graph import TaskGraphEvaluator


def evaluator_factory(
    receiver: ServiceBusReceiver,
    task: Task,
    task_publisher: Callable[[Task], Awaitable[None]],
    function_registry: dict[str, TaskHandler],
    state: typing.Any | None = None,
    storage_interface: StorageInterface | None = None,
    delete_successful_graphs: bool = False,
) -> MessageHandler:
    if storage_interface and task.graph_id is not None:
        return TaskGraphEvaluator(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
            delete_successful_graphs=delete_successful_graphs,
        )
    elif storage_interface and task.graph_id is None:
        return ResultsStorageTaskEvaluator(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
        )

    return NoStorageEvaluator(
        receiver,
        task,
        task_publisher,
        function_registry,
        state=state,
    )


__all__ = [
    "evaluator_factory",
    "MessageActions",
    "MessageHandler",
    "NoStorageEvaluator",
    "ResultsStorageTaskEvaluator",
    "TaskGraphEvaluator",
]
