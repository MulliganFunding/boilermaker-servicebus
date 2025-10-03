import logging
import typing

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker.storage.base import StorageInterface
from boilermaker.task import Task

from .common import (
    MessageActions,
    TaskEvaluatorBase,
    TaskHandler,
    TaskHandlerRegistry,
    TaskPublisher,
)
from .results_store import ResultsStorageTaskEvaluator
from .simple import NoStorageEvaluator
from .task_graph import TaskGraphEvaluator

logger = logging.getLogger("boilermaker.app")


def evaluator_factory(
    receiver: ServiceBusReceiver,
    task: Task,
    task_publisher: TaskPublisher,
    function_registry: dict[str, TaskHandler],
    state: typing.Any | None = None,
    storage_interface: StorageInterface | None = None,
) -> TaskEvaluatorBase:
    if storage_interface and task.graph_id is not None:
        return TaskGraphEvaluator(
            receiver,
            task,
            task_publisher,
            function_registry,
            state=state,
            storage_interface=storage_interface,
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
    "TaskEvaluatorBase",
    "NoStorageEvaluator",
    "ResultsStorageTaskEvaluator",
    "TaskGraphEvaluator",
    "TaskHandler",
    "TaskPublisher",
    "TaskHandlerRegistry",
]
