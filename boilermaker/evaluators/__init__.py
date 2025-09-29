import typing
from collections.abc import Awaitable, Callable

from azure.servicebus.aio import ServiceBusReceiver

from boilermaker.storage.base import StorageInterface
from boilermaker.task import Task
from boilermaker.types import TaskHandler

from .basic import BaseTaskEvaluator
from .results_store import ResultsStorageTaskEvaluator
from .task_graphs import TaskGraphEvaluator


def evaluator_factory(
    receiver: ServiceBusReceiver,
    task_publisher: Callable[[Task], Awaitable[None]],
    function_registry: dict[str, TaskHandler],
    state: typing.Any | None = None,
    storage_interface: StorageInterface | None = None,
    graphs_enabled: bool = False
) -> BaseTaskEvaluator:

    if storage_interface and graphs_enabled:
        return TaskGraphEvaluator(
            receiver,
            task_publisher,
            function_registry,
            state,
            storage_interface
        )
    elif storage_interface and not graphs_enabled:
        return ResultsStorageTaskEvaluator(
            receiver,
            task_publisher,
            function_registry,
            state,
            storage_interface
        )

    return BaseTaskEvaluator(
        receiver,
        task_publisher,
        function_registry,
        state,
    )
