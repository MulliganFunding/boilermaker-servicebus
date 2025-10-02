from unittest.mock import AsyncMock

import pytest
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus._common.constants import SEQUENCENUBMERNAME
from azure.servicebus._pyamqp.message import Message
from boilermaker.app import Boilermaker
from boilermaker.evaluators import (
    evaluator_factory,
    NoStorageEvaluator,
    ResultsStorageTaskEvaluator,
    TaskGraphEvaluator,
)
from boilermaker.task import Task


def make_message(task, sequence_number: int = 123):
    # Example taken from:
    # azure-sdk-for-python/blob/main/sdk/servicebus/azure-servicebus/tests/test_message.py#L233
    my_frame = [0, 0, 0]
    amqp_received_message = Message(
        data=[task.model_dump_json().encode("utf-8")],
        message_annotations={SEQUENCENUBMERNAME: sequence_number},
    )
    return ServiceBusReceivedMessage(amqp_received_message, receiver=None, frame=my_frame)


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]


@pytest.fixture
def app(sbus):
    async def somefunc(state, x):
        return x * 2

    app = Boilermaker(State({}), sbus)
    app.register_async(somefunc)
    return app


@pytest.mark.parametrize(
    "task",
    [
        Task.default("somefunc", payload={"args": [1]}),
        Task.default(
            "somefunc",
            payload={"args": [1]},
            graph_id="graph1",
            task_id="task1",
        ),
    ],
)
@pytest.mark.parametrize("has_storage", [True, False])
def test_evaluator_factory(task, has_storage, app, mockservicebus):
    evaluator = evaluator_factory(
        mockservicebus._receiver,
        task,
        app.publish_task,
        app.function_registry,
        state={},
        storage_interface=AsyncMock() if has_storage else None,
    )
    if task.graph_id:
        if has_storage:
            assert isinstance(evaluator, TaskGraphEvaluator)
        else:
            assert isinstance(evaluator, NoStorageEvaluator)
    elif has_storage:
        assert isinstance(evaluator, ResultsStorageTaskEvaluator)
    else:
        assert isinstance(evaluator, NoStorageEvaluator)
