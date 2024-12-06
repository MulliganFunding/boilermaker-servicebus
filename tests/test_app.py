import pytest

from boilermaker.app import Boilermaker
from boilermaker import retries


class State:
    def __init__(self, inner):
        self.inner = inner

    def __getitem__(self, key):
        return self.inner[key]

DEFAULT_STATE = State({"somekey": "somevalue"})




@pytest.fixture
def app(sbus):
    return Boilermaker(DEFAULT_STATE, sbus)



def test_app_state(app):
    assert app.state == DEFAULT_STATE


async def test_task_decorator(app):
    @app.task()
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__

    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_task_decorator_with_policy(app):
    @app.task(policy=retries.RetryPolicy.default())
    async def somefunc(state):
        return state["somekey"]

    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert app.task_registry[somefunc.__name__].policy == retries.RetryPolicy.default()
    assert await somefunc(DEFAULT_STATE) == "somevalue"


async def test_app_register_async(app):
    async def somefunc(state):
        return state["somekey"]

    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    assert somefunc.__name__ in app.task_registry
    assert app.task_registry[somefunc.__name__].function_name == somefunc.__name__
    assert await somefunc(DEFAULT_STATE) == "somevalue"

async def test_create_task(app):
    async def somefunc(state, **kwargs):
        state.inner.update(kwargs)
        return state["somekey"]

    # Function must be registered  first
    app.register_async(somefunc, policy=retries.RetryPolicy.default())
    # Now we can create a task out of it
    task = app.create_task(somefunc, somekwarg="akwargval")
    assert task.function_name == "somefunc"
    assert task.payload == {"args": (), "kwargs": {"somekwarg": "akwargval"}}
    assert task.attempts.attempts == 0
    assert task.acks_late
    assert task.acks_early is False
    assert task.can_retry
    assert task.get_next_delay() == retries.RetryPolicy.default().delay
    assert task.record_attempt()
    assert task.attempts.attempts == 1
