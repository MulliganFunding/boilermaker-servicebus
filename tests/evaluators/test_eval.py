import pytest
from boilermaker.evaluators import eval
from boilermaker.exc import BoilermakerUnregisteredFunction
from boilermaker.failure import TaskFailureResultType
from boilermaker.retries import RetryExceptionDefault, RetryExceptionDefaultExponential
from boilermaker.task import Task, TaskStatus


async def somefunc(state, x):
    return x * 2


async def failfunc(state, x) -> TaskFailureResultType:
    return TaskFailureResultType()


async def raisefunc(state, x):
    raise RuntimeError("Intentional error for testing")


async def raiseretry(state, x):
    raise RetryExceptionDefault("Intentional retry for testing")


async def raiseretry_custom_policy(state, x):
    raise RetryExceptionDefaultExponential(
        "Intentional retry for testing", max_tries=99, delay_max=1000
    )


REGISTRY = {
    "somefunc": somefunc,
    "failfunc": failfunc,
    "raisefunc": raisefunc,
    "raiseretry": raiseretry,
    "raiseretry_custom_policy": raiseretry_custom_policy,
}


@pytest.fixture
def task(make_message):
    t1 = Task.default("somefunc", payload={"args": [21], "kwargs": {}})
    t1.msg = make_message(t1)
    return t1


@pytest.fixture
def fail_task(make_message):
    t1 = Task.default("failfunc", payload={"args": [21], "kwargs": {}})
    t1.msg = make_message(t1)
    return t1


@pytest.fixture
def raise_task(make_message):
    t1 = Task.default("raisefunc", payload={"args": [21], "kwargs": {}})
    t1.msg = make_message(t1)
    return t1


@pytest.fixture
def retry_task(make_message):
    t1 = Task.default("raiseretry", payload={"args": [21], "kwargs": {}})
    t1.msg = make_message(t1)
    return t1


@pytest.fixture
def retry_custom_policy_task(make_message):
    t1 = Task.default("raiseretry_custom_policy", payload={"args": [21], "kwargs": {}})
    t1.msg = make_message(t1)
    return t1


async def test_task_handler_success(task):
    """Test that task_handler executes a registered function and returns the result."""

    result = await eval.eval_task(task, REGISTRY)
    assert result.status == TaskStatus.Success
    assert result.result == 42
    assert result.task_id == task.task_id
    assert result.graph_id is None


async def test_task_handler_unregistered_function(task):
    """Test that task_handler raises an error for unregistered functions."""
    with pytest.raises(BoilermakerUnregisteredFunction):
        await eval.eval_task(task, {})  # Empty registry


async def test_task_handler_failure(fail_task):
    """Test that task_handler handles a function that returns TaskFailureResult."""
    result = await eval.eval_task(fail_task, REGISTRY)
    assert result.status == TaskStatus.Failure
    assert result.result is None
    assert "Task returned TaskFailureResult" in result.errors
    assert result.task_id == fail_task.task_id
    assert result.graph_id is None


async def test_task_handler_exception(raise_task):
    """Test that task_handler handles exceptions raised by the function."""

    result = await eval.eval_task(raise_task, REGISTRY)
    assert result.status == TaskStatus.Failure
    assert result.result is None
    assert any("Intentional error for testing" in err for err in result.errors)
    assert result.task_id == raise_task.task_id
    assert result.graph_id is None


async def test_task_handler_retry(retry_task):
    """Test that task_handler handles RetryException raised by the function."""

    result = await eval.eval_task(retry_task, REGISTRY)
    assert result.status == TaskStatus.Retry
    assert result.result is None
    assert any("Intentional retry for testing" in err for err in result.errors)
    assert result.task_id == retry_task.task_id
    assert result.graph_id is None


async def test_task_handler_retry_custom_policy(retry_custom_policy_task):
    """Test that task_handler handles RetryException with custom policy raised by the function."""

    result = await eval.eval_task(retry_custom_policy_task, REGISTRY)
    assert result.status == TaskStatus.Retry
    assert result.result is None
    assert any("Intentional retry for testing" in err for err in result.errors)
    assert result.task_id == retry_custom_policy_task.task_id
    assert result.graph_id is None
    assert retry_custom_policy_task.policy.max_tries == 99

