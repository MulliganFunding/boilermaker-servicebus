import pytest
from boilermaker.sample import debug_task, debug_task_retry_policy, STATIC_DEBUG_TASK

DummyState = {}


@pytest.mark.asyncio
async def test_debug_task_returns_zero():
    """Test that debug_task returns 0."""
    result = await debug_task(DummyState)
    assert result == 0


def test_static_debug_task_payload():
    """Test STATIC_DEBUG_TASK payload is correct."""
    assert STATIC_DEBUG_TASK.payload == {"args": [], "kwargs": {}}


@pytest.mark.asyncio
async def test_debug_task_retry_policy_default():
    """Test debug_task_retry_policy raises RetryException when use_default is True."""
    with pytest.raises(Exception) as exc:
        await debug_task_retry_policy(DummyState, True)
    assert "RETRY TEST" in str(exc.value)


@pytest.mark.asyncio
async def test_debug_task_retry_policy_exponential():
    """Test debug_task_retry_policy raises RetryExceptionDefaultExponential when use_default is False."""
    with pytest.raises(Exception) as exc:
        await debug_task_retry_policy(
            DummyState, False, msg="EXPONENTIAL", max_tries=3, delay=10, delay_max=100
        )
    assert "EXPONENTIAL" in str(exc.value)
