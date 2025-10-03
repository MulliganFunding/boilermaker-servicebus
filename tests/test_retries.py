import datetime

import pytest
from boilermaker import retries

ATTEMPTS = tuple(range(10))


@pytest.fixture()
def default():
    return retries.RetryPolicy.default()


@pytest.fixture()
def linear():
    return retries.RetryPolicy(max_tries=5, delay=30, delay_max=600, retry_mode=retries.RetryMode.Linear)


@pytest.fixture()
def exponential():
    return retries.RetryPolicy(max_tries=5, delay=30, delay_max=600, retry_mode=retries.RetryMode.Exponential)


@pytest.mark.parametrize("invalid_value", (-1, None))
def test_invalid_init(invalid_value):
    with pytest.raises(ValueError):
        retries.RetryPolicy(max_tries=invalid_value)
    with pytest.raises(ValueError):
        retries.RetryPolicy(delay=invalid_value)
    with pytest.raises(ValueError):
        retries.RetryPolicy(delay_max=invalid_value)


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_fixed(attempts, default):
    assert default.get_delay_interval(attempts) == default.delay


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_linear(attempts, linear):
    assert linear.get_delay_interval(attempts) == linear.delay * attempts


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_no_retry(attempts):
    policy = retries.RetryPolicy.no_retry()
    assert policy.get_delay_interval(attempts) == policy.delay
    assert policy.max_tries == 1


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_exponential(attempts, exponential):
    assert exponential.get_delay_interval(attempts) <= exponential.delay_max


def test_isomorphism(default, linear, exponential):
    for policy in [default, linear, exponential]:
        assert retries.RetryPolicy.model_validate_json(policy.model_dump_json()) == policy


def test_attempts():
    now = datetime.datetime.now(tz=datetime.UTC)

    with pytest.raises(ValueError):
        retries.RetryAttempts(attempts=-1, last_retry=now)

    attempts = retries.RetryAttempts(attempts=1, last_retry=now)
    later = datetime.datetime.now(tz=datetime.UTC)
    attempts.inc(when=later)
    attempts.inc(when=later)
    assert attempts.attempts == 3
    assert attempts.last_retry == later


def test_retry_exception_default():
    """Test RetryExceptionDefault uses default retry policy."""
    exception = retries.RetryExceptionDefault("test message")

    # Should use default retry policy
    assert exception.policy == retries.RetryPolicy.default()
    assert str(exception) == "test message"


def test_retry_exception_default_linear():
    """Test RetryExceptionDefaultLinear uses linear defaults and accepts kwargs."""
    # Test with default values
    exception = retries.RetryExceptionDefaultLinear("test message")

    expected_policy = retries.RetryPolicy(
        max_tries=5,
        delay=30,
        delay_max=600,
        retry_mode=retries.RetryMode.Linear,
    )

    assert exception.policy == expected_policy
    assert str(exception) == "test message"

    # Test with overridden values
    custom_exception = retries.RetryExceptionDefaultLinear("custom message", max_tries=3, delay=60)

    expected_custom_policy = retries.RetryPolicy(
        max_tries=3,
        delay=60,
        delay_max=600,  # default not overridden
        retry_mode=retries.RetryMode.Linear,
    )

    assert custom_exception.policy == expected_custom_policy
    assert str(custom_exception) == "custom message"
