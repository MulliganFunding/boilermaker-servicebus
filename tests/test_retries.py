import pytest

from boilermaker import retries


ATTEMPTS = tuple(range(10))


@pytest.fixture()
def default():
    return retries.RetryPolicy.default()


@pytest.fixture()
def linear():
    return retries.RetryPolicy(
        max_tries=5, delay=30, delay_max=600, retry_mode=retries.RetryMode.Linear
    )


@pytest.fixture()
def exponential():
    return retries.RetryPolicy(
        max_tries=5, delay=30, delay_max=600, retry_mode=retries.RetryMode.Exponential
    )


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_fixed(attempts, default):
    assert default.get_delay_interval(attempts) == default.delay


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_linear(attempts, linear):
    assert linear.get_delay_interval(attempts) == linear.delay * attempts


@pytest.mark.parametrize("attempts", ATTEMPTS)
def test_policy_exponential(attempts, exponential):
    assert exponential.get_delay_interval(attempts) <= exponential.delay_max


def test_isomorphism(default, linear, exponential):
    for policy in [default, linear, exponential]:
        assert retries.RetryPolicy.parse_raw(policy.json()) == policy
