from boilermaker import task


def test_can_retry():
    atask = task.Task.default("somefunc")
    assert atask.policy.max_tries > 1
    for _ in range(atask.policy.max_tries):
        atask.record_attempt()
        assert atask.can_retry

    # can no longer retry
    atask.record_attempt()
    assert not atask.can_retry
