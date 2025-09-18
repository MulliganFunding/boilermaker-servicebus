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


def test_get_next_delay():
    atask = task.Task.default("somefunc")
    first_delay = atask.get_next_delay()
    atask.record_attempt()
    second_delay = atask.get_next_delay()

    assert first_delay <= atask.policy.delay_max
    assert second_delay <= atask.policy.delay_max
    assert second_delay >= first_delay


def test_rightwise_bitshift_on_failure():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    result = t1 >> t2
    assert t1.on_success is t2
    assert result is t2


def test_leftwise_bitshift_on_failure():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    result = t1 << t2
    assert t2.on_success is t1
    assert result is t1


async def sample_task(state, number1, number2: int = 4):
    if hasattr(state, "sample_task_called"):
        state.sample_task_called += number1
    return number1 + number2


def test_task_signature():
    t = task.Task.si(sample_task, 3, number2=5)
    assert t.function_name == "sample_task"
    assert t.payload == {"args": (3,), "kwargs": {"number2": 5}}
