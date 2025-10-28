from unittest.mock import Mock

from boilermaker import retries, task


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


def test_task_default_creation():
    t = task.Task.default("test_func")
    assert t.function_name == "test_func"
    assert t.should_dead_letter is True
    assert t.acks_late is True
    assert t.payload == {}
    assert t.attempts.attempts == 0
    assert t.policy.max_tries > 1
    assert t.task_id is not None
    assert t.graph_id is None
    assert t.on_success is None
    assert t.on_failure is None


def test_task_si_with_custom_policy():
    custom_policy = retries.RetryPolicy(max_tries=10, delay=30)
    t = task.Task.si(sample_task, 1, 2, arg3="test", policy=custom_policy)
    assert t.function_name == "sample_task"
    assert t.payload == {"args": (1, 2), "kwargs": {"arg3": "test"}}
    assert t.policy.max_tries == 10
    assert t.policy.delay == 30


def test_task_si_with_flags():
    t = task.Task.si(sample_task, should_dead_letter=False, acks_late=False)
    assert t.should_dead_letter is False
    assert t.acks_late is False
    assert t.acks_early is True


def test_task_hash():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func1")

    # Hash should be based on task_id
    assert hash(t1) == hash(t1.task_id)
    assert hash(t1) != hash(t2)
    assert hash(t1) != hash(t3)  # Different task_ids even with same function


def test_task_properties():
    t = task.Task.default("test_func")

    # Test acks_early property
    assert t.acks_early is False  # Default acks_late is True
    t.acks_late = False
    assert t.acks_early is True


def test_task_message_properties():


    t = task.Task.default("test_func")

    # Initially no message
    assert t.msg is None
    assert t.sequence_number is None
    assert t.diagnostic_id is None

    # Set a mock message using the setter
    mock_msg = Mock()
    mock_msg.sequence_number = 12345
    t.msg = mock_msg  # Use the setter which sets _msg

    assert t.msg is mock_msg
    # Clear the cached property to make it re-evaluate
    if hasattr(t, "__dict__") and "sequence_number" in t.__dict__:
        del t.__dict__["sequence_number"]
    assert t.sequence_number == 12345


def test_task_record_attempt():
    t = task.Task.default("test_func")
    initial_attempts = t.attempts.attempts

    result = t.record_attempt()

    assert t.attempts.attempts == initial_attempts + 1
    assert result == initial_attempts + 1  # inc() returns the new attempt count


def test_task_chaining_operations():
    t1 = task.Task.default("func1")
    t2 = task.Task.default("func2")
    t3 = task.Task.default("func3")

    # Test right-shift chaining (>>)
    result = t1 >> t2
    assert t1.on_success is t2
    assert result is t2

    # Test chaining multiple tasks
    t1 >> t2 >> t3
    assert t1.on_success is t2
    assert t2.on_success is t3

    # Test left-shift chaining (<<)
    t4 = task.Task.default("func4")
    t5 = task.Task.default("func5")
    result = t4 << t5
    assert t5.on_success is t4
    assert result is t4


def test_task_serialization():
    t = task.Task.default("test_func")
    t.payload = {"key": "value"}  # Set payload after creation

    # Should be serializable to dict
    task_dict = t.model_dump()
    assert task_dict["function_name"] == "test_func"
    assert task_dict["payload"] == {"key": "value"}

    # Should be deserializable from dict
    t2 = task.Task.model_validate(task_dict)
    assert t2.function_name == "test_func"
    assert t2.payload == {"key": "value"}
    assert t2.task_id == t.task_id



def test_task_diagnostic_id_when_no_message():
    """Test diagnostic_id property returns None when _msg is None (line 132)."""
    t = task.Task.default("test_func")
    # By default, _msg should be None
    assert t._msg is None
    assert t.diagnostic_id is None


def test_task_default_with_custom_policy_in_kwargs():
    """Test Task.default() method when policy is provided in kwargs - covers line 113"""
    custom_policy = retries.RetryPolicy(max_retries=5, initial_delay=2.0)
    atask = task.Task.default("test_function", policy=custom_policy)

    assert atask.policy == custom_policy
    assert atask.function_name == "test_function"
    # This test specifically covers the "if 'policy' in kwargs:" branch at line 113


def test_task_diagnostic_id_when_no_message_set():
    """Test diagnostic_id property when no message is set - covers line 132"""
    atask = task.Task.si(sample_task)
    # Task created without a message should return None for diagnostic_id
    assert atask.diagnostic_id is None


def test_task_si_uses_default_retry_attempts():
    """Test that Task.si() uses RetryAttempts.default() - covers line 111"""
    atask = task.Task.si(sample_task)

    # Should use default retry attempts (line 111 in si method)
    assert isinstance(atask.attempts, retries.RetryAttempts)
    assert atask.attempts.attempts == 0  # Default should start with 0 attempts
