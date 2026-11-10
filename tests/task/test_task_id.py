import time
import uuid

from boilermaker.task.task_id import new_task_id


def test_new_task_id_is_uuid7():
    before_ms = int(time.time() * 1000)
    task_id = new_task_id()
    after_ms = int(time.time() * 1000)

    parsed = uuid.UUID(task_id)
    assert parsed.version == 7
    assert before_ms <= parsed.time <= after_ms


def test_new_task_id_is_time_ordered():
    first = new_task_id()
    second = new_task_id()
    assert first < second
