import typing

import uuid_utils as uuid
from pydantic import Field

TaskId = typing.NewType("TaskId", str)
GraphId = typing.NewType("GraphId", str)


NullTaskId = TaskId("00000000-0000-0000-0000-000000000000")


def truncate_task_id(task_id: TaskId, limit: int = 12) -> str:
    full = str(task_id)
    trunc = full[-limit:] if len(full) > limit else full
    return f"...{trunc}"


def new_task_id() -> TaskId:
    return TaskId(str(uuid.uuid7()))


def ident_field():
    return Field(default_factory=new_task_id)
