import typing

import uuid_utils as uuid
from pydantic import Field

TaskId = typing.NewType("TaskId", str)
GraphId = typing.NewType("GraphId", str)


def truncate_task_id(task_id: TaskId, limit: int = 12) -> str:
    full = str(task_id)
    trunc = full[-limit:] if len(full) > limit else full
    return f"...{trunc}"


def ident_field():
    return Field(default_factory=lambda: TaskId(str(uuid.uuid7())))
