import typing

import uuid_utils as uuid
from pydantic import Field

TaskId = typing.NewType("TaskId", str)
GraphId = typing.NewType("GraphId", str)


def ident_field():
    return Field(default_factory=lambda: TaskId(str(uuid.uuid7())))
