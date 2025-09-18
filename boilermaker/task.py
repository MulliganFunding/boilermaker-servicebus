import datetime
import typing

import uuid_utils as uuid
from pydantic import BaseModel

from . import retries


class Task(BaseModel):
    # Unique identifier for this task: UUID7 for timestamp ordered identifiers.
    # We include a default for users upgrading previous versions where this key is missing.
    task_id: str = ""
    # Whether we should dead-letter a failing message
    should_dead_letter: bool = True
    # At-most-once vs at-least-once (default)
    acks_late: bool = True
    # function name for this task
    function_name: str
    # Records how many attempts for this task (if previous)
    attempts: retries.RetryAttempts
    # For retries, we want a policy to govern how we retry this task
    policy: retries.RetryPolicy
    # Represents actual arguments: must be jsonable!
    payload: dict[str, typing.Any]
    # Eventhub event metadata below
    # opentelemetry parent trace id is included here
    diagnostic_id: str | None
    # Internal use: Service Bus sequence number once published
    _sequence_number: int | None = None

    # Callbacks for success and failure
    on_success: typing.Optional["Task"] = None
    on_failure: typing.Optional["Task"] = None

    @classmethod
    def default(cls, function_name: str, **kwargs):
        attempts = retries.RetryAttempts(
            attempts=0, last_retry=datetime.datetime.now(datetime.UTC)
        )
        policy = retries.RetryPolicy.default()
        if "policy" in kwargs:
            policy = kwargs.pop("policy")
        return cls(
            task_id=str(uuid.uuid7()),
            attempts=attempts,
            function_name=function_name,
            policy=policy,
            payload={},
            diagnostic_id=None,
            **kwargs,
        )

    @property
    def acks_early(self):
        return not self.acks_late

    @property
    def can_retry(self):
        return self.attempts.attempts <= self.policy.max_tries

    def get_next_delay(self):
        return self.policy.get_delay_interval(self.attempts.attempts)

    def record_attempt(self):
        now = datetime.datetime.now(datetime.UTC)
        return self.attempts.inc(now)

    def __rshift__(self, other: "Task") -> "Task":
        """
        Adds a failure callback (`other`) to this task with >> operator.

        Returns self to allow chaining `on_success` callback also.

        In other words:

        (task1 >> task2) >= task3 means:
        - if task1 fails, run task2
        - if task1 succeeds, run task3
        """
        self.on_failure = other
        return self

    def __lshift__(self, other: "Task") -> "Task":
        """
        Adds a failure callback (`other`) to this task with << operator

        Returns other to allow chaining a success callback also.
        """
        other.on_failure = self
        return other

    def __ge__(self, other: "Task") -> "Task":
        """
        Adds a success callback (`other`) to this task with >= operator

        Returns other to allow chaining additional tasks like this:
        task1 >= task2 >= task3
        """
        self.on_success = other
        return other

    def __le__(self, other: "Task") -> "Task":
        """
        Adds a success callback (`other`) to this task with <= operator
        Returns self to allow chaining additional tasks like this:
        task1 <= task2 <= task3
        """
        other.on_success = self
        return self
