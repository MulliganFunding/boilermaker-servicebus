import datetime
import typing

import uuid_utils as uuid
from pydantic import BaseModel, ConfigDict, Field

from . import retries

DEFAULT_RETRY_ATTEMPTS = 1


class Task(BaseModel):
    """Represents a serializable task with retry policies and callback chains.

    A Task encapsulates a function call with its arguments, retry configuration,
    and optional success/failure callbacks. Tasks are JSON-serializable and can
    be published to Azure Service Bus for asynchronous execution.

    Attributes:
        task_id: Unique UUID7 identifier for timestamp-ordered task identification
        should_dead_letter: Whether failed tasks should be dead-lettered (default: True)
        acks_late: Whether to acknowledge messages after processing (default: True)
        function_name: Name of the registered function to execute
        attempts: Retry attempt tracking and metadata
        policy: Retry policy governing backoff and max attempts
        payload: Function arguments and keyword arguments (must be JSON-serializable)
        diagnostic_id: OpenTelemetry parent trace ID for distributed tracing
        _sequence_number: Service Bus sequence number (set after publishing)
        on_success: Optional callback task to run on successful completion
        on_failure: Optional callback task to run on failure

    Example:
        >>> # Create task with default settings
        >>> task = Task.default("my_function", args=[1, 2], kwargs={"key": "value"})
        >>>
        >>> # Create task with custom retry policy
        >>> policy = RetryPolicy(max_tries=5, backoff_mode="exponential")
        >>> task = Task.si(my_function, arg1, kwarg=value, policy=policy)
        >>>
        >>> # Chain tasks with callbacks
        >>> task1 >> task2 >> task3  # Success chain
        >>> task1.on_failure = error_handler_task
    """
    # Unique identifier for this task: UUID7 for timestamp ordered identifiers.
    # We include a default for users upgrading previous versions where this key is missing.
    task_id: str = Field(default_factory=lambda: str(uuid.uuid7()))
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

    # Required for the uuid.UUID type annotation
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def default(cls, function_name: str, **kwargs):
        """Create a Task with default retry settings.

        Convenience method to create a task with sensible defaults for
        retry attempts and policy. Additional keyword arguments override
        default values.

        Args:
            function_name: Name of the function to execute
            **kwargs: Additional task attributes to override defaults

        Returns:
            Task: New task instance with default retry configuration

        Example:
            >>> task = Task.default("process_data", payload={"data": [1, 2, 3]})
        """
        attempts = retries.RetryAttempts.default()
        policy = retries.RetryPolicy.default()
        if "policy" in kwargs:
            policy = kwargs.pop("policy")
        return cls(
            attempts=attempts,
            function_name=function_name,
            policy=policy,
            payload={},
            diagnostic_id=None,
            **kwargs,
        )

    @property
    def acks_early(self):
        """Whether the task acknowledges messages before processing.

        Returns:
            bool: True if acks_late is False, meaning messages are
                  acknowledged immediately upon receipt
        """
        return not self.acks_late

    @property
    def can_retry(self):
        """Whether the task can be retried based on current attempts.

        Returns:
            bool: True if current attempts are less than or equal to
                  the maximum tries allowed by the retry policy
        """
        return self.attempts.attempts <= self.policy.max_tries

    def get_next_delay(self):
        """Calculate the delay before the next retry attempt.

        Uses the task's retry policy to determine the appropriate
        delay based on the current number of attempts.

        Returns:
            int: Delay in seconds before next retry attempt
        """
        return self.policy.get_delay_interval(self.attempts.attempts)

    def record_attempt(self):
        """Record a new execution attempt with current timestamp.

        Increments the attempt counter and records the current UTC
        timestamp for tracking retry intervals and debugging.

        Returns:
            RetryAttempts: Updated attempts object with incremented count
        """
        now = datetime.datetime.now(datetime.UTC)
        return self.attempts.inc(now)

    def __rshift__(self, other: "Task") -> "Task":
        """Set success callback using >> operator (right-shift chaining).

        Creates a success callback chain where the right-hand task
        executes if this task completes successfully.

        Args:
            other: Task to execute on success

        Returns:
            Task: The other task, allowing for continued chaining

        Example:
            >>> task1 >> task2 >> task3
            # If task1 succeeds, run task2
            # If task2 succeeds, run task3
        """
        self.on_success = other
        return other

    def __lshift__(self, other: "Task") -> "Task":
        """Set success callback using << operator (left-shift chaining).

        Creates a success callback chain where this task executes
        if the right-hand task completes successfully.

        Args:
            other: Task that will trigger this task on success

        Returns:
            Task: This task, allowing for continued chaining

        Example:
            >>> task1 << task2 << task3
            # If task3 succeeds, run task2
            # If task2 succeeds, run task1
        """
        other.on_success = self
        return self

    @classmethod
    def si(
        cls,
        fn: typing.Callable,
        *fn_args,
        should_dead_letter: bool = True,
        acks_late: bool = True,
        policy: retries.RetryPolicy | None = None,
        **fn_kwargs,
    ) -> "Task":
        """Create an immutable signature task from a function and arguments.

        Creates a task bound to specific function arguments, useful for
        preparing tasks with callbacks or custom settings before publishing.
        The function arguments are captured at creation time.

        Note: This implementation creates immutable signatures only.
        Future versions may support mutable signatures where task outputs
        are passed to subsequent tasks in a chain.

        Args:
            fn: The function to be executed
            *fn_args: Positional arguments for the function
            should_dead_letter: Whether to dead-letter failed tasks (default: True)
            acks_late: Whether to acknowledge after processing (default: True)
            policy: Custom retry policy (uses default if None)
            **fn_kwargs: Keyword arguments for the function

        Returns:
            Task: New task with bound function signature

        Example:
            >>> def process_data(data, format="json"):
            ...     return f"Processed {data} as {format}"
            >>>
            >>> task = Task.si(process_data, [1, 2, 3], format="xml")
            >>> # Arguments are bound to the task
        """
        attempts = retries.RetryAttempts.default()
        policy = policy or retries.RetryPolicy.default()

        return cls(
            should_dead_letter=should_dead_letter,
            acks_late=acks_late,
            attempts=attempts,
            function_name=fn.__name__,
            policy=policy,
            payload={
                "args": fn_args,
                "kwargs": fn_kwargs,
            },
            diagnostic_id=None,
        )
