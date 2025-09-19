import datetime
import enum
import random

from pydantic import BaseModel, field_validator

DEFAULT_MAX_TRIES = 5
MAX_REASONABLE_RETRIES = 100
MAX_BACKOFF_SECONDS = 3600  # an hour


@enum.unique
class RetryMode(enum.IntEnum):
    """Enumeration of retry backoff strategies.

    Defines how delay intervals are calculated between retry attempts.
    Each mode provides different backoff behavior suitable for various
    failure scenarios and system load characteristics.

    Attributes:
        Fixed: Constant delay between retries (e.g., 30s, 30s, 30s...)
        Exponential: Exponentially increasing delay with jitter to prevent thundering herd
        Linear: Linearly increasing delay (e.g., 30s, 60s, 90s...)
    """
    # Every schedule-interval is the same, e.g., "sleep for 30s and retry loop"
    Fixed = 0
    # Schedule-interval exponentially increases (with jitter) up to max
    Exponential = 1
    # Schedule-interval linearly increases
    Linear = 2


# Default RetryPolicy is try up to 5 times waiting 60s each time
class RetryPolicy(BaseModel):
    """Configuration for task retry behavior and backoff strategies.

    Defines how many times a task should be retried and with what
    delay pattern when it fails. Supports multiple backoff modes
    with configurable limits.

    Attributes:
        max_tries: Maximum number of execution attempts (default: 5)
        delay: Base delay in seconds between retries (default: 0)
        delay_max: Maximum delay cap in seconds (default: 3600)
        retry_mode: Strategy for calculating delays (default: Fixed)

    Example:
        >>> # Fixed delay of 60 seconds, max 3 attempts
        >>> policy = RetryPolicy(max_tries=3, delay=60, retry_mode=RetryMode.Fixed)
        >>>
        >>> # Exponential backoff starting at 30s, max 10 minutes
        >>> policy = RetryPolicy(
        ...     max_tries=5,
        ...     delay=30,
        ...     delay_max=600,
        ...     retry_mode=RetryMode.Exponential
        ... )
    """
    # Capped at this value
    max_tries: int = DEFAULT_MAX_TRIES
    # Seconds
    delay: int = 0
    # Max seconds
    delay_max: int = MAX_BACKOFF_SECONDS
    # Mode for retrying
    retry_mode: RetryMode = RetryMode.Fixed

    def __str__(self):
        return (
            f"RetryPolicy(max_tries={self.max_tries}, delay={self.delay}, "
            f"delay_max={self.delay_max}, retry_mode={self.retry_mode})"
        )

    def __eq__(self, other):
        return (
            self.max_tries == other.max_tries
            and self.delay == other.delay
            and self.delay_max == other.delay_max
            and self.retry_mode == other.retry_mode
        )

    @field_validator("delay")
    def delay_is_positive(cls, v):
        if v < 0:
            raise ValueError("delay must be 0 or positive")
        return v

    @field_validator("delay_max")
    def delay_max_is_positive(cls, v):
        if v < 0:
            raise ValueError("delay_max must be 0 or positive")
        return v

    @field_validator("max_tries")
    def max_tries_is_reasonable(cls, v):
        if v < 0 or v > MAX_REASONABLE_RETRIES:
            raise ValueError(f"number not in range of 0 < n < {MAX_REASONABLE_RETRIES}")
        return v

    @classmethod
    def default(cls):
        # Default is try up to 5 times waiting 60s each time
        return cls(max_tries=5, delay=120, delay_max=120, retry_mode=RetryMode.Fixed)

    @classmethod
    def no_retry(cls):
        # max_tries=1 should disable retries. Duplicates NoRetry class below
        return cls(max_tries=1, delay=5, delay_max=5, retry_mode=RetryMode.Fixed)

    def get_delay_interval(self, attempts_so_far: int) -> int:
        """Calculate delay in seconds before the next retry attempt.

        Uses the configured retry mode to determine appropriate delay:
        - Fixed: Returns constant delay value (capped at delay_max)
        - Linear: Returns delay * attempts_so_far (capped at delay_max)
        - Exponential: Returns 2^attempts * delay with jitter (capped at delay_max)

        Exponential mode includes random jitter to prevent thundering herd
        effects when many tasks retry simultaneously.

        Args:
            attempts_so_far: Number of previous attempts (0-based)

        Returns:
            int: Delay in seconds before next retry

        Example:
            >>> policy = RetryPolicy(delay=30, retry_mode=RetryMode.Exponential)
            >>> policy.get_delay_interval(0)  # First retry: ~15-30s
            >>> policy.get_delay_interval(1)  # Second retry: ~30-60s
            >>> policy.get_delay_interval(2)  # Third retry: ~60-120s
        """
        match self.retry_mode:
            case RetryMode.Fixed:
                return min(self.delay, self.delay_max)
            case RetryMode.Linear:
                return min(self.delay * attempts_so_far, self.delay_max)
            case RetryMode.Exponential:
                # Jitter is added so that a lot of work doesn't get bunched up at the
                # end and eventually hurt throughput.
                exponential_delay = min(
                    (2**attempts_so_far) * self.delay, self.delay_max
                )
                return (exponential_delay // 2) + random.randint(
                    0, exponential_delay // 2
                )


# NoRetry only tries one time
class NoRetry(RetryPolicy):
    """Retry policy that disables retries (single execution attempt only).

    Convenience class that creates a RetryPolicy with max_tries=1,
    effectively disabling retry behavior. Tasks using this policy
    will be marked as failed after the first execution attempt.

    Example:
        >>> no_retry_task = Task.si(risky_function, policy=NoRetry())
        >>> # Task will not retry on failure
    """
    def __init__(self):
        """Duplicates RetryPolicy.no_retry() constructor"""
        super().__init__(max_tries=1, delay=5, delay_max=5, retry_mode=RetryMode.Fixed)


class RetryAttempts(BaseModel):
    """Tracks retry attempt count and timing for a task.

    Maintains state about how many times a task has been attempted
    and when the last attempt occurred. Used by the retry system
    to determine if more retries are allowed.

    Attributes:
        attempts: Number of execution attempts so far (0-based)
        last_retry: Timestamp of the most recent retry attempt

    Example:
        >>> attempts = RetryAttempts.default()  # Start with 0 attempts
        >>> attempts.inc(datetime.now())        # Record first attempt
        >>> print(f"Attempts: {attempts.attempts}")  # Prints: Attempts: 1
    """
    # how many so far
    attempts: int = 0
    # Timestamp of last retry
    last_retry: datetime.datetime

    @classmethod
    def default(cls):
        now = datetime.datetime.now(datetime.UTC)
        return cls(attempts=0, last_retry=now)

    @field_validator("attempts")
    def attempts_is_positive(cls, v):
        if v < 0:
            raise ValueError("attempts must be positive")
        return v

    def inc(self, when: datetime.datetime):
        """Increment attempt count and update last retry timestamp.

        Args:
            when: Timestamp of the attempt

        Returns:
            int: Updated attempt count
        """
        self.attempts += 1
        self.last_retry = when
        return self.attempts


class RetryException(Exception):
    """Base exception class that can specify a custom retry policy.

    When raised by a task function, this exception allows the task
    to override its default retry policy for this specific failure.

    Attributes:
        msg: Error message describing the failure
        policy: Custom retry policy to use (None for task default)

    Example:
        >>> def unreliable_task():
        ...     if should_retry_aggressively():
        ...         aggressive_policy = RetryPolicy(max_tries=10, delay=5)
        ...         raise RetryException("Temporary failure", aggressive_policy)
        ...     else:
        ...         raise RetryException("Use default retry policy")
    """
    def __init__(self, msg: str, policy: RetryPolicy | None = None):
        self.msg = msg
        self.policy = policy


class RetryExceptionDefault(RetryException):
    """Retry exception that uses the default retry policy.

    Convenience class for raising retriable exceptions with standard
    retry behavior (5 attempts, 120s fixed delay).

    Example:
        >>> def task_with_retries():
        ...     if network_unavailable():
        ...         raise RetryExceptionDefault("Network timeout")
    """
    def __init__(self, msg: str):
        super().__init__(msg, policy=RetryPolicy.default())


class RetryExceptionDefaultExponential(RetryException):
    """Retry exception with exponential backoff default policy.

    Uses exponential backoff with jitter (30s base, 600s max, 5 attempts).
    Suitable for failures that may benefit from backing off more aggressively
    over time, such as rate limiting or temporary resource exhaustion.

    Args:
        msg: Error message
        **kwargs: Override default policy parameters

    Example:
        >>> def rate_limited_task():
        ...     if rate_limit_exceeded():
        ...         raise RetryExceptionDefaultExponential(
        ...             "Rate limited", max_tries=3, delay=10
        ...         )
    """
    def __init__(self, msg: str, **kwargs):
        defaults = {
            "max_tries": 5,
            "delay": 30,
            "delay_max": 600,
            "retry_mode": RetryMode.Exponential,
        }
        defaults.update(kwargs)
        super().__init__(msg, policy=RetryPolicy.model_validate(defaults))


class RetryExceptionDefaultLinear(RetryException):
    """Retry exception with linear backoff default policy.

    Uses linear backoff (30s base, 600s max, 5 attempts) where delay
    increases linearly with each attempt. Suitable for failures where
    a steady increase in wait time is appropriate.

    Args:
        msg: Error message
        **kwargs: Override default policy parameters

    Example:
        >>> def database_task():
        ...     if database_busy():
        ...         raise RetryExceptionDefaultLinear(
        ...             "Database busy", delay=60, delay_max=300
        ...         )
    """
    def __init__(self, msg: str, **kwargs):
        defaults = {
            "max_tries": 5,
            "delay": 30,
            "delay_max": 600,
            "retry_mode": RetryMode.Linear,
        }
        defaults.update(kwargs)
        super().__init__(msg, policy=RetryPolicy.model_validate(defaults))
