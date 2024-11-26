import datetime
import enum
import random

from pydantic import BaseModel, field_validator

MAX_REASONABLE_RETRIES = 30
MAX_BACKOFF_SECONDS = 3600  # an hour


@enum.unique
class RetryMode(enum.IntEnum):
    # Every schedule-interval is the same, e.g., "sleep for 30s and retry loop"
    Fixed = 0
    # Schedule-interval exponentially increases (with jitter) up to max
    Exponential = 1
    # Schedule-interval linearly increases
    Linear = 2


# Default RetryPolicy is try up to 5 times waiting 60s each time
class RetryPolicy(BaseModel):
    # Capped at this value
    max_tries: int = MAX_REASONABLE_RETRIES
    # Seconds
    delay: int = 0
    # Max seconds
    delay_max: int = MAX_BACKOFF_SECONDS
    # Mode for retrying
    retry_mode: RetryMode = RetryMode.Fixed

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

    def get_delay_interval(self, attempts_so_far: int) -> int:
        """Figure out how many seconds of delay to wait before next attempt"""
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
    def __init__(self):
        super().__init__(max_tries=1, delay=5, delay_max=5, retry_mode=RetryMode.Fixed)


class RetryAttempts(BaseModel):
    # how many so far
    attempts: int = 0
    # Timestamp of last retry
    last_retry: datetime.datetime

    @field_validator("attempts")
    def attempts_is_positive(cls, v):
        if v < 0:
            raise ValueError("attempts must be positive")
        return v

    def inc(self, when: datetime.datetime):
        self.attempts += 1
        self.last_retry = when
        return self.attempts


class RetryException(Exception):
    def __init__(self, msg: str, policy: RetryPolicy):
        self.msg = msg
        self.policy = policy


class RetryExceptionDefault(RetryException):
    def __init__(self, msg: str):
        super().__init__(msg, RetryPolicy.default())


class RetryExceptionDefaultExponential(RetryException):
    def __init__(self, msg: str):
        policy = RetryPolicy(
            max_tries=5, delay=30, delay_max=600, retry_mode=RetryMode.Exponential
        )
        super().__init__(msg, policy)


class RetryExceptionDefaultLinear(RetryException):
    def __init__(self, msg: str):
        policy = RetryPolicy(
            max_tries=5, delay=30, delay_max=600, retry_mode=RetryMode.Linear
        )
        super().__init__(msg, policy)
