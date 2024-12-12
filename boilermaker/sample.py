import logging

from .retries import RetryExceptionDefaultExponential
from .task import Task

logger = logging.getLogger(__name__)
TASK_NAME = "debug_task"


async def debug_task(state):
    """
    This task can be used to establish that boilermaker is working properly
    """
    logger.debug("DEBUG TASK INVOKED")
    return 0


STATIC_DEBUG_TASK = Task.default(TASK_NAME, acks_late=False)
STATIC_DEBUG_TASK.payload = {"args": [], "kwargs": {}}


async def debug_task_retry_policy(state, max_tries=5, delay=30, delay_max=600):
    """
    This task does nothing
    """
    raise RetryExceptionDefaultExponential(
        "This task is for testing retries", max_tries=max_tries, delay=delay, delay_max=delay_max
    )
