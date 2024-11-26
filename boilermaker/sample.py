import logging

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
