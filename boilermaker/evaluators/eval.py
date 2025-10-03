import logging
import time
import traceback
import typing

from boilermaker.exc import BoilermakerUnregisteredFunction
from boilermaker.failure import TaskFailureResult
from boilermaker.retries import RetryException
from boilermaker.task import Task, TaskResult, TaskStatus

from .common import TaskHandler

logger = logging.getLogger("boilermaker.app")


async def eval_task(
    task: Task,
    function_registry: dict[str, TaskHandler],
    state: typing.Any | None = None,
) -> TaskResult:
    """
    Dynamically look up function requested and then evaluate it using the task payload state.

    Wrap results, retry, or exception in TaskResult.

    Note: no side-effects (unless you count logging)!

    This is a pure function of type `Task -> TaskResult`!

    In accordance with no-side-effects, we do not publish callbacks or retries here.
    (Those are handled by `message_handler` implementations which can call this function.)

    Note: if callers return the special TaskFailureResult, store result as a failure result
    with no actual result data.

    Returns:
        TaskResult instance
    Raises:
        BoilermakerUnregisteredFunction: If the function is not found in the registry.
    """
    start = time.monotonic()
    function = function_registry.get(task.function_name)

    # Look up function associated with the task
    logger.info(f"[{task.function_name}] Begin Task {task.sequence_number=}")

    if function is None:
        raise BoilermakerUnregisteredFunction(
            f"Function {task.function_name} not found in registry"
        )

    try:
        result = await function(
            state,
            *task.payload["args"],
            **task.payload["kwargs"],
        )
        # Check if result is TaskFailureResult (special failure case)
        if result is TaskFailureResult:
            # Store as failure result (no actual result data to store)
            task_result = TaskResult(
                task_id=task.task_id,
                graph_id=task.graph_id,
                result=None,  # No result data for failures
                status=TaskStatus.Failure,
                errors=["Task returned TaskFailureResult"],
            )
            logger.warning(
                f"[{task.function_name}] Task {task.sequence_number=} "
                f"returned TaskFailureResult in {time.monotonic() - start:.3f}s"
            )
        else:
            # Create success result
            task_result = TaskResult(
                task_id=task.task_id,
                graph_id=task.graph_id,
                result=result,
                status=TaskStatus.Success,
            )
            logger.info(
                f"[{task.function_name}] Completed Task {task.sequence_number=} "
                f"in {time.monotonic() - start:.3f}s"
            )
    except RetryException as retry:
        # A retry has been requested:
        # Calculate next delay and publish retry.
        # Do not run on_failure run until after retries exhausted!
        # The `retry` RetryException may have a policy -> What if it's different from the Task?
        if retry.policy and retry.policy != task.policy:
            # This will publish the *next* instance of the task using *this* policy
            task.policy = retry.policy
            logger.warning(f"Task policy updated to retry policy {retry.policy}")

        task_result = TaskResult(
            task_id=task.task_id,
            graph_id=task.graph_id,
            status=TaskStatus.Retry,
            errors=[retry.msg],
            formatted_exception=traceback.format_exc(),
        )
    except Exception as exc:
        # Some other exception has been thrown
        err_msg = f"Exception in task sequence_number={task.sequence_number} {traceback.format_exc()}"
        logger.error(err_msg)
        task_result = TaskResult(
            task_id=task.task_id,
            graph_id=task.graph_id,
            status=TaskStatus.Failure,
            errors=[str(exc)],
            formatted_exception=traceback.format_exc(),
        )

    return task_result
