
import logging
import time
import traceback
import typing
from json.decoder import JSONDecodeError

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusReceiver
from pydantic import ValidationError

from boilermaker import sample
from boilermaker.failure import TaskFailureResult, TaskFailureResultType
from boilermaker.retries import RetryException
from boilermaker.task import Task

logger = logging.getLogger(__name__)


class TaskGraphEvaluator:

    async def message_handler(
        self, msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ):
        """Individual message handler"""
        message_settled = False
        sequence_number = msg.sequence_number
        self._current_message = msg
        try:
            task = Task.model_validate_json(str(msg))
        except (JSONDecodeError, ValidationError):
            log_err_msg = f"Invalid task sequence_number={sequence_number} exc_info={traceback.format_exc()}"
            logger.error(log_err_msg)
            # This task is not parseable
            await self.complete_message(msg, receiver)
            return None

        # Immediately record an attempt
        task.record_attempt()

        # at-most once: "complete" msg even if it fails later
        if task.acks_early:
            await self.complete_message(msg, receiver)
            message_settled = True

        if not task.can_retry:
            logger.error(f"Retries exhausted for event {task.function_name}")
            if not message_settled:
                await self.deadletter_or_complete_task(
                    receiver, msg, "ProcessingError", task, detail="Retries exhausted"
                )

            # TODO: Storage task result as failure

            # This task is a failure because it did not succeed and retries are exhausted
            if task.on_failure is not None:
                await self.publish_task(task.on_failure)
            # Early return here: no more processing
            return None

        # Actually handle the task or TaskGraph here
        try:
            result = await self.task_handler(task, sequence_number)

            # TODO: modify behavior here for DAG invocations...
            # Check and handle failure first
            if result is TaskFailureResult:
                # Deadletter or complete the message
                if not message_settled:
                    await self.deadletter_or_complete_task(
                        receiver, msg, "TaskFailed", task
                    )
                    message_settled = True
                if task.on_failure is not None:
                    # Schedule on_failure task
                    await self.publish_task(task.on_failure)
            # Success case: publish the next task (if desired)
            elif task.on_success is not None:
                # Success case: publish the next task
                await self.publish_task(task.on_success)
        except RetryException as retry:
            # A retry has been requested:
            # Calculate next delay and publish retry.
            # Do not run on_failure run until after retries exhausted!
            # The `retry` RetryException may have a policy -> What if it's different from the Task?
            if retry.policy and retry.policy != task.policy:
                # This will publish the *next* instance of the task using *this* policy
                task.policy = retry.policy
                logger.warning(f"Task policy updated to retry policy {retry.policy}")

            delay = task.get_next_delay()
            warn_msg = (
                f"{retry.msg} "
                f"[attempt {task.attempts.attempts} of {task.policy.max_tries}] "
                f"Publishing retry... {sequence_number=} <function={task.function_name}> with {delay=} "
            )
            logger.warning(warn_msg)
            await self.publish_task(
                task,
                delay=delay,
            )
        except Exception as exc:
            # Some other exception has been thrown
            err_msg = f"Failed processing task sequence_number={sequence_number}  {traceback.format_exc()}"
            logger.error(err_msg)
            if task.on_failure is not None:
                await self.publish_task(task.on_failure)

            if not message_settled:
                await self.deadletter_or_complete_task(
                    receiver, msg, "ExceptionThrown", task, detail=exc
                )
                message_settled = True

        # at-least once: settle at the end
        if task.acks_late and not message_settled:
            await self.complete_message(msg, receiver)
            message_settled = True

    async def task_handler(
        self, task: Task, sequence_number: int | None
    ) -> typing.Any | TaskFailureResultType:
        """
        Dynamically look up function requested and then evaluate it.
        """
        start = time.monotonic()
        logger.info(f"[{task.function_name}] Begin Task {sequence_number=}")

        # Check if it's a debug task
        if task.function_name == sample.TASK_NAME:
            return await sample.debug_task(self.state)

        # Look up function associated with the task
        function = self.function_registry.get(task.function_name)
        if not function:
            raise ValueError(f"Missing registered function {task.function_name}")
        try:
            result = await function(
                self.state, *task.payload["args"], **task.payload["kwargs"]
            )
            task_result = TaskResult(
                task_id=task.id,
                graph_id=task.graph_id,
                result=result,
                status=TaskStatus.SUCCESS,
            )
            await self.results_storage.store_task_result(task_result)
        except Exception as exc:
            logger.error(f"Function {task.function_name} raised exception: {exc}")
            task_result = TaskResult(
                task_id=task.id,
                graph_id=task.graph_id,
                status=TaskStatus.Failure,
                errors=str(exc),
                formatted_exception=traceback.format_exc(),
            )
            await self.results_storage.store_task_result(task_result)
            raise

        logger.info(
            f"[{task.function_name}] Completed Task {sequence_number=} in {time.monotonic()-start}s"
        )
        return result

    async def deadletter_or_complete_task(
        self,
        receiver: ServiceBusReceiver,
        msg: ServiceBusReceivedMessage,
        reason: str,
        task: Task,
        detail: Exception | str | None = None,
    ):
        description = f"Task failed: {detail}" if detail else "Task failed"
        if task.should_dead_letter:
            await receiver.dead_letter_message(
                msg,
                reason=reason,
                error_description=description,
            )
        else:
            await self.complete_message(msg, receiver)
        return None
