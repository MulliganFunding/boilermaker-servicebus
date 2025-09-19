"""
Async tasks received from Service Bus go in here

"""

import copy
import inspect
import itertools
import logging
import signal
import time
import traceback
import typing
import weakref
from functools import wraps
from json.decoder import JSONDecodeError

from aio_azure_clients_toolbox import AzureServiceBus, ManagedAzureServiceBusSender  # type: ignore
from anyio import create_task_group, open_signal_receiver
from anyio.abc import CancelScope
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusReceiver
from azure.servicebus.exceptions import (
    MessageLockLostError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusConnectionError,
    ServiceBusError,
    SessionLockLostError,
)
from opentelemetry import trace
from pydantic import ValidationError

from . import sample, tracing
from .failure import TaskFailureResult, TaskFailureResultType
from .retries import RetryException, RetryPolicy
from .task import Task

tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)
TaskHandler: typing.TypeAlias = typing.Callable[..., typing.Awaitable[typing.Any]]


class BoilermakerAppException(Exception):
    def __init__(self, message: str, errors: list):
        super().__init__(message + str(errors))

        self.errors = errors


class Boilermaker:
    """
    A Boilermaker instance is a worker that can run a tasks-processing worker
    or send tasks to the Service Bus queue.
    """

    def __init__(
        self,
        state: typing.Any,
        service_bus_client: AzureServiceBus | ManagedAzureServiceBusSender = None,
        enable_opentelemetry=False,
    ):
        # This is likely going to be circular, the app referencing
        # the worker as well. Opt for weakrefs for improved mem safety.
        self.state = weakref.proxy(state)
        self.service_bus_client = service_bus_client
        self.otel_enabled = enable_opentelemetry
        # Callables and Tasks
        self.function_registry: dict[str, typing.Any] = {}
        self.task_registry: dict[str, Task] = {}
        self._current_message: ServiceBusReceivedMessage | None = None

    # ~~ ** Task Registration and Publishing ** ~~
    def task(self, **options):
        """A task decorator can mark a task as backgroundable"""

        def deco(fn):
            self.register_async(fn, **options)

            @wraps(fn)
            async def inner(state, *args, **kwargs):
                return await fn(state, *args, **kwargs)

            return inner

        return deco

    def register_async(self, fn: TaskHandler, **options):
        """Register a task to be callable as background"""
        fn_name = fn.__name__

        # Check if already registered
        if fn_name in self.function_registry:
            raise ValueError(f"Function already registered: {fn_name}")

        # Check if TaskHandler is async callable
        if not inspect.iscoroutinefunction(fn):
            raise ValueError(f"Function must be async: {fn_name}")

        task = Task.default(fn_name, **options)
        self.function_registry[fn_name] = fn
        self.task_registry[fn_name] = task
        logger.info(f"Registered background function fn={fn_name}")
        return self

    def register_many_async(self, fns: list[TaskHandler], **options):
        """Register many tasks at once using the same `options` for each"""
        for fn in fns:
            self.register_async(fn, **options)
        return self

    def create_task(
        self, fn, *args, policy: RetryPolicy | None = None, **kwargs
    ) -> Task:
        """
        Create a Task object (but do not publish it).
        """
        if fn.__name__ not in self.function_registry:
            raise ValueError(f"Unregistered function invoked: {fn.__name__}")
        if fn.__name__ not in self.task_registry:
            raise ValueError(f"Unregistered task: {fn.__name__}")

        payload = {"args": args, "kwargs": kwargs}
        task_proto = self.task_registry[fn.__name__]
        task = copy.deepcopy(task_proto)
        task.payload = payload
        if policy is not None:
            task.policy = policy

        return task

    async def apply_async(
        self,
        fn,
        *args,
        delay: int = 0,
        publish_attempts: int = 1,
        policy: RetryPolicy | None = None,
        **kwargs,
    ) -> Task:
        """
        Wrap up this function call as a task and publish to broker.

        The task will be retried `publish_attempts` times if there are
        transient errors publishing to the broker.

        After publishing, the task's `sequence_number` will be set.

        Raises `BoilermakerAppException` if unable to publish.

        :param fn: The function to call
        :param args: Positional arguments to the (backgrounded) function
        :param delay: Optional delay in seconds before task is visible
        :param publish_attempts: How many times to attempt publishing
        :param policy: Optional retry policy to use for this task
        :param kwargs: Keyword arguments to the (backgrounded) function
        :returns: The published Task.
        """
        task = self.create_task(fn, *args, policy=policy, **kwargs)
        return await self.publish_task(
            task, delay=delay, publish_attempts=publish_attempts
        )

    def chain(self, *tasks: Task, on_failure: Task | None = None) -> Task:
        """
        Chain tasks together so that each task runs after the previous one
        completes successfully.

        The `on_failure` task (if provided) will be set as the
        on_failure callback for *all* tasks in the chain.

        :param tasks: The tasks to chain together
        :returns: The first task in the chain.
        """
        if len(tasks) < 2:
            raise ValueError("At least two tasks are required to form a chain")

        if on_failure is not None and not isinstance(on_failure, Task):
            raise ValueError("if passed, `on_failure` must be a Task instance")

        # Maintain pointer to the head of the chain
        task1 = tasks[0]
        task1.on_failure = on_failure
        for t1, t2 in itertools.pairwise(tasks):
            t1.on_success = t2
            # Set on_failure for all tasks if provided (None is fine here)
            t2.on_failure = on_failure

        return task1

    @tracer.start_as_current_span("boilermaker.publish-task")
    async def publish_task(
        self,
        task: Task,
        delay: int = 0,
        publish_attempts: int = 1,
    ) -> Task:
        """
        Turn the task into JSON and publish to Service Bus.
        Assign the sequence number once published.

        :param task: The Task to publish
        :param delay: Optional delay in seconds before task is visible
        :param publish_attempts: How many times to attempt publishing
        :returns: The published Task.
        """
        encountered_errors = []
        for _i in range(publish_attempts):
            try:
                result: list[int] = await self.service_bus_client.send_message(
                    task.model_dump_json(),
                    delay=delay,
                )
                if result and len(result) > 0:
                    task._sequence_number = result[0]
                return task
            except (
                ServiceBusError,
                ServiceBusConnectionError,
                ServiceBusAuthorizationError,
                ServiceBusAuthenticationError,
            ) as exc:
                encountered_errors.append(exc)
        else:
            raise BoilermakerAppException(
                "Error encountered while publishing task to service bus",
                encountered_errors,
            )

    # ~~ ** Signal handling, receiver run, and message processing methods ** ~~
    async def signal_handler(self, receiver: ServiceBusReceiver, scope: CancelScope):
        """We would like to reschedule any open messages on SIGINT/SIGTERM"""
        with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for signum in signals:
                if self._current_message is not None:
                    sequence_number = self._current_message.sequence_number
                    try:
                        await receiver.abandon_message(self._current_message)
                    except (
                        MessageLockLostError,
                        ServiceBusError,
                        SessionLockLostError,
                    ):
                        msg = (
                            f"Failed to requeue message {sequence_number=} "
                            f"exc_info={traceback.format_exc()}"
                        )
                        logger.error(msg)
                    self._current_message = None
                    logger.warning(
                        f"Signal {signum=} received: shutting down. "
                        f"Msg returned to queue {sequence_number=}"
                    )
                scope.cancel()
                return

    async def run(self):
        """
        Task-Worker processing message queue loop.

        This method uses the service bus to run a receive loop.

        Note: *all* messages will be marked "complete" after the handler runs.

        See `AzureServiceBus` for details.
        """
        async with create_task_group() as tg:
            async with self.service_bus_client.get_receiver() as receiver:
                # Handle SIGTERM: when found, agbandon message
                tg.start_soon(self.signal_handler, receiver, tg.cancel_scope)

                async for msg in receiver:
                    # This separate method is easier to test
                    # and easier to early-return from in case of skip or fail msg
                    async with tracing.start_span_from_parent_event_async(
                        tracer,
                        msg,
                        "BoilermakerWorker",
                        otel_enabled=self.otel_enabled,
                    ):
                        await self.message_handler(msg, receiver)

    async def complete_message(
        self, msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver
    ):
        try:
            await receiver.complete_message(msg)
        except (MessageLockLostError, ServiceBusError, SessionLockLostError):
            logmsg = (
                f"Failed to settle message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
        self._current_message = None

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
            # This task is a failure because it did not succeed and retries are exhausted
            if task.on_failure is not None:
                await self.publish_task(task.on_failure)
            # Early return here: no more processing
            return None

        # Actually handle the task here
        try:
            result = await self.task_handler(task, sequence_number)
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
        result = await function(
            self.state, *task.payload["args"], **task.payload["kwargs"]
        )
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
