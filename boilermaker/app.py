"""
Async tasks received from Service Bus go in here

"""
import copy
from functools import wraps
from json.decoder import JSONDecodeError
import logging
import signal
import time
import traceback
import typing
import weakref

from anyio import open_signal_receiver, create_task_group
from anyio.abc import CancelScope

from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusReceiver
from azure.servicebus.exceptions import (
    MessageLockLostError,
    ServiceBusError,
    ServiceBusConnectionError,
    ServiceBusAuthorizationError,
    ServiceBusAuthenticationError,
    SessionLockLostError,
)
from opentelemetry import trace
from pydantic import ValidationError
from .retries import RetryException
from . import sample
from .task import Task
from . import tracing


tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)


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
        self, state: typing.Any, service_bus_client=None, enable_opentelemetry=False
    ):
        # This is likely going to be circular, the app referencing
        # the worker as well. Opt for weakrefs for improved mem safety.
        self.state = weakref.proxy(state)
        self.service_bus_client = service_bus_client
        self.otel_enabled = enable_opentelemetry
        # Callables and Tasks
        self.function_registry: typing.Dict[str, typing.Any] = {}
        self.task_registry: typing.Dict[str, Task] = {}
        self._current_message: ServiceBusReceivedMessage | None = None

    def task(self, **options):
        """A task decorator can mark a task as backgroundable"""

        def deco(fn):
            self.register_async(fn, **options)

            @wraps(fn)
            async def inner(state, *args, **kwargs):
                return await fn(state, *args, **kwargs)

            return inner

        return deco

    def register_async(self, fn, **options):
        """Register a task to be callable as background"""
        fn_name = fn.__name__
        task = Task.default(fn_name, **options)
        self.function_registry[fn_name] = fn
        self.task_registry[fn_name] = task
        logger.info(f"Registered background function fn={fn_name}")
        return self

    async def apply_async(self, fn, *args, delay: int = 0, retries: int = 3, **kwargs):
        """
        Wrap up this function call as a task and publish to broker.
        """
        if fn.__name__ not in self.function_registry:
            raise ValueError(f"Unregistered function invoked: {fn.__name__}")
        if fn.__name__ not in self.task_registry:
            raise ValueError(f"Unregistered task: {fn.__name__}")

        payload = {"args": args, "kwargs": kwargs}
        task = self.task_registry[fn.__name__]
        task_copy = copy.deepcopy(task)
        task_copy.payload = payload
        return await self.publish_task(task_copy, delay=delay, retries=retries)

    @tracer.start_as_current_span("publish-task")
    async def publish_task(self, task: Task, delay: int = 0, retries: int = 3):
        """Turn the task into JSON and publish to Service Bus"""
        encountered_errors = []

        for _i in range(0, retries):
            try:
                return await self.service_bus_client.send_message(
                    task.model_dump_json(),
                    delay=delay,
                )
            except (
                ServiceBusError,
                ServiceBusConnectionError,
                ServiceBusAuthorizationError,
                ServiceBusAuthenticationError,
            ) as e:
                encountered_errors.append(e)
        else:
            raise BoilermakerAppException(
                "Error encountered while publishing task to service bus",
                encountered_errors,
            )

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
                    logger.warn(
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
            msg = (
                f"Failed to settle message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(msg)
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
            msg = f"Invalid task sequence_number={sequence_number} exc_info={traceback.format_exc()}"
            logger.error(msg)
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
            if task.should_dead_letter:
                await receiver.dead_letter_message(
                    msg,
                    reason="ProcessingError",
                    error_description="Task failed: retries exhausted",
                )
            else:
                await self.complete_message(msg, receiver)

            message_settled = True
            return None

        # Actually handle the task here
        try:
            await self.task_handler(task, sequence_number)
        except RetryException as retry:
            # A retry has been requested
            delay = task.get_next_delay()
            warn_msg = (
                "Event retry requested. Publishing retry:"
                f"[ {task.function_name=}"
                f" {retry.msg=}"
                f" {task.attempts.attempts=}"
                f" {delay=}"
                f" {sequence_number=} ]"
            )
            logger.warn(warn_msg)
            await self.publish_task(
                task,
                delay=delay,
            )
        except Exception as exc:
            # Some other exception has been thrown
            err_msg = f"Failed processing task sequence_number={sequence_number}  {traceback.format_exc()}"
            logger.error(err_msg)
            if task.should_dead_letter:
                await receiver.dead_letter_message(
                    msg,
                    reason="ProcessingError",
                    error_description=f"Task failed: {exc}",
                )
            elif not message_settled:
                await self.complete_message(msg, receiver)

            message_settled = True

        # at-least once: settle at the end
        if task.acks_late and not message_settled:
            await self.complete_message(msg, receiver)
            message_settled = True

    async def task_handler(self, task: Task, sequence_number: int):
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
            f"[{task.function_name}] Compeleted Task {sequence_number=} in {time.monotonic()-start}s"
        )
        return result
