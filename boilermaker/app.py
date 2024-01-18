"""
Async tasks received from Service Bus go in here

"""
import copy
from functools import wraps
from json.decoder import JSONDecodeError
import logging
import traceback
import typing
import weakref
import time

from opentelemetry import trace
from pydantic import ValidationError

from .retries import RetryException, RetryPolicy
from .task import Task
from . import tracing


tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)


class BoilermakerAppException(Exception):
    def __init__(self, message: str, errors: list):
        super().__init__(message)

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

    async def apply_async(self, fn, *args, delay: int = 0, retry_policy: RetryPolicy = RetryPolicy.default(), **kwargs):
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
        return await self.publish_task(task_copy, delay=delay, retry_policy=retry_policy)

    @tracer.start_as_current_span("publish-task")
    async def publish_task(self, task: Task, delay: int = 0, retry_policy: RetryPolicy = RetryPolicy.default()):
        """Turn the task into JSON and publish to Service Bus"""
        encountered_errors = []

        for i in range(0, retry_policy.max_tries):
            try:
                await self.service_bus_client.send_message(
                    task.model_dump_json(),
                    delay=delay,
                )
                break
            except Exception as e:
                encountered_errors.append(e)

                if i == retry_policy.max_tries - 1:
                    raise BoilermakerAppException("Error encountered while publishing task to service bus", encountered_errors)

            time.sleep(retry_policy.get_delay_interval(i+1))


    async def run(self):
        """
        Task-Worker processing message queue loop.

        This method uses the service bus to run a receive loop.

        Note: *all* messages will be marked "complete" after the handler runs.

        See `document_storage.clients.AzureServiceBus.run_receive_messages` for details.
        """
        async with self.service_bus_client.get_receiver() as receiver:
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

    async def message_handler(self, msg: str, receiver):
        """Individual message handler"""
        message_settled = False
        try:
            task = Task.model_validate_json(str(msg))
        except (JSONDecodeError, ValidationError):
            msg = "Invalid task " f"exc_info={traceback.format_exc()}"
            logger.error(msg)
            # This task is not parseable
            await receiver.complete_message(msg)
            return None

        # Immediately record an attempt
        task.record_attempt()

        # at-most once: "complete" msg even if it fails later
        if task.acks_early:
            await receiver.complete_message(msg)
            message_settled = True

        if not task.can_retry:
            logger.error(f"Retries exhausted for event {task.function_name}")
            if task.should_dead_letter:
                await receiver.dead_letter_message(
                    msg,
                    reason="ProcessingError",
                    error_description="Task failed",
                )
            else:
                await receiver.complete_message(msg)

            message_settled = True
            return None

        # Actually handle the task here
        try:
            await self.task_handler(task)
        except RetryException as retry:
            # A retry has been requested
            delay = task.get_next_delay()
            warn_msg = (
                "Event retry requested. Publishing retry "
                f"{task.function_name=}"
                f"{retry.msg=}"
                f"{task.attempts.attempts=}"
                f"{delay=}"
            )
            logger.warn(warn_msg)
            await self.publish_task(
                task,
                delay=delay,
            )
        except Exception:
            # Some other exception has been thrown
            err_msg = "Failed processing task " f"{traceback.format_exc()}"
            logger.error(err_msg)
            if task.should_dead_letter:
                await receiver.dead_letter_message(
                    msg,
                    reason="ProcessingError",
                    error_description="Task failed",
                )
            elif not message_settled:
                await receiver.complete_message(msg)

            message_settled = True

        # at-least once: settle at the end
        if task.acks_late and not message_settled:
            await receiver.complete_message(msg)
            message_settled = True

    async def task_handler(self, task: Task):
        """
        Dynamically look up function requested and then evaluate it.
        """
        logger.info(f"[{task.function_name}] Received task args={task.payload}")
        function = self.function_registry.get(task.function_name)
        if not function:
            raise ValueError(f"Missing registered function {task.function_name}")
        return await function(
            self.state, *task.payload["args"], **task.payload["kwargs"]
        )
