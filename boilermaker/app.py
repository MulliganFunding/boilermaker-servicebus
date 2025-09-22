"""
Async tasks received from Service Bus go in here

"""

import copy
import datetime
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
    MessageAlreadySettled,
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
    """Async task runner for Azure Service Bus queues.

    Boilermaker allows you to register async functions as background tasks,
    schedule them for execution, and run workers to process them. It provides
    retry policies, task chaining, callbacks, and comprehensive error handling.

    Args:
        state: Shared application state passed to all tasks as first argument
        service_bus_client: Azure Service Bus client for message handling
        enable_opentelemetry: Enable OpenTelemetry tracing (default: False)

    Example:
        >>> from boilermaker import Boilermaker
        >>> from boilermaker.service_bus import AzureServiceBus
        >>>
        >>> # Set up ServiceBus client
        >>> client = AzureServiceBus.from_config(config)
        >>>
        >>> # Create app with shared state
        >>> app = Boilermaker({"counter": 0}, client)
        >>>
        >>> # Register a task
        >>> @app.task()
        >>> async def my_task(state, message: str):
        >>>     state["counter"] += 1
        >>>     print(f"Processing: {message}")
        >>>     return "completed"
        >>>
        >>> # Schedule a task
        >>> await app.apply_async(my_task, "hello world")
        >>>
        >>> # Run worker (in separate process)
        >>> await app.run()
    """

    def __init__(
        self,
        state: typing.Any,
        service_bus_client: AzureServiceBus | ManagedAzureServiceBusSender = None,
        enable_opentelemetry: bool = False,
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
        self._receiver: ServiceBusReceiver | None = None

    # ~~ ** Task Registration and Publishing ** ~~
    def task(self, **options):
        """Decorator to register an async function as a background task.

        Args:
            **options: Optional task configuration including 'policy' for retry settings

        Returns:
            Decorator function that registers the task and returns the original function

        Example:
            >>> @app.task(policy=retries.RetryPolicy.default())
            >>> async def process_data(state, item_id: str):
            >>>     return await state.db.process(item_id)
        """

        def deco(fn):
            self.register_async(fn, **options)

            @wraps(fn)
            async def inner(state, *args, **kwargs):
                return await fn(state, *args, **kwargs)

            return inner

        return deco

    def register_async(self, fn: TaskHandler, **options):
        """Register an async function as a background task.

        Args:
            fn: Async function that takes state as first parameter
            **options: Task configuration options including:
                - policy: RetryPolicy for handling failures
                - should_dead_letter: Whether to dead letter failed messages
                - acks_late: Message acknowledgment timing

        Returns:
            self: For method chaining

        Raises:
            ValueError: If function is already registered or not async

        Example:
            >>> async def send_email(state, recipient: str, subject: str):
            >>>     await state.email_client.send(recipient, subject)
            >>>
            >>> app.register_async(
            >>>     send_email,
            >>>     policy=retries.RetryPolicy(max_tries=3)
            >>> )
        """
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
        """Register multiple async functions with the same configuration.

        Args:
            fns: List of async functions to register
            **options: Common task configuration applied to all functions

        Returns:
            self: For method chaining

        Example:
            >>> tasks = [process_email, send_notification, update_metrics]
            >>> app.register_many_async(tasks, policy=retries.NoRetry())
        """
        for fn in fns:
            self.register_async(fn, **options)
        return self

    def create_task(
        self, fn: TaskHandler, *args, policy: RetryPolicy | None = None, **kwargs
    ) -> Task:
        """Create a Task instance without publishing it to the queue.

        This allows you to set up callbacks, modify task properties, or
        build workflows before publishing.

        Args:
            fn: Registered async function to create task for
            *args: Positional arguments to pass to the function
            policy: Optional retry policy override for this task instance
            **kwargs: Keyword arguments to pass to the function

        Returns:
            Task: Configured task instance ready for publishing

        Raises:
            ValueError: If function is not registered

        Example:
            >>> # Create task without publishing
            >>> task = app.create_task(send_email, "user@example.com", subject="Welcome")
            >>>
            >>> # Set up callback
            >>> task.on_success = app.create_task(track_email_sent)
            >>>
            >>> # Publish when ready
            >>> await app.publish_task(task)
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
        fn: TaskHandler,
        *args,
        delay: int = 0,
        publish_attempts: int = 1,
        policy: RetryPolicy | None = None,
        **kwargs,
    ) -> Task:
        """Schedule a task for background execution.

        Creates a task and immediately publishes it to the Azure Service Bus queue.
        This is the most common way to schedule background work.

        Args:
            fn: Registered async function to execute
            *args: Positional arguments for the function
            delay: Seconds to delay before task becomes visible (default: 0)
            publish_attempts: Retry attempts for publishing failures (default: 1)
            policy: Override retry policy for this task instance
            **kwargs: Keyword arguments for the function

        Returns:
            Task: The published task with sequence_number set

        Raises:
            BoilermakerAppException: If publishing fails after all attempts
            ValueError: If function is not registered

        Example:
            >>> # Simple task scheduling
            >>> await app.apply_async(send_email, "user@example.com")
            >>>
            >>> # With custom retry policy
            >>> await app.apply_async(
            >>>     process_image,
            >>>     image_url="https://example.com/image.jpg",
            >>>     policy=retries.RetryPolicy(max_tries=3)
            >>> )
            >>>
            >>> # Delayed execution (5 minutes)
            >>> await app.apply_async(cleanup_temp_files, delay=300)
        """
        task = self.create_task(fn, *args, policy=policy, **kwargs)
        return await self.publish_task(
            task, delay=delay, publish_attempts=publish_attempts
        )

    def chain(self, *tasks: Task, on_failure: Task | None = None) -> Task:
        """Chain multiple tasks to run sequentially on success.

        Creates a workflow where each task runs only if the previous one
        succeeds. If any task fails, the chain stops and the optional
        failure handler runs.

        Args:
            *tasks: Task instances to chain together (minimum 2 required)
            on_failure: Optional task to run if any task in the chain fails

        Returns:
            Task: The first task in the chain (publish this to start the workflow)

        Raises:
            ValueError: If fewer than 2 tasks provided or on_failure is not a Task

        Example:
            >>> # Create individual tasks
            >>> fetch = app.create_task(fetch_data, url)
            >>> process = app.create_task(process_data)
            >>> save = app.create_task(save_results)
            >>> cleanup = app.create_task(cleanup_on_failure)
            >>>
            >>> # Chain them together
            >>> workflow = app.chain(fetch, process, save, on_failure=cleanup)
            >>>
            >>> # Start the workflow
            >>> await app.publish_task(workflow)
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
        """Publish a task to the Azure Service Bus queue.

        Serializes the task to JSON and sends it to the configured queue.
        Sets the task's sequence_number after successful publishing.

        Args:
            task: Task instance to publish
            delay: Seconds to delay before task becomes visible (default: 0)
            publish_attempts: Number of retry attempts for publishing (default: 1)

        Returns:
            Task: The same task with sequence_number populated

        Raises:
            BoilermakerAppException: If publishing fails after all attempts

        Example:
            >>> task = app.create_task(my_function, "arg1", kwarg="value")
            >>> published = await app.publish_task(task, delay=60)  # 1 minute delay
            >>> print(f"Published with sequence: {published._sequence_number}")
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
                    self._receiver = None
                    logger.warning(
                        f"Signal {signum=} received: shutting down. "
                        f"Msg returned to queue {sequence_number=}"
                    )
                scope.cancel()
                return

    async def run(self):
        """Start the worker to process tasks from the queue.

        This is the main worker loop that:
        1. Connects to Azure Service Bus queue
        2. Receives messages containing tasks
        3. Executes the tasks with registered functions
        4. Handles retries, callbacks, and error scenarios
        5. Manages graceful shutdown on SIGINT/SIGTERM

        The worker runs indefinitely until interrupted. Each message is
        processed according to its task configuration (retries, callbacks, etc.).

        Note:
            - Run this in a separate process from your main application
            - Multiple workers can run in parallel for horizontal scaling
            - Workers automatically handle message acknowledgment
            - Interrupted workers will abandon current message back to queue

        Example:
            >>> # In your worker process
            >>> app = Boilermaker(app_state, service_bus_client)
            >>> # Register your tasks...
            >>> await app.run()  # Runs forever

        Raises:
            Various Azure Service Bus exceptions for connection issues
        """
        async with create_task_group() as tg:
            async with self.service_bus_client.get_receiver() as receiver:
                # Handle SIGTERM: when found, agbandon message
                tg.start_soon(self.signal_handler, receiver, tg.cancel_scope)
                # Keep reference to receiver for lock renewals
                self._receiver = receiver
                # Main message loop
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
        except (
            MessageAlreadySettled,
            MessageLockLostError,
            ServiceBusError,
            SessionLockLostError,
        ):
            logmsg = (
                f"Failed to settle message sequence_number={msg.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
        self._current_message = None

    async def renew_message_lock(self) -> datetime.datetime | None:
        """Renew the lock on the current message being processed."""
        if self._receiver is None:
            logger.warning("No receiver to renew lock for")
            return None
        if self._current_message is None:
            logger.warning("No current message to renew lock for")
            return None

        try:
            # Returns new expiration time if successful
            return await self._receiver.renew_message_lock(self._current_message)
        except (MessageLockLostError, MessageAlreadySettled, SessionLockLostError):
            logmsg = (
                f"Failed to renew message lock sequence_number={self._current_message.sequence_number} "
                f"exc_info={traceback.format_exc()}"
            )
            logger.error(logmsg)
            return None

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
