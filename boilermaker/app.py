"""
Async tasks received from Service Bus go in here

"""

import copy
import inspect
import itertools
import logging
import signal
import typing
import weakref
from functools import wraps

from aio_azure_clients_toolbox import AzureServiceBus, ManagedAzureServiceBusSender  # type: ignore
from anyio import create_task_group, open_signal_receiver
from anyio.abc import CancelScope
from azure.servicebus import ServiceBusReceivedMessage
from azure.servicebus.aio import ServiceBusReceiver
from azure.servicebus.exceptions import (
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusConnectionError,
    ServiceBusError,
)
from opentelemetry import trace

from . import tracing
from .evaluators import (
    evaluator_factory,
    MessageActions,
    TaskEvaluatorBase,
    TaskHandler,
    TaskHandlerRegistry,
    TaskPublisher,
)
from .exc import BoilermakerAppException, BoilermakerStorageError
from .retries import RetryPolicy
from .storage import StorageInterface
from .task import Task, TaskGraph, TaskId

tracer: trace.Tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)


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

        from boilermaker import Boilermaker
        from boilermaker.service_bus import AzureServiceBus

        # Set up ServiceBus client
        client = AzureServiceBus.from_config(config)

        # Create app with shared state
        app = Boilermaker({"counter": 0}, client)

        # Register a task
        @app.task()
        async def my_task(state, message: str):
            state["counter"] += 1
            print(f"Processing: {message}")
            return "completed"

        # Schedule a task
        await app.apply_async(my_task, "hello world")

        # Run worker (in separate process)
        await app.run()
    """

    def __init__(
        self,
        state: typing.Any,
        service_bus_client: AzureServiceBus | ManagedAzureServiceBusSender,
        enable_opentelemetry: bool = False,
        results_storage: StorageInterface | None = None,
    ):
        # This is likely going to be circular, the app referencing
        # the worker as well. Opt for weakrefs for improved mem safety.
        self.state = weakref.proxy(state)
        self.service_bus_client = service_bus_client
        self.otel_enabled = enable_opentelemetry
        self.results_storage = results_storage
        # Callables and Tasks
        self.function_registry: TaskHandlerRegistry = {}
        self.task_registry: dict[str, Task] = {}
        self._message_evaluators: dict[TaskId, TaskEvaluatorBase] = {}

    # ~~ ** Task Registration and Publishing ** ~~
    def task(self, **options):
        """Decorator to register an async function as a background task.

        Args:
            **options: Optional task configuration including 'policy' for retry settings

        Returns:
            Decorator function that registers the task and returns the original function

        Example:

            @app.task(policy=retries.RetryPolicy.default())
            async def process_data(state, item_id: str):
                return await state.db.process(item_id)
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

            async def send_email(state, recipient: str, subject: str):
                await state.email_client.send(recipient, subject)

            app.register_async(
                send_email,
                policy=retries.RetryPolicy(max_tries=3)
            )
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

            tasks = [process_email, send_notification, update_metrics]
            app.register_many_async(tasks, policy=retries.NoRetry())
        """
        for fn in fns:
            self.register_async(fn, **options)
        return self

    def create_task(self, fn: TaskHandler, *args, policy: RetryPolicy | None = None, **kwargs) -> Task:
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

            # Create task without publishing
            task = app.create_task(send_email, "user@example.com", subject="Welcome")

            # Set up callback
            task.on_success = app.create_task(track_email_sent)

            # Publish when ready
            await app.publish_task(task)
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

            # Create individual tasks
            fetch = app.create_task(fetch_data, url)
            process = app.create_task(process_data)
            save = app.create_task(save_results)
            cleanup = app.create_task(cleanup_on_failure)

            # Chain them together
            workflow = app.chain(fetch, process, save, on_failure=cleanup)

            # Start the workflow
            await app.publish_task(workflow)
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
            Task: The published task

        Raises:
            BoilermakerAppException: If publishing fails after all attempts
            ValueError: If function is not registered

        Example:

            # Simple task scheduling
            await app.apply_async(send_email, "user@example.com")

            # With custom retry policy
            await app.apply_async(
                process_image,
                image_url="https://example.com/image.jpg",
                policy=retries.RetryPolicy(max_tries=3)
            )

            # Delayed execution (5 minutes)
            await app.apply_async(cleanup_temp_files, delay=300)
        """
        task = self.create_task(fn, *args, policy=policy, **kwargs)
        return await self.publish_task(
            task, delay=delay, publish_attempts=publish_attempts
        )

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
            Task: The published task instance

        Raises:
            BoilermakerAppException: If publishing fails after all attempts

        Example:

            task = app.create_task(my_function, "arg1", kwarg="value")
            published = await app.publish_task(task, delay=60)  # 1 minute delay
            print(f"Published with sequence: {published.sequence_number}")
        """
        encountered_errors = []
        for _i in range(publish_attempts):
            try:
                results: list[int] = await self.service_bus_client.send_message(
                    task.model_dump_json(),
                    delay=delay,
                )
                if results and len(results) == 1:
                    sequence_number = results[0]
                    task.mark_published(sequence_number)
                    logger.debug(
                        f"Published task {task.task_id} to queue with sequence_number={sequence_number}"
                    )
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

    async def publish_graph(self, graph: TaskGraph) -> TaskGraph:
        """Publish a TaskGraph workflow to storage and schedule initial tasks.

        Args:
            graph: TaskGraph instance to publish

        Returns:
            TaskGraph: The same graph instance

        Raises:
            BoilermakerAppException: If storage or task publishing fails
        """
        if not self.results_storage:
            raise BoilermakerAppException("Results storage is required for TaskGraph workflows", [])

        # Validate graph_id for all tasks in the graph
        for task in graph.children.values():
            if task.graph_id != graph.graph_id:
                raise BoilermakerAppException(
                    "All tasks must have graph_id matching graph",
                    [f"Expected graph_id={graph.graph_id}, found {task.graph_id=}"],
                )

        # Store the graph definition and all pending task results
        # If this graph has already been stored, this should fail.
        try:
            await self.results_storage.store_graph(graph)
        except BoilermakerStorageError as exc:
            raise BoilermakerAppException("Error storing TaskGraph to storage", [str(exc)]) from exc

        # Publish all ready tasks (should be root nodes with no dependencies)
        for task in graph.generate_ready_tasks():
            await self.publish_task(task)

        return graph

    # ~~ ** Signal handling, receiver run, and message processing methods ** ~~
    async def signal_handler(
        self,
        scope: CancelScope,
    ):
        """We would like to reschedule any open messages on SIGINT/SIGTERM"""
        with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
            async for signum in signals:
                # We want all of these evaluators to abandon their current message
                try:
                    async with create_task_group() as abandon_group:
                        for (
                            sequence_number,
                            evaluator,
                        ) in self._message_evaluators.items():
                            logger.info(f"Pushing message back to queue {sequence_number=}")
                            abandon_group.start_soon(evaluator.abandon_current_message)
                except* Exception as excgroup:
                    for exc in excgroup.exceptions:
                        logger.error(f"Error occurred while abandoning messages: {exc}")

                logger.warning(f"Signal {signum=} received: shutting down. ")
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

            # In your worker process
            app = Boilermaker(app_state, service_bus_client)
            # Register your tasks...
            await app.run()  # Runs forever

        Raises:
            Various Azure Service Bus exceptions for connection issues
        """
        async with create_task_group() as tg:
            async with self.service_bus_client.get_receiver() as receiver:
                # Handle SIGTERM: when found -> instruct evaluator to abandon message
                tg.start_soon(self.signal_handler, tg.cancel_scope)

                # Main message loop
                async for msg in receiver:
                    # This separate method is easier to test and also allows for future enhancements:
                    # - allow concurrent batch eval via receive_messages + prefetch (with a CapacityLimiter)
                    # - allow prioritizing certain messages
                    # - etc.
                    try:
                        await self.message_handler(msg, receiver)
                    except Exception as exc:
                        # We catch everything here to avoid bringing down the worker
                        logger.error(f"Error in message handler: {exc}", exc_info=True)

    async def message_handler(self, msg: ServiceBusReceivedMessage, receiver: ServiceBusReceiver):
        """Process a single Service Bus message containing a Task.

        This method is called by the worker loop for each received message.
        It deserializes the Task, executes the associated function, and
        handles success, failure, retries, and callbacks.

        Args:

            msg: The received ServiceBusReceivedMessage containing the Task JSON

        Returns:

            None
        """
        task = await MessageActions.task_decoder(msg, receiver)
        if task is None:
            # Message could not be decoded so it was dead-lettered
            return None

        async with tracing.start_span_from_parent_event_async(
            tracer,
            msg,
            "BoilermakerWorker",
            otel_enabled=self.otel_enabled,
        ):
            evaluator = evaluator_factory(
                receiver,
                task,
                typing.cast(TaskPublisher, self.publish_task),
                self.function_registry,
                state=self.state,
                storage_interface=self.results_storage,
            )
            self._message_evaluators[task.task_id] = evaluator
            await evaluator()
            del self._message_evaluators[task.task_id]

    async def close(self):
        """Close any open connections to Azure Service Bus."""
        await self.service_bus_client.close()
        return None
