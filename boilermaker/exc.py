from azure.servicebus.exceptions import ServiceBusError


class BoilermakerError(Exception):
    """Base class for Boilermaker-specific exceptions."""

    pass


class ContinueGraphError(BoilermakerError):
    """Raised when continue_graph cannot load the graph after retries.

    Signals message_handler that settlement must be suppressed so that
    Service Bus will redeliver the message and downstream dispatch can be retried.
    """

    pass


class BoilermakerAppException(Exception):
    def __init__(self, message: str, errors: list):
        super().__init__(message + str(errors))
        self.errors = errors


class BoilermakerStorageError(Exception):
    """Custom exception for storage-related errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.details = kwargs or {}

    def __getattr__(self, item):
        if self.details.get(item, None):
            return self.details[item]
        raise AttributeError(f"BoilermakerStorageError object has no attribute '{item}'")


class BoilermakerUnregisteredFunction(ValueError):
    """Custom exception indicating that an unregistered function was called."""

    pass


class BoilermakerTaskLeaseLost(ValueError):
    """
    Custom exception indicating that a task lease has been lost.

    Typically raised when a message lock expires before processing is complete.

    This indicates that the task may be processed again, potentially leading to
    duplicate processing if the original processing eventually completes.
    """

    pass


class BoilermakerServiceBusConfigurationError(Exception):
    """Custom exception for Service Bus configuration errors."""

    pass


class BoilermakerServiceBusError(ServiceBusError):
    """Custom exception for Service Bus related errors."""

    pass
