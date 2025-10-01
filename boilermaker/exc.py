from azure.servicebus.exceptions import ServiceBusError


class BoilermakerAppException(Exception):
    def __init__(self, message: str, errors: list):
        super().__init__(message + str(errors))

        self.errors = errors


class BoilermakerStorageError(Exception):
    """Custom exception for storage-related errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.details = kwargs or {}

    @property
    def reason(self) -> str | None:
        return self.details.get("reason")

    @property
    def status_code(self) -> int | None:
        return self.details.get("status_code")



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
