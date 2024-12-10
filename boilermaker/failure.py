from typing import TypeAlias


# Singleton Failure: we'll create one specific instance to represent a failure
# and check identity to see if it's been returned.
# Do not use this class directly, use `TaskFailureResult` for a value of type `TaskFailureResultType`.
class _TaskFailureResult:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Put any initialization here.
        return cls._instance

    def __call__(self):
        return self

# This is the Singleton instance of TaskFailureResult.
# Tasks can *return* this value to signal a failure.
# This is a singleton, so we can check for identity.
TaskFailureResult = _TaskFailureResult()
TaskFailureResultType: TypeAlias = _TaskFailureResult

