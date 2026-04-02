from collections.abc import Awaitable
from typing import Any, ParamSpec, Protocol, TypeAlias, TypeVar

# params
P = ParamSpec("P")
# return type
R = TypeVar("R")


# Enforce the followign rule: anything Callable but it must have a __name__ attribute!
# functools.partials by default do not have a __name__ attribute. This offers a warning for users.
class NamedAwaitable(Protocol[P, R]):
    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...


TaskHandler: TypeAlias = NamedAwaitable[..., Awaitable[Any]]
