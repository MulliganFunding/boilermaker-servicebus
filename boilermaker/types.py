import typing
from collections.abc import Awaitable, Callable

TaskHandler: typing.TypeAlias = Callable[..., Awaitable[typing.Any]]
