"""Shared constants and console configuration for the boilermaker CLI."""

from rich.console import Console

from boilermaker.task import TaskStatus

EXIT_HEALTHY = 0
EXIT_STALLED = 1
EXIT_ERROR = 2

STALLED_STATUSES = {TaskStatus.Scheduled, TaskStatus.Started, TaskStatus.Retry}

OLDER_THAN_MIN = 1
OLDER_THAN_MAX = 30


def configure_console(no_color: bool) -> Console:
    """Create a Rich Console configured for CLI output.

    When no_color is True, all styling is stripped. Rich also auto-disables
    formatting when stdout is not a TTY.
    """
    return Console(no_color=no_color)
