"""CLI tool for inspecting and recovering TaskGraph state in Azure Blob Storage."""

import argparse
import asyncio
import logging
import os
import sys

from azure.identity.aio import DefaultAzureCredential

from boilermaker.cli._globals import configure_console, EXIT_ERROR
from boilermaker.cli.inspect import run_inspect
from boilermaker.cli.invoke import run_invoke
from boilermaker.cli.purge import _validate_older_than, run_purge
from boilermaker.cli.recover import run_recover
from boilermaker.storage.blob_storage import BlobClientStorage

logger = logging.getLogger("boilermaker.cli")

_NO_COLOR_ENV_VAR = "NO_COLOR"
_STORAGE_URL_ENV_VAR = "BOILERMAKER_STORAGE_URL"
_CONTAINER_ENV_VAR = "BOILERMAKER_CONTAINER"


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="boilermaker",
        description="Inspect and recover TaskGraph state in Azure Blob Storage",
    )

    # Global options — apply to all subcommands
    parser.add_argument(
        "--storage-url",
        default=os.environ.get(_STORAGE_URL_ENV_VAR),
        metavar="URL",
        help=f"Azure Blob Storage account URL (env: {_STORAGE_URL_ENV_VAR})",
    )
    parser.add_argument(
        "--container",
        default=os.environ.get(_CONTAINER_ENV_VAR),
        metavar="NAME",
        help=f"Blob container name (env: {_CONTAINER_ENV_VAR})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--no-color",
        action="store_true",
        default=_NO_COLOR_ENV_VAR in os.environ,
        help=f"Disable colored output (env: {_NO_COLOR_ENV_VAR})",
    )

    subparsers = parser.add_subparsers(dest="command")

    _add_inspect_subparser(subparsers)
    _add_recover_subparser(subparsers)
    _add_purge_subparser(subparsers)
    _add_invoke_subparser(subparsers)

    return parser


def _add_inspect_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    inspect_parser = subparsers.add_parser("inspect", help="Inspect a TaskGraph or single task")
    inspect_parser.add_argument(
        "--graph",
        metavar="GRAPH_ID",
        help="Inspect the entire graph",
    )
    inspect_parser.add_argument(
        "--task",
        metavar="TASK_ID",
        help="Inspect a single task (requires --graph)",
    )
    inspect_parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of Rich-formatted output",
    )
    inspect_parser.add_argument(
        "--visual",
        action="store_true",
        help="Generate an HTML DAG visualization and open in browser (requires --graph)",
    )


def _add_recover_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    recover_parser = subparsers.add_parser("recover", help="Re-publish stalled tasks for a graph")
    recover_parser.add_argument("--graph", required=True, metavar="GRAPH_ID", help="Graph ID to recover")
    recover_parser.add_argument(
        "--sb-namespace-url",
        required=True,
        metavar="URL",
        help="Service Bus namespace URL",
    )
    recover_parser.add_argument(
        "--sb-queue-name",
        required=True,
        metavar="NAME",
        help="Service Bus queue name",
    )


def _add_purge_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    purge_parser = subparsers.add_parser("purge", help="Delete old task-result blobs from Azure Blob Storage")
    purge_parser.add_argument(
        "--older-than",
        required=True,
        type=_validate_older_than,
        metavar="DAYS",
        help="Delete blobs last modified more than DAYS days ago (1–30 inclusive)",
    )
    purge_parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted without deleting")
    purge_parser.add_argument(
        "--all-graphs",
        action="store_true",
        default=False,
        help=(
            "Discover graphs by listing all graph-id prefixes and using UUID7 temporal ordering "
            "instead of tag-based filtering. Use for containers with pre-tag blobs."
        ),
    )


def _add_invoke_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    invoke_parser = subparsers.add_parser("invoke", help="Re-publish a single task to Service Bus")
    invoke_parser.add_argument("task_id", metavar="TASK_ID", help="Task ID to invoke")
    invoke_parser.add_argument("--graph", required=True, metavar="GRAPH_ID", help="Graph ID containing the task")
    invoke_parser.add_argument(
        "--sb-namespace-url",
        required=True,
        metavar="URL",
        help="Service Bus namespace URL",
    )
    invoke_parser.add_argument(
        "--sb-queue-name",
        required=True,
        metavar="NAME",
        help="Service Bus queue name",
    )
    invoke_parser.add_argument(
        "--force",
        action="store_true",
        help="Allow re-invocation of tasks already in a terminal state",
    )


def _validate_global_required_options(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Validate that --storage-url and --container are set after env var fallback resolution.

    argparse does not support required=True with env var defaults. We use default=os.environ.get(...)
    and perform this post-parse check instead.
    """
    missing = []
    if args.storage_url is None:
        missing.append(f"--storage-url (or set {_STORAGE_URL_ENV_VAR})")
    if args.container is None:
        missing.append(f"--container (or set {_CONTAINER_ENV_VAR})")
    if missing:
        parser.error("the following arguments are required: " + ", ".join(missing))


def _validate_inspect_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Validate inspect subcommand argument constraints that argparse cannot express natively."""
    if args.graph is None and args.task is None:
        parser.error("inspect: one of --graph or --task is required")
    if args.task is not None and args.graph is None:
        parser.error("inspect: --task requires --graph")
    if hasattr(args, "visual") and args.visual:
        if args.graph is None:
            parser.error("inspect: --visual requires --graph")
        if args.json:
            parser.error("inspect: --visual and --json are mutually exclusive")
        if args.task is not None:
            parser.error("inspect: --visual and --task are mutually exclusive")


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(EXIT_ERROR)

    _validate_global_required_options(args, parser)

    if args.command == "inspect":
        _validate_inspect_args(args, parser)

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    console = configure_console(no_color=args.no_color)

    async def run() -> int:
        credentials = DefaultAzureCredential()
        storage = BlobClientStorage(args.storage_url, args.container, credentials)
        try:
            if args.command == "inspect":
                return await run_inspect(
                    storage,
                    args.graph,
                    console=console,
                    output_json=args.json,
                    task_id=args.task,
                    visual=getattr(args, "visual", False),
                )
            if args.command == "recover":
                return await run_recover(
                    storage,
                    args.graph,
                    sb_namespace_url=args.sb_namespace_url,
                    sb_queue_name=args.sb_queue_name,
                    console=console,
                )
            if args.command == "purge":
                return await run_purge(
                    storage,
                    older_than_days=args.older_than,
                    dry_run=args.dry_run,
                    all_graphs=getattr(args, "all_graphs", False),
                    console=console,
                )
            if args.command == "invoke":
                return await run_invoke(
                    storage,
                    args.graph,
                    args.task_id,
                    sb_namespace_url=args.sb_namespace_url,
                    sb_queue_name=args.sb_queue_name,
                    force=args.force,
                    console=console,
                )
            print(f"ERROR: Unknown command: {args.command}", file=sys.stderr)
            return EXIT_ERROR
        finally:
            await credentials.close()

    exit_code = asyncio.run(run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
