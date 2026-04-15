"""Tests for boilermaker.cli build_parser() — new command tree structure."""

import os
from unittest import mock

import pytest
from boilermaker.cli import build_parser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STORAGE_URL = "https://example.blob.core.windows.net"
_CONTAINER = "my-container"
_GRAPH_ID = "019d8c0c-bd9b-7c23-be84-4d0799d7ecd4"
_TASK_ID = "019d8c0c-be84-4c5388b8de6c"
_SB_NAMESPACE = "https://myns.servicebus.windows.net"
_SB_QUEUE = "tasks"

_GLOBAL_OPTS = [
    "--storage-url", _STORAGE_URL,
    "--container", _CONTAINER,
]


# ---------------------------------------------------------------------------
# Prog name
# ---------------------------------------------------------------------------


class TestProgName:
    def test_prog_name_is_boilermaker(self):
        parser = build_parser()
        assert parser.prog == "boilermaker"


# ---------------------------------------------------------------------------
# Global options
# ---------------------------------------------------------------------------


class TestGlobalOptions:
    def test_storage_url_is_global(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", _STORAGE_URL,
            "--container", _CONTAINER,
            "inspect", "--graph", _GRAPH_ID,
        ])
        assert args.storage_url == _STORAGE_URL

    def test_container_is_global(self):
        parser = build_parser()
        args = parser.parse_args([
            "--storage-url", _STORAGE_URL,
            "--container", _CONTAINER,
            "inspect", "--graph", _GRAPH_ID,
        ])
        assert args.container == _CONTAINER

    def test_verbose_short_flag_is_global(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "-v", "inspect", "--graph", _GRAPH_ID])
        assert args.verbose is True

    def test_verbose_long_flag_is_global(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "--verbose", "inspect", "--graph", _GRAPH_ID])
        assert args.verbose is True

    def test_verbose_defaults_false(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
        assert args.verbose is False

    def test_no_color_flag_is_global(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "--no-color", "inspect", "--graph", _GRAPH_ID])
        assert args.no_color is True

    def test_no_color_defaults_false_without_env(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            # Reimport to get a fresh parser without env var influence
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
            assert args.no_color is False

    def test_no_color_defaults_true_when_no_color_env_set(self):
        with mock.patch.dict(os.environ, {"NO_COLOR": "1"}):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
            assert args.no_color is True


# ---------------------------------------------------------------------------
# Env var fallbacks
# ---------------------------------------------------------------------------


class TestEnvVarFallbacks:
    def test_storage_url_from_env(self):
        with mock.patch.dict(os.environ, {"BOILERMAKER_STORAGE_URL": _STORAGE_URL}):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args(["--container", _CONTAINER, "inspect", "--graph", _GRAPH_ID])
            assert args.storage_url == _STORAGE_URL

    def test_container_from_env(self):
        with mock.patch.dict(os.environ, {"BOILERMAKER_CONTAINER": _CONTAINER}):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args(["--storage-url", _STORAGE_URL, "inspect", "--graph", _GRAPH_ID])
            assert args.container == _CONTAINER

    def test_cli_flag_overrides_env_storage_url(self):
        with mock.patch.dict(os.environ, {"BOILERMAKER_STORAGE_URL": "https://env.blob.core.windows.net"}):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args([
                "--storage-url", _STORAGE_URL,
                "--container", _CONTAINER,
                "inspect", "--graph", _GRAPH_ID,
            ])
            assert args.storage_url == _STORAGE_URL

    def test_storage_url_is_none_when_not_set(self):
        env_without_storage = {k: v for k, v in os.environ.items() if k != "BOILERMAKER_STORAGE_URL"}
        with mock.patch.dict(os.environ, env_without_storage, clear=True):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args(["--container", _CONTAINER, "inspect", "--graph", _GRAPH_ID])
            assert args.storage_url is None

    def test_container_is_none_when_not_set(self):
        env_without_container = {k: v for k, v in os.environ.items() if k != "BOILERMAKER_CONTAINER"}
        with mock.patch.dict(os.environ, env_without_container, clear=True):
            from boilermaker.cli import build_parser as fresh_build_parser
            parser = fresh_build_parser()
            args = parser.parse_args(["--storage-url", _STORAGE_URL, "inspect", "--graph", _GRAPH_ID])
            assert args.container is None


# ---------------------------------------------------------------------------
# inspect subcommand
# ---------------------------------------------------------------------------


class TestInspectSubcommand:
    def test_inspect_graph_parses_correctly(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
        assert args.command == "inspect"
        assert args.graph == _GRAPH_ID
        assert args.task is None
        assert args.json is False

    def test_inspect_task_with_graph_parses_correctly(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--task", _TASK_ID, "--graph", _GRAPH_ID])
        assert args.command == "inspect"
        assert args.task == _TASK_ID
        assert args.graph == _GRAPH_ID

    def test_inspect_json_flag_parses(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID, "--json"])
        assert args.json is True

    def test_inspect_json_defaults_false(self):
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID])
        assert args.json is False

    def test_inspect_graph_and_task_together(self):
        """--graph and --task can be provided together (task inspect requires both)."""
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--graph", _GRAPH_ID, "--task", _TASK_ID])
        assert args.graph == _GRAPH_ID
        assert args.task == _TASK_ID


# ---------------------------------------------------------------------------
# inspect post-parse validation
# ---------------------------------------------------------------------------


class TestInspectValidation:
    def test_inspect_without_graph_or_task_is_error(self):
        """inspect with neither --graph nor --task should fail post-parse validation."""
        from boilermaker.cli import _validate_inspect_args
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect"])
        with pytest.raises(SystemExit) as exc_info:
            _validate_inspect_args(args, parser)
        assert exc_info.value.code == 2

    def test_inspect_task_without_graph_is_error(self):
        """--task without --graph should fail post-parse validation."""
        from boilermaker.cli import _validate_inspect_args
        parser = build_parser()
        args = parser.parse_args([*_GLOBAL_OPTS, "inspect", "--task", _TASK_ID])
        with pytest.raises(SystemExit) as exc_info:
            _validate_inspect_args(args, parser)
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# recover subcommand
# ---------------------------------------------------------------------------


class TestRecoverSubcommand:
    def test_recover_parses_all_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS,
            "recover",
            "--graph", _GRAPH_ID,
            "--sb-namespace-url", _SB_NAMESPACE,
            "--sb-queue-name", _SB_QUEUE,
        ])
        assert args.command == "recover"
        assert args.graph == _GRAPH_ID
        assert args.sb_namespace_url == _SB_NAMESPACE
        assert args.sb_queue_name == _SB_QUEUE

    def test_recover_without_graph_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "recover",
                "--sb-namespace-url", _SB_NAMESPACE,
                "--sb-queue-name", _SB_QUEUE,
            ])
        assert exc_info.value.code == 2

    def test_recover_without_sb_namespace_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "recover",
                "--graph", _GRAPH_ID,
                "--sb-queue-name", _SB_QUEUE,
            ])
        assert exc_info.value.code == 2

    def test_recover_without_sb_queue_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "recover",
                "--graph", _GRAPH_ID,
                "--sb-namespace-url", _SB_NAMESPACE,
            ])
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# purge subcommand
# ---------------------------------------------------------------------------


class TestPurgeSubcommand:
    def test_purge_parses_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS,
            "purge",
            "--task-results",
            "--older-than", "7",
        ])
        assert args.command == "purge"
        assert args.task_results is True
        assert args.older_than == 7
        assert args.dry_run is False

    def test_purge_dry_run_flag_parses(self):
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS,
            "purge",
            "--task-results",
            "--older-than", "7",
            "--dry-run",
        ])
        assert args.dry_run is True

    def test_purge_without_task_results_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "purge",
                "--older-than", "7",
            ])
        assert exc_info.value.code == 2

    def test_purge_without_older_than_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "purge",
                "--task-results",
            ])
        assert exc_info.value.code == 2


# ---------------------------------------------------------------------------
# invoke subcommand
# ---------------------------------------------------------------------------


class TestInvokeSubcommand:
    def test_invoke_parses_all_required_args(self):
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS,
            "invoke", _TASK_ID,
            "--graph", _GRAPH_ID,
            "--sb-namespace-url", _SB_NAMESPACE,
            "--sb-queue-name", _SB_QUEUE,
        ])
        assert args.command == "invoke"
        assert args.task_id == _TASK_ID
        assert args.graph == _GRAPH_ID
        assert args.sb_namespace_url == _SB_NAMESPACE
        assert args.sb_queue_name == _SB_QUEUE
        assert args.force is False

    def test_invoke_force_flag_parses(self):
        parser = build_parser()
        args = parser.parse_args([
            *_GLOBAL_OPTS,
            "invoke", _TASK_ID,
            "--graph", _GRAPH_ID,
            "--sb-namespace-url", _SB_NAMESPACE,
            "--sb-queue-name", _SB_QUEUE,
            "--force",
        ])
        assert args.force is True

    def test_invoke_without_task_id_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "invoke",
                "--graph", _GRAPH_ID,
                "--sb-namespace-url", _SB_NAMESPACE,
                "--sb-queue-name", _SB_QUEUE,
            ])
        assert exc_info.value.code == 2

    def test_invoke_without_graph_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "invoke", _TASK_ID,
                "--sb-namespace-url", _SB_NAMESPACE,
                "--sb-queue-name", _SB_QUEUE,
            ])
        assert exc_info.value.code == 2

    def test_invoke_without_sb_namespace_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "invoke", _TASK_ID,
                "--graph", _GRAPH_ID,
                "--sb-queue-name", _SB_QUEUE,
            ])
        assert exc_info.value.code == 2

    def test_invoke_without_sb_queue_is_error(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([
                *_GLOBAL_OPTS,
                "invoke", _TASK_ID,
                "--graph", _GRAPH_ID,
                "--sb-namespace-url", _SB_NAMESPACE,
            ])
        assert exc_info.value.code == 2
