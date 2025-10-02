# just manual: https://github.com/casey/just#readme

_default:
    just --list

# Install dependencies used by this project
bootstrap default="3.12":
    uv venv --python {{default}}
    just sync

# Sync dependencies with environment
sync:
    uv sync

# Build the project as a package (uv build)
build *args:
    uv build

# Run the code formatter
format:
    uv run ruff format boilermaker tests

# Run code quality checks
check:
    #!/bin/bash -eux
    uv run ruff check boilermaker tests
    just check-types

# Run mypy checks
check-types:
    #!/bin/bash -eux
    uv run mypy boilermaker

# Run all tests locally
test *args:
    #!/bin/bash -eux
    uv run pytest --import-mode importlib {{args}}

# Run the project tests for CI environment (e.g. with code coverage)
ci-test coverage_dir='./coverage':
    uv run pytest --import-mode importlib --cov=boilermaker --cov-report xml --junitxml=./coverage/unittest.junit.xml

# Build documentation locally
docs-build *args:
    uv run mkdocs build {{args}}

# Serve documentation locally with auto-reload
docs-serve:
    uv run mkdocs serve
