#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

uv run --project "$script_dir" --locked --all-extras --group dev \
    ruff check --fix "$script_dir/src"
uv run --project "$script_dir" --locked --all-extras --group dev \
    black "$script_dir/src/lonelypsc"
