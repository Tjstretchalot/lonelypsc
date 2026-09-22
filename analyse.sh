#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

uv run --project "$script_dir" --locked --all-extras --group dev \
    black --check "$script_dir/src/lonelypsc"
uv run --project "$script_dir" --locked --all-extras --group dev \
    ruff check "$script_dir/src"

(
    cd "$script_dir/src"
    uv run --project "$script_dir" --locked --all-extras --group dev \
        mypy lonelypsc --explicit-package-bases --disallow-untyped-defs
)
