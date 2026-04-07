#!/usr/bin/env bash
# Install development dependencies for SentinelBudget.

set -euo pipefail

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is not installed. Install from https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi

uv sync --all-groups

echo "Dependencies synced."
echo "Next: copy .env.example to .env, set values, then run preflight."
