#!/usr/bin/env pwsh
# Install development dependencies for SentinelBudget.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Error "uv is not installed. Install from https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
}

uv sync --all-groups

Write-Host "Dependencies synced."
Write-Host "Next: copy .env.example to .env, set values, then run preflight."
