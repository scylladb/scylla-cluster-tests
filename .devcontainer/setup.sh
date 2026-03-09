#!/usr/bin/env bash
# Dev container setup: install uv, sync dependencies, install pre-commit hooks.
# Used by GitHub Codespaces and Copilot Workspace via devcontainer.json postCreateCommand.
set -euo pipefail

# ── 1. Install uv ────────────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

echo "uv version: $(uv --version)"

# ── 2. Install managed Python ────────────────────────────────────────────
PYTHON_VERSION=$(cat .python-version 2>/dev/null || echo "3.14")
echo "Installing Python $PYTHON_VERSION..."
uv python install "$PYTHON_VERSION"

# ── 3. Sync project dependencies ─────────────────────────────────────────
export UV_PROJECT_ENVIRONMENT=.venv
echo "Syncing project dependencies..."
uv sync --all-groups

# ── 4. Install pre-commit git hooks ──────────────────────────────────────
echo "Installing pre-commit hooks..."
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg

echo "Setup complete: Python $PYTHON_VERSION, dependencies synced, pre-commit hooks installed."
