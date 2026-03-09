#!/usr/bin/env bash
# SessionStart hook: install Python via uv, sync dependencies, and install pre-commit hooks.
# stdout is injected into Claude's context; stderr is visible to the user.
set -euo pipefail

PROJECT_DIR="${CLAUDE_PROJECT_DIR:-.}"

# ── 1. Install managed Python and create venv ────────────────────────────
PYTHON_VERSION=$(cat "$PROJECT_DIR/.python-version" 2>/dev/null || echo "3.14")

if ! uv python list --only-installed 2>/dev/null | grep -q "$PYTHON_VERSION"; then
    echo "Installing Python $PYTHON_VERSION via uv..." >&2
    uv python install "$PYTHON_VERSION" >&2
fi

# ── 2. Sync project dependencies ─────────────────────────────────────────
# UV_PROJECT_ENVIRONMENT tells uv where to put the venv.
export UV_PROJECT_ENVIRONMENT="${PROJECT_DIR}/.venv"

if [ ! -d "$UV_PROJECT_ENVIRONMENT" ] || [ "$PROJECT_DIR/uv.lock" -nt "$UV_PROJECT_ENVIRONMENT/pyvenv.cfg" ]; then
    echo "Syncing project dependencies (uv sync --all-groups)..." >&2
    uv sync --all-groups --project "$PROJECT_DIR" >&2
else
    echo "Dependencies already up to date, skipping uv sync." >&2
fi

# ── 3. Install pre-commit git hooks ──────────────────────────────────────
if [ -d "$PROJECT_DIR/.git" ]; then
    # Check if pre-commit hook is already installed
    if [ ! -f "$PROJECT_DIR/.git/hooks/pre-commit" ] || ! grep -q "pre-commit" "$PROJECT_DIR/.git/hooks/pre-commit" 2>/dev/null; then
        echo "Installing pre-commit hooks..." >&2
        uv run --project "$PROJECT_DIR" pre-commit install >&2
        uv run --project "$PROJECT_DIR" pre-commit install --hook-type commit-msg >&2
    else
        echo "Pre-commit hooks already installed." >&2
    fi
fi

echo "Project initialized: Python $PYTHON_VERSION, dependencies synced, pre-commit hooks installed."
