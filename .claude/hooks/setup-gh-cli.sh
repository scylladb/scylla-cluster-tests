#!/usr/bin/env bash
# SessionStart hook: install and configure gh CLI for Claude Code web sessions.
# stdout is injected into Claude's context; stderr is visible to the user.
set -euo pipefail

GH_VERSION="2.87.3"

# Skip if gh is already installed
if command -v gh &>/dev/null; then
    echo "gh CLI is available: $(gh --version | head -1)"
    exit 0
fi

# Install from GitHub releases (works through egress proxies that allow github.com)
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
esac

TARBALL="gh_${GH_VERSION}_linux_${ARCH}.tar.gz"
URL="https://github.com/cli/cli/releases/download/v${GH_VERSION}/${TARBALL}"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

if curl -fsSL "$URL" -o "$TMP_DIR/gh.tar.gz" 2>/dev/null; then
    tar -xzf "$TMP_DIR/gh.tar.gz" -C "$TMP_DIR"
    # Try /usr/local/bin first (needs sudo), fall back to ~/.local/bin
    if sudo cp "$TMP_DIR/gh_${GH_VERSION}_linux_${ARCH}/bin/gh" /usr/local/bin/gh 2>/dev/null; then
        true
    else
        mkdir -p "$HOME/.local/bin"
        cp "$TMP_DIR/gh_${GH_VERSION}_linux_${ARCH}/bin/gh" "$HOME/.local/bin/gh"
        export PATH="$HOME/.local/bin:$PATH"
    fi
fi

if command -v gh &>/dev/null; then
    echo "gh CLI installed: $(gh --version | head -1)"
else
    echo "WARNING: gh CLI could not be installed."
fi
