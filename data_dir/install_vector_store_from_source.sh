#!/bin/bash

# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Install vector-store from source on a host machine.
# Designed to run on a provisioned VS node (base VS AMI or plain Ubuntu) during SCT test setup.
#
# Usage:
#   install_vector_store_from_source.sh [--repo <url>] [--ref <branch|tag|sha>] \
#                                       [--user <name>] [--install-dir <path>] [--verbose]
#
# This script:
#   1. Installs build prerequisites (Rust toolchain, system libs)
#   2. Shallow-fetches the requested ref
#   3. Builds the native vector-store binary (release mode)
#   4. Installs the binary into the install dir
#   5. Installs/refreshes the systemd service unit
#
# It does NOT start the service -- SCT handles that after writing .env config.
#
# This copy is vendored into SCT rather than fetched from the commit being built, so that any
# vector-store ref can be built -- including ones predating the script. Mirror changes to
# scylladb/vector-store (scripts/install-from-source) for people building outside SCT.

set -euo pipefail

# Defaults. Note SERVICE_USER rather than USER: overwriting the well known variable would leak
# into every command below through the environment.
REPO="https://github.com/scylladb/vector-store.git"
REF="master"
SERVICE_USER="ubuntu"
INSTALL_DIR=""
VERBOSE=false

usage() {
    echo "Usage: $0 [--repo <url>] [--ref <branch|tag|sha>] [--user <name>] [--install-dir <path>] [--verbose]"
    exit 1
}

log() {
    echo "[install-vector-store-from-source] $*"
}

# 'set -u' would turn a missing value into a bare "$2: unbound variable"; say what is wrong instead.
require_value() {
    [[ $# -ge 2 && -n "$2" ]] || { echo "Missing value for $1"; usage; }
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo) require_value "$@"; REPO="$2"; shift 2 ;;
        --ref) require_value "$@"; REF="$2"; shift 2 ;;
        --user) require_value "$@"; SERVICE_USER="$2"; shift 2 ;;
        --install-dir) require_value "$@"; INSTALL_DIR="$2"; shift 2 ;;
        --verbose) VERBOSE=true; shift ;;
        *) usage ;;
    esac
done

# Derived from SERVICE_USER unless given explicitly, so the two can never disagree.
INSTALL_DIR="${INSTALL_DIR:-/home/${SERVICE_USER}/vector-store}"
SERVICE_FILE="/etc/systemd/system/vector-store.service"
CLONE_DIR="/home/${SERVICE_USER}/vector-store-src"

if [[ "$VERBOSE" == "true" ]]; then
    set -x
fi

log "repo=${REPO} ref=${REF} user=${SERVICE_USER} install_dir=${INSTALL_DIR}"

# --- Step 1: Install build prerequisites ---
log "Installing build prerequisites..."

export DEBIAN_FRONTEND=noninteractive
# Retries matter here: this runs on a freshly booted node, where apt mirrors are a common
# source of transient failures that would otherwise fail the whole test run.
APT_OPTS=(-y -o Acquire::Retries=3 -o DPkg::Lock::Timeout=300)
apt-get "${APT_OPTS[@]}" update
apt-get "${APT_OPTS[@]}" install \
    build-essential \
    pkg-config \
    libssl-dev \
    git \
    curl \
    protobuf-compiler

# Install Rust toolchain for the user
# Retries for the same reason as the apt retries above: a freshly booted node hits transient
# network failures often enough that an unretried fetch is a recurring source of failed runs. Plain
# '--retry' is not enough for that, since it does not cover the connection resets (curl 35/56) a
# fresh node sees -- hence '--retry-all-errors', probed for at runtime the way
# 'sdcm.utils.curl.RETRY_ALL_ERRORS_PROBE' does it, because the curl shipped by older distros
# hard-fails on the flag. Timeouts and counts are that helper's defaults.
log "Installing Rust toolchain..."
if [[ ! -d "/home/${SERVICE_USER}/.rustup" ]]; then
    sudo -u "$SERVICE_USER" bash -c 'curl --proto "=https" --tlsv1.2 -sSf \
        --retry 5 --retry-delay 3 --retry-max-time 300 --connect-timeout 10 \
        $(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors) \
        https://sh.rustup.rs | sh -s -- -y --default-toolchain none'
fi

# --- Step 2: Fetch the requested ref ---
# A shallow fetch of the single ref instead of 'git clone' of the whole repo: the history is
# not needed to build one commit, and vector-store's history is large enough for the
# difference to show up in every provisioning run. Works for branches, tags and SHAs.
log "Fetching ${REF}..."

rm -rf "$CLONE_DIR"
sudo -u "$SERVICE_USER" mkdir -p "$CLONE_DIR"
sudo -u "$SERVICE_USER" git -C "$CLONE_DIR" init -q
sudo -u "$SERVICE_USER" git -C "$CLONE_DIR" remote add origin "$REPO"
sudo -u "$SERVICE_USER" git -C "$CLONE_DIR" fetch --depth 1 origin "$REF"
sudo -u "$SERVICE_USER" git -C "$CLONE_DIR" checkout -q FETCH_HEAD

# Resolve to a concrete SHA, so what actually got built is recorded even when REF is a branch.
BUILT_SHA=$(sudo -u "$SERVICE_USER" git -C "$CLONE_DIR" rev-parse HEAD)
log "Building ${BUILT_SHA} (${REF})"

# Install the exact toolchain version specified in the repo.
# The file may mention 'channel' more than once (a comment, a commented out alternative), so match
# the assignment at the start of a line rather than the word: '-m1' alone would just as happily pick
# up the commented out one if it comes first.
RUST_CHANNEL=$(grep -m1 '^channel *=' "${CLONE_DIR}/rust-toolchain.toml" 2>/dev/null | cut -d '"' -f 2 || echo "stable")
log "Installing Rust toolchain: ${RUST_CHANNEL}"
sudo -u "$SERVICE_USER" bash -c "source /home/${SERVICE_USER}/.cargo/env && rustup install ${RUST_CHANNEL} && rustup default ${RUST_CHANNEL}"

# --- Step 3: Build native binary ---
log "Building vector-store (release mode)..."

sudo -u "$SERVICE_USER" bash -c "source /home/${SERVICE_USER}/.cargo/env && cd ${CLONE_DIR} && cargo build --release --bin vector-store"

# --- Step 4: Install binary ---
log "Installing binary to ${INSTALL_DIR}..."

mkdir -p "$INSTALL_DIR"
cp "${CLONE_DIR}/target/release/vector-store" "${INSTALL_DIR}/vector-store"
chmod +x "${INSTALL_DIR}/vector-store"

# Record what was built so the test run can report it -- read back by
# VectorStoreAWSNode.get_vector_store_source_build_info().
printf '%s %s\n' "$BUILT_SHA" "$REF" > "${INSTALL_DIR}/.source-commit"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "$INSTALL_DIR"

# --- Step 5: Install/refresh systemd service ---
# Kept byte-identical to the unit baked into the VS AMI by packer
# (packer/files/vector-store.service), so an ami-mode and a source-mode node run the service
# the same way. EnvironmentFile makes the contract with the .env that SCT writes explicit --
# vector-store would also pick it up from WorkingDirectory, but then nothing here says so.
log "Installing vector-store.service..."

cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Vector Rust Service
After=network.target

[Service]
User=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}/
ExecStart=${INSTALL_DIR}/vector-store
EnvironmentFile=-${INSTALL_DIR}/.env
Restart=on-failure
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
EOF

cp "$SERVICE_FILE" /usr/lib/systemd/system/vector-store.service 2>/dev/null || true
systemctl daemon-reload

# --- Cleanup ---
log "Cleaning up source directory..."
rm -rf "$CLONE_DIR"

log "Done. vector-store installed from ${BUILT_SHA} (${REF})"
