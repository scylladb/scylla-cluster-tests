#!/bin/bash
set -euo pipefail

# MinicloudManager in tester.py handles all lifecycle management:
# - Preflight checks (/dev/kvm, AWS creds, binary availability)
# - minicloud-setup.sh execution
# - minicloud process start and monitoring
# - Health checks and wait loops
# - Region preparation
# - Cleanup on exit
#
# This script only sets test-specific environment variable overrides.

export MINICLOUD_BINARY="${MINICLOUD_BINARY:-/usr/local/bin/minicloud}"
export MINICLOUD_SETUP_SCRIPT="${MINICLOUD_SETUP_SCRIPT:-}"
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS="${SCT_N_LOADERS:-1}"
export SCT_N_MONITOR_NODES="${SCT_N_MONITOR_NODES:-1}"
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend aws \
    --config test-cases/minicloud-provision-test.yaml
