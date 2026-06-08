#!/bin/bash
set -euo pipefail

# MinicloudManager in tester.py handles all lifecycle management:
# - Preflight checks (/dev/kvm, docker available, AWS creds)
# - Container start (minicloud-setup.sh runs inside the container)
# - Health checks and wait loops
# - Region preparation
# - Cleanup on exit
#
# This script only sets test-specific environment variable overrides.

export MINICLOUD_DOCKER="${MINICLOUD_DOCKER:-scylladb/minicloud:dev}"
export MINICLOUD_GCS_BUCKET="${MINICLOUD_GCS_BUCKET:-}"
export SCT_CLUSTER_BACKEND=gce
export SCT_REGION_NAME="${SCT_REGION_NAME:-us-east1}"
export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_ENABLE_ARGUS=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-3}"
export SCT_N_LOADERS="${SCT_N_LOADERS:-1}"
export SCT_N_MONITOR_NODES="${SCT_N_MONITOR_NODES:-1}"
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend gce \
    --config test-cases/minicloud-provision-test.yaml
