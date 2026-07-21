#!/bin/bash
set -euo pipefail

# Test the provision-resources step locally with minicloud (AWS backend).
#
# This exercises the same flow as CI Stage 6 ("Provision Resources"):
#   hydra --execute-on-runner <IP> provision-resources -b aws -t <test_name>
#
# Expected behavior:
#   1. MinicloudManager detects MINICLOUD_DOCKER and starts the container
#   2. AWS_ENDPOINT_URL is set to http://localhost:5000
#   3. SCTProvisionLayout creates instances via minicloud API
#   4. Instances reach "running" state (QEMU VMs boot)
#   5. minicloud container stays alive (keep_alive) for the subsequent run-test stage
#
# After this succeeds, run the full test with:
#   scripts/run-minicloud-provision-test.sh

cleanup() {
    echo "Stopping minicloud container..."
    docker rm -f minicloud 2>/dev/null || true
}
trap cleanup EXIT

export MINICLOUD_DOCKER="${MINICLOUD_DOCKER:-ghcr.io/scylladb/minicloud:0.1.0}"
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"
export SCT_CLUSTER_BACKEND=aws
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export MINICLOUD_AWS_REGION="${SCT_REGION_NAME}"
export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS="${SCT_N_LOADERS:-1}"
export SCT_N_MONITOR_NODES="${SCT_N_MONITOR_NODES:-1}"
export SCT_INSTANCE_PROVISION=on_demand
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false

export SCT_TEST_ID="${SCT_TEST_ID:-$(python3 -c 'import uuid; print(uuid.uuid4())')}"
echo "Using test_id: ${SCT_TEST_ID}"

docker rm -f minicloud 2>/dev/null || true

uv run sct.py start-minicloud \
    -b aws \
    --config test-cases/minicloud-provision-test.yaml

uv run sct.py provision-resources \
    -b aws \
    -t longevity_test.LongevityTest.test_custom_time \
    --config test-cases/minicloud-provision-test.yaml
