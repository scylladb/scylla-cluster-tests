#!/bin/bash
set -euo pipefail

# Test the provision-resources step locally with minicloud (AWS backend).
#
# This exercises the same flow as CI Stage 6 ("Provision Resources"):
#   hydra --execute-on-runner <IP> provision-resources -b aws -t <test_name>
#
# Expected behavior:
#   1. MinicloudManager detects SCT_MINICLOUD_ENDPOINT_URL and starts the container
#   2. AWS_ENDPOINT_URL is set to http://localhost:5000
#   3. SCTProvisionLayout creates instances via minicloud API
#   4. Instances reach "running" state (QEMU VMs boot)
#   5. minicloud container stays alive (keep_alive) for the subsequent run-test stage
#
# After this succeeds, run the full test with:
#   scripts/run-minicloud-test.sh -f provision

# Only on failure: on success the container must survive (keep_alive) so the follow-up
# run-test stage reaches the same live endpoint, exactly as the CI stages do.
cleanup_on_failure() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        echo "Provisioning failed (exit $exit_code) — stopping minicloud container..."
        docker rm -f minicloud 2>/dev/null || true
    else
        echo "minicloud left running for the next stage; stop it with: docker rm -f minicloud"
    fi
}
trap cleanup_on_failure EXIT

export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"
export SCT_CLUSTER_BACKEND=aws
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
# region narrowing (if wanted) goes via SCT_MINICLOUD_REGIONS
export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS="${SCT_N_LOADERS:-1}"
export SCT_N_MONITOR_NODES="${SCT_N_MONITOR_NODES:-1}"
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"


export SCT_TEST_ID="${SCT_TEST_ID:-$(python3 -c 'import uuid; print(uuid.uuid4())')}"
echo "Using test_id: ${SCT_TEST_ID}"

docker rm -f minicloud 2>/dev/null || true

uv run sct.py start-minicloud \
    -b aws \
    --config test-cases/minicloud-provision-test.yaml \
    --config configurations/minicloud.yaml

uv run sct.py provision-resources \
    -b aws \
    -t longevity_test.LongevityTest.test_custom_time \
    --config test-cases/minicloud-provision-test.yaml \
    --config configurations/minicloud.yaml
