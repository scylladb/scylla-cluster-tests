#!/bin/bash
# Full minicloud CI pipeline — local execution via sct.py
# Mirrors minicloudPipeline.groovy stages using direct sct.py calls.
#
# Run: bash scripts/test-minicloud-local-pipeline.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TEST_NAME="longevity_test.LongevityTest.test_custom_time"
TEST_CONFIG="test-cases/minicloud-provision-test.yaml"
BACKEND="aws"

export SCT_TEST_ID="${SCT_TEST_ID:-$(uuidgen)}"
export SCT_CLUSTER_BACKEND="$BACKEND"
export SCT_CONFIG_FILES="[\"$TEST_CONFIG\"]"
export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_REGION_NAME="${SCT_REGION_NAME:-us-east-1}"
export SCT_IP_SSH_CONNECTIONS="private"
export SCT_INSTANCE_PROVISION="on_demand"
export SCT_ENTERPRISE_DISABLE_KMS="true"
export SCT_ENABLE_KMS_KEY_ROTATION="false"
export SCT_FORCE_RUN_IOTUNE="false"
export SCT_COLLECT_LOGS="false"
export SCT_ENABLE_ARGUS="false"
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"

STAGE_RESULTS=()
FAILED=0

run_stage() {
    local stage_name="$1"
    shift
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  STAGE: $stage_name"
    echo "════════════════════════════════════════════════════════════════"
    echo "  CMD: $*"
    echo ""

    if "$@"; then
        STAGE_RESULTS+=("✅ $stage_name")
        echo ""
        echo "  ✅ $stage_name PASSED"
        return 0
    else
        local rc=$?
        STAGE_RESULTS+=("❌ $stage_name (exit $rc)")
        FAILED=$((FAILED + 1))
        echo ""
        echo "  ❌ $stage_name FAILED (exit $rc)"
        return $rc
    fi
}

cd "$SCT_DIR"

run_stage "Start Minicloud" \
    uv run sct.py start-minicloud -b "$BACKEND" -c "$TEST_CONFIG"

run_stage "Provision Resources" \
    uv run sct.py provision-resources -b "$BACKEND" -t "$TEST_NAME" -c "$TEST_CONFIG"

run_stage "Run Test" \
    uv run sct.py run-test "$TEST_NAME" --backend "$BACKEND" --config "$TEST_CONFIG"

run_stage "Collect Logs" \
    uv run sct.py collect-logs --backend "$BACKEND" --test-id "$SCT_TEST_ID" || true

run_stage "Clean Resources" \
    uv run sct.py clean-resources --post-behavior -b "$BACKEND" --test-id "$SCT_TEST_ID" || true

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  PIPELINE SUMMARY"
echo "════════════════════════════════════════════════════════════════"
for result in "${STAGE_RESULTS[@]}"; do
    echo "  $result"
done
echo ""

if [ "$FAILED" -gt 0 ]; then
    echo "  $FAILED stage(s) failed"
    exit 1
else
    echo "  All stages passed"
fi
