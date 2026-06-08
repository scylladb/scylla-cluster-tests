#!/bin/bash
set -euo pipefail

# Run the SCT artifact test (artifacts_test.ArtifactsTest.test_scylla_service) via minicloud.
#
# What it tests:
#   - ENA support (AWS)
#   - Scylla service starts, responds to CQL, nodetool status
#   - stop/start and restart cycles
#   - snitch, node health, node_exporter liveness
#   - housekeeping DB version reporting
#   - perftune output (if use_preinstalled_scylla=true)
#   - time sync service presence
#
# Required env vars:
#   MINICLOUD_BINARY        - path to minicloud binary
#   MINICLOUD_SETUP_SCRIPT  - path to minicloud-setup.sh
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0  (OR SCT_AMI_ID_DB_SCYLLA for specific AMI)
#
# Optional env vars:
#   SCT_AMI_ID_DB_SCYLLA    - specific AMI to test (e.g. ami-0abc1234)
#   SCT_REGION_NAME         - AWS region (default: eu-west-1)
#   MINICLOUD_KEEP_ALIVE    - set to 1 to keep cluster running after test
#   SCT_N_DB_NODES          - override DB node count (default: 1)

BACKEND="${1:-aws}"

export MINICLOUD_BINARY="${MINICLOUD_BINARY:?ERROR: MINICLOUD_BINARY not set}"
export MINICLOUD_SETUP_SCRIPT="${MINICLOUD_SETUP_SCRIPT:-}"

export SCT_USE_MGMT=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0

# Small Scylla memory footprint — minicloud VMs are constrained
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

# KMS disabled — minicloud has no KMS endpoint
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false

case "$BACKEND" in
    aws)
        export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:5000}"
        export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
        export SCT_INSTANCE_TYPE_DB="${SCT_INSTANCE_TYPE_DB:-i4i.large}"
        CONFIG="test-cases/artifacts/ami.yaml"
        ;;
    *)
        echo "Usage: $0 [aws]"
        echo "GCE support deferred until minicloud NVMe lands."
        exit 1
        ;;
esac

# Scylla version or AMI ID — one must be set
if [[ -z "${SCT_AMI_ID_DB_SCYLLA:-}" && -z "${SCT_SCYLLA_VERSION:-}" ]]; then
    echo "ERROR: set SCT_SCYLLA_VERSION=x.y.z  or  SCT_AMI_ID_DB_SCYLLA=ami-xxxx"
    exit 1
fi

echo ""
echo "======================================="
echo "  Minicloud Artifact Test"
echo "  Backend  : $BACKEND"
echo "  Version  : ${SCT_SCYLLA_VERSION:-from AMI ${SCT_AMI_ID_DB_SCYLLA:-}}"
echo "  Instance : ${SCT_INSTANCE_TYPE_DB}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "======================================="
echo ""

uv run sct.py run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend "$BACKEND" \
    --config "$CONFIG"
