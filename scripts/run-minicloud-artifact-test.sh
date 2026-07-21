#!/bin/bash
set -euo pipefail

# Run the SCT artifact test (artifacts_test.ArtifactsTest.test_scylla_service) via minicloud.
#
# Minicloud runs as a Docker container (ghcr.io/scylladb/minicloud:0.1.0).
# The MinicloudManager in tester.py auto-starts it when SCT_MINICLOUD_ENDPOINT_URL is set,
# but this script can also pre-start it via scripts/start-minicloud.sh for explicit control.
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
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0  (OR SCT_AMI_ID_DB_SCYLLA for specific AMI)
#
# Optional env vars:
#   SCT_AMI_ID_DB_SCYLLA    - specific AMI to test (e.g. ami-0abc1234)
#   SCT_REGION_NAME         - AWS region (default: eu-west-1)
#   SCT_N_DB_NODES          - override DB node count (default: 1)
#   MINICLOUD_DOCKER        - Docker image (default: ghcr.io/scylladb/minicloud:0.1.0)
#   MINICLOUD_PORT          - API port (default: 5000)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINICLOUD_PORT="${MINICLOUD_PORT:-5000}"

export SCT_USE_MGMT=false
export SCT_ENABLE_ARGUS=false
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0

# Small Scylla memory footprint — minicloud VMs are constrained
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

# KMS disabled — minicloud has no KMS endpoint
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false

export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:${MINICLOUD_PORT}}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_INSTANCE_TYPE_DB="${SCT_INSTANCE_TYPE_DB:-i4i.large}"

# Scylla version or AMI ID — one must be set
if [[ -z "${SCT_AMI_ID_DB_SCYLLA:-}" && -z "${SCT_SCYLLA_VERSION:-}" ]]; then
    echo "ERROR: set SCT_SCYLLA_VERSION=x.y.z  or  SCT_AMI_ID_DB_SCYLLA=ami-xxxx"
    exit 1
fi

# Start minicloud container if not already running
if ! curl -sf "http://localhost:${MINICLOUD_PORT}" -d "Action=DescribeRegions&Version=2016-11-15" >/dev/null 2>&1; then
    echo "Minicloud not running — starting via scripts/start-minicloud.sh..."
    bash "${SCRIPT_DIR}/start-minicloud.sh"
fi

echo ""
echo "======================================="
echo "  Minicloud Artifact Test"
echo "  Version  : ${SCT_SCYLLA_VERSION:-from AMI ${SCT_AMI_ID_DB_SCYLLA:-}}"
echo "  Instance : ${SCT_INSTANCE_TYPE_DB}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Endpoint : ${SCT_MINICLOUD_ENDPOINT_URL}"
echo "======================================="
echo ""

uv run sct.py run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend aws \
    --config test-cases/artifacts/ami.yaml
