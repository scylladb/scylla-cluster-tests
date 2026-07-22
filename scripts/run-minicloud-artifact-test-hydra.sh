#!/bin/bash
set -euo pipefail

# Run the minicloud artifact test inside hydra (SCT's Docker environment).
#
# Minicloud runs as a sibling Docker container (ghcr.io/scylladb/minicloud:0.1.0)
# on the host network. Hydra connects to it via localhost:5000.
#
# Prerequisites:
#   - /dev/kvm available on host
#   - Docker running
#   - AWS credentials configured (for S3 passthrough: AMI downloads, keystore)
#
# Optional env vars:
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0 (default)
#   SCT_AMI_ID_DB_SCYLLA    - specific AMI to test
#   SCT_REGION_NAME         - AWS region (default: eu-west-1)
#   MINICLOUD_DOCKER        - Docker image (default: ghcr.io/scylladb/minicloud:0.1.0)
#   MINICLOUD_PORT          - API port (default: 5000)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINICLOUD_PORT="${MINICLOUD_PORT:-5000}"

if [[ ! -e /dev/kvm ]]; then
    echo "ERROR: /dev/kvm not available on host. KVM is required for minicloud."
    exit 1
fi

export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_ENABLE_ARGUS=false
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:${MINICLOUD_PORT}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_INSTANCE_TYPE_DB="${SCT_INSTANCE_TYPE_DB:-i4i.large}"
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

# Start minicloud container if not already running
if ! curl -sf "http://localhost:${MINICLOUD_PORT}" -d "Action=DescribeRegions&Version=2016-11-15" >/dev/null 2>&1; then
    echo "Minicloud not running — starting via scripts/start-minicloud.sh..."
    bash "${SCRIPT_DIR}/start-minicloud.sh"
fi

echo ""
echo "======================================="
echo "  Minicloud Artifact Test (via Hydra)"
echo "  Version  : ${SCT_SCYLLA_VERSION}"
echo "  Instance : ${SCT_INSTANCE_TYPE_DB}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Endpoint : ${SCT_MINICLOUD_ENDPOINT_URL}"
echo "======================================="
echo ""

./docker/env/hydra.sh run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend aws \
    --config test-cases/artifacts/ami.yaml
