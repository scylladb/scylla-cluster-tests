#!/bin/bash
set -euo pipefail

# Run the minicloud artifact test inside hydra.
# Minicloud runs in its own container (started via scripts/start-minicloud.sh).
# /dev/kvm must be available on the host.
#
# Optional env vars:
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0 (default)
#   SCT_AMI_ID_DB_SCYLLA    - specific AMI to test
#   MINICLOUD_DOCKER        - minicloud Docker image (default: scylladb/minicloud:dev)

MINICLOUD_DOCKER="${MINICLOUD_DOCKER:-scylladb/minicloud:dev}"

if [[ ! -e /dev/kvm ]]; then
    echo "ERROR: /dev/kvm not available on host. KVM is required for minicloud."
    exit 1
fi

# Start minicloud container (idempotent — reuses if already running)
export MINICLOUD_DOCKER
bash scripts/start-minicloud.sh

export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_ENABLE_ARGUS=false
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_INSTANCE_TYPE_DB="${SCT_INSTANCE_TYPE_DB:-i4i.large}"
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

echo ""
echo "======================================="
echo "  Minicloud Artifact Test (via Hydra)"
echo "  Version  : ${SCT_SCYLLA_VERSION}"
echo "  Instance : ${SCT_INSTANCE_TYPE_DB}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Image    : ${MINICLOUD_DOCKER}"
echo "======================================="
echo ""

./docker/env/hydra.sh run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend aws \
    --config test-cases/artifacts/ami.yaml
