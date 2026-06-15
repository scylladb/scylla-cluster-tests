#!/bin/bash
set -euo pipefail

# Run the minicloud artifact test inside hydra (Docker container with QEMU/KVM).
#
# Usage:
#   MINICLOUD_BINARY=/path/to/minicloud bash scripts/run-minicloud-artifact-test-hydra.sh
#
# The minicloud binary must be inside the SCT tree (it gets mounted into hydra).
# /dev/kvm must be available on the host.
#
# Optional env vars:
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0 (default)
#   SCT_AMI_ID_DB_SCYLLA    - specific AMI to test
#   MINICLOUD_BINARY        - path to minicloud binary (default: ./minicloud)
#   MINICLOUD_SETUP_SCRIPT  - path to minicloud-setup.sh

MINICLOUD_BINARY="${MINICLOUD_BINARY:-./minicloud}"

if [[ ! -f "$MINICLOUD_BINARY" ]]; then
    echo "ERROR: minicloud binary not found at: $MINICLOUD_BINARY"
    echo "Place the minicloud binary inside the SCT directory (it gets mounted into hydra)."
    echo "Set MINICLOUD_BINARY=/path/to/binary if it's elsewhere in the SCT tree."
    exit 1
fi

if [[ ! -e /dev/kvm ]]; then
    echo "ERROR: /dev/kvm not available on host. KVM is required for minicloud."
    exit 1
fi

export SCT_SCYLLA_VERSION="${SCT_SCYLLA_VERSION:-2025.3.0}"
export SCT_USE_MGMT=false
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:5000"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_INSTANCE_TYPE_DB="${SCT_INSTANCE_TYPE_DB:-i4i.large}"
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

export MINICLOUD_BINARY
export MINICLOUD_SETUP_SCRIPT="${MINICLOUD_SETUP_SCRIPT:-}"

echo ""
echo "======================================="
echo "  Minicloud Artifact Test (via Hydra)"
echo "  Version  : ${SCT_SCYLLA_VERSION}"
echo "  Instance : ${SCT_INSTANCE_TYPE_DB}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Binary   : ${MINICLOUD_BINARY}"
echo "======================================="
echo ""

./docker/env/hydra.sh run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend aws \
    --config test-cases/artifacts/ami.yaml
