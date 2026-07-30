#!/bin/bash
set -euo pipefail

# Run the GCE artifact test inside hydra (SCT's Docker environment) via minicloud.
#
# Minicloud runs as a sibling Docker container (ghcr.io/scylladb/minicloud:master-4bd3fb6)
# on the host network. Hydra connects to it via localhost:5000.
#
# Prerequisites:
#   - /dev/kvm available on host
#   - Docker running
#   - GCP credentials configured (for GCE API passthrough)
#
# Optional env vars:
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0 (default)
#   SCT_GCE_IMAGE_DB        - specific GCE image to test
#   SCT_REGION_NAME         - GCE region (default: us-east1)
#   MINICLOUD_DOCKER        - Docker image (default: ghcr.io/scylladb/minicloud:master-4bd3fb6)
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

# On-demand only — minicloud does not implement preemptible/spot instances.
# test-cases/artifacts/gce-image.yaml requests spot, and MinicloudManager's
# SCT_INSTANCE_PROVISION override is applied too late to affect config parsing,
# so it has to be set here, before sct.py builds SCTConfiguration.
export SCT_INSTANCE_PROVISION=on_demand
export SCT_MINICLOUD_ENDPOINT_URL="http://localhost:${MINICLOUD_PORT}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-us-east1}"
export SCT_N_DB_NODES="${SCT_N_DB_NODES:-1}"
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

# minicloud needs the GCP service account key mounted at container start — it cannot be
# added to an already-running container. Without it minicloud's gcp_auth falls back to the
# GCE metadata service (absent in the container) and every GCP call fails to authenticate.
export SCT_CLUSTER_BACKEND=gce
GCS_KEY_FILE="${GCS_KEY_FILE:-${GOOGLE_APPLICATION_CREDENTIALS:-${HOME}/.cache/minicloud/gcp-credentials.json}}"
export GCS_KEY_FILE

# Start minicloud container if not already running
if ! curl -sf "http://localhost:${MINICLOUD_PORT}" -d "Action=DescribeVpcs&Version=2016-11-15" >/dev/null 2>&1; then
    echo "Minicloud not running — starting via scripts/start-minicloud.sh..."
    bash "${SCRIPT_DIR}/start-minicloud.sh"
fi

echo ""
echo "======================================="
echo "  Minicloud GCE Artifact Test (via Hydra)"
echo "  Version  : ${SCT_SCYLLA_VERSION}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Endpoint : ${SCT_MINICLOUD_ENDPOINT_URL}"
echo "======================================="
echo ""

./docker/env/hydra.sh run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend gce \
    --config test-cases/artifacts/gce-image.yaml
