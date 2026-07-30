#!/bin/bash
set -euo pipefail

# Run the GCE artifact test (artifacts_test.ArtifactsTest.test_scylla_service) via minicloud.
#
# Minicloud runs as a Docker container (ghcr.io/scylladb/minicloud:master-4bd3fb6).
# The MinicloudManager in tester.py auto-starts it when SCT_MINICLOUD_ENDPOINT_URL is set,
# but this script can also pre-start it via scripts/start-minicloud.sh for explicit control.
#
# What it tests:
#   - Scylla service starts on GCE image, responds to CQL, nodetool status
#   - stop/start and restart cycles
#   - snitch, node health, node_exporter liveness
#   - GCE user verification
#   - housekeeping DB version reporting
#   - perftune output (if use_preinstalled_scylla=true)
#   - time sync service presence
#
# Required env vars:
#   SCT_SCYLLA_VERSION      - e.g. 2025.3.0  (OR SCT_GCE_IMAGE_DB for specific image)
#
# Optional env vars:
#   SCT_GCE_IMAGE_DB        - specific GCE image to test
#   SCT_REGION_NAME         - GCE region (default: us-east1)
#   SCT_N_DB_NODES          - override DB node count (default: 1)
#   MINICLOUD_DOCKER        - Docker image (default: ghcr.io/scylladb/minicloud:master-4bd3fb6)
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

# On-demand only — minicloud does not implement preemptible/spot instances.
# test-cases/artifacts/gce-image.yaml requests spot, and MinicloudManager's
# SCT_INSTANCE_PROVISION override is applied too late to affect config parsing,
# so it has to be set here, before sct.py builds SCTConfiguration.
export SCT_INSTANCE_PROVISION=on_demand

export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:${MINICLOUD_PORT}}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-us-east1}"

# minicloud needs the GCP service account key mounted at container start — it cannot be
# added to an already-running container. Without it minicloud's gcp_auth falls back to the
# GCE metadata service (absent in the container) and every GCP call fails to authenticate.
export SCT_CLUSTER_BACKEND=gce
GCS_KEY_FILE="${GCS_KEY_FILE:-${GOOGLE_APPLICATION_CREDENTIALS:-${HOME}/.cache/minicloud/gcp-credentials.json}}"
export GCS_KEY_FILE

# Scylla version or GCE image — one must be set
if [[ -z "${SCT_GCE_IMAGE_DB:-}" && -z "${SCT_SCYLLA_VERSION:-}" ]]; then
    echo "ERROR: set SCT_SCYLLA_VERSION=x.y.z  or  SCT_GCE_IMAGE_DB=<image-name>"
    exit 1
fi

# Start minicloud container if not already running
if ! curl -sf "http://localhost:${MINICLOUD_PORT}" -d "Action=DescribeVpcs&Version=2016-11-15" >/dev/null 2>&1; then
    echo "Minicloud not running — starting via scripts/start-minicloud.sh..."
    bash "${SCRIPT_DIR}/start-minicloud.sh"
fi

echo ""
echo "======================================="
echo "  Minicloud GCE Artifact Test"
echo "  Version  : ${SCT_SCYLLA_VERSION:-from image ${SCT_GCE_IMAGE_DB:-}}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "  Endpoint : ${SCT_MINICLOUD_ENDPOINT_URL}"
echo "======================================="
echo ""

uv run sct.py run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend gce \
    --config test-cases/artifacts/gce-image.yaml
