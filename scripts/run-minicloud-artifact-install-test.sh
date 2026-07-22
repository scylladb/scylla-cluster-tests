#!/bin/bash
set -euo pipefail

# Run the SCT artifact install test (clean distro + Scylla repo install) via minicloud.
#
# What it tests:
#   - Clean Ubuntu 26.04 base image provisions correctly
#   - Scylla installs from deb repository (master/branch)
#   - Scylla service starts, responds to CQL, nodetool status
#   - stop/start and restart cycles
#   - node_exporter, time sync, perftune
#
# Required env vars:
#   MINICLOUD_DOCKER        - Docker image for minicloud (e.g. ghcr.io/scylladb/minicloud:0.1.0)
#   SCT_SCYLLA_REPO         - deb repo URL (e.g. https://downloads.scylladb.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list)
#
# Optional env vars:
#   SCT_REGION_NAME         - GCE region (default: us-east1)
#   MINICLOUD_KEEP_ALIVE    - set to 1 to keep cluster running after test

export MINICLOUD_DOCKER="${MINICLOUD_DOCKER:?ERROR: MINICLOUD_DOCKER not set. Set to minicloud image (e.g. ghcr.io/scylladb/minicloud:0.1.0)}"

# Repo URL is required for install-from-repo tests
if [[ -z "${SCT_SCYLLA_REPO:-}" ]]; then
    echo "ERROR: SCT_SCYLLA_REPO must be set to a deb repo URL"
    echo "  Example: export SCT_SCYLLA_REPO='https://downloads.scylladb.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list'"
    exit 1
fi

export SCT_USE_MGMT=false
export SCT_ENABLE_ARGUS=false
export SCT_N_DB_NODES=1
export SCT_N_LOADERS=0
export SCT_N_MONITOR_NODES=0

# Small Scylla memory footprint — minicloud VMs are constrained
export SCT_APPEND_SCYLLA_ARGS="${SCT_APPEND_SCYLLA_ARGS:---memory 256M}"

# KMS disabled — minicloud has no KMS endpoint
export SCT_ENTERPRISE_DISABLE_KMS=true
export SCT_ENABLE_KMS_KEY_ROTATION=false

# GCE minicloud settings
export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:5000}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-us-east1}"

# Distro install settings — match test-cases/artifacts/ubuntu2604.yaml
export SCT_SCYLLA_LINUX_DISTRO="ubuntu-resolute"
export SCT_USE_PREINSTALLED_SCYLLA=false
export SCT_GCE_IMAGE_USERNAME="ubuntu"

echo ""
echo "======================================="
echo "  Minicloud Artifact Install Test"
echo "  Distro   : Ubuntu 26.04 (resolute)"
echo "  Repo     : ${SCT_SCYLLA_REPO}"
echo "  Region   : ${SCT_REGION_NAME}"
echo "======================================="
echo ""

uv run sct.py run-test artifacts_test.ArtifactsTest.test_scylla_service \
    --backend gce \
    --config test-cases/artifacts/ubuntu2604.yaml
