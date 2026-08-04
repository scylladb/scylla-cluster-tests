#!/bin/bash
# One entry point for every local minicloud test flow.
#
# Usage:
#   scripts/run-minicloud-test.sh [-b aws|gce] [-f ami|repo|provision] [-m direct|hydra]
#
#   -b  backend to emulate                          (default: aws)
#   -f  flavor: ami       - image-based artifacts test (AMI / GCE image)
#               repo      - deb/rpm repo-install artifacts test
#               provision - provisioning smoke test (longevity + nemesis)
#                                                   (default: ami)
#   -m  direct: run sct.py from this checkout; hydra: run inside the hydra container
#                                                   (default: direct)
#
# Required env (per flavor):
#   SCT_SCYLLA_VERSION      - e.g. 2026.2  (or SCT_AMI_ID_DB_SCYLLA / SCT_GCE_IMAGE_DB for ami)
#   GCS_KEY_FILE / GOOGLE_APPLICATION_CREDENTIALS - GCE backend only (image export)
#
# Optional env:
#   SCT_TEST_CASE           - override the flavor's default test-case yaml
#   SCT_MINICLOUD_*         - any documented minicloud_* config option
#
# The container is started via `sct.py start-minicloud` (keep_alive), which owns
# preflight, host networking, region preparation and health checks.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

BACKEND="aws"
FLAVOR="ami"
MODE="direct"
while getopts "b:f:m:h" opt; do
    case "$opt" in
        b) BACKEND="$OPTARG" ;;
        f) FLAVOR="$OPTARG" ;;
        m) MODE="$OPTARG" ;;
        h) grep '^#' "$0" | head -25; exit 0 ;;
        *) exit 5 ;;
    esac
done

case "$FLAVOR" in
    ami)
        TEST="artifacts_test.ArtifactsTest.test_scylla_service"
        [[ "$BACKEND" == "gce" ]] && DEFAULT_CASE="test-cases/artifacts/gce-image.yaml" \
                                  || DEFAULT_CASE="test-cases/artifacts/ami.yaml"
        ;;
    repo)
        TEST="artifacts_test.ArtifactsTest.test_scylla_service"
        DEFAULT_CASE="test-cases/artifacts/ubuntu2604.yaml"
        ;;
    provision)
        TEST="longevity_test.LongevityTest.test_custom_time"
        # The minicloud-specific case, not test-cases/PR-provision-test.yaml: the generic one
        # keeps the gce_config.yaml default of 4 local SSDs, which the emulated GCE path
        # (qcow2-backed disks, no NVMe passthrough) cannot serve.
        DEFAULT_CASE="test-cases/minicloud-provision-test.yaml"
        ;;
    *) echo "ERROR: unknown flavor '$FLAVOR' (ami|repo|provision)" >&2; exit 5 ;;
esac
TEST_CASE="${SCT_TEST_CASE:-$DEFAULT_CASE}"

if [[ "$FLAVOR" != "provision" && -z "${SCT_SCYLLA_VERSION:-}" &&
      -z "${SCT_AMI_ID_DB_SCYLLA:-}" && -z "${SCT_GCE_IMAGE_DB:-}" ]]; then
    echo "ERROR: set SCT_SCYLLA_VERSION=x.y.z (or SCT_AMI_ID_DB_SCYLLA / SCT_GCE_IMAGE_DB)" >&2
    exit 5
fi

export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:5000}"
# AMIs live in eu-west-1 (minicloud validates uncached AMIs against its own region)
[[ "$BACKEND" == "aws" ]] && export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
# local-dev conveniences — CI sets its own
export SCT_USE_MGMT="${SCT_USE_MGMT:-false}"
export SCT_ENABLE_ARGUS="${SCT_ENABLE_ARGUS:-false}"

if [[ "$MODE" == "hydra" ]]; then
    RUNNER=(./docker/env/hydra.sh)
else
    RUNNER=(uv run sct.py)
fi

echo "=== minicloud $FLAVOR test | backend=$BACKEND mode=$MODE case=$TEST_CASE ==="

"${RUNNER[@]}" start-minicloud -b "$BACKEND" \
    -c "$TEST_CASE" -c configurations/minicloud.yaml

"${RUNNER[@]}" run-test "$TEST" --backend "$BACKEND" \
    --config "$TEST_CASE" --config configurations/minicloud.yaml
