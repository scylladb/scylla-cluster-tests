#!/bin/bash
# One entry point for every local minicloud test flow.
#
# Usage:
#   scripts/run-minicloud-test.sh [-b aws|gce] [-f ami|repo|provision|upgrade] [-m direct|hydra]
#
#   -b  backend to emulate                          (default: aws)
#   -f  flavor: ami       - image-based artifacts test (AMI / GCE image)
#               repo      - deb/rpm repo-install artifacts test
#               provision - provisioning smoke test (longevity + nemesis)
#               upgrade   - rolling upgrade, 3 nodes, shrunk workloads
#                                                   (default: ami)
#   -m  direct: run sct.py from this checkout; hydra: run inside the hydra container
#                                                   (default: direct)
#
# Required env (per flavor):
#   SCT_SCYLLA_VERSION      - e.g. 2026.2  (or SCT_AMI_ID_DB_SCYLLA / SCT_GCE_IMAGE_DB for ami)
#   GCS_KEY_FILE / GOOGLE_APPLICATION_CREDENTIALS - GCE backend only (image export)
#
#   upgrade flavor also needs the target to upgrade *to*, one of:
#     SCT_NEW_SCYLLA_REPO   - repo URL of the target build (works on every backend)
#     SCT_NEW_VERSION       - target version; GCE only, unsupported for AWS AMIs
#   with SCT_SCYLLA_VERSION (or SCT_AMI_ID_DB_SCYLLA / SCT_GCE_IMAGE_DB) as the base to
#   start from. e.g.
#     SCT_SCYLLA_VERSION=2026.1 SCT_NEW_SCYLLA_REPO=<url> scripts/run-minicloud-test.sh -f upgrade
#
# Optional env:
#   SCT_TEST_CASE           - override the flavor's default test-case yaml
#   SCT_MINICLOUD_*         - any documented minicloud_* config option, e.g.
#                             SCT_MINICLOUD_LIGHTWEIGHT_MEMORY / _VCPUS to resize the guests
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
        h) awk '/^#/ {print; next} {exit}' "$0"; exit 0 ;;
        *) exit 5 ;;
    esac
done

# EXTRA_CONFIGS are layered between the test-case and configurations/minicloud.yaml, so a
# flavor can shrink a production test-case instead of duplicating it.
EXTRA_CONFIGS=()
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
    upgrade)
        # Same test the rollingUpgradePipeline runs in CI, so a local pass means something.
        TEST="upgrade_test.UpgradeTest.test_rolling_upgrade"
        DEFAULT_CASE="test-cases/upgrades/rolling-upgrade.yaml"
        # 6 nodes x i4i.2xlarge and 20M-row workloads do not fit on one host - the overlay cuts
        # the cluster to 3 nodes and the workloads to what a 1-vCPU guest can serve.
        EXTRA_CONFIGS=(configurations/minicloud/rolling-upgrade.yaml)
        ;;
    *) echo "ERROR: unknown flavor '$FLAVOR' (ami|repo|provision|upgrade)" >&2; exit 5 ;;
esac
TEST_CASE="${SCT_TEST_CASE:-$DEFAULT_CASE}"
# A --config given by the caller replaces the overlay's base, so do not shrink someone else's yaml.
[[ -n "${SCT_TEST_CASE:-}" ]] && EXTRA_CONFIGS=()

if [[ "$FLAVOR" != "provision" && -z "${SCT_SCYLLA_VERSION:-}" &&
      -z "${SCT_AMI_ID_DB_SCYLLA:-}" && -z "${SCT_GCE_IMAGE_DB:-}" ]]; then
    echo "ERROR: set SCT_SCYLLA_VERSION=x.y.z (or SCT_AMI_ID_DB_SCYLLA / SCT_GCE_IMAGE_DB)" >&2
    exit 5
fi

if [[ "$FLAVOR" == "upgrade" ]]; then
    # Without a target the test provisions the whole cluster and only then discovers it has
    # nothing to upgrade to.
    if [[ -z "${SCT_NEW_SCYLLA_REPO:-}" && -z "${SCT_NEW_VERSION:-}" ]]; then
        echo "ERROR: the upgrade flavor needs a target: set SCT_NEW_SCYLLA_REPO=<repo url>" >&2
        echo "       (or SCT_NEW_VERSION=x.y.z on the gce backend), with SCT_SCYLLA_VERSION" >&2
        echo "       as the base version to start from" >&2
        exit 5
    fi
    # sct_config rejects new_version for AWS AMIs - catch it here rather than after provisioning.
    if [[ -n "${SCT_NEW_VERSION:-}" && "$BACKEND" == "aws" ]]; then
        echo "ERROR: SCT_NEW_VERSION is not supported for AWS AMIs - use SCT_NEW_SCYLLA_REPO" >&2
        exit 5
    fi
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

# configurations/minicloud.yaml goes last so its mandatory values win. Both commands get the same
# list: start-minicloud sizes the container from the node counts the test will provision, so a
# mismatch would mean the preflight memory gate checked a different test than the one that runs.
CONFIGS=("$TEST_CASE" "${EXTRA_CONFIGS[@]}" configurations/minicloud.yaml)
CONFIG_ARGS=()
for config in "${CONFIGS[@]}"; do
    CONFIG_ARGS+=(-c "$config")
done

echo "=== minicloud $FLAVOR test | backend=$BACKEND mode=$MODE ==="
echo "=== configs: ${CONFIGS[*]} ==="

"${RUNNER[@]}" start-minicloud -b "$BACKEND" "${CONFIG_ARGS[@]}"

"${RUNNER[@]}" run-test "$TEST" --backend "$BACKEND" "${CONFIG_ARGS[@]}"
