#!/bin/bash
set -euo pipefail

# Run the clean-resources stage against minicloud, as the CI pipeline does after a
# minicloud provision test.
#
# SAFETY: clean-resources terminates whatever its filters match. Minicloud mode is
# decided from SCT_MINICLOUD_ENDPOINT_URL or a localhost AWS_ENDPOINT_URL; with
# neither set, boto3 talks to *real* AWS and the same filters would match real
# instances. So this script pins the endpoint and refuses to run unless minicloud
# actually answers on it - never let a missing endpoint fall through to a real account.
#
# Usage:
#   scripts/run-minicloud-clean-resources.sh                    # latest run
#   TEST_ID=<uuid> scripts/run-minicloud-clean-resources.sh     # a specific run
#   DRY_RUN=1 scripts/run-minicloud-clean-resources.sh          # show, do not delete
#   BACKEND=gce scripts/run-minicloud-clean-resources.sh        # GCE provision test
#   POST_BEHAVIOR=1 scripts/run-minicloud-clean-resources.sh    # honour keep-on-failure
#
# Any extra arguments are passed through to sct.py clean-resources.

MINICLOUD_PORT="${MINICLOUD_PORT:-5000}"  # local probe only
MINICLOUD_ENDPOINT="http://localhost:${MINICLOUD_PORT}"

BACKEND="${BACKEND:-aws}"

# Same variables the provision-test scripts export, so the cleanup talks to exactly
# the cloud the test provisioned into.
export AWS_ENDPOINT_URL="${MINICLOUD_ENDPOINT}"
export SCT_MINICLOUD_ENDPOINT_URL="${MINICLOUD_ENDPOINT}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
if [[ "${BACKEND}" == "gce" ]]; then
    export GCE_ENDPOINT_URL="${MINICLOUD_ENDPOINT}"
fi

# Fail closed. Same liveness probe the manager uses: DescribeVpcs is
# implemented locally and never proxied to real AWS.
if ! curl -sf "${MINICLOUD_ENDPOINT}" -d "Action=DescribeVpcs&Version=2016-11-15" >/dev/null 2>&1; then
    echo "ERROR: minicloud is not answering at ${MINICLOUD_ENDPOINT}." >&2
    echo "" >&2
    echo "Refusing to run clean-resources: without a reachable minicloud, boto3 would" >&2
    echo "target real AWS and these filters would match real instances." >&2
    echo "" >&2
    echo "Start it first:  uv run sct.py start-minicloud" >&2
    exit 1
fi

echo "minicloud is reachable at ${MINICLOUD_ENDPOINT} (backend: ${BACKEND})"

CLEAN_ARGS=(--backend "${BACKEND}")

if [[ -n "${TEST_ID:-}" ]]; then
    CLEAN_ARGS+=(--test-id "${TEST_ID}")
    echo "Cleaning resources for test-id ${TEST_ID}"
else
    # No filter: clean-resources falls back to the most recent run in the logdir.
    echo "Cleaning resources for the latest run"
fi

# Deliberately not defaulted: --user matches every run by that user, which is a much
# broader blast radius than one test-id. Opt in explicitly.
if [[ -n "${USER_FILTER:-}" ]]; then
    CLEAN_ARGS+=(--user "${USER_FILTER}")
fi

if [[ -n "${LOGDIR:-}" ]]; then
    CLEAN_ARGS+=(--logdir "${LOGDIR}")
fi

# Off by default: post-behavior honours keep-on-failure, so a failed run would leave
# its resources behind. When you invoke this script you usually want them gone.
if [[ -n "${POST_BEHAVIOR:-}" ]]; then
    CLEAN_ARGS+=(--post-behavior)
fi

if [[ -n "${DRY_RUN:-}" ]]; then
    CLEAN_ARGS+=(--dry-run)
    echo "DRY RUN - nothing will be deleted"
fi

echo "+ sct.py clean-resources ${CLEAN_ARGS[*]} $*"
uv run sct.py clean-resources "${CLEAN_ARGS[@]}" "$@"

# clean-resources only removes what the cloud API knows about. minicloud additionally
# keeps per-VM state on the host, which it will not touch - report it rather than
# deleting, since the AMI cache next to it is expensive to rebuild.
STATE_DIR="${MINICLOUD_STATE_DIR:-${HOME}/.cache/minicloud}"
if [[ -d "${STATE_DIR}/instances" ]]; then
    leftover=$(find "${STATE_DIR}/instances" -mindepth 1 -maxdepth 1 | wc -l)
    if [[ "${leftover}" -gt 0 ]]; then
        echo ""
        echo "NOTE: ${leftover} VM state dir(s) remain under ${STATE_DIR}/instances"
        echo "      clean-resources does not remove these. To clear them:"
        echo "        docker rm -f minicloud && rm -rf ${STATE_DIR}/instances/*"
        echo "      Keep ${STATE_DIR}/amis - re-downloading images is slow."
    fi
fi
