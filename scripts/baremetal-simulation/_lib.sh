#!/bin/bash
# Shared shell helpers for the SCT-901 bare-metal simulation.
# Sourced by the 0*.sh scripts; not meant to be executed directly.

SIM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SIM_DIR}/../.." && pwd)"
SIM_STATE_DIR="${SIM_DIR}/.state"

sim_log() { echo "[sim] $*" >&2; }
sim_die() { echo "[sim] ERROR: $*" >&2; exit 1; }

# Load config.env, letting an already-exported variable win (same rule as common.py).
sim_load_config() {
    local line key value
    while IFS= read -r line; do
        line="${line%%$'\r'}"
        [[ -z "${line}" || "${line}" == \#* || "${line}" != *=* ]] && continue
        key="${line%%=*}"
        value="${line#*=}"
        key="${key//[[:space:]]/}"
        if [[ -z "${!key:-}" ]]; then
            export "${key}=${value}"
        else
            export "${key}"
        fi
    done < "${SIM_DIR}/config.env"
}

# A test id per run, remembered so 06_collect_logs.sh can find the run again.
sim_new_test_id() {
    local test_id
    test_id="$(uuidgen)"
    mkdir -p "${SIM_STATE_DIR}"
    echo "${test_id}" > "${SIM_STATE_DIR}/test_id"
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ${test_id} $*" >> "${SIM_STATE_DIR}/runs.log"
    echo "${test_id}"
}

sim_last_test_id() {
    [[ -f "${SIM_STATE_DIR}/test_id" ]] || sim_die "no test id recorded yet -- run 05_run_artifact_test.sh first"
    cat "${SIM_STATE_DIR}/test_id"
}

sim_require_config_json() {
    local path="${REPO_ROOT}/${SIM_BAREMETAL_CONFIG_NAME}.json"
    [[ -f "${path}" ]] || sim_die "${path} is missing -- run 02_write_baremetal_config.py"
    echo "${path}"
}

sim_hydra() {
    # hydra must run from the repo root: get_baremetal_config() resolves
    # ./<name>.json relative to the working directory.
    cd "${REPO_ROOT}" || sim_die "cannot cd to ${REPO_ROOT}"
    # Without a controlling terminal hydra's "docker run -it" fails with
    # "the input device is not a TTY".  BUILD_TAG puts it on its build-server
    # path, which drops -it; SCT only uses the value as an extra resource tag.
    if [[ ! -t 0 ]]; then
        export BUILD_TAG="${BUILD_TAG:-${SIM_TEST_TAG}-local}"
    fi
    # hydra forwards JOB_NAME into the container even when it is empty, and an
    # empty JOB_NAME is not "local_run", so TestConfig builds a *real* Argus
    # client for a run Argus has never heard of.  Every submission then raises
    # "Run not found", and the ERROR-severity event fails the test at teardown.
    # Naming the job explicitly puts the client in replay-only mode.
    export JOB_NAME="${JOB_NAME:-local_run}"
    sim_log "hydra $*"
    ./docker/env/hydra.sh "$@"
}

# Run an SCT test through hydra, retrying once on a known-transient setup failure.
#
# SCT resolves the Scylla version from the repo at config time with a single 30s
# budget and no retry (SCYLLA_URL_RESPONSE_TIMEOUT in sdcm/utils/version_utils.py),
# on every invocation.  downloads.scylladb.com throttled those objects four times
# on 2026-09-22, each time recovering within minutes, and each time failing the run
# before it touched the hardware -- once with five machines already provisioned.
#
# The retry is keyed on the failure signature, not on how long the run took: hydra
# spends minutes on container startup before SCT even begins, so a config-time
# failure can still take ~5 minutes of wall clock (measured: 293s for a run whose
# pytest phase was 40.74s).  Timing cannot tell the two apart; the message can.
SIM_TRANSIENT_SETUP_FAILURES='repodata/repomd\.xml|Connection reset by peer|ParallelObjectException|MaxRetryError'

sim_run_test_with_retry() {
    local test_name="$1" test_case="$2" label="$3"
    shift 3
    local backoff="${SIM_RETRY_BACKOFF_SECONDS:-120}"
    local attempt rc output
    output="$(mktemp -t sct901-run-XXXXXX.log)"

    for attempt in 1 2; do
        rc=0
        set -o pipefail
        sim_hydra run-test "${test_name}" --backend baremetal --config "${test_case}" "$@" 2>&1 \
            | tee "${output}" || rc=$?
        set +o pipefail

        if [[ ${rc} -eq 0 ]]; then
            rm -f "${output}"
            return 0
        fi
        if [[ ${attempt} -ge 2 ]]; then
            sim_log "run failed again (rc=${rc}) -- giving up"
            rm -f "${output}"
            return "${rc}"
        fi
        if ! grep -qE "${SIM_TRANSIENT_SETUP_FAILURES}" "${output}"; then
            sim_log "run failed (rc=${rc}) -- not a transient setup failure, not retrying"
            rm -f "${output}"
            return "${rc}"
        fi
        sim_log "transient setup failure detected -- retrying once in ${backoff}s"
        sleep "${backoff}"
        SCT_TEST_ID="$(sim_new_test_id "${label}-retry")"
        export SCT_TEST_ID
    done
}
