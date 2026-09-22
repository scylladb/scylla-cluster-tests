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
