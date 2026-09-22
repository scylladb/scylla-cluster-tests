#!/bin/bash
# Step 5 -- run the artifact test against the simulated bare-metal hosts.
#
#   scripts/baremetal-simulation/05_run_artifact_test.sh [extra hydra args...]
#
# Expected sequence in the log (sdcm/cluster.py:6199 node_setup):
#   ssh up -> disable_firewall -> update_repo_cache -> clean_scylla ->
#   download_scylla_repo -> yum install -y scylla -> detect_disks(nvme=True) ->
#   scylla_setup --setup-nic-and-disks -> scylla-server up -> artifact sub-tests.
# verify_snitch self-skips because use_preinstalled_scylla is false.
#
# Results land in ~/sct-results/<timestamp>/; collect them with 06_collect_logs.sh.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

config_json="$(sim_require_config_json)"
[[ -f "${REPO_ROOT}/${SIM_TEST_CASE}" ]] || sim_die "${SIM_TEST_CASE} is missing -- run 04_render_test_case.py"

if ! grep -qE '"4[1-9]"' "${REPO_ROOT}/sdcm/utils/distro.py"; then
    sim_log "WARNING: sdcm/utils/distro.py does not know Fedora 41+; the install will take the apt branch."
    sim_log "         run: uv run python scripts/baremetal-simulation/03_patch_distro_for_fedora.py --apply"
fi

SCT_TEST_ID="${SCT_TEST_ID:-$(sim_new_test_id artifact)}"
export SCT_TEST_ID
export SCT_S3_BAREMETAL_CONFIG="${SIM_BAREMETAL_CONFIG_NAME}"

sim_log "test id     : ${SCT_TEST_ID}"
sim_log "test case   : ${SIM_TEST_CASE}"
sim_log "node config : ${config_json}"

# SCT resolves the Scylla version from the repo at config time with a single
# 30s budget and no retry (SCYLLA_URL_RESPONSE_TIMEOUT in sdcm/utils/version_utils.py),
# so a momentarily slow downloads.scylladb.com fails the whole run before it
# touches the hardware.  Observed twice on 2026-09-22, both times on the second
# invocation within a run, right after a ~400s S3 log upload.
# A real test failure takes minutes; anything that dies inside SETUP_FAILURE_SECONDS
# died in setup, so retry that once.
SETUP_FAILURE_SECONDS=150

for attempt in 1 2; do
    started=$(date +%s)
    if sim_hydra run-test artifacts_test.ArtifactsTest.test_scylla_service \
        --backend baremetal \
        --config "${SIM_TEST_CASE}" \
        "$@"; then
        sim_log "done -- collect logs with: scripts/baremetal-simulation/06_collect_logs.sh"
        exit 0
    else
        # $? inside the else branch is still the condition's status; after `fi`
        # it would be the `if` compound's own status, which is 0.
        rc=$?
    fi
    elapsed=$(( $(date +%s) - started ))
    if [[ ${attempt} -ge 2 || ${elapsed} -ge ${SETUP_FAILURE_SECONDS} ]]; then
        sim_log "run failed after ${elapsed}s (rc=${rc})"
        exit ${rc}
    fi
    # Back off before retrying: the usual cause is downloads.scylladb.com
    # throttling the repo objects after repeated runs hit them, and an immediate
    # retry just spends another 30s budget on the same throttled endpoint.
    backoff="${SIM_RETRY_BACKOFF_SECONDS:-120}"
    sim_log "failed after ${elapsed}s, i.e. during setup -- retrying once in ${backoff}s"
    sleep "${backoff}"
    SCT_TEST_ID="$(sim_new_test_id artifact-retry)"
    export SCT_TEST_ID
done
