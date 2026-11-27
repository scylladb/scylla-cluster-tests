#!/bin/bash
# Step 6 -- collect the logs of the last run through the bare-metal path.
#
#   scripts/baremetal-simulation/06_collect_logs.sh [<test-id>]
#
# Exercises sdcm/logcollector.py:2103 get_baremetal_instances_by_testid(), which
# re-reads the same JSON as the test did.  Collecting logs is part of the
# experiment: "log collection works for this backend" is one of the things
# SCT-901 needs answered.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

test_id="${1:-$(sim_last_test_id)}"
export SCT_S3_BAREMETAL_CONFIG="${SIM_BAREMETAL_CONFIG_NAME}"

# Collect with the test case the run used: runs.log labels each run "perf..." or "artifact...".
# A perf run collected with the artifact test case fails when that file is absent, and reads the wrong one otherwise.
label="$(awk -v id="${test_id}" '$2 == id {print $3}' "${SIM_STATE_DIR}/runs.log" 2>/dev/null | tail -n 1 || true)"
test_case="${SIM_TEST_CASE}"
[[ "${label}" == perf* ]] && test_case="${SIM_PERF_TEST_CASE}"

sim_log "collecting logs for ${test_id} (${label:-unknown run}, ${test_case})"
sim_hydra collect-logs --test-id "${test_id}" --backend baremetal --config-file "${test_case}"
