#!/bin/bash
# Step 8 (optional) -- perf smoke on the same backend, once the artifact run is green.
#
#   SIM_DB_COUNT=3 SIM_LOADER_COUNT=1 SIM_MONITOR_COUNT=1 \
#       scripts/baremetal-simulation/01_launch_hosts.py       # re-provision first
#   scripts/baremetal-simulation/08_run_perf_test.sh
#
# Uses the existing test-cases/performance/perf-regression-throughput-baremetal-5gb.yaml
# with s3_baremetal_config pointed at this simulation's JSON.  Expect extra friction
# on the monitor node (docker + scylla-monitoring on Fedora) and on the loader
# (cassandra-stress / java) -- note both as findings, neither invalidates the DB-node
# conclusion.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

config_json="$(sim_require_config_json)"
for section in db_nodes loader_nodes monitor_nodes; do
    count="$(python3 -c "import json; print(len(json.load(open('${config_json}'))['${section}']['node_list']))")"
    sim_log "${section}: ${count}"
    [[ "${count}" -gt 0 ]] || sim_die "${section} is empty in ${config_json}; the perf test needs db + loader + monitor"
done

SCT_TEST_ID="${SCT_TEST_ID:-$(sim_new_test_id perf)}"
export SCT_TEST_ID
export SCT_S3_BAREMETAL_CONFIG="${SIM_BAREMETAL_CONFIG_NAME}"
export SCT_USE_PREINSTALLED_SCYLLA="false"
export SCT_SCYLLA_REPO="${SIM_SCYLLA_REPO}"

sim_log "test id: ${SCT_TEST_ID}"
sim_hydra run-test performance_regression_test.PerformanceRegressionTest.test_write \
    --backend baremetal \
    --config "${SIM_PERF_TEST_CASE}" \
    "$@"
