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
# The perf test case does not set logs_transport, so it takes the global default
# of "vector".  On Fedora 44 that binary core-dumped repeatedly on every node
# (2026-09-22), and each dump raises an ERROR-severity CoreDumpEvent that fails
# the run at teardown -- and the coredump exporter's own `yum list installed`
# deadlocked dnf, hanging node_setup with no timeout.  Ship logs over SSH instead,
# as the artifact test case already does.
export SCT_LOGS_TRANSPORT="${SIM_LOGS_TRANSPORT:-ssh}"
# perf-regression-throughput-baremetal-5gb.yaml asks for RF=3, but every
# PhysicalMachineNode reports the same rack (RACK0) -- the backend has no rack
# awareness, whereas on AWS/GCE SCT derives racks from AZs.  ScyllaDB 2025.3
# enables rf_rack_valid_keyspaces by default, so RF=3 in a single rack is
# rejected and cassandra-stress dies creating its keyspace:
#
#   InvalidQueryException: The option `rf_rack_valid_keyspaces` is enabled.
#   It requires that all keyspaces are RF-rack-valid.
#
# The real fix is either rack support for the baremetal backend or an RF the
# topology can satisfy; this override only lets the workload run meanwhile.
# NB: Python literal syntax, not JSON/YAML.  sdcm/sct_config/types.py's
# dict_or_str_or_pydantic() only tries ast.literal_eval() on a string, unlike its
# sibling dict_or_str() which falls back to yaml.safe_load() -- so a lowercase
# `false` here fails with "isn't a dict, str or Pydantic model".
export SCT_APPEND_SCYLLA_YAML="${SIM_APPEND_SCYLLA_YAML:-{\"rf_rack_valid_keyspaces\": False\}}"

sim_log "test id: ${SCT_TEST_ID}"
sim_run_test_with_retry performance_regression_test.PerformanceRegressionTest.test_write \
    "${SIM_PERF_TEST_CASE}" perf "$@"

sim_log "done -- collect logs with: scripts/baremetal-simulation/06_collect_logs.sh"
