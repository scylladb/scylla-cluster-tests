#!/bin/bash
# Step 7 -- second pass on the deliberately dirty host (SCT-901 criterion 2).
#
#   scripts/baremetal-simulation/07_rerun_dirty_host.sh
#
# Re-runs the identical test on the same host without resetting anything.
# clean_scylla() (sdcm/cluster.py:2790) removes the package and wipes the data
# directories, but it leaves behind the RAID array built by scylla_setup, the
# generated /etc/scylla.d/io.conf, the sysctl/kernel tuning and the CPU governor.
# PhysicalMachineNode.scylla_setup() then swallows the "already run" failure
# (sdcm/cluster_baremetal.py) -- which is exactly the seam where drift accumulates
# silently on a reused physical host.
#
# Host state is snapshotted before each pass into .state/host-state-<n>/ so the two
# runs can be diffed afterwards.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

config_json="$(sim_require_config_json)"
db_host="$(python3 -c "import json,sys; print(json.load(open('${config_json}'))['db_nodes']['node_list'][0]['public_ip'])")"

pass=1
while [[ -d "${SIM_STATE_DIR}/host-state-${pass}" ]]; do
    pass=$((pass + 1))
done
snapshot_dir="${SIM_STATE_DIR}/host-state-${pass}"
mkdir -p "${snapshot_dir}"

ssh_opts=(-i "${SIM_SSH_KEY/#\~/${HOME}}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
remote="${SIM_SSH_USER}@${db_host}"

sim_log "snapshotting host state before pass ${pass} into ${snapshot_dir}"
ssh "${ssh_opts[@]}" "${remote}" "cat /etc/scylla.d/io.conf 2>/dev/null"            > "${snapshot_dir}/io.conf"      || true
ssh "${ssh_opts[@]}" "${remote}" "cat /proc/mounts"                                 > "${snapshot_dir}/mounts"       || true
ssh "${ssh_opts[@]}" "${remote}" "cat /proc/mdstat 2>/dev/null; lsblk -p"           > "${snapshot_dir}/raid"         || true
ssh "${ssh_opts[@]}" "${remote}" "rpm -q scylla scylla-server 2>&1"                 > "${snapshot_dir}/packages"     || true
ssh "${ssh_opts[@]}" "${remote}" "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null" \
                                                                                    > "${snapshot_dir}/governor"     || true
ssh "${ssh_opts[@]}" "${remote}" "sysctl -a 2>/dev/null | sort"                      > "${snapshot_dir}/sysctl"       || true

if [[ ${pass} -gt 1 ]]; then
    previous="${SIM_STATE_DIR}/host-state-$((pass - 1))"
    sim_log "diff against pass $((pass - 1)) (empty output = no drift in these files):"
    diff -u "${previous}/io.conf" "${snapshot_dir}/io.conf" || true
    diff -u "${previous}/raid"    "${snapshot_dir}/raid"    || true
fi

sim_log "starting pass ${pass} on the un-reset host"
SCT_TEST_ID="$(sim_new_test_id "artifact-pass-${pass}")" "${SIM_DIR}/05_run_artifact_test.sh"

sim_log "record for SCT-901: was pass ${pass} green, did scylla_setup hit the 'already' path,"
sim_log "and did io.conf change between passes (see ${snapshot_dir})"
