#!/bin/bash
# Step 9 -- reset the hosts to a pre-scylla_setup state.
#
#   scripts/baremetal-simulation/09_reset_host.sh
#
# Required before re-running on a host that has already been used.  Measured on
# 2026-09-22: without this, a second pass fails.  clean_scylla() removes the
# package and wipes the data directories, but everything scylla_setup built
# survives, and scylla_setup then refuses to redo its work:
#
#   /etc/systemd/system/var-lib-scylla.mount already exists, skipping RAID setup.
#   -> exit 1, swallowed by PhysicalMachineNode.scylla_setup()
#
# Skipping RAID setup also skips scylla_io_setup, so nothing regenerates
# /etc/scylla.d/io.conf -- which the RPM reinstall has meanwhile reset to its
# packaged stub (saving the generated one as io.conf.rpmsave).  Scylla then
# starts with no I/O properties and dies with:
#
#   Startup failed: std::runtime_error (Bad I/O Scheduler configuration)
#
# This script removes exactly that accumulated state: the systemd mount unit, the
# mount itself, the RAID/filesystem signatures on the data disks, and the
# generated files under /etc/scylla.d.  The root disk is never touched.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

config_json="$(sim_require_config_json)"
mapfile -t hosts < <(python3 -c "
import json
config = json.load(open('${config_json}'))
for section in ('db_nodes', 'loader_nodes', 'monitor_nodes'):
    for node in config[section]['node_list']:
        print(node['public_ip'])
")
[[ ${#hosts[@]} -gt 0 ]] || sim_die "no hosts in ${config_json}"

ssh_opts=(-i "${SIM_SSH_KEY/#\~/${HOME}}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15)

read -r -d '' RESET_SCRIPT <<'REMOTE' || true
set -u
echo "== stopping scylla =="
sudo systemctl stop scylla-server.service 2>/dev/null || true
sudo systemctl stop scylla-jmx.service 2>/dev/null || true

echo "== releasing /var/lib/scylla =="
sudo systemctl stop var-lib-scylla.mount 2>/dev/null || true
sudo umount /var/lib/scylla 2>/dev/null || true
# The unit's presence is what makes scylla_setup skip RAID setup.
sudo rm -f /etc/systemd/system/var-lib-scylla.mount /etc/systemd/system/var-lib-scylla.automount
sudo rm -f /etc/systemd/system/*.wants/var-lib-scylla.mount
sudo sed -i '\#/var/lib/scylla#d' /etc/fstab
sudo systemctl daemon-reload

echo "== wiping the data disks (anything in use is skipped) =="
# findmnt prints "/dev/nvme0n1p3[/root]" for a btrfs subvolume, which is not a
# device path -- strip the subvolume before asking lsblk for the parent disk.
root_source=$(findmnt -no SOURCE / | sed 's/\[.*//')
root_disk=$(lsblk -no PKNAME "${root_source}" 2>/dev/null | head -1)
[ -n "${root_disk}" ] && root_disk="/dev/${root_disk}"
echo "   root disk: ${root_disk:-unknown}"

for md in $(awk '/^md[0-9]/{print $1}' /proc/mdstat 2>/dev/null); do
    sudo mdadm --stop "/dev/${md}" 2>/dev/null || true
done

for disk in $(lsblk -dnp -o NAME,TYPE | awk '$2=="disk"{print $1}'); do
    case "${disk}" in
        /dev/zram*) echo "   skip ${disk} (zram)"; continue ;;
    esac
    if [ -n "${root_disk}" ] && [ "${disk}" = "${root_disk}" ]; then
        echo "   skip ${disk} (root disk)"
        continue
    fi
    # Three independent guards, because getting this wrong destroys a machine
    # that is not ours to destroy: a data disk for Scylla carries a bare
    # filesystem, so anything partitioned or still mounted is not it.
    if lsblk -nr -o TYPE "${disk}" | grep -qx part; then
        echo "   skip ${disk} (has partitions)"
        continue
    fi
    if [ -n "$(lsblk -nr -o MOUNTPOINT "${disk}" | tr -d ' ')" ]; then
        echo "   skip ${disk} (still mounted)"
        continue
    fi
    echo "   wipefs ${disk}"
    sudo mdadm --zero-superblock "${disk}" 2>/dev/null || true
    if ! sudo wipefs -a "${disk}"; then
        echo "   WARNING: wipefs failed on ${disk}"
    fi
done

echo "== removing generated scylla config =="
sudo rm -rf /var/lib/scylla/* 2>/dev/null || true
sudo rm -f /etc/scylla.d/io.conf /etc/scylla.d/io_properties.yaml /etc/scylla.d/perftune.yaml
sudo rm -f /etc/scylla.d/*.rpmsave /etc/scylla/*.rpmsave

echo "== state after reset =="
findmnt -no SOURCE,TARGET -T /var/lib/scylla || true
ls -1 /etc/scylla.d/ 2>/dev/null || true
test -e /etc/systemd/system/var-lib-scylla.mount && echo "MOUNT UNIT STILL PRESENT" || echo "mount unit removed"
REMOTE

for host in "${hosts[@]}"; do
    sim_log "resetting ${host}"
    ssh "${ssh_opts[@]}" "${SIM_SSH_USER}@${host}" "${RESET_SCRIPT}" 2>&1 | sed 's/^/    /'
done

sim_log "hosts reset -- scylla_setup will run in full on the next pass"
