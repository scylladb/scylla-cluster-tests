# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB
from dataclasses import dataclass

from sdcm.sct_provision.user_data_objects import SctUserDataObject


NVME_FAULT_INJECT_SERVICE = "sct-nvme-fault-inject.service"
NVME_FAULT_INJECT_SCRIPT = "/usr/local/sbin/sct-nvme-fault-inject.sh"
NVME_MACHINE_IMAGE_CONFIGURED_MARKER = "/etc/scylla/machine_image_configured"
# remove mode only: expected non-root controller count, recorded on the settled first
# boot, read by the injector on the repro boot (see _remove_mode_script).
NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE = "/var/lib/sct-nvme-fault-inject/expected_controllers"
# remove mode only: PCI addresses the injector removed on the repro boot, one per line,
# read by the test's re-enumeration assertions. /run is per-boot by design.
NVME_FAULT_INJECT_REMOVED_PCI_ADDRS_FILE = "/run/sct-nvme-fault-inject/removed"


@dataclass
class NvmeFaultInjectUserDataObject(SctUserDataObject):
    """Fault injection for the NVMe namespace-rescan repro test.

    See docs/plans/testing/nvme-namespace-rescan-repro-test.md. Installs a oneshot
    systemd unit, ordered before scylla-image-setup.service, that breaks the node's
    local/ephemeral NVMe disk(s) in one of two ways, selected by the
    'nvme_rescan_repro_mode' test-case option:

    - 'unbind' (default): unbinds the PCI function's driver, leaving the pci_dev
      registered - simulating the "controller present, namespace missing" boot race.
      Unbinding removes both the NVMe controller and its namespace - there is no
      userspace-reachable way to remove only the namespace - so this only exercises
      the fix's PCI-bus-rescan fallback branch, not its primary 'rescan_controller'
      write branch. NOTE: a PCI-bus rescan cannot rebind an unbound-but-registered
      pci_dev (pci_scan_slot() skips occupied slots), so on NVMe-root SKUs this mode
      cannot validate the bus-rescan path at all - use 'remove' there.

    - 'remove': deletes the pci_dev entirely (echo 1 > .../remove) - reproducing the
      Azure Standard_L8s_v4 production incident where the local-disk controller was
      absent from PCI enumeration for the whole boot while the *root* disk's own
      healthy NVMe controller answered rescan_controller writes. Pre-02bb3c74
      scylla-machine-image code short-circuited on that success and never reached the
      PCI-bus rescan; only the unconditional bus rescan (scylla-machine-image commit
      02bb3c74) can re-enumerate a removed device. ALL non-root controllers are
      removed: leaving any survivor keeps get_local_disks() non-empty, so
      wait_for_devices() succeeds on its first pass, no rescan ever runs, and RAID
      silently builds on the surviving subset - partial injection is therefore treated
      as failure. Because the injector unit starts ~3s into boot while controllers
      enumerate at ~2.2s, the first boot records how many non-root controllers to
      expect and the injector waits for all of them before removing anything.
      Verified on Azure Standard_L8s_v4 (Hyper-V vPCI) only - re-scout before using
      this mode on another backend.

    This script runs during the first boot's cloud-init runcmd stage, i.e. after
    scylla-image-setup.service has already completed successfully on that same boot.
    It only enables the fault for the *next* boot; it does not reboot the node
    itself. The test triggers that reboot explicitly so it can observe the recovery
    deterministically.

    IMPORTANT (ordering): the first-boot cloud-init script must NOT delete the
    idempotency marker (/etc/scylla/machine_image_configured) or tear down the
    /var/lib/scylla mount. SCT's generic node-provisioning wait
    (Node.wait_for_machine_image_configured(), used by every artifacts test's
    setUp()) only starts polling for that marker *after* cloud-init returns - which
    is exactly when this first-boot script has already finished. If the marker were
    removed here it would never reappear before the test body's reboot, so setUp()
    would time out and fail deterministically before the test method ever runs.

    Instead, the destructive teardown (marker removal, mount/md0 teardown) is
    performed by the fault-inject systemd unit itself on the *next* boot, ordered
    Before=local-fs-pre.target (see below) and Before=scylla-image-setup.service.
    That forces a genuine re-run of scylla-image-setup on that boot while keeping
    the first boot's node setup intact.

    ORDERING (why Before=local-fs-pre.target, not just Before=scylla-image-setup.service):
    on the re-run boot, /var/lib/scylla (created by the first boot's successful setup)
    is mounted by local-fs.target - which completes, by default systemd dependency
    ordering, before scylla-image-setup.service ever starts (a plain .service unit
    without DefaultDependencies=no is implicitly ordered After=sysinit.target, which
    requires local-fs.target). Trying to fault-inject only Before=scylla-image-setup.service
    therefore means /var/lib/scylla is already actively mounted by the time the
    injector runs, forcing it to lazily unmount (`umount -l`) a live, busy mount and
    race scylla_raid_setup's own mount check on the same boot - this raced in practice
    (device still reported busy) and is why an earlier version of this injector, despite
    tearing down the mount before its own root-device scan, still misidentified the data
    device as "root" (it was still visible via findmnt) and still hit "device is busy"
    when scylla_raid_setup tried to reformat it. Ordering the injector
    Before=local-fs-pre.target instead runs it before local-fs.target ever attempts
    the mount in the first place (local-fs-pre.target is systemd's documented hook for
    "run this before any local filesystem is mounted", see systemd.special(7)) - so
    there is no live mount to race against at all: the injector deletes the stale
    mount unit/fstab line and unbinds the device before local-fs.target looks for
    either. The explicit Before=scylla-image-setup.service ordering is also kept
    as a second, redundant guarantee independent of this default-dependency chain.

    After=systemd-remount-fs.service is required for the same DefaultDependencies=no
    reason: without it, this unit can start concurrently with the remount of root
    from its initial read-only boot state, and its own `rm` of the idempotency marker
    races that remount. Observed in practice: the unit started at boot+3.15s, root
    remounted r/w at boot+3.36s, and the unit's `rm` failed with "Read-only file
    system" and (via set -e) aborted before ever reaching the NVMe unbind loop -
    silently defeating the whole fault injection.
    """

    @property
    def is_applicable(self) -> bool:
        return self.node_type == "scylla-db" and bool(self.params.get("nvme_rescan_repro"))

    @property
    def script_to_run(self) -> str:
        mode = self.params.get("nvme_rescan_repro_mode") or "unbind"
        if mode == "unbind":
            return self._unbind_mode_script
        if mode == "remove":
            return self._remove_mode_script
        raise ValueError(f"unsupported nvme_rescan_repro_mode: {mode!r} (expected 'unbind' or 'remove')")

    def _service_install_script(self, fault_verb: str) -> str:
        return f"""
cat > /etc/systemd/system/{NVME_FAULT_INJECT_SERVICE} <<'SERVICE_EOF'
[Unit]
Description=SCT: {fault_verb} local NVMe device before local mounts and scylla-image-setup (fault injection)
DefaultDependencies=no
After=systemd-remount-fs.service
Before=local-fs-pre.target scylla-image-setup.service

[Service]
Type=oneshot
ExecStart={NVME_FAULT_INJECT_SCRIPT}
RemainAfterExit=yes

[Install]
WantedBy=sysinit.target
SERVICE_EOF

cat > /etc/systemd/system/scylla-image-setup.service.d/override.conf <<'OVERRIDE_EOF'
[Unit]
After={NVME_FAULT_INJECT_SERVICE}
OVERRIDE_EOF

systemctl daemon-reload
systemctl enable {NVME_FAULT_INJECT_SERVICE}

# Deliberately DO NOT remove the idempotency marker or tear down /var/lib/scylla
# here. This first-boot setup completed successfully and must stay intact so SCT's
# generic setUp() machine-image wait (which starts polling only after cloud-init
# returns) succeeds. The marker removal and mount teardown happen in
# {NVME_FAULT_INJECT_SERVICE} on the next boot, right before scylla-image-setup
# re-runs - see {NVME_FAULT_INJECT_SCRIPT}.
"""

    @property
    def _unbind_mode_script(self) -> str:
        return f"""
install -d -m 0755 /etc/systemd/system/scylla-image-setup.service.d

cat > {NVME_FAULT_INJECT_SCRIPT} <<'INJECTOR_EOF'
#!/bin/bash
# Runs on the *next* boot, ordered Before=local-fs-pre.target (i.e. before any local
# filesystem, including a leftover /var/lib/scylla from a prior successful boot, is
# mounted) and Before=scylla-image-setup.service. Removes the idempotency marker and
# any leftover mount-unit/fstab/RAID state for /var/lib/scylla so scylla-image-setup
# genuinely re-runs and local-fs.target never (re)mounts it on this boot, then unbinds
# the PCI function backing any non-root local NVMe namespace. Iterates every NVMe
# controller, not just the first, since some shapes (OCI DenseIO, AWS instance-store)
# expose more than one local NVMe namespace.
#
# Deliberately does NOT umount /var/lib/scylla: because this unit runs before
# local-fs-pre.target, the mount never gets (re)activated on this boot in the first
# place, so there's nothing live to unmount or race against.
set -euo pipefail

# Force a genuine re-run of scylla_create_devices/scylla-image-setup on this boot -
# the previous boot's setup completed successfully and must not be skipped via the
# idempotency marker. Done here (not in first-boot cloud-init) so the marker stays
# present until this reboot; otherwise SCT's generic setUp() machine-image wait,
# which only starts polling after cloud-init returns, would time out before the
# test body ever reboots the node.
md0_members=$(awk '$1 == "md0" {{for (i=5; i<=NF; i++) {{ sub(/\\[.*/, "", $i); print $i }} }}' /proc/mdstat 2>/dev/null || true)

rm -f {NVME_MACHINE_IMAGE_CONFIGURED_MARKER}
mdadm --stop /dev/md0 2>/dev/null || true
for member in $md0_members; do
    mdadm --zero-superblock "/dev/$member" 2>/dev/null || true
done
rm -f /etc/systemd/system/var-lib-scylla.mount
sed -i '\\#/var/lib/scylla#d' /etc/fstab || true
systemctl daemon-reload

root_devs=$(findmnt -n -o SOURCE --list | grep -E '^/dev/nvme' || true)
found=0

for ctrl_path in /sys/class/nvme/nvme*; do
    [ -e "$ctrl_path" ] || continue
    ctrl_name=$(basename "$ctrl_path")
    for ns_path in "$ctrl_path"/nvme*n*; do
        [ -e "$ns_path" ] || continue
        ns_name=$(basename "$ns_path")
        dev="/dev/$ns_name"
        [ -e "$dev" ] || continue

        is_root=0
        for root_dev in $root_devs; do
            case "$root_dev" in
                "$dev"*) is_root=1 ;;
            esac
        done
        if mount | grep -q "^$dev on / "; then
            is_root=1
        fi
        if [ "$is_root" -eq 1 ]; then
            echo "sct-nvme-fault-inject: skipping root device $dev"
            continue
        fi

        pci_addr=$(basename "$(readlink -f "/sys/class/nvme/$ctrl_name/device")")
        if [ ! -e "/sys/bus/pci/devices/$pci_addr/driver/unbind" ]; then
            echo "sct-nvme-fault-inject: no driver bound for $pci_addr, skipping" >&2
            continue
        fi
        echo "sct-nvme-fault-inject: unbinding $ctrl_name (PCI $pci_addr), backs $dev"
        echo "$pci_addr" > "/sys/bus/pci/devices/$pci_addr/driver/unbind"
        found=1
    done
done

if [ "$found" -eq 0 ]; then
    echo "sct-nvme-fault-inject: no non-root NVMe device found to unbind" >&2
    exit 1
fi
INJECTOR_EOF
chmod 0755 {NVME_FAULT_INJECT_SCRIPT}
{self._service_install_script(fault_verb="unbind")}"""

    @property
    def _remove_mode_script(self) -> str:
        return f"""
install -d -m 0755 /etc/systemd/system/scylla-image-setup.service.d

cat > {NVME_FAULT_INJECT_SCRIPT} <<'INJECTOR_EOF'
#!/bin/bash
# Runs on the *next* boot, ordered Before=local-fs-pre.target and
# Before=scylla-image-setup.service (same teardown rationale as unbind mode - see
# NvmeFaultInjectUserDataObject). Then, instead of unbinding drivers, fully DELETES
# the PCI device of every non-root NVMe controller (echo 1 > .../remove) - after
# which only an unconditional PCI-bus rescan can re-discover them, exactly like the
# production "controller never enumerated" incident on NVMe-root SKUs.
#
# ALL non-root controllers must be removed: any survivor keeps get_local_disks()
# non-empty, wait_for_devices() succeeds on its first pass without rescanning, and
# RAID silently builds on the surviving subset - so partial injection exits non-zero.
# Controllers enumerate at ~2.2s while this unit starts at ~3s, so wait for the
# non-root controller count recorded on the settled first boot before removing.
set -euo pipefail

rm -f {NVME_MACHINE_IMAGE_CONFIGURED_MARKER}
mdadm --stop /dev/md0 2>/dev/null || true
rm -f /etc/systemd/system/var-lib-scylla.mount
sed -i '\\#/var/lib/scylla#d' /etc/fstab || true
systemctl daemon-reload

if [ ! -s {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE} ]; then
    echo "sct-nvme-fault-inject: {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE} missing or empty - first boot never recorded it" >&2
    exit 1
fi
expected=$(cat {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE})
if [ "$expected" -le 0 ]; then
    echo "sct-nvme-fault-inject: first boot recorded $expected non-root NVMe controllers - nothing to inject" >&2
    exit 1
fi

root_devs=$(findmnt -n -o SOURCE --list | grep -E '^/dev/nvme' || true)

list_nonroot_nvme() {{
    # one "pci_addr /dev/nvmeXnY" line per non-root NVMe namespace
    for ctrl_path in /sys/class/nvme/nvme*; do
        [ -e "$ctrl_path" ] || continue
        for ns_path in "$ctrl_path"/nvme*n*; do
            [ -e "$ns_path" ] || continue
            dev="/dev/$(basename "$ns_path")"
            [ -e "$dev" ] || continue
            is_root=0
            for root_dev in $root_devs; do
                case "$root_dev" in
                    "$dev"*) is_root=1 ;;
                esac
            done
            if mount | grep -q "^$dev on / "; then
                is_root=1
            fi
            if [ "$is_root" -eq 1 ]; then
                echo "sct-nvme-fault-inject: skipping root device $dev" >&2
                continue
            fi
            echo "$(basename "$(readlink -f "$ctrl_path/device")") $dev"
        done
    done
}}

echo "sct-nvme-fault-inject: waiting for $expected non-root NVMe controllers to enumerate"
found_count=0
for _ in $(seq 1 60); do
    found_count=$(list_nonroot_nvme | awk '{{print $1}}' | sort -u | wc -l)
    if [ "$found_count" -ge "$expected" ]; then
        break
    fi
    sleep 1
done
if [ "$found_count" -lt "$expected" ]; then
    echo "sct-nvme-fault-inject: only $found_count of $expected non-root NVMe controllers enumerated - partial injection would silently void the repro" >&2
    exit 1
fi

# udev's incremental assembly may have re-assembled md0 from members that enumerated
# after the teardown above; stop it again so the superblocks can be zeroed.
mdadm --stop /dev/md0 2>/dev/null || true
list_nonroot_nvme | while read -r pci_addr dev; do
    mdadm --zero-superblock "$dev" 2>/dev/null || true
done

target_pci_addrs=$(list_nonroot_nvme | awk '{{print $1}}' | sort -u)
for pci_addr in $target_pci_addrs; do
    echo "sct-nvme-fault-inject: removing PCI device $pci_addr"
    echo 1 > "/sys/bus/pci/devices/$pci_addr/remove"
done

remaining=1
for _ in $(seq 1 10); do
    remaining=0
    for pci_addr in $target_pci_addrs; do
        if [ -e "/sys/bus/pci/devices/$pci_addr" ]; then
            remaining=$((remaining + 1))
        fi
    done
    if [ "$remaining" -eq 0 ]; then
        break
    fi
    sleep 1
done
if [ "$remaining" -ne 0 ]; then
    echo "sct-nvme-fault-inject: $remaining PCI devices still present after remove" >&2
    exit 1
fi

install -d -m 0755 $(dirname {NVME_FAULT_INJECT_REMOVED_PCI_ADDRS_FILE})
printf '%s\\n' $target_pci_addrs > {NVME_FAULT_INJECT_REMOVED_PCI_ADDRS_FILE}
echo "sct-nvme-fault-inject: removed PCI devices:" $target_pci_addrs
INJECTOR_EOF
chmod 0755 {NVME_FAULT_INJECT_SCRIPT}
{self._service_install_script(fault_verb="remove")}
# Record how many non-root NVMe controllers the injector must wait for on the repro
# boot. Counted here, on the fully settled first boot, because on the repro boot the
# injector starts before late-enumerating controllers appear and has no other way to
# know it is seeing all of them.
install -d -m 0755 $(dirname {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE})
root_devs=$(findmnt -n -o SOURCE --list | grep -E '^/dev/nvme' || true)
expected=0
seen_pci_addrs=""
for ctrl_path in /sys/class/nvme/nvme*; do
    [ -e "$ctrl_path" ] || continue
    for ns_path in "$ctrl_path"/nvme*n*; do
        [ -e "$ns_path" ] || continue
        dev="/dev/$(basename "$ns_path")"
        [ -e "$dev" ] || continue
        is_root=0
        for root_dev in $root_devs; do
            case "$root_dev" in
                "$dev"*) is_root=1 ;;
            esac
        done
        if [ "$is_root" -eq 1 ]; then
            continue
        fi
        pci_addr=$(basename "$(readlink -f "$ctrl_path/device")")
        case " $seen_pci_addrs " in
            *" $pci_addr "*) ;;
            *)
                seen_pci_addrs="$seen_pci_addrs $pci_addr"
                expected=$((expected + 1))
                ;;
        esac
    done
done
echo "$expected" > {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE}
echo "sct-nvme-fault-inject: recorded $expected non-root NVMe controllers for the repro boot"
"""
