import subprocess

import pytest

from sdcm.sct_provision.user_data_objects.nvme_fault_inject import (
    NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE,
    NVME_FAULT_INJECT_REMOVED_PCI_ADDRS_FILE,
    NVME_FAULT_INJECT_SCRIPT,
    NVME_FAULT_INJECT_SERVICE,
    NvmeFaultInjectUserDataObject,
)
from sdcm.test_config import TestConfig


def _make_object(node_type="scylla-db", nvme_rescan_repro=True, mode=None):
    params = {"nvme_rescan_repro": nvme_rescan_repro}
    if mode is not None:
        params["nvme_rescan_repro_mode"] = mode
    return NvmeFaultInjectUserDataObject(
        test_config=TestConfig(),
        params=params,
        instance_name="test-instance",
        node_type=node_type,
    )


def _injector_body(script):
    start = script.find("INJECTOR_EOF")
    end = script.find("INJECTOR_EOF", start + 1)
    assert start != -1 and end != -1
    return script[start:end]


def test_is_applicable_only_for_db_node_with_flag_enabled():
    assert _make_object(node_type="scylla-db", nvme_rescan_repro=True).is_applicable is True


def test_is_not_applicable_when_flag_disabled():
    assert _make_object(node_type="scylla-db", nvme_rescan_repro=False).is_applicable is False


def test_is_not_applicable_when_flag_missing():
    sud = NvmeFaultInjectUserDataObject(
        test_config=TestConfig(), params={}, instance_name="test-instance", node_type="scylla-db"
    )
    assert sud.is_applicable is False


def test_is_not_applicable_for_non_db_node_types():
    for node_type in ("loader", "monitor"):
        assert _make_object(node_type=node_type, nvme_rescan_repro=True).is_applicable is False


def test_script_installs_and_enables_fault_inject_unit_before_local_mounts_and_image_setup():
    script = _make_object().script_to_run

    assert f"cat > /etc/systemd/system/{NVME_FAULT_INJECT_SERVICE}" in script
    # DefaultDependencies=no + Before=local-fs-pre.target is required so this unit runs
    # before local-fs.target (re)mounts a leftover /var/lib/scylla from a prior boot -
    # otherwise the injector would have to unmount a live, busy mount and race
    # scylla_raid_setup's own mount check on the same boot (see docstring/plan doc).
    assert "DefaultDependencies=no" in script
    assert "Before=local-fs-pre.target scylla-image-setup.service" in script
    assert f"ExecStart={NVME_FAULT_INJECT_SCRIPT}" in script
    assert "WantedBy=sysinit.target" in script
    assert "cat > /etc/systemd/system/scylla-image-setup.service.d/override.conf" in script
    assert f"After={NVME_FAULT_INJECT_SERVICE}" in script
    assert f"systemctl enable {NVME_FAULT_INJECT_SERVICE}" in script


def test_marker_teardown_deferred_to_injector_unit_not_first_boot():
    """The destructive teardown must live inside the fault-inject unit script (which runs
    on the next boot, before scylla-image-setup), NOT in the first-boot cloud-init script.

    If the marker were removed on the first boot, SCT's generic setUp() machine-image wait
    (which only starts polling after cloud-init returns) would time out before the test body
    ever reboots the node. See NvmeFaultInjectUserDataObject docstring.
    """
    script = _make_object().script_to_run

    injector_start = script.find("INJECTOR_EOF")
    injector_end = script.find("INJECTOR_EOF", injector_start + 1)
    assert injector_start != -1 and injector_end != -1
    injector_body = script[injector_start:injector_end]

    # teardown lives inside the injector heredoc (next boot)
    assert "rm -f /etc/scylla/machine_image_configured" in injector_body
    assert "rm -f /etc/systemd/system/var-lib-scylla.mount" in injector_body
    assert "mdadm --stop /dev/md0" in injector_body
    assert "mdadm --zero-superblock" in injector_body

    # no live-mount teardown: the unit runs Before=local-fs-pre.target, so
    # /var/lib/scylla is never (re)mounted on this boot in the first place - there is
    # nothing to unmount, so no umount command must appear anywhere in the script.
    assert "umount -l" not in script

    # ...and NOT in the first-boot cloud-init part (after the injector heredoc closes)
    first_boot_tail = script[injector_end:]
    assert "rm -f /etc/scylla/machine_image_configured" not in first_boot_tail

    # the script must never issue a reboot/shutdown itself - the test triggers the reboot
    assert "systemctl reboot" not in script
    assert "\nreboot" not in script
    assert "shutdown" not in script


def test_script_captures_raid_members_before_stopping_the_array():
    script = _make_object().script_to_run

    members_idx = script.find("md0_members=")
    stop_idx = script.find("mdadm --stop /dev/md0")
    assert members_idx != -1 and stop_idx != -1
    assert members_idx < stop_idx, "RAID members must be captured from /proc/mdstat before the array is stopped"


def test_injector_script_iterates_all_nvme_controllers_and_skips_root():
    script = _make_object().script_to_run

    assert "for ctrl_path in /sys/class/nvme/nvme*" in script
    assert "skipping root device" in script
    assert "no non-root NVMe device found to unbind" in script


def test_default_mode_is_unbind():
    assert _make_object().script_to_run == _make_object(mode="unbind").script_to_run


def test_unknown_mode_raises():
    with pytest.raises(ValueError, match="nvme_rescan_repro_mode"):
        _make_object(mode="detach").script_to_run  # noqa: B018


def test_remove_mode_script_removes_pci_devices_instead_of_unbinding():
    injector = _injector_body(_make_object(mode="remove").script_to_run)

    assert '> "/sys/bus/pci/devices/$pci_addr/remove"' in injector
    assert "driver/unbind" not in injector
    assert "skipping root device" in injector


def test_remove_mode_waits_for_expected_controller_count_and_fails_on_partial_injection():
    """Removing fewer than all non-root controllers must fail the unit: any survivor keeps
    get_local_disks() non-empty, so wait_for_devices() succeeds on its first pass without
    ever rescanning and the repro is silently voided (see plan doc, Current State)."""
    injector = _injector_body(_make_object(mode="remove").script_to_run)

    assert NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE in injector
    wait_idx = injector.find("waiting for $expected non-root NVMe controllers")
    remove_idx = injector.find('> "/sys/bus/pci/devices/$pci_addr/remove"')
    assert wait_idx != -1 and remove_idx != -1
    assert wait_idx < remove_idx, "the enumeration wait must happen before any removal"
    assert "partial injection would silently void the repro" in injector
    assert "PCI devices still present after remove" in injector


def test_remove_mode_records_removed_pci_addresses_for_test_assertions():
    injector = _injector_body(_make_object(mode="remove").script_to_run)

    assert f"> {NVME_FAULT_INJECT_REMOVED_PCI_ADDRS_FILE}" in injector


def test_remove_mode_records_expected_count_on_first_boot_only():
    script = _make_object(mode="remove").script_to_run
    first_boot_tail = script[script.find("INJECTOR_EOF", script.find("INJECTOR_EOF") + 1) :]

    assert f'echo "$expected" > {NVME_FAULT_INJECT_EXPECTED_CONTROLLERS_FILE}' in first_boot_tail
    # same first-boot invariants as unbind mode: marker/mount teardown stays deferred
    # to the injector unit, and nothing reboots the node
    assert "rm -f /etc/scylla/machine_image_configured" not in first_boot_tail
    assert "shutdown" not in script
    assert "systemctl reboot" not in script


def test_remove_mode_keeps_unbind_service_scaffolding():
    script = _make_object(mode="remove").script_to_run

    assert "DefaultDependencies=no" in script
    assert "After=systemd-remount-fs.service" in script
    assert "Before=local-fs-pre.target scylla-image-setup.service" in script
    assert f"systemctl enable {NVME_FAULT_INJECT_SERVICE}" in script
    assert "cat > /etc/systemd/system/scylla-image-setup.service.d/override.conf" in script


@pytest.mark.parametrize("mode", ["unbind", "remove"], ids=["unbind", "remove"])
def test_generated_scripts_are_valid_bash(mode, tmp_path):
    """Both the first-boot cloud-init script (run via `bash -eux`) and the embedded
    injector heredoc (run via its own #!/bin/bash shebang) must parse."""
    script = _make_object(mode=mode).script_to_run

    first_boot = tmp_path / "first_boot.sh"
    first_boot.write_text(script)
    subprocess.run(["bash", "-n", str(first_boot)], check=True)

    injector = _injector_body(script)
    injector_file = tmp_path / "injector.sh"
    injector_file.write_text(injector[injector.find("\n") + 1 :])
    subprocess.run(["bash", "-n", str(injector_file)], check=True)
