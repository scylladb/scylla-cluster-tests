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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for sdcm.utils.nvme module.

Tests cover all parsing logic with sample command outputs for both
JSON and human-readable formats across different nvme-cli versions.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, PropertyMock


from sdcm.cluster import BaseScyllaCluster
from sdcm.sct_events import Severity
from sdcm.utils.nvme_diagnostics import (
    ERROR_LOG_ENTRY_LEN,
    IDCTRL_ELPE_OFFSET,
    IDCTRL_OACS_OFFSET,
    IDENTIFY_CONTROLLER_LEN,
    LOG_PAGE_ERROR_INFORMATION,
    LOG_PAGE_SMART_HEALTH,
    OACS_DEVICE_SELF_TEST,
    SELF_TEST_LOG_PAGE_LEN,
    SELF_TEST_RESULT_LEN,
    SMART_LOG_PAGE_LEN,
    NvmeDevice,
    NvmeSelfTestLog,
    NvmeSelfTestResult,
    NvmeSmartLog,
    SelfTestType,
    _check_single_device_health,
    _collect_error_log_with_timestamp,
    abort_self_test,
    check_nvme_health,
    check_self_test_results,
    collect_all_smart_logs,
    error_log_entry_count,
    filter_data_disks,
    get_error_log,
    get_self_test_log,
    get_smart_log,
    install_nvme_cli,
    is_nvme_cli_available,
    list_nvme_devices,
    parse_error_log_entry,
    parse_error_log_page,
    parse_nvme_list_output,
    parse_self_test_log_page,
    parse_smart_log_page,
    poll_self_test_completion,
    run_self_test,
    run_self_test_on_all_devices,
    store_baseline_smart_logs,
    supports_self_test,
)


# ---------------------------------------------------------------------------
# Sample outputs for testing
# ---------------------------------------------------------------------------

NVME_LIST_JSON_V2 = json.dumps(
    {
        "Devices": [
            {
                "Namespaces": [
                    {
                        "DevicePath": "/dev/nvme0n1",
                        "ModelNumber": "Amazon Elastic Block Store",
                        "SerialNumber": "vol0abc123",
                        "Firmware": "1.0",
                        "PhysicalSize": 8589934592,
                        "UsedBytes": 4294967296,
                        "SectorSize": 512,
                    }
                ]
            },
            {
                "Namespaces": [
                    {
                        "DevicePath": "/dev/nvme1n1",
                        "ModelNumber": "Amazon EC2 NVMe Instance Storage",
                        "SerialNumber": "AWS1234567890",
                        "Firmware": "0",
                        "PhysicalSize": 1900000000000,
                        "UsedBytes": 1900000000000,
                        "SectorSize": 512,
                    }
                ]
            },
        ]
    }
)

NVME_LIST_JSON_FLAT = json.dumps(
    [
        {
            "DevicePath": "/dev/nvme0n1",
            "ModelNumber": "Samsung SSD 970 EVO Plus",
            "SerialNumber": "S4EWNX0N123456",
            "Firmware": "2B2QEXM7",
            "PhysicalSize": 500107862016,
            "UsedBytes": 250000000000,
            "SectorSize": 512,
        },
        {
            "DevicePath": "/dev/nvme1n1",
            "ModelNumber": "Samsung SSD 980 PRO",
            "SerialNumber": "S5GXNF0N789012",
            "Firmware": "5B2QGXA7",
            "PhysicalSize": 1000204886016,
            "UsedBytes": 600000000000,
            "SectorSize": 4096,
        },
    ]
)

NVME_LIST_EMPTY_JSON = json.dumps({"Devices": []})

# nvme-cli pads the entry index and prints hex values with a "0x" prefix
ERROR_LOG_TEXT_PADDED_OUTPUT = """\
Error Log Entries for device:nvme1n1 entries:2
Entry[ 0]
.................
error_count	: 7
sqid	: 0
cmdid	: 0x12
status_field	: 0x4004
parm_error_location	: 0
lba	: 0
nsid	: 0x1
vs	: 0
trtype	: 0
cs	: 0
opcode	: 0x2
.................
Entry[ 1]
.................
error_count	: 6
sqid	: 0
cmdid	: 0xa
status_field	: 0x4004
parm_error_location	: 0
lba	: 0x1000
nsid	: 0x1
vs	: 0
trtype	: 0
cs	: 0
opcode	: 0x1
.................
"""

# Format printed by nvme-cli itself: hex values and its own field names
SELF_TEST_LOG_TEXT_NVME_CLI_OUTPUT = """\
Device Self Test Log for NVME device:nvme1n1
Current operation  : 0x2
Current Completion : 67%
Self Test Result[0]:
  Operation Result             : 0x0
  Self Test Code               : 0x1
  Valid Diagnostic Information : 0x0
  Power on hours (POH)         : 0x2238
  Namespace Identifier         : 0x1
  Failing LBA                  : 0x0
  Status Code Type             : 0x0
  Status Code                  : 0x0
  Segment Number               : 0x0
Self Test Result[1]:
  Operation Result             : 0x7
  Self Test Code               : 0x2
  Valid Diagnostic Information : 0x1
  Power on hours (POH)         : 0x2000
  Namespace Identifier         : 0x1
  Failing LBA                  : 0x1234
  Status Code Type             : 0x1
  Status Code                  : 0x3
  Segment Number               : 0x2
"""

SELF_TEST_LOG_IN_PROGRESS_TEXT = """\
Device Self Test Log for NVME device:nvme1n1
Current operation  : 2
Current Completion : 67
"""


# ---------------------------------------------------------------------------
# Tests: parse_nvme_list_output
# ---------------------------------------------------------------------------


def test_parse_nvme_list_output_v2_format():
    """Parse nvme-cli 2.x format with Devices/Namespaces nesting."""
    devices = parse_nvme_list_output(NVME_LIST_JSON_V2)
    assert len(devices) == 2
    assert devices[0].device_path == "/dev/nvme0n1"
    assert devices[0].model == "Amazon Elastic Block Store"
    assert devices[0].serial == "vol0abc123"
    assert devices[0].size_bytes == 8589934592
    assert devices[1].device_path == "/dev/nvme1n1"
    assert devices[1].model == "Amazon EC2 NVMe Instance Storage"
    assert devices[1].size_bytes == 1900000000000


def test_parse_nvme_list_output_flat_format():
    """Parse older nvme-cli flat list format."""
    devices = parse_nvme_list_output(NVME_LIST_JSON_FLAT)
    assert len(devices) == 2
    assert devices[0].device_path == "/dev/nvme0n1"
    assert devices[0].model == "Samsung SSD 970 EVO Plus"
    assert devices[0].sector_size == 512
    assert devices[1].device_path == "/dev/nvme1n1"
    assert devices[1].sector_size == 4096


def test_parse_nvme_list_output_empty_devices():
    """Empty Devices list returns empty list."""
    devices = parse_nvme_list_output(NVME_LIST_EMPTY_JSON)
    assert devices == []


def test_parse_nvme_list_output_empty_string():
    """Empty string input returns empty list without error."""
    assert parse_nvme_list_output("") == []


def test_parse_nvme_list_output_none_input():
    """None input returns empty list without error."""
    assert parse_nvme_list_output(None) == []


def test_parse_nvme_list_output_invalid_json():
    """Invalid JSON returns empty list without error."""
    assert parse_nvme_list_output("not json at all") == []


def test_parse_nvme_list_output_whitespace_only():
    """Whitespace-only input returns empty list."""
    assert parse_nvme_list_output("   \n  ") == []


# ---------------------------------------------------------------------------
# Tests: parse_smart_log_output
# ---------------------------------------------------------------------------


def test_parse_smart_log_temperature_celsius_zero_kelvin():
    """Temperature conversion handles zero kelvin gracefully."""
    smart = NvmeSmartLog(device_path="/dev/nvme0n1", temperature_kelvin=0)
    assert smart.temperature_celsius == 0


# ---------------------------------------------------------------------------
# Tests: parse_error_log_output
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: parse_self_test_log_output
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: filter_data_disks
# ---------------------------------------------------------------------------


def test_filter_data_disks_excludes_ebs():
    """EBS volumes (Amazon Elastic Block Store) are excluded."""
    devices = [
        NvmeDevice(
            device_path="/dev/nvme0n1",
            model="Amazon Elastic Block Store",
            serial="vol0abc",
            firmware="1.0",
            size_bytes=8589934592,
            used_bytes=4294967296,
            sector_size=512,
        ),
        NvmeDevice(
            device_path="/dev/nvme1n1",
            model="Amazon EC2 NVMe Instance Storage",
            serial="AWS123",
            firmware="0",
            size_bytes=1900000000000,
            used_bytes=1900000000000,
            sector_size=512,
        ),
    ]
    data_disks = filter_data_disks(devices)
    assert len(data_disks) == 1
    assert data_disks[0].device_path == "/dev/nvme1n1"
    assert data_disks[0].is_data_disk is True


def test_filter_data_disks_excludes_gce_pd():
    """GCE Persistent Disk is excluded."""
    devices = [
        NvmeDevice(
            device_path="/dev/nvme0n1",
            model="Google PersistentDisk",
            serial="gce-boot",
            firmware="1",
            size_bytes=10737418240,
            used_bytes=5000000000,
            sector_size=512,
        ),
        NvmeDevice(
            device_path="/dev/nvme1n1",
            model="nvme_card",
            serial="local-ssd-0",
            firmware="1",
            size_bytes=375809638400,
            used_bytes=375809638400,
            sector_size=512,
        ),
    ]
    data_disks = filter_data_disks(devices)
    assert len(data_disks) == 1
    assert data_disks[0].device_path == "/dev/nvme1n1"


def test_filter_data_disks_excludes_azure_managed():
    """Azure managed disks are excluded."""
    devices = [
        NvmeDevice(
            device_path="/dev/nvme0n1",
            model="Msft Virtual Disk",
            serial="azure-boot",
            firmware="1",
            size_bytes=34359738368,
            used_bytes=10000000000,
            sector_size=512,
        ),
    ]
    data_disks = filter_data_disks(devices)
    assert data_disks == []


def test_filter_data_disks_explicit_boot_device():
    """Explicit boot device path is excluded."""
    devices = [
        NvmeDevice(
            device_path="/dev/nvme0n1",
            model="Samsung SSD 970",
            serial="ABC",
            firmware="1",
            size_bytes=500000000000,
            used_bytes=250000000000,
            sector_size=512,
        ),
        NvmeDevice(
            device_path="/dev/nvme1n1",
            model="Samsung SSD 980",
            serial="DEF",
            firmware="1",
            size_bytes=1000000000000,
            used_bytes=500000000000,
            sector_size=512,
        ),
    ]
    data_disks = filter_data_disks(devices, boot_device_path="/dev/nvme0n1")
    assert len(data_disks) == 1
    assert data_disks[0].device_path == "/dev/nvme1n1"


def test_filter_data_disks_empty_list():
    """Empty device list returns empty list."""
    assert filter_data_disks([]) == []


def test_filter_data_disks_all_data():
    """All devices are data disks when none match boot indicators."""
    devices = [
        NvmeDevice(
            device_path="/dev/nvme0n1",
            model="Samsung SSD 970",
            serial="ABC",
            firmware="1",
            size_bytes=500000000000,
            used_bytes=250000000000,
            sector_size=512,
        ),
        NvmeDevice(
            device_path="/dev/nvme1n1",
            model="Intel P4510",
            serial="DEF",
            firmware="1",
            size_bytes=2000000000000,
            used_bytes=1000000000000,
            sector_size=4096,
        ),
    ]
    data_disks = filter_data_disks(devices)
    assert len(data_disks) == 2
    assert all(d.is_data_disk for d in data_disks)


# ---------------------------------------------------------------------------
# Tests: SelfTestType enum
# ---------------------------------------------------------------------------


def test_self_test_type_values():
    """SelfTestType enum has correct integer values."""
    assert int(SelfTestType.SHORT) == 1
    assert int(SelfTestType.EXTENDED) == 2


# ---------------------------------------------------------------------------
# Tests: Node-level command wrappers (mock remoter)
# ---------------------------------------------------------------------------


def _make_mock_node():
    """Create a mock node with remoter and log."""
    node = MagicMock()
    node.log = MagicMock()
    node.remoter = MagicMock()
    return node


def _make_result(stdout="", stderr="", exited=0):
    """Create a mock command result."""
    result = MagicMock()
    result.stdout = stdout
    result.stderr = stderr
    result.exited = exited
    type(result).ok = PropertyMock(return_value=(exited == 0))
    type(result).failed = PropertyMock(return_value=(exited != 0))
    return result


def test_install_nvme_cli_already_installed():
    """Skip installation if nvme is already available."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")

    result = install_nvme_cli(node)
    assert result is True
    node.install_package.assert_not_called()


def test_install_nvme_cli_installs_successfully():
    """Install nvme-cli when not present, verify success."""
    node = _make_mock_node()
    # First call: not installed. Second call: installed after install_package.
    node.remoter.run.side_effect = [
        _make_result(exited=1),  # which nvme -> not found
        _make_result(stdout="/usr/sbin/nvme"),  # which nvme -> found after install
    ]

    result = install_nvme_cli(node)
    assert result is True
    node.install_package.assert_called_once_with("nvme-cli", ignore_status=True)


def test_install_nvme_cli_install_fails():
    """Return False when installation fails."""
    node = _make_mock_node()
    node.remoter.run.side_effect = [
        _make_result(exited=1),  # which nvme -> not found
        _make_result(exited=1),  # which nvme -> still not found after install
    ]

    result = install_nvme_cli(node)
    assert result is False


def test_install_nvme_cli_exception_during_install():
    """Return False when install_package raises an exception."""
    node = _make_mock_node()
    node.remoter.run.side_effect = [
        _make_result(exited=1),  # which nvme -> not found
    ]
    node.install_package.side_effect = RuntimeError("apt lock timeout")

    result = install_nvme_cli(node)
    assert result is False
    node.log.warning.assert_called()


def test_is_nvme_cli_available_true():
    """Return True when nvme binary exists."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")
    assert is_nvme_cli_available(node) is True


def test_is_nvme_cli_available_false():
    """Return False when nvme binary does not exist."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(exited=1)
    assert is_nvme_cli_available(node) is False


def test_list_nvme_devices_returns_devices():
    """list_nvme_devices returns parsed device list."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")
    node.remoter.sudo.return_value = _make_result(stdout=NVME_LIST_JSON_FLAT)

    devices = list_nvme_devices(node)
    assert len(devices) == 2
    assert devices[0].device_path == "/dev/nvme0n1"


def test_list_nvme_devices_no_nvme_cli():
    """Return empty list when nvme-cli is not installed."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(exited=1)

    devices = list_nvme_devices(node)
    assert devices == []


def test_list_nvme_devices_command_fails():
    """Return empty list when nvme list command fails."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="permission denied")

    devices = list_nvme_devices(node)
    assert devices == []


def test_get_smart_log_failure():
    """get_smart_log returns None when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="device not found")

    smart = get_smart_log(node, "/dev/nvme99n1")
    assert smart is None


def test_get_error_log_failure():
    """get_error_log returns empty list when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    entries = get_error_log(node, "/dev/nvme99n1")
    assert entries == []


def test_run_self_test_success(monkeypatch):
    """run_self_test returns True on success."""
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.supports_self_test", lambda n, d: True)
    node.remoter.sudo.return_value = _make_result()

    result = run_self_test(node, "/dev/nvme1n1", SelfTestType.SHORT)
    assert result is True
    node.remoter.sudo.assert_called_once_with(
        "nvme device-self-test -s 1 /dev/nvme1n1",
        ignore_status=True,
        timeout=30,
    )


def test_run_self_test_extended(monkeypatch):
    """run_self_test passes correct type for extended test."""
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.supports_self_test", lambda n, d: True)
    node.remoter.sudo.return_value = _make_result()

    result = run_self_test(node, "/dev/nvme1n1", SelfTestType.EXTENDED)
    assert result is True
    node.remoter.sudo.assert_called_once_with(
        "nvme device-self-test -s 2 /dev/nvme1n1",
        ignore_status=True,
        timeout=30,
    )


def test_run_self_test_command_rejected(monkeypatch):
    """run_self_test returns False when a supported controller rejects the command."""
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.supports_self_test", lambda n, d: True)
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="not supported")

    result = run_self_test(node, "/dev/nvme1n1")
    assert result is False


def test_run_self_test_skipped_when_controller_lacks_support(monkeypatch):
    """The self-test command is never issued to a controller that does not implement it.

    Issuing it anyway records an entry in the device Error Information Log and
    increments num_err_log_entries, so the diagnostic would report an anomaly
    it created itself.
    """
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.supports_self_test", lambda n, d: False)

    assert run_self_test(node, "/dev/nvme1n1") is False
    node.remoter.sudo.assert_not_called()


def test_abort_self_test_success():
    """abort_self_test sends abort command (code 0xf) successfully."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result()

    result = abort_self_test(node, "/dev/nvme1n1")
    assert result is True
    node.remoter.sudo.assert_called_once_with(
        "nvme device-self-test -s 0xf /dev/nvme1n1",
        ignore_status=True,
        timeout=30,
    )


def test_abort_self_test_failure():
    """abort_self_test returns False when abort command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="no test running")

    result = abort_self_test(node, "/dev/nvme1n1")
    assert result is False
    node.log.warning.assert_called()


def test_get_self_test_log_failure():
    """get_self_test_log returns None when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    log = get_self_test_log(node, "/dev/nvme1n1")
    assert log is None


def test_collect_all_smart_logs_no_devices():
    """collect_all_smart_logs returns empty list when no devices found."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")
    node.remoter.sudo.return_value = _make_result(stdout=NVME_LIST_EMPTY_JSON)

    smart_logs = collect_all_smart_logs(node)
    assert smart_logs == []


def test_collect_all_smart_logs_no_nvme_cli():
    """collect_all_smart_logs returns empty list when nvme-cli not installed."""
    node = _make_mock_node()
    node.remoter.run.return_value = _make_result(exited=1)

    smart_logs = collect_all_smart_logs(node)
    assert smart_logs == []


# ---------------------------------------------------------------------------
# Tests: _setup_nvme_diagnostics (cluster integration)
# ---------------------------------------------------------------------------


def _make_mock_cluster(collect_nvme_diagnostics=True):
    """Create a mock cluster object with params and the _setup_nvme_diagnostics method."""
    cluster = MagicMock(spec=BaseScyllaCluster)
    cluster.params = MagicMock()
    cluster.params.get = MagicMock(
        side_effect=lambda key, *a, **kw: {
            "collect_nvme_diagnostics": collect_nvme_diagnostics,
        }.get(key)
    )
    # Bind the real method to the mock so we test actual logic
    cluster._setup_nvme_diagnostics = BaseScyllaCluster._setup_nvme_diagnostics.__get__(cluster)
    return cluster


def test_setup_nvme_diagnostics_installs_and_logs_baseline(monkeypatch):
    """_setup_nvme_diagnostics installs nvme-cli and logs baseline SMART data."""
    node = _make_mock_node()
    cluster = _make_mock_cluster()

    smart = NvmeSmartLog(
        device_path="/dev/nvme1n1",
        temperature_kelvin=315,
        available_spare=100,
        percentage_used=2,
        media_errors=0,
        num_err_log_entries=0,
        power_on_hours=8760,
    )
    monkeypatch.setattr("sdcm.cluster.install_nvme_cli", lambda n: True)
    monkeypatch.setattr("sdcm.cluster.collect_all_smart_logs", lambda n: [smart])

    cluster._setup_nvme_diagnostics(node)

    # Verify baseline was logged
    node.log.info.assert_called()
    logged_args = node.log.info.call_args_list[-1]
    assert "/dev/nvme1n1" in str(logged_args)
    assert "8760" in str(logged_args)


def test_setup_nvme_diagnostics_skips_when_install_fails(monkeypatch):
    """_setup_nvme_diagnostics skips gracefully when nvme-cli install fails."""
    node = _make_mock_node()
    cluster = _make_mock_cluster()

    monkeypatch.setattr("sdcm.cluster.install_nvme_cli", lambda n: False)
    collect_mock = MagicMock()
    monkeypatch.setattr("sdcm.cluster.collect_all_smart_logs", collect_mock)

    cluster._setup_nvme_diagnostics(node)

    # collect_all_smart_logs should not be called
    collect_mock.assert_not_called()
    # Should log that it's skipping
    node.log.info.assert_called()
    assert "skipping" in str(node.log.info.call_args_list[0]).lower()


def test_setup_nvme_diagnostics_skips_when_no_data_disks(monkeypatch):
    """_setup_nvme_diagnostics skips gracefully when no NVMe data disks found."""
    node = _make_mock_node()
    cluster = _make_mock_cluster()

    monkeypatch.setattr("sdcm.cluster.install_nvme_cli", lambda n: True)
    monkeypatch.setattr("sdcm.cluster.collect_all_smart_logs", lambda n: [])

    cluster._setup_nvme_diagnostics(node)

    # Should log that no data disks were found
    node.log.info.assert_called()
    assert "skipping" in str(node.log.info.call_args_list[0]).lower()


# ---------------------------------------------------------------------------
# Tests: _check_single_device_health (threshold logic)
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS = {
    "percentage_used_warning": 90,
    "temperature_warning_celsius": 70,
}


def _make_smart_log(**overrides) -> NvmeSmartLog:
    """Create an NvmeSmartLog with healthy defaults, overridable per field."""
    defaults = {
        "device_path": "/dev/nvme1n1",
        "critical_warning": 0,
        "temperature_kelvin": 310,  # 37°C
        "available_spare": 100,
        "available_spare_threshold": 10,
        "percentage_used": 2,
        "media_errors": 0,
        "num_err_log_entries": 0,
        "power_on_hours": 1000,
    }
    defaults.update(overrides)
    return NvmeSmartLog(**defaults)


def test_threshold_healthy_device_yields_nothing():
    """A healthy device yields no events."""
    node = _make_mock_node()
    smart = _make_smart_log()
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))
    assert events == []


def test_threshold_critical_warning_yields_critical():
    """critical_warning != 0 yields a CRITICAL event."""
    node = _make_mock_node()
    smart = _make_smart_log(critical_warning=4)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    critical_events = [e for e in events if e.severity == Severity.CRITICAL]
    assert len(critical_events) == 1
    assert "critical_warning=4" in critical_events[0].error


def test_threshold_media_errors_yields_error():
    """media_errors > 0 yields an ERROR event."""
    node = _make_mock_node()
    node.logdir = "/tmp/opencode/test_logdir"
    smart = _make_smart_log(media_errors=42)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    error_events = [e for e in events if e.severity == Severity.ERROR]
    assert len(error_events) == 1
    assert "42 new media_errors" in error_events[0].error


def test_threshold_error_log_entries_yields_warning():
    """num_err_log_entries > 0 yields a WARNING event."""
    node = _make_mock_node()
    node.logdir = "/tmp/opencode/test_logdir"
    smart = _make_smart_log(num_err_log_entries=5)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    warning_events = [e for e in events if e.severity == Severity.WARNING]
    assert len(warning_events) == 1
    assert "5 new error log entries" in warning_events[0].message


def test_threshold_percentage_used_above_threshold_yields_warning():
    """percentage_used above threshold yields a WARNING event."""
    node = _make_mock_node()
    smart = _make_smart_log(percentage_used=95)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    warning_events = [e for e in events if e.severity == Severity.WARNING]
    assert len(warning_events) == 1
    assert "percentage_used=95%" in warning_events[0].message


def test_threshold_percentage_used_at_threshold_no_event():
    """percentage_used exactly at threshold does not yield an event."""
    node = _make_mock_node()
    smart = _make_smart_log(percentage_used=90)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))
    assert events == []


def test_threshold_available_spare_below_device_threshold_yields_warning():
    """available_spare below the device's own spare_threshold yields a WARNING."""
    node = _make_mock_node()
    smart = _make_smart_log(available_spare=5, available_spare_threshold=10)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    warning_events = [e for e in events if e.severity == Severity.WARNING]
    assert len(warning_events) == 1
    assert "available_spare=5%" in warning_events[0].message


def test_threshold_temperature_above_threshold_yields_warning():
    """Temperature above threshold yields a WARNING event."""
    node = _make_mock_node()
    smart = _make_smart_log(temperature_kelvin=348)  # 75°C
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    warning_events = [e for e in events if e.severity == Severity.WARNING]
    assert len(warning_events) == 1
    assert "temperature=75" in warning_events[0].message


def test_threshold_temperature_at_threshold_no_event():
    """Temperature exactly at threshold does not yield an event."""
    node = _make_mock_node()
    smart = _make_smart_log(temperature_kelvin=343)  # 70°C
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))
    assert events == []


def test_threshold_multiple_issues_yields_multiple_events():
    """A device with multiple issues yields one event per issue."""
    node = _make_mock_node()
    node.logdir = "/tmp/opencode/test_logdir"
    smart = _make_smart_log(
        critical_warning=1,
        media_errors=10,
        num_err_log_entries=20,
        percentage_used=95,
    )
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    # critical_warning -> CRITICAL, media_errors -> ERROR,
    # num_err_log_entries -> WARNING, percentage_used -> WARNING
    assert len(events) == 4
    severities = [e.severity for e in events]
    assert Severity.CRITICAL in severities
    assert Severity.ERROR in severities
    assert severities.count(Severity.WARNING) == 2


def test_threshold_custom_thresholds_override_defaults():
    """Custom thresholds override the default values."""
    node = _make_mock_node()
    smart = _make_smart_log(percentage_used=50, temperature_kelvin=333)  # 60°C
    custom = {"percentage_used_warning": 40, "temperature_warning_celsius": 55}
    events = list(_check_single_device_health(node, smart, custom))

    # Both should trigger with lowered thresholds
    assert len(events) == 2


# ---------------------------------------------------------------------------
# Tests: check_nvme_health (full generator)
# ---------------------------------------------------------------------------


def test_check_nvme_health_disabled_by_config(monkeypatch):
    """check_nvme_health yields nothing when collect_nvme_diagnostics is False."""
    node = _make_mock_node()
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.return_value = False

    events = list(check_nvme_health(current_node=node))
    assert events == []


def test_check_nvme_health_no_nvme_cli(monkeypatch):
    """check_nvme_health yields nothing when nvme-cli not available."""
    node = _make_mock_node()
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.return_value = True
    node.remoter.run.return_value = _make_result(exited=1)  # which nvme -> not found

    events = list(check_nvme_health(current_node=node))
    assert events == []


def test_check_nvme_health_healthy_device(monkeypatch):
    """check_nvme_health yields nothing for a healthy device."""
    node = _make_mock_node()
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.return_value = True

    smart = _make_smart_log()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.collect_all_smart_logs", lambda n: [smart])

    events = list(check_nvme_health(current_node=node))
    assert events == []


def test_check_nvme_health_yields_events_for_errors(monkeypatch):
    """check_nvme_health yields events when SMART data shows errors."""
    node = _make_mock_node()
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.return_value = True
    node.logdir = "/tmp/opencode/test_logdir"

    smart = _make_smart_log(media_errors=5)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.collect_all_smart_logs", lambda n: [smart])
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_error_log", lambda n, d, **kw: [])

    events = list(check_nvme_health(current_node=node))
    assert len(events) == 1
    assert events[0].severity == Severity.ERROR


# ---------------------------------------------------------------------------
# Tests: poll_self_test_completion
# ---------------------------------------------------------------------------


def test_poll_self_test_completion_immediate(monkeypatch):
    """poll_self_test_completion returns immediately when test is not in progress."""
    node = _make_mock_node()
    completed_log = NvmeSelfTestLog(device_path="/dev/nvme0n1", current_operation=0)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_self_test_log", lambda n, d: completed_log)

    result = poll_self_test_completion(node, "/dev/nvme0n1", timeout=10)
    assert result is not None
    assert not result.test_in_progress


def test_poll_self_test_completion_waits_then_completes(monkeypatch):
    """poll_self_test_completion polls until test completes."""
    node = _make_mock_node()
    in_progress_log = NvmeSelfTestLog(device_path="/dev/nvme0n1", current_operation=1, current_completion=50)
    completed_log = NvmeSelfTestLog(device_path="/dev/nvme0n1", current_operation=0)

    call_count = 0

    def mock_get_self_test_log(n, d):
        nonlocal call_count
        call_count += 1
        return completed_log if call_count >= 3 else in_progress_log

    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_self_test_log", mock_get_self_test_log)
    # Use a very short poll interval for testing
    result = poll_self_test_completion(node, "/dev/nvme0n1", timeout=60, poll_interval=0)
    assert result is not None
    assert not result.test_in_progress
    assert call_count >= 3


def test_poll_self_test_completion_timeout_aborts(monkeypatch):
    """poll_self_test_completion aborts the test on timeout."""
    node = _make_mock_node()
    in_progress_log = NvmeSelfTestLog(device_path="/dev/nvme0n1", current_operation=1, current_completion=10)

    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_self_test_log", lambda n, d: in_progress_log)
    # Mock abort_self_test to track it was called
    abort_called = []
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.abort_self_test", lambda n, d: abort_called.append(d) or True)

    poll_self_test_completion(node, "/dev/nvme0n1", timeout=0, poll_interval=0)
    assert len(abort_called) == 1
    assert abort_called[0] == "/dev/nvme0n1"


# ---------------------------------------------------------------------------
# Tests: check_self_test_results
# ---------------------------------------------------------------------------


def test_check_self_test_results_passed():
    """No events for a passing self-test."""
    node = _make_mock_node()
    log = NvmeSelfTestLog(
        device_path="/dev/nvme0n1",
        results=[NvmeSelfTestResult(result_code=0, self_test_code=1)],
    )
    events = list(check_self_test_results(node, log))
    assert events == []


def test_check_self_test_results_failure():
    """ERROR event for a self-test failure with result_code >= 4."""
    node = _make_mock_node()
    node.name = "test-node"
    log = NvmeSelfTestLog(
        device_path="/dev/nvme0n1",
        results=[NvmeSelfTestResult(result_code=4, self_test_code=1, failing_lba=0x1000)],
    )
    events = list(check_self_test_results(node, log))
    assert len(events) == 1
    assert events[0].severity == Severity.ERROR
    assert "self-test failed" in events[0].error


def test_check_self_test_results_aborted():
    """No event for user-aborted self-test (result_code=1)."""
    node = _make_mock_node()
    log = NvmeSelfTestLog(
        device_path="/dev/nvme0n1",
        results=[NvmeSelfTestResult(result_code=1, self_test_code=1)],
    )
    events = list(check_self_test_results(node, log))
    assert events == []


def test_check_self_test_results_empty():
    """No events when no results in self-test log."""
    node = _make_mock_node()
    log = NvmeSelfTestLog(device_path="/dev/nvme0n1", results=[])
    events = list(check_self_test_results(node, log))
    assert events == []


def test_check_self_test_results_entry_not_used():
    """No events for result_code=0xf (entry not used)."""
    node = _make_mock_node()
    log = NvmeSelfTestLog(
        device_path="/dev/nvme0n1",
        results=[NvmeSelfTestResult(result_code=0xF)],
    )
    events = list(check_self_test_results(node, log))
    assert events == []


# ---------------------------------------------------------------------------
# Tests: run_self_test_on_all_devices
# ---------------------------------------------------------------------------


def test_run_self_test_on_all_devices_no_nvme_cli(monkeypatch):
    """Returns empty list when nvme-cli is not available."""
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.is_nvme_cli_available", lambda n: False)

    result = run_self_test_on_all_devices(node)
    assert result == []


def test_run_self_test_on_all_devices_triggers_all_before_polling(monkeypatch):
    """Self-tests are triggered on all disks first, so they run in parallel."""
    node = _make_mock_node()
    disks = [
        NvmeDevice(
            device_path=f"/dev/nvme{i}n1",
            model="Amazon EC2 NVMe Instance Storage",
            serial=f"AWS{i}",
            firmware="0",
            size_bytes=1900000000000,
            used_bytes=0,
            sector_size=512,
            is_data_disk=True,
        )
        for i in range(3)
    ]
    calls = []

    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.list_nvme_devices", lambda n: disks)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.filter_data_disks", lambda devices, **kwargs: disks)
    monkeypatch.setattr(
        "sdcm.utils.nvme_diagnostics.run_self_test",
        lambda n, device_path, test_type: calls.append(("trigger", device_path)) is None,
    )
    monkeypatch.setattr(
        "sdcm.utils.nvme_diagnostics.poll_self_test_completion",
        lambda n, device_path, timeout=None, test_type=None: calls.append(("poll", device_path)),
    )

    result = run_self_test_on_all_devices(node, SelfTestType.SHORT)

    assert result == []
    assert [device_path for action, device_path in calls if action == "trigger"] == [d.device_path for d in disks]
    assert [action for action, _ in calls] == ["trigger"] * 3 + ["poll"] * 3


def test_run_self_test_on_all_devices_no_devices(monkeypatch):
    """Returns empty list when no NVMe devices are found."""
    node = _make_mock_node()
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.list_nvme_devices", lambda n: [])

    result = run_self_test_on_all_devices(node)
    assert result == []


# ---------------------------------------------------------------------------
# Regression tests: parsing contract
#
# The sample below is a verbatim excerpt of "nvme error-log /dev/nvme1n1 -e 64"
# from an i4i.4xlarge perf run. Both populated entries carry a status_field with
# a trailing description, which an end-of-line-anchored value regex silently
# dropped - the collected artifact reported status=0x0000 for every entry while
# the device had reported 0x2001 and 0x2002.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Regression tests: temperature unit handling
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: supports_self_test
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: JSON is the preferred output contract
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: baseline delta reporting
# ---------------------------------------------------------------------------


def test_error_entries_present_at_baseline_yield_no_event():
    """Entries that already existed before the test are not reported as new.

    A fresh cloud instance can arrive with a non-empty error log, so an absolute
    "num_err_log_entries > 0" check fires on healthy nodes on every run.
    """
    node = _make_mock_node()
    node.logdir = None
    store_baseline_smart_logs(node, [_make_smart_log(num_err_log_entries=2, media_errors=1)])
    smart = _make_smart_log(num_err_log_entries=2, media_errors=1)

    assert list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS)) == []


def test_error_entries_above_baseline_yield_warning(monkeypatch):
    """Only growth over the baseline is reported, and it reports the delta."""
    node = _make_mock_node()
    node.logdir = None
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_error_log", lambda n, d, **kw: [])
    store_baseline_smart_logs(node, [_make_smart_log(num_err_log_entries=2)])
    smart = _make_smart_log(num_err_log_entries=5)

    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    assert len(events) == 1
    assert events[0].severity == Severity.WARNING
    assert "3 new error log entries" in events[0].message
    assert "total=5" in events[0].message


def test_missing_baseline_falls_back_to_absolute_count(monkeypatch):
    """With no baseline captured, every entry counts as new."""
    node = _make_mock_node()
    node.logdir = None
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_error_log", lambda n, d, **kw: [])
    smart = _make_smart_log(num_err_log_entries=3)

    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    assert len(events) == 1
    assert "3 new error log entries" in events[0].message


# ---------------------------------------------------------------------------
# Tests: collected error log artifact
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Raw log page builders
#
# Pages are assembled at the offsets the NVMe Base Specification defines, so a
# test failure means a parser offset drifted from the spec, not that a fixture
# was captured from a differently-behaved nvme-cli.
# ---------------------------------------------------------------------------


def _put(buf: bytearray, offset: int, value: int, size: int) -> None:
    buf[offset : offset + size] = value.to_bytes(size, "little")


def _make_smart_page(**fields) -> bytes:
    """Build a 512-byte SMART / Health Information log page (02h)."""
    layout = {
        "critical_warning": (0, 1),
        "temperature_kelvin": (1, 2),
        "available_spare": (3, 1),
        "available_spare_threshold": (4, 1),
        "percentage_used": (5, 1),
        "data_units_read": (32, 16),
        "data_units_written": (48, 16),
        "host_read_commands": (64, 16),
        "host_write_commands": (80, 16),
        "controller_busy_time": (96, 16),
        "power_cycles": (112, 16),
        "power_on_hours": (128, 16),
        "unsafe_shutdowns": (144, 16),
        "media_errors": (160, 16),
        "num_err_log_entries": (176, 16),
    }
    buf = bytearray(SMART_LOG_PAGE_LEN)
    for name, value in fields.items():
        offset, size = layout[name]
        _put(buf, offset, value, size)
    return bytes(buf)


def _make_error_entry(**fields) -> bytes:
    """Build one 64-byte Error Information Log entry (01h).

    ``status_field`` and ``phase_tag`` are given separately and packed into the
    single on-wire field, where bits 15:1 are the status and bit 0 the phase tag.
    """
    layout = {
        "error_count": (0, 8),
        "sqid": (8, 2),
        "cmdid": (10, 2),
        "parm_error_location": (14, 2),
        "lba": (16, 8),
        "nsid": (24, 4),
        "vs": (28, 1),
        "trtype": (29, 1),
        "csi": (30, 1),
        "opcode": (31, 1),
        "cs": (32, 8),
        "log_page_version": (63, 1),
    }
    buf = bytearray(ERROR_LOG_ENTRY_LEN)
    _put(buf, 0, fields.pop("error_count", 1), 8)
    status_field = fields.pop("status_field", 0)
    phase_tag = fields.pop("phase_tag", 0)
    _put(buf, 12, (status_field << 1) | phase_tag, 2)
    for name, value in fields.items():
        offset, size = layout[name]
        _put(buf, offset, value, size)
    return bytes(buf)


def _make_self_test_page(current_operation=0, current_completion=0, results=()) -> bytes:
    """Build a 564-byte Device Self-test log page (06h)."""
    buf = bytearray(SELF_TEST_LOG_PAGE_LEN)
    buf[0] = current_operation
    buf[1] = current_completion
    # Unused result slots are marked 0xf ("entry not used").
    for index in range(20):
        buf[4 + index * SELF_TEST_RESULT_LEN] = 0x0F
    for index, res in enumerate(results):
        offset = 4 + index * SELF_TEST_RESULT_LEN
        buf[offset] = (res.get("self_test_code", 1) << 4) | res.get("result_code", 0)
        buf[offset + 1] = res.get("segment_number", 0)
        _put(buf, offset + 4, res.get("power_on_hours", 0), 8)
        _put(buf, offset + 12, res.get("nsid", 0), 4)
        _put(buf, offset + 16, res.get("failing_lba", 0), 8)
        buf[offset + 24] = res.get("status_code_type", 0)
        buf[offset + 25] = res.get("status_code", 0)
    return bytes(buf)


def _make_id_ctrl(oacs=0, elpe_zero_based=63) -> bytes:
    buf = bytearray(IDENTIFY_CONTROLLER_LEN)
    _put(buf, IDCTRL_OACS_OFFSET, oacs, 2)
    buf[IDCTRL_ELPE_OFFSET] = elpe_zero_based
    return bytes(buf)


def _b64_result(payload: bytes):
    return _make_result(stdout=base64.b64encode(payload).decode())


# ---------------------------------------------------------------------------
# Tests: SMART page parsing
# ---------------------------------------------------------------------------


def test_parse_smart_page_reads_spec_offsets():
    """Every field is decoded from its spec-defined offset."""
    page = _make_smart_page(
        critical_warning=0x1,
        temperature_kelvin=315,
        available_spare=100,
        available_spare_threshold=10,
        percentage_used=2,
        data_units_read=8041270,
        data_units_written=12219712,
        host_read_commands=52368094,
        host_write_commands=49760027,
        controller_busy_time=120,
        power_cycles=15,
        power_on_hours=8760,
        unsafe_shutdowns=3,
        media_errors=10,
        num_err_log_entries=5,
    )
    smart = parse_smart_log_page("/dev/nvme1n1", page)

    assert smart.device_path == "/dev/nvme1n1"
    assert smart.critical_warning == 1
    assert smart.temperature_kelvin == 315
    assert smart.temperature_celsius == 42
    assert smart.available_spare == 100
    assert smart.available_spare_threshold == 10
    assert smart.percentage_used == 2
    assert smart.data_units_read == 8041270
    assert smart.data_units_written == 12219712
    assert smart.host_read_commands == 52368094
    assert smart.host_write_commands == 49760027
    assert smart.controller_busy_time == 120
    assert smart.power_cycles == 15
    assert smart.power_on_hours == 8760
    assert smart.unsafe_shutdowns == 3
    assert smart.media_errors == 10
    assert smart.num_err_log_entries == 5


def test_parse_smart_page_temperature_is_kelvin_by_definition():
    """Composite Temperature is Kelvin on the wire, so no unit guessing is possible.

    The text parser had to infer the unit, and nvme-cli 1.x/2.x disagree on the
    order ("315 K (42 Celsius)" vs "42 C (315 K, 107 F)"), which silently made
    the over-temperature check unreachable.
    """
    assert parse_smart_log_page("/dev/nvme1n1", _make_smart_page(temperature_kelvin=315)).temperature_celsius == 42


def test_parse_smart_page_handles_128_bit_counters():
    """The 16-byte counters are decoded whole, not truncated to 64 bits."""
    huge = 2**100 + 12345
    smart = parse_smart_log_page("/dev/nvme1n1", _make_smart_page(data_units_read=huge))
    assert smart.data_units_read == huge


# ---------------------------------------------------------------------------
# Tests: error log page parsing
# ---------------------------------------------------------------------------


def test_parse_error_entry_splits_status_field_and_phase_tag():
    """Bits 15:1 are the status field, bit 0 the phase tag.

    0x2001 is the status the AWS Nitro controller reported for the rejected
    'nvme device-self-test' - the value the previous text parser zeroed out.
    """
    entry = parse_error_log_entry(_make_error_entry(error_count=2, status_field=0x2001, phase_tag=1))

    assert entry.status_field == 0x2001
    assert entry.phase_tag == 1


def test_parse_error_entry_reads_spec_offsets():
    entry = parse_error_log_entry(
        _make_error_entry(
            error_count=2,
            sqid=0,
            cmdid=0x4,
            status_field=0x2001,
            parm_error_location=0x105,
            lba=0xDEADBEEF,
            nsid=0xFFFFFFFF,
            vs=0x80,
            trtype=1,
            cs=0x1234,
        )
    )

    assert entry.error_count == 2
    assert entry.command_id == 0x4
    assert entry.parm_error_location == 0x105
    assert entry.lba == 0xDEADBEEF
    assert entry.nsid == 0xFFFFFFFF
    assert entry.vendor_specific == 0x80
    assert entry.transport_type == 1
    assert entry.command_specific == 0x1234


def test_parse_error_entry_opcode_requires_log_page_version_1():
    """csi and opcode are reserved bytes unless Log Page Version is 1h.

    All 64 entries captured from an i4i node report log_page_version 0, so the
    'opcode=0x00' nvme-cli prints there is a reserved byte, not an opcode.
    """
    without = parse_error_log_entry(_make_error_entry(csi=0x2, opcode=0x14, log_page_version=0))
    assert without.opcode is None
    assert without.command_set_indicator is None

    with_version = parse_error_log_entry(_make_error_entry(csi=0x2, opcode=0x14, log_page_version=1))
    assert with_version.opcode == 0x14
    assert with_version.command_set_indicator == 0x2


def test_parse_error_entry_zero_error_count_is_an_invalid_entry():
    """Spec: an Error Count of 0h marks an unused slot or a lost entry."""
    assert parse_error_log_entry(_make_error_entry(error_count=0, status_field=0x2001)) is None


def test_parse_error_log_page_drops_unused_slots():
    """A full page of slots yields only the populated entries.

    'nvme get-log' always returns every slot the controller supports; a 64-slot
    page with two real errors previously produced 64 rows in the collected
    artifact, 62 of them identical zeros.
    """
    page = (
        _make_error_entry(error_count=2, cmdid=0x4, status_field=0x2001)
        + _make_error_entry(error_count=1, cmdid=0x14, status_field=0x2002)
        + _make_error_entry(error_count=0) * 62
    )
    entries = parse_error_log_page(page)

    assert len(entries) == 2
    assert [e.status_field for e in entries] == [0x2001, 0x2002]


# ---------------------------------------------------------------------------
# Tests: self-test log page parsing
# ---------------------------------------------------------------------------


def test_parse_self_test_page_reads_results():
    page = _make_self_test_page(
        results=[
            {"result_code": 0, "self_test_code": 1, "power_on_hours": 8760, "nsid": 1},
            {
                "result_code": 7,
                "self_test_code": 2,
                "segment_number": 3,
                "power_on_hours": 8500,
                "nsid": 1,
                "failing_lba": 0x1234,
                "status_code_type": 2,
                "status_code": 0x81,
            },
        ]
    )
    log = parse_self_test_log_page("/dev/nvme1n1", page)

    assert not log.test_in_progress
    assert len(log.results) == 2
    assert log.results[0].passed
    assert log.results[0].self_test_code == 1
    assert log.results[0].power_on_hours == 8760
    assert log.results[1].result_code == 7
    assert log.results[1].segment_number == 3
    assert log.results[1].failing_lba == 0x1234
    assert log.results[1].status_code == 0x81


def test_parse_self_test_page_skips_unused_entries():
    """Result code 0xf means the slot holds no test result."""
    assert parse_self_test_log_page("/dev/nvme1n1", _make_self_test_page()).results == []


def test_parse_self_test_page_reports_test_in_progress():
    page = _make_self_test_page(current_operation=2, current_completion=45)
    log = parse_self_test_log_page("/dev/nvme1n1", page)

    assert log.test_in_progress
    assert log.current_operation == 2
    assert log.current_completion == 45


# ---------------------------------------------------------------------------
# Tests: raw page reads
# ---------------------------------------------------------------------------


def test_get_smart_log_reads_raw_log_page():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(_make_smart_page(temperature_kelvin=315, media_errors=2))

    smart = get_smart_log(node, "/dev/nvme1n1")

    assert smart.temperature_kelvin == 315
    assert smart.media_errors == 2
    node.remoter.sudo.assert_called_once_with(
        f"nvme get-log /dev/nvme1n1 --log-id={LOG_PAGE_SMART_HEALTH} --log-len={SMART_LOG_PAGE_LEN} -b | base64 -w0",
        ignore_status=True,
        timeout=30,
    )


def test_get_smart_log_rejects_short_read():
    """A truncated page is a failed read, never a partially-trusted one."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(b"\x00" * 128)

    assert get_smart_log(node, "/dev/nvme1n1") is None
    node.log.warning.assert_called()


def test_get_smart_log_rejects_undecodable_output():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(stdout="not base64 !!!")

    assert get_smart_log(node, "/dev/nvme1n1") is None
    node.log.warning.assert_called()


def test_get_error_log_sizes_the_page_from_elpe():
    """The page length comes from the controller, not from a fixed guess.

    Asking get-log for more entries than the controller supports can be
    rejected outright, so ELPE (0's based) decides the length.
    """
    node = _make_mock_node()
    node.remoter.sudo.side_effect = [
        _b64_result(_make_id_ctrl(elpe_zero_based=3)),  # id-ctrl -> 4 entries
        _b64_result(_make_error_entry(error_count=2, status_field=0x2001) + _make_error_entry(error_count=0) * 3),
    ]

    entries = get_error_log(node, "/dev/nvme1n1")

    assert len(entries) == 1
    assert entries[0].status_field == 0x2001
    log_page_cmd = node.remoter.sudo.call_args_list[1][0][0]
    assert f"--log-id={LOG_PAGE_ERROR_INFORMATION}" in log_page_cmd
    assert f"--log-len={4 * ERROR_LOG_ENTRY_LEN}" in log_page_cmd


def test_error_log_entry_count_is_zero_based():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(_make_id_ctrl(elpe_zero_based=63))

    assert error_log_entry_count(node, "/dev/nvme1n1") == 64


def test_supports_self_test_reads_oacs_bit_4():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(_make_id_ctrl(oacs=OACS_DEVICE_SELF_TEST | 0x8))

    assert supports_self_test(node, "/dev/nvme1n1") is True


def test_supports_self_test_false_for_aws_oacs():
    """oacs=0x8 (Namespace Management only) is what AWS Nitro controllers report."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(_make_id_ctrl(oacs=0x8))

    assert supports_self_test(node, "/dev/nvme1n1") is False


def test_supports_self_test_false_when_identify_unreadable():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    assert supports_self_test(node, "/dev/nvme1n1") is False


def test_get_self_test_log_reads_raw_log_page():
    node = _make_mock_node()
    node.remoter.sudo.return_value = _b64_result(
        _make_self_test_page(results=[{"result_code": 0, "self_test_code": 1}])
    )

    log = get_self_test_log(node, "/dev/nvme1n1")

    assert log is not None
    assert len(log.results) == 1
    assert f"--log-len={SELF_TEST_LOG_PAGE_LEN}" in node.remoter.sudo.call_args[0][0]


def test_collected_artifact_marks_opcode_unavailable(monkeypatch, tmp_path):
    """opcode is written as n/a when the controller did not report one."""
    node = _make_mock_node()
    node.logdir = str(tmp_path)
    entry = parse_error_log_entry(_make_error_entry(error_count=2, cmdid=0x4, status_field=0x2001, opcode=0x14))
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_error_log", lambda n, d, **kw: [entry])

    _collect_error_log_with_timestamp(node, "/dev/nvme1n1")

    written = list(tmp_path.glob("nvme_error_log_nvme1n1_*.log"))
    assert len(written) == 1
    line = written[0].read_text().strip()
    assert "status=0x2001" in line
    assert "opcode=n/a" in line


def test_collect_error_log_writes_nothing_when_no_populated_entries(monkeypatch, tmp_path):
    """No artifact is produced when the page holds only unused slots."""
    node = _make_mock_node()
    node.logdir = str(tmp_path)
    monkeypatch.setattr("sdcm.utils.nvme_diagnostics.get_error_log", lambda n, d, **kw: [])

    _collect_error_log_with_timestamp(node, "/dev/nvme1n1")

    assert list(tmp_path.glob("nvme_error_log_*.log")) == []


def test_get_error_log_returns_empty_when_identify_fails():
    """Without ELPE there is no defensible page length, so nothing is read."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    assert get_error_log(node, "/dev/nvme1n1") == []
