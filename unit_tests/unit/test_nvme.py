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

import json
from unittest.mock import MagicMock, PropertyMock

from sdcm.cluster import BaseScyllaCluster
from sdcm.sct_events import Severity
from sdcm.utils.nvme import (
    NvmeDevice,
    NvmeSmartLog,
    SelfTestType,
    _check_single_device_health,
    abort_self_test,
    check_nvme_health,
    collect_all_smart_logs,
    filter_data_disks,
    get_error_log,
    get_self_test_log,
    get_smart_log,
    install_nvme_cli,
    is_nvme_cli_available,
    list_nvme_devices,
    parse_error_log_output,
    parse_nvme_list_output,
    parse_self_test_log_output,
    parse_smart_log_output,
    run_self_test,
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

SMART_LOG_TEXT_OUTPUT = """\
Smart Log for NVME device:nvme1n1 namespace-id:ffffffff
critical_warning                        : 0
temperature                             : 315 K (42 Celsius)
available_spare                         : 100%
available_spare_threshold               : 10%
percentage_used                         : 2%
data_units_read                         : 1,234,567
data_units_written                      : 9,876,543
host_read_commands                      : 50,000,000
host_write_commands                     : 30,000,000
controller_busy_time                    : 120
power_cycles                            : 15
power_on_hours                          : 8760
unsafe_shutdowns                        : 3
media_errors                            : 0
num_err_log_entries                     : 5
"""

SMART_LOG_TEXT_WITH_ERRORS = """\
Smart Log for NVME device:nvme1n1 namespace-id:ffffffff
critical_warning                        : 4
temperature                             : 345 K (72 Celsius)
available_spare                         : 5%
available_spare_threshold               : 10%
percentage_used                         : 95%
data_units_read                         : 10,000,000
data_units_written                      : 50,000,000
host_read_commands                      : 100,000,000
host_write_commands                     : 200,000,000
controller_busy_time                    : 5000
power_cycles                            : 50
power_on_hours                          : 20000
unsafe_shutdowns                        : 10
media_errors                            : 42
num_err_log_entries                     : 100
"""

SMART_LOG_JSON_OUTPUT = json.dumps(
    {
        "critical_warning": 0,
        "temperature": 310,
        "avail_spare": 95,
        "spare_thresh": 10,
        "percent_used": 5,
        "data_units_read": 500000,
        "data_units_written": 300000,
        "host_read_commands": 10000000,
        "host_write_commands": 8000000,
        "controller_busy_time": 60,
        "power_cycles": 8,
        "power_on_hours": 4000,
        "unsafe_shutdowns": 1,
        "media_errors": 0,
        "num_err_log_entries": 0,
    }
)

ERROR_LOG_TEXT_OUTPUT = """\
Error Log Entries for device:nvme1n1 entries:2
Entry[0]
error_count                    : 5
submission queue id            : 0
command id                     : 0x0012
status field                   : 0x4004
parm error location            : 0x0000
lba                            : 0x00000000
nsid                           : 1
vs                             : 0
transport type                 : 0
command specific               : 0
opcode                         : 0x02
Entry[1]
error_count                    : 4
submission queue id            : 0
command id                     : 0x000a
status field                   : 0x4004
parm error location            : 0x0000
lba                            : 0x00001000
nsid                           : 1
vs                             : 0
transport type                 : 0
command specific               : 0
opcode                         : 0x01
"""

ERROR_LOG_JSON_OUTPUT = json.dumps(
    [
        {
            "error_count": 3,
            "sqid": 0,
            "cmdid": 18,
            "status_field": 16388,
            "parm_error_location": 0,
            "lba": 0,
            "nsid": 1,
            "vs": 0,
            "trtype": 0,
            "cs": 0,
            "opcode": 2,
        },
        {
            "error_count": 2,
            "sqid": 1,
            "cmdid": 5,
            "status_field": 16388,
            "parm_error_location": 0,
            "lba": 4096,
            "nsid": 1,
            "vs": 0,
            "trtype": 0,
            "cs": 0,
            "opcode": 1,
        },
    ]
)

SELF_TEST_LOG_TEXT_OUTPUT = """\
Device Self Test Log for NVME device:nvme1n1
Current operation  : 0
Current Completion : 0
Self Test Result[0]
Device Self-test Status             : 0
Self Test Code                      : 1
Segment Number                      : 0
Power On Hours                      : 8760
nsid                                : 1
Failing LBA                         : 0
SCT                                 : 0
SC                                  : 0
Self Test Result[1]
Device Self-test Status             : 2
Self Test Code                      : 2
Segment Number                      : 1
Power On Hours                      : 8500
nsid                                : 1
Failing LBA                         : 0x00001234
SCT                                 : 0
SC                                  : 0
"""

SELF_TEST_LOG_JSON_OUTPUT = json.dumps(
    {
        "current_operation": 1,
        "current_completion": 45,
        "results": [
            {
                "result": 0,
                "self_test_code": 1,
                "segment": 0,
                "power_on_hours": 9000,
                "nsid": 1,
                "failing_lba": 0,
                "sct": 0,
                "sc": 0,
            },
            {
                "result": 1,
                "self_test_code": 2,
                "segment": 0,
                "power_on_hours": 8900,
                "nsid": 1,
                "failing_lba": 512,
                "sct": 1,
                "sc": 3,
            },
        ],
    }
)

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


def test_parse_smart_log_text_normal():
    """Parse human-readable SMART log with normal values."""
    smart = parse_smart_log_output("/dev/nvme1n1", SMART_LOG_TEXT_OUTPUT)
    assert smart is not None
    assert smart.device_path == "/dev/nvme1n1"
    assert smart.critical_warning == 0
    assert smart.temperature_kelvin == 315
    assert smart.temperature_celsius == 42
    assert smart.available_spare == 100
    assert smart.available_spare_threshold == 10
    assert smart.percentage_used == 2
    assert smart.data_units_read == 1234567
    assert smart.data_units_written == 9876543
    assert smart.host_read_commands == 50000000
    assert smart.host_write_commands == 30000000
    assert smart.controller_busy_time == 120
    assert smart.power_cycles == 15
    assert smart.power_on_hours == 8760
    assert smart.unsafe_shutdowns == 3
    assert smart.media_errors == 0
    assert smart.num_err_log_entries == 5
    assert not smart.has_critical_warning
    assert not smart.has_media_errors
    assert smart.has_error_log_entries


def test_parse_smart_log_text_with_errors():
    """Parse SMART log showing critical conditions."""
    smart = parse_smart_log_output("/dev/nvme1n1", SMART_LOG_TEXT_WITH_ERRORS)
    assert smart is not None
    assert smart.critical_warning == 4
    assert smart.temperature_kelvin == 345
    assert smart.temperature_celsius == 72
    assert smart.available_spare == 5
    assert smart.percentage_used == 95
    assert smart.media_errors == 42
    assert smart.num_err_log_entries == 100
    assert smart.has_critical_warning
    assert smart.has_media_errors
    assert smart.has_error_log_entries


def test_parse_smart_log_json():
    """Parse JSON format SMART log."""
    smart = parse_smart_log_output("/dev/nvme0n1", SMART_LOG_JSON_OUTPUT)
    assert smart is not None
    assert smart.device_path == "/dev/nvme0n1"
    assert smart.critical_warning == 0
    assert smart.temperature_kelvin == 310
    assert smart.available_spare == 95
    assert smart.available_spare_threshold == 10
    assert smart.percentage_used == 5
    assert smart.power_on_hours == 4000
    assert smart.media_errors == 0
    assert smart.num_err_log_entries == 0


def test_parse_smart_log_empty_input():
    """Empty input returns None."""
    assert parse_smart_log_output("/dev/nvme0n1", "") is None
    assert parse_smart_log_output("/dev/nvme0n1", None) is None


def test_parse_smart_log_temperature_celsius_zero_kelvin():
    """Temperature conversion handles zero kelvin gracefully."""
    smart = NvmeSmartLog(device_path="/dev/nvme0n1", temperature_kelvin=0)
    assert smart.temperature_celsius == 0


# ---------------------------------------------------------------------------
# Tests: parse_error_log_output
# ---------------------------------------------------------------------------


def test_parse_error_log_text():
    """Parse human-readable error log with two entries."""
    entries = parse_error_log_output(ERROR_LOG_TEXT_OUTPUT)
    assert len(entries) == 2

    assert entries[0].error_count == 5
    assert entries[0].command_id == 0x0012
    assert entries[0].status_field == 0x4004
    assert entries[0].lba == 0
    assert entries[0].nsid == 1
    assert entries[0].opcode == 0x02

    assert entries[1].error_count == 4
    assert entries[1].command_id == 0x000A
    assert entries[1].lba == 0x1000
    assert entries[1].opcode == 0x01


def test_parse_error_log_json():
    """Parse JSON format error log."""
    entries = parse_error_log_output(ERROR_LOG_JSON_OUTPUT)
    assert len(entries) == 2

    assert entries[0].error_count == 3
    assert entries[0].submission_queue_id == 0
    assert entries[0].command_id == 18
    assert entries[0].status_field == 16388
    assert entries[0].lba == 0
    assert entries[0].nsid == 1
    assert entries[0].opcode == 2

    assert entries[1].error_count == 2
    assert entries[1].submission_queue_id == 1
    assert entries[1].lba == 4096
    assert entries[1].opcode == 1


def test_parse_error_log_empty_input():
    """Empty input returns empty list."""
    assert parse_error_log_output("") == []
    assert parse_error_log_output(None) == []


def test_parse_error_log_no_entries():
    """Output with header but no entries returns empty list."""
    output = "Error Log Entries for device:nvme0n1 entries:0\n"
    assert parse_error_log_output(output) == []


# ---------------------------------------------------------------------------
# Tests: parse_self_test_log_output
# ---------------------------------------------------------------------------


def test_parse_self_test_log_text():
    """Parse human-readable self-test log with two result entries."""
    log = parse_self_test_log_output("/dev/nvme1n1", SELF_TEST_LOG_TEXT_OUTPUT)
    assert log is not None
    assert log.device_path == "/dev/nvme1n1"
    assert log.current_operation == 0
    assert log.current_completion == 0
    assert not log.test_in_progress
    assert len(log.results) == 2

    assert log.results[0].result_code == 0
    assert log.results[0].self_test_code == 1
    assert log.results[0].power_on_hours == 8760
    assert log.results[0].passed

    assert log.results[1].result_code == 2
    assert log.results[1].self_test_code == 2
    assert log.results[1].power_on_hours == 8500
    assert log.results[1].failing_lba == 0x1234
    assert not log.results[1].passed


def test_parse_self_test_log_json():
    """Parse JSON format self-test log with test in progress."""
    log = parse_self_test_log_output("/dev/nvme1n1", SELF_TEST_LOG_JSON_OUTPUT)
    assert log is not None
    assert log.current_operation == 1
    assert log.current_completion == 45
    assert log.test_in_progress
    assert len(log.results) == 2

    assert log.results[0].result_code == 0
    assert log.results[0].self_test_code == 1
    assert log.results[0].power_on_hours == 9000
    assert log.results[0].passed

    assert log.results[1].result_code == 1
    assert log.results[1].self_test_code == 2
    assert log.results[1].failing_lba == 512
    assert log.results[1].status_code_type == 1
    assert log.results[1].status_code == 3
    assert not log.results[1].passed


def test_parse_self_test_log_in_progress():
    """Parse self-test log showing test in progress, no results."""
    log = parse_self_test_log_output("/dev/nvme1n1", SELF_TEST_LOG_IN_PROGRESS_TEXT)
    assert log is not None
    assert log.current_operation == 2
    assert log.current_completion == 67
    assert log.test_in_progress
    assert log.results == []


def test_parse_self_test_log_empty_input():
    """Empty input returns None."""
    assert parse_self_test_log_output("/dev/nvme0n1", "") is None
    assert parse_self_test_log_output("/dev/nvme0n1", None) is None


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


def test_get_smart_log_success():
    """get_smart_log returns parsed SMART data."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(stdout=SMART_LOG_TEXT_OUTPUT)

    smart = get_smart_log(node, "/dev/nvme1n1")
    assert smart is not None
    assert smart.power_on_hours == 8760
    assert smart.media_errors == 0


def test_get_smart_log_failure():
    """get_smart_log returns None when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="device not found")

    smart = get_smart_log(node, "/dev/nvme99n1")
    assert smart is None


def test_get_error_log_success():
    """get_error_log returns parsed entries."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(stdout=ERROR_LOG_TEXT_OUTPUT)

    entries = get_error_log(node, "/dev/nvme1n1")
    assert len(entries) == 2


def test_get_error_log_failure():
    """get_error_log returns empty list when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    entries = get_error_log(node, "/dev/nvme99n1")
    assert entries == []


def test_run_self_test_success():
    """run_self_test returns True on success."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result()

    result = run_self_test(node, "/dev/nvme1n1", SelfTestType.SHORT)
    assert result is True
    node.remoter.sudo.assert_called_once_with(
        "nvme device-self-test -s 1 /dev/nvme1n1",
        ignore_status=True,
        timeout=30,
    )


def test_run_self_test_extended():
    """run_self_test passes correct type for extended test."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result()

    result = run_self_test(node, "/dev/nvme1n1", SelfTestType.EXTENDED)
    assert result is True
    node.remoter.sudo.assert_called_once_with(
        "nvme device-self-test -s 2 /dev/nvme1n1",
        ignore_status=True,
        timeout=30,
    )


def test_run_self_test_not_supported():
    """run_self_test returns False when device doesn't support self-test."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="not supported")

    result = run_self_test(node, "/dev/nvme1n1")
    assert result is False


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


def test_get_self_test_log_success():
    """get_self_test_log returns parsed log."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(stdout=SELF_TEST_LOG_TEXT_OUTPUT)

    log = get_self_test_log(node, "/dev/nvme1n1")
    assert log is not None
    assert len(log.results) == 2


def test_get_self_test_log_failure():
    """get_self_test_log returns None when command fails."""
    node = _make_mock_node()
    node.remoter.sudo.return_value = _make_result(exited=1, stderr="error")

    log = get_self_test_log(node, "/dev/nvme1n1")
    assert log is None


def test_collect_all_smart_logs_full_pipeline():
    """collect_all_smart_logs performs discovery -> filter -> collect."""
    node = _make_mock_node()
    # is_nvme_cli_available check (via run "which nvme")
    node.remoter.run.return_value = _make_result(stdout="/usr/sbin/nvme")
    # nvme list returns one EBS + one instance store
    node.remoter.sudo.side_effect = [
        _make_result(stdout=NVME_LIST_JSON_V2),  # nvme list
        _make_result(stdout=SMART_LOG_TEXT_OUTPUT),  # smart-log for instance store
    ]

    smart_logs = collect_all_smart_logs(node)
    # Only the instance store disk (not EBS) should be collected
    assert len(smart_logs) == 1
    assert smart_logs[0].device_path == "/dev/nvme1n1"


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
    assert "media_errors=42" in error_events[0].error


def test_threshold_error_log_entries_yields_warning():
    """num_err_log_entries > 0 yields a WARNING event."""
    node = _make_mock_node()
    node.logdir = "/tmp/opencode/test_logdir"
    smart = _make_smart_log(num_err_log_entries=5)
    events = list(_check_single_device_health(node, smart, DEFAULT_THRESHOLDS))

    warning_events = [e for e in events if e.severity == Severity.WARNING]
    assert len(warning_events) == 1
    assert "num_err_log_entries=5" in warning_events[0].message


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
    monkeypatch.setattr("sdcm.utils.nvme.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme.collect_all_smart_logs", lambda n: [smart])

    events = list(check_nvme_health(current_node=node))
    assert events == []


def test_check_nvme_health_yields_events_for_errors(monkeypatch):
    """check_nvme_health yields events when SMART data shows errors."""
    node = _make_mock_node()
    node.parent_cluster = MagicMock()
    node.parent_cluster.params.get.return_value = True
    node.logdir = "/tmp/opencode/test_logdir"

    smart = _make_smart_log(media_errors=5)
    monkeypatch.setattr("sdcm.utils.nvme.is_nvme_cli_available", lambda n: True)
    monkeypatch.setattr("sdcm.utils.nvme.collect_all_smart_logs", lambda n: [smart])
    monkeypatch.setattr("sdcm.utils.nvme.get_error_log", lambda n, d, **kw: [])

    events = list(check_nvme_health(current_node=node))
    assert len(events) == 1
    assert events[0].severity == Severity.ERROR
