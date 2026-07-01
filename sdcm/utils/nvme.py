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

"""NVMe diagnostics utilities for collecting SMART logs, error logs, and self-test results.

This module provides functions to:
- Install nvme-cli on cluster nodes
- Discover NVMe devices via ``nvme list -o json``
- Collect and parse SMART health logs
- Collect and parse error logs
- Trigger and collect self-test results
- Filter devices to identify Scylla data disks

All functions accept a ``node`` argument that exposes ``.remoter`` (with ``.run()`` / ``.sudo()``)
and ``.log`` for logging. Device discovery returns an empty list (never raises) when no NVMe
devices are present, making it safe for docker and EBS-only backends.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import TYPE_CHECKING, Generator

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)

# Timeout for nvme-cli commands (seconds). SMART/error-log queries are fast,
# self-test polling may need longer but uses its own timeout.
NVME_CMD_TIMEOUT = 30

# Default interval between periodic SMART log collections (seconds).
DEFAULT_COLLECTION_INTERVAL = 3600


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


class SelfTestType(IntEnum):
    """NVMe device self-test types per NVM Express spec."""

    SHORT = 1
    EXTENDED = 2


@dataclass
class NvmeDevice:
    """Represents a single NVMe namespace discovered via ``nvme list``."""

    device_path: str  # e.g. "/dev/nvme0n1"
    model: str  # e.g. "Amazon EC2 NVMe Instance Storage"
    serial: str  # Serial number
    firmware: str  # Firmware revision
    size_bytes: int  # Namespace size in bytes
    used_bytes: int  # Namespace utilization in bytes
    sector_size: int  # Logical block size (512 or 4096)
    is_data_disk: bool = False  # True if identified as a Scylla data disk


@dataclass
class NvmeSmartLog:
    """Parsed NVMe SMART / Health Information Log (Log Page 02h)."""

    device_path: str
    critical_warning: int = 0
    temperature_kelvin: int = 0
    available_spare: int = 100
    available_spare_threshold: int = 0
    percentage_used: int = 0
    data_units_read: int = 0
    data_units_written: int = 0
    host_read_commands: int = 0
    host_write_commands: int = 0
    controller_busy_time: int = 0
    power_cycles: int = 0
    power_on_hours: int = 0
    unsafe_shutdowns: int = 0
    media_errors: int = 0
    num_err_log_entries: int = 0

    @property
    def temperature_celsius(self) -> int:
        """Convert Kelvin temperature to Celsius."""
        return self.temperature_kelvin - 273 if self.temperature_kelvin > 0 else 0

    @property
    def has_critical_warning(self) -> bool:
        return self.critical_warning != 0

    @property
    def has_media_errors(self) -> bool:
        return self.media_errors > 0

    @property
    def has_error_log_entries(self) -> bool:
        return self.num_err_log_entries > 0


@dataclass
class NvmeErrorLogEntry:
    """A single entry from the NVMe Error Information Log."""

    error_count: int = 0
    submission_queue_id: int = 0
    command_id: int = 0
    status_field: int = 0
    parm_error_location: int = 0
    lba: int = 0
    nsid: int = 0
    vendor_specific: int = 0
    transport_type: int = 0
    command_specific: int = 0
    opcode: int = 0


@dataclass
class NvmeSelfTestResult:
    """A single self-test result entry from the Device Self-test Log."""

    # Self-test result code: 0=no error, 1=aborted, 2=aborted by reset, etc.
    result_code: int = 0
    # Self-test code that was run (1=short, 2=extended)
    self_test_code: int = 0
    segment_number: int = 0
    power_on_hours: int = 0
    nsid: int = 0
    failing_lba: int = 0
    status_code_type: int = 0
    status_code: int = 0

    @property
    def passed(self) -> bool:
        """True if the self-test completed without error (code 0)."""
        return self.result_code == 0


@dataclass
class NvmeSelfTestLog:
    """Parsed Device Self-test Log (Log Page 06h)."""

    device_path: str
    current_operation: int = 0  # 0=no test in progress, 1=short, 2=extended
    current_completion: int = 0  # Percentage complete (0-100) if test in progress
    results: list[NvmeSelfTestResult] = field(default_factory=list)

    @property
    def test_in_progress(self) -> bool:
        return self.current_operation != 0


# ---------------------------------------------------------------------------
# Parsing functions
# ---------------------------------------------------------------------------


def parse_nvme_list_output(json_output: str) -> list[NvmeDevice]:
    """Parse the JSON output of ``nvme list -o json``.

    Args:
        json_output: Raw JSON string from ``nvme list -o json``.

    Returns:
        List of NvmeDevice instances. Returns empty list if output is empty
        or contains no devices.
    """
    if not json_output or not json_output.strip():
        return []

    try:
        data = json.loads(json_output)
    except json.JSONDecodeError:
        LOGGER.warning("Failed to parse nvme list JSON output")
        return []

    devices = []
    # nvme-cli 2.x uses {"Devices": [...]}, older versions use [...]
    device_list = data.get("Devices", data) if isinstance(data, dict) else data
    if not isinstance(device_list, list):
        return []

    for entry in device_list:
        # nvme-cli 2.x nests device info under "Subsystems" -> "Namespaces"
        # or directly as a flat list depending on version
        if "Namespaces" in entry:
            for ns in entry.get("Namespaces", []):
                device = _parse_device_entry(ns)
                if device:
                    devices.append(device)
        elif "DevicePath" in entry or "NameSpace" in entry:
            device = _parse_device_entry(entry)
            if device:
                devices.append(device)

    return devices


def _parse_device_entry(entry: dict) -> NvmeDevice | None:
    """Parse a single device entry from nvme list JSON output."""
    device_path = entry.get("DevicePath", entry.get("NameSpace", ""))
    if not device_path:
        return None

    return NvmeDevice(
        device_path=device_path,
        model=entry.get("ModelNumber", entry.get("Model", "")).strip(),
        serial=entry.get("SerialNumber", entry.get("Serial", "")).strip(),
        firmware=entry.get("Firmware", entry.get("FirmwareRevision", "")).strip(),
        size_bytes=entry.get("PhysicalSize", entry.get("Size", 0)),
        used_bytes=entry.get("UsedBytes", entry.get("Used", 0)),
        sector_size=entry.get("SectorSize", entry.get("LbaSize", 512)),
    )


def parse_smart_log_output(device_path: str, raw_output: str) -> NvmeSmartLog | None:
    """Parse the output of ``nvme smart-log <device>`` (human-readable format).

    Handles both the human-readable key/value format and JSON output.

    Args:
        device_path: The NVMe device path (e.g. "/dev/nvme0n1").
        raw_output: Raw command output from ``nvme smart-log``.

    Returns:
        NvmeSmartLog instance, or None if parsing fails completely.
    """
    if not raw_output or not raw_output.strip():
        return None

    # Try JSON first (if user passed -o json)
    try:
        data = json.loads(raw_output)
        return _parse_smart_log_json(device_path, data)
    except (json.JSONDecodeError, ValueError):
        pass

    # Fall back to human-readable key:value parsing
    return _parse_smart_log_text(device_path, raw_output)


def _parse_smart_log_json(device_path: str, data: dict) -> NvmeSmartLog:
    """Parse SMART log from JSON output."""
    return NvmeSmartLog(
        device_path=device_path,
        critical_warning=data.get("critical_warning", 0),
        temperature_kelvin=data.get("temperature", data.get("temperature_sensor_1", 0)),
        available_spare=data.get("avail_spare", data.get("available_spare", 100)),
        available_spare_threshold=data.get("spare_thresh", data.get("available_spare_threshold", 0)),
        percentage_used=data.get("percent_used", data.get("percentage_used", 0)),
        data_units_read=data.get("data_units_read", 0),
        data_units_written=data.get("data_units_written", 0),
        host_read_commands=data.get("host_read_commands", data.get("host_reads", 0)),
        host_write_commands=data.get("host_write_commands", data.get("host_writes", 0)),
        controller_busy_time=data.get("controller_busy_time", 0),
        power_cycles=data.get("power_cycles", 0),
        power_on_hours=data.get("power_on_hours", 0),
        unsafe_shutdowns=data.get("unsafe_shutdowns", 0),
        media_errors=data.get("media_errors", 0),
        num_err_log_entries=data.get("num_err_log_entries", 0),
    )


# Regex patterns for human-readable smart-log output.
# Lines look like: "critical_warning                        : 0" or
#                  "temperature                             : 315 K (42 Celsius)"
_SMART_KEY_VALUE_RE = re.compile(r"^\s*(.+?)\s*:\s*(.+?)\s*$", re.MULTILINE)

# Map from human-readable field names to NvmeSmartLog attribute names.
# nvme-cli uses slightly different names across versions, so we handle variants.
_SMART_FIELD_MAP = {
    "critical_warning": "critical_warning",
    "critical warning": "critical_warning",
    "temperature": "temperature_kelvin",
    "available spare": "available_spare",
    "available_spare": "available_spare",
    "available spare threshold": "available_spare_threshold",
    "available_spare_threshold": "available_spare_threshold",
    "percentage used": "percentage_used",
    "percentage_used": "percentage_used",
    "percent_used": "percentage_used",
    "data units read": "data_units_read",
    "data_units_read": "data_units_read",
    "data units written": "data_units_written",
    "data_units_written": "data_units_written",
    "host read commands": "host_read_commands",
    "host_read_commands": "host_read_commands",
    "host_reads": "host_read_commands",
    "host write commands": "host_write_commands",
    "host_write_commands": "host_write_commands",
    "host_writes": "host_write_commands",
    "controller busy time": "controller_busy_time",
    "controller_busy_time": "controller_busy_time",
    "power cycles": "power_cycles",
    "power_cycles": "power_cycles",
    "power on hours": "power_on_hours",
    "power_on_hours": "power_on_hours",
    "unsafe shutdowns": "unsafe_shutdowns",
    "unsafe_shutdowns": "unsafe_shutdowns",
    "media errors": "media_errors",
    "media_errors": "media_errors",
    "media and data integrity errors": "media_errors",
    "num err log entries": "num_err_log_entries",
    "num_err_log_entries": "num_err_log_entries",
    "number of error log entries": "num_err_log_entries",
    "number of error information log entries": "num_err_log_entries",
}


def _extract_numeric(value_str: str) -> int:
    """Extract the first integer from a value string.

    Handles formats like "315 K (42 Celsius)", "100%", "0", "1,234", etc.
    """
    # Remove commas used as thousands separator
    cleaned = value_str.replace(",", "")
    # Find first integer-like token
    match = re.search(r"\d+", cleaned)
    return int(match.group()) if match else 0


def _parse_smart_log_text(device_path: str, raw_output: str) -> NvmeSmartLog:
    """Parse SMART log from human-readable text output."""
    kwargs: dict = {"device_path": device_path}

    for match in _SMART_KEY_VALUE_RE.finditer(raw_output):
        key = match.group(1).lower().strip()
        value_str = match.group(2).strip()

        attr_name = _SMART_FIELD_MAP.get(key)
        if attr_name:
            kwargs[attr_name] = _extract_numeric(value_str)

    return NvmeSmartLog(**kwargs)


def parse_error_log_output(raw_output: str) -> list[NvmeErrorLogEntry]:
    """Parse the output of ``nvme error-log <device>``.

    Handles both JSON and human-readable formats.

    Args:
        raw_output: Raw command output from ``nvme error-log``.

    Returns:
        List of NvmeErrorLogEntry instances. Returns empty list if no errors
        or parsing fails.
    """
    if not raw_output or not raw_output.strip():
        return []

    # Try JSON first
    try:
        data = json.loads(raw_output)
        return _parse_error_log_json(data)
    except (json.JSONDecodeError, ValueError):
        pass

    # Fall back to human-readable parsing
    return _parse_error_log_text(raw_output)


def _parse_error_log_json(data: list | dict) -> list[NvmeErrorLogEntry]:
    """Parse error log entries from JSON output."""
    entries_list = data if isinstance(data, list) else data.get("errors", [])
    results = []
    for entry in entries_list:
        results.append(
            NvmeErrorLogEntry(
                error_count=entry.get("error_count", entry.get("err_count", 0)),
                submission_queue_id=entry.get("sqid", 0),
                command_id=entry.get("cmdid", entry.get("cid", 0)),
                status_field=entry.get("status_field", entry.get("status", 0)),
                parm_error_location=entry.get("parm_error_location", entry.get("pel", 0)),
                lba=entry.get("lba", 0),
                nsid=entry.get("nsid", 0),
                vendor_specific=entry.get("vs", entry.get("vendor_specific", 0)),
                transport_type=entry.get("trtype", entry.get("transport_type", 0)),
                command_specific=entry.get("cs", entry.get("command_specific", 0)),
                opcode=entry.get("opcode", entry.get("opc", 0)),
            )
        )
    return results


# Regex for error log entries in text format.
# Each entry starts with "Entry[N]" or "Error Log Entry N:" followed by key:value lines.
_ERROR_ENTRY_HEADER_RE = re.compile(r"(?:Entry\[|\bError Log Entry\s*)(\d+)", re.IGNORECASE)
_ERROR_KEY_VALUE_RE = re.compile(r"^\s*(.+?)\s*:\s*(0x[\da-fA-F]+|\d+)\s*$", re.MULTILINE)


def _parse_error_log_text(raw_output: str) -> list[NvmeErrorLogEntry]:
    """Parse error log from human-readable text output."""
    # Split on entry headers
    entries = re.split(r"(?:Entry\[|Error Log Entry\s*)\d+", raw_output, flags=re.IGNORECASE)
    results = []

    for entry_text in entries[1:]:  # Skip text before first entry
        entry = _parse_single_error_entry(entry_text)
        if entry:
            results.append(entry)

    return results


_ERROR_FIELD_MAP = {
    "error count": "error_count",
    "error_count": "error_count",
    "sqid": "submission_queue_id",
    "submission queue id": "submission_queue_id",
    "cmdid": "command_id",
    "cid": "command_id",
    "command id": "command_id",
    "status field": "status_field",
    "status_field": "status_field",
    "status": "status_field",
    "parm error location": "parm_error_location",
    "parm_error_location": "parm_error_location",
    "parameter error location": "parm_error_location",
    "lba": "lba",
    "nsid": "nsid",
    "namespace id": "nsid",
    "vs": "vendor_specific",
    "vendor specific": "vendor_specific",
    "trtype": "transport_type",
    "transport type": "transport_type",
    "cs": "command_specific",
    "command specific": "command_specific",
    "opcode": "opcode",
    "opc": "opcode",
}


def _parse_hex_or_int(value_str: str) -> int:
    """Parse a value that may be hex (0x...) or decimal."""
    value_str = value_str.strip()
    if value_str.startswith(("0x", "0X")):
        return int(value_str, 16)
    return int(value_str)


def _parse_single_error_entry(entry_text: str) -> NvmeErrorLogEntry | None:
    """Parse a single error log entry from text."""
    kwargs: dict = {}
    for match in _ERROR_KEY_VALUE_RE.finditer(entry_text):
        key = match.group(1).lower().strip()
        value_str = match.group(2).strip()

        attr_name = _ERROR_FIELD_MAP.get(key)
        if attr_name:
            kwargs[attr_name] = _parse_hex_or_int(value_str)

    if not kwargs:
        return None
    return NvmeErrorLogEntry(**kwargs)


def parse_self_test_log_output(device_path: str, raw_output: str) -> NvmeSelfTestLog | None:
    """Parse the output of ``nvme self-test-log <device>``.

    Handles both JSON and human-readable formats.

    Args:
        device_path: The NVMe device path.
        raw_output: Raw command output from ``nvme self-test-log``.

    Returns:
        NvmeSelfTestLog instance, or None if parsing fails completely.
    """
    if not raw_output or not raw_output.strip():
        return None

    # Try JSON first
    try:
        data = json.loads(raw_output)
        return _parse_self_test_log_json(device_path, data)
    except (json.JSONDecodeError, ValueError):
        pass

    # Fall back to human-readable parsing
    return _parse_self_test_log_text(device_path, raw_output)


def _parse_self_test_log_json(device_path: str, data: dict) -> NvmeSelfTestLog:
    """Parse self-test log from JSON output."""
    current_op = data.get("current_operation", data.get("crnt_dev_selftest_oprn", 0))
    current_completion = data.get("current_completion", data.get("crnt_dev_selftest_compln", 0))

    results = []
    for entry in data.get("results", data.get("self_test_result", [])):
        results.append(
            NvmeSelfTestResult(
                result_code=entry.get("result", entry.get("dsts", 0)),
                self_test_code=entry.get("self_test_code", entry.get("code", 0)),
                segment_number=entry.get("segment", entry.get("seg", 0)),
                power_on_hours=entry.get("power_on_hours", entry.get("poh", 0)),
                nsid=entry.get("nsid", 0),
                failing_lba=entry.get("failing_lba", entry.get("flba", 0)),
                status_code_type=entry.get("sct", entry.get("status_code_type", 0)),
                status_code=entry.get("sc", entry.get("status_code", 0)),
            )
        )

    return NvmeSelfTestLog(
        device_path=device_path,
        current_operation=current_op,
        current_completion=current_completion,
        results=results,
    )


_SELF_TEST_CURRENT_OP_RE = re.compile(r"(?:current\s+operation|crnt_dev_selftest_oprn)\s*:\s*(\d+)", re.IGNORECASE)
_SELF_TEST_COMPLETION_RE = re.compile(r"(?:current\s+completion|crnt_dev_selftest_compln)\s*:\s*(\d+)", re.IGNORECASE)
_SELF_TEST_RESULT_HEADER_RE = re.compile(r"(?:Self Test Result\s*\[|Result\s*\[)\s*(\d+)\s*\]", re.IGNORECASE)


def _parse_self_test_log_text(device_path: str, raw_output: str) -> NvmeSelfTestLog:
    """Parse self-test log from human-readable text output."""
    current_op = 0
    current_completion = 0

    op_match = _SELF_TEST_CURRENT_OP_RE.search(raw_output)
    if op_match:
        current_op = int(op_match.group(1))

    comp_match = _SELF_TEST_COMPLETION_RE.search(raw_output)
    if comp_match:
        current_completion = int(comp_match.group(1))

    # Split on result entry headers
    entries = re.split(r"(?:Self Test Result|Result)\s*\[\s*\d+\s*\]", raw_output, flags=re.IGNORECASE)
    results = []

    for entry_text in entries[1:]:  # Skip text before first result
        result = _parse_single_self_test_result(entry_text)
        if result:
            results.append(result)

    return NvmeSelfTestLog(
        device_path=device_path,
        current_operation=current_op,
        current_completion=current_completion,
        results=results,
    )


_SELF_TEST_FIELD_MAP = {
    "result": "result_code",
    "device self-test status": "result_code",
    "dsts": "result_code",
    "self test code": "self_test_code",
    "self_test_code": "self_test_code",
    "code": "self_test_code",
    "segment number": "segment_number",
    "seg": "segment_number",
    "segment": "segment_number",
    "power on hours": "power_on_hours",
    "power_on_hours": "power_on_hours",
    "poh": "power_on_hours",
    "nsid": "nsid",
    "namespace id": "nsid",
    "failing lba": "failing_lba",
    "flba": "failing_lba",
    "sct": "status_code_type",
    "status code type": "status_code_type",
    "sc": "status_code",
    "status code": "status_code",
}

_SELF_TEST_KEY_VALUE_RE = re.compile(r"^\s*(.+?)\s*:\s*(0x[\da-fA-F]+|\d+)\s*$", re.MULTILINE)


def _parse_single_self_test_result(entry_text: str) -> NvmeSelfTestResult | None:
    """Parse a single self-test result entry from text."""
    kwargs: dict = {}
    for match in _SELF_TEST_KEY_VALUE_RE.finditer(entry_text):
        key = match.group(1).lower().strip()
        value_str = match.group(2).strip()

        attr_name = _SELF_TEST_FIELD_MAP.get(key)
        if attr_name:
            kwargs[attr_name] = _parse_hex_or_int(value_str)

    if not kwargs:
        return None
    return NvmeSelfTestResult(**kwargs)


# ---------------------------------------------------------------------------
# Device filtering
# ---------------------------------------------------------------------------

# Patterns for identifying OS/boot disks vs data disks.
# AWS EBS root volumes are typically nvme0n1, local instance store starts at nvme1n1+.
# On GCE, boot disk is usually the first device as well.
_BOOT_DEVICE_INDICATORS = [
    "Amazon Elastic Block Store",  # AWS EBS model name
    "Google PersistentDisk",  # GCE PD model name
    "Msft Virtual Disk",  # Azure managed disk model name
]


def filter_data_disks(devices: list[NvmeDevice], boot_device_path: str | None = None) -> list[NvmeDevice]:
    """Filter NVMe devices to identify Scylla data disks (not boot/OS disks).

    Uses a combination of model name matching and optional explicit boot device
    path exclusion to identify which NVMe devices are local instance storage
    (data disks) vs cloud-provider managed/boot disks.

    Args:
        devices: List of discovered NvmeDevice instances.
        boot_device_path: Optional explicit boot device to exclude
            (e.g. "/dev/nvme0n1").

    Returns:
        List of NvmeDevice instances identified as data disks, with
        ``is_data_disk`` set to True.
    """
    data_disks = []
    for device in devices:
        # Skip explicit boot device
        if boot_device_path and device.device_path == boot_device_path:
            continue

        # Skip devices whose model matches known cloud managed/boot disk patterns
        if any(indicator.lower() in device.model.lower() for indicator in _BOOT_DEVICE_INDICATORS):
            continue

        device.is_data_disk = True
        data_disks.append(device)

    return data_disks


# ---------------------------------------------------------------------------
# Node-level operations (command wrappers)
# ---------------------------------------------------------------------------


def install_nvme_cli(node: "BaseNode") -> bool:
    """Install nvme-cli package on a node.

    Uses the node's install_package() method for cross-distro support.
    Returns False (does not raise) if installation fails.

    Args:
        node: SCT node with remoter and install_package() method.

    Returns:
        True if nvme-cli was installed (or already present), False on failure.
    """
    # Check if already installed
    result = node.remoter.run("which nvme", ignore_status=True)
    if result.ok:
        node.log.debug("nvme-cli already installed")
        return True

    node.log.info("Installing nvme-cli")
    try:
        node.install_package("nvme-cli", ignore_status=True)
    except Exception as exc:  # noqa: BLE001
        node.log.warning("Failed to install nvme-cli: %s", exc)
        return False

    # Verify installation
    result = node.remoter.run("which nvme", ignore_status=True)
    if not result.ok:
        node.log.warning("nvme-cli installation did not produce 'nvme' binary")
        return False

    return True


def is_nvme_cli_available(node: "BaseNode") -> bool:
    """Check if nvme-cli is available on the node.

    Args:
        node: SCT node with remoter.

    Returns:
        True if ``nvme`` command is available.
    """
    result = node.remoter.run("which nvme", ignore_status=True)
    return result.ok


def list_nvme_devices(node: "BaseNode") -> list[NvmeDevice]:
    """Discover NVMe devices on a node using ``nvme list -o json``.

    Returns an empty list (never raises) when no NVMe devices are present
    or when nvme-cli is not installed.

    Args:
        node: SCT node with remoter.

    Returns:
        List of NvmeDevice instances. Empty list if no devices or nvme-cli
        not available.
    """
    if not is_nvme_cli_available(node):
        node.log.debug("nvme-cli not available, skipping device discovery")
        return []

    result = node.remoter.sudo(
        "nvme list -o json",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning("'nvme list' failed with exit code %d: %s", result.exited, result.stderr)
        return []

    return parse_nvme_list_output(result.stdout)


def get_smart_log(node: "BaseNode", device_path: str) -> NvmeSmartLog | None:
    """Collect SMART log for a single NVMe device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").

    Returns:
        NvmeSmartLog instance, or None on failure.
    """
    result = node.remoter.sudo(
        f"nvme smart-log {device_path}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning("'nvme smart-log %s' failed: %s", device_path, result.stderr)
        return None

    return parse_smart_log_output(device_path, result.stdout)


def get_error_log(node: "BaseNode", device_path: str, max_entries: int = 64) -> list[NvmeErrorLogEntry]:
    """Collect error log for a single NVMe device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").
        max_entries: Maximum number of error log entries to retrieve.

    Returns:
        List of NvmeErrorLogEntry instances. Empty list on failure.
    """
    result = node.remoter.sudo(
        f"nvme error-log {device_path} -e {max_entries}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning("'nvme error-log %s' failed: %s", device_path, result.stderr)
        return []

    return parse_error_log_output(result.stdout)


def run_self_test(node: "BaseNode", device_path: str, test_type: SelfTestType = SelfTestType.SHORT) -> bool:
    """Trigger a device self-test on an NVMe device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").
        test_type: Type of self-test to run (SHORT=1, EXTENDED=2).

    Returns:
        True if the self-test was triggered successfully, False otherwise.
    """
    result = node.remoter.sudo(
        f"nvme device-self-test -s {int(test_type)} {device_path}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning(
            "'nvme device-self-test' on %s failed (may not be supported): %s",
            device_path,
            result.stderr,
        )
        return False

    node.log.info("Self-test type %d triggered on %s", int(test_type), device_path)
    return True


def abort_self_test(node: "BaseNode", device_path: str) -> bool:
    """Abort a running device self-test on an NVMe device.

    Uses the NVMe spec abort action (self-test code 0xf) to stop any
    in-progress self-test on the device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").

    Returns:
        True if abort was sent successfully, False otherwise.
    """
    result = node.remoter.sudo(
        f"nvme device-self-test -s 0xf {device_path}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning(
            "Failed to abort self-test on %s: %s",
            device_path,
            result.stderr,
        )
        return False

    node.log.info("Self-test aborted on %s", device_path)
    return True


def get_self_test_log(node: "BaseNode", device_path: str) -> NvmeSelfTestLog | None:
    """Collect self-test log for a single NVMe device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").

    Returns:
        NvmeSelfTestLog instance, or None on failure.
    """
    result = node.remoter.sudo(
        f"nvme self-test-log {device_path}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning("'nvme self-test-log %s' failed: %s", device_path, result.stderr)
        return None

    return parse_self_test_log_output(device_path, result.stdout)


def collect_all_smart_logs(node: "BaseNode") -> list[NvmeSmartLog]:
    """Collect SMART logs for all data disks on a node.

    Performs full discovery -> filtering -> SMART collection pipeline.
    Returns empty list if no NVMe devices or nvme-cli not installed.

    Args:
        node: SCT node with remoter.

    Returns:
        List of NvmeSmartLog instances for all data disks.
    """
    devices = list_nvme_devices(node)
    if not devices:
        return []

    data_disks = filter_data_disks(devices)
    if not data_disks:
        node.log.debug("No NVMe data disks found (all devices appear to be boot/OS disks)")
        return []

    smart_logs = []
    for disk in data_disks:
        smart_log = get_smart_log(node, disk.device_path)
        if smart_log:
            smart_logs.append(smart_log)

    return smart_logs


# ---------------------------------------------------------------------------
# Health check thresholds
# ---------------------------------------------------------------------------

# Default thresholds for NVMe SMART health checks. These can be overridden
# via the thresholds parameter in check_nvme_health().
DEFAULT_NVME_THRESHOLDS = {
    "percentage_used_warning": 90,
    "temperature_warning_celsius": 70,
}


# ---------------------------------------------------------------------------
# Health check generator
# ---------------------------------------------------------------------------

# Type alias matching health_checker.py convention
NvmeHealthEventsGenerator = Generator[ClusterHealthValidatorEvent, None, None]


def check_nvme_health(
    current_node: "BaseNode",
    thresholds: dict | None = None,
) -> NvmeHealthEventsGenerator:
    """Check NVMe device health and yield events for detected issues.

    Collects SMART logs for all NVMe data disks on the node and evaluates
    them against health thresholds. Automatically collects error logs when
    media errors or error log entries are detected.

    Severity mapping:
        - critical_warning != 0 -> CRITICAL
        - media_errors > 0 -> ERROR
        - num_err_log_entries > 0 -> WARNING (also collects error log)
        - percentage_used > threshold -> WARNING
        - available_spare < available_spare_threshold -> WARNING
        - temperature > threshold -> WARNING

    Args:
        current_node: SCT node to check.
        thresholds: Optional dict overriding DEFAULT_NVME_THRESHOLDS.

    Yields:
        ClusterHealthValidatorEvent.NvmeHealth events for each issue found.
    """
    if not current_node.parent_cluster.params.get("collect_nvme_diagnostics"):
        return

    if not is_nvme_cli_available(current_node):
        return

    effective_thresholds = {**DEFAULT_NVME_THRESHOLDS, **(thresholds or {})}
    smart_logs = collect_all_smart_logs(current_node)
    if not smart_logs:
        return

    for smart_log in smart_logs:
        yield from _check_single_device_health(current_node, smart_log, effective_thresholds)


def _check_single_device_health(
    current_node: "BaseNode",
    smart_log: NvmeSmartLog,
    thresholds: dict,
) -> NvmeHealthEventsGenerator:
    """Evaluate a single device's SMART log against health thresholds."""
    device = smart_log.device_path

    # critical_warning != 0 -> CRITICAL
    if smart_log.has_critical_warning:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.CRITICAL,
            node=current_node.name,
            error=f"NVMe {device}: critical_warning={smart_log.critical_warning} (non-zero indicates hardware issue)",
        )

    # media_errors > 0 -> ERROR
    if smart_log.has_media_errors:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.ERROR,
            node=current_node.name,
            error=f"NVMe {device}: media_errors={smart_log.media_errors}",
        )
        _collect_error_log_with_timestamp(current_node, device)

    # num_err_log_entries > 0 -> WARNING (also collect error log)
    if smart_log.has_error_log_entries:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.WARNING,
            node=current_node.name,
            message=f"NVMe {device}: num_err_log_entries={smart_log.num_err_log_entries}",
        )
        if not smart_log.has_media_errors:
            # Only collect if not already collected above
            _collect_error_log_with_timestamp(current_node, device)

    # percentage_used > threshold -> WARNING
    pct_threshold = thresholds["percentage_used_warning"]
    if smart_log.percentage_used > pct_threshold:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.WARNING,
            node=current_node.name,
            message=f"NVMe {device}: percentage_used={smart_log.percentage_used}% (threshold {pct_threshold}%)",
        )

    # available_spare < available_spare_threshold -> WARNING
    if smart_log.available_spare < smart_log.available_spare_threshold:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.WARNING,
            node=current_node.name,
            message=(
                f"NVMe {device}: available_spare={smart_log.available_spare}% "
                f"below threshold {smart_log.available_spare_threshold}%"
            ),
        )

    # temperature above threshold -> WARNING
    temp_threshold = thresholds["temperature_warning_celsius"]
    if smart_log.temperature_celsius > temp_threshold:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.WARNING,
            node=current_node.name,
            message=(f"NVMe {device}: temperature={smart_log.temperature_celsius}°C (threshold {temp_threshold}°C)"),
        )


def _collect_error_log_with_timestamp(node: "BaseNode", device_path: str) -> None:
    """Collect NVMe error log and save with timestamp for post-mortem analysis.

    Saves the raw error log output to the node's log directory with a
    timestamped filename for correlation with test events.
    """
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    device_name = device_path.replace("/dev/", "")
    filename = f"nvme_error_log_{device_name}_{timestamp}.log"

    entries = get_error_log(node, device_path)
    if not entries:
        node.log.debug("NVMe error log for %s is empty", device_path)
        return

    lines = []
    for entry in entries:
        lines.append(
            f"error_count={entry.error_count} sqid={entry.submission_queue_id} "
            f"cmdid={entry.command_id} status=0x{entry.status_field:04x} "
            f"lba=0x{entry.lba:x} nsid={entry.nsid} opcode=0x{entry.opcode:02x}"
        )
    content = "\n".join(lines) + "\n"

    try:
        log_dir = node.logdir
        if log_dir:
            filepath = f"{log_dir}/{filename}"
            with open(filepath, "w", encoding="utf-8") as fobj:
                fobj.write(content)
            node.log.info("NVMe error log saved to %s (%d entries)", filepath, len(entries))
    except Exception as exc:  # noqa: BLE001
        node.log.warning("Failed to save NVMe error log: %s", exc)


# ---------------------------------------------------------------------------
# Self-test orchestration (Phase 5)
# ---------------------------------------------------------------------------

# Default timeout for short self-test polling (seconds). NVMe short self-tests
# typically complete in 1-2 minutes; extended tests can take hours.
SHORT_SELF_TEST_TIMEOUT = 300
EXTENDED_SELF_TEST_TIMEOUT = 14400  # 4 hours
SELF_TEST_POLL_INTERVAL = 10  # seconds between poll attempts


def poll_self_test_completion(
    node: "BaseNode",
    device_path: str,
    timeout: int | None = None,
    poll_interval: int = SELF_TEST_POLL_INTERVAL,
    test_type: SelfTestType = SelfTestType.SHORT,
) -> NvmeSelfTestLog | None:
    """Poll for NVMe self-test completion with timeout.

    Checks the self-test log periodically until the test completes or
    the timeout expires. If the timeout is reached, the in-progress
    test is aborted.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").
        timeout: Maximum seconds to wait. Defaults based on test_type.
        poll_interval: Seconds between poll attempts.
        test_type: Type of self-test being polled (for default timeout).

    Returns:
        NvmeSelfTestLog with results, or None on failure.
    """
    import time  # noqa: PLC0415 -- avoid adding to module-level imports used only here

    if timeout is None:
        timeout = SHORT_SELF_TEST_TIMEOUT if test_type == SelfTestType.SHORT else EXTENDED_SELF_TEST_TIMEOUT

    deadline = time.monotonic() + timeout
    node.log.info(
        "Polling self-test completion on %s (timeout=%ds, interval=%ds)",
        device_path,
        timeout,
        poll_interval,
    )

    while time.monotonic() < deadline:
        test_log = get_self_test_log(node, device_path)
        if test_log is None:
            node.log.warning("Failed to read self-test log for %s, retrying...", device_path)
            time.sleep(poll_interval)
            continue

        if not test_log.test_in_progress:
            node.log.info("Self-test completed on %s", device_path)
            return test_log

        node.log.debug(
            "Self-test in progress on %s: operation=%d, completion=%d%%",
            device_path,
            test_log.current_operation,
            test_log.current_completion,
        )
        time.sleep(poll_interval)

    # Timeout reached — abort the test
    node.log.warning(
        "Self-test on %s did not complete within %ds, aborting",
        device_path,
        timeout,
    )
    abort_self_test(node, device_path)

    # Collect final log after abort
    return get_self_test_log(node, device_path)


def check_self_test_results(
    node: "BaseNode",
    test_log: NvmeSelfTestLog,
) -> NvmeHealthEventsGenerator:
    """Evaluate self-test results and yield events for failures.

    Checks the most recent self-test result entry. A result_code of 0
    means success; any other value indicates a failure or abort.

    Result codes (NVMe spec):
        0 = completed without error
        1 = aborted by Device Self-test command
        2 = aborted by Controller Level Reset
        3 = aborted by namespace removal
        4 = aborted by Format NVM command
        5-7 = vendor specific
        15 = entry not used (no test run)

    Args:
        node: SCT node (for event node name).
        test_log: Parsed self-test log.

    Yields:
        ClusterHealthValidatorEvent.NvmeHealth for failed results.
    """
    if not test_log.results:
        return

    latest = test_log.results[0]

    # result_code 15 (0xf) means "entry not used" — no test was run
    if latest.result_code == 0xF:
        return

    # result_code 1 means aborted by user (e.g., timeout abort) — just warn
    if latest.result_code == 1:
        node.log.info(
            "Self-test on %s was aborted (code=%d)",
            test_log.device_path,
            latest.result_code,
        )
        return

    # result_code 0 means success
    if latest.passed:
        node.log.info("Self-test on %s passed", test_log.device_path)
        return

    # Any other code is a real failure
    severity = Severity.ERROR if latest.result_code >= 4 else Severity.WARNING
    yield ClusterHealthValidatorEvent.NvmeHealth(
        severity=severity,
        node=node.name,
        error=(
            f"NVMe {test_log.device_path}: self-test failed "
            f"(result_code={latest.result_code}, test_type={latest.self_test_code}, "
            f"nsid={latest.nsid}, failing_lba=0x{latest.failing_lba:x})"
        ),
    )


def run_self_test_on_all_devices(
    node: "BaseNode",
    test_type: SelfTestType = SelfTestType.SHORT,
    timeout: int | None = None,
) -> list[NvmeSelfTestLog]:
    """Run self-test on all NVMe data disks and collect results.

    Triggers a self-test on each data disk, polls for completion, and
    generates events for any failures. This is the high-level entry point
    for end-of-test self-test execution.

    Args:
        node: SCT node with remoter.
        test_type: Type of self-test (SHORT or EXTENDED).
        timeout: Max seconds to wait per device. Defaults based on test_type.

    Returns:
        List of NvmeSelfTestLog results for all tested devices.
    """
    if not is_nvme_cli_available(node):
        node.log.debug("nvme-cli not available, skipping self-tests")
        return []

    devices = list_nvme_devices(node)
    if not devices:
        return []

    data_disks = filter_data_disks(devices)
    if not data_disks:
        node.log.debug("No NVMe data disks found, skipping self-tests")
        return []

    results = []
    for disk in data_disks:
        if not run_self_test(node, disk.device_path, test_type):
            node.log.warning("Failed to trigger self-test on %s, skipping", disk.device_path)
            continue

        test_log = poll_self_test_completion(
            node,
            disk.device_path,
            timeout=timeout,
            test_type=test_type,
        )
        if test_log:
            results.append(test_log)
            # Publish events for failures
            for event in check_self_test_results(node, test_log):
                event.publish()

    return results
