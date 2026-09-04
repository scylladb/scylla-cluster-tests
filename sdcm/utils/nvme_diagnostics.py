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

Output format
-------------
Log pages are read as raw bytes (``nvme get-log ... -b``) and decoded at the fixed offsets the
NVMe Base Specification defines. The layouts are stable across spec revisions; only the way
nvme-cli renders them changes, and it has changed repeatedly - inline value descriptions
("status_field : 0x2001 (Invalid Command Opcode: ...)"), the temperature unit order flipping
between 1.x and 2.x, and a field rename due in 3.0. Reading the page directly removes that whole
dependency. The contract is a length check: either the page was read in full or it was not.
Device discovery still uses ``nvme list -o json``, which is not a log page.

Self-test availability
----------------------
Device Self-test is optional in the NVMe spec and is advertised by Identify Controller OACS
bit 4. It is *not* available on AWS - neither on instance-store (Nitro SSD) nor on EBS - so
``nvme_self_test_type`` has no effect there and the self-test code paths never run. See
supports_self_test() for the measurements and the reason. Support elsewhere is probed at
runtime, so the feature activates by itself on hardware that does implement it.

What remains usable everywhere is the passive evidence: the SMART counters that AWS does
populate (``media_errors``, ``critical_warning``, ``available_spare``, ``percentage_used``,
data/command counters) and the Error Information Log. Note that AWS leaves ``temperature``,
``power_on_hours``, ``power_cycles``, ``unsafe_shutdowns`` and ``controller_busy_time`` at
placeholder zeros, so those must not be used as health signals on that backend.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import time
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
    """A single entry from the NVMe Error Information Log (Log Identifier 01h)."""

    error_count: int = 0
    submission_queue_id: int = 0
    command_id: int = 0
    # Bits 15:1 of the on-disk Status Field; bit 0 is split out as phase_tag.
    status_field: int = 0
    phase_tag: int = 0
    parm_error_location: int = 0
    lba: int = 0
    nsid: int = 0
    # "Vendor Specific Information Available": the log page identifier holding
    # extra vendor data (0h = none), not the vendor data itself.
    vendor_specific: int = 0
    transport_type: int = 0
    command_specific: int = 0
    # csi and opcode are only meaningful when log_page_version is 1; None means
    # the controller did not report them rather than "opcode 0".
    command_set_indicator: int | None = None
    opcode: int | None = None
    log_page_version: int = 0


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


# ---------------------------------------------------------------------------
# Log page parsing
#
# Log page layouts are fixed by the NVMe Base Specification and are stable
# across revisions; only the way nvme-cli renders them changes between its
# versions. Parsing the raw pages therefore removes a whole class of bugs that
# text/JSON scraping is prone to: inline value descriptions
# ("status_field : 0x2001 (Invalid Command Opcode: ...)"), the temperature unit
# order flipping between nvme-cli 1.x and 2.x, and the field renaming due in
# nvme-cli 3.0.
#
# Offsets below were cross-checked against libnvme v1.16 (struct nvme_smart_log,
# struct nvme_error_log_page, struct nvme_st_result), the Linux kernel's
# include/linux/nvme.h and SPDK's include/spdk/nvme_spec.h, which agree.
# ---------------------------------------------------------------------------

# Get Log Page - Log Page Identifiers.
LOG_PAGE_ERROR_INFORMATION = 0x01
LOG_PAGE_SMART_HEALTH = 0x02
LOG_PAGE_DEVICE_SELF_TEST = 0x06

SMART_LOG_PAGE_LEN = 512
ERROR_LOG_ENTRY_LEN = 64
SELF_TEST_LOG_PAGE_LEN = 564
SELF_TEST_RESULT_LEN = 28
SELF_TEST_MAX_RESULTS = 20
IDENTIFY_CONTROLLER_LEN = 4096

# Identify Controller offsets.
IDCTRL_OACS_OFFSET = 256  # Optional Admin Command Support, 2 bytes
IDCTRL_ELPE_OFFSET = 262  # Error Log Page Entries, 1 byte, 0's based

# OACS bit 4: "Device Self-test command supported". The same bit gates log page
# 06h, so one read covers both the trigger and the result read.
OACS_DEVICE_SELF_TEST = 0x10

# Error Information Log Entry: csi and opcode only carry meaning when the entry's
# Log Page Version is 1h. Controllers predating that revision leave those bytes
# reserved, and nvme-cli prints them regardless - which is why the collected
# artifacts showed "opcode=0x00" on hardware that never populated the field.
ERROR_LOG_PAGE_VERSION_WITH_OPCODE = 0x1

# Self-test result code 0xf means "entry not used" (no test recorded).
SELF_TEST_RESULT_NOT_USED = 0xF


def _le_uint(buf: bytes, offset: int, size: int) -> int:
    """Read a little-endian unsigned integer of ``size`` bytes at ``offset``."""
    return int.from_bytes(buf[offset : offset + size], "little")


def parse_smart_log_page(device_path: str, buf: bytes) -> NvmeSmartLog:
    """Parse the SMART / Health Information log page (02h, 512 bytes).

    Composite Temperature is in Kelvin by definition, so no unit guessing is
    needed. Data Units Read/Written are counted in thousands of 512-byte units
    and Controller Busy Time in minutes; both are reported raw, exactly as
    nvme-cli does.
    """
    return NvmeSmartLog(
        device_path=device_path,
        critical_warning=buf[0],
        temperature_kelvin=_le_uint(buf, 1, 2),
        available_spare=buf[3],
        available_spare_threshold=buf[4],
        percentage_used=buf[5],
        data_units_read=_le_uint(buf, 32, 16),
        data_units_written=_le_uint(buf, 48, 16),
        host_read_commands=_le_uint(buf, 64, 16),
        host_write_commands=_le_uint(buf, 80, 16),
        controller_busy_time=_le_uint(buf, 96, 16),
        power_cycles=_le_uint(buf, 112, 16),
        power_on_hours=_le_uint(buf, 128, 16),
        unsafe_shutdowns=_le_uint(buf, 144, 16),
        media_errors=_le_uint(buf, 160, 16),
        num_err_log_entries=_le_uint(buf, 176, 16),
    )


def parse_error_log_entry(buf: bytes) -> NvmeErrorLogEntry | None:
    """Parse one 64-byte Error Information Log entry.

    Returns None for an entry the spec defines as invalid: an Error Count of 0h
    marks an unused slot or a lost entry. ``nvme get-log`` always returns every
    slot the controller supports, most of them unused, so this is what keeps the
    collected artifact down to the entries that carry information.
    """
    error_count = _le_uint(buf, 0, 8)
    if error_count == 0:
        return None

    # Bits 15:1 are the Status Field; bit 0 is the Phase Tag.
    status = _le_uint(buf, 12, 2)
    log_page_version = buf[63]
    extended_fields_valid = log_page_version == ERROR_LOG_PAGE_VERSION_WITH_OPCODE

    return NvmeErrorLogEntry(
        error_count=error_count,
        submission_queue_id=_le_uint(buf, 8, 2),
        command_id=_le_uint(buf, 10, 2),
        status_field=status >> 1,
        phase_tag=status & 0x1,
        parm_error_location=_le_uint(buf, 14, 2),
        lba=_le_uint(buf, 16, 8),
        nsid=_le_uint(buf, 24, 4),
        vendor_specific=buf[28],
        transport_type=buf[29],
        command_set_indicator=buf[30] if extended_fields_valid else None,
        opcode=buf[31] if extended_fields_valid else None,
        command_specific=_le_uint(buf, 32, 8),
        log_page_version=log_page_version,
    )


def parse_error_log_page(buf: bytes) -> list[NvmeErrorLogEntry]:
    """Parse the Error Information log page (01h) into its populated entries."""
    entries = []
    for offset in range(0, len(buf) - ERROR_LOG_ENTRY_LEN + 1, ERROR_LOG_ENTRY_LEN):
        entry = parse_error_log_entry(buf[offset : offset + ERROR_LOG_ENTRY_LEN])
        if entry is not None:
            entries.append(entry)
    return entries


def parse_self_test_log_page(device_path: str, buf: bytes) -> NvmeSelfTestLog:
    """Parse the Device Self-test log page (06h, 564 bytes).

    Byte 0 holds the operation currently running (0 = none) in bits 3:0, byte 1
    its completion percentage in bits 6:0, and 20 result entries of 28 bytes
    follow from offset 4. Entries marked "not used" (result code 0xf) are
    skipped.
    """
    results = []
    for index in range(SELF_TEST_MAX_RESULTS):
        offset = 4 + index * SELF_TEST_RESULT_LEN
        entry = buf[offset : offset + SELF_TEST_RESULT_LEN]
        if len(entry) < SELF_TEST_RESULT_LEN:
            break

        # Device Self-test Status: bits 3:0 result, bits 7:4 the test type run.
        dsts = entry[0]
        result_code = dsts & 0xF
        if result_code == SELF_TEST_RESULT_NOT_USED:
            continue

        results.append(
            NvmeSelfTestResult(
                result_code=result_code,
                self_test_code=dsts >> 4,
                segment_number=entry[1],
                power_on_hours=_le_uint(entry, 4, 8),
                nsid=_le_uint(entry, 12, 4),
                failing_lba=_le_uint(entry, 16, 8),
                status_code_type=entry[24],
                status_code=entry[25],
            )
        )

    return NvmeSelfTestLog(
        device_path=device_path,
        current_operation=buf[0] & 0xF,
        current_completion=buf[1] & 0x7F,
        results=results,
    )


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


def _read_raw(node: "BaseNode", command: str, length: int, what: str) -> bytes | None:
    """Run an nvme-cli command that emits binary and return its bytes.

    The payload is base64-encoded on the node because the remoter returns text.
    The length check is the entire contract: either the structure was read in
    full or it was not. There is no notion of a "missing field" in a fixed-layout
    NVMe structure, so nothing here can silently degrade into a healthy-looking
    default the way scraping formatted output could.

    Args:
        node: SCT node with remoter.
        command: nvme-cli command emitting binary on stdout (without ``sudo``).
        length: Exact number of bytes the structure must contain.
        what: Human-readable name used in log messages.

    Returns:
        Exactly ``length`` bytes, or None if the read failed or came up short.
    """
    result = node.remoter.sudo(f"{command} | base64 -w0", ignore_status=True, timeout=NVME_CMD_TIMEOUT)
    if result.failed:
        node.log.warning("Reading %s failed: %s", what, result.stderr.strip())
        return None

    try:
        buf = base64.b64decode(result.stdout.strip(), validate=True)
    except (binascii.Error, ValueError) as exc:
        node.log.warning("Reading %s returned undecodable output: %s", what, exc)
        return None

    if len(buf) != length:
        # nvme writes its diagnostics to stderr, so it still explains a short read
        # even though the pipeline's exit status comes from base64.
        node.log.warning(
            "Reading %s returned %d bytes, expected %d: %s",
            what,
            len(buf),
            length,
            result.stderr.strip(),
        )
        return None

    return buf


def read_log_page(node: "BaseNode", device_path: str, log_id: int, length: int) -> bytes | None:
    """Read a raw NVMe log page via ``nvme get-log``."""
    return _read_raw(
        node,
        f"nvme get-log {device_path} --log-id={log_id} --log-len={length} -b",
        length,
        what=f"log page {log_id:#04x} on {device_path}",
    )


def read_identify_controller(node: "BaseNode", device_path: str) -> bytes | None:
    """Read the raw Identify Controller data structure.

    Identify is mandatory for every NVMe controller, so unlike an optional
    command it can never be rejected as an unsupported opcode - which is why it
    is safe to use for capability probing: it cannot add an Error Information
    Log entry the way probing with the real command would.
    """
    return _read_raw(
        node,
        f"nvme id-ctrl {device_path} -b",
        IDENTIFY_CONTROLLER_LEN,
        what=f"Identify Controller for {device_path}",
    )


def supports_self_test(node: "BaseNode", device_path: str) -> bool:
    """Check whether a controller implements the Device Self-test command.

    Issuing ``nvme device-self-test`` against a controller that does not
    support it is not free: the rejected admin command is recorded in the
    device's Error Information Log and increments ``num_err_log_entries``,
    so the diagnostic would manufacture the very anomaly it reports. The same
    OACS bit gates log page 06h, so this also decides whether the self-test log
    can be read at all.

    On AWS neither NVMe disk type supports it. Measured on i4i.4xlarge:

        /dev/nvme0n1  "Amazon Elastic Block Store"          OACS bit 4 clear
        /dev/nvme1n1  "Amazon EC2 NVMe Instance Storage"    oacs=0x8

    Both are presented by the Nitro card rather than by the physical SSD's
    own controller, so the media operation a self-test performs has nothing
    to act on - EBS is network-attached and has no local medium at all. AWS
    monitors and retires the physical drives itself; the guest was never
    meant to run media diagnostics. Other instance families and other cloud
    backends are unverified, hence the runtime probe rather than a hardcoded
    backend check.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").

    Returns:
        True only when the controller advertises self-test support. Any
        failure to determine it returns False, so we never probe blindly.
    """
    buf = read_identify_controller(node, device_path)
    if buf is None:
        node.log.warning("Could not read OACS for %s, assuming no self-test support", device_path)
        return False

    oacs = _le_uint(buf, IDCTRL_OACS_OFFSET, 2)
    supported = bool(oacs & OACS_DEVICE_SELF_TEST)
    if not supported:
        node.log.debug("Device self-test not supported on %s (oacs=%#x)", device_path, oacs)
    return supported


def error_log_entry_count(node: "BaseNode", device_path: str) -> int | None:
    """Number of Error Information Log entries the controller holds.

    Identify Controller ELPE is 0's based. Asking ``get-log`` for more entries
    than the controller supports can be rejected outright, so the page length
    has to come from the device rather than from a fixed guess.
    """
    buf = read_identify_controller(node, device_path)
    if buf is None:
        return None
    return buf[IDCTRL_ELPE_OFFSET] + 1


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
    buf = read_log_page(node, device_path, LOG_PAGE_SMART_HEALTH, SMART_LOG_PAGE_LEN)
    if buf is None:
        return None

    return parse_smart_log_page(device_path, buf)


def get_error_log(node: "BaseNode", device_path: str, max_entries: int | None = None) -> list[NvmeErrorLogEntry]:
    """Collect the populated Error Information Log entries for a device.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").
        max_entries: Number of entries to request. Defaults to the count the
            controller reports via Identify Controller ELPE.

    Returns:
        List of NvmeErrorLogEntry instances, unused slots omitted. Empty list
        on failure.
    """
    if max_entries is None:
        max_entries = error_log_entry_count(node, device_path)
        if max_entries is None:
            return []

    buf = read_log_page(node, device_path, LOG_PAGE_ERROR_INFORMATION, max_entries * ERROR_LOG_ENTRY_LEN)
    if buf is None:
        return []

    return parse_error_log_page(buf)


def run_self_test(node: "BaseNode", device_path: str, test_type: SelfTestType = SelfTestType.SHORT) -> bool:
    """Trigger a device self-test on an NVMe device.

    Controllers that do not implement the command are skipped rather than
    probed: see supports_self_test() for why issuing it blindly corrupts the
    very error-log counters this module reports on.

    Args:
        node: SCT node with remoter.
        device_path: NVMe device path (e.g. "/dev/nvme0n1").
        test_type: Type of self-test to run (SHORT=1, EXTENDED=2).

    Returns:
        True if the self-test was triggered successfully, False otherwise.
    """
    if not supports_self_test(node, device_path):
        node.log.info("Skipping self-test on %s: controller does not support it", device_path)
        return False

    result = node.remoter.sudo(
        f"nvme device-self-test -s {int(test_type)} {device_path}",
        ignore_status=True,
        timeout=NVME_CMD_TIMEOUT,
    )
    if result.failed:
        node.log.warning(
            "'nvme device-self-test' on %s failed: %s",
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
    buf = read_log_page(node, device_path, LOG_PAGE_DEVICE_SELF_TEST, SELF_TEST_LOG_PAGE_LEN)
    if buf is None:
        return None

    return parse_self_test_log_page(device_path, buf)


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


# Node attribute holding the SMART logs captured at node setup, keyed by device
# path. Error and media counters are lifetime totals, so only the growth since
# this baseline says anything about the test that just ran.
NVME_BASELINE_ATTR = "nvme_baseline_smart_logs"


def store_baseline_smart_logs(node: "BaseNode", smart_logs: list[NvmeSmartLog]) -> None:
    """Record the node's SMART logs as the baseline for later delta checks."""
    setattr(node, NVME_BASELINE_ATTR, {log.device_path: log for log in smart_logs})


def get_baseline_smart_log(node: "BaseNode", device_path: str) -> NvmeSmartLog | None:
    """Return the baseline SMART log for a device, or None if none was captured.

    A missing baseline is normal: node setup skips it when nvme-cli could not be
    installed, and reused clusters never ran it at all.
    """
    baseline = getattr(node, NVME_BASELINE_ATTR, None)
    if not isinstance(baseline, dict):
        return None
    return baseline.get(device_path)


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
    media errors or error log entries appeared during the test.

    Severity mapping:
        - critical_warning != 0 -> CRITICAL
        - media_errors above the setup baseline -> ERROR
        - num_err_log_entries above the setup baseline -> WARNING
          (also collects error log)
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

    # media_errors and num_err_log_entries are lifetime counters: a fresh cloud
    # instance can already carry entries from before the test started. Only the
    # growth over the setup baseline says something happened during this run.
    baseline = get_baseline_smart_log(current_node, device)
    new_media_errors = smart_log.media_errors - (baseline.media_errors if baseline else 0)
    new_error_entries = smart_log.num_err_log_entries - (baseline.num_err_log_entries if baseline else 0)

    # media errors appeared during the test -> ERROR
    if new_media_errors > 0:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.ERROR,
            node=current_node.name,
            error=(f"NVMe {device}: {new_media_errors} new media_errors during test (total={smart_log.media_errors})"),
        )
        _collect_error_log_with_timestamp(current_node, device)

    # error log entries appeared during the test -> WARNING (also collect error log)
    if new_error_entries > 0:
        yield ClusterHealthValidatorEvent.NvmeHealth(
            severity=Severity.WARNING,
            node=current_node.name,
            message=(
                f"NVMe {device}: {new_error_entries} new error log entries during test "
                f"(total={smart_log.num_err_log_entries})"
            ),
        )
        if new_media_errors <= 0:
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

    Saves a summary of the populated error log entries to the node's log
    directory with a timestamped filename for correlation with test events.
    The full unparsed output is collected separately as ``nvme_error_log.log``.
    """
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    device_name = device_path.replace("/dev/", "")
    filename = f"nvme_error_log_{device_name}_{timestamp}.log"

    # This runs inside the teardown health check, so a collection failure must
    # never take the health check down with it.
    try:
        # get_error_log already drops the unused slots: the spec defines an
        # Error Count of 0h as an invalid entry.
        entries = get_error_log(node, device_path)
        if not entries:
            node.log.debug("NVMe error log for %s has no populated entries", device_path)
            return

        lines = []
        for entry in entries:
            # opcode is only reported when the entry's log page version is 1
            opcode = "n/a" if entry.opcode is None else f"0x{entry.opcode:02x}"
            lines.append(
                f"error_count={entry.error_count} sqid={entry.submission_queue_id} "
                f"cmdid={entry.command_id} status=0x{entry.status_field:04x} "
                f"lba=0x{entry.lba:x} nsid={entry.nsid} opcode={opcode}"
            )
        content = "\n".join(lines) + "\n"

        log_dir = node.logdir
        if log_dir:
            filepath = f"{log_dir}/{filename}"
            with open(filepath, "w", encoding="utf-8") as fobj:
                fobj.write(content)
            node.log.info("NVMe error log saved to %s (%d entries)", filepath, len(entries))
    except Exception as exc:  # noqa: BLE001
        node.log.warning("Failed to collect NVMe error log for %s: %s", device_path, exc)


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

    Self-tests are triggered on every data disk before polling starts, so they
    run concurrently on the (independent) devices. The wall-clock time for a
    node therefore stays close to a single device's timeout instead of growing
    with the number of disks.

    Args:
        node: SCT node with remoter.
        test_type: Type of self-test (SHORT or EXTENDED).
        timeout: Max seconds to wait for all devices to complete. Defaults
            based on test_type.

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

    if timeout is None:
        timeout = SHORT_SELF_TEST_TIMEOUT if test_type == SelfTestType.SHORT else EXTENDED_SELF_TEST_TIMEOUT

    started_disks = []
    for disk in data_disks:
        if run_self_test(node, disk.device_path, test_type):
            started_disks.append(disk)
        else:
            node.log.warning("Failed to trigger self-test on %s, skipping", disk.device_path)

    # All tests run in parallel on the devices, so they share one deadline
    deadline = time.monotonic() + timeout
    results = []
    for disk in started_disks:
        # Keep at least one poll iteration so a finished test is not aborted
        remaining = max(int(deadline - time.monotonic()), 1)
        test_log = poll_self_test_completion(
            node,
            disk.device_path,
            timeout=remaining,
            test_type=test_type,
        )
        if test_log:
            results.append(test_log)
            # Publish events for failures
            for event in check_self_test_results(node, test_log):
                event.publish()

    return results
