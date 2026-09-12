"""Pre-start checks: KVM/docker presence, host and guest memory arithmetic, AWS credentials."""

import logging
import re
import subprocess
from pathlib import Path

from sdcm.utils.minicloud.config import (
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT,
    MinicloudConfig,
    MinicloudError,
)

LOGGER = logging.getLogger(__name__)

# Every SCT param whose nodes become a minicloud QEMU guest, and therefore has to be part of
# the host-memory arithmetic. Keep in sync with the node pools tester.py provisions.
GUEST_NODE_COUNT_PARAMS = (
    "n_db_nodes",
    "n_loaders",
    "n_monitor_nodes",
    "n_test_oracle_db_nodes",
    "n_db_zero_token_nodes",
    "n_vector_store_nodes",
)


def sum_node_counts(value) -> int:
    """Sum an IntOrList param value: an int, a list, or a '3 3' multi-DC string."""
    if not value:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return sum(int(part) for part in value.split())
    return sum(int(part) for part in value)


def parse_memory_gib(value: str) -> float:
    """Parse a '4GiB'/'2.5GiB'/'4096MiB' memory string into GiB."""
    # One optional decimal point, not [\d.]+: the loose form matches '1.2.3GiB' and '..GiB',
    # and float() then raises a bare ValueError instead of the actionable MinicloudError.
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT])i?B?\s*", str(value), flags=re.IGNORECASE)
    if not match:
        raise MinicloudError(f"cannot parse minicloud_lightweight_memory value: {value!r}")
    factor = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1, "T": 1024}[match.group(2).upper()]
    return float(match.group(1)) * factor


# Scylla own reservation for the guest OS (from seastar resource.cc:calculate_memory()):
#   max(1.5GiB, 7% of RAM) + 50MiB per shard
SCYLLA_DEFAULT_RESERVE_FLOOR_GIB = 1.5
SCYLLA_DEFAULT_RESERVE_FRACTION = 0.07
# What Scylla itself has to keep
SCYLLA_MIN_MEMORY_GIB = 2.0
SCYLLA_MIN_MEMORY_PER_SHARD_GIB = 1.0


def _resolve_scylla_reserve_memory(params) -> tuple[str | None, str | None]:
    """Resolve the reserve-memory value and an optional warning/error message."""
    requested = str(params.get("minicloud_scylla_reserve_memory") or "").strip()
    if not requested:
        return None, None

    lightweight = params.get("minicloud_lightweight")
    if lightweight is not None and not lightweight:
        return None, None

    guest = str(params.get("minicloud_lightweight_memory") or MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT)
    guest_gib = parse_memory_gib(guest)
    requested_gib = parse_memory_gib(requested)
    vcpus = int(params.get("minicloud_lightweight_vcpus") or MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT)

    scylla_min_gib = max(SCYLLA_MIN_MEMORY_GIB, SCYLLA_MIN_MEMORY_PER_SHARD_GIB * vcpus)
    default_reserve_gib = max(SCYLLA_DEFAULT_RESERVE_FLOOR_GIB, SCYLLA_DEFAULT_RESERVE_FRACTION * guest_gib)
    reserve_gib = min(requested_gib, guest_gib - scylla_min_gib)

    shards = "1 shard" if vcpus == 1 else f"{vcpus} shards"
    if reserve_gib <= default_reserve_gib:
        return None, (
            f"minicloud_scylla_reserve_memory={requested} does not fit {guest} guest: Scylla needs "
            f"{scylla_min_gib:.0f}GiB for {shards}, leaving only {max(reserve_gib, 0.0):.1f}GiB for the OS - less than "
            f"{default_reserve_gib:.1f}GiB it reserves anyway.\nRaise minicloud_lightweight_memory or drop the option."
        )
    if reserve_gib < requested_gib:
        return f"{int(reserve_gib * 1024)}M", (
            f"minicloud_scylla_reserve_memory={requested} capped to {reserve_gib:.1f}GiB: {guest} guest "
            f"must leave Scylla {scylla_min_gib:.0f}GiB for {shards}"
        )

    return f"{int(reserve_gib * 1024)}M", None


def scylla_reserve_memory(params) -> str | None:
    """Return the lightweight guest reserve-memory value, or None if nothing should be added."""
    try:
        value, _ = _resolve_scylla_reserve_memory(params)
    except MinicloudError as exc:
        LOGGER.warning("ignoring minicloud_scylla_reserve_memory: %s", exc)
        return None
    return value


def check_scylla_memory_budget(params) -> None:
    """Fail early when the requested reserve-memory cannot be honored."""
    value, complaint = _resolve_scylla_reserve_memory(params)
    if complaint:
        if value is None:
            raise MinicloudError(complaint)
        LOGGER.warning("%s", complaint)
    if value:
        LOGGER.info("scylla-server will run with --reserve-memory %s on minicloud guests", value)


def check_host_memory(config: MinicloudConfig, params) -> None:
    """Fail before start when the test's guests cannot fit into this host's free memory.

    Lightweight mode gives every guest a fixed ``lightweight_memory``, so the requirement
    is exactly guests x per-guest plus host headroom - and only params knows the guest
    count: ``n_db_nodes`` is IntOrList ('3 3' for multi-DC), summed the way
    sct_config.py:sum(n_db_nodes) does. Without this check the container is
    cgroup-OOM-killed mid-test (exit 137) and every VM dies with it.

    When ``minicloud_container_memory`` caps the container, that cap - not the host's free
    memory - is what the guests actually have to fit into, and it is the figure the cgroup
    OOM killer enforces. Measuring against the host instead would happily pass a test that
    the cap kills.

    The ``minicloud_skip_memory_check`` param (SCT_MINICLOUD_SKIP_MEMORY_CHECK) disables
    the gate — the arithmetic is deliberately conservative, and a developer who knows the
    workload's real footprint should not be blocked by it.
    """
    if config.skip_memory_check:
        LOGGER.warning(
            "minicloud_skip_memory_check is set — skipping the host-memory gate; an oversized "
            "test will die mid-run as a container OOM kill (exit 137) taking every VM with it"
        )
        return
    if not config.lightweight:
        return  # non-lightweight sizing follows the requested instance types; out of scope here
    # every pool that becomes a guest has to be counted, or a test with an oracle cluster,
    # zero-token nodes or a vector store passes the gate and still OOM-kills the container.
    guests = sum(sum_node_counts(params.get(name)) for name in GUEST_NODE_COUNT_PARAMS)
    # n_db_nodes is only where the cluster *starts*. A test that grows it - the scale tests set
    # cluster_target_size, and longevity_test grows to it - peaks higher, and the peak is what has
    # to fit: a gate that sizes the initial cluster only would pass and then let the run die at the
    # exact moment it adds the node nobody budgeted for. Same idiom as
    # provision/aws/capacity_reservation.py, which sizes its reservation off the target too.
    if target_size := sum_node_counts(params.get("cluster_target_size")):
        guests += max(0, target_size - sum_node_counts(params.get("n_db_nodes")))
    if not guests:
        return
    per_guest_gib = parse_memory_gib(config.lightweight_memory)
    if config.container_memory:
        # The cap is the whole budget the guests get, so no host headroom is subtracted from
        # it - dockerd and SCT live outside the cgroup.
        budget_gib = parse_memory_gib(config.container_memory)
        needed_gib = guests * per_guest_gib
        budget_source = f"the minicloud_container_memory cap ({config.container_memory})"
        headroom_note = ""
    else:
        meminfo = Path("/proc/meminfo")
        if not meminfo.exists():  # non-Linux dev box; the container will not run here anyway
            return
        budget_gib = 0.0
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                budget_gib = int(line.split()[1]) / 1024 / 1024
                break
        host_headroom_gib = 2.0  # dockerd, hydra, SCT itself and the page cache need to live too
        needed_gib = guests * per_guest_gib + host_headroom_gib
        budget_source = "available host memory"
        headroom_note = f" + {host_headroom_gib:.0f}GiB host headroom"
    if budget_gib and budget_gib < needed_gib:
        raise MinicloudError(
            f"not enough memory for this test: {guests} guest(s) x "
            f"{per_guest_gib:.1f}GiB ({config.lightweight_memory}){headroom_note} = "
            f"{needed_gib:.1f}GiB needed, but only {budget_gib:.1f}GiB is available from "
            f"{budget_source}. Reduce {'/'.join(GUEST_NODE_COUNT_PARAMS)}, lower "
            f"minicloud_lightweight_memory, raise the budget, or set "
            f"SCT_MINICLOUD_SKIP_MEMORY_CHECK=true if you know the real footprint - otherwise the "
            f"container is OOM-killed mid-test (exit 137) taking every VM with it."
        )


def check_aws_credentials() -> None:
    """Verify AWS credentials are configured and valid.

    Pins real STS explicitly, the same way sdcm.utils.aws_okta does: a localhost
    ``AWS_ENDPOINT_URL`` is itself one of the minicloud activation paths, so this
    subprocess would otherwise ask the emulator to validate the credentials the
    emulator needs for its own passthrough calls.
    """
    sts_cmd = ["aws", "--endpoint-url", "https://sts.amazonaws.com", "--region", "us-east-1"]
    try:
        result = subprocess.run(
            [*sts_cmd, "sts", "get-caller-identity"],
            capture_output=True,
            timeout=15,
            check=False,
        )
        if result.returncode != 0:
            raise MinicloudError(
                f"AWS credentials are not configured or are expired. "
                f"Run '{' '.join(sts_cmd)} sts get-caller-identity' to diagnose."
            )
    except FileNotFoundError as exc:
        raise MinicloudError("AWS CLI not found. Install it or ensure it is on PATH.") from exc
