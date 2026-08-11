"""Pre-start checks: KVM/docker presence, host memory arithmetic, AWS credentials."""

import logging
import re
import subprocess
from pathlib import Path

from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError

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
