"""Pre-start checks: KVM/docker presence, host and guest memory arithmetic, AWS credentials."""

import dataclasses
import logging
import re
import subprocess
import sys
from pathlib import Path

from sdcm.utils.minicloud.config import (
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT,
    MinicloudConfig,
    MinicloudError,
)

LOGGER = logging.getLogger(__name__)

HOST_HEADROOM_BASE_GIB = 2.0
HOST_HEADROOM_PER_GUEST_GIB = 1.0

# Every SCT param whose nodes become a minicloud QEMU guest, and therefore has to be part of
# the host-memory arithmetic. Keep in sync with the node pools tester.py provisions.
GUEST_NODE_COUNT_PARAMS = (
    "n_db_nodes",
    "n_loaders",
    "n_monitor_nodes",
    "n_db_zero_token_nodes",
    "n_vector_store_nodes",
)

# Count `n_test_oracle_db_nodes` only for db types that actually provision an oracle cluster
ORACLE_GUEST_DB_TYPES = ("mixed_scylla", "mixed_cassandra")


def sum_node_counts(value) -> int:
    """Sum an IntOrList param value: an int or a list."""
    if not value:
        return 0
    if isinstance(value, int):
        return value
    return sum(int(part) for part in value)


def db_guest_count(params) -> int:
    """Return the peak number of db guests in the run, at least 1.

    Counts the target size of a scale test and the oracle nodes of a db type that provisions them.
    """
    guests = max(sum_node_counts(params.get("n_db_nodes")), sum_node_counts(params.get("cluster_target_size")))
    if params.get("db_type") in ORACLE_GUEST_DB_TYPES:
        guests += sum_node_counts(params.get("n_test_oracle_db_nodes"))
    return max(guests, 1)


def _as_list(value) -> list:
    """Return a StringOrList param value as a list."""
    if not value:
        return []
    return [value] if isinstance(value, str) else list(value)


def _nemesis_thread_can_grow(runner: str, selector: str) -> bool:
    """Whether one nemesis thread may add nodes: every nemesis that adds nodes is disruptive."""
    if runner == "NoOpMonkey":
        return False
    if runner != "SisyphusMonkey":
        return True
    from sdcm.nemesis import NemesisBaseClass, NemesisFlags  # noqa: PLC0415 - circular import avoidance
    from sdcm.nemesis.registry import NemesisRegistry  # noqa: PLC0415 - circular import avoidance

    phrase = f"({selector}) and disruptive" if selector.strip() else "disruptive"
    try:
        return bool(NemesisRegistry(NemesisBaseClass, flag_class=NemesisFlags).filter_subclasses(phrase))
    except Exception:  # noqa: BLE001
        return True


def nemesis_growth_guests(params) -> int:
    """Return the most db guests the nemesis threads can add.

    Each nemesis thread that can run a disruptive nemesis adds `nemesis_add_node_cnt` guests.
    Selectors are matched to threads the same way as in the tester.
    """
    add_count = params.get("nemesis_add_node_cnt") or 0
    if not add_count:
        return 0
    selectors = _as_list(params.get("nemesis_selector"))
    growing = 0
    for index, runner in enumerate(_as_list(params.get("nemesis_class_name"))):
        if len(selectors) == 1:
            selector = selectors[0]
        else:
            selector = selectors[index] if index < len(selectors) else ""
        growing += _nemesis_thread_can_grow(runner.strip(), selector)
    return add_count * growing


def counted_guests(params) -> tuple[int, list[str]]:
    """Return the peak guest count of the run and the params it came from.

    Count every node pool, or a test with extra guests (oracle, zero-token, vector store)
    passes the gate and then OOM-kills the container.
    """
    counted_params: list[str] = list(GUEST_NODE_COUNT_PARAMS)
    if params.get("db_type") in ORACLE_GUEST_DB_TYPES:
        counted_params.append("n_test_oracle_db_nodes")
    guests = sum(sum_node_counts(params.get(name)) for name in counted_params)
    # n_db_nodes is only the starting size. A test that grows to cluster_target_size must fit at
    # its peak, or it dies when it adds the extra node.
    if target_size := sum_node_counts(params.get("cluster_target_size")):
        guests += max(0, target_size - sum_node_counts(params.get("n_db_nodes")))
    if growth := nemesis_growth_guests(params):
        guests += growth
        counted_params.append("nemesis_add_node_cnt")

    return guests, counted_params


def parse_memory_gib(value: str, param_name: str = "") -> float:
    """Parse a '4GiB'/'2.5GiB'/'4096MiB' memory string into GiB.

    ``param_name`` names the option the value came from: four different options are parsed
    here, and naming the wrong one sends whoever typed the typo to the wrong line of yaml.
    """
    # One optional decimal point, not [\d.]+: the loose form matches '1.2.3GiB' and '..GiB',
    # and float() then raises a bare ValueError instead of the actionable MinicloudError.
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT])i?B?\s*", str(value), flags=re.IGNORECASE)
    if not match:
        raise MinicloudError(f"cannot parse {param_name or 'memory'} value: {value!r}")
    factor = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1, "T": 1024}[match.group(2).upper()]
    return float(match.group(1)) * factor


# Scylla own reservation for the guest OS (from seastar resource.cc:calculate_memory()):
#   max(1.5GiB, 7% of RAM) + 50MiB per shard
SCYLLA_DEFAULT_RESERVE_FLOOR_GIB = 1.5
SCYLLA_DEFAULT_RESERVE_FRACTION = 0.07
SCYLLA_DEFAULT_RESERVE_PER_SHARD_GIB = 50 / 1024
# The guest kernel keeps ~0.2GiB of the VM's RAM before Scylla sees it; 0.3GiB leaves headroom
GUEST_KERNEL_RESERVE_GIB = 0.3
# What Scylla itself has to keep
SCYLLA_MIN_MEMORY_GIB = 2.0
SCYLLA_MIN_MEMORY_PER_SHARD_GIB = 1.0

DOCKER_STATS_TIMEOUT = 20


def _resolve_scylla_reserve_memory(params) -> tuple[str | None, str | None]:
    """Resolve the reserve-memory value and an optional warning/error message."""
    requested = str(params.get("minicloud_scylla_reserve_memory") or "").strip()
    if not requested:
        return None, None

    lightweight = params.get("minicloud_lightweight")
    if lightweight is not None and not lightweight:
        return None, None

    guest = str(params.get("minicloud_lightweight_memory") or MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT)
    guest_gib = parse_memory_gib(guest, "minicloud_lightweight_memory")
    requested_gib = parse_memory_gib(requested, "minicloud_scylla_reserve_memory")
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


def _scylla_os_reserve_gib(guest_gib: float, vcpus: int) -> float:
    """Memory Scylla keeps for the guest OS by default, from what the guest kernel leaves it."""
    visible_gib = guest_gib - GUEST_KERNEL_RESERVE_GIB
    return (
        max(SCYLLA_DEFAULT_RESERVE_FLOOR_GIB, SCYLLA_DEFAULT_RESERVE_FRACTION * visible_gib)
        + SCYLLA_DEFAULT_RESERVE_PER_SHARD_GIB * vcpus
    )


def memory_per_shard_gib(guest_gib: float, vcpus: int) -> float:
    """Memory each Scylla shard gets in a db guest of ``guest_gib`` with ``vcpus`` shards."""
    return (guest_gib - GUEST_KERNEL_RESERVE_GIB - _scylla_os_reserve_gib(guest_gib, vcpus)) / vcpus


def _check_memory_per_shard(params) -> None:
    """Fail when Scylla's default OS reservation leaves a shard under 1 GiB: production mode refuses to start."""
    if params.get("minicloud_lightweight") is False or (params.get("minicloud_scylla_reserve_memory") or "").strip():
        return
    guest = params.get("minicloud_lightweight_memory") or MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    guest_gib = parse_memory_gib(guest, "minicloud_lightweight_memory")
    vcpus = int(params.get("minicloud_lightweight_vcpus") or MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT)
    reserve_gib = _scylla_os_reserve_gib(guest_gib, vcpus)
    per_shard_gib = memory_per_shard_gib(guest_gib, vcpus)
    if per_shard_gib < SCYLLA_MIN_MEMORY_PER_SHARD_GIB:
        floor_gib = GUEST_KERNEL_RESERVE_GIB + reserve_gib + SCYLLA_MIN_MEMORY_PER_SHARD_GIB * vcpus
        raise MinicloudError(
            f"minicloud_lightweight_memory={guest} with minicloud_lightweight_vcpus={vcpus} leaves "
            f"{per_shard_gib * 1024:.0f}MiB per shard after Scylla reserves {reserve_gib:.2f}GiB for the OS; "
            f"Scylla refuses to start below 1GiB per shard.\nRaise minicloud_lightweight_memory to at least "
            f"{floor_gib:.1f}GiB or lower minicloud_lightweight_vcpus."
        )


def check_scylla_memory_budget(params) -> None:
    """Fail early when Scylla cannot start in the db guest, or the requested reserve-memory cannot be honored."""
    _check_memory_per_shard(params)
    value, complaint = _resolve_scylla_reserve_memory(params)
    if complaint:
        if value is None:
            raise MinicloudError(complaint)
        LOGGER.warning("%s", complaint)
    if value:
        LOGGER.info("scylla-server will run with --reserve-memory %s on minicloud guests", value)


def container_mem_in_use_gib(container_name: str) -> float:
    """Return the memory in use by the minicloud container and its QEMU guests, in GiB.

    Returns 0.0 when `docker stats` cannot give the figure, so the gate falls back to MemAvailable alone.
    """
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container_name],
            capture_output=True,
            timeout=DOCKER_STATS_TIMEOUT,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        LOGGER.debug("cannot read the memory in use by container %s: %s", container_name, exc)
        return 0.0
    if result.returncode != 0:
        LOGGER.debug(
            "cannot read the memory in use by container %s: docker stats exited %s", container_name, result.returncode
        )
        return 0.0

    mem_in_use = result.stdout.decode(errors="replace").split("/")[0].strip()
    try:
        return parse_memory_gib(mem_in_use, "docker stats MemUsage")
    except MinicloudError as exc:
        LOGGER.debug("%s", exc)
        return 0.0


def _guest_memory_gib(config: MinicloudConfig, params, guests: int) -> tuple[float, str]:
    """Return the memory ``guests`` need and its per-role breakdown for messages.

    Loaders and monitors use their own sizes (the db size when unset), all other guests the db size.
    """
    loaders = sum_node_counts(params.get("n_loaders"))
    monitors = sum_node_counts(params.get("n_monitor_nodes"))
    db_gib = parse_memory_gib(config.lightweight_memory, "minicloud_lightweight_memory")
    loader_gib = monitor_gib = db_gib
    if config.lightweight_memory_loader:
        loader_gib = parse_memory_gib(config.lightweight_memory_loader, "minicloud_lightweight_memory_loader")
    if config.lightweight_memory_monitor:
        monitor_gib = parse_memory_gib(config.lightweight_memory_monitor, "minicloud_lightweight_memory_monitor")
    roles = [
        (role, count, gib)
        for role, count, gib in (
            ("db", guests - loaders - monitors, db_gib),
            ("loader", loaders, loader_gib),
            ("monitor", monitors, monitor_gib),
        )
        if count
    ]
    return (
        sum(count * gib for _, count, gib in roles),
        " + ".join(f"{count} {role} x {gib:.1f}GiB" for role, count, gib in roles),
    )


def _vm_stat_available_gib() -> float | None:
    """Free plus inactive memory from macOS `vm_stat`, in GiB, else None."""
    try:
        result = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=10, check=False)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        LOGGER.debug("cannot run vm_stat: %s", exc)
        return None
    if result.returncode != 0:
        return None
    page_size = re.search(r"page size of (\d+) bytes", result.stdout)
    pages = dict(re.findall(r"^Pages (free|inactive):\s+(\d+)\.", result.stdout, re.MULTILINE))
    if not page_size or len(pages) != 2:
        return None
    return (int(pages["free"]) + int(pages["inactive"])) * int(page_size.group(1)) / 1024**3


def host_available_memory_gib() -> float | None:
    """Memory that the host can give to new guests, in GiB, else None.

    Linux reports MemAvailable in /proc/meminfo. macOS runs the native minicloud, and its
    `vm_stat` gives the free and the inactive pages, the same figure that psutil uses there.
    """
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024 / 1024
        return None
    if sys.platform == "darwin":
        return _vm_stat_available_gib()
    return None


def host_total_memory_gib() -> float | None:
    """MemTotal of a Linux host in GiB, else None."""
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return None
    for line in meminfo.read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) / 1024 / 1024
    return None


@dataclasses.dataclass(frozen=True)
class MemoryBudget:
    """Memory the guests of a run have to fit into, and the headroom the gate adds to the guests own size."""

    gib: float
    source: str
    headroom_gib: float


def host_memory_budget(config: MinicloudConfig, guests: int) -> MemoryBudget | None:
    """Return the budget the memory gate measures ``guests`` guests against, or None when the host reports none."""
    if config.container_memory:
        # only the per-guest headroom counts: QEMU runs inside the cap, dockerd and SCT run outside it.
        return MemoryBudget(
            gib=parse_memory_gib(config.container_memory, "minicloud_container_memory"),
            source=f"the minicloud_container_memory cap ({config.container_memory})",
            headroom_gib=HOST_HEADROOM_PER_GUEST_GIB * guests,
        )
    available_gib = host_available_memory_gib()
    if available_gib is None:
        return None

    mem_in_use_gib = container_mem_in_use_gib(config.container_name)
    source = "available host memory"
    if mem_in_use_gib:
        source += (
            f" ({available_gib:.1f}GiB MemAvailable + {mem_in_use_gib:.1f}GiB already held by the "
            f"running '{config.container_name}' container)"
        )
    # the base headroom is for dockerd, hydra, SCT and the page cache, the per-guest one for QEMU:
    # 8 guests take ~9GiB of host memory on top of their own.
    return MemoryBudget(
        gib=available_gib + mem_in_use_gib,
        source=source,
        headroom_gib=HOST_HEADROOM_BASE_GIB + HOST_HEADROOM_PER_GUEST_GIB * guests,
    )


def check_host_memory(config: MinicloudConfig, params) -> None:
    """Fail before start when the test's guests cannot fit into this host's free memory.

    Lightweight mode gives every guest a fixed size for its role (``lightweight_memory`` for
    db-like guests, the loader and monitor sizes for theirs, which default to the db size), so
    the requirement is exactly the per-role sum plus host headroom - and only params knows the
    guest counts: ``n_db_nodes`` is IntOrList ([3, 3] for multi-DC), summed the way
    sct_config.py:sum(n_db_nodes) does. Without this check the container is
    cgroup-OOM-killed mid-test (exit 137) and every VM dies with it.

    The host budget is MemAvailable plus whatever the running minicloud container already
    holds: the CI pipeline provisions the guests before run-test re-runs this gate, and by
    then MemAvailable excludes their resident memory while the guests still have to be
    budgeted in full. Leaving the memory in use out charged the cluster twice.

    When ``minicloud_container_memory`` caps the container, that cap - not the host's free
    memory - is what the guests actually have to fit into, and it is the figure the cgroup
    OOM killer enforces. Measuring against the host instead would happily pass a test that
    the cap kills. Guests already running live inside the cap, so their memory in use is not
    added there.

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
    guests, counted_params = counted_guests(params)
    if not guests:
        return
    needed_gib, breakdown = _guest_memory_gib(config, params, guests)
    budget = host_memory_budget(config, guests)
    if budget is None:
        return

    needed_gib += budget.headroom_gib
    budget_gib, budget_source = budget.gib, budget.source
    if config.container_memory:
        headroom_note = (
            f" + {budget.headroom_gib:.0f}GiB QEMU overhead ({HOST_HEADROOM_PER_GUEST_GIB:.0f}GiB per guest)"
        )
    else:
        headroom_note = (
            f" + {budget.headroom_gib:.0f}GiB host headroom ({HOST_HEADROOM_BASE_GIB:.0f}GiB + "
            f"{HOST_HEADROOM_PER_GUEST_GIB:.0f}GiB per guest)"
        )
    LOGGER.info(
        "host-memory gate: %.1fGiB budget from %s for %d guest(s) needing %.1fGiB",
        budget_gib,
        budget_source,
        guests,
        needed_gib,
    )
    if budget_gib < needed_gib:
        raise MinicloudError(
            f"not enough memory for this test: {guests} guest(s): {breakdown}{headroom_note} = "
            f"{needed_gib:.1f}GiB needed, but only {budget_gib:.1f}GiB is available from "
            f"{budget_source}. Reduce {'/'.join(counted_params)}, lower "
            f"minicloud_lightweight_memory or minicloud_lightweight_memory_loader/_monitor, "
            f"raise the budget, or set "
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
