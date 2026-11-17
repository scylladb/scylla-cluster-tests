"""Guest sizes derived from the host that runs minicloud: the `auto` value of the size knobs."""

import dataclasses
import json
import logging
import os
import socket
from pathlib import Path

from argus.client.generic_result import Status
from sdcm.argus_results import MinicloudHostSizingTable, submit_results_to_argus
from sdcm.utils.minicloud.config import (
    AUTO_SIZE_PARAMS,
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT,
    MINICLOUD_ROLE_TAG_KEY,
    MinicloudConfig,
    MinicloudError,
    is_auto,
)
from sdcm.utils.minicloud.preflight import (
    SCYLLA_MIN_MEMORY_PER_SHARD_GIB,
    MemoryBudget,
    _guest_memory_gib,
    counted_guests,
    host_memory_budget,
    host_total_memory_gib,
    memory_per_shard_gib,
    nemesis_growth_guests,
    parse_memory_gib,
    sum_node_counts,
)

LOGGER = logging.getLogger(__name__)

AUTO_KNOBS = AUTO_SIZE_PARAMS
# written to minicloud_state_dir when SCT sizes the guests; read back only for the report
SIZING_FILE = "sizing.json"

# largest db guest memory `auto` picks when minicloud_auto_memory_max is unset: 60GiB hosts ran 6GiB
AUTO_MEMORY_MAX_DEFAULT = "6GiB"
# free memory kept on top of the gate formula: an exact fit leaves only ~1GiB free, and the gate
# that runs again after provisioning sees a smaller budget.
DERIVATION_MARGIN_GIB = 2.0
# `auto` never gives a db guest fewer vCPUs: the 6-node rolling upgrade fails at 1 vCPU per db guest
DB_VCPUS = 2
# largest db guest vCPU count `auto` picks when minicloud_auto_vcpus_max is unset
AUTO_VCPUS_MAX_DEFAULT = 2
# the loader runs full-size stress: at least 2 vCPUs, up to 4 if the host has threads to spare.
LOADER_VCPUS_MIN = 2
LOADER_VCPUS_MAX = 4


def db_memory_floor_gib(db_vcpus: int) -> int:
    """Return the smallest whole-GiB db guest that gives each Scylla shard 1GiB (4GiB at 2 vCPUs)."""
    gib = 1
    while memory_per_shard_gib(gib, db_vcpus) < SCYLLA_MIN_MEMORY_PER_SHARD_GIB:
        gib += 1
    return gib


def _auto_memory_max_gib(params) -> int:
    return int(
        parse_memory_gib(
            params.get("minicloud_auto_memory_max") or AUTO_MEMORY_MAX_DEFAULT, "minicloud_auto_memory_max"
        )
    )


def _needed_gib(config: MinicloudConfig, params, guests: int, budget: MemoryBudget, db_gib: int) -> tuple[float, str]:
    """Return what ``guests`` need with ``db_gib`` db guests, the gate headroom and the margin, and the breakdown."""
    needed_gib, breakdown = _guest_memory_gib(
        dataclasses.replace(config, lightweight_memory=f"{db_gib}GiB"), params, guests
    )
    return needed_gib + budget.headroom_gib + DERIVATION_MARGIN_GIB, breakdown


def derive_db_memory(config: MinicloudConfig, params, guests: int, budget: MemoryBudget, db_vcpus: int) -> str:
    """Return the largest db guest size that fits ``budget`` with the gate's headroom and the margin.

    Tries whole GiB from minicloud_auto_memory_max down to the floor of ``db_vcpus`` shards.
    """
    ceiling = _auto_memory_max_gib(params)
    floor = db_memory_floor_gib(db_vcpus)
    if ceiling < floor:
        raise MinicloudError(
            f"minicloud_auto_memory_max={ceiling}GiB is below the {floor}GiB a db guest needs at {db_vcpus} "
            "vCPUs (1GiB per shard). Raise it or lower minicloud_lightweight_vcpus."
        )

    usable = range(ceiling, floor - 1, -1)
    for gib in usable:
        needed_total, breakdown = _needed_gib(config, params, guests, budget, gib)
        if needed_total <= budget.gib:
            return f"{gib}GiB"
    headroom = "QEMU overhead" if config.container_memory else "host headroom"
    raise MinicloudError(
        f"{guests} guest(s) do not fit even with {usable[-1]}GiB db guests: {breakdown} + "
        f"{budget.headroom_gib:.0f}GiB {headroom} + {DERIVATION_MARGIN_GIB:.0f}GiB margin = {needed_total:.1f}GiB "
        f"needed, {budget.gib:.1f}GiB available ({budget.source}), {needed_total - budget.gib:.1f}GiB short. "
        "Reduce the node counts or set minicloud_lightweight_memory to a number."
    )


def _memory_allows(config: MinicloudConfig, params, derived: list[str], guests: int, budget, vcpus: int) -> bool:
    """Whether the db guest memory can give each of ``vcpus`` shards 1GiB, within the memory ceiling and the budget."""
    if "minicloud_lightweight_memory" not in derived:
        memory = params.get("minicloud_lightweight_memory") or MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
        return (
            memory_per_shard_gib(parse_memory_gib(memory, "minicloud_lightweight_memory"), vcpus)
            >= SCYLLA_MIN_MEMORY_PER_SHARD_GIB
        )
    floor = db_memory_floor_gib(vcpus)
    if floor > _auto_memory_max_gib(params):
        return False
    return budget is None or _needed_gib(config, params, guests, budget, floor)[0] <= budget.gib


def derive_db_vcpus(
    config: MinicloudConfig, params, derived: list[str], guests: int, budget: MemoryBudget | None, host_threads: int
) -> int:
    """Return the most db guest vCPUs the host allows, from minicloud_auto_vcpus_max down to 2.

    A count fits when all guests (an `auto` loader at 2 vCPUs) fit the host threads and every shard
    gets 1GiB. If none fits, return 2: CPU overcommit slows the guests but does not kill them.
    """
    loaders = sum_node_counts(params.get("n_loaders"))
    monitors = sum_node_counts(params.get("n_monitor_nodes"))
    db_guests = guests - loaders - monitors
    ceiling = int(params.get("minicloud_auto_vcpus_max") or AUTO_VCPUS_MAX_DEFAULT)
    if "minicloud_lightweight_vcpus_loader" in derived:
        loader_vcpus = LOADER_VCPUS_MIN
    else:
        loader_vcpus = params.get("minicloud_lightweight_vcpus_loader")
    for vcpus in range(ceiling, DB_VCPUS, -1):
        monitor_vcpus = config.lightweight_vcpus_monitor or vcpus
        threads = db_guests * vcpus + monitors * monitor_vcpus + loaders * (loader_vcpus or vcpus)
        if threads <= host_threads and _memory_allows(config, params, derived, guests, budget, vcpus):
            return vcpus
    return DB_VCPUS


def derive_loader_vcpus(
    host_threads: int, db_guests: int, db_vcpus: int, loaders: int, monitors: int, monitor_vcpus: int
) -> int:
    """Return each loader share of the host threads the db guests and monitors leave, from 2 to 4."""
    spare = host_threads - db_guests * db_vcpus - monitors * monitor_vcpus
    return min(max(spare // max(loaders, 1), LOADER_VCPUS_MIN), LOADER_VCPUS_MAX)


@dataclasses.dataclass(frozen=True)
class LightweightSizes:
    """The db guest size and the loader vCPUs a running minicloud was started with."""

    memory: str
    vcpus: int
    vcpus_loader: int | None


def _flag_value(cmd: list[str], flag: str) -> str | None:
    index = cmd.index(flag) + 1 if flag in cmd else 0
    return cmd[index] if 0 < index < len(cmd) else None


def parse_lightweight_flags(cmd: list[str]) -> LightweightSizes | None:
    """Read the sizes from the command line of a running minicloud, or None when it has none."""
    memory_flag, vcpus_flag = _flag_value(cmd, "--lightweight-memory"), _flag_value(cmd, "--lightweight-vcpus")
    if not memory_flag or not vcpus_flag:
        return None

    memory = next((item for item in memory_flag.split(",") if "=" not in item), None)
    vcpus = next((item for item in vcpus_flag.split(",") if "=" not in item), None)
    if not memory or not vcpus:
        return None

    loader_prefix = f"{MINICLOUD_ROLE_TAG_KEY}=loader=".lower()
    loader = next(
        (item[len(loader_prefix) :] for item in vcpus_flag.split(",") if item.lower().startswith(loader_prefix)), None
    )
    return LightweightSizes(memory=memory, vcpus=int(vcpus), vcpus_loader=int(loader) if loader else None)


def host_thread_budget(config: MinicloudConfig) -> int:
    """Return the host threads the guests share: all of them, or the minicloud_container_cpus cap."""
    threads = os.cpu_count() or 1
    if config.container_cpus:
        threads = min(threads, max(int(float(config.container_cpus)), 1))
    return threads


def _derive_sizes(
    params, config: MinicloudConfig, derived: list[str], guests: int, budget: MemoryBudget | None, host_threads: int
):
    """Return the db memory, db vCPUs and loader vCPUs: derived for the `auto` knobs, as given for the others."""
    loaders = sum_node_counts(params.get("n_loaders"))
    monitors = sum_node_counts(params.get("n_monitor_nodes"))
    db_guests = guests - loaders - monitors
    if "minicloud_lightweight_vcpus" in derived:
        db_vcpus = derive_db_vcpus(config, params, derived, guests, budget, host_threads)
    else:
        db_vcpus = int(params.get("minicloud_lightweight_vcpus") or MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT)
    if "minicloud_lightweight_memory" not in derived:
        db_memory = params.get("minicloud_lightweight_memory")
    elif budget is None:
        db_memory = f"{db_memory_floor_gib(db_vcpus)}GiB"
        LOGGER.warning("the host reports no free memory: db guests get the %s floor", db_memory)
    else:
        db_memory = derive_db_memory(config, params, guests, budget, db_vcpus)

    monitor_vcpus = config.lightweight_vcpus_monitor or db_vcpus
    if "minicloud_lightweight_vcpus_loader" in derived:
        loader_vcpus = derive_loader_vcpus(host_threads, db_guests, db_vcpus, loaders, monitors, monitor_vcpus)
    else:
        loader_vcpus = params.get("minicloud_lightweight_vcpus_loader")

    guest_vcpus = db_guests * db_vcpus + loaders * (loader_vcpus or db_vcpus) + monitors * monitor_vcpus
    if guest_vcpus > host_threads:
        LOGGER.warning("the guests get %d vCPUs at their peak on a host with %d threads", guest_vcpus, host_threads)
    return db_memory, db_vcpus, loader_vcpus


def _sizing_record(
    params, derived: list[str], guests: int, budget: MemoryBudget | None, host_threads: int, sizes: tuple
) -> dict:
    """The host figures, the peak guest counts and the sizes chosen, as stored in `sizing.json`."""
    loaders = sum_node_counts(params.get("n_loaders"))
    monitors = sum_node_counts(params.get("n_monitor_nodes"))
    return {
        "host": {
            "name": socket.gethostname(),
            "cpu_threads": host_threads,
            "mem_total_gib": host_total_memory_gib(),
            "budget_gib": budget.gib if budget else None,
            "budget_source": budget.source if budget else "not reported by the host",
        },
        "guests": {
            "total": guests,
            "db": guests - loaders - monitors,
            "loaders": loaders,
            "monitors": monitors,
            "nemesis_growth": nemesis_growth_guests(params),
        },
        "sizes": dict(zip(("db_memory", "db_vcpus", "loader_vcpus"), sizes)),
        "derived": derived,
        "flags": list(MinicloudConfig.from_env(params=params).lightweight_flag_values()),
    }


def resolve_auto_sizing(params, config: MinicloudConfig, running_cmd: list[str] | None) -> dict | None:
    """Replace every `auto` size in ``params`` with a number.

    If minicloud is running, reuse its sizes, so every stage gets the same values and none
    restarts it. Otherwise, size from the host, write the record to `sizing.json` in the state
    dir and return it. Return None when nothing was sized from the host.
    """
    derived = [knob for knob in AUTO_KNOBS if is_auto(params.get(knob))]
    if not derived:
        return None
    if params.get("minicloud_lightweight") is False:
        raise MinicloudError("minicloud_lightweight_* = auto needs minicloud_lightweight: true")

    if running_cmd and (running := parse_lightweight_flags(running_cmd)):
        adopted = dict(zip(AUTO_KNOBS, (running.memory, running.vcpus, running.vcpus_loader)))
        for knob in derived:
            params[knob] = adopted[knob]
        LOGGER.info("minicloud sizes adopted from the running minicloud: %s", {knob: adopted[knob] for knob in derived})
        return None

    guests, _ = counted_guests(params)
    budget = host_memory_budget(config, guests)
    host_threads = host_thread_budget(config)
    sizes = _derive_sizes(params, config, derived, guests, budget, host_threads)
    for knob, value in zip(AUTO_KNOBS, sizes):
        if knob in derived:
            params[knob] = value
    record = _sizing_record(params, derived, guests, budget, host_threads, sizes)
    state_dir = Path(config.state_dir)
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / SIZING_FILE).write_text(json.dumps(record, indent=2))
    except OSError as exc:  # the record is for the report only
        LOGGER.warning("cannot write %s: %s", state_dir / SIZING_FILE, exc)

    LOGGER.info("minicloud sizes derived from the host:\n%s", json.dumps(record, indent=2))
    return record


def load_sizing_record(config: MinicloudConfig) -> dict | None:
    """Return the `sizing.json` record of the sizes ``config`` runs with, or None.

    A record written for other sizes describes another run: the flag values tie it to its sizes.
    """
    try:
        record = json.loads((Path(config.state_dir) / SIZING_FILE).read_text())
    except OSError, ValueError:
        return None
    if not isinstance(record, dict) or record.get("flags") != list(config.lightweight_flag_values()):
        return None
    return record


def submit_minicloud_sizing_to_argus(argus_client, record: dict) -> None:
    """Store the host figures and the derived guest sizes of a minicloud run, one row per host."""
    if not argus_client:
        LOGGER.warning("Will not submit to argus - no client initialized")
        return
    if not argus_client.get_run().get("test_id"):
        LOGGER.warning("No test exists for this run, skipping submitting results")
        return
    host, sizes = record["host"], record["sizes"]
    values = {
        "cpu_threads": host["cpu_threads"],
        "mem_total_gib": host["mem_total_gib"],
        "budget_gib": host["budget_gib"],
        "peak_guests": record["guests"]["total"],
        "db_memory_gib": parse_memory_gib(sizes["db_memory"], "minicloud_lightweight_memory"),
        "db_vcpus": sizes["db_vcpus"],
        "loader_vcpus": sizes["loader_vcpus"],
    }
    table = MinicloudHostSizingTable()
    for column, value in values.items():
        if value is not None:
            table.add_result(column=column, row=host["name"], value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, table)
