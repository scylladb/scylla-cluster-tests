"""Test a perftune.py change on DB nodes before it ships in a Scylla release.

Two opt-in knobs, applied after Scylla setup and before Scylla starts (scylla_prepare runs perftune on every start):

- ``perftune_script_url``: replace the packaged perftune.py with one downloaded from a URL, e.g. the raw file of a
  seastar pull request branch.
- ``perftune_irq_on_all_cpus``: put NIC IRQs on every CPU (one RX queue per CPU, as far as the NIC has queues) and run
  Scylla on every CPU, instead of reserving IRQ cores that Scylla doesn't use.
"""

import logging

from sdcm.remote.remote_file import dict_to_yaml_file, remote_file, yaml_file_to_dict

LOGGER = logging.getLogger(__name__)

# The packaged /opt/scylladb/scripts/perftune.py is a wrapper that runs this file with Scylla's own python.
PERFTUNE_SCRIPT_PATH = "/opt/scylladb/scripts/libexec/perftune.py"
PERFTUNE_YAML_PATH = "/etc/scylla.d/perftune.yaml"
CPUSET_CONF_PATH = "/etc/scylla.d/cpuset.conf"


def irq_on_all_cpus_options(perftune_options: dict) -> dict:
    """Return perftune.yaml options that bind NIC IRQs to every CPU perftune tunes.

    perftune treats ``irq_cpu_mask == cpu_mask`` as "IRQs and Scylla share all CPUs": it keeps one RX channel per IRQ
    CPU and spreads the IRQs over all of them. A ``mode`` key would override the mask, so it is dropped.

    Args:
        perftune_options: the content of perftune.yaml, as scylla_setup wrote it.

    Returns:
        A new options dict with ``irq_cpu_mask`` set to ``cpu_mask``.
    """
    if "cpu_mask" not in perftune_options:
        raise ValueError(f"{PERFTUNE_YAML_PATH} has no cpu_mask: {perftune_options}")
    options = {key: value for key, value in perftune_options.items() if key != "mode"}
    options["irq_cpu_mask"] = options["cpu_mask"]
    return options


def cpu_mask_to_cpuset(mask: str) -> str:
    """Convert a perftune/hwloc CPU mask to a Scylla ``--cpuset`` list.

    Args:
        mask: comma-separated 32-bit hex words, most significant first, e.g. ``0x0000ffff`` or
            ``0x00000003,0x00000003``. Empty words mean zero, as hwloc prints them.

    Returns:
        CPU ranges, e.g. ``0-15`` or ``0-1,32-33``.
    """
    value = 0
    for word in mask.split(","):
        value = (value << 32) | int(word.strip().removeprefix("0x") or "0", 16)
    cpus = [cpu for cpu in range(value.bit_length()) if value >> cpu & 1]
    if not cpus:
        raise ValueError(f"CPU mask {mask!r} has no CPUs")

    ranges = []
    start = prev = cpus[0]
    for cpu in cpus[1:] + [None]:
        if cpu is not None and cpu == prev + 1:
            prev = cpu
            continue
        ranges.append(str(start) if start == prev else f"{start}-{prev}")
        if cpu is not None:
            start = prev = cpu
    return ",".join(ranges)


def apply_perftune_overrides(node, script_url: str | None, irq_on_all_cpus: bool) -> None:
    """Apply the perftune test knobs on a DB node whose Scylla is set up but not started yet.

    Args:
        node: the DB node.
        script_url: URL of a perftune.py to install over the packaged one; empty to keep the packaged one.
        irq_on_all_cpus: put NIC IRQs and Scylla on every CPU.
    """
    if script_url:
        node.log.info("Installing perftune.py from %s", script_url)
        node.remoter.sudo(f"curl -fsSL --retry 5 --retry-delay 5 -o {PERFTUNE_SCRIPT_PATH} '{script_url}'")
        node.remoter.sudo(f"python3 -m py_compile {PERFTUNE_SCRIPT_PATH}")

    if irq_on_all_cpus:
        with remote_file(
            remoter=node.remoter,
            remote_path=PERFTUNE_YAML_PATH,
            serializer=dict_to_yaml_file,
            deserializer=yaml_file_to_dict,
            sudo=True,
        ) as perftune_options:
            new_options = irq_on_all_cpus_options(perftune_options)
            perftune_options.clear()
            perftune_options.update(new_options)
        cpuset = cpu_mask_to_cpuset(new_options["cpu_mask"])
        node.log.info("perftune: NIC IRQs and Scylla on all CPUs (%s)", cpuset)
        with remote_file(remoter=node.remoter, remote_path=CPUSET_CONF_PATH, sudo=True) as cpuset_conf:
            cpuset_conf.seek(0)
            cpuset_conf.truncate()
            cpuset_conf.write(f'# Written by SCT (perftune_irq_on_all_cpus)\nCPUSET="--cpuset {cpuset} "\n')
