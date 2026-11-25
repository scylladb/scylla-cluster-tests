"""Test a perftune.py change on DB nodes before it ships in a Scylla release.

``perftune_script_url`` replaces the packaged perftune.py with one downloaded from a URL, e.g. the raw file of a
seastar pull request branch. It runs after Scylla setup and before Scylla starts.

perftune decides the IRQ CPUs only once, at setup: ``scylla_sysconfig_setup`` asks it for the compute CPU mask and
writes Scylla's cpuset from it, and ``scylla_prepare`` later derives perftune.yaml's ``irq_cpu_mask`` from that cpuset.
A new perftune.py that changes the IRQ CPU choice only takes effect when both are redone, so this module redoes them
with the new script, the same way ``scylla_sysconfig_setup`` does.
"""

import logging
import re

LOGGER = logging.getLogger(__name__)

# The packaged /opt/scylladb/scripts/perftune.py is a wrapper that runs this file with Scylla's own python.
PERFTUNE_SCRIPT_PATH = "/opt/scylladb/scripts/libexec/perftune.py"
PERFTUNE_YAML_PATH = "/etc/scylla.d/perftune.yaml"


def parse_cpu_mask(output: str) -> str:
    """Return the CPU mask from ``perftune.py --get-cpu-mask`` output, which may start with warning lines.

    Args:
        output: the command's stdout, e.g. ``"WARNING: ...\\n0x0000fffe\\n"``.

    Returns:
        The mask, e.g. ``0x0000fffe`` or ``0x00000003,,0x00000003``.
    """
    masks = re.findall(r"^(0x[0-9a-f]+(?:,(?:0x[0-9a-f]+)?)*)\s*$", output, re.MULTILINE)
    if not masks:
        raise ValueError(f"No CPU mask in perftune output: {output!r}")
    return masks[-1]


def cpu_mask_to_cpuset(mask: str) -> str:
    """Convert a perftune/hwloc CPU mask to a Scylla ``--cpuset`` list.

    Args:
        mask: comma-separated 32-bit hex words, most significant first, e.g. ``0x0000fffe`` or
            ``0x00000003,0x00000003``. Empty words mean zero, as hwloc prints them.

    Returns:
        CPU ranges, e.g. ``1-15`` or ``0-1,32-33``.
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


def install_perftune_script(node, script_url: str) -> None:
    """Install a perftune.py on a DB node whose Scylla is set up but not started, and redo its CPU layout.

    Args:
        node: the DB node.
        script_url: URL of the perftune.py to install over the packaged one.
    """
    node.log.info("Installing perftune.py from %s", script_url)
    node.remoter.sudo(f"curl -fsSL --retry 5 --retry-delay 5 -o {PERFTUNE_SCRIPT_PATH} '{script_url}'")
    node.remoter.sudo(f"python3 -m py_compile {PERFTUNE_SCRIPT_PATH}")

    nic = node.get_nic_devices()[0]
    output = node.remoter.sudo(f"/opt/scylladb/scripts/perftune.py --tune net --nic {nic} --get-cpu-mask").stdout
    cpuset = cpu_mask_to_cpuset(parse_cpu_mask(output))
    node.log.info("perftune.py from %s gives Scylla the CPUs %s", script_url, cpuset)
    node.remoter.sudo(f"/opt/scylladb/scripts/scylla_cpuset_setup --cpuset {cpuset}")
    # scylla_prepare writes it again from the new cpuset when Scylla starts
    node.remoter.sudo(f"rm -f {PERFTUNE_YAML_PATH}")
