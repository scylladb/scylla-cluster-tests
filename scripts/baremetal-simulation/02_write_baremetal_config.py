#!/usr/bin/env python3
"""Step 2 -- collect the addresses and write the bare-metal KeyStore JSON.

    uv run python scripts/baremetal-simulation/02_write_baremetal_config.py

sdcm/keystore.py:383 get_baremetal_config() reads ./<name>.json from the current
working directory *before* falling back to the scylla-qa-keystore S3 bucket, and
hydra mounts the repo at the container's CWD.  So the file simply goes in the repo
root and no keystore upload is needed.

All three sections must exist even when empty -- sdcm/tester.py:2363
get_cluster_baremetal() indexes db_nodes/loader_nodes/monitor_nodes
unconditionally.  An empty node_list is fine: n_nodes is derived from the list
length, and PhysicalMachineCluster only raises NodeIpsNotConfiguredError when
there are fewer IPs than nodes.

The script also waits for SSH and prints the host facts that decide whether the
run can work at all: the distro (Fedora > 36 is unknown to SCT), an unpartitioned
local NVMe disk (detect_disks) and the SELinux mode.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    ROLES,
    baremetal_config_path,
    cfg,
    die,
    ec2_client,
    log,
    simulation_instances,
    ssh,
)


def wait_for_ssh(host: str, timeout: int = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = ssh(host, "true", check=False)
        if result.returncode == 0:
            return
        time.sleep(10)
    die(f"no SSH on {host} after {timeout}s -- check the workstation security group and the public IP")


def describe_host(host: str) -> dict:
    """The facts that decide whether the bare-metal install path can work here."""
    probe = (
        "grep -E '^(ID|VERSION_ID)=' /etc/os-release | tr '\\n' ' '; echo; "
        "getenforce 2>/dev/null || echo unknown; "
        "lsblk -dnpo NAME,SIZE,TYPE | tr '\\n' '|'; echo; "
        "nproc; free -g | awk '/Mem:/{print $2}'"
    )
    result = ssh(host, probe, check=False)
    if result.returncode != 0:
        return {"error": result.stderr.strip()}
    lines = result.stdout.strip().splitlines()
    return {
        "os_release": lines[0].strip() if len(lines) > 0 else "",
        "selinux": lines[1].strip() if len(lines) > 1 else "",
        "block_devices": lines[2].strip("|") if len(lines) > 2 else "",
        "cpus": lines[3].strip() if len(lines) > 3 else "",
        "memory_gb": lines[4].strip() if len(lines) > 4 else "",
    }


def unpartitioned_nvme(host: str) -> list[str]:
    """Mirror of sdcm/cluster.py:3060 detect_disks(nvme=True)."""
    result = ssh(host, "ls /dev/nvme*n*", check=False)
    disks = re.findall(r"/dev/nvme\d+n\d+", result.stdout)
    return sorted({disk for disk in disks if disks.count(disk) == 1})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-probe", action="store_true", help="write the JSON without SSHing into the hosts")
    args = parser.parse_args()

    instances = simulation_instances(ec2_client(), states=("running",))
    if not instances:
        die(f"no running instance tagged TestId={cfg('SIM_TEST_TAG')} -- run 01_launch_hosts.py first")

    config: dict[str, dict] = {f"{role}_nodes": {"username": cfg("SIM_SSH_USER"), "node_list": []} for role in ROLES}
    for instance in instances:
        if not instance["public_ip"]:
            die(f"{instance['id']} has no public IP; ip_ssh_connections is 'public' for this run")
        config[f"{instance['role']}_nodes"]["node_list"].append(
            {"public_ip": instance["public_ip"], "private_ip": instance["private_ip"]}
        )

    path = baremetal_config_path()
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    log(f"wrote {path} (s3_baremetal_config: {cfg('SIM_BAREMETAL_CONFIG_NAME')})")
    print(json.dumps(config, indent=2))

    if args.skip_probe:
        return 0

    problems = []
    for instance in instances:
        host = instance["public_ip"]
        log(f"waiting for SSH on {instance['role']} {host}")
        wait_for_ssh(host)
        facts = describe_host(host)
        log(f"  {facts}")
        if instance["role"] == "db":
            disks = unpartitioned_nvme(host)
            log(f"  detect_disks(nvme=True) would return: {disks}")
            if not disks:
                problems.append(
                    f"{host}: no unpartitioned NVMe disk -- scylla_setup will fail, use an NVMe instance type"
                )
            if "fedora" in facts.get("os_release", "") and "Enforcing" in facts.get("selinux", ""):
                problems.append(
                    f"{host}: SELinux is Enforcing -- run 'sudo setenforce 0' (no reboot available on this backend)"
                )

    for problem in problems:
        log(f"PROBLEM: {problem}")
    if problems:
        return 1

    log("hosts are usable -- next: 03_patch_distro_for_fedora.py --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
