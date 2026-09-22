#!/usr/bin/env python3
"""Step 0 -- check everything the simulation needs before spending money on it.

    uv run python scripts/baremetal-simulation/00_preflight.py

Checks AWS reachability, the SCT infrastructure the hosts are launched into, the
SSH key, hydra/docker, and whether the Fedora distro patch (step 3) is applied.
Exits non-zero if a hard prerequisite is missing; soft findings are warnings.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    REPO_ROOT,
    cfg,
    ec2_client,
    log,
    lookup_security_group_id,
    lookup_subnet_id,
    lookup_vpc_id,
    resolve_fedora_ami,
    ssh_key_path,
)

FAILURES: list[str] = []
WARNINGS: list[str] = []


def check(name: str, ok: bool, detail: str, hard: bool = True) -> None:
    mark = "OK  " if ok else ("FAIL" if hard else "WARN")
    print(f"[{mark}] {name}: {detail}")
    if not ok:
        (FAILURES if hard else WARNINGS).append(f"{name}: {detail}")


def check_aws() -> None:
    try:
        identity = boto3.client("sts", region_name=cfg("SIM_REGION")).get_caller_identity()
        check("aws credentials", True, f"account {identity['Account']} as {identity['Arn'].rsplit('/', 1)[-1]}")
    except (BotoCoreError, ClientError) as exc:
        check("aws credentials", False, str(exc))


def check_infrastructure() -> None:
    client = ec2_client()
    vpc_id = lookup_vpc_id(client)
    check("sct vpc", True, f"{cfg('SIM_VPC_NAME')} = {vpc_id}")

    subnet_id = lookup_subnet_id(client, vpc_id)
    check("sct subnet", True, f"{cfg('SIM_AZ')} = {subnet_id}")

    sg_id = lookup_security_group_id(client, cfg("SIM_SCT_SG_NAME"), vpc_id)
    check("sct security group", bool(sg_id), f"{cfg('SIM_SCT_SG_NAME')} = {sg_id}")

    try:
        client.describe_key_pairs(KeyNames=[cfg("SIM_KEYPAIR")])
        check("ec2 key pair", True, cfg("SIM_KEYPAIR"))
    except ClientError as exc:
        check("ec2 key pair", False, str(exc))


def check_ssh_key() -> None:
    key = ssh_key_path()
    if not key.exists():
        check("ssh private key", False, f"{key} is missing")
        return
    mode = key.stat().st_mode & 0o777
    check("ssh private key", mode in (0o400, 0o600), f"{key} (mode {mode:o})")


def check_ami() -> None:
    ami = resolve_fedora_ami(ec2_client())
    if not ami:
        check("fedora ami", False, f"nothing matched {cfg('SIM_AMI_NAME_PATTERN')} for owner {cfg('SIM_AMI_OWNER')}")
        return
    pinned = " (pinned)" if cfg("SIM_AMI_ID", "") else ""
    check("fedora ami", True, f"{ami['id']} {ami['name']}{pinned}")


def check_hydra() -> None:
    hydra = REPO_ROOT / "docker" / "env" / "hydra.sh"
    check("hydra script", hydra.exists(), str(hydra))
    docker = shutil.which("docker") or shutil.which("podman")
    check("container runtime", bool(docker), docker or "neither docker nor podman on PATH")
    if docker:
        probe = subprocess.run([docker, "info"], capture_output=True, text=True, check=False)
        check(
            "container runtime usable",
            probe.returncode == 0,
            "daemon reachable" if probe.returncode == 0 else probe.stderr.strip()[:120],
        )


def check_distro_patch() -> None:
    """Fedora > 36 resolves to Distro.UNKNOWN, which routes the install to apt."""
    distro_py = REPO_ROOT / "sdcm" / "utils" / "distro.py"
    line = next((ln for ln in distro_py.read_text(encoding="utf-8").splitlines() if '"FEDORA"' in ln), "")
    versions = re.findall(r'"(\d+)"', line)
    patched = any(int(v) >= 41 for v in versions)
    check(
        "fedora distro support",
        patched,
        f"sdcm/utils/distro.py knows Fedora {versions or '?'} -- run 03_patch_distro_for_fedora.py --apply",
        hard=False,
    )


def main() -> int:
    log(f"preflight for region {cfg('SIM_REGION')}, tag {cfg('SIM_TEST_TAG')}")
    check_aws()
    check_infrastructure()
    check_ssh_key()
    check_ami()
    check_hydra()
    check_distro_patch()

    print()
    if FAILURES:
        print(f"{len(FAILURES)} blocking problem(s):")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    if WARNINGS:
        print(f"{len(WARNINGS)} warning(s):")
        for warning in WARNINGS:
            print(f"  - {warning}")
    print("preflight passed -- next: 01_launch_hosts.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
