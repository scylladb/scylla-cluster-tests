#!/usr/bin/env python3
"""Shared helpers for the SCT-901 bare-metal simulation scripts.

The simulation provisions plain EC2 instances by hand and then hands them to SCT
through the ``baremetal`` backend, exactly the way a Spider machine would be
handed over.  SCT never provisions, tags, reboots or destroys these hosts -- the
scripts in this directory own their whole lifecycle.

See docs/plans/sct901-baremetal-aws-simulation-runbook.md for the full runbook.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import boto3

SIM_DIR = Path(__file__).resolve().parent
REPO_ROOT = SIM_DIR.parents[1]
CONFIG_ENV = SIM_DIR / "config.env"

#: Generated, host-specific state: instance ids, resolved AMI, the SCT test id.
#: Never committed -- see .gitignore in this directory.
STATE_DIR = SIM_DIR / ".state"

#: Tag keys used to find the simulation's own instances again.
TAG_TEST = "TestId"
TAG_ROLE = "SimRole"

ROLES = ("db", "loader", "monitor")


def log(message: str) -> None:
    print(f"[sim] {message}", flush=True)


def die(message: str) -> None:
    print(f"[sim] ERROR: {message}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def load_config() -> dict[str, str]:
    """Parse config.env, letting an already-exported environment variable win."""
    config: dict[str, str] = {}
    for raw_line in CONFIG_ENV.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        config[key] = os.environ.get(key, value.strip().strip("'\""))
    # Anything SIM_* exported but absent from config.env is still usable.
    config.update({k: v for k, v in os.environ.items() if k.startswith("SIM_") and k not in config})
    return config


CONFIG = load_config()


def cfg(name: str, default: str | None = None) -> str:
    value = CONFIG.get(name, default)
    if value is None:
        die(f"{name} is not set in {CONFIG_ENV} nor in the environment")
    return value


def cfg_int(name: str, default: int | None = None) -> int:
    raw = cfg(name, str(default) if default is not None else None)
    try:
        return int(raw)
    except ValueError:
        die(f"{name} must be an integer, got {raw!r}")
        raise  # unreachable, keeps type checkers quiet


def ec2_client():
    return boto3.client("ec2", region_name=cfg("SIM_REGION"))


def ec2_resource():
    return boto3.resource("ec2", region_name=cfg("SIM_REGION"))


def tag_value(tags: list[dict] | None, key: str) -> str:
    for tag in tags or []:
        if tag["Key"] == key:
            return tag["Value"]
    return ""


def lookup_vpc_id(client) -> str:
    vpcs = client.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [cfg("SIM_VPC_NAME")]}])["Vpcs"]
    if not vpcs:
        die(
            f"VPC {cfg('SIM_VPC_NAME')} not found in {cfg('SIM_REGION')}. "
            f"Run: hydra prepare-regions -c aws -r {cfg('SIM_REGION')}"
        )
    return vpcs[0]["VpcId"]


def lookup_subnet_id(client, vpc_id: str) -> str:
    subnets = client.describe_subnets(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "availability-zone", "Values": [cfg("SIM_AZ")]},
        ]
    )["Subnets"]
    if not subnets:
        die(f"no SCT subnet in {cfg('SIM_AZ')} of {vpc_id}")
    # Prefer a subnet that hands out a public IP -- ip_ssh_connections is 'public'.
    subnets.sort(key=lambda subnet: not subnet.get("MapPublicIpOnLaunch"))
    return subnets[0]["SubnetId"]


def lookup_security_group_id(client, name: str, vpc_id: str | None = None) -> str | None:
    filters = [{"Name": "group-name", "Values": [name]}]
    if vpc_id:
        filters.append({"Name": "vpc-id", "Values": [vpc_id]})
    groups = client.describe_security_groups(Filters=filters)["SecurityGroups"]
    if not groups:
        # SCT-2-sg is found by its Name *tag*, its group name may differ.
        groups = client.describe_security_groups(Filters=[{"Name": "tag:Name", "Values": [name]}])["SecurityGroups"]
    return groups[0]["GroupId"] if groups else None


def simulation_instances(client, states: tuple[str, ...] = ("pending", "running", "stopping", "stopped")) -> list[dict]:
    """Every instance this simulation owns, newest last, with role and addresses."""
    reservations = client.describe_instances(
        Filters=[
            {"Name": f"tag:{TAG_TEST}", "Values": [cfg("SIM_TEST_TAG")]},
            {"Name": "instance-state-name", "Values": list(states)},
        ]
    )["Reservations"]
    instances = [
        {
            "id": instance["InstanceId"],
            "role": tag_value(instance.get("Tags"), TAG_ROLE) or "db",
            "public_ip": instance.get("PublicIpAddress", ""),
            "private_ip": instance.get("PrivateIpAddress", ""),
            "state": instance["State"]["Name"],
            "launch_time": instance["LaunchTime"],
            "instance_type": instance["InstanceType"],
        }
        for reservation in reservations
        for instance in reservation["Instances"]
    ]
    instances.sort(key=lambda i: (ROLES.index(i["role"]) if i["role"] in ROLES else 9, i["launch_time"]))
    return instances


def resolve_fedora_ami(client) -> dict:
    """Newest stable Fedora Cloud AMI, or the pinned SIM_AMI_ID.

    EC2 name filters understand only ``*`` and ``?``, so the release number is
    filtered here rather than in the API call: names look like
    ``Fedora-Cloud-Base-AmazonEC2.x86_64-44-20260922.0``, and the pre-release
    channels (Rawhide, ELN, Prerelease, Beta) carry a word in that position.
    """
    if pinned := cfg("SIM_AMI_ID", ""):
        image = client.describe_images(ImageIds=[pinned])["Images"][0]
        return {"id": image["ImageId"], "name": image["Name"], "version": None}

    minimum = int(cfg("SIM_MIN_FEDORA_VERSION", "40"))
    candidates = []
    for image in client.describe_images(
        Owners=[cfg("SIM_AMI_OWNER")],
        Filters=[
            {"Name": "name", "Values": [cfg("SIM_AMI_NAME_PATTERN")]},
            {"Name": "architecture", "Values": ["x86_64"]},
        ],
    )["Images"]:
        # A stable name is "...x86_64-<release>-<YYYYMMDD>.<n>"; the pre-release
        # channels put a word where the date is (or where the release is).
        match = re.search(r"x86_64-(\d+)-\d{8}", image["Name"])
        if not match or int(match.group(1)) < minimum:
            continue
        if re.search(r"Rawhide|ELN|Prerelease|Beta", image["Name"], re.IGNORECASE):
            continue
        candidates.append((int(match.group(1)), image["CreationDate"], image))
    if not candidates:
        return {}
    version, _, newest = max(candidates)
    return {"id": newest["ImageId"], "name": newest["Name"], "version": version}


def baremetal_config_path() -> Path:
    """Where get_baremetal_config() looks: ./<name>.json relative to the repo root."""
    return REPO_ROOT / f"{cfg('SIM_BAREMETAL_CONFIG_NAME')}.json"


def ssh_key_path() -> Path:
    return Path(cfg("SIM_SSH_KEY")).expanduser()


def ssh(host: str, command: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command on one of the simulated hosts, the same way SCT would."""
    argv = [
        "ssh",
        "-i",
        str(ssh_key_path()),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "LogLevel=ERROR",
        f"{cfg('SIM_SSH_USER')}@{host}",
        command,
    ]
    log(f"ssh {host}: {command}")
    return subprocess.run(argv, check=check, capture_output=True, text=True)  # noqa: PLW1510


def state_path(name: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / name


def write_state(name: str, payload: dict) -> Path:
    path = state_path(name)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def read_state(name: str) -> dict:
    path = state_path(name)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def print_command(argv: list[str]) -> None:
    print("  " + " ".join(shlex.quote(part) for part in argv))
