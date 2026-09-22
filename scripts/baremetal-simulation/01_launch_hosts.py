#!/usr/bin/env python3
"""Step 1 -- provision the "physical" hosts on AWS.

    uv run python scripts/baremetal-simulation/01_launch_hosts.py [--dry-run]

Plain EC2 instances launched by hand, outside SCT.  They go into the existing SCT
VPC/subnet with the SCT key pair, so SCT's own SSH credentials work unchanged, and
they carry keep/keep_action tags so the cloud-cleanup robot neither reaps them
mid-run nor leaks them if the run is abandoned.

Two security groups are attached:
  * SCT-2-sg          -- allows everything *between* the nodes (see sdcm/utils/aws_region.py)
  * <tag>-sg          -- created here, allows this workstation's public IP in.
                         SCT-2-sg alone does not let an outside runner SSH in.

SELinux is set to permissive from user-data: scylla_setup's SELinux step wants a
reboot, and PhysicalMachineNode.reboot() raises NotImplementedError.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    ROLES,
    TAG_ROLE,
    TAG_TEST,
    cfg,
    cfg_int,
    die,
    ec2_client,
    log,
    lookup_security_group_id,
    lookup_subnet_id,
    lookup_vpc_id,
    resolve_fedora_ami,
    simulation_instances,
    write_state,
)

# Ports the test runner needs to reach on the hosts when it runs outside the VPC.
# Node-level metrics (9100/9180) are scraped over SSH by SCT itself, these are for
# manual poking and for the monitor node's Grafana/Prometheus.
INGRESS_PORTS = ((22, 22), (3000, 3000), (9042, 9042), (9090, 9100), (9180, 9180), (10000, 10000))

USER_DATA = """#!/bin/bash
# Prepare the host the way a hand-prepared physical machine would be prepared.
setenforce 0 || true
sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config || true
"""


def my_public_ip() -> str:
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=15) as response:
        return response.read().decode().strip()


def ensure_workstation_security_group(client, vpc_id: str) -> str:
    name = f"{cfg('SIM_TEST_TAG')}-sg"
    if existing := lookup_security_group_id(client, name, vpc_id):
        log(f"reusing security group {name} = {existing}")
        return existing

    public_ip = my_public_ip()
    log(f"creating security group {name} for {public_ip}/32")
    sg_id = client.create_security_group(
        GroupName=name,
        Description=f"SCT-901 bare-metal simulation ({cfg('SIM_TEST_TAG')})",
        VpcId=vpc_id,
        TagSpecifications=[
            {"ResourceType": "security-group", "Tags": [{"Key": TAG_TEST, "Value": cfg("SIM_TEST_TAG")}]}
        ],
    )["GroupId"]
    client.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": from_port,
                "ToPort": to_port,
                "IpRanges": [{"CidrIp": f"{public_ip}/32", "Description": "SCT-901 simulation runner"}],
            }
            for from_port, to_port in INGRESS_PORTS
        ],
    )
    return sg_id


def instance_type_for(role: str) -> str:
    return {
        "db": cfg("SIM_INSTANCE_TYPE"),
        "loader": cfg("SIM_LOADER_INSTANCE_TYPE"),
        "monitor": cfg("SIM_MONITOR_INSTANCE_TYPE"),
    }[role]


def launch(client, role: str, count: int, ami_id: str, subnet_id: str, security_groups: list[str]) -> list[str]:
    log(f"launching {count} x {instance_type_for(role)} as {role}")
    response = client.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type_for(role),
        MinCount=count,
        MaxCount=count,
        KeyName=cfg("SIM_KEYPAIR"),
        SubnetId=subnet_id,
        SecurityGroupIds=security_groups,
        UserData=USER_DATA,
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": cfg_int("SIM_ROOT_DISK_GB"),
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": f"{cfg('SIM_TEST_TAG')}-{role}"},
                    {"Key": TAG_TEST, "Value": cfg("SIM_TEST_TAG")},
                    {"Key": TAG_ROLE, "Value": role},
                    {"Key": "RunByUser", "Value": cfg("SIM_RUN_BY_USER", "")},
                    # keep/<hours> + keep_action are honoured by utils/cloud_cleanup/aws/clean_aws.py
                    {"Key": "keep", "Value": cfg("SIM_KEEP_HOURS")},
                    {"Key": "keep_action", "Value": "terminate"},
                ],
            }
        ],
    )
    return [instance["InstanceId"] for instance in response["Instances"]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="resolve everything, launch nothing")
    args = parser.parse_args()

    client = ec2_client()

    if existing := simulation_instances(client):
        log(f"{len(existing)} instance(s) already tagged {TAG_TEST}={cfg('SIM_TEST_TAG')}:")
        for instance in existing:
            log(f"  {instance['role']:<7} {instance['id']} {instance['state']} {instance['public_ip']}")
        die("refusing to launch on top of an existing simulation -- run 99_teardown.py first")

    vpc_id = lookup_vpc_id(client)
    subnet_id = lookup_subnet_id(client, vpc_id)
    sct_sg = lookup_security_group_id(client, cfg("SIM_SCT_SG_NAME"), vpc_id)
    if not sct_sg:
        die(f"{cfg('SIM_SCT_SG_NAME')} not found -- run: hydra prepare-regions -c aws -r {cfg('SIM_REGION')}")
    ami = resolve_fedora_ami(client)
    if not ami:
        die(f"no Fedora image matched {cfg('SIM_AMI_NAME_PATTERN')} in {cfg('SIM_REGION')}")

    counts = {role: cfg_int(f"SIM_{role.upper()}_COUNT") for role in ROLES}
    log(f"region={cfg('SIM_REGION')} az={cfg('SIM_AZ')} vpc={vpc_id} subnet={subnet_id}")
    log(f"ami={ami['id']} ({ami['name']}) ssh_user={cfg('SIM_SSH_USER')}")
    log(f"counts={counts}")

    if args.dry_run:
        log("--dry-run: nothing launched")
        return 0

    workstation_sg = ensure_workstation_security_group(client, vpc_id)
    security_groups = [sct_sg, workstation_sg]

    launched: dict[str, list[str]] = {}
    try:
        for role, count in counts.items():
            if count:
                launched[role] = launch(client, role, count, ami["id"], subnet_id, security_groups)
    except ClientError as exc:
        die(f"launch failed ({exc}); instances already created are tagged, clean up with 99_teardown.py")

    all_ids = [instance_id for ids in launched.values() for instance_id in ids]
    log(f"waiting for {len(all_ids)} instance(s) to reach 'running'")
    client.get_waiter("instance_running").wait(InstanceIds=all_ids)

    state = write_state(
        "hosts.json",
        {
            "test_tag": cfg("SIM_TEST_TAG"),
            "region": cfg("SIM_REGION"),
            "ami": ami,
            "vpc_id": vpc_id,
            "subnet_id": subnet_id,
            "security_groups": security_groups,
            "workstation_security_group": workstation_sg,
            "instances": launched,
        },
    )
    log(f"wrote {state}")

    for instance in simulation_instances(client):
        log(
            f"  {instance['role']:<7} {instance['id']} {instance['instance_type']} {instance['public_ip']} / {instance['private_ip']}"
        )

    log("hosts are booting; cloud-init needs ~30s more -- next: 02_write_baremetal_config.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
