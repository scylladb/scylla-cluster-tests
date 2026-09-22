#!/usr/bin/env python3
"""Step 9 -- terminate everything this simulation created.

    uv run python scripts/baremetal-simulation/99_teardown.py [--yes] [--all]

    --yes   do not ask for confirmation
    --all   also remove the generated local files: <name>.json, the rendered test
            case, and .state/ (the distro.py patch is reverted separately, with
            03_patch_distro_for_fedora.py --revert)

Terminates by the TestId tag, then deletes the workstation security group created
in step 1 (it can only go once nothing references it).  Safe to run repeatedly.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    REPO_ROOT,
    STATE_DIR,
    TAG_TEST,
    baremetal_config_path,
    cfg,
    ec2_client,
    log,
    lookup_security_group_id,
    lookup_vpc_id,
    simulation_instances,
)


def terminate_instances(client) -> list[str]:
    instances = simulation_instances(client)
    if not instances:
        log(f"no instance tagged {TAG_TEST}={cfg('SIM_TEST_TAG')}")
        return []
    ids = [instance["id"] for instance in instances]
    for instance in instances:
        log(f"terminating {instance['role']:<7} {instance['id']} {instance['public_ip']}")
    client.terminate_instances(InstanceIds=ids)
    log("waiting for termination")
    client.get_waiter("instance_terminated").wait(InstanceIds=ids)
    return ids


def delete_workstation_security_group(client) -> None:
    name = f"{cfg('SIM_TEST_TAG')}-sg"
    sg_id = lookup_security_group_id(client, name, lookup_vpc_id(client))
    if not sg_id:
        log(f"security group {name} already gone")
        return
    # ENIs linger for a few seconds after the instances are terminated.
    for attempt in range(6):
        try:
            client.delete_security_group(GroupId=sg_id)
            log(f"deleted security group {name} ({sg_id})")
            return
        except ClientError as exc:
            if "DependencyViolation" not in str(exc):
                log(f"could not delete {name}: {exc}")
                return
            time.sleep(10 * (attempt + 1))
    log(f"security group {name} ({sg_id}) still has dependencies -- delete it by hand")


def remove_local_files() -> None:
    for path in (baremetal_config_path(), REPO_ROOT / cfg("SIM_TEST_CASE")):
        if path.exists():
            path.unlink()
            log(f"removed {path}")
    if STATE_DIR.exists():
        shutil.rmtree(STATE_DIR)
        log(f"removed {STATE_DIR}")
    log("remember: uv run python scripts/baremetal-simulation/03_patch_distro_for_fedora.py --revert")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    parser.add_argument("--all", action="store_true", help="also remove the generated local files")
    args = parser.parse_args()

    client = ec2_client()
    instances = simulation_instances(client)
    if instances and not args.yes:
        log(f"about to terminate {len(instances)} instance(s) in {cfg('SIM_REGION')}:")
        for instance in instances:
            log(f"  {instance['role']:<7} {instance['id']} {instance['instance_type']} {instance['public_ip']}")
        if input("[sim] type 'yes' to continue: ").strip() != "yes":
            log("aborted")
            return 1

    terminate_instances(client)
    delete_workstation_security_group(client)
    if args.all:
        remove_local_files()
    log("teardown done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
