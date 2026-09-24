#!/usr/bin/env python3
"""Step 9 -- terminate everything this simulation created.

    uv run python scripts/baremetal-simulation/99_teardown.py [--yes] [--all] [--force]

    --yes    do not ask for confirmation
    --all    also remove the generated local files: <name>.json, the rendered test
             case, and .state/ (the distro.py patch is reverted separately, with
             03_patch_sct_for_fedora.py --revert)
    --force  terminate even while a run of this simulation is still executing

Terminates by the TestId tag, then deletes the workstation security group created
in step 1 (it can only go once nothing references it).  Safe to run repeatedly.

Refuses to run while one of this simulation's own hydra containers is still up.
The pytest summary is not the end of a run: log collection, and the teardown steps
of run_simulation.py itself, keep using the hosts for several minutes afterwards.
Terminating on the summary line loses the collected logs -- done twice by hand
before this guard existed.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
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


#: hydra names its container "<test id>_<epoch>".
HYDRA_CONTAINER = re.compile(r"^(?P<test_id>[0-9a-f-]{36})_\d+$")


def known_test_ids() -> set[str]:
    """Test ids this simulation has launched, from .state/runs.log."""
    runs_log = STATE_DIR / "runs.log"
    if not runs_log.exists():
        return set()
    ids = set()
    for line in runs_log.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 2:
            ids.add(parts[1])
    return ids


def runs_still_executing() -> list[str]:
    """Container names of this simulation's runs that are still going.

    Scoped to our own test ids on purpose: an unrelated SCT run on the same
    workstation is none of our business and must not block a teardown.
    """
    try:
        listed = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log(f"could not ask docker what is running ({exc}); skipping the in-flight check")
        return []
    if listed.returncode != 0:
        log("could not ask docker what is running; skipping the in-flight check")
        return []

    ours = known_test_ids()
    return [
        name
        for name in listed.stdout.split()
        if (match := HYDRA_CONTAINER.match(name)) and match.group("test_id") in ours
    ]


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
    log("remember: uv run python scripts/baremetal-simulation/03_patch_sct_for_fedora.py --revert")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    parser.add_argument("--all", action="store_true", help="also remove the generated local files")
    parser.add_argument(
        "--force", action="store_true", help="terminate even while a run of this simulation is still executing"
    )
    args = parser.parse_args()

    if in_flight := runs_still_executing():
        if args.force:
            log(f"--force: terminating although {len(in_flight)} run(s) are still executing")
        else:
            log("refusing to terminate: a run of this simulation is still executing")
            for name in in_flight:
                log(f"  {name}")
            log("a passing pytest summary is not the end of the run -- log collection comes after it.")
            log("wait for run_simulation.py to exit, or pass --force.")
            return 1

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
