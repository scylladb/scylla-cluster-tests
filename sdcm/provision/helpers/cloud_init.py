# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2022 ScyllaDB
import json
import logging

from invoke.runners import Result
from packaging.version import Version

from sdcm.provision.provisioner import VmInstance
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)

# The tightest CI stages that contain this wait are 30 min (artifacts test stage, and longevity
# provisioning stage). 600s fails at roughly a third of the stage, leaving ample room for the
# error, log collection and node termination to happen inside SCT.
CLOUD_INIT_WAIT_TIMEOUT = 600
CLOUD_INIT_SSH_TIMEOUT_MARGIN = 60
CLOUD_INIT_OUTPUT_LOG = "/var/log/cloud-init-output.log"
CLOUD_INIT_LOG_TAIL_LINES = 50
TIMEOUT_COMMAND_EXIT_CODE = 124  # GNU coreutils `timeout` exit code when the limit is hit.


class CloudInitError(Exception):
    pass


@retrying(n=20, sleep_time=10, allowed_exceptions=(CloudInitError,), message="waiting for cloud-init to complete")
def wait_cloud_init_completes(remoter: RemoteCmdRunnerBase, instance: VmInstance):
    """Connects to VM with SSH and waits for cloud-init to complete. Verify if everything went ok."""
    LOGGER.info(
        "Waiting up to %s seconds for cloud-init to complete on node %s...", CLOUD_INIT_WAIT_TIMEOUT, instance.name
    )
    errors_found = False
    remoter.is_up(60 * 5)
    # Check if cloud-init is installed before trying to use it
    if not remoter.sudo("bash -c 'command -v cloud-init'", ignore_status=True).ok:
        LOGGER.info("cloud-init is not installed on node %s, skipping cloud-init check.", instance.name)
        return
    # examples: 24.1.3-0ubuntu3.3, 19.3-46.amzn2.0.2
    cloud_init_version = Version(remoter.run("cloud-init --version 2>&1").stdout.split()[1].split("-")[0])
    # cloud-init supports json output from version 23.4, see:
    # https://cloudinit.readthedocs.io/en/latest/explanation/return_codes.html#id1
    if cloud_init_version >= Version("23.4"):
        result = _wait_for_cloud_init_status(remoter, instance, "cloud-init status --format=json --wait")
        status = json.loads(result.stdout)

        LOGGER.debug("cloud-init status: %s", status)
        if status["status"] != "done" or status["errors"] or result.return_code == 1:
            LOGGER.error("Some errors during cloud-init %s", status)
            errors_found = True
    else:
        result = _wait_for_cloud_init_status(remoter, instance, "cloud-init status --wait")
        status = result.stdout
        if "done" not in status or result.return_code == 1:
            LOGGER.error("Some errors during cloud-init %s", status)
            errors_found = True
    scripts_errors_found = log_user_data_scripts_errors(remoter=remoter)
    if errors_found or scripts_errors_found:
        raise CloudInitError("Errors during cloud-init provisioning phase. See logs for errors.")


def _wait_for_cloud_init_status(remoter: RemoteCmdRunnerBase, instance: VmInstance, status_cmd: str) -> Result:
    """Run a cloud-init status/wait command bounded by an independent SCT-side timeout.

    `cloud-init status --wait` can block forever if a user-data script hangs (e.g. apt-get stuck
    against an unresponsive package mirror), and the SSH transport itself has no command timeout
    by default. Wrapping the remote command in coreutils `timeout` gives us an independent bound
    so a stuck node fails fast with a clear SCT error instead of hanging until an external CI stage
    timeout kills the whole job with zero SCT-side diagnostics.
    """
    result = remoter.sudo(
        f"timeout --kill-after=10s --signal=TERM {CLOUD_INIT_WAIT_TIMEOUT} {status_cmd}",
        ignore_status=True,
        timeout=CLOUD_INIT_WAIT_TIMEOUT + CLOUD_INIT_SSH_TIMEOUT_MARGIN,
        # retry=0: this call is already bounded by the SCT-side `timeout` wrapper above; the
        # default SSH-level retry-on-transient-network-error would multiply that bound and could
        # blow past the CI stage's time budget, so it is deliberately disabled here.
        retry=0,
    )
    if result.return_code == TIMEOUT_COMMAND_EXIT_CODE:
        # TimeoutError (builtin) rather than a CloudInitError subclass: wait_cloud_init_completes is
        # wrapped in @retrying(allowed_exceptions=(CloudInitError,), n=20), and retrying a bounded
        # timeout 20x would turn a 10-minute bound back into hours. Matches the existing in-package
        # convention (see sdcm/provision/aws/emr_provisioner.py, sdcm/provision/aws/dedicated_host.py).
        raise TimeoutError(
            f"cloud-init did not complete on node {instance.name} within "
            f"{CLOUD_INIT_WAIT_TIMEOUT} seconds. The node is most likely stuck inside a "
            f"user-data script (e.g. a hanging package-manager update against an unresponsive "
            f"mirror). Check {CLOUD_INIT_OUTPUT_LOG} on the node; its last {CLOUD_INIT_LOG_TAIL_LINES} "
            f"lines are:\n{_cloud_init_output_log_tail(remoter)}"
        )
    return result


def _cloud_init_output_log_tail(remoter: RemoteCmdRunnerBase) -> str:
    """Best-effort tail of cloud-init-output.log to embed in a timeout error message."""
    try:
        result = remoter.sudo(
            f"tail -n {CLOUD_INIT_LOG_TAIL_LINES} {CLOUD_INIT_OUTPUT_LOG}",
            ignore_status=True,
            timeout=60,
            retry=0,
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics helper must never mask the timeout error
        LOGGER.warning("Could not read %s for diagnostics: %s", CLOUD_INIT_OUTPUT_LOG, exc)
        return f"<could not read {CLOUD_INIT_OUTPUT_LOG}: {exc}>"
    return result.stdout.strip() or f"<{CLOUD_INIT_OUTPUT_LOG} is empty or missing>"


def log_user_data_scripts_errors(remoter: RemoteCmdRunnerBase) -> bool:
    errors_found = False
    result = remoter.run(f"ls {CLOUD_INIT_SCRIPTS_PATH}", ignore_status=True)
    if result.failed:
        LOGGER.error("Error listing generated scripts: return_code: %s, stderr: %s", result.return_code, result.stderr)
        errors_found = True
    files_list = result.stdout
    if not files_list:
        LOGGER.error("No user data scripts were generated.")
        errors_found = True
    elif ".failed" in files_list:
        LOGGER.error("Some user data scripts have failed: %s", files_list)
        errors_found = True
    elif "done" not in files_list:
        LOGGER.error("User data scripts were not executed at all.")
        errors_found = True
    return errors_found
