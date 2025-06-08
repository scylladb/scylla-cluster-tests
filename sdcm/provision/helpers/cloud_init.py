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

from packaging.version import Version

from sdcm.provision.provisioner import VmInstance
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


class CloudInitError(Exception):
    pass


@retrying(n=20, sleep_time=10, allowed_exceptions=(CloudInitError, ),
          message="waiting for cloud-init to complete")
def wait_cloud_init_completes(remoter: RemoteCmdRunnerBase, instance: VmInstance):
    """Connects to VM with SSH and waits for cloud-init to complete. Verify if everything went ok.
    """
    LOGGER.info("Waiting for cloud-init to complete on node %s...", instance.name)
    errors_found = False
    remoter.is_up(60 * 5)
    # examples: 24.1.3-0ubuntu3.3, 19.3-46.amzn2.0.2
    cloud_init_version = Version(remoter.sudo("cloud-init --version 2>&1").stdout.split()[1].split('-')[0])
    # cloud-init supports json output from version 23.4, see:
    # https://cloudinit.readthedocs.io/en/latest/explanation/return_codes.html#id1
    if cloud_init_version >= Version("23.4"):
        result = remoter.sudo("cloud-init status --format=json --wait", ignore_status=True)
        status = json.loads(result.stdout)

        LOGGER.debug("cloud-init status: %s", status)
        if status['status'] != "done" or status['errors'] or result.return_code == 1:
            LOGGER.error("Some errors during cloud-init %s", status)
            errors_found = True
    else:
        result = remoter.sudo("cloud-init status --wait", ignore_status=True)
        status = result.stdout
        if "done" not in status or result.return_code == 1:
            LOGGER.error("Some errors during cloud-init %s", status)
            errors_found = True
    scripts_errors_found = log_user_data_scripts_errors(remoter=remoter)
    if errors_found or scripts_errors_found:
        raise CloudInitError("Errors during cloud-init provisioning phase. See logs for errors.")


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
