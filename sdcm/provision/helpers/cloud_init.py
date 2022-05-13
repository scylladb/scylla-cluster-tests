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
import logging

from sdcm.provision.provisioner import VmInstance
from sdcm.provision.user_data import CLOUD_INIT_SCRIPTS_PATH
from sdcm.remote import RemoteCmdRunnerBase

LOGGER = logging.getLogger(__name__)


class CloudInitError(Exception):
    pass


def wait_cloud_init_completes(remoter: RemoteCmdRunnerBase, instance: VmInstance):
    """Connects to VM with SSH and waits for cloud-init to complete. Verify if everything went ok.
    """
    LOGGER.info("Waiting for cloud-init to complete on node %s...", instance.name)
    errors_found = False
    remoter.is_up(60 * 5)
    result = remoter.sudo("cloud-init status --wait")
    status = result.stdout
    LOGGER.debug("cloud-init status: %s", status)
    if "done" not in status:
        LOGGER.error("Some error during cloud-init %s", status)
        errors_found = True
    scripts_errors_found = log_user_data_scripts_errors(remoter=remoter)
    if errors_found or scripts_errors_found:
        raise CloudInitError("Errors during cloud-init provisioning phase. See logs for errors.")


def log_user_data_scripts_errors(remoter: RemoteCmdRunnerBase) -> bool:
    errors_found = False
    result = remoter.run(f"ls {CLOUD_INIT_SCRIPTS_PATH}")
    ls_errors = result.stderr
    if ls_errors:
        LOGGER.error("Error listing generated scripts: %s", ls_errors)
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
