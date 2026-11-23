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
# Copyright (c) 2026 ScyllaDB
from dataclasses import dataclass

from sdcm.provision.common.utils import allow_oci_run_command_sudo, forward_boot_journal_to_console
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class BootJournalToConsoleUserDataObject(SctUserDataObject):
    """The sshd and cloud-init journal on the serial console, for a node whose SSH never comes up.

    AWS builds its user data from `ConfigurationScriptBuilder`, which runs the same script.
    """

    @property
    def is_applicable(self) -> bool:
        return True

    @property
    def script_to_run(self) -> str:
        return forward_boot_journal_to_console()


@dataclass
class OciRunCommandSudoUserDataObject(SctUserDataObject):
    """Let the OCI Run Command user sudo: the log collector's channel to a node whose SSH is down."""

    @property
    def is_applicable(self) -> bool:
        return self.params.get("cluster_backend") == "oci"

    @property
    def script_to_run(self) -> str:
        return allow_oci_run_command_sudo()
