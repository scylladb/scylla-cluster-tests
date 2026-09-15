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

from sdcm.provision.common.utils import disable_firewall
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class DisableFirewallUserDataObject(SctUserDataObject):
    """Disable the firewall of the OCI images at first boot, before anything needs the node.

    Those images block everything but SSH and restore that ruleset on every boot, which leaves
    a rebooted node locally healthy and invisible to the rest of the cluster (SCT-479). Doing it
    from cloud-init is the earliest point available and covers every node of the run at once,
    whatever restarts or reboots it later.
    """

    @property
    def is_applicable(self) -> bool:
        # only OCI ships such a ruleset; the other backends have nothing to disable
        return self.params.get("cluster_backend") == "oci"

    @property
    def script_to_run(self) -> str:
        return disable_firewall()
