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
# Copyright (c) 2025 ScyllaDB
from dataclasses import dataclass
from textwrap import dedent

from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class EnableWaLinuxAgent(SctUserDataObject):
    """
    Scylla machines on Azure have WaLinuxAgent disabled by default. This script enables it.
    https://github.com/scylladb/scylla-machine-image/pull/627
    """
    @property
    def is_applicable(self) -> bool:
        return self.node_type == "scylla-db" and self.params.get("cluster_backend") == "azure"

    @property
    def script_to_run(self) -> str:
        return dedent("""
            systemctl daemon-reload
            systemctl unmask walinuxagent
            systemctl enable walinuxagent
            systemctl start walinuxagent
            systemctl status walinuxagent --no-pager
        """)
