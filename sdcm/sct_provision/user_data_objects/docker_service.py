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

from sdcm.provision.common.utils import install_docker_service
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class DockerUserDataObject(SctUserDataObject):

    @property
    def is_applicable(self) -> bool:
        return self.node_type == "loader"

    @property
    def script_to_run(self) -> str:
        script = install_docker_service()
        return script
