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
from dataclasses import dataclass
from textwrap import dedent

from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class ScyllaUserDataObject(SctUserDataObject):

    @property
    def is_applicable(self) -> bool:
        return self.node_type == "scylla-db"

    @property
    def scylla_machine_image_json(self) -> str:
        """Specifies configuration file accepted by scylla-machine-image service"""
        return dedent("""{
                     "scylla_yaml": {
                         "experimental": true
                     },
                     "start_scylla_on_first_boot": false
                }""")
