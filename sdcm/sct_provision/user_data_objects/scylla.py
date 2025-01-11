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
from dataclasses import dataclass

from sdcm.provision.scylla_yaml import ScyllaYaml
from sdcm.sct_provision.user_data_objects import SctUserDataObject


@dataclass
class ScyllaUserDataObject(SctUserDataObject):

    @property
    def is_applicable(self) -> bool:
        return self.node_type == "scylla-db"

    @property
    def scylla_machine_image_json(self) -> str:
        """Specifies configuration file accepted by scylla-machine-image service"""
        if (self.params.get("data_volume_disk_num") or 0) > 0:
            data_device = 'attached'
        else:
            data_device = 'instance_store'
        scylla_yaml = ScyllaYaml()
        scylla_yaml.cluster_name = f"{self.params.get('user_prefix')}-{self.test_config.test_id()[:8]}"
        smi_payload = {
            "start_scylla_on_first_boot": False,
            "data_device": data_device,
            "raid_level": self.params.get("raid_level") or 0,
            "scylla_yaml": scylla_yaml.model_dump(exclude_defaults=True, exclude_none=True, exclude_unset=True)
        }
        return json.dumps(smi_payload)
