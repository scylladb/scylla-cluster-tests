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
# Copyright (c) 2021 ScyllaDB

import logging
from typing import List

from pydantic import BaseModel

from sdcm.provision.common.provisioner import ProvisionParameters, InstanceProvisionerBase, InstanceParamsBase, TagsType

LOGGER = logging.getLogger(__name__)


class ProvisionPlan(BaseModel):
    provision_steps: List[ProvisionParameters]
    provisioner: InstanceProvisionerBase

    @property
    def name(self):
        return self.__class__.__name__

    def provision_instances(
        self,
        instance_parameters: InstanceParamsBase | List[InstanceParamsBase],
        node_count: int,
        node_tags: List[TagsType],
        node_names: List[str],
    ):
        for provision_parameters in self.provision_steps:
            if instances := self.provisioner.provision(
                provision_parameters=provision_parameters,
                instance_parameters=instance_parameters,
                count=node_count,
                tags=node_tags,
                names=node_names,
            ):
                LOGGER.info(
                    '%s: Instances has been provisioned using "%s":\n%s',
                    self.name,
                    provision_parameters.name,
                    instances,
                )
                return instances
            else:
                LOGGER.error('%s: Failed to provision instances using "%s"', self.name, provision_parameters.name)
        return []
