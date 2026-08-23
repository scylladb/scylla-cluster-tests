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
from sdcm.sct_events.system import SpotProvisionOutcomeEvent

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
        # The first step is what the test asked for; any later step is a downgrade (spot -> on-demand).
        requested = self.provision_steps[0].name if self.provision_steps else "unknown"
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
                self._publish_outcome(requested, provision_parameters, instance_parameters, node_count)
                return instances
            else:
                LOGGER.error('%s: Failed to provision instances using "%s"', self.name, provision_parameters.name)
        if self.provision_steps:
            self._publish_outcome(requested, None, instance_parameters, node_count)
        return []

    def _publish_outcome(
        self,
        requested: str,
        realized_parameters: ProvisionParameters | None,
        instance_parameters: InstanceParamsBase | List[InstanceParamsBase],
        node_count: int,
    ) -> None:
        """Record requested vs realized provision type, so silent spot->on-demand downgrades are visible."""
        location = realized_parameters or self.provision_steps[0]
        first_params = instance_parameters[0] if isinstance(instance_parameters, list) else instance_parameters
        SpotProvisionOutcomeEvent(
            requested=requested,
            realized=realized_parameters.name if realized_parameters else None,
            region=location.region_name,
            availability_zone=location.availability_zone,
            instance_type=getattr(first_params, "InstanceType", None),
            count=node_count,
        ).publish_or_dump(default_logger=LOGGER)
