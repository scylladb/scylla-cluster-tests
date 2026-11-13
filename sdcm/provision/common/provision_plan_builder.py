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
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel

from sdcm.provision.aws.provisioner import AWSInstanceProvisioner
from sdcm.provision.common.provision_plan import ProvisionPlan
from sdcm.provision.common.provisioner import InstanceProvisionerBase, ProvisionParameters

LOGGER = logging.getLogger(__name__)


class ProvisionType(str, Enum):
    ON_DEMAND = "on_demand"
    SPOT = "spot"
    SPOT_FLEET = "spot_fleet"


class ProvisionPlanBuilder(BaseModel):
    initial_provision_type: ProvisionType
    duration: int = None
    fallback_provision_on_demand: bool
    region_name: str
    availability_zone: str
    provisioner: InstanceProvisionerBase

    @property
    def _provision_request_on_demand(self) -> Optional[ProvisionParameters]:
        return ProvisionParameters(
            name="On Demand",
            spot=False,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
        )

    @property
    def _provision_request_spot(self) -> ProvisionParameters:
        return ProvisionParameters(
            name="Spot",
            spot=True,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
        )

    @property
    def _provision_request_spot_fleet(self) -> ProvisionParameters:
        return ProvisionParameters(
            name="Spot Fleet",
            spot=True,
            fleet=True,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
        )

    @property
    def _provision_steps(self) -> List[ProvisionParameters]:
        if self.initial_provision_type == ProvisionType.ON_DEMAND:
            return [self._provision_request_on_demand]
        if self.initial_provision_type == ProvisionType.SPOT_FLEET:
            # Choosing spot_fleet means spot or nothing: never fall back to on-demand, whatever
            # `instance_provision_fallback_on_demand` says. AZ/region fallback still applies.
            if self.fallback_provision_on_demand:
                LOGGER.info("instance_provision is spot_fleet: ignoring instance_provision_fallback_on_demand")
            return [self._provision_request_spot_fleet]
        provision_plan = []
        if self.initial_provision_type == ProvisionType.SPOT:
            provision_plan.extend(
                [
                    self._provision_request_spot,
                ]
            )
        if self.fallback_provision_on_demand:
            provision_plan.append(self._provision_request_on_demand)
        return provision_plan

    @property
    def provision_plan(self):
        return ProvisionPlan(provision_steps=self._provision_steps, provisioner=AWSInstanceProvisioner())
