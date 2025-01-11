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
from sdcm.provision.common.provisioner import ProvisionParameters, InstanceProvisionerBase

LOGGER = logging.getLogger(__name__)


class ProvisionType(str, Enum):
    ON_DEMAND = 'on_demand'
    SPOT_LOW_PRICE = 'spot_low_price'
    SPOT = 'spot'


class ProvisionPlanBuilder(BaseModel):
    initial_provision_type: ProvisionType
    duration: int = None
    fallback_provision_on_demand: bool
    region_name: str
    availability_zone: str
    spot_low_price: float | None = None
    provisioner: InstanceProvisionerBase

    @property
    def _provision_request_on_demand(self) -> Optional[ProvisionParameters]:
        return ProvisionParameters(
            name='On Demand',
            spot=False,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
        )

    @property
    def _provision_request_spot(self) -> ProvisionParameters:
        return ProvisionParameters(
            name='Spot',
            spot=True,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
        )

    @property
    def _provision_request_spot_low_price(self) -> ProvisionParameters:
        return ProvisionParameters(
            name='Spot(Low Price)',
            spot=True,
            region_name=self.region_name,
            availability_zone=self.availability_zone,
            price=self.spot_low_price
        )

    @property
    def _provision_steps(self) -> List[ProvisionParameters]:
        if self.initial_provision_type == ProvisionType.ON_DEMAND:
            return [self._provision_request_on_demand]
        provision_plan = []
        if self.initial_provision_type == ProvisionType.SPOT:
            provision_plan.extend([
                self._provision_request_spot,
            ])
        elif self.initial_provision_type == ProvisionType.SPOT_LOW_PRICE:
            provision_plan.extend([
                self._provision_request_spot_low_price,
                self._provision_request_spot,
            ])
        if self.fallback_provision_on_demand:
            provision_plan.append(self._provision_request_on_demand)
        return provision_plan

    @property
    def provision_plan(self):
        return ProvisionPlan(provision_steps=self._provision_steps, provisioner=AWSInstanceProvisioner())
