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

from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional


class InstancePurpose(Enum):
    SCT = "sct"
    SCYLLA = "db"
    LOADER = "loader"
    MONITOR = "monitor"
    ORACLE = "oracle"


class VmArch(Enum):
    X86 = "x86_64"
    ARM = "aarch64"


@dataclass
class InstanceDefinition:  # pylint: disable=too-many-instance-attributes
    name: str
    purpose: InstancePurpose
    version: str  # can provide version or image url
    size: str   # instance_type_db from yaml
    admin_name: str
    admin_public_key: str = field(repr=False)
    tags: Dict[str, str]
    arch: VmArch = VmArch.X86
    root_disk_size: str = None


class PricingModel(Enum):
    ON_DEMAND = 'on_demand'
    SPOT = 'spot'
    SPOT_FLEET = 'spot_fleet'
    SPOT_LOW_PRICE = 'spot_low_price'  # maybe not needed
    SPOT_DURATION = 'spot_duration'  # maybe not needed

    def is_spot(self) -> bool:
        return self is not PricingModel.ON_DEMAND


@dataclass
class VmInstance:  # pylint: disable=too-many-instance-attributes
    name: str
    purpose: InstancePurpose
    region: str
    admin_name: str
    public_ip_address: str
    private_ip_address: str
    tags: Dict[str, str]
    pricing_model: PricingModel
    image: str


class Provisioner(ABC):
    """Abstract class for virtual machines provisioner, cloud-provider and sct agnostic.
    Limits only to machines related to provided test_id. """
    test_id: str

    def create_virtual_machine(self,
                               region: str,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT
                               ) -> VmInstance:
        """Create virtual machine in provided region, specified by InstanceDefinition"""
        raise NotImplementedError()

    def list_virtual_machines(self, region: Optional[str] = None, purpose: Optional[InstancePurpose] = None
                              ) -> List[VmInstance]:
        """List virtual machines for given region. Filter by region and/or purpose"""
        raise NotImplementedError()

    def cleanup(self, wait: bool = False) -> None:
        """Cleans up all the resources. If wait == True, waits till cleanup fully completes."""
        raise NotImplementedError()
