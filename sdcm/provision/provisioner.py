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


class VmArch(Enum):
    X86 = "x86_64"
    ARM = "aarch64"


@dataclass
class DataDisk:
    type: str
    size: int
    iops: int


@dataclass
class InstanceDefinition:  # pylint: disable=too-many-instance-attributes
    name: str
    image_id: str
    type: str   # instance_type from yaml
    user_name: Optional[str] = None
    ssh_public_key: Optional[str] = field(default=None, repr=False)
    tags: Dict[str, str] = field(default_factory=dict)
    arch: VmArch = VmArch.X86
    root_disk_size: Optional[int] = None
    data_disks: Optional[List[DataDisk]] = None


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
    region: str
    user_name: str
    public_ip_address: str
    private_ip_address: str
    tags: Dict[str, str]
    pricing_model: PricingModel
    image: str
    provisioner: "Provisioner"

    def terminate(self, wait=True):
        """terminates VM instance.
        If wait is set to True, waits until deletion, otherwise, returns when termination
        was triggered."""
        self.provisioner.terminate_virtual_machine(self.region, self.name, wait=wait)


class Provisioner(ABC):
    """Abstract class for virtual machines provisioner, cloud-provider and sct agnostic.
    Limits only to machines related to provided test_id. """
    _test_id: str
    _region: str

    @property
    def test_id(self):
        return self._test_id

    @property
    def region(self):
        return self._region

    def create_virtual_machine(self,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT
                               ) -> VmInstance:
        """Create virtual machine in provided region, specified by InstanceDefinition"""
        raise NotImplementedError()

    def terminate_virtual_machine(self, name: str, wait: bool = False) -> None:
        """Terminate virtual machine by name"""
        raise NotImplementedError()

    def list_virtual_machines(self) -> List[VmInstance]:
        """List virtual machines for given provisioner."""
        raise NotImplementedError()

    def cleanup(self, wait: bool = False) -> None:
        """Cleans up all the resources. If wait == True, waits till cleanup fully completes."""
        raise NotImplementedError()
