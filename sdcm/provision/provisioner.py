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
    _provisioner: "Provisioner"

    def terminate(self, wait=True):
        """terminates VM instance.
        If wait is set to True, waits until deletion, otherwise, returns when termination
        was triggered."""
        self._provisioner.terminate_instance(self.name, wait=wait)

    def reboot(self, wait=True):
        """Reboots the instance.
        If wait is set to True, waits until machine is up, otherwise, returns when reboot was triggered."""
        self._provisioner.reboot_instance(self.name, wait)

    def add_tags(self, tags: Dict[str, str]) -> None:
        """Adds tags to the instance."""
        self._provisioner.add_instance_tags(self.name, tags)
        self.tags.update(tags)


class Provisioner(ABC):
    """Abstract class for instance (virtual machines) provisioner, cloud-provider and sct agnostic.
    Limits only to machines related to provided test_id. """

    def __init__(self, test_id, region):
        self._test_id = test_id
        self._region = region

    @property
    def test_id(self) -> str:
        return self._test_id

    @property
    def region(self) -> str:
        return self._region

    @classmethod
    def discover_regions(cls, test_id) -> List["Provisioner"]:
        """Returns provisioner class instance for each region where resources exist."""
        raise NotImplementedError()

    def get_or_create_instance(self,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT
                               ) -> VmInstance:
        """Create instance in provided region, specified by InstanceDefinition.
        If instance already exists, returns it."""
        raise NotImplementedError()

    def terminate_instance(self, name: str, wait: bool = False) -> None:
        """Terminate instance by name"""
        raise NotImplementedError()

    def reboot_instance(self, name: str, wait: bool) -> None:
        """Reboot instance by name. """
        raise NotImplementedError()

    def list_instances(self) -> List[VmInstance]:
        """List instances for given provisioner."""
        raise NotImplementedError()

    def cleanup(self, wait: bool = False) -> None:
        """Cleans up all the resources. If wait == True, waits till cleanup fully completes."""
        raise NotImplementedError()

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Adds tags to instance."""
        raise NotImplementedError()


class ProvisionerFactory:

    def __init__(self) -> None:
        self._classes = {}

    def register_provisioner(self, backend: str, provisioner_class: Provisioner) -> None:
        self._classes[backend] = provisioner_class

    def create_provisioner(self, backend: str, test_id: str, region: str, **config) -> Provisioner:
        """Creates provisioner for given backend, test_id and region and returns it."""
        provisioner = self._classes.get(backend)
        if not provisioner:
            raise ValueError(backend)
        return provisioner(test_id, region, **config)

    def discover_provisioners(self, backend: str, test_id: str, **config) -> List[Provisioner]:
        """Discovers regions where resources for given test_id are created.
        Returning provisioner class instance for each region."""
        provisioner = self._classes.get(backend)
        return provisioner.discover_regions(test_id, **config)


provisioner_factory = ProvisionerFactory()
