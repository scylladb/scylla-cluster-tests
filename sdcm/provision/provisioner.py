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
from datetime import datetime
from enum import Enum
from typing import List, Dict

from invoke import Result

from sdcm.keystore import SSHKey
from sdcm.provision.user_data import UserDataObject


class VmArch(Enum):
    X86 = "x86_64"
    ARM = "aarch64"


@dataclass
class DataDisk:
    type: str
    size: int
    iops: int


@dataclass
class InstanceDefinition:
    name: str
    image_id: str
    type: str   # instance_type from yaml
    user_name: str
    ssh_key: SSHKey = field(repr=False)
    tags: Dict[str, str] = field(default_factory=dict)
    arch: VmArch = VmArch.X86
    root_disk_size: int | None = None
    data_disks: List[DataDisk] | None = None
    user_data: List[UserDataObject] | None = field(
        default_factory=list, repr=False)  # None when no cloud-init use at all


class ProvisionError(Exception):
    pass


class ProvisionerError(Exception):
    pass


class PricingModel(Enum):
    ON_DEMAND = 'on_demand'
    SPOT = 'spot'
    SPOT_FLEET = 'spot_fleet'
    SPOT_LOW_PRICE = 'spot_low_price'

    def is_spot(self) -> bool:
        return self is not PricingModel.ON_DEMAND


@dataclass
class VmInstance:
    name: str
    region: str
    user_name: str
    ssh_key_name: str
    public_ip_address: str
    private_ip_address: str
    tags: Dict[str, str]
    pricing_model: PricingModel
    image: str
    creation_time: datetime | None
    _provisioner: "Provisioner"

    def terminate(self, wait: bool = True) -> None:
        """terminates VM instance.
        If wait is set to True, waits until deletion, otherwise, returns when termination
        was triggered."""
        self._provisioner.terminate_instance(self.name, wait=wait)

    def reboot(self, wait: bool = True, hard: bool = False) -> None:
        """Reboots the instance.
        If wait is set to True, waits until machine is up, otherwise, returns when reboot was triggered."""
        self._provisioner.reboot_instance(self.name, wait, hard)

    def add_tags(self, tags: Dict[str, str]) -> None:
        """Adds tags to the instance."""
        self._provisioner.add_instance_tags(self.name, tags)
        self.tags.update(tags)

    @property
    def availability_zone(self) -> str:
        return self._provisioner.availability_zone

    def run_command(self, command: str) -> Result:
        """Runs command on instance."""
        return self._provisioner.run_command(self.name, command)


class Provisioner(ABC):
    """Abstract class for instance (virtual machines) provisioner, cloud-provider and sct agnostic.
    Limits only to machines related to provided test_id. """

    def __init__(self, test_id: str, region: str, availability_zone: str) -> None:
        self._test_id = test_id
        self._region = region
        self._az = availability_zone

    @property
    def test_id(self) -> str:
        return self._test_id

    @property
    def region(self) -> str:
        return self._region

    @property
    def availability_zone(self) -> str:
        return self._az

    @classmethod
    def discover_regions(cls, test_id: str) -> List["Provisioner"]:
        """Returns provisioner class instance for each region where resources exist."""
        raise NotImplementedError()

    def get_or_create_instance(self,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT
                               ) -> VmInstance:
        """Create an instance specified by an InstanceDefinition.
        If instance already exists, returns it."""
        raise NotImplementedError()

    def get_or_create_instances(self,
                                definitions: List[InstanceDefinition],
                                pricing_model: PricingModel = PricingModel.SPOT
                                ) -> List[VmInstance]:
        """Create a set of instances specified by a list of InstanceDefinition.
        If instances already exist, returns them."""
        raise NotImplementedError()

    def terminate_instance(self, name: str, wait: bool = False) -> None:
        """Terminate instance by name"""
        raise NotImplementedError()

    def reboot_instance(self, name: str, wait: bool, hard: bool = False) -> None:
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

    def run_command(self, name: str, command: str) -> Result:
        """Runs command on instance."""
        raise NotImplementedError()


class ProvisionerFactory:

    def __init__(self) -> None:
        self._classes = {}

    def register_provisioner(self, backend: str, provisioner_class: Provisioner) -> None:
        self._classes[backend] = provisioner_class

    def create_provisioner(self, backend: str, test_id: str, region: str, availability_zone: str, **config) -> Provisioner:
        """Creates provisioner for given backend, test_id and region and returns it."""
        provisioner = self._classes.get(backend)
        if not provisioner:
            raise ProvisionerError(f"No provisioner found for backend '{backend}'. "
                                   f"Register it with provisioner_factory.register_provisioner method.")
        return provisioner(test_id, region, availability_zone, **config)

    def discover_provisioners(self, backend: str, test_id: str, **config) -> List[Provisioner]:
        """Discovers regions where resources for given test_id are created.
        Returning provisioner class instance for each region."""
        provisioner = self._classes.get(backend)
        if not provisioner:
            raise ProvisionerError("Provisioner class was not registered for the '%s' backend" % backend)
        return provisioner.discover_regions(test_id, **config)


provisioner_factory = ProvisionerFactory()
