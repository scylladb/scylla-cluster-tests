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

from typing import List, Dict

from sdcm.provision.provisioner import Provisioner, VmInstance, InstanceDefinition, PricingModel


class FakeProvisioner(Provisioner):
    """Fake provisioner for tests purposes. Imitates provisioner api by creating fake provisioners in memory."""
    _provisioners = {}

    def __new__(cls, test_id: str, region: str, **kwargs) -> Provisioner:  # pylint: disable=unused-argument
        if provisioner := cls._provisioners.get(test_id, {}).get(region):
            return provisioner
        provisioner = super().__new__(cls)
        cls._provisioners[test_id] = cls._provisioners.get(test_id, {}) | {region: provisioner}
        return provisioner

    def __init__(self, test_id: str, region: str, **kwargs) -> None:  # pylint: disable=unused-argument
        super().__init__(test_id, region)
        self._instances: Dict[str, VmInstance] = getattr(self, "_instances", {})

    def get_or_create_instance(self,
                               definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT
                               ) -> VmInstance:
        if v_m := self._instances.get(definition.name):
            return v_m
        v_m = VmInstance(name=definition.name, region=self.region,
                         user_name=definition.user_name,
                         ssh_key_name=definition.ssh_key.name,
                         public_ip_address='123.123.123.123',
                         private_ip_address='10.10.10.10', tags=definition.tags,
                         pricing_model=pricing_model,
                         image=definition.image_id,
                         _provisioner=self)
        self._instances[definition.name] = v_m
        return v_m

    def get_or_create_instances(self,
                                definitions: List[InstanceDefinition],
                                pricing_model: PricingModel = PricingModel.SPOT
                                ) -> List[VmInstance]:
        return [self.get_or_create_instance(definition, pricing_model) for definition in definitions]

    def list_instances(self) -> List[VmInstance]:
        return list(self._instances.values())

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        self._instances[name].tags.update(tags)

    def terminate_instance(self, name: str, wait: bool = False) -> None:
        del self._instances[name]

    def cleanup(self, wait: bool = False) -> None:
        self._instances = {}

    def reboot_instance(self, name: str, wait: bool) -> None:
        pass

    @classmethod
    def discover_regions(cls, test_id: str, **kwargs) -> List[Provisioner]:  # pylint: disable=unused-argument
        return list(cls._provisioners.get(test_id, {}).values())
