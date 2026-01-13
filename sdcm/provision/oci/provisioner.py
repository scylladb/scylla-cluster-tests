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
# Copyright (c) 2026 ScyllaDB

import logging
import string
from typing import Dict, List
from invoke import Result

from sdcm.provision.oci.constants import OCI_REGION_NAMES_MAPPING
from sdcm.provision.oci.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import (
    InstanceDefinition,
    PricingModel,
    Provisioner,
    VmInstance,
)
from sdcm.utils.oci_region import OciRegion
from sdcm.utils.oci_utils import (
    OciService,
    get_oci_compartment_id,
)

LOGGER = logging.getLogger(__name__)


class OciProvisioner(Provisioner):
    """Provides API for VM provisioning in Oracle Cloud Infrastructure (OCI), tuned for Scylla QA."""

    def __init__(
        self, test_id: str, region: str, availability_zone: str, oci_service: OciService = OciService(), **config
    ):
        availability_zone = self._convert_az_to_zone(availability_zone)
        region = OCI_REGION_NAMES_MAPPING.get(region, region)
        super().__init__(test_id, region, availability_zone)
        self._oci_service: OciService = oci_service
        self._cache: Dict[str, VmInstance] = {}
        self._compartment_id = get_oci_compartment_id()
        self._vm_provider = VirtualMachineProvider(self._compartment_id, self._region, self._az, self._oci_service)
        for instance_data in self._vm_provider.list_instances(test_id=self.test_id):
            try:
                vm_instance = self._vm_provider.convert_to_vm_instance(instance_data, self)
                self._cache[vm_instance.name] = vm_instance
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to cache instance. Error: %s", exc)

    @staticmethod
    def _convert_az_to_zone(availability_zone: str) -> str:
        """OCI uses numbers for availiability zones, while in tests we use letters.
        In case user provides letter instead of zone number, convert it to number (a -> 1, b -> 2)
        """
        if availability_zone and availability_zone in (chars := string.ascii_lowercase):
            return str(chars.find(availability_zone) + 1)
        return availability_zone

    def __str__(self):
        return f"{self.__class__.__name__}(region={self.region}, az={self.availability_zone})"

    @classmethod
    def discover_regions(
        cls, test_id: str = "", regions: list = None, oci_service: OciService = OciService(), **kwargs
    ) -> List["OciProvisioner"]:
        """Discovers provisioners for given test id."""
        discovered_provisioners = []
        target_regions = regions if regions else [oci_service.oci_credentials["region"]]
        for region in target_regions:
            provider = VirtualMachineProvider(get_oci_compartment_id(), region, "", oci_service)
            instances = provider.list_instances(test_id)
            found_azs = set()
            for inst in instances:
                # NOTE: Extract AD number from "ewbj:US-ASHBURN-AD-2" -> "2"
                ad_name = inst.availability_domain
                if ad_name:
                    found_azs.add(ad_name.split("-")[-1])
            if found_azs:
                for az in found_azs:
                    discovered_provisioners.append(cls(test_id, region, az, oci_service))
        return discovered_provisioners

    def get_or_create_instance(
        self, definition: InstanceDefinition, pricing_model: PricingModel = PricingModel.SPOT
    ) -> VmInstance:
        return self.get_or_create_instances(definitions=[definition], pricing_model=pricing_model)[0]

    def get_or_create_instances(
        self, definitions: List[InstanceDefinition], pricing_model: PricingModel = PricingModel.SPOT
    ) -> List[VmInstance]:
        """Create a set of instances specified by a list of InstanceDefinition."""
        provisioned_vm_instances = []
        definitions_to_provision = []

        # Construct definitions for VMs
        oci_region = OciRegion(self.region)
        for definition in definitions:
            if definition.name in self._cache:
                inst = self._cache[definition.name]
                provisioned_vm_instances.append(inst)
            else:
                definitions_to_provision.append(definition)

            # NOTE: we get definitions for DB nodes, loaders and monitor here
            #       Monitor going to use public subnet and DB, most probably, private one.
            #       So, process each definition eparately.
            LOGGER.warning(
                "DEBUG: node '%s' - going to look for a subnet with following visibility: %s",
                definition.name,
                definition.use_public_ip,
            )
            subnet = oci_region.subnet(public=definition.use_public_ip)
            LOGGER.warning("DEBUG: node '%s' - found subnet: %s", definition.name, subnet)
            if not subnet:
                LOGGER.warning("DEBUG: node '%s' - going to create a subnet", definition.name)
                subnet = oci_region.create_subnet(public=definition.use_public_ip)
                LOGGER.warning("DEBUG: node '%s' - created subnet: %s", definition.name, subnet)
            if not subnet:
                prefix = "Public" if definition.use_public_ip else "Private"
                raise Exception(
                    f"{prefix} subnet not found in region '{self.region}'. Check infrastructure configuration"
                )

        if not definitions_to_provision:
            LOGGER.info("DEBUG: reusing existing instances")
            return provisioned_vm_instances

        # Provision VMs
        new_instances = self._vm_provider.create_instances(oci_region, definitions_to_provision, pricing_model)
        for inst in new_instances:
            vm_inst = self._vm_provider.convert_to_vm_instance(inst, self)
            self._cache[vm_inst.name] = vm_inst
            provisioned_vm_instances.append(vm_inst)

        return provisioned_vm_instances

    def terminate_instance(self, name: str, wait: bool = True) -> None:
        """Terminate instance by name"""
        self._vm_provider.terminate(name, wait)
        if name in self._cache:
            del self._cache[name]

    def reboot_instance(self, name: str, wait: bool, hard: bool = False) -> None:
        """Reboot instance by name."""
        self._vm_provider.reboot(name, wait, hard)

    def list_instances(self) -> List[VmInstance]:
        """List instances for given provisioner."""
        return list(self._cache.values())

    def cleanup(self, wait: bool = False) -> None:
        """Cleans up all the resources. If wait == True, waits till cleanup fully completes.

        Currently defaults to a subset of cleanup actions:
        - Terminate known instances
        """
        # TODO: cleanup block devices provisioned for non-dense shapes
        for name in list(self._cache.keys()):
            self.terminate_instance(name, wait=False)

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Adds tags to instance."""
        self._vm_provider.add_tags(name, tags)

    def run_command(self, name: str, command: str) -> Result:
        raise NotImplementedError()
