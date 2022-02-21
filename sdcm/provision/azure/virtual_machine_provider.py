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

import logging
import os
from dataclasses import dataclass, field

from typing import Dict, Optional, Any

import binascii
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.compute.models import VirtualMachine

from sdcm.provision.provisioner import InstanceDefinition, PricingModel
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class VirtualMachineProvider:
    _resource_group_name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, VirtualMachine] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing virtual machines for resource group."""
        try:
            v_ms = self._azure_service.compute.virtual_machines.list(self._resource_group_name)
            for v_m in v_ms:
                v_m = self._azure_service.compute.virtual_machines.get(self._resource_group_name, v_m.name)
                self._cache[v_m.name] = v_m
        except ResourceNotFoundError:
            pass

    def get_or_create(self, definition: InstanceDefinition, nic_id: str, pricing_model: PricingModel) -> VirtualMachine:
        if definition.name in self._cache:
            return self._cache[definition.name]
        LOGGER.info(
            "Creating '{name}' VM in resource group {rg}...".format(name=definition.name, rg=self._resource_group_name))
        LOGGER.info("Instance params: {definition}".format(definition=definition))
        params = {
            "location": self._region,
            "tags": definition.tags,
            "hardware_profile": {
                "vm_size": definition.type,
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_id,
                    "properties": {"deleteOption": "Delete"}
                }],
            },
        }
        if definition.user_name is None:
            # in case we use specialized image, we don't change things like computer_name, usernames, ssh_keys
            os_profile = {}
        else:
            os_profile = self._get_os_profile(computer_name=definition.name,
                                              admin_username=definition.user_name,
                                              admin_password=binascii.hexlify(os.urandom(20)).decode(),
                                              ssh_public_key=definition.ssh_public_key)
        storage_profile = self._get_scylla_storage_profile(image_id=definition.image_id, name=definition.name,
                                                           disk_size=definition.root_disk_size)
        params.update(os_profile)
        params.update(storage_profile)
        params.update(self._get_pricing_params(pricing_model))
        self._azure_service.compute.virtual_machines.begin_create_or_update(
            resource_group_name=self._resource_group_name,
            vm_name=definition.name,
            parameters=params).wait()
        v_m = self._azure_service.compute.virtual_machines.get(self._resource_group_name, definition.name)
        LOGGER.info("Provisioned VM {name} in the {resource} resource group".format(
            name=v_m.name, resource=self._resource_group_name))
        self._cache[v_m.name] = v_m
        return v_m

    def list(self):
        return list(self._cache.values())

    def delete(self, name: str, wait: bool = True):
        LOGGER.info("Triggering termination of instance: {name}".format(name=name))
        self._azure_service.compute.virtual_machines.begin_update(self._resource_group_name,
                                                                  vm_name=name,
                                                                  parameters={
                                                                      "storageProfile": {
                                                                          "osDisk": {
                                                                              "createOption": "FromImage",
                                                                              "deleteOption": "Delete"
                                                                          }
                                                                      }
                                                                  })

        task = self._azure_service.compute.virtual_machines.begin_delete(self._resource_group_name, vm_name=name)
        if wait is True:
            LOGGER.info("Waiting for termination of instance: {name}...".format(name=name))
            task.wait()
            LOGGER.info("Instance {name} has been terminated.".format(name=name))
        del self._cache[name]

    def reboot(self, name: str, wait: bool = True) -> None:
        LOGGER.info("Triggering reboot of instance: {name}".format(name=name))
        task = self._azure_service.compute.virtual_machines.begin_restart(self._resource_group_name, vm_name=name)
        if wait is True:
            LOGGER.info("Waiting for reboot of instance: {name}...".format(name=name))
            task.wait()
            LOGGER.info("Instance {name} has been rebooted.".format(name=name))

    @staticmethod
    def _get_os_profile(computer_name: str, admin_username: str,
                        admin_password: str, ssh_public_key: str):
        os_profile = {"os_profile": {
            "computer_name": computer_name,
            "admin_username": admin_username,
            "admin_password": admin_password if admin_password else binascii.hexlify(os.urandom(20)).decode(),
            "linux_configuration": {
                "disable_password_authentication": True,
                "ssh": {
                    "public_keys": [{
                        "path": f"/home/{admin_username}/.ssh/authorized_keys",
                        "key_data": ssh_public_key,
                    }],
                },
            },
        }
        }
        return os_profile

    @staticmethod
    def _get_scylla_storage_profile(image_id: str, name: str, disk_size: Optional[int] = None) -> Dict[str, Any]:
        """Creates storage profile based on image_id. image_id may refer to scylla-crafted images
         (starting with '/subscription') or to 'Urn' of image (see output of e.g. `az vm image list --output table`)"""
        storage_profile = {"storage_profile": {
            "os_disk": {
                           "name": f"{name}-os-disk",
                           "os_type": "linux",
                           "caching": "ReadWrite",
                           "create_option": "FromImage",
                           "deleteOption": "Delete",  # somehow deletion of VM does not delete os_disk anyway...
                           "managed_disk": {
                               "storage_account_type": "Premium_LRS",  # SSD
                           }
                           } | ({} if disk_size is None else {"disk_size_gb": disk_size}),
        }}
        if image_id.startswith("/subscriptions/"):
            storage_profile.update({
                "storage_profile": {
                    "image_reference": {"id": image_id},
                    "deleteOption": "Delete"
                }
            })
        else:
            image_reference_values = image_id.split(":")
            storage_profile.update({
                "storage_profile": {
                    "image_reference": {
                        "publisher": image_reference_values[0],
                        "offer": image_reference_values[1],
                        "sku": image_reference_values[2],
                        "version": image_reference_values[3],
                    },
                }
            })
        return storage_profile

    @staticmethod
    def _get_pricing_params(pricing_model: PricingModel):
        if pricing_model != PricingModel.ON_DEMAND:
            return {
                "priority": "Spot",  # possible values are "Regular", "Low", or "Spot"
                "eviction_policy": "Delete",  # can be "Deallocate" or "Delete", Deallocate leaves disks intact
                "billing_profile": {
                    "max_price": -1,  # -1 indicates the VM shouldn't be evicted for price reasons
                }
            }
        else:
            return {}

    def clear_cache(self):
        self._cache = {}
