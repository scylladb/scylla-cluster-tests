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
import base64
import time
from datetime import datetime
import logging
import os
from dataclasses import dataclass, field

from typing import Dict, Optional, Any, List

import binascii
from azure.core.exceptions import ResourceNotFoundError, AzureError, ODataV4Error
from azure.mgmt.compute.models import VirtualMachine, RunCommandInput
from invoke import Result

from sdcm.provision.provisioner import InstanceDefinition, PricingModel, ProvisionError, OperationPreemptedError
from sdcm.provision.user_data import UserDataBuilder
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class VirtualMachineProvider:
    _resource_group_name: str
    _region: str
    _az: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, VirtualMachine] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing virtual machines for resource group."""
        try:
            v_ms = self._azure_service.compute.virtual_machines.list(self._resource_group_name)
            for _v_m in v_ms:
                v_m = self._azure_service.compute.virtual_machines.get(self._resource_group_name, _v_m.name)
                if v_m.provisioning_state != "Deleting":
                    self._cache[v_m.name] = v_m
        except ResourceNotFoundError:
            pass

    def get_or_create(self, definitions: List[InstanceDefinition], nics_ids: List[str], pricing_model: PricingModel
                      ) -> List[VirtualMachine]:

        v_ms = []
        pollers = []
        error_to_raise = None
        for definition, nic_id in zip(definitions, nics_ids):
            if definition.name in self._cache:
                v_ms.append(self._cache[definition.name])
                continue
            LOGGER.info(
                "Creating '%s' VM in resource group %s...", definition.name, self._resource_group_name)
            LOGGER.info("Instance params: %s", definition)
            tags = definition.tags | {"ssh_user": definition.user_name, "ssh_key": definition.ssh_key.name,
                                      "creation_time": datetime.utcnow().isoformat(sep=" ", timespec="seconds")}
            tags = self._replace_null_value_from_tags_with_empty_string(tags)
            params = {
                "location": self._region,
                "zones": [self._az] if self._az else [],
                "tags": tags,
                "hardware_profile": {
                    "vm_size": definition.type,
                },
                "network_profile": {
                    "network_interfaces": [{
                        "id": nic_id,
                        "properties": {"deleteOption": "Detach"}
                    }],
                },
            }
            if definition.user_data is None:
                # in case we use specialized image, we don't change things like computer_name, usernames, ssh_keys
                os_profile = {}
            else:
                builder = UserDataBuilder(user_data_objects=definition.user_data)
                custom_data = builder.build_user_data_yaml()
                os_profile = self._get_os_profile(computer_name=definition.name,
                                                  admin_username=definition.user_name,
                                                  admin_password=binascii.hexlify(os.urandom(20)).decode(),
                                                  ssh_public_key=definition.ssh_key.public_key.decode(),
                                                  custom_data=custom_data)
                params.update({"user_data": base64.b64encode(
                    builder.get_scylla_machine_image_json().encode('utf-8')).decode('latin-1')})
            storage_profile = self._get_scylla_storage_profile(image_id=definition.image_id, name=definition.name,
                                                               disk_size=definition.root_disk_size)
            params.update(os_profile)
            params.update(storage_profile)
            params.update(self._get_pricing_params(pricing_model))
            try:
                poller = self._azure_service.compute.virtual_machines.begin_create_or_update(
                    resource_group_name=self._resource_group_name,
                    vm_name=definition.name,
                    parameters=params)
                pollers.append((definition, poller))
            except AzureError as err:
                LOGGER.error("Error when sending create vm request for VM %s: %s", definition.name, str(err))
                error_to_raise = err
        for definition, poller in pollers:
            try:
                poller.wait()
                v_m = self._azure_service.compute.virtual_machines.get(
                    self._resource_group_name, definition.name, expand="instanceView")
                if v_m.instance_view and v_m.instance_view.statuses:
                    statuses = ", ".join(
                        f"{status.code}: {status.display_status}" for status in v_m.instance_view.statuses
                    )
                    LOGGER.debug("Provisioned VM %s instance state: %s", v_m.name, statuses)
                else:
                    LOGGER.warning("Instance view is not available for VM %s", v_m.name)
                self._cache[v_m.name] = v_m
                v_ms.append(v_m)
            except ODataV4Error as err:
                LOGGER.error("Error when waiting for VM %s: %s", definition.name, str(err))
                if err.code == 'OperationPreempted':
                    raise OperationPreemptedError(err)  # spot instance preemption, abort provision immediately
            except AzureError as err:
                LOGGER.error("Error when waiting for creation of VM %s: %s", definition.name, str(err))
                error_to_raise = err
        if error_to_raise:
            raise ProvisionError(error_to_raise)
        return v_ms

    def list(self):
        return list(self._cache.values())

    def delete(self, name: str, wait: bool = True):
        LOGGER.info("Triggering termination of instance: %s", name)
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
            LOGGER.info("Waiting for termination of instance: %s...", name)
            task.wait()
            LOGGER.info("Instance %s has been terminated.", name)
        del self._cache[name]

    def reboot(self, name: str, wait: bool = True, hard: bool = False) -> None:
        LOGGER.info("Triggering reboot of instance: %s", name)
        flags = "-ff" if hard else "-f"
        self.run_command(name, f"reboot {flags}")
        start_time = time.time()
        while wait and time.time() - start_time < 600:  # 10 minutes
            time.sleep(10)
            instance_view = self._azure_service.compute.virtual_machines.instance_view(
                self._resource_group_name, vm_name=name)
            if instance_view and instance_view.statuses[-1].display_status == 'VM running':
                break

    def add_tags(self, name: str, tags: Dict[str, str]) -> VirtualMachine:
        """Adds tags to instance (with waiting for completion)"""
        if name not in self._cache:
            raise AttributeError(f"Instance '{name}' does not exist in resource group '{self._resource_group_name}'")
        current_tags = self._cache[name].tags
        tags = self._replace_null_value_from_tags_with_empty_string(tags)
        current_tags.update(tags)
        self._azure_service.compute.virtual_machines.begin_update(self._resource_group_name, name, parameters={
            "tags": current_tags
        }).wait()
        v_m = self._azure_service.compute.virtual_machines.get(self._resource_group_name, name)
        self._cache[v_m.name] = v_m
        return v_m

    @staticmethod
    def _get_os_profile(computer_name: str, admin_username: str,
                        admin_password: str, ssh_public_key: str, custom_data: str) -> Dict[str, Any]:
        os_profile = {"os_profile": {
            "computer_name": computer_name,
            "admin_username": admin_username,
            "admin_password": admin_password,
            "custom_data": base64.b64encode(custom_data.encode('utf-8')).decode('latin-1'),
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
                               "storage_account_type": "StandardSSD_LRS",  # SSD
                           }
                           } | ({} if disk_size is None else {"disk_size_gb": disk_size}),
        }}
        if image_id.startswith("/subscriptions/"):
            storage_profile['storage_profile'].update({
                "image_reference": {"id": image_id},
                "deleteOption": "Delete"
            })
        elif image_id.startswith("/CommunityGalleries/"):
            storage_profile['storage_profile'].update({
                "image_reference": {"community_gallery_image_id": image_id},
                "deleteOption": "Delete"
            })
        else:
            image_reference_values = image_id.split(":")
            storage_profile['storage_profile'].update({
                "image_reference": {
                    "publisher": image_reference_values[0],
                    "offer": image_reference_values[1],
                    "sku": image_reference_values[2],
                    "version": image_reference_values[3],
                },
            })
        return storage_profile

    def run_command(self, name: str, command: str) -> Result:
        if name not in self._cache:
            raise AttributeError(f"Instance '{name}' does not exist in resource group '{self._resource_group_name}'")
        LOGGER.debug("Running command '%s' on instance: %s", command, name)
        command_object = RunCommandInput(command_id="RunShellScript", script=[command])
        result = self._azure_service.compute.virtual_machines.begin_run_command(
            self._resource_group_name, name, command_object).result()
        LOGGER.debug("Finished running command '%s' on instance: %s: %s", command, name, result.value[0])
        try:
            stdout, stderr = result.value[0].message.split('[stdout]\n')[1].split('\n[stderr]\n')[0:2]
            exited = 0  # doesn't mean it passed, Azure does not provide rc
        except IndexError:
            stdout, stderr = result.value[0].message, ""
            exited = 1
        return Result(
            stdout=stdout,
            stderr=stderr,
            exited=exited,
            encoding="utf-8"
        )

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
            return {
                "priority": "Regular"
            }

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def _replace_null_value_from_tags_with_empty_string(tags: Dict[str, str]) -> Dict[str, str]:
        """Azure API does not accept 'null' as value for tags, so we replace it with empty string."""
        for key, value in tags.items():
            if value == "null":
                LOGGER.warning("Tag '%s' has value 'null' which is not allowed by Azure API. Replacing with empty string.",
                               key)
                tags[key] = ""
        return tags
