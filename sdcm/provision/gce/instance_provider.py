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

"""VM instance management provider for GCE provisioning."""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional

import google.api_core.exceptions
from google.cloud import compute_v1

from sdcm.provision.provisioner import InstanceDefinition, PricingModel, ProvisionError
from sdcm.provision.gce.disk_provider import DiskProvider
from sdcm.provision.gce.network_provider import NetworkProvider
from sdcm.provision.gce.constants import DISK_TYPE_PD_STANDARD, DISK_TYPE_LOCAL_SSD
from sdcm.provision.gce.utils import tags_to_gce_labels, normalize_instance_name
from sdcm.utils.gce_utils import (
    get_gce_compute_instances_client,
    wait_for_extended_operation,
    gce_set_labels,
    get_gce_service_accounts,
)
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


class VirtualMachineProvider:
    """Provider for creating and managing GCE VM instances."""

    def __init__(
        self, project_id: str, zone: str, test_id: str, disk_provider: DiskProvider, network_provider: NetworkProvider
    ):
        self.project_id = project_id
        self.zone = zone
        self.test_id = test_id
        self.disk_provider = disk_provider
        self.network_provider = network_provider
        self._instances_client, _ = get_gce_compute_instances_client()
        self._cache: Dict[str, compute_v1.Instance] = {}

    def get(self, name: str) -> Optional[compute_v1.Instance]:
        """Get an instance by name."""
        if name in self._cache:
            return self._cache[name]
        try:
            instance = self._instances_client.get(project=self.project_id, zone=self.zone, instance=name)
            self._cache[name] = instance
            return instance
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Instance %s not found: %s", name, exc)
            return None

    def list(self) -> List[compute_v1.Instance]:
        """List all instances in the zone for this test."""
        try:
            return [
                inst
                for inst in self._instances_client.list(project=self.project_id, zone=self.zone)
                if inst.metadata
                and inst.metadata.items
                and any(item.key == "TestId" and item.value == self.test_id for item in inst.metadata.items)
            ]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to list instances: %s", exc)
            return []

    def get_or_create(
        self,
        definitions: List[InstanceDefinition],
        pricing_model: PricingModel,
        user_data_list: Optional[List[str]] = None,
        startup_script_list: Optional[List[str]] = None,
    ) -> List[compute_v1.Instance]:
        """
        Get existing instances or create new ones with retry logic.

        Uses _create_instance_with_retries to handle transient failures
        such as preemption or quota issues with automatic retries.
        """
        user_data_list = user_data_list or [""] * len(definitions)
        startup_script_list = startup_script_list or [""] * len(definitions)

        instances, error_to_raise = [], None

        for definition, user_data, startup_script in zip(definitions, user_data_list, startup_script_list):
            normalized_name = normalize_instance_name(definition.name)
            if normalized_name in self._cache:
                instances.append(self._cache[normalized_name])
                continue

            LOGGER.info("Creating '%s' VM in zone %s...", normalized_name, self.zone)
            LOGGER.info("Instance params: %s", definition)

            try:
                instance = self._create_instance_with_retries(definition, pricing_model, user_data, startup_script)
                instances.append(instance)
            except google.api_core.exceptions.GoogleAPIError as err:
                LOGGER.error("Error when creating instance %s: %s", normalized_name, str(err))
                error_to_raise = err

        if error_to_raise:
            raise ProvisionError(f"Failed to create instances: {error_to_raise}") from error_to_raise
        return instances

    @retrying(
        n=3,
        sleep_time=900,
        allowed_exceptions=(google.api_core.exceptions.GoogleAPIError, google.api_core.exceptions.NotFound),
        message="Retrying to create a GCE node...",
        raise_on_exceeded=True,
    )
    def _create_instance_with_retries(
        self, definition: InstanceDefinition, pricing_model: PricingModel, user_data: str = "", startup_script: str = ""
    ) -> compute_v1.Instance:
        """Create a single GCE instance with retry logic."""
        normalized_name = normalize_instance_name(definition.name)
        try:
            operation = self._build_and_insert_instance(
                definition, pricing_model, user_data, startup_script, normalized_name
            )
            wait_for_extended_operation(operation, f"instance creation for {normalized_name}")
            instance = self._instances_client.get(project=self.project_id, zone=self.zone, instance=normalized_name)
            self._set_instance_labels(instance, definition.tags, normalized_name)
            LOGGER.info("Instance %s created successfully", normalized_name)
            self._cache[normalized_name] = instance
            return instance
        except google.api_core.exceptions.GoogleAPIError as gce_error:
            LOGGER.warning("Instance %s creation failed: %s", normalized_name, gce_error)
            self._cleanup_failed_instance(normalized_name, str(gce_error))
            raise

    def _build_and_insert_instance(
        self,
        definition: InstanceDefinition,
        pricing_model: PricingModel,
        user_data: str,
        startup_script: str,
        normalized_name: str,
    ):
        """Build instance configuration and initiate creation (non-blocking)."""
        # Disk configuration based on machine type
        is_z3 = "z3-highmem" in definition.type
        root_disk_type = "hyperdisk-balanced" if is_z3 else DISK_TYPE_PD_STANDARD
        data_disks = (
            [d for d in (definition.data_disks or []) if d.type != DISK_TYPE_LOCAL_SSD]
            if is_z3
            else definition.data_disks
        )

        disks = self._build_disks(
            normalized_name, definition.image_id, root_disk_type, definition.root_disk_size or 50, data_disks
        )

        # Metadata
        tags = definition.tags | {
            "ssh_user": definition.user_name,
            "ssh_key": definition.ssh_key.name,
            "creation_time": datetime.now(tz=timezone.utc).isoformat(sep=" ", timespec="seconds"),
        }
        metadata = {**tags, "TestId": self.test_id}
        if user_data:
            metadata["user-data"] = user_data
        if startup_script:
            metadata["startup-script"] = startup_script
        if definition.ssh_key:
            metadata["ssh-keys"] = f"{definition.user_name}:{definition.ssh_key.public_key.decode('utf-8').strip()}"
            metadata["block-project-ssh-keys"] = "true"

        # Build instance
        instance = compute_v1.Instance(
            name=normalized_name,
            machine_type=f"zones/{self.zone}/machineTypes/{definition.type}",
            disks=disks,
            network_interfaces=[self._build_network_interface()],
        )
        instance.metadata = compute_v1.Metadata()
        for k, v in metadata.items():
            instance.metadata.items.append({"key": k, "value": str(v)})

        # Scheduling
        instance.scheduling = compute_v1.Scheduling()
        if is_z3:
            instance.scheduling.on_host_maintenance = "MIGRATE"
            instance.disks = [d for d in disks if "-data-local-ssd-" not in d.device_name]
        elif pricing_model.is_spot():
            instance.scheduling.on_host_maintenance = "TERMINATE"
            instance.scheduling.provisioning_model = compute_v1.Scheduling.ProvisioningModel.SPOT.name
            instance.scheduling.instance_termination_action = "STOP"

        # Network tags and service accounts
        network_tags = self.network_provider.get_network_tags(allow_public_access=definition.use_public_ip)
        if network_tags:
            instance.tags = compute_v1.Tags(items=network_tags)
        service_accounts = get_gce_service_accounts()
        if service_accounts:
            instance.service_accounts = [compute_v1.ServiceAccount(**sa) for sa in service_accounts]

        request = compute_v1.InsertInstanceRequest(zone=self.zone, project=self.project_id, instance_resource=instance)
        LOGGER.info("Creating instance %s in zone %s...", normalized_name, self.zone)
        return self._instances_client.insert(request=request)

    def _build_disks(
        self, instance_name: str, image_url: str, root_disk_type: str, root_disk_size_gb: int, data_disks
    ) -> List[compute_v1.AttachedDisk]:
        """Build disk configuration."""
        disks_config = self.disk_provider.create_disks_config(
            instance_name=instance_name,
            image_url=image_url,
            root_disk_type=root_disk_type,
            root_disk_size_gb=root_disk_size_gb,
            data_disks=data_disks,
        )
        disks = []
        for cfg in disks_config:
            disk = compute_v1.AttachedDisk(
                boot=cfg.get("boot", False),
                auto_delete=cfg.get("auto_delete", True),
                type_=cfg.get("type_", "PERSISTENT"),
                device_name=cfg.get("device_name", ""),
                mode=cfg.get("mode", "READ_WRITE"),
            )
            if cfg.get("interface"):
                disk.interface = cfg["interface"]
            if "initialize_params" in cfg:
                p = cfg["initialize_params"]
                disk.initialize_params = compute_v1.AttachedDiskInitializeParams(
                    source_image=p.get("source_image"), disk_size_gb=p.get("disk_size_gb"), disk_type=p.get("disk_type")
                )
            disks.append(disk)
        return disks

    def _build_network_interface(self) -> compute_v1.NetworkInterface:
        """Build network interface with external access."""
        access = compute_v1.AccessConfig(
            type_=compute_v1.AccessConfig.Type.ONE_TO_ONE_NAT.name,
            name="External NAT",
            network_tier=compute_v1.AccessConfig.NetworkTier.PREMIUM.name,
        )
        return compute_v1.NetworkInterface(network=self.network_provider.get_network_url(), access_configs=[access])

    def _set_instance_labels(self, instance: compute_v1.Instance, tags: Dict[str, str], name: str) -> None:
        """Set labels on instance."""
        labels = tags_to_gce_labels(tags)
        if labels:
            try:
                gce_set_labels(
                    instances_client=self._instances_client,
                    instance=instance,
                    new_labels=labels,
                    project=self.project_id,
                    zone=self.zone,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to set labels on instance %s: %s", name, exc)

    def _cleanup_failed_instance(self, name: str, error_message: str) -> None:
        """Cleanup instance that failed due to preemption or quota."""
        if "Instance failed to start due to preemption" in error_message or "Quota exceeded" in error_message:
            LOGGER.warning("Instance %s failed due to preemption or quota, attempting cleanup...", name)
            try:
                self.delete(name, wait=True)
            except google.api_core.exceptions.NotFound:
                LOGGER.warning("Instance %s not found during cleanup.", name)

    def delete(self, name: str, wait: bool = True) -> None:
        """Delete an instance."""
        try:
            operation = self._instances_client.delete(project=self.project_id, zone=self.zone, instance=name)
            if wait:
                wait_for_extended_operation(operation, "instance deletion")
            LOGGER.info("Instance %s deleted", name)
            self._cache.pop(name, None)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to delete instance %s: %s", name, exc)

    def reboot(self, name: str, wait: bool = True, hard: bool = False) -> None:
        """Reboot an instance."""
        try:
            if hard:
                operation = self._instances_client.reset(project=self.project_id, zone=self.zone, instance=name)
            else:
                operation = self._instances_client.stop(project=self.project_id, zone=self.zone, instance=name)
                if wait:
                    wait_for_extended_operation(operation, "instance stop")
                operation = self._instances_client.start(project=self.project_id, zone=self.zone, instance=name)
            if wait:
                wait_for_extended_operation(operation, "instance reboot")
            LOGGER.info("Instance %s rebooted", name)
            self._cache.pop(name, None)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to reboot instance %s: %s", name, exc)

    def add_tags(self, name: str, tags: Dict[str, str]) -> compute_v1.Instance:
        """Add labels to an instance."""
        instance = self.get(name)
        if not instance:
            raise ProvisionError(f"Instance {name} not found")
        gce_set_labels(
            instances_client=self._instances_client,
            instance=instance,
            new_labels=tags_to_gce_labels(tags),
            project=self.project_id,
            zone=self.zone,
        )
        self._cache.pop(name, None)
        return self.get(name)

    def clear_cache(self) -> None:
        """Clear the instance cache."""
        self._cache.clear()
