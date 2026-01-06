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

"""
VM instance management provider for GCE provisioning.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional

from google.cloud import compute_v1

from sdcm.provision.provisioner import (
    InstanceDefinition,
    PricingModel,
    ProvisionError,
)
from sdcm.provision.gce.disk_provider import DiskProvider
from sdcm.provision.gce.network_provider import NetworkProvider
from sdcm.provision.gce.utils import tags_to_gce_labels, normalize_instance_name
from sdcm.utils.gce_utils import (
    get_gce_compute_instances_client,
    wait_for_extended_operation,
    gce_set_labels,
)

LOGGER = logging.getLogger(__name__)


class VirtualMachineProvider:
    """Provider for creating and managing GCE VM instances."""

    def __init__(
        self,
        project_id: str,
        zone: str,
        test_id: str,
        disk_provider: DiskProvider,
        network_provider: NetworkProvider,
    ):
        """
        Initialize the VirtualMachineProvider.

        Args:
            project_id: GCE project ID
            zone: GCE zone (e.g., 'us-east1-b')
            test_id: Test ID for tagging/filtering instances
            disk_provider: DiskProvider instance for disk configurations
            network_provider: NetworkProvider instance for network configurations
        """
        self.project_id = project_id
        self.zone = zone
        self.test_id = test_id
        self.disk_provider = disk_provider
        self.network_provider = network_provider
        self._instances_client, _ = get_gce_compute_instances_client()
        self._cache: Dict[str, compute_v1.Instance] = {}

    def get(self, name: str) -> Optional[compute_v1.Instance]:
        """
        Get an instance by name.

        Args:
            name: Instance name

        Returns:
            Instance object or None if not found
        """
        if name in self._cache:
            return self._cache[name]

        try:
            instance = self._instances_client.get(
                project=self.project_id,
                zone=self.zone,
                instance=name,
            )
            self._cache[name] = instance
            return instance
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Instance %s not found: %s", name, exc)
            return None

    def list(self) -> List[compute_v1.Instance]:
        """
        List all instances in the zone for this test.

        Returns:
            List of instance objects
        """
        try:
            instances = self._instances_client.list(
                project=self.project_id,
                zone=self.zone,
            )
            # Filter by test_id if instances have the metadata
            filtered = []
            for instance in instances:
                if instance.metadata and instance.metadata.items:
                    for item in instance.metadata.items:
                        if hasattr(item, "key") and hasattr(item, "value"):
                            if item.key == "TestId" and item.value == self.test_id:
                                filtered.append(instance)
                                break
            return filtered
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to list instances: %s", exc)
            return []

    def get_or_create(
        self,
        definition: InstanceDefinition,
        pricing_model: PricingModel,
        user_data: str = "",
        startup_script: str = "",
    ) -> compute_v1.Instance:
        """
        Get an existing instance or create a new one.

        Args:
            definition: Instance definition
            pricing_model: Pricing model (spot or on-demand)
            user_data: Cloud-init user data
            startup_script: Startup script to run on boot

        Returns:
            Created or existing instance

        Raises:
            ProvisionError: If instance creation fails
        """
        # Check if instance already exists
        existing = self.get(definition.name)
        if existing:
            LOGGER.info("Instance %s already exists", definition.name)
            return existing

        # Normalize instance name
        normalized_name = normalize_instance_name(definition.name)
        if normalized_name != definition.name:
            LOGGER.warning(
                "Instance name normalized from '%s' to '%s'",
                definition.name,
                normalized_name,
            )

        try:
            instance = self._create_instance(definition, pricing_model, user_data, startup_script)
            self._cache[normalized_name] = instance
            return instance
        except Exception as exc:  # noqa: BLE001
            raise ProvisionError(f"Failed to create instance {normalized_name}: {exc}") from exc

    def _create_instance(  # noqa: PLR0914
        self,
        definition: InstanceDefinition,
        pricing_model: PricingModel,
        user_data: str,
        startup_script: str,
    ) -> compute_v1.Instance:
        """
        Create a new GCE instance.

        Args:
            definition: Instance definition
            pricing_model: Pricing model
            user_data: Cloud-init user data
            startup_script: Startup script

        Returns:
            Created instance object
        """
        normalized_name = normalize_instance_name(definition.name)

        # Create disk configuration
        disks_config = self.disk_provider.create_disks_config(
            instance_name=normalized_name,
            image_url=definition.image_id,
            root_disk_size_gb=definition.root_disk_size or 50,
            local_ssd_count=definition.local_ssd_count,
            data_disks=definition.data_disks,
        )

        # Convert disk configs to AttachedDisk objects
        disks = []
        for disk_config in disks_config:
            disk = compute_v1.AttachedDisk()
            disk.boot = disk_config.get("boot", False)
            disk.auto_delete = disk_config.get("auto_delete", True)
            disk.type_ = disk_config.get("type_", "PERSISTENT")
            disk.device_name = disk_config.get("device_name", "")
            disk.mode = disk_config.get("mode", "READ_WRITE")

            if disk_config.get("interface"):
                disk.interface = disk_config["interface"]

            if "initialize_params" in disk_config:
                init_params = compute_v1.AttachedDiskInitializeParams()
                params = disk_config["initialize_params"]
                if "source_image" in params:
                    init_params.source_image = params["source_image"]
                if "disk_size_gb" in params:
                    init_params.disk_size_gb = params["disk_size_gb"]
                if "disk_type" in params:
                    init_params.disk_type = params["disk_type"]
                disk.initialize_params = init_params

            disks.append(disk)

        tags = definition.tags | {
            "ssh_user": definition.user_name,
            "ssh_key": definition.ssh_key.name,
            "creation_time": datetime.now(tz=timezone.utc).isoformat(sep=" ", timespec="seconds"),
        }
        # Prepare metadata
        metadata = {
            **tags,
            "TestId": self.test_id,
        }

        if user_data:
            metadata["user-data"] = user_data

        if startup_script:
            metadata["startup-script"] = startup_script

        # Add SSH key
        if definition.ssh_key:
            # SSHKey has public_key as bytes, decode it to get the key string
            # The public_key is already in OpenSSH format (e.g., "ssh-ed25519 AAAA... comment")
            public_key_str = definition.ssh_key.public_key.decode("utf-8").strip()
            metadata["ssh-keys"] = f"{definition.user_name}:{public_key_str}"
            metadata["block-project-ssh-keys"] = "true"

        # Create network interface
        network_interface = compute_v1.NetworkInterface()
        network_interface.network = self.network_provider.get_network_url()

        # Always add external access config for GCE instances
        access = compute_v1.AccessConfig()
        access.type_ = compute_v1.AccessConfig.Type.ONE_TO_ONE_NAT.name
        access.name = "External NAT"
        access.network_tier = access.NetworkTier.PREMIUM.name
        network_interface.access_configs = [access]

        # Create instance object
        instance = compute_v1.Instance()
        instance.name = normalized_name
        instance.machine_type = f"zones/{self.zone}/machineTypes/{definition.type}"
        instance.disks = disks
        instance.network_interfaces = [network_interface]

        # Add metadata
        instance.metadata = compute_v1.Metadata()
        for key, value in metadata.items():
            instance.metadata.items.append({"key": key, "value": str(value)})

        # Configure scheduling (spot vs on-demand)
        instance.scheduling = compute_v1.Scheduling()
        if pricing_model.is_spot():
            instance.scheduling.provisioning_model = compute_v1.Scheduling.ProvisioningModel.SPOT.name
            instance.scheduling.instance_termination_action = "STOP"

        # Add network tags
        network_tags = self.network_provider.get_network_tags(allow_public_access=definition.use_public_ip)
        if network_tags:
            instance.tags = compute_v1.Tags()
            instance.tags.items = network_tags

        # Add service accounts for KMS/API access
        if definition.service_accounts:
            instance.service_accounts = [compute_v1.ServiceAccount(**sa) for sa in definition.service_accounts]

        # Create the instance
        request = compute_v1.InsertInstanceRequest()
        request.zone = self.zone
        request.project = self.project_id
        request.instance_resource = instance

        LOGGER.info("Creating instance %s in zone %s...", normalized_name, self.zone)
        operation = self._instances_client.insert(request=request)
        wait_for_extended_operation(operation, "instance creation")

        # Get the created instance
        created_instance = self._instances_client.get(
            project=self.project_id,
            zone=self.zone,
            instance=normalized_name,
        )

        # Set labels (GCE labels are separate from metadata)
        labels = tags_to_gce_labels(definition.tags)
        if labels:
            try:
                gce_set_labels(
                    instances_client=self._instances_client,
                    instance=created_instance,
                    new_labels=labels,
                    project=self.project_id,
                    zone=self.zone,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to set labels on instance %s: %s", normalized_name, exc)

        LOGGER.info("Instance %s created successfully", normalized_name)
        return created_instance

    def delete(self, name: str, wait: bool = True) -> None:
        """
        Delete an instance.

        Args:
            name: Instance name
            wait: Whether to wait for deletion to complete
        """
        try:
            operation = self._instances_client.delete(
                project=self.project_id,
                zone=self.zone,
                instance=name,
            )
            if wait:
                wait_for_extended_operation(operation, "instance deletion")
            LOGGER.info("Instance %s deleted", name)
            if name in self._cache:
                del self._cache[name]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to delete instance %s: %s", name, exc)

    def reboot(self, name: str, wait: bool = True, hard: bool = False) -> None:
        """
        Reboot an instance.

        Args:
            name: Instance name
            wait: Whether to wait for reboot to complete
            hard: Whether to perform a hard reboot (reset)
        """
        try:
            if hard:
                operation = self._instances_client.reset(
                    project=self.project_id,
                    zone=self.zone,
                    instance=name,
                )
            else:
                operation = self._instances_client.stop(
                    project=self.project_id,
                    zone=self.zone,
                    instance=name,
                )
                if wait:
                    wait_for_extended_operation(operation, "instance stop")

                operation = self._instances_client.start(
                    project=self.project_id,
                    zone=self.zone,
                    instance=name,
                )

            if wait:
                wait_for_extended_operation(operation, "instance reboot")

            LOGGER.info("Instance %s rebooted", name)
            # Invalidate cache
            if name in self._cache:
                del self._cache[name]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to reboot instance %s: %s", name, exc)

    def add_tags(self, name: str, tags: Dict[str, str]) -> compute_v1.Instance:
        """
        Add labels to an instance.

        Args:
            name: Instance name
            tags: Tags to add (will be converted to labels)

        Returns:
            Updated instance object
        """
        instance = self.get(name)
        if not instance:
            raise ProvisionError(f"Instance {name} not found")

        labels = tags_to_gce_labels(tags)
        gce_set_labels(
            instances_client=self._instances_client,
            instance=instance,
            new_labels=labels,
            project=self.project_id,
            zone=self.zone,
        )

        # Invalidate cache and get updated instance
        if name in self._cache:
            del self._cache[name]

        return self.get(name)

    def clear_cache(self) -> None:
        """Clear the instance cache."""
        self._cache.clear()
