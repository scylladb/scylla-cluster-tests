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
Main GCE provisioner implementation.
"""

import logging
from datetime import datetime
from typing import List, Dict

from google.cloud import compute_v1
from invoke import Result

from sdcm.provision.provisioner import (
    Provisioner,
    InstanceDefinition,
    VmInstance,
    PricingModel,
    ProvisionError,
)
from sdcm.provision.gce.disk_provider import DiskProvider
from sdcm.provision.gce.network_provider import NetworkProvider
from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.gce.kms_provider import GcpKmsProvider
from sdcm.utils.gce_utils import get_gce_compute_instances_client, random_zone
from sdcm.keystore import KeyStore
from sdcm.remote import RemoteCmdRunnerBase

LOGGER = logging.getLogger(__name__)


class GceProvisioner(Provisioner):
    """Provides API for VM provisioning in GCE cloud, tuned for Scylla QA."""

    def __init__(self, test_id: str, region: str, availability_zone: str, network_name: str = "default", **config):
        """
        Initialize the GCE Provisioner.

        Args:
            test_id: Test ID for tagging resources
            region: GCE region (e.g., 'us-east1')
            availability_zone: GCE zone letter (e.g., 'b') or None for random
            network_name: VPC network name
            **config: Additional configuration options
        """
        # If availability_zone is None or empty, use random_zone
        # This matches the logic in GCECluster.__init__
        if not availability_zone:
            availability_zone = random_zone(region)

        # Construct full zone name (region + zone)
        zone = f"{region}-{availability_zone}"
        super().__init__(test_id, region, zone)

        # Get GCE credentials
        credentials = KeyStore().get_gcp_credentials()
        self.project_id = credentials["project_id"]

        # Initialize providers
        self._disk_provider = DiskProvider(self.project_id, zone)
        self._network_provider = NetworkProvider(self.project_id, network_name)
        self._vm_provider = VirtualMachineProvider(
            self.project_id,
            zone,
            test_id,
            self._disk_provider,
            self._network_provider,
        )

        # Initialize KMS provider if needed
        self._kms_provider = GcpKmsProvider()

        # Cache for instances
        self._cache: Dict[str, VmInstance] = {}

        # Populate cache with existing instances
        for gce_instance in self._vm_provider.list():
            try:
                vm_instance = self._gce_instance_to_vm_instance(gce_instance)
                self._cache[vm_instance.name] = vm_instance
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to cache instance %s: %s", gce_instance.name, exc)

        LOGGER.debug("Initialized GceProvisioner for test_id=%s, region=%s, zone=%s", test_id, region, zone)

    def __str__(self):
        return f"{self.__class__.__name__}(region={self.region}, zone={self.availability_zone})"

    @property
    def zone(self) -> str:
        """Get the full zone name."""
        return self.availability_zone

    @classmethod
    def discover_regions(cls, test_id: str, **config) -> List["GceProvisioner"]:
        """
        Discover provisioners for each region where resources exist for the test.

        Args:
            test_id: Test ID to filter resources
            **config: Additional configuration

        Returns:
            List of GceProvisioner instances, one per region
        """
        # Get GCE credentials
        credentials = KeyStore().get_gcp_credentials()
        project_id = credentials["project_id"]

        instances_client, _ = get_gce_compute_instances_client()

        # Find all zones with instances for this test
        zones_with_instances = set()

        # List instances in all zones (aggregated list)
        try:
            for zone, instances in instances_client.aggregated_list(project=project_id):
                if not zone.startswith("zones/"):
                    continue

                zone_name = zone.split("/")[-1]

                for instance in instances.instances:
                    # Check if instance belongs to this test
                    if instance.metadata and instance.metadata.items:
                        for item in instance.metadata.items:
                            if hasattr(item, "key") and hasattr(item, "value"):
                                if item.key == "TestId" and item.value == test_id:
                                    zones_with_instances.add(zone_name)
                                    break
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to discover regions: %s", exc)

        # Create provisioners for each zone
        provisioners = []
        for zone_name in zones_with_instances:
            # Split zone into region and zone letter
            # e.g., "us-east1-b" -> region="us-east1", zone="b"
            parts = zone_name.rsplit("-", 1)
            if len(parts) == 2:
                region, zone_letter = parts
                provisioners.append(cls(test_id, region, zone_letter, **config))

        return provisioners

    def get_or_create_instance(
        self, definition: InstanceDefinition, pricing_model: PricingModel = PricingModel.SPOT
    ) -> VmInstance:
        """
        Create an instance specified by an InstanceDefinition.

        Args:
            definition: Instance definition
            pricing_model: Pricing model (spot or on-demand)

        Returns:
            VmInstance object
        """
        return self.get_or_create_instances([definition], pricing_model)[0]

    def get_or_create_instances(
        self, definitions: List[InstanceDefinition], pricing_model: PricingModel = PricingModel.SPOT
    ) -> List[VmInstance]:
        """
        Create a set of instances specified by a list of InstanceDefinition.

        Args:
            definitions: List of instance definitions
            pricing_model: Pricing model (spot or on-demand)

        Returns:
            List of VmInstance objects
        """
        provisioned_instances = []

        for definition in definitions:
            # Check if already in cache
            if definition.name in self._cache:
                provisioned_instances.append(self._cache[definition.name])
                continue

            # Prepare user data
            user_data = ""
            if definition.user_data:
                from sdcm.provision.user_data import UserDataBuilder  # noqa: PLC0415

                builder = UserDataBuilder(user_data_objects=definition.user_data)
                user_data = builder.build_user_data_yaml()

            # Create the instance
            gce_instance = self._vm_provider.get_or_create(
                definition,
                pricing_model,
                user_data=user_data,
                startup_script="",
            )

            # Convert to VmInstance and cache
            vm_instance = self._gce_instance_to_vm_instance(gce_instance)
            self._cache[definition.name] = vm_instance
            provisioned_instances.append(vm_instance)

        return provisioned_instances

    def terminate_instance(self, name: str, wait: bool = False) -> None:
        """
        Terminate instance by name.

        Args:
            name: Instance name
            wait: Whether to wait for termination to complete
        """
        self._vm_provider.delete(name, wait=wait)
        if name in self._cache:
            del self._cache[name]

    def reboot_instance(self, name: str, wait: bool, hard: bool = False) -> None:
        """
        Reboot instance by name.

        Args:
            name: Instance name
            wait: Whether to wait for reboot to complete
            hard: Whether to perform a hard reboot
        """
        self._vm_provider.reboot(name, wait=wait, hard=hard)
        # Invalidate cache
        if name in self._cache:
            del self._cache[name]

    def list_instances(self) -> List[VmInstance]:
        """
        List instances for given provisioner.

        Returns:
            List of VmInstance objects
        """
        return list(self._cache.values())

    def cleanup(self, wait: bool = False) -> None:
        """
        Clean up all resources for this test.

        Args:
            wait: Whether to wait for cleanup to complete
        """
        LOGGER.info("Cleaning up instances for test %s in zone %s", self.test_id, self.zone)

        # Delete all cached instances
        instance_names = list(self._cache.keys())
        for name in instance_names:
            try:
                self.terminate_instance(name, wait=wait)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to terminate instance %s: %s", name, exc)

        # Clear cache
        self._cache.clear()
        self._vm_provider.clear_cache()

        LOGGER.info("Cleanup completed for test %s", self.test_id)

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        """
        Add tags (labels) to instance.

        Args:
            name: Instance name
            tags: Tags to add
        """
        self._vm_provider.add_tags(name, tags)

        # Update cache
        if name in self._cache:
            self._cache[name].tags.update(tags)

    def run_command(self, name: str, command: str) -> Result:
        """
        Run command on instance.

        Args:
            name: Instance name
            command: Command to run

        Returns:
            Result object with command output
        """
        if name not in self._cache:
            raise ProvisionError(f"Instance {name} not found in cache")

        vm_instance = self._cache[name]

        # Create SSH connection
        ssh_login_info = {
            "hostname": vm_instance.public_ip_address or vm_instance.private_ip_address,
            "user": vm_instance.user_name,
            "key_file": f"~/.ssh/{vm_instance.ssh_key_name}",
        }

        remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)
        return remoter.run(command)

    def _gce_instance_to_vm_instance(self, gce_instance: compute_v1.Instance) -> VmInstance:
        """
        Convert a GCE Instance to a VmInstance.

        Args:
            gce_instance: GCE Instance object

        Returns:
            VmInstance object
        """
        # Extract metadata tags
        tags = {}
        user_name = "root"
        ssh_key_name = "scylla-qa-ec2"

        if gce_instance.metadata and gce_instance.metadata.items:
            for item in gce_instance.metadata.items:
                if hasattr(item, "key") and hasattr(item, "value"):
                    key = item.key or ""
                    value = item.value or ""
                else:
                    # Fallback for dict-like items
                    key = item.get("key", "") if isinstance(item, dict) else ""
                    value = item.get("value", "") if isinstance(item, dict) else ""

                # Skip special keys
                if key in ("ssh-keys", "block-project-ssh-keys", "startup-script", "user-data"):
                    continue

                tags[key] = value

        # Extract IP addresses
        public_ip = None
        private_ip = None

        if gce_instance.network_interfaces:
            interface = gce_instance.network_interfaces[0]
            private_ip = interface.network_ip

            if interface.access_configs:
                public_ip = interface.access_configs[0].nat_ip

        # Get machine type (extract simple name from full URL)
        instance_type = gce_instance.machine_type.split("/")[-1]

        # Get image (extract from boot disk)
        image = ""
        if gce_instance.disks:
            for disk in gce_instance.disks:
                if disk.boot and disk.source:
                    image = disk.source
                    break

        # Determine pricing model
        pricing_model = PricingModel.ON_DEMAND
        if gce_instance.scheduling:
            if gce_instance.scheduling.provisioning_model == "SPOT":
                pricing_model = PricingModel.SPOT

        # Get creation time
        creation_time = None
        if gce_instance.creation_timestamp:
            creation_time = datetime.fromisoformat(gce_instance.creation_timestamp.replace("Z", "+00:00"))

        return VmInstance(
            name=gce_instance.name,
            region=self.region,
            user_name=user_name,
            ssh_key_name=ssh_key_name,
            public_ip_address=public_ip,
            private_ip_address=private_ip,
            tags=tags,
            pricing_model=pricing_model,
            image=image,
            creation_time=creation_time,
            instance_type=instance_type,
            _provisioner=self,
        )
