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
Disk configuration provider for GCE provisioning.
"""

import logging
from typing import List, Dict, Any

from sdcm.provision.gce.constants import (
    DISK_TYPE_PD_STANDARD,
    DISK_TYPE_LOCAL_SSD,
    DISK_INTERFACE_NVME,
    DEFAULT_BOOT_DISK_SIZE_GB,
)
from sdcm.provision.provisioner import DataDisk

LOGGER = logging.getLogger(__name__)


class DiskProvider:
    """Provider for creating GCE disk configurations."""

    def __init__(self, project_id: str, zone: str):
        """
        Initialize the DiskProvider.

        Args:
            project_id: GCE project ID
            zone: GCE zone (e.g., 'us-east1-b')
        """
        self.project_id = project_id
        self.zone = zone

    def _get_disk_type_url(self, disk_type: str) -> str:
        """
        Get the full disk type URL for GCE.

        Args:
            disk_type: Disk type name (e.g., 'pd-standard', 'local-ssd')

        Returns:
            Full disk type URL
        """
        return f"projects/{self.project_id}/zones/{self.zone}/diskTypes/{disk_type}"

    def create_root_disk_config(
        self,
        instance_name: str,
        image_url: str,
        disk_type: str = DISK_TYPE_PD_STANDARD,
        disk_size_gb: int = DEFAULT_BOOT_DISK_SIZE_GB,
    ) -> Dict[str, Any]:
        """
        Create root (boot) disk configuration.

        Args:
            instance_name: Name of the instance
            image_url: Full URL of the source image
            disk_type: Type of disk (pd-standard, pd-ssd, pd-balanced)
            disk_size_gb: Size of the disk in GB

        Returns:
            Disk configuration dictionary for GCE API
        """
        device_name = f"{instance_name}-root-{disk_type}"

        return {
            "boot": True,
            "auto_delete": True,
            "initialize_params": {
                "source_image": image_url,
                "disk_size_gb": disk_size_gb,
                "disk_type": self._get_disk_type_url(disk_type),
            },
            "device_name": device_name,
            "mode": "READ_WRITE",
            "type_": "PERSISTENT",
        }

    def create_local_ssd_config(
        self,
        instance_name: str,
        index: int,
        interface: str = DISK_INTERFACE_NVME,
    ) -> Dict[str, Any]:
        """
        Create local SSD disk configuration.

        Args:
            instance_name: Name of the instance
            index: Index of the local SSD (for multiple SSDs)
            interface: Interface type (NVME or SCSI)

        Returns:
            Disk configuration dictionary for GCE API
        """
        device_name = f"{instance_name}-data-local-ssd-{index}"

        return {
            "boot": False,
            "auto_delete": True,
            "type_": "SCRATCH",
            "initialize_params": {
                "disk_type": self._get_disk_type_url(DISK_TYPE_LOCAL_SSD),
            },
            "device_name": device_name,
            "interface": interface,
            "mode": "READ_WRITE",
        }

    def create_persistent_disk_config(
        self,
        instance_name: str,
        disk_type: str,
        disk_size_gb: int,
    ) -> Dict[str, Any]:
        """
        Create persistent disk configuration (non-boot data disk).

        Args:
            instance_name: Name of the instance
            disk_type: Type of disk (pd-standard, pd-ssd, pd-balanced)
            disk_size_gb: Size of the disk in GB

        Returns:
            Disk configuration dictionary for GCE API
        """
        device_name = f"{instance_name}-data-{disk_type}"

        return {
            "boot": False,
            "auto_delete": True,
            "type_": "PERSISTENT",
            "initialize_params": {
                "disk_size_gb": disk_size_gb,
                "disk_type": self._get_disk_type_url(disk_type),
            },
            "device_name": device_name,
            "mode": "READ_WRITE",
        }

    def create_disks_config(
        self,
        instance_name: str,
        image_url: str,
        root_disk_type: str = DISK_TYPE_PD_STANDARD,
        root_disk_size_gb: int = DEFAULT_BOOT_DISK_SIZE_GB,
        data_disks: List[DataDisk] = None,
    ) -> List[Dict[str, Any]]:
        """
        Create complete disk configuration for an instance.

        Args:
            instance_name: Name of the instance
            image_url: Full URL of the source image
            root_disk_type: Type of root disk
            root_disk_size_gb: Size of root disk in GB
            data_disks: List of additional data disks (including local SSDs)

        Returns:
            List of disk configurations for GCE API
        """
        disks = []

        # Add root disk
        disks.append(self.create_root_disk_config(instance_name, image_url, root_disk_type, root_disk_size_gb))

        # Add additional data disks
        if data_disks:
            for data_disk in data_disks:
                if data_disk.type == DISK_TYPE_LOCAL_SSD:
                    for i in range(data_disk.count):
                        disks.append(self.create_local_ssd_config(instance_name, i, DISK_INTERFACE_NVME))
                elif int(data_disk.size) > 0:
                    disks.append(self.create_persistent_disk_config(instance_name, data_disk.type, int(data_disk.size)))

        LOGGER.debug("Created disk configuration for %s: %s disks", instance_name, len(disks))
        return disks
