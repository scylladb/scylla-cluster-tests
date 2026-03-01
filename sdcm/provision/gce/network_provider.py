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
Network configuration provider for GCE provisioning.
"""

import logging
from typing import List

from sdcm.provision.gce.constants import (
    NETWORK_TAG_SCT_NETWORK_ONLY,
    NETWORK_TAG_SCT_ALLOW_PUBLIC,
)

LOGGER = logging.getLogger(__name__)


class NetworkProvider:
    """Provider for managing GCE network configurations."""

    def __init__(self, project_id: str, network_name: str = "default"):
        """
        Initialize the NetworkProvider.

        Args:
            project_id: GCE project ID
            network_name: Name of the VPC network to use
        """
        self.project_id = project_id
        self.network_name = network_name

    def get_network_url(self) -> str:
        """
        Get the full network URL for GCE.

        Returns:
            Full network URL
        """
        return f"projects/{self.project_id}/global/networks/{self.network_name}"

    def get_network_tags(
        self,
        allow_public_access: bool = False,
        custom_tags: List[str] = None,
    ) -> List[str]:
        """
        Get network tags for firewall rules.

        Args:
            allow_public_access: Whether to allow public internet access
            custom_tags: Additional custom network tags

        Returns:
            List of network tags
        """
        tags = []

        if allow_public_access:
            # Only add sct-allow-public tag for instances that need public access
            tags.append(NETWORK_TAG_SCT_ALLOW_PUBLIC)
        else:
            # Only add sct-network-only tag for instances that should be restricted
            tags.append(NETWORK_TAG_SCT_NETWORK_ONLY)

        if custom_tags:
            tags.extend(custom_tags)

        LOGGER.debug("Network tags: %s", tags)
        return tags

    def create_network_interface(
        self,
        network_url: str = None,
    ) -> List[dict]:
        """
        Create network interface configuration.

        Args:
            network_url: Full network URL (uses self.network_name if None)

        Returns:
            List of network interface configurations for GCE API
        """
        if network_url is None:
            network_url = self.get_network_url()

        interface = {
            "network": network_url,
            # Always add access config for external IP on GCE
            "access_configs": [
                {
                    "name": "External NAT",
                    "type_": "ONE_TO_ONE_NAT",
                }
            ],
        }

        return [interface]
