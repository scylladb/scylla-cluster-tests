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
# Copyright (c) 2021 ScyllaDB
from functools import cached_property
from typing import Optional, List, Any

from pydantic import Field

from sdcm.provision.network_configuration import is_ip_ssh_connections_ipv6
from sdcm.provision.scylla_yaml.auxiliaries import ScyllaYamlAttrBuilderBase, SeedProvider


# Disabling no-member since can't import BaseNode from 'sdcm.cluster' due to a circular import
# pylint: disable=no-member
class ScyllaYamlNodeAttrBuilder(ScyllaYamlAttrBuilderBase):
    """
    Builds scylla yaml attributes that are needed to keep node connected to the other nodes in the cluster
    """
    node: Any = Field(as_dict=False)

    @property
    def _seed_address(self) -> str:
        return ','.join(self.node.parent_cluster.seed_nodes_addresses)

    @property
    def _private_ip_address(self) -> str:
        return self.node.private_ip_address

    @property
    def _public_ip_address(self) -> str:
        return self.node.public_ip_address

    @property
    def _ipv6_ip_address(self) -> str:
        return self.node.ipv6_ip_address

    @property
    def seed_provider(self) -> Optional[List[SeedProvider]]:
        if not self._seed_address:
            return None
        return [
            SeedProvider(
                class_name='org.apache.cassandra.locator.SimpleSeedProvider',
                parameters=[{'seeds': self._seed_address}]
            )
        ]

    @cached_property
    def _is_ip_ssh_connections_ipv6(self):
        return is_ip_ssh_connections_ipv6(self.params)

    @property
    def listen_address(self) -> Optional[str]:
        if self.node.scylla_network_configuration:
            return self.node.scylla_network_configuration.listen_address

        # TODO: remove next lines when scylla_network_configuration is supported for all backends
        if self._is_ip_ssh_connections_ipv6:
            return self._ipv6_ip_address
        if self.params.get('extra_network_interface'):
            # Scylla should be listening on all interfaces
            return '0.0.0.0'
        return self._private_ip_address

    @property
    def rpc_address(self) -> Optional[str]:
        if self.node.scylla_network_configuration:
            return self.node.scylla_network_configuration.rpc_address

        # TODO: remove next lines when scylla_network_configuration is supported for all backends
        if self._is_ip_ssh_connections_ipv6:
            return self._ipv6_ip_address
        if self.params.get('extra_network_interface'):
            # Scylla should be listening on all interfaces
            return '0.0.0.0'
        return self._private_ip_address

    @property
    def broadcast_rpc_address(self) -> Optional[str]:
        if self.node.scylla_network_configuration:
            return self.node.scylla_network_configuration.broadcast_rpc_address

        # TODO: remove next lines when scylla_network_configuration is supported for all backends
        if self._is_ip_ssh_connections_ipv6:
            return self._ipv6_ip_address
        if self.params.get('extra_network_interface'):
            # Scylla should be listening on all interfaces
            return self._private_ip_address
        if self._intra_node_comm_public:
            return self._public_ip_address
        return None

    @property
    def broadcast_address(self) -> Optional[str]:
        if self.node.scylla_network_configuration:
            return self.node.scylla_network_configuration.broadcast_address

        # TODO: remove next lines when scylla_network_configuration is supported for all backends
        if self._is_ip_ssh_connections_ipv6:
            return self._ipv6_ip_address
        if self.params.get('extra_network_interface'):
            # Scylla should be listening on all interfaces
            return self._private_ip_address
        if self._intra_node_comm_public:
            return self._public_ip_address
        return None

    @property
    def enable_ipv6_dns_lookup(self) -> bool:
        return self._is_ip_ssh_connections_ipv6

    @property
    def prometheus_address(self) -> Optional[str]:
        if self._is_ip_ssh_connections_ipv6:
            return self._ipv6_ip_address
        return "0.0.0.0"
