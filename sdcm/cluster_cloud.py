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
# Copyright (c) 2025 ScyllaDB

import ipaddress
import logging
from functools import cached_property
from types import SimpleNamespace
from typing import Any

from sdcm import cluster, wait
from sdcm.cloud_api_client import ScyllaCloudAPIClient, CloudProviderType

LOGGER = logging.getLogger(__name__)


def format_ip_with_cidr(ip_str: str) -> str:
    """Format IP address with CIDR notation."""
    ip = ipaddress.ip_address(ip_str)
    return f"{ip_str}/32" if ip.version == 4 else f"{ip_str}/128"


class ScyllaCloudError(Exception):
    """Exception for Scylla Cloud related errors"""


class CloudNode(cluster.BaseNode):
    """A node running on Scylla Cloud"""

    METADATA_BASE_URL = None
    log = LOGGER

    def __init__(self,
                 cloud_instance_data: dict[str, Any],
                 parent_cluster: cluster.BaseScyllaCluster,
                 node_prefix: str = 'node',
                 node_index: int = 1,
                 base_logdir: str | None = None,
                 dc_idx: int = 0,
                 rack: int = 0):
        self.node_index = node_index
        self._cloud_instance_data = cloud_instance_data
        self._api_client: ScyllaCloudAPIClient = parent_cluster._api_client
        self._account_id = parent_cluster._account_id
        self._cluster_id = parent_cluster._cluster_id

        self._node_id = cloud_instance_data.get('id')
        self._instance_name = cloud_instance_data.get('name', f'{node_prefix}-{node_index}')

        self._public_ip = cloud_instance_data.get('publicIp')
        self._private_ip = cloud_instance_data.get('privateIp')

        name = f"{node_prefix}-{dc_idx}-{node_index}".lower()
        super().__init__(
            name=name,
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            dc_idx=dc_idx,
            rack=rack
        )

        instance_info = cloud_instance_data.get('instance', {})
        self._instance_type = instance_info.get('externalId', 'CloudManaged')

    @cached_property
    def network_interfaces(self):
        return [{
            'public_ip': self._public_ip,
            'private_ip': self._private_ip,
            'interface_name': 'eth0'
        }]

    def _refresh_instance_state(self):
        try:
            node_details = self._api_client.get_cluster_nodes(
                account_id=self._account_id,
                cluster_id=self._cluster_id)
            for node_data in node_details:
                if node_data.get('id') == self._node_id:
                    self._cloud_instance_data = node_data
                    self._public_ip = node_data.get('publicIp', self._public_ip)
                    self._private_ip = node_data.get('privateIp', self._private_ip)
                    break

            return [self._public_ip], [self._private_ip]
        except Exception as e:  # noqa: BLE001
            self.log.warning("Failed to refresh instance state: %s", e)
            return [self._public_ip], [self._private_ip]

    @property
    def public_ip_address(self):
        return self._public_ip

    @property
    def private_ip_address(self):
        return self._private_ip

    @property
    def ipv6_ip_address(self):
        return None

    @property
    def vm_region(self):
        return self._cloud_instance_data.get('region', 'unknown')

    @property
    def region(self):
        return self.vm_region

    @property
    def datacenter(self):
        return f"datacenter{self.dc_idx + 1}"

    def _get_ipv6_ip_address(self):
        return None

    def wait_for_cloud_init(self):
        # TODO: Implement waiting for cloud-init completion for Scylla Cloud
        self.log.debug("Skip waiting for cloud-init on scylla-cloud, no approach to SSHing nodes for now")

    def _init_port_mapping(self):
        pass

    def set_keep_alive(self):
        # TODO: Implement keep alive tagging for Scylla Cloud
        self.log.info("Setting keep alive for node %s", self.name)
        return True

    def restart(self):
        raise NotImplementedError("There is no public Scylla Cloud API for node restart.\n"
                                  "This should be implemented after approach to accessing cloud nodes is defined")

    def terminate(self):
        raise NotImplementedError("Individual node termination is not supported in Scylla Cloud.\n"
                                  "Use cluster resize operations instead.")

    def check_spot_termination(self):
        return 0  # no spot instances in Scylla Cloud

    @property
    def is_spot(self):
        return False

    @cached_property
    def tags(self) -> dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
            "CloudProvider": "scylla-cloud",
            "NodeId": str(self._node_id)
        }

    @property
    def private_dns_name(self):
        return self.private_ip_address

    @property
    def public_dns_name(self):
        return self.public_ip_address

    def _init_remoter(self, ssh_login_info):
        # TODO: Implement remoter initialization for Scylla Cloud
        self.log.debug(
            "Skip initializing remoter abstraction on scylla-cloud, pending until approach to SSHing/accessing nodes is developed")

    def wait_ssh_up(self, verbose=True, timeout=500):
        # TODO: Implement waiting for SSH up for Scylla Cloud
        self.log.debug("Skip waiting for SSH up on scylla-cloud, pending until approach to SSHing nodes is developed")

    def wait_db_up(self, verbose=True, timeout=3600):
        # TODO: Implement waiting for DB up for Scylla Cloud
        self.log.debug("Skip waiting for DB up on scylla-cloud, pending until approach to SSHing/accessing nodes is developed")

    def do_default_installations(self):
        # TODO: Implement default installations for Scylla Cloud
        self.log.debug(
            "Skip default installations on scylla-cloud, pending until approach to SSHing/accessing nodes is developed")

    def configure_remote_logging(self):
        # TODO: Implement remote logging configuration for Scylla Cloud
        self.log.debug(
            "Skip configuring remote logging on scylla-cloud, pending until API/approach to collect logs is developed")

    def start_coredump_thread(self):
        # TODO: Implement coredump thread for Scylla Cloud
        self.log.debug(
            "Skip starting coredump thread on scylla-cloud, pending until approach to SSHing/accessing nodes is developed")

    @staticmethod
    def is_cloud() -> bool:
        return True

    @cached_property
    def raft(self):
        """Override BaseNode.raft property to return a dummy raft object for PoC purposes"""
        return SimpleNamespace(
            is_enabled=True,
            is_ready=lambda: True)


class ScyllaCloudCluster(cluster.BaseScyllaCluster, cluster.BaseCluster):
    """Scylla DB cluster running on Scylla Cloud"""

    def __init__(self,
                 cloud_api_client: ScyllaCloudAPIClient,
                 user_prefix: str | None = None,
                 n_nodes: int = 3,
                 params: dict[str, Any] | None = None,
                 node_type: str = 'scylla-db',
                 add_nodes: bool = True,
                 allowed_ips: list | None = None):

        self._api_client = cloud_api_client
        self._account_id = cloud_api_client.get_current_account_id()
        self._cluster_id = None
        self._cluster_request_id = None
        self._allowed_ips = allowed_ips or []

        self._cluster_created = False
        self._pending_node_configs = []
        if self.test_config.REUSE_CLUSTER and self._cluster_id:
            self._cluster_created = True

        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        super().__init__(
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=params.cloud_provider_params.get('region'),
            node_type=node_type,
            add_nodes=add_nodes)

        if self.test_config.REUSE_CLUSTER and self._cluster_id:
            self._reuse_existing_cluster()

    @property
    def parallel_startup(self):
        # nodes provisioning and startup is managed by the Scylla Cloud itself
        return False

    def _create_node(self, cloud_instance_data: dict[str, Any], node_index: int, dc_idx: int, rack: int) -> CloudNode:
        try:
            node = CloudNode(
                cloud_instance_data=cloud_instance_data,
                parent_cluster=self,
                node_prefix=self.node_prefix,
                node_index=node_index,
                base_logdir=self.logdir,
                dc_idx=dc_idx,
                rack=rack)
            node.init()
            return node
        except Exception as e:
            raise ScyllaCloudError(f"Failed to create node: {e}") from e

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = '',
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False,
                  instance_type: str | None = None) -> list[CloudNode]:
        """
        Add nodes to cluster. For cloud backend, this creates the entire cluster
        on first call, then performs resize for subsequent calls.
        """
        if not count:
            return []

        self.log.info("Adding %s nodes to Scylla Cloud cluster", count)
        if self._cluster_created:
            return self._resize_cluster(count, dc_idx, rack, instance_type)
        return self._create_cluster(count, dc_idx, rack, enable_auto_bootstrap, instance_type)

    def _create_cluster(self,
                        count: int,
                        dc_idx: int,
                        rack: int,
                        enable_auto_bootstrap: bool,
                        instance_type: str | None) -> list[CloudNode]:
        self.log.info("Creating new Scylla Cloud cluster with %s nodes", count)
        cluster_config = self._prepare_cluster_config(count, instance_type)

        self._cluster_request_id = self._api_client.create_cluster_request(**cluster_config)['requestId']
        self._cluster_id = self._api_client.get_cluster_request_details(
            account_id=self._account_id, request_id=self._cluster_request_id)['clusterId']

        self.log.debug("Cluster creation initiated. Cluster ID: %s", self._cluster_id)
        self._wait_for_cluster_ready()

        self._get_cluster_credentials()

        nodes = self._init_nodes_from_cluster(count, dc_idx, rack)
        self._cluster_created = True
        self.log.info("Successfully created cluster %s with %s nodes", self._cluster_id, len(nodes))

        return nodes

    def _init_nodes_from_cluster(self, count: int, dc_idx: int, rack: int) -> list[CloudNode]:
        """Decompose the created cluster into individual CloudNode objects"""
        cluster_nodes = self._api_client.get_cluster_nodes(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)
        if not cluster_nodes:
            raise ScyllaCloudError("No nodes found in the cluster")
        if len(cluster_nodes) < count:
            self.log.warning("Expected %s nodes, but found %s", count, len(cluster_nodes))

        self.log.info("Initializing %s individual CloudNode node objects", count)
        created_nodes = []
        for i, node_data in enumerate(cluster_nodes[:count]):
            self._node_index += 1
            node_rack = self._node_index % self.racks_count if rack is None else rack

            node = self._create_node(
                cloud_instance_data=node_data,
                node_index=self._node_index,
                dc_idx=dc_idx,
                rack=node_rack)
            self.nodes.append(node)
            created_nodes.append(node)

            self.log.info("Created node %s with public IP: %s", node.name, node.public_ip_address)
        return created_nodes

    def _prepare_cluster_config(self, node_count: int, instance_type: str) -> dict[str, Any]:
        cloud_provider_type = CloudProviderType.from_sct_backend(self.params.get('xcloud_provider'))

        provider_id = self._api_client.cloud_provider_ids[cloud_provider_type]

        region_name = self.params.cloud_provider_params.get('region')
        region_id = self._api_client.get_region_id_by_name(
            cloud_provider_id=provider_id, region_name=region_name)

        instance_type_name = instance_type or self.params.cloud_provider_params.get('instance_type_db')
        instance_id = self._api_client.get_instance_id_by_name(
            cloud_provider_id=provider_id, region_id=region_id, instance_type_name=instance_type_name)

        allowed_ips = [format_ip_with_cidr(self._api_client.client_ip)]
        allowed_ips.extend(format_ip_with_cidr(ip) for ip in self._allowed_ips)

        return {
            'account_id': self._account_id,
            'cluster_name': self.name,
            'scylla_version': self.params.get('scylla_version'),
            'cidr_block': None,
            'broadcast_type': "PUBLIC",
            'allowed_ips': allowed_ips,
            'cloud_provider_id': provider_id,
            'region_id': region_id,
            'instance_id': instance_id,
            'replication_factor': self.params.get('xcloud_replication_factor'),
            'number_of_nodes': node_count,
            'account_credential_id': self.params.get('xcloud_credential_id'),
            'free_trial': False,
            'user_api_interface': "CQL",
            'enable_dns_association': True,
            'jump_start': False,
            'encryption_at_rest': None,
            'maintenance_windows': [],
            'scaling': {}
        }

    def _wait_for_cluster_ready(self, timeout: int = 600) -> None:
        self.log.info("Waiting for Scylla Cloud cluster to be ready")

        def check_cluster_status():
            try:
                cluster_details = self._api_client.get_cluster_details(
                    account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)
                status = cluster_details.get('status', '').upper()
                self.log.debug("Cluster status: %s", status)
                return status == 'ACTIVE'
            except Exception as e:  # noqa: BLE001
                self.log.debug("Error checking cluster status: %s", e)
                return False

        wait.wait_for(
            func=check_cluster_status,
            step=15,
            text="Waiting for cluster to be ready",
            timeout=timeout,
            throw_exc=True)
        self.log.info("Scylla Cloud cluster is ready")

    def _resize_cluster(self, count, dc_idx, rack, instance_type):
        """Handle subsequent add_nodes calls using cluster resize operations"""
        self.log.info("Resizing cluster to add %s nodes", count)
        raise NotImplementedError("Not yet implemented in POC")

    def destroy(self):
        self.log.info("Destroying Scylla Cloud cluster %s", self.name)
        if self._cluster_id:
            self._api_client.delete_cluster(
                account_id=self._account_id, cluster_id=self._cluster_id, cluster_name=self.name)

    def _reuse_existing_cluster(self):
        """Decompose the existing cluster into individual CloudNode objects"""
        self.log.info("Reusing existing Scylla Cloud cluster: %s", self._cluster_id)
        try:
            cluster_details = self._api_client.get_cluster_details(
                account_id=self._account_id, cluster_id=self._cluster_id)
            self.name = cluster_details.get('clusterName', self.name)
            self.log.info("Found existing cluster: %s", self.name)

            cluster_nodes = self._api_client.get_cluster_nodes(
                account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)

            self.log.debug("Reusing %s nodes from existing cluster", len(cluster_nodes))
            for i, node_data in enumerate(cluster_nodes):
                node_index = i + 1
                rack = node_index % self.racks_count
                node = self._create_node(
                    cloud_instance_data=node_data,
                    node_index=node_index,
                    dc_idx=0,
                    rack=rack)
                self.nodes.append(node)
            self._cluster_created = True
        except Exception as e:
            raise ScyllaCloudError(f"Failed to reuse cluster: {e}") from e

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        pass

    def node_startup(self, node, verbose=False, timeout=3600):
        self.log.info("Starting up Scylla Cloud node %s", node.name)
        node.wait_db_up(verbose=verbose, timeout=timeout)

    def node_setup(self, node, verbose=False, timeout=3600):
        self.log.info("Setting up Scylla Cloud node %s", node.name)
        node.wait_ssh_up(verbose=verbose, timeout=timeout)

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, check_node_health=True):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)

    def update_seed_provider(self):
        # TODO: Implement seed provider update logic for Scylla Cloud
        self.log.debug(
            "Skip updating seed provider on Scylla Cloud, pending until approach to SSHing/accessing nodes is developed")

    def validate_seeds_on_all_nodes(self):
        # TODO: Implement seed validation logic for Scylla Cloud
        self.log.debug("Skip validating seeds on Scylla Cloud, pending until approach to SSHing/accessing nodes is developed")

    def start_nemesis(self, interval=None, cycles_count: int = -1):
        # TODO: Enable nemesis start for Scylla Cloud
        self.log.info('Skip starting nemesis on Scylla Cloud')

    def check_nodes_up_and_normal(self, nodes=None, verification_node=None):
        """Checks via Scylla Cloud API that nodes are in ACTIVE/NORMAL state"""
        if not nodes:
            nodes = self.nodes

        cluster_nodes = self._api_client.get_cluster_nodes(account_id=self._account_id, cluster_id=self._cluster_id)
        api_nodes_by_id = {node_data['id']: node_data for node_data in cluster_nodes}

        down_nodes = []
        for node in nodes:
            api_node_data = api_nodes_by_id.get(node._node_id)
            node_status = api_node_data.get('status', '').upper()
            node_state = api_node_data.get('state', '').upper()
            self.log.debug(f"Node {node.name}: status={node_status}, state={node_state}")

            if node_status != 'ACTIVE' or node_state != 'NORMAL':
                down_nodes.append(node)

        if down_nodes:
            raise cluster.ClusterNodesNotReady(
                f"Nodes {','.join([node.name for node in down_nodes])} are not in ACTIVE/NORMAL state")

    def _get_cluster_credentials(self):
        """Retrieve default credentials for password authentification"""
        connection_details = self._api_client.get_cluster_connection(
            account_id=self._account_id, cluster_id=self._cluster_id)

        creds = connection_details.get('credentials', {})
        username = creds.get('username')
        password = creds.get('password')
        if username and password:
            self.params.update({
                'authenticator_user': username,
                'authenticator_password': password
            })
        else:
            self.log.error("No default username/password found in cluster connection details")

    def get_node_ips_param(self, public_ip=True):
        return 'xcloud_nodes_public_ip' if public_ip else 'xcloud_nodes_private_ip'
