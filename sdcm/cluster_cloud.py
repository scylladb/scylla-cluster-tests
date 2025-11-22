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

import os
import ipaddress
import logging
import re
from functools import cached_property
from types import SimpleNamespace
from typing import Any
import functools
from pathlib import Path

import requests

from sdcm import cluster, wait
from sdcm.cloud_api_client import ScyllaCloudAPIClient, CloudProviderType
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.cidr_pool import CidrPoolManager, CidrAllocationError
from sdcm.utils.cloud_api_utils import XCLOUD_VS_INSTANCE_TYPES, compute_cluster_exp_hours
from sdcm.utils.gce_region import GceRegion
from sdcm.test_config import TestConfig
from sdcm.remote import RemoteCmdRunner, shell_script_cmd
from sdcm.provision.network_configuration import ssh_connection_ip_type
from sdcm.provision.common.utils import configure_vector_target_script

LOGGER = logging.getLogger(__name__)


def format_ip_with_cidr(ip_str: str) -> str:
    """Format IP address with CIDR notation."""
    ip = ipaddress.ip_address(ip_str)
    return f"{ip_str}/32" if ip.version == 4 else f"{ip_str}/128"


def xcloud_super_if_supported(method):
    """
    Decorator for instance methods: if self.xcloud_connect_supported is True,
    call the super method with all arguments; otherwise, do nothing.

    This decorator correctly handles inheritance by calling the parent class
    of where the method is defined, not the parent of the runtime instance type.
    """
    # Extract owner class name from 'ClassName.method_name' format
    # method.__qualname__ provides the qualified name like "CloudNode.wait_ssh_up"
    # or "test_func.<locals>.CloudNode.wait_ssh_up" for nested classes
    # We parse this to get "CloudNode" - the class where the method is defined
    qualname_parts = method.__qualname__.rsplit('.', 1)
    if len(qualname_parts) == 2:
        # Extract just the class name (last component before method name)
        # e.g., "test.<locals>.CloudNode" -> "CloudNode"
        owner_class_name = qualname_parts[0].split('.')[-1]
    else:
        owner_class_name = None

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if getattr(self, 'xcloud_connect_supported', False):
            # Get the owner class from the instance's class hierarchy
            if owner_class_name:
                owner_class = None
                for cls in type(self).__mro__:
                    if cls.__name__ == owner_class_name:
                        owner_class = cls
                        break

                if owner_class:
                    # Call the parent class of the owner class
                    return getattr(super(owner_class, self), method.__name__)(*args, **kwargs)

            # Fallback for edge cases where owner class cannot be found
            # This should not happen in normal usage; log a warning if it does
            self.log.warning(
                f"Unable to find owner class '{owner_class_name}' in MRO for {method.__name__}. "
                f"Using fallback super() call which may cause issues with deep inheritance."
            )
            return getattr(super(type(self), self), method.__name__)(*args, **kwargs)

        # Skip when xcloud is not supported
        self.log.debug(f"Skip {method.__name__} on scylla-cloud, no ssh connectivity available")
        return None

    return wrapper


def download_file(url, dest, chunk_size=16384):
    """Download a file from url to dest using requests, atomically."""
    if os.path.exists(dest):
        LOGGER.debug(f"✔ Already downloaded: {dest}")
        return dest
    tmp_dest = dest + ".tmp"
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(tmp_dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        os.replace(tmp_dest, dest)
        LOGGER.debug(f"✔ Downloaded: {dest}")
        return dest
    except Exception as e:
        if os.path.exists(tmp_dest):
            os.remove(tmp_dest)
        LOGGER.error(f"Failed to download {url} to {dest}: {e}")
        raise


CLUSTER_NAME_REGEX = re.compile(r'\b[0-9a-f]{8}\b')


def extract_short_test_id_from_name(name: str) -> str | None:
    """Extract short test ID (8 hex chars) from cluster/resource name.

    Handles names like:
    - "PR-provision-test-fruch-db-cluster-3dc74f22-keep-4h" -> "3dc74f22"
    - "my-cluster-12345678" -> "12345678"
    - "simple-3dc74f22" -> "3dc74f22"

    Returns None if no test ID pattern is found.
    """
    # Pattern to match 8 hexadecimal characters (typical short UUID format)
    # This looks for 8 hex chars that are either at word boundaries or surrounded by hyphens
    if match := CLUSTER_NAME_REGEX.search(name.lower()):
        return match.group(0)

    return


VECTOR_BASE_URL = "https://packages.timber.io/vector/latest"


def download_vector_locally(arch="amd64", dest_dir="downloads"):
    """
    Download the latest vector.dev installer .deb for a given architecture.
    """
    os.makedirs(dest_dir, exist_ok=True)
    url = f"{VECTOR_BASE_URL}/vector_latest-1_{arch}.deb"
    dest = Path(dest_dir) / f"vector_latest-1_{arch}.deb"

    LOGGER.debug(f"➡ Downloading {url} -> {dest}")
    download_file(url=url, dest=str(dest))
    LOGGER.debug(f"✔ Downloaded: {dest}")
    return dest


class ScyllaCloudError(Exception):
    """Exception for Scylla Cloud related errors"""


class VpcPeeringError(ScyllaCloudError):
    """Exception for VPC peering related errors"""


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
        return self._cloud_instance_data.get('region', {}).get('name', 'unknown')

    @property
    def region(self):
        return self.vm_region

    @property
    def datacenter(self):
        return f"datacenter{self.dc_idx + 1}"

    def _get_ipv6_ip_address(self):
        return None

    def wait_for_cloud_init(self):
        pass

    def _init_port_mapping(self):
        pass

    # For cloud clusters, the keep duration is calculated and set during cluster creation.
    # _set_keep_alive and _set_keep_duration methods in base classes are invoked after cluster
    # creation, during nodes init, when it is already late to modify cluster details.
    # The basic implementations of these methods remain here for backward compatibility.
    def _set_keep_alive(self) -> bool:
        return True

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

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

    @cached_property
    def xcloud_connect_supported(self):
        localhost = TestConfig().tester_obj().localhost
        return localhost.xcloud_connect_supported(self.parent_cluster.params)

    def _init_remoter(self, ssh_login_info):
        localhost = TestConfig().tester_obj().localhost
        if localhost.xcloud_connect_supported(self.parent_cluster.params):
            ssh_login_info = localhost.xcloud_connect_get_ssh_address(node=self)
            # hardcode the fabric implementation for now, as it the only one we support right now
            self.remoter = RemoteCmdRunner(**ssh_login_info)
            self.log.debug(self.remoter.ssh_debug_cmd())
        else:
            self.log.warning("XCloud connectivity is not supported, SSH remoter is not initialized")

    @xcloud_super_if_supported
    def wait_ssh_up(self, verbose=True, timeout=500):
        pass

    @xcloud_super_if_supported
    def wait_db_up(self, verbose=True, timeout=3600):
        pass

    @xcloud_super_if_supported
    def do_default_installations(self):
        pass

    def db_up(self):
        if (self.parent_cluster.vpc_peering_enabled and
                ssh_connection_ip_type(self.parent_cluster.params) == 'public'):
            self.log.info("Skipping db_up check for node %s in VPC peering mode + public communication", self.name)
            return True
        else:
            return super().db_up()

    def configure_remote_logging(self):
        if self.xcloud_connect_supported and self.parent_cluster.params.get('logs_transport') == 'vector':
            ret = self.remoter.run("dpkg --print-architecture", retry=0)
            arch = ret.stdout.strip() if ret.return_code == 0 else "amd64"

            package_path = download_vector_locally(arch=arch)

            remote_path = f"/tmp/{os.path.basename(package_path)}"

            LOGGER.debug(f"➡ Copying {package_path}")
            self.remoter.send_files(str(package_path), remote_path)

            LOGGER.debug("➡ Installing Vector")
            ssh_cmd = f"dpkg -i {remote_path} && apt-get update && apt-get install -y vector && systemctl enable vector && systemctl start vector"
            self.remoter.sudo(shell_script_cmd(ssh_cmd), retry=0, verbose=True)
            host, port = TestConfig().get_logging_service_host_port()
            ssh_cmd = configure_vector_target_script(host=host, port=port)
            self.remoter.sudo(shell_script_cmd(ssh_cmd, quote="'"), retry=0, verbose=True)
        else:
            self.log.debug(
                "Skip configuring remote logging on scylla-cloud, for anything but vector transport")

    @xcloud_super_if_supported
    def start_coredump_thread(self):
        pass

    @staticmethod
    def is_cloud() -> bool:
        return True

    @cached_property
    def cql_address(self):
        return self._private_ip if self.parent_cluster.vpc_peering_enabled else self._public_ip

    @cached_property
    def raft(self):
        """Override BaseNode.raft property to return a dummy raft object for PoC purposes"""
        return SimpleNamespace(
            is_enabled=True,
            is_ready=lambda: True)


class CloudVSNode(CloudNode):
    """A Vector Search node running on Scylla Cloud"""

    def __init__(
        self,
        cloud_instance_data: dict[str, Any],
        parent_cluster: cluster.BaseScyllaCluster,
        node_prefix: str = 'vs-node',
        node_index: int = 1,
        base_logdir: str | None = None,
        dc_idx: int = 0,
        rack: int = 0
    ):
        super().__init__(
            cloud_instance_data=cloud_instance_data,
            parent_cluster=parent_cluster,
            node_prefix=node_prefix,
            node_index=node_index,
            base_logdir=base_logdir,
            dc_idx=dc_idx,
            rack=rack
        )


class CloudManagerNode(CloudNode):
    """A Scylla Manager node running on Scylla Cloud"""

    def __init__(
        self,
        parent_cluster: cluster.BaseScyllaCluster,
        node_prefix: str = 'manager',
        node_index: int = 1,
        base_logdir: str | None = None,
        dc_idx: int = 0,
        rack: int = 0
    ):
        # minimal cloud_instance_data for manager node (most of the node details will be fetched via SDM)
        cloud_instance_data = {
            'id': None,
            'privateIp': None,
            'publicIp': None,
            'status': 'ACTIVE',
            'instanceType': 'CloudManaged'}

        super().__init__(
            cloud_instance_data=cloud_instance_data,
            parent_cluster=parent_cluster,
            node_prefix=node_prefix,
            node_index=node_index,
            base_logdir=base_logdir,
            dc_idx=dc_idx,
            rack=rack)

    def _init_remoter(self, ssh_login_info=None):
        localhost = TestConfig().tester_obj().localhost
        if localhost.xcloud_connect_supported(self.parent_cluster.params):
            sdm_env = self.parent_cluster.params.cloud_env_credentials.get("sdm_environment")
            ssh_login_info = localhost.xcloud_connect_get_manager_ssh_address(
                cluster_id=self._cluster_id, sdm_environment=sdm_env)
            self.remoter = RemoteCmdRunner(**ssh_login_info)
        else:
            self.log.warning("XCloud connectivity is not supported, Manager node SSH remoter is not initialized")

    @property
    def node_type(self) -> str:
        return "manager"


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
        self._cloud_provider = params.get('xcloud_provider').lower()
        self._cluster_id = None
        self._cluster_request_id = None
        self._allowed_ips = allowed_ips or []

        self.vpc_peering_params = params.get('xcloud_vpc_peering')
        self.vpc_peering_enabled = self.vpc_peering_params['enabled']
        self.vpc_peering_id = None
        self.vpc_peering_details = None
        self._aws_region = None
        self._gce_region = None

        if self.vpc_peering_enabled:
            region_name = params.cloud_provider_params.get('region')
            if self._cloud_provider == 'aws':
                self._aws_region = AwsRegion(region_name)
            elif self._cloud_provider == 'gce':
                self._gce_region = GceRegion(region_name)

        self._cluster_created = False
        self._pending_node_configs = []
        self._deploy_vs_nodes = int(params.get('n_vector_store_nodes')) > 0
        self.vs_nodes = []
        self.manager_node = None

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

    @property
    def parallel_startup(self):
        # nodes provisioning and startup is managed by the Scylla Cloud itself
        return False

    @cached_property
    def provider_id(self) -> int:
        cloud_provider_type = CloudProviderType.from_sct_backend(self._cloud_provider)
        return self._api_client.cloud_provider_ids[cloud_provider_type]

    @cached_property
    def region_id(self) -> int:
        region_name = self.params.cloud_provider_params.get('region')
        return self._api_client.get_region_id_by_name(cloud_provider_id=self.provider_id, region_name=region_name)

    @cached_property
    def dc_id(self) -> int:
        return self._api_client.get_cluster_details(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)['dc']['id']

    def _resolve_cluster_id(self) -> int:
        """Resolve cluster ID from test_id for cluster reuse"""
        search_pattern = str(self.uuid)[:8]
        clusters = self._api_client.search_clusters_by_name_pattern(
            account_id=self._account_id, name_pattern=search_pattern)
        if not clusters:
            raise ScyllaCloudError(
                f"No Scylla Cloud cluster found with test_id pattern '{search_pattern}' in name.\n"
                f"Check that the cluster wasn't deleted and test_id is correct. Full test_id: {self.uuid}")

        cluster = clusters[0]
        cluster_id = cluster.get('id')
        self.log.info("Found cluster to reuse: '%s' (ID: %s, Status: %s)",
                      cluster.get('clusterName'), cluster_id, cluster.get('status'))
        return cluster_id

    def _create_node(self, cloud_instance_data: dict[str, Any], node_index: int, dc_idx: int, rack: int,
                     node_class: type[CloudNode] = CloudNode, node_prefix: str | None = None) -> CloudNode:
        try:
            node = node_class(
                cloud_instance_data=cloud_instance_data,
                parent_cluster=self,
                node_prefix=node_prefix or self.node_prefix,
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
        """Create a new cluster with the specified number of nodes or reuse an existing one"""
        if not count:
            return []

        if self.nodes:
            return self.nodes

        if self.test_config.REUSE_CLUSTER:
            self._cluster_id = self._resolve_cluster_id()
            self._reuse_existing_cluster()
            return self.nodes

        self.log.info("Adding %s nodes to Scylla Cloud cluster", count)
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
        if self._deploy_vs_nodes:
            self._wait_for_vs_nodes_ready()

        if self.vpc_peering_enabled:
            self.setup_vpc_peering(self.dc_id)

        self._get_cluster_credentials()

        nodes = self._init_nodes_from_cluster(count, dc_idx, rack)
        vs_nodes = self._init_vs_nodes_from_cluster() if self._deploy_vs_nodes else []

        if self.params.get('use_cloud_manager'):
            self._init_manager_node()

        self._cluster_created = True
        self.log.info(
            "Successfully created cluster %s with %s DB nodes%s%s",
            self.name, len(nodes),
            " and {} VS nodes".format(len(vs_nodes)) if vs_nodes else "",
            " and Manager node" if self.manager_node else "")

        return nodes

    def _init_nodes_from_data(
        self,
        nodes_data: list[dict[str, Any]],
        node_class: type[CloudNode] = CloudNode,
        node_prefix: str | None = None,
        dc_idx: int = 0,
        rack: int = 0
    ) -> list[CloudNode]:
        """Create cloud node objects from API data"""
        if not nodes_data:
            return []

        self.log.info("Initializing %s %s objects", len(nodes_data), node_class.__name__)

        return [
            self._create_node(
                cloud_instance_data=node_data,
                node_index=i,
                dc_idx=dc_idx,
                rack=rack,
                node_class=node_class,
                node_prefix=node_prefix)
            for i, node_data in enumerate(nodes_data, start=1)
        ]

    def _init_nodes_from_cluster(self, count: int, dc_idx: int, rack: int) -> list[CloudNode]:
        """Decompose the created cluster into individual CloudNode objects"""
        cluster_nodes = self._api_client.get_cluster_nodes(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)
        if not cluster_nodes:
            raise ScyllaCloudError("No nodes found in the cluster")
        if len(cluster_nodes) < count:
            self.log.warning("Expected %s nodes, but found %s", count, len(cluster_nodes))

        created_nodes = self._init_nodes_from_data(
            nodes_data=cluster_nodes[:count], node_class=CloudNode, dc_idx=dc_idx, rack=rack)
        self.nodes.extend(created_nodes)
        return created_nodes

    @property
    def vs_nodes_data(self) -> list[dict]:
        """Fetch and flatten Vector Search nodes data from Scylla Cloud API"""
        vs_info = self._api_client.get_vector_search_nodes(
            account_id=self._account_id, cluster_id=self._cluster_id, dc_id=self.dc_id)
        return [
            node for az in vs_info.get('availabilityZones', [])
            for node in az.get('nodes', [])
        ]

    def _init_vs_nodes_from_cluster(self) -> list[CloudVSNode]:
        """Decompose the created cluster VS nodes info into individual CloudVSNode objects"""
        created_nodes = self._init_nodes_from_data(
            nodes_data=self.vs_nodes_data, node_class=CloudVSNode, node_prefix='vs-node', dc_idx=0, rack=0)
        self.vs_nodes.extend(created_nodes)
        return created_nodes

    def _init_manager_node(self) -> None:
        self.log.info("Initializing Manager node for cluster %s", self._cluster_id)

        self._wait_for_manager_node_ready()
        self.manager_node = CloudManagerNode(parent_cluster=self)
        self.manager_node._init_remoter()

        if self.manager_node.xcloud_connect_supported:
            self.manager_node.wait_ssh_up()
            self.log.info("Manager node SSH connection established")

    def _prepare_cluster_config(self, node_count: int, instance_type: str) -> dict[str, Any]:
        instance_type_name = instance_type or self.params.cloud_provider_params.get('instance_type_db')
        instance_id = self._api_client.get_instance_id_by_name(
            cloud_provider_id=self.provider_id, region_id=self.region_id, instance_type_name=instance_type_name)

        allowed_ips = [format_ip_with_cidr(self._api_client.client_ip)]
        allowed_ips.extend(format_ip_with_cidr(ip) for ip in self._allowed_ips)

        broadcast_type = "PRIVATE" if self.vpc_peering_enabled else "PUBLIC"

        cidr_block = None
        if self.vpc_peering_enabled:
            try:
                cidr_manager = CidrPoolManager(
                    cidr_base=self.vpc_peering_params['cidr_pool_base'],
                    subnet_size=self.vpc_peering_params['cidr_subnet_size'])
                cidr_block = cidr_manager.get_available_cidr(
                    cloud_provider=self._cloud_provider, region=self.params.cloud_provider_params.get('region'))
                self.log.info("'%s' CIDR block is allocated for Scylla Cloud cluster %s", cidr_block, self.name)
            except CidrAllocationError as e:
                raise ScyllaCloudError(f"CIDR allocation failed: {e}") from e

        vs_config = {
            'defaultNodes': int(self.params.get('n_vector_store_nodes')),
            'defaultInstanceTypeId': XCLOUD_VS_INSTANCE_TYPES[self._cloud_provider][self.params.get('instance_type_vector_store')]
        } if self._deploy_vs_nodes else None

        expiration_hours = compute_cluster_exp_hours(
            self.test_config.TEST_DURATION, self.test_config.should_keep_alive(self.node_type))
        cluster_name = f"{self.name}-keep-{expiration_hours}h"
        self.log.info("Cluster will be created with expiration time of %s hours", expiration_hours)

        return {
            'account_id': self._account_id,
            'cluster_name': cluster_name,
            'scylla_version': self.params.get('scylla_version'),
            'cidr_block': cidr_block,
            'broadcast_type': broadcast_type,
            'allowed_ips': allowed_ips,
            'cloud_provider_id': self.provider_id,
            'region_id': self.region_id,
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
            'prom_proxy': True,
            'scaling': {},
            'vector_search': vs_config
        }

    def _wait_for_cluster_ready(self, timeout: int = 600) -> None:
        self.log.info("Waiting for Scylla Cloud cluster to be ready")

        def check_cluster_status():
            try:
                cluster_details = self._api_client.get_cluster_details(
                    account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)
                status = cluster_details.get('status', '').upper()
                prom_proxy_enabled = cluster_details.get('promProxyEnabled', False)
                self.log.debug("Cluster status: %s, prom_proxy_enabled: %s", status, prom_proxy_enabled)
                return status == 'ACTIVE' and prom_proxy_enabled
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

    def _wait_for_vs_nodes_ready(self, timeout: int = 600) -> None:
        self.log.info("Waiting for Vector Search nodes to be ready")

        def check_vs_nodes_status():
            try:
                all_nodes = self.vs_nodes_data
                if not all_nodes:
                    return False

                active_count = sum(node.get('status', '').upper() == 'ACTIVE' for node in all_nodes)
                self.log.debug("VS nodes status: %d/%d active", active_count, len(all_nodes))
                return active_count == len(all_nodes)
            except Exception as e:  # noqa: BLE001
                self.log.debug("Error checking Vector Search nodes status: %s", e)
                return False

        wait.wait_for(
            func=check_vs_nodes_status,
            step=15,
            text="Waiting for Vector Search nodes to be ready",
            timeout=timeout,
            throw_exc=True)
        self.log.info("Vector Search nodes are ready")

    def _wait_for_manager_node_ready(self, timeout: int = 600) -> None:
        self.log.info("Waiting for Manager node installation to complete")
        cluster_requests = self._api_client.get_cluster_requests(
            account_id=self._account_id, cluster_id=self._cluster_id)
        mgr_request = next((req for req in cluster_requests if req.get('requestType') == 'INSTALL_MANAGER'), None)
        if not mgr_request:
            self.log.warning("No INSTALL_MANAGER request found for cluster %s", self._cluster_id)
            return

        mgr_request_id = mgr_request.get('id')
        self.log.info("Found INSTALL_MANAGER request (ID: %s), waiting for completion", mgr_request_id)

        def is_manager_ready():
            details = self._api_client.get_cluster_request_details(
                account_id=self._account_id, request_id=mgr_request_id)
            self.log.debug("Manager installation status: %s (%s%%)",
                           details.get('status', '').upper(),
                           details.get('progressPercent', 0))
            return details.get('status', '').upper() == 'COMPLETED'

        wait.wait_for(
            func=is_manager_ready,
            step=15,
            text="Waiting for manager node installation to complete",
            timeout=timeout,
            throw_exc=True)
        self.log.info("Manager node installation completed")

    def get_promproxy_config(self):
        """Retrieve Prometheus proxy configuration for Scylla Cloud cluster"""
        return self._api_client.get_cluster_promproxy_config(
            account_id=self._account_id, cluster_id=self._cluster_id)

    def _resize_cluster(self, count, dc_idx, rack, instance_type):
        """Handle subsequent add_nodes calls using cluster resize operations"""
        self.log.info("Resizing cluster to add %s nodes", count)
        raise NotImplementedError("Not yet implemented in POC")

    def destroy(self):
        self.log.info("Destroying Scylla Cloud cluster %s", self.name)

        if self.vpc_peering_enabled and self.vpc_peering_id:
            self.cleanup_vpc_peering()

        if self._cluster_id:
            self._api_client.delete_cluster(
                account_id=self._account_id, cluster_id=self._cluster_id, cluster_name=self.name)

    def _reuse_existing_cluster(self):
        """Decompose the existing cluster into individual CloudNode objects"""
        self.log.info("Reusing existing Scylla Cloud cluster (ID: %s)", self._cluster_id)

        cluster_details = self._api_client.get_cluster_details(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)

        # validate cluster status
        self.name = cluster_details.get('clusterName', self.name)
        cluster_status = cluster_details.get('status', '').upper()
        if cluster_status != 'ACTIVE':
            raise ScyllaCloudError(f"Cluster is not in 'ACTIVE' status. Current status: '{cluster_status}'")

        # validate DB nodes config parameters
        db_nodes = self._api_client.get_cluster_nodes(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)
        if len(db_nodes) != self.params.get('n_db_nodes'):
            raise ScyllaCloudError(
                "DB node count mismatch. Update 'n_db_nodes' in test config or provision a new cluster.")
        if db_nodes[0].get('instance', {}).get('externalId') != self.params.cloud_provider_params.get('instance_type_db'):
            raise ScyllaCloudError(
                "DB node instance type mismatch. Update DB instance type in test config or provision a new cluster.")

        # validate Scylla version
        version = cluster_details.get('scyllaVersion', {}).get('version')
        expected_version = self.params.get('scylla_version')
        if version != expected_version:
            self.log.warning("Scylla version mismatch: '%s' in cluster, '%s' in test config. "
                             "Continuing with existing cluster version.", version, expected_version)

        # validate VS nodes config parameters
        vs_nodes = (self._api_client.get_vector_search_nodes(
            account_id=self._account_id, cluster_id=self._cluster_id, dc_id=self.dc_id).get('availabilityZones') or [])
        if vs_nodes:
            vs_nodes_per_az = [
                n for n in vs_nodes[0].get('nodes', []) if n.get('status') not in ('DELETED', 'PENDING_DELETE')]
            if vs_nodes_per_az and len(vs_nodes_per_az) != int(self.params.get('n_vector_store_nodes')):
                raise ScyllaCloudError("Vector Search node count mismatch. "
                                       "Update 'n_vector_store_nodes' in test config or provision a new cluster.")

        self._get_cluster_credentials()
        if self.vpc_peering_enabled:
            self._retrieve_vpc_peering_info()

        self.nodes.extend(self._init_nodes_from_data(db_nodes, CloudNode))
        if vs_nodes:
            self.vs_nodes.extend(self._init_nodes_from_data(self.vs_nodes_data, CloudVSNode, node_prefix='vs-node'))

        if self.params.get('use_cloud_manager'):
            self._init_manager_node()

        self._cluster_created = True
        self.log.info("Successfully reused cluster '%s' with %s DB nodes%s%s",
                      self.name, len(db_nodes),
                      f" and {len(vs_nodes)} VS nodes" if vs_nodes else "",
                      " and manager node" if self.manager_node else "")

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

    def _retrieve_vpc_peering_info(self) -> None:
        """Retrieve existing VPC peering information"""
        self.log.debug("Retrieving existing VPC peering info for cluster %s", self._cluster_id)
        vpc_peers = self._api_client.get_vpc_peers(account_id=self._account_id, cluster_id=self._cluster_id)
        if not vpc_peers:
            return

        if len(vpc_peers) > 1:
            self.log.warning("Multiple VPC peering connections found (%s), using the first one", len(vpc_peers))
        self.vpc_peering_id = vpc_peers[0].get('id')
        self.vpc_peering_details = self._api_client.get_vpc_peer_details(
            account_id=self._account_id, cluster_id=self._cluster_id, peer_id=self.vpc_peering_id)

    def setup_vpc_peering(self, dc_id: int) -> None:
        """Set up VPC peering connection between SCT and Scylla Cloud VPC"""
        if not self.vpc_peering_enabled:
            return

        self.log.info("Setting up Scylla Cloud VPC peering for cluster %s", self._cluster_id)
        sct_vpc_info = self._get_sct_vpc_info()

        if ipaddress.ip_network(sct_vpc_info['cidr']).overlaps(ipaddress.ip_network(self.cloud_cidr)):
            raise VpcPeeringError(
                f"SCT VPC CIDR {sct_vpc_info['cidr']} overlaps with Scylla Cloud VPC CIDR {self.cloud_cidr}")

        self.vpc_peering_id = self._api_client.create_vpc_peer(
            account_id=self._account_id,
            cluster_id=self._cluster_id,
            vpc_id=sct_vpc_info['vpc_id'],
            cidr_block=sct_vpc_info['cidr'],
            owner_id=sct_vpc_info['owner_id'],
            region_id=self.region_id,
            dc_id=dc_id,
            allow_cql=True
        ).get('id')
        self.vpc_peering_details = self._api_client.get_vpc_peer_details(
            account_id=self._account_id, cluster_id=self._cluster_id, peer_id=self.vpc_peering_id)

        if self._cloud_provider == 'aws':
            self.accept_aws_vpc_peering_connection()

        self.configure_vpc_networking()

    def _get_sct_vpc_info(self) -> dict[str, Any]:
        """Get SCT infrastructure VPC information"""
        if self._cloud_provider == 'aws':
            return {
                'vpc_id': self._aws_region.sct_vpc.vpc_id,
                'cidr': str(self._aws_region.vpc_ipv4_cidr),
                'owner_id': self._aws_region.sct_vpc.owner_id
            }
        if self._cloud_provider == 'gce':
            return {
                'vpc_id': self._gce_region.network.name,
                'cidr': self._gce_region.region_subnet.ip_cidr_range,
                'owner_id': self._gce_region.project
            }
        raise VpcPeeringError(f"Unsupported cloud provider for Scylla Cloud VPC peering: {self._cloud_provider}")

    @cached_property
    def cloud_cidr(self) -> str:
        """Get actual Cloud CIDR from cluster details"""
        return self._api_client.get_cluster_details(
            account_id=self._account_id, cluster_id=self._cluster_id, enriched=True)['dc']['cidrBlock']

    def configure_vpc_networking(self) -> None:
        """Configure routing tables for private connectivity"""
        self.log.info("Configuring %s side VPC peering %s", self._cloud_provider.upper(), self.vpc_peering_id)
        if self._cloud_provider == 'aws':
            self.configure_aws_route_tables()
        elif self._cloud_provider == 'gce':
            self._gce_region.add_network_peering(
                peering_name=self.gcp_peering_name,
                peer_project=self.vpc_peering_details.get('projectId'),
                peer_net=self.vpc_peering_details.get('networkName'),
                wait_for_active=True)
            if self._gce_region.get_peering_status(self.gcp_peering_name) != "ACTIVE":
                raise VpcPeeringError(f"GCP network peering {self.gcp_peering_name} is not ACTIVE")
        else:
            raise VpcPeeringError(f"Unsupported provider: {self._cloud_provider}")
        self.log.info("%s side VPC peering %s configuration completed",
                      self._cloud_provider.upper(), self.vpc_peering_id)

    def configure_aws_route_tables(self) -> None:
        """Configure SCT* route tables to route traffic to Scylla Cloud VPC through peering connection"""
        peering_id = self.vpc_peering_details.get('externalId')
        for route_table in self._aws_region.sct_route_tables:
            try:
                self.log.debug("Adding route for %s via %s to route table %s",
                               self.cloud_cidr, peering_id, route_table.id)
                route_table.create_route(DestinationCidrBlock=self.cloud_cidr, VpcPeeringConnectionId=peering_id)
            except Exception as e:  # noqa: BLE001
                if 'RouteAlreadyExists' in str(e):
                    self.log.debug("Route for %s already exists in route table %s", self.cloud_cidr, route_table.id)
                else:
                    raise VpcPeeringError(
                        f"Failed to add route for {self.cloud_cidr} to route table {route_table.id}: {e}") from e

    def cleanup_vpc_peering(self) -> None:
        """Clean up VPC peering connection and local network configuration"""
        if not self.vpc_peering_id:
            return

        self.log.info("Deleting Scylla Cloud VPC peering %s", self.vpc_peering_id)
        try:
            if self._cloud_provider == 'aws':
                self.cleanup_aws_route_tables()
            elif self._cloud_provider == 'gce':
                cleanup_success = self._gce_region.cleanup_vpc_peering_connection(self.gcp_peering_name)
                if not cleanup_success:
                    self.log.error("Failed to clean up GCP side network peering for peering %s", self.vpc_peering_id)
            self._api_client.delete_vpc_peer(
                account_id=self._account_id, cluster_id=self._cluster_id, peer_id=self.vpc_peering_id)
        except Exception as e:  # noqa: BLE001
            self.log.error("Error during Scylla Cloud VPC peering cleanup: %s", e)
        self.log.info("Scylla Cloud VPC peering %s cleanup completed", self.vpc_peering_id)

    def cleanup_aws_route_tables(self) -> None:
        """Remove SCT* route table entries for Scylla Cloud VPC CIDRs"""
        peering_id = self.vpc_peering_details.get('externalId')
        for route_table in self._aws_region.sct_route_tables:
            routes = route_table.routes_attribute
            for route in routes:
                if (route.get('DestinationCidrBlock') == self.cloud_cidr and
                        route.get('VpcPeeringConnectionId') == peering_id):
                    try:
                        self.log.debug("Removing route for %s from route table %s", self.cloud_cidr, route_table.id)
                        self._aws_region.client.delete_route(
                            RouteTableId=route_table.id, DestinationCidrBlock=self.cloud_cidr)
                    except Exception as e:  # noqa: BLE001
                        if 'InvalidRoute.NotFound' in str(e):
                            self.log.debug(
                                "Route for %s already removed from route table %s", self.cloud_cidr, route_table.id)
                        else:
                            self.log.warning(
                                "Failed to remove route for %s from route table %s: %s", self.cloud_cidr, route_table.id, e)

    def accept_aws_vpc_peering_connection(self) -> None:
        """Accept the VPC peering connection on SCT/AWS side"""
        peering_id = self.vpc_peering_details['externalId']
        try:
            self._aws_region.client.accept_vpc_peering_connection(VpcPeeringConnectionId=peering_id)

            def check_peering_status():
                try:
                    connections = self._aws_region.client.describe_vpc_peering_connections(
                        VpcPeeringConnectionIds=[peering_id]
                    ).get('VpcPeeringConnections', [])
                    if connections:
                        status = connections[0]['Status']['Code']
                        self.log.debug("AWS VPC peering connection %s status: %s", peering_id, status)
                        return status == 'active'
                    return False
                except Exception as e:  # noqa: BLE001
                    self.log.debug("Error checking AWS VPC peering status: %s", e)
                    return False

            wait.wait_for(
                func=check_peering_status,
                step=10,
                text=f"Waiting for AWS VPC peering connection {peering_id} to become active",
                timeout=180,
                throw_exc=True)
        except Exception as e:  # noqa: BLE001
            raise VpcPeeringError(f"Failed to accept VPC peering connection {peering_id} on AWS side: {e}") from e
        self.log.debug("VPC peering connection is accepted on AWS side")

    @cached_property
    def gcp_peering_name(self) -> str:
        return f"sct-to-scylla-cloud-{self.vpc_peering_details.get('projectId')}-{self.vpc_peering_id}"

    @property
    def scylla_manager_node(self) -> cluster.BaseNode:
        return self.manager_node
