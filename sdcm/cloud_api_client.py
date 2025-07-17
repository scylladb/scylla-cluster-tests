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

import json
import logging
import requests
from enum import Enum
from functools import cached_property
from pprint import pformat
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin
from typing import Any, Literal

from urllib3.util.retry import Retry


LOGGER = logging.getLogger(__name__)


class ScyllaCloudAPIError(Exception):
    """Base exception for Scylla Cloud API errors"""


class CloudProviderType(Enum):
    AWS = 'AWS'
    GCP = 'GCP'


class ScyllaCloudAPIClient:
    """REST API client for Scylla Cloud operations"""

    exception_class: Exception = ScyllaCloudAPIError

    def __init__(self, api_url: str, auth_token: str, raise_for_status: bool = False):
        self.api_url = api_url.rstrip('/')
        self.auth_token = auth_token.strip()
        self.raise_for_status = raise_for_status
        self.session = self._create_session()

        self.session.headers.update({
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'})

        LOGGER.info("Initialized Scylla Cloud API client for %s", self.api_url)

    @staticmethod
    def _create_session(retries: int = 5) -> requests.Session:
        """Create a requests session with retry configuration"""
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"])
        adapter = HTTPAdapter(max_retries=retry_strategy)

        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def request(self, method: str, endpoint: str, /, params=None, **body) -> dict | None:
        """
        Make HTTP request to Scylla Cloud API

        :param method: HTTP method
        :param endpoint: API endpoint (relative to API endpoint URL)
        :param params: query parameters to be sent in url
        :param body: keyword args to be sent as JSON body
        """
        url = urljoin(self.api_url, endpoint.lstrip('/'))

        LOGGER.debug("Making %s request to %s with query parameters %s and payload %s",
                     method, url, pformat(params), pformat(body))
        response = self.session.request(method, url, params=params, json=body)

        if self.raise_for_status:
            response.raise_for_status()

        if not response.text:
            return None
        response = response.json()
        if error := self.get_error(response):
            raise self.exception_class(error)

        LOGGER.debug("Got response:\n%s", json.dumps(response, indent=4))
        return self._parse_response_data(response)

    @staticmethod
    def _parse_response_data(response_json: dict[str, Any]) -> dict[str, Any]:
        """Parse API response handling the top level 'data' attribute in response"""
        return response_json.get('data', response_json)

    @staticmethod
    def get_error(response: dict) -> str | None:
        """Extract error message from API response"""
        return response.get("error")

    ### Deployment related APIs ###
    def get_cloud_providers(self) -> dict[str, Any]:
        """Get details about supported cloud providers"""
        return self.request("GET", "deployment/cloud-providers")["cloudProviders"]

    def get_scylla_versions(self, defaults: bool = False) -> dict[str, Any]:
        """List available ScyllaDB Cloud versions"""
        url = "deployment/scylla-versions"
        params = {}
        if defaults:
            params['defaults'] = 'true'
        return self.request("GET", url, params=params)

    @cached_property
    def current_scylla_version(self) -> dict:
        """Get the latest ScyllaDB Cloud version that can be used to create a new cluster"""
        return next(v for v in reversed(self.get_scylla_versions()) if v["newCluster"] == "ENABLED")

    def get_regions(self, *, cloud_provider_id: int, defaults: bool = False) -> dict[str, Any]:
        """Get regions supported by a given cloud provider"""
        url = f'/deployment/cloud-provider/{cloud_provider_id}/regions'
        params = {}
        if defaults:
            params['defaults'] = 'true'
        return self.request('GET', url, params=params)

    def get_region_id_by_name(self, *, cloud_provider_id: int, region_name: str) -> int:
        """Get the ID of a cloud provider region by its name"""
        regions = self.get_regions(cloud_provider_id=cloud_provider_id)['regions']
        return next(region for region in regions if region["externalId"] == region_name)["id"]

    def get_instance_types(
            self, *, cloud_provider_id: int, region_id: int, defaults: bool = False) -> dict[str, Any]:
        """Get instance types available for a given cloud provider and region"""
        url = f'/deployment/cloud-provider/{cloud_provider_id}/region/{region_id}'
        params = {}
        if defaults:
            params['defaults'] = 'true'
        return self.request('GET', url, params=params)

    def get_instance_id_by_name(
            self, *, cloud_provider_id: int, region_id: int, instance_type_name: str) -> int:
        """Get the ID of an instance type by its name in a given cloud provider and region"""
        instance_types = self.get_instance_types(
            cloud_provider_id=cloud_provider_id, region_id=region_id)['instances']
        try:
            return next(t for t in instance_types if t["externalId"] == instance_type_name)["id"]
        except StopIteration:
            raise ScyllaCloudAPIError(
                f"Instance type '{instance_type_name}' not found in region_id: {region_id} for cloud_provider_id: "
                f"{cloud_provider_id}, available instance types: {', '.join(t['externalId'] for t in instance_types)}")

    @cached_property
    def cloud_provider_ids(self) -> dict[CloudProviderType, int]:
        """Get a mapping of cloud provider names to their IDs"""
        return {CloudProviderType(p["name"]): p["id"] for p in self.get_cloud_providers()}

    @cached_property
    def client_ip(self) -> str:
        return self.request("GET", "deployment/client-ip")["clientIp"]

    ### Account related APIs ###
    def get_active_accounts(self, *, account_id: int) -> list[dict[str, Any]]:
        """From given account list active cloud-accounts for all cloud-providers"""
        return self.request('GET', f'/account/{account_id}/cloud-account')

    def get_account_details(self) -> dict[str, Any]:
        """Get details of the account tied to the authorized user"""
        return self.request('GET', '/account/default')

    def get_current_account_id(self) -> int:
        """Get the ID of the account tied to the authorized user"""
        account_details = self.get_account_details()
        return account_details.get('accountId', 1)  # TODO: is 1 a valid default account ID?

    def set_account_notifications_email(self, *, account_id: int, emails: list[str]) -> dict[str, Any]:
        """Set the email address(es) used for account notifications"""
        return self.request('POST', f'/account/{account_id}/notifications/email', emails=emails)

    ### Cluster related APIs ###
    def get_cluster_requests(self, *, account_id: int, cluster_id: int) -> list[dict[str, Any]]:
        """list of cluster requests created in the context of a given account"""
        return self.request('GET', f'/account/{account_id}/cluster/{cluster_id}/request')

    def get_cluster_request_details(self, *, account_id: int, request_id: int) -> dict[str, Any]:
        """Get details of a specific cluster request"""
        return self.request('GET', f'/account/{account_id}/cluster/request/{request_id}')

    def create_cluster_request(  # noqa: PLR0913
        self,
        *,
        account_id: int,
        cluster_name: str,
        scylla_version: str,
        cidr_block: str | None,
        broadcast_type: Literal["PRIVATE", "PUBLIC"],
        allowed_ips: list[str],
        cloud_provider_id: int,
        region_id: int,
        instance_id: int,
        replication_factor: int,
        number_of_nodes: int,
        account_credential_id: int,
        free_trial: bool,
        user_api_interface: Literal["CQL", "ALTERNATOR"],
        enable_dns_association: bool,
        jump_start: bool,
        encryption_at_rest: dict | None,
        maintenance_windows: list[dict],
        scaling: dict[str, str],
    ) -> dict[str, Any]:
        """
        Create cluster-create request.

        :param account_id: account ID to create the cluster for
        :param cluster_name: name of the cluster
        :param scylla_version: scylla version to deploy
        :param cidr_block: CIDR block for the cluster
        :param broadcast_type: type of broadcast (PRIVATE or PUBLIC)
        :param allowed_ips: list of CIDR formatted firewall rules for the cluster
        :param cloud_provider_id: ID of the cloud provider
        :param region_id: ID of the cloud provider region
        :param instance_id: ID of the instance type
        :param replication_factor: cluster replication factor
        :param number_of_nodes: number of nodes in the cluster
        :param account_credential_id: ID of the account credential
        :param free_trial: whether to create a free tier cluster (Default: False)
        :param user_api_interface: API interface to use (CQL or ALTERNATOR)
        :param enable_dns_association: whether to enable DNS on the cluster and creation of the DNS records
            for the seed nodes at cluster creation time (Default: True)
        :param jump_start: whether to enable jump start for the cluster (Default: False)
        :param encryption_at_rest: encryption at rest configuration
        :param maintenance_windows: list of maintenance windows specifications
        :param scaling: scaling configuration

        :return: created cluster details
        """
        response = self.request(
            'POST',
            f'/account/{account_id}/cluster',
            clusterName=cluster_name,
            scyllaVersion=scylla_version,
            cidrBlock=cidr_block,
            broadcastType=broadcast_type,
            allowedIPs=allowed_ips,
            cloudProviderId=cloud_provider_id,
            regionId=region_id,
            instanceId=instance_id,
            replicationFactor=replication_factor,
            numberOfNodes=number_of_nodes,
            accountCredentialId=account_credential_id,
            freeTier=free_trial,
            userApiInterface=user_api_interface,
            enableDnsAssociation=enable_dns_association,
            jumpStart=jump_start,
            encryptionAtRest=encryption_at_rest,
            maintenanceWindows=maintenance_windows,
            scaling=scaling,)
        return self._parse_response_data(response)

    def get_clusters(self, *, account_id: int, metrics: str = '', enriched: bool = False) -> list[dict[str, Any]]:
        """List all clusters for a given account"""
        url = f'/account/{account_id}/clusters'
        params = {}
        if enriched:
            params['enriched'] = 'true'
        if metrics:
            params['metrics'] = metrics
        return self.request('GET', url, params=params)['clusters']

    def get_cluster_details(self, *, account_id: int, cluster_id: int, enriched: bool = False) -> dict[str, Any]:
        """Get details of a cluster"""
        url = f'/account/{account_id}/cluster/{cluster_id}'
        params = {}
        if enriched:
            params['enriched'] = 'true'
        return self.request('GET', url, params=params)['cluster']

    def get_cluster_id_by_name(self, *, account_id: int, cluster_name: str) -> int | None:
        if clusters := self.get_clusters(account_id=account_id):
            return next(cluster for cluster in clusters if cluster["clusterName"] == cluster_name)["id"]
        return None

    def get_cluster_nodes(self, *, account_id: int, cluster_id: int, enriched: bool = False) -> list[dict[str, Any]]:
        """Get the list of cluster nodes"""
        url = f'/account/{account_id}/cluster/{cluster_id}/nodes'
        params = {}
        if enriched:
            params['enriched'] = 'true'
        return self.request('GET', url, params=params)['nodes']

    def get_cluster_dcs(self, *, account_id: int, cluster_id: int, enriched: bool = False) -> list[dict[str, Any]]:
        """Get the list of data centers used by a cluster"""
        url = f'/account/{account_id}/cluster/{cluster_id}/dcs'
        params = {}
        if enriched:
            params['enriched'] = 'true'
        return self.request('GET', url, params=params)['dataCenters']

    def delete_cluster(self, *, account_id: int,  cluster_id: int, cluster_name: str) -> dict[str, Any]:
        """Delete a cluster"""
        return self.request('POST', f'/account/{account_id}/cluster/{cluster_id}/delete',
                            clusterName=cluster_name)

    def resize_cluster(self, *, account_id: int, cluster_id: int, dc_nodes: list[dict]) -> dict[str, Any]:
        """
       `Create cluster-create request.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster to resize
        :param dc_nodes: list of the requested changes for each DC
            [
                {
                    "dcId": int,
                    "wantedSize": int,
                    "instanceTypeId": int
                },
                ...
            ]
        """
        return self.request(
            'POST', f'/account/{account_id}/cluster/{cluster_id}/resize', dcNodes=dc_nodes)

    def update_cluster_name(self, *, account_id: int, cluster_id: int, new_name: str) -> dict[str, Any]:
        """Update the name of a cluster"""
        return self.request('PATCH', f'/account/{account_id}/cluster/{cluster_id}/name', name=new_name)

    def set_cluster_notifications_email(self, *, account_id: int, cluster_id: int, emails: list[str]) -> dict[str, Any]:
        """Set the email address(es) used for cluster notifications"""
        return self.request(
            'POST', f'/account/{account_id}/cluster/{cluster_id}/notifications/email', emails=emails)

    ### Account cluster network related APIs ###
    def create_fw_rule(self, *, account_id: int, cluster_id: int, ip_address: str) -> dict[str, Any]:
        """Create a CIDR formatted firewall rule for a given cluster"""
        return self.request('POST',
                            f'/account/{account_id}/cluster/{cluster_id}/network/firewall/allowed',
                            ipAddress=ip_address)

    def get_fw_rules(self, *, account_id: int, cluster_id: int) -> list[dict[str, Any]]:
        """Get the list of firewall rules for a given cluster"""
        return self.request(
            'GET', f'/account/{account_id}/cluster/{cluster_id}/network/firewall/allowed')

    def delete_fw_rule(self, *, account_id: int, cluster_id: int, rule_id: int) -> dict[str, Any]:
        """Delete a firewall rule in a given cluster"""
        return self.request(
            'DELETE', f'/account/{account_id}/cluster/{cluster_id}/network/firewall/allowed/{rule_id}')

    def get_vpc_peers(self, *, account_id: int, cluster_id: int) -> list[dict[str, Any]]:
        """Get list of all VPC peers for a given cluster"""
        return self.request('GET', f'/account/{account_id}/cluster/{cluster_id}/network/vpc/peer')

    def create_vpc_peer(
            self,
            *,
            account_id: int,
            cluster_id: int,
            vpc_id: str,
            cidr_block: str,
            owner_id: str,
            region_id: int,
            dc_id: int,
            allow_cql: bool,
            asynchronous: bool = False
    ) -> dict[str, Any]:
        """
        Create a VPC peer for a given cluster.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster
        :param vpc_id: ID of the VPC to peer with
        :param cidr_block: single address or list of comma separated CIDR block addresses for the VPC peer
        :param owner_id: owner ID of the VPC (AWS account ID or GCP project ID)
        :param region_id: ID of the region where the VPC is located
        :param dc_id: ID of the data center where the VPC is located
        :param allow_cql: whether to allow CQL traffic over the VPC peer
        :param asynchronous: whether to perform the operation asynchronously (Default: False)
        """
        url = f'/account/{account_id}/cluster/{cluster_id}/network/vpc/peer'
        params = {}
        if asynchronous:
            params['asynchronous'] = 'true'
        return self.request(
            'POST',
            url,
            vpcId=vpc_id,
            cidrBlock=cidr_block,
            ownerId=owner_id,
            regionId=region_id,
            dcId=dc_id,
            allowCql=allow_cql,
            params=params)

    def get_vpc_peer_details(self, *, account_id: int, cluster_id: int, peer_id: str) -> dict[str, Any]:
        """Get details of a VPC peer for a given cluster"""
        return self.request(
            'GET', f'/account/{account_id}/cluster/{cluster_id}/network/vpc/peer/{peer_id}')

    def update_vpc_peer(
            self,
            *,
            account_id: int,
            cluster_id: int,
            peer_id: str,
            status: str,
            cidr_block: str
    ) -> dict[str, Any]:
        """
        Update a VPC peer for a given cluster.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster
        :param peer_id: ID of the VPC peer to update
        :param status: new status of the VPC peer ("ACTIVE" or "INACTIVE")
        :param cidr_block: new single address or list of comma separated CIDR block addresses for the VPC peer
        """
        return self.request(
            'PUT',
            f'/account/{account_id}/cluster/{cluster_id}/network/vpc/peer/{peer_id}',
            status=status,
            cidrBlock=cidr_block)

    def delete_vpc_peer(self, *, account_id: int, cluster_id: int, peer_id: str) -> dict[str, Any]:
        """Delete a VPC peer for a given cluster"""
        return self.request(
            'DELETE', f'/account/{account_id}/cluster/{cluster_id}/network/vpc/peer/{peer_id}')

    ### Account cluster network connection related APIs ###
    def get_network_connections(
            self, *, account_id: int, cluster_id: int, type: str = '', dc: int | None = None
    ) -> list[dict[str, Any]]:
        """Get the list of network connections for a given cluster"""
        url = f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection'
        params = {}
        if type:
            params['type'] = type
        if dc:
            params['dc'] = dc
        return self.request('GET', url, params=params)

    def create_network_connection(
            self,
            *,
            account_id: int,
            cluster_id: int,
            cidr_list: list[str],
            dc_id: int,
            data: dict[str, Any],
            name: str,
            connection_type: Literal["AWS_TGW_ATTACHMENT"]
    ) -> dict[str, Any]:
        """
        Create a network connection for a given cluster.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster
        :param cidr_list: list of networks to be accessible through this connection
        :param dc_id: ID of the data center where the connection will be established
        :param data: data for the connection
            {
                "ramARN": str,  # AWS arn of the RAM
                "tgwID": str,   # ID of the TGW connection
            }
        :param name: name of the connection
        :param connection_type: type of the connection (only "AWS_TGW_ATTACHMENT" for now)
        """
        url = f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection'
        return self.request(
            'POST',
            url,
            cidrList=cidr_list,
            clusterDCID=dc_id,
            data=data,
            name=name,
            type=connection_type)

    def get_network_connection_details(
            self, *, account_id: int, cluster_id: int, connection_id: str
    ) -> dict[str, Any]:
        """Get details of a network connection for a given cluster"""
        return self.request(
            'GET', f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection/{connection_id}')

    def update_network_connection(
            self,
            *,
            account_id: int,
            cluster_id: int,
            connection_id: str,
            cidr_list: list[str],
            name: str,
            status: Literal["ACTIVE", "INACTIVE"],
    ) -> dict[str, Any]:
        """
        Update a network connection for a given cluster.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster
        :param connection_id: ID of the network connection to update
        :param cidr_list: list of networks to be accessible through this connection
        :param name: new name for the connection
        :param status: new status of the connection ("ACTIVE" or "INACTIVE")
        """
        return self.request(
            'PATCH',
            f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection/{connection_id}',
            cidrList=cidr_list,
            name=name,
            status=status)

    def recreate_network_connection(
            self,
            *,
            account_id: int,
            cluster_id: int,
            connection_id: str,
            cidr_list: list[str],
            data: dict[str, Any],
            name: str,
            status: Literal["ACTIVE", "INACTIVE"]
    ) -> dict[str, Any]:
        """Recreate a network connection for a given cluster.

        :param account_id: ID of the account
        :param cluster_id: ID of the cluster
        :param connection_id: ID of the network connection to recreate
        :param cidr_list: list of networks to be accessible through this connection
        :param data: data for the connection
            {
                "ramARN": str,  # AWS arn of the RAM
                "tgwID": str,   # ID of the TGW connection
            }
        :param name: new name for the connection
        :param status: new status of the connection ("ACTIVE" or "INACTIVE")
        """
        return self.request(
            'PUT',
            f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection/{connection_id}',
            cidrList=cidr_list,
            data=data,
            name=name,
            status=status)

    def delete_network_connection(self, *, account_id: int, cluster_id: int, connection_id: str) -> dict[str, Any]:
        """Delete a network connection for a given cluster"""
        return self.request(
            'DELETE', f'/account/{account_id}/cluster/{cluster_id}/network/vpc/connection/{connection_id}')
