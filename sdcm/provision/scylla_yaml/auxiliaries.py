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


import logging
from typing import Literal, List, Union, Optional

from pydantic import Field, validator, BaseModel  # pylint: disable=no-name-in-module

from sdcm.provision.common.builders import AttrBuilder
from sdcm.sct_config import SCTConfiguration


SEED_PROVIDERS = [
    'org.apache.cassandra.locator.SimpleSeedProvider',
    'org.apache.cassandra.locator.GossipingPropertyFileSnitch',
    'org.apache.cassandra.locator.Ec2Snitch',
    'org.apache.cassandra.locator.Ec2MultiRegionSnitch',
    'org.apache.cassandra.locator.GoogleCloudSnitch',
    'org.apache.cassandra.locator.RackInferringSnitch',
    'SimpleSeedProvider',
    'GossipingPropertyFileSnitch',
    'Ec2Snitch',
    'Ec2MultiRegionSnitch',
    'GoogleCloudSnitch',
    'RackInferringSnitch',
]
SASLAUTHD_AUTHENTICATOR = 'com.scylladb.auth.SaslauthdAuthenticator'
LOGGER = logging.getLogger(__file__)


class SeedProvider(BaseModel):  # pylint: disable=too-few-public-methods
    class_name: Literal[
        'org.apache.cassandra.locator.SimpleSeedProvider',
        'org.apache.cassandra.locator.GossipingPropertyFileSnitch',
        'org.apache.cassandra.locator.Ec2Snitch',
        'org.apache.cassandra.locator.Ec2MultiRegionSnitch',
        'org.apache.cassandra.locator.GoogleCloudSnitch',
        'org.apache.cassandra.locator.RackInferringSnitch',
        'SimpleSeedProvider',
        'GossipingPropertyFileSnitch',
        'Ec2Snitch',
        'Ec2MultiRegionSnitch',
        'GoogleCloudSnitch',
        'RackInferringSnitch',
    ]
    parameters: List[dict] = None

    # pylint: disable=no-self-argument,no-self-use
    @validator("class_name", pre=True, always=True)
    def set_class_name(cls, class_name: str):
        if class_name.startswith('org.apache.cassandra.locator.'):
            return class_name
        return 'org.apache.cassandra.locator.' + class_name


class ServerEncryptionOptions(BaseModel):  # pylint: disable=too-few-public-methods
    internode_encryption: Literal['all', 'none', 'dc', 'rack'] = 'none'
    certificate: str = 'conf/scylla.crt'
    keyfile: str = 'conf/scylla.key'
    truststore: str = None
    priority_string: str = None
    require_client_auth: bool = False


class ClientEncryptionOptions(BaseModel):  # pylint: disable=too-few-public-methods
    enabled: bool = False
    certificate: str = 'conf/scylla.crt'
    keyfile: str = 'conf/scylla.key'
    truststore: str = None
    priority_string: str = None
    require_client_auth: bool = False


class RequestSchedulerOptions(BaseModel):  # pylint: disable=too-few-public-methods
    throttle_limit: int = None
    default_weight: int = 1
    weights: int = 1


EndPointSnitchType = Literal[
    'org.apache.cassandra.locator.SimpleSnitch',
    'org.apache.cassandra.locator.GossipingPropertyFileSnitch',
    'org.apache.cassandra.locator.PropertyFileSnitch',
    'org.apache.cassandra.locator.Ec2Snitch',
    'org.apache.cassandra.locator.Ec2MultiRegionSnitch',
    'org.apache.cassandra.locator.RackInferringSnitch',
    'org.apache.cassandra.locator.GoogleCloudSnitch',
    'org.apache.cassandra.locator.AzureSnitch',
    'SimpleSnitch',
    'GossipingPropertyFileSnitch',
    'PropertyFileSnitch',
    'Ec2Snitch',
    'Ec2MultiRegionSnitch',
    'RackInferringSnitch',
    'GoogleCloudSnitch',
    'AzureSnitch'
]


class ScyllaYamlAttrBuilderBase(AttrBuilder):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)

    @property
    def _cluster_backend(self) -> str:
        return self.params.get('cluster_backend')

    @property
    def _cloud_provider(self) -> Literal['aws', 'gce', 'azure', None]:
        for provider in ['aws', 'gce', 'azure']:
            if provider in self._cluster_backend:
                return provider
        if self._cluster_backend == "k8s-eks":
            return 'aws'
        if self._cluster_backend == "k8s-gke":
            return 'gce'
        return None

    @property
    def _regions(self) -> List[str]:
        if self._cloud_provider == 'aws':
            regions = self.params.get('region_name')
        elif self._cloud_provider == 'gce':
            regions = self.params.get('gce_datacenter')
        elif self._cloud_provider == 'azure':
            regions = self.params.get('region_name')
        else:
            regions = []
        if isinstance(regions, list):
            return regions
        if isinstance(regions, str):
            return regions.split()
        else:
            return []

    @property
    def _multi_region(self) -> bool:
        """
        Analog of TestConfig.MULTI_REGION
        """
        if not self._regions:
            return False
        return len(self._regions) > 1  # pylint: disable=no-member

    @property
    def _intra_node_comm_public(self) -> bool:
        """
        Analog of TestConfig.INTRA_NODE_COMM_PUBLIC
        """
        return self.params.get('intra_node_comm_public') or self._multi_region

    @property
    def _authenticator(self) -> Optional[str]:
        return self.params.get('authenticator')

    @property
    def _is_authenticator_valid(self) -> bool:
        return self._authenticator in ['AllowAllAuthenticator', 'PasswordAuthenticator', SASLAUTHD_AUTHENTICATOR]

    @property
    def _authorizer(self) -> Optional[str]:
        return self.params.get('authorizer')

    @property
    def _is_ip_ssh_connections_ipv6(self):
        if self.params.get('ip_ssh_connections') == 'ipv6':
            return True
        return False

    @property
    def _default_endpoint_snitch(self) -> Literal[
        'org.apache.cassandra.locator.Ec2MultiRegionSnitch',
            'org.apache.cassandra.locator.GossipingPropertyFileSnitch']:
        if self._cluster_backend == 'aws':
            return 'org.apache.cassandra.locator.Ec2MultiRegionSnitch'
        return 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'
