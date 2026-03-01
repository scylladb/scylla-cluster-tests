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
# Copyright (c) 2022 ScyllaDB
import abc
from dataclasses import dataclass
from functools import cache
from typing import List, Dict, Type, Any
from pathlib import Path

from sdcm.keystore import KeyStore, SSHKey
from sdcm.provision.network_configuration import ssh_connection_ip_type
from sdcm.provision.provisioner import DataDisk, InstanceDefinition
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision.common.types import NodeTypeType

from sdcm.sct_provision.user_data_objects import SctUserDataObject
from sdcm.sct_provision.user_data_objects.apt_daily_triggers import DisableAptTriggersUserDataObject
from sdcm.sct_provision.user_data_objects.scylla import ScyllaUserDataObject
from sdcm.sct_provision.user_data_objects.sshd import SshdUserDataObject
from sdcm.sct_provision.user_data_objects.syslog_ng import SyslogNgUserDataObject, SyslogNgExporterUserDataObject
from sdcm.sct_provision.user_data_objects.vector_dev import VectorDevUserDataObject
from sdcm.sct_provision.user_data_objects.walinuxagent import EnableWaLinuxAgent
from sdcm.sct_provision.user_data_objects.docker_service import DockerUserDataObject
from sdcm.sct_provision.user_data_objects.sct_agent import SctAgentUserDataObject
from sdcm.test_config import TestConfig


@dataclass
class RegionDefinition:
    """List of InstancesDefinitions and Provisioner creation attributes.

    Contains complete information needed to create instances for given region and test id"""

    backend: str
    test_id: str
    region: str
    availability_zone: str
    definitions: List[InstanceDefinition]
    provisioner_config: Dict[str, Any] = None


@dataclass
class ConfigParamsMap:
    """Maps basic params to sct configuration parameters in yaml files."""

    image_id: str
    type: str
    user_name: str
    root_disk_size: str
    local_ssd_count: str | None = None  # Maps to gce_n_local_ssd_disk_* parameters


class DefinitionBuilder(abc.ABC):
    """Class for building region/instances definitions based on test configuration.

    Builds InstanceDefinition objects based on sct configuration file mapping (SCT_PARAM_MAPPER),
    which maps sct params to proper attributes in InstanceDefinition.
    """

    BACKEND: str
    SCT_PARAM_MAPPER: Dict[NodeTypeType, ConfigParamsMap]
    REGION_MAP: str

    def __init__(self, params: SCTConfiguration, test_config: TestConfig) -> None:
        self.params = params
        self.test_config = test_config
        self.test_id = self.params.get("test_id")

    @property
    def regions(self) -> List[str]:
        return self.params.get(self.REGION_MAP)

    @abc.abstractmethod
    def instance_name(self, user_prefix, node_type_short, short_test_id, region, index, dc_idx: int = 0) -> str:
        """Generate instance name for the given parameters.

        Each backend should implement its own naming convention.

        Args:
            user_prefix: User prefix from configuration
            node_type_short: Short node type (db, loader, monitor)
            short_test_id: Shortened test ID (first 8 characters)
            region: Target region name
            index: Instance index number
            dc_idx: Datacenter index (default: 0)

        Returns:
            Formatted instance name string
        """

    def get_provisioner_config(self) -> Dict[str, Any]:
        """Return backend-specific provisioner configuration.

        Override this method in backend-specific builders to provide
        custom provisioner configuration parameters.
        """
        return {}

    def build_instance_definition(
        self, region: str, node_type: NodeTypeType, index: int, dc_idx: int = 0, instance_type: str = None
    ) -> InstanceDefinition:
        """Builds one instance definition of given type and index for given region"""
        user_prefix = self.params.get("user_prefix")
        common_tags = TestConfig.common_tags()
        node_type_short = "db" if "db" in node_type else node_type
        short_test_id = self.test_config.test_id()[:8]
        name = self.instance_name(user_prefix, node_type_short, short_test_id, region, index, dc_idx)
        action = self.params.get(f"post_behavior_{node_type_short}_nodes")
        tags = common_tags | {
            "NodeType": node_type,
            "keep_action": "terminate" if action == "destroy" else "",
            "NodeIndex": str(index),
        }
        user_data = self._get_user_data_objects(node_type=node_type, instance_name=name)
        mapper = self.SCT_PARAM_MAPPER[node_type]
        use_public_ip = ssh_connection_ip_type(self.params) == "public" or node_type == "monitor"
        local_ssd_count = self.params.get(mapper.local_ssd_count) if mapper.local_ssd_count else 0
        data_disks = []
        if local_ssd_count:
            data_disks.append(DataDisk(type="local-ssd", size=375, count=local_ssd_count))
        return InstanceDefinition(
            name=name,
            image_id=self.params.get(mapper.image_id),
            type=instance_type or self.params.get(mapper.type),
            user_name=self.params.get(mapper.user_name),
            root_disk_size=self.params.get(mapper.root_disk_size),
            data_disks=data_disks or None,
            tags=tags,
            ssh_key=self._get_ssh_key(),
            user_data=user_data,
            use_public_ip=use_public_ip,
        )

    def build_region_definition(
        self,
        region: str,
        availability_zone: str,
        n_db_nodes: int,
        n_loader_nodes: int,
        n_monitor_nodes: int,
        dc_idx: int = 0,
    ) -> RegionDefinition:
        """Builds instances definitions for given region"""
        definitions = []
        for idx in range(n_db_nodes):
            definitions.append(
                self.build_instance_definition(region=region, node_type="scylla-db", index=idx + 1, dc_idx=dc_idx)
            )
        for idx in range(n_loader_nodes):
            definitions.append(
                self.build_instance_definition(region=region, node_type="loader", index=idx + 1, dc_idx=dc_idx)
            )
        for idx in range(n_monitor_nodes):
            definitions.append(
                self.build_instance_definition(region=region, node_type="monitor", index=idx + 1, dc_idx=dc_idx)
            )
        return RegionDefinition(
            backend=self.BACKEND,
            test_id=self.test_id,
            region=region,
            availability_zone=availability_zone,
            definitions=definitions,
            provisioner_config=self.get_provisioner_config(),
        )

    def build_all_region_definitions(self) -> List[RegionDefinition]:
        """Builds all instances definitions in all regions based on SCT test configuration."""
        region_definitions = []
        availability_zone = self.params.get("availability_zone")
        n_db_nodes = self._get_node_count_for_each_region(str(self.params.get("n_db_nodes")))
        n_loader_nodes = self._get_node_count_for_each_region(str(self.params.get("n_loaders")))
        n_monitor_nodes = self._get_node_count_for_each_region(str(self.params.get("n_monitor_nodes")))

        # skip DB node provisioning for Scylla Cloud
        if self.params.get("cluster_backend") == "xcloud" or self.params.get("xcloud_provisioning_mode"):
            n_db_nodes = [0] * len(self.regions)

        for dc_idx, (region, db_nodes, loader_nodes, monitor_nodes) in enumerate(
            zip(self.regions, n_db_nodes, n_loader_nodes, n_monitor_nodes)
        ):
            region_definitions.append(
                self.build_region_definition(
                    region=region,
                    availability_zone=availability_zone,
                    n_db_nodes=db_nodes,
                    n_loader_nodes=loader_nodes,
                    n_monitor_nodes=monitor_nodes,
                    dc_idx=dc_idx,
                )
            )
        return region_definitions

    @cache
    def _get_ssh_key(self) -> SSHKey:
        return KeyStore().get_ssh_key_pair(name=Path(self.params.get("user_credentials_path")).name)

    def _get_node_count_for_each_region(self, n_str: str) -> List[int]:
        """generates node count for each region from configuration parameter string (e.g. n_db_nodes).
        When parameter string has less regions defined than regions, fills with zero for each missing region.

        E.g. regions: 'eastus westus centralus' and n_db_nodes: '2 1' - will generate [2, 1, 0] list"""
        regions = self.params.get(self.REGION_MAP)
        region_count = len(regions)
        return ([int(v) for v in str(n_str).split()] + [0] * region_count)[:region_count]

    def _get_user_data_objects(self, instance_name: str, node_type: NodeTypeType) -> List[SctUserDataObject]:
        user_data_object_classes: List[Type[SctUserDataObject]] = [
            DisableAptTriggersUserDataObject,
            SyslogNgUserDataObject,
            SyslogNgExporterUserDataObject,
            VectorDevUserDataObject,
            SshdUserDataObject,
            EnableWaLinuxAgent,
            ScyllaUserDataObject,
            DockerUserDataObject,
            SctAgentUserDataObject,
        ]
        user_data_objects = [
            klass(test_config=self.test_config, params=self.params, instance_name=instance_name, node_type=node_type)
            for klass in user_data_object_classes
        ]
        applicable_user_data_objects = [obj for obj in user_data_objects if obj.is_applicable]
        return applicable_user_data_objects


class RegionDefinitionBuilder:
    """Entry point for creation all needed information to create Provisioners and instances, based on SCT Configuration.

    Each backend must register own callable (e.g. function) which will be used when given backend is used."""

    def __init__(self) -> None:
        self._builder_classes = {}

    def register_builder(self, backend: str, builder_class: Type[DefinitionBuilder]) -> None:
        """Registers builder for given backend

        Must be used before calling RegionDefinitionBuilder for given backend."""
        self._builder_classes[backend] = builder_class

    def get_builder(self, params: SCTConfiguration, test_config: TestConfig) -> DefinitionBuilder:
        """Creates RegionDefinition for each region based on SCTConfiguration.

        Prior use, must register builder for given backend."""
        backend = params.get("cluster_backend")
        return self._builder_classes[backend](params, test_config)
