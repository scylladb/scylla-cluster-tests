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
# Copyright (c) 2020 ScyllaDB

import os
import time
import logging
from typing import Dict, ParamSpec, TypeVar, List
from functools import cached_property, cache
from collections.abc import Callable

import tenacity
import google.api_core.exceptions
from google.cloud import compute_v1

from sdcm import cluster
from sdcm.provision.gce.provisioner import GceProvisioner
from sdcm.provision.network_configuration import ssh_connection_ip_type
from sdcm.provision.provisioner import PricingModel
from sdcm.provision.helpers.cloud_init import wait_cloud_init_completes
from sdcm.sct_provision import region_definition_builder
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.sct_events import Severity
from sdcm.sct_events.gce_events import GceInstanceEvent
from sdcm.utils.gce_utils import (
    GceLoggingClient,
    get_gce_compute_disks_client,
    wait_for_extended_operation,
    gce_private_addresses,
    gce_public_addresses,
    random_zone,
    gce_set_labels,
)
from sdcm.wait import exponential_retry
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.utils.common import list_instances_gce, gce_meta_to_dict
from sdcm.utils.decorators import retrying
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.utils.net import resolve_ip_to_dns


SPOT_TERMINATION_CHECK_DELAY = 5 * 60

LOGGER = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class CreateGCENodeError(Exception):
    pass


class GCENode(cluster.BaseNode):
    """
    Wraps GCE instances, so that we can also control the instance through SSH.
    """

    METADATA_BASE_URL = "http://metadata.google.internal/computeMetadata/v1/"

    log = LOGGER

    def __init__(
        self,
        gce_instance: compute_v1.Instance,
        gce_service: compute_v1.InstancesClient,
        credentials,
        parent_cluster,
        node_prefix: str = "node",
        node_index: int = 1,
        gce_image_username: str = "root",
        base_logdir: str = None,
        dc_idx: int = 0,
        rack: int = 0,
        gce_project: str = None,
    ):
        self._instance = gce_instance
        name = self._instance.name
        self.node_index = node_index
        self.project = gce_project
        self._instance_type = gce_instance.machine_type.split("/")[-1]
        self._gce_service = gce_service
        self._gce_logging_client = GceLoggingClient(instance_name=name, zone=self.zone)
        self._last_logs_fetch_time = 0.0
        ssh_login_info = {
            "hostname": None,
            "user": gce_image_username,
            "key_file": credentials.key_file,
            "extra_ssh_options": "-tt",
        }
        super().__init__(
            name=name,
            parent_cluster=parent_cluster,
            ssh_login_info=ssh_login_info,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            dc_idx=dc_idx,
            rack=rack,
        )

    def refresh_network_interfaces_info(self):
        pass

    @staticmethod
    def is_gce() -> bool:
        return True

    def init(self):
        self._wait_public_ip()

        # sleep 10 seconds for waiting users are added to system
        # related issue: https://github.com/scylladb/scylla-cluster-tests/issues/1121
        time.sleep(10)

        super().init()

    def wait_for_cloud_init(self):
        if self.remoter.sudo("bash -c 'command -v cloud-init'", ignore_status=True).ok:
            wait_cloud_init_completes(self.remoter, self)

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    def _set_keep_alive(self) -> bool:
        return (
            gce_set_labels(
                instances_client=self._gce_service,
                instance=self._instance,
                new_labels={"keep": "alive"},
                project=self.project,
                zone=self.zone,
            )
            and super()._set_keep_alive()
        )

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self._refresh_instance_state()
        gce_set_labels(
            instances_client=self._gce_service,
            instance=self._instance,
            new_labels={"keep": str(duration_in_hours)},
            project=self.project,
            zone=self.zone,
        )

    def _instance_wait_safe(self, instance_method: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return exponential_retry(func=lambda: instance_method(*args, **kwargs), logger=self.log)
        except tenacity.RetryError:
            raise cluster.NodeError(
                f"Timeout while running '{instance_method.__name__}' method on GCE instance '{self._instance.id}'"
            ) from None

    def _refresh_instance_state(self):
        instance = self._instance_wait_safe(
            self._gce_service.get, project=self.project, zone=self.zone, instance=self._instance.name
        )
        self._instance = instance

        ip_tuple = (gce_public_addresses(instance), gce_private_addresses(instance))
        return ip_tuple

    @property
    def network_interfaces(self):
        pass

    @property
    def vm_region(self):
        return self._instance.zone.split("/")[-1][:-2]

    @property
    def zone(self):
        return self._instance.zone.split("/")[-1]

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.scheduling.preemptible

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        There are few different methods how to detect this event in GCE:

            https://cloud.google.com/compute/docs/instances/create-start-preemptible-instance#detecting_if_an_instance_was_preempted

        We moved from internal metadata because sometimes it was missing the spot. Moved to GCE logging along with providing more
        important notifications about node state.
        """
        since = self._last_logs_fetch_time
        self._last_logs_fetch_time = time.time()
        try:
            for entry in self._gce_logging_client.get_system_events(from_=since, until=self._last_logs_fetch_time):
                match entry["protoPayload"]["methodName"]:
                    case "compute.instances.preempted":
                        self.log.warning("Got spot termination notification from GCE")
                        SpotTerminationEvent(node=self, message="Instance was preempted.").publish()
                    case "compute.instances.automaticRestart" | "compute.instances.hostError":
                        GceInstanceEvent(entry).publish()
                    case _:
                        GceInstanceEvent(entry, severity=Severity.WARNING).publish()
        except Exception as details:  # noqa: BLE001
            self.log.warning("Error during getting spot termination notification %s", details)
            self._last_logs_fetch_time = since
        return SPOT_TERMINATION_CHECK_DELAY

    def restart(self):
        # When using local_ssd disks in GCE, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance_wait_safe(
            self._gce_service.reset, instance=self._instance.name, project=self.project, zone=self.zone
        )

    def hard_reboot(self):
        self._instance_wait_safe(
            self._gce_service.reset, instance=self._instance.name, project=self.project, zone=self.zone
        )

    def _safe_destroy(self):
        try:
            operation = self._gce_service.delete(instance=self._instance.name, project=self.project, zone=self.zone)
            wait_for_extended_operation(operation, "Wait for instance deletion")
        except google.api_core.exceptions.NotFound:
            self.log.exception("Instance doesn't exist, skip destroy")

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance_wait_safe(self._safe_destroy)
        super().destroy()

    def get_console_output(self):
        # TODO adding console output from instance on GCE
        self.log.warning("Method is not implemented for GCENode")
        return ""

    def get_console_screenshot(self):
        # TODO adding console output from instance on GCE
        self.log.warning("Method is not implemented for GCENode")
        return b""

    @cache
    def _get_ipv6_ip_address(self):
        self.log.warning(
            "On GCE, VPC networks only support IPv4 unicast traffic. "
            "They do not support IPv6 traffic within the network."
        )
        return ""

    @cached_property
    def image(self):
        disk_client, _ = get_gce_compute_disks_client()
        disk = disk_client.get(disk=self._instance.disks[0].source.split("/")[-1], project=self.project, zone=self.zone)
        return disk.source_image

    def query_gce_metadata(self, path: str) -> str:
        return self.query_metadata(url=f"{self.METADATA_BASE_URL}{path}", headers={"Metadata-Flavor": "Google"})

    @cached_property
    def private_dns_name(self) -> str:
        return self.query_gce_metadata("instance/hostname")

    @cached_property
    def public_dns_name(self) -> str:
        return resolve_ip_to_dns(self.public_ip_address)


class GCECluster(cluster.BaseCluster):
    """
    Cluster of Node objects, started on GCE (Google Compute Engine).
    """

    _gce_service: compute_v1.InstancesClient

    def __init__(  # noqa: PLR0913
        self,
        gce_image,
        gce_image_type,
        gce_image_size,
        gce_network,
        gce_service,
        credentials,
        provisioners: List[GceProvisioner],
        cluster_uuid=None,
        gce_instance_type="n2-standard-1",
        gce_region_names=None,
        gce_n_local_ssd=1,
        gce_image_username="root",
        cluster_prefix="cluster",
        node_prefix="node",
        n_nodes=3,
        add_disks=None,
        params=None,
        node_type=None,
        add_nodes=True,
    ):
        self.provisioners: List[GceProvisioner] = provisioners
        self._gce_image = gce_image
        self._gce_image_type = gce_image_type
        self._gce_image_size = gce_image_size
        self._gce_network = gce_network
        self._gce_service, info = gce_service
        self.project = info["project_id"]
        self._credentials = credentials
        self._gce_instance_type = gce_instance_type
        self._gce_image_username = gce_image_username
        availability_zone = self.params.get("availability_zone")
        self._gce_zone_names: list[str] = [
            f"{region}-{availability_zone or random_zone(region)}" for region in gce_region_names
        ]
        # Keep this print out for debugging purposes: validate that zones are correctly set
        LOGGER.debug("GCE zones used: %s", self._gce_zone_names)
        self._gce_n_local_ssd = int(gce_n_local_ssd) if gce_n_local_ssd else 0
        self._add_disks = add_disks
        # the full node prefix will contain unique uuid, so use this for search of existing nodes
        self._node_prefix = node_prefix.lower()
        self._definition_builder = region_definition_builder.get_builder(params, test_config=self.test_config)
        super().__init__(
            cluster_uuid=cluster_uuid,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=gce_region_names,
            node_type=node_type,
            add_nodes=add_nodes,
        )
        self.log.debug("GCECluster constructor")

    def __str__(self):
        identifier = "GCE Cluster %s | " % self.name
        identifier += "Image: %s | " % os.path.basename(self._gce_image)
        identifier += "Root Disk: %s %s GB | " % (self._gce_image_type, self._gce_image_size)
        if self._gce_n_local_ssd:
            identifier += "Local SSD: %s | " % self._gce_n_local_ssd
        if self._add_disks:
            for disk_type, disk_size in self._add_disks.items():
                if int(disk_size):
                    identifier += "%s: %s | " % (disk_type, disk_size)
        return identifier

    def _create_instances(self, count, dc_idx=0, enable_auto_bootstrap=False, instance_type=None):
        region = self._definition_builder.regions[dc_idx]
        pricing_model = PricingModel.SPOT if "spot" in self.instance_provision else PricingModel.ON_DEMAND
        definitions = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            definitions.append(
                self._definition_builder.build_instance_definition(
                    region=region,
                    node_type=self.node_type,
                    index=node_index,
                    dc_idx=dc_idx,
                    instance_type=instance_type,
                )
            )
        return provision_instances_with_fallback(
            self.provisioners[dc_idx],
            definitions=definitions,
            pricing_model=pricing_model,
            fallback_on_demand=self.params.get("instance_provision_fallback_on_demand"),
        )

    def _destroy_instance(self, name: str, dc_idx: int):
        target_node = self._get_instances_by_name(dc_idx=dc_idx, name=name)
        operation = self._gce_service.delete(
            instance=target_node, project=self.project, zone=self._gce_zone_names[dc_idx]
        )
        wait_for_extended_operation(operation, "Wait for instance deletion")

    def _get_instances_by_prefix(self, dc_idx: int = 0):
        instances_by_zone = self._gce_service.list(project=self.project, zone=self._gce_zone_names[dc_idx])
        return [node for node in instances_by_zone if node.name.startswith(self._node_prefix)]

    def _get_instances_by_name(self, name: str, dc_idx: int = 0):
        """Search for an instance by name in the zone corresponding to dc_idx.

        The provisioner may create instances in a different zone than the cluster expects,
        so we search in the specific zone for the given dc_idx.
        """
        zone = self._gce_zone_names[dc_idx]
        all_instances = self._gce_service.aggregated_list(project=self.project)
        for zone_key, response in all_instances:
            # zone_key format is "zones/zone-name"
            if response.instances and zone in zone_key:
                for instance in response.instances:
                    if instance.name == name:
                        return instance
        return None

    @retrying(
        n=20,
        sleep_time=30,
        allowed_exceptions=(CreateGCENodeError,),
        message="Waiting for GCE instance to be available and in RUNNING state...",
        raise_on_exceeded=True,
    )
    def _get_instance_with_retry(self, name: str, dc_idx: int) -> compute_v1.Instance:
        """Fetch GCE instance by name with retry logic.

        The provisioner may take some time to create the instance and bring it to RUNNING state,
        so we retry until the instance is found and ready.
        """
        instance = self._get_instances_by_name(name=name, dc_idx=dc_idx)
        if not instance:
            raise CreateGCENodeError(f"Instance {name} not found")
        if instance.status != "RUNNING":
            raise CreateGCENodeError(f"Instance {name} is not in RUNNING state: {instance.status}")
        return instance

    def _get_instances(self, dc_idx: int) -> list[compute_v1.Instance]:
        test_id = self.test_config.test_id()
        if not test_id:
            raise ValueError("test_id should be configured for using reuse_cluster")

        instances_by_tags = list_instances_gce(tags_dict={"TestId": test_id, "NodeType": self.node_type})
        region = self._gce_zone_names[dc_idx][:-2]
        # Use ssh_connection_ip_type from params if _node_public_ips is not yet set (during __init__)
        # If _node_public_ips exists, use it; otherwise fallback to ssh_connection_ip_type
        if hasattr(self, "_node_public_ips"):
            use_public_ips = bool(self._node_public_ips)
        else:
            use_public_ips = ssh_connection_ip_type(self.params) == "public"
        ip_addresses = gce_public_addresses if use_public_ips else gce_private_addresses

        # Filter instances by ip addresses and region
        instances = [instance for instance in instances_by_tags if ip_addresses(instance) and region in instance.zone]

        # Filter out instances that are already in use by existing nodes in this cluster
        existing_node_names = {node.name for node in self.nodes}
        instances = [instance for instance in instances if instance.name not in existing_node_names]

        def sort_by_index(instance):
            metadata = gce_meta_to_dict(instance.metadata)
            return metadata.get("NodeIndex", 0)

        instances = sorted(instances, key=sort_by_index)
        return instances

    def _create_node(self, instance, node_index, dc_idx, rack):
        try:
            node = GCENode(
                gce_instance=instance,
                gce_service=self._gce_service,
                credentials=self._credentials[0],
                gce_project=self.project,
                parent_cluster=self,
                gce_image_username=self._gce_image_username,
                node_prefix=self.node_prefix,
                node_index=node_index,
                base_logdir=self.logdir,
                dc_idx=dc_idx,
                rack=rack,
            )
            node.init()
            return node
        except Exception as ex:  # noqa: BLE001
            raise CreateGCENodeError("Failed to create node: %s" % ex) from ex

    @mark_new_nodes_as_running_nemesis
    def add_nodes(self, count, ec2_user_data="", dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        if count <= 0:
            return []
        self.log.info("Adding nodes to cluster")
        nodes = []
        instance_dc = 0 if self.params.get("simulated_regions") else dc_idx
        if self.test_config.REUSE_CLUSTER:
            instances = self._get_instances(instance_dc)
            if not instances:
                raise RuntimeError("No nodes found for testId %s " % (self.test_config.test_id(),))
        else:
            self.log.info("Found no provisioned instances. Provision them.")
            provisioned_vms = self._create_instances(
                count, instance_dc, enable_auto_bootstrap, instance_type=instance_type
            )
            # The provisioner returns VmInstance objects, but GCENode expects compute_v1.Instance.
            # Fetch the native GCE instances by name with retry to ensure they are in RUNNING state.
            instances = []
            for vm in provisioned_vms:
                gce_instance = self._get_instance_with_retry(name=vm.name, dc_idx=instance_dc)
                instances.append(gce_instance)

        self.log.debug("instances: %s", instances)
        if instances:
            self.log.debug("GCE instance extra info: %s", instances[0])
        for node_index, instance in enumerate(instances, start=self._node_index + 1):
            # in case rack is not specified, spread nodes to different racks
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(instance, node_index, dc_idx, rack=node_rack)
            nodes.append(node)
            self.nodes.append(node)
            self.log.info("Added node: %s", node.name)
            node.enable_auto_bootstrap = enable_auto_bootstrap

        self._node_index += count
        self.log.info("added nodes: %s", nodes)
        return nodes


class ScyllaGCECluster(cluster.BaseScyllaCluster, GCECluster):
    def __init__(  # noqa: PLR0913
        self,
        gce_image,
        gce_image_type,
        gce_image_size,
        gce_network,
        gce_service,
        credentials,
        provisioners: List[GceProvisioner],
        gce_instance_type="n2-standard-1",
        gce_n_local_ssd=1,
        gce_image_username="centos",
        user_prefix=None,
        n_nodes=3,
        add_disks=None,
        params=None,
        gce_datacenter=None,
    ):
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "db-cluster")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "db-node")
        super().__init__(
            gce_image=gce_image,
            gce_image_type=gce_image_type,
            gce_image_size=gce_image_size,
            gce_n_local_ssd=gce_n_local_ssd,
            gce_network=gce_network,
            gce_instance_type=gce_instance_type,
            gce_image_username=gce_image_username,
            gce_service=gce_service,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            add_disks=add_disks,
            params=params,
            gce_region_names=gce_datacenter,
            node_type="scylla-db",
            provisioners=provisioners,
        )
        self.version = "2.1"

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()

    def _reuse_cluster_setup(self, node: GCENode) -> None:
        node.run_startup_script()


class LoaderSetGCE(cluster.BaseLoaderSet, GCECluster):
    def __init__(  # noqa: PLR0913
        self,
        gce_image,
        gce_image_type,
        gce_image_size,
        gce_network,
        gce_service,
        credentials,
        provisioners: List[GceProvisioner],
        gce_instance_type="n2-standard-1",
        gce_n_local_ssd=1,
        gce_image_username="centos",
        user_prefix=None,
        n_nodes=10,
        add_disks=None,
        params=None,
        gce_datacenter=None,
    ):
        node_prefix = cluster.prepend_user_prefix(user_prefix, "loader-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "loader-set")
        cluster.BaseLoaderSet.__init__(self, params=params)
        GCECluster.__init__(
            self,
            gce_image=gce_image,
            gce_network=gce_network,
            gce_image_type=gce_image_type,
            gce_image_size=gce_image_size,
            gce_n_local_ssd=gce_n_local_ssd,
            gce_instance_type=gce_instance_type,
            gce_image_username=gce_image_username,
            gce_service=gce_service,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            add_disks=add_disks,
            params=params,
            node_type="loader",
            gce_region_names=gce_datacenter,
            provisioners=provisioners,
        )


class MonitorSetGCE(cluster.BaseMonitorSet, GCECluster):
    def __init__(  # noqa: PLR0913
        self,
        gce_image,
        gce_image_type,
        gce_image_size,
        gce_network,
        gce_service,
        credentials,
        provisioners: List[GceProvisioner],
        gce_instance_type="n2-standard-1",
        gce_n_local_ssd=1,
        gce_image_username="centos",
        user_prefix=None,
        n_nodes=1,
        targets=None,
        add_disks=None,
        params=None,
        gce_datacenter=None,
        add_nodes=True,
        monitor_id=None,
    ):
        node_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-set")

        targets = targets if targets else {}
        cluster.BaseMonitorSet.__init__(
            self,
            targets=targets,
            params=params,
            monitor_id=monitor_id,
        )
        GCECluster.__init__(
            self,
            gce_image=gce_image,
            gce_image_type=gce_image_type,
            gce_image_size=gce_image_size,
            gce_n_local_ssd=gce_n_local_ssd,
            gce_network=gce_network,
            gce_instance_type=gce_instance_type,
            gce_image_username=gce_image_username,
            gce_service=gce_service,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            add_disks=add_disks,
            params=params,
            node_type="monitor",
            gce_region_names=gce_datacenter,
            add_nodes=add_nodes,
            provisioners=provisioners,
        )
