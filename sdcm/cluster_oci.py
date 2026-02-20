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

import json
import logging
import time
from datetime import datetime
from functools import cached_property
from typing import Dict, List

from sdcm import cluster
from sdcm.nemesis.utils.node_allocator import mark_new_nodes_as_running_nemesis
from sdcm.provision.oci.provisioner import OciProvisioner
from sdcm.provision.provisioner import PricingModel, VmInstance
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.sct_provision import region_definition_builder
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.utils.decorators import retrying
from sdcm.utils.net import resolve_ip_to_dns

LOGGER = logging.getLogger(__name__)
SPOT_TERMINATION_CHECK_DELAY = 15
SPOT_TERMINATION_CHECK_OVERHEAD = 5


class CreateOciNodeError(Exception):
    pass


class OciNode(cluster.BaseNode):
    """
    Wraps OCI instances, so that we can also control the instance through SSH.
    """

    METADATA_BASE_URL = "http://169.254.169.254/opc/v2/instance/"

    log = LOGGER

    def __init__(
        self,
        oci_instance: VmInstance,
        credentials,
        parent_cluster,
        node_prefix="node",
        node_index=1,
        base_logdir=None,
        dc_idx=0,
        rack=0,
    ):
        self.node_index = node_index
        self.dc_idx = dc_idx
        self.parent_cluster = parent_cluster
        self._instance = oci_instance
        self._instance_type = oci_instance.instance_type
        name = f"{node_prefix}-{self.region}-{node_index}".lower()
        ssh_login_info = {
            "hostname": None,
            "user": oci_instance.user_name,
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

    def wait_for_cloud_init(self):
        pass

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {
            **super().tags,
            "NodeIndex": str(self.node_index),
        }

    @property
    def network_interfaces(self):
        pass

    def refresh_network_interfaces_info(self):
        pass

    @retrying(n=6, sleep_time=1)
    def _set_keep_alive(self) -> bool:
        self._instance.add_tags({"keep": "alive"})
        return super()._set_keep_alive()

    @retrying(n=6, sleep_time=1)
    def _set_keep_duration(self, duration_in_hours: int) -> None:
        self._instance.add_tags({"keep": str(duration_in_hours)})

    def _refresh_instance_state(self):
        ip_tuple = ([self._instance.public_ip_address], [self._instance.private_ip_address])
        return ip_tuple

    @property
    def vm_region(self):
        return self._instance.region

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.pricing_model.is_spot()

    def query_oci_metadata(self, path: str) -> str:
        # TODO: verify it
        return self.query_metadata(
            url=f"{self.METADATA_BASE_URL}{path}",
            headers={"Authorization": "Bearer Oracle"},
        )

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        Returns number of seconds to wait before next check.
        """
        try:
            self.wait_ssh_up(verbose=False)

            status = self.query_oci_metadata("termination-notification")
            try:
                terminate_action = json.loads(status)
            except ValueError:
                return SPOT_TERMINATION_CHECK_DELAY

            self.log.warning("Got spot termination notification from OCI %s", status)
            terminate_action_timestamp = time.mktime(
                datetime.strptime(terminate_action["timeCreated"], "%Y-%m-%dT%H:%M:%SZ").timetuple()
            )
            # OCI termination happens 120 seconds after notification
            termination_time = terminate_action_timestamp + 120
            next_check_delay = terminate_action["time-left"] = termination_time - time.time()

            SpotTerminationEvent(node=self, message=terminate_action).publish()
            return max(next_check_delay - SPOT_TERMINATION_CHECK_OVERHEAD, 0)
        except Exception as details:  # noqa: BLE001
            self.log.warning("Error during getting OCI spot termination notification: %s", details)
            return 0
        return SPOT_TERMINATION_CHECK_DELAY

    def restart(self):
        # TODO: use restart or stop/start APIs if possible
        self._instance.reboot(wait=True, hard=False)

    def hard_reboot(self):
        self._instance.reboot(wait=True, hard=True)

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance.terminate(wait=True)
        super().destroy()

    def _get_ipv6_ip_address(self):
        # TODO: implement it
        return ""

    @property
    def image(self):
        return self._instance.image

    def _get_public_ip_address(self) -> str | None:
        return self._instance.public_ip_address

    def _get_private_ip_address(self) -> str | None:
        return self._instance.private_ip_address

    def configure_remote_logging(self) -> None:
        """Remote logging configured upon vm provisioning using UserDataObject"""
        # TODO: not needed for OCI?
        return

    @cached_property
    def private_dns_name(self) -> str:
        return resolve_ip_to_dns(self.private_ip_address)


class OciCluster(cluster.BaseCluster):
    def __init__(  # noqa: PLR0913
        self,
        image_id,
        root_disk_size,
        provisioners: List[OciProvisioner],
        credentials,
        cluster_uuid=None,
        instance_type="VM.DenseIO.E4.Flex",
        region_names=None,
        user_name="root",
        cluster_prefix="cluster",
        node_prefix="node",
        n_nodes=3,
        params=None,
        node_type=None,
    ):
        self.provisioners: List[OciProvisioner] = provisioners
        self._image_id = image_id
        self._root_disk_size = root_disk_size
        self._credentials = credentials
        self._instance_type = instance_type
        self._user_name = user_name
        self._oci_region_names = region_names
        self._node_prefix = node_prefix
        self._definition_builder = region_definition_builder.get_builder(params, test_config=self.test_config)
        super().__init__(
            cluster_uuid=cluster_uuid,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=region_names,
            node_type=node_type,
        )
        self.log.debug("OciCluster constructor")

    @mark_new_nodes_as_running_nemesis
    def add_nodes(self, count, ec2_user_data="", dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        self.log.info("Adding nodes to cluster")
        nodes = []

        instance_dc = 0 if self.params.get("simulated_regions") else dc_idx
        instances = self._create_instances(count, instance_dc, instance_type=instance_type)

        self.log.debug("instances: %s", instances)
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

    def _create_node(self, instance, node_index, dc_idx, rack):
        try:
            node = OciNode(
                oci_instance=instance,
                credentials=self._credentials[0],
                parent_cluster=self,
                node_prefix=self.node_prefix,
                node_index=node_index,
                base_logdir=self.logdir,
                dc_idx=dc_idx,
                rack=rack,
            )
            node.init()
            return node
        except Exception as ex:  # noqa: BLE001
            raise CreateOciNodeError("Failed to create node: %s" % ex) from ex

    def _create_instances(self, count, dc_idx=0, instance_type=None) -> List[VmInstance]:
        region = self._definition_builder.regions[dc_idx]
        assert region, "no region provided, please add `oci_region_name` param"
        pricing_model = PricingModel.SPOT if "spot" in self.instance_provision else PricingModel.ON_DEMAND
        definitions = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            definitions.append(
                self._definition_builder.build_instance_definition(
                    region=region, node_type=self.node_type, index=node_index, instance_type=instance_type
                )
            )
        return provision_instances_with_fallback(
            self.provisioners[dc_idx],
            definitions=definitions,
            pricing_model=pricing_model,
            fallback_on_demand=self.params.get("instance_provision_fallback_on_demand"),
        )


class ScyllaOciCluster(cluster.BaseScyllaCluster, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners: List[OciProvisioner],
        credentials,
        instance_type="VM.DenseIO.E4.Flex",
        user_name="scyllaadm",
        user_prefix=None,
        n_nodes=3,
        params=None,
        region_names=None,
    ):
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "db-cluster")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "db-node")
        super().__init__(
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=region_names,
            node_type="scylla-db",
        )
        self.version = "2.1"

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()

    def _reuse_cluster_setup(self, node: OciNode) -> None:
        node.run_startup_script()


class LoaderSetOci(cluster.BaseLoaderSet, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="VM.Standard3.Flex",
        user_name="ubuntu",
        user_prefix=None,
        n_nodes=1,
        params=None,
        region_names=None,
    ):
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "loader-set")
        node_prefix = cluster.prepend_user_prefix(user_prefix, "loader-node")
        cluster.BaseLoaderSet.__init__(self, params=params)
        OciCluster.__init__(
            self,
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            node_type="loader",
            region_names=region_names,
        )


class MonitorSetOci(cluster.BaseMonitorSet, OciCluster):
    def __init__(
        self,
        image_id,
        root_disk_size,
        provisioners,
        credentials,
        instance_type="VM.Standard3.Flex",
        user_name="ubuntu",
        user_prefix=None,
        n_nodes=1,
        targets=None,
        params=None,
        region_names=None,
    ):
        node_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-node")
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, "monitor-set")
        targets = targets if targets else {}
        cluster.BaseMonitorSet.__init__(self, targets=targets, params=params)
        OciCluster.__init__(
            self,
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            node_type="monitor",
            region_names=region_names,
        )
