# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB

import time

from longevity_test import LongevityTest
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.cluster_tools import group_nodes_by_dc_idx
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events import Severity
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseScyllaCluster


class ScaleClusterTest(LongevityTest):
    @staticmethod
    def is_target_reached(current: list[int], target: list[int]) -> bool:
        """ Check that cluster size reached target size in each dc"""
        return all([x >= y for x, y in zip(current, target)])

    @staticmethod
    def init_nodes(db_cluster: BaseScyllaCluster):
        """
        method is required to be rewritten  to support setup large clusters.
        """
        db_cluster.set_seeds(first_only=True)
        db_cluster.wait_for_init(node_list=db_cluster.nodes, timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP)
        db_cluster.set_seeds()
        db_cluster.update_seed_provider()

    @property
    def cluster_target_size(self) -> list[int]:
        cluster_target_size = self.params.get('cluster_target_size')
        if not cluster_target_size:
            return []
        return list(map(int, cluster_target_size.split())) if isinstance(cluster_target_size, str) else [cluster_target_size]

    def grow_to_cluster_target_size(self, cluster_target_size: list[int]):
        """ Bootstrap node in each dc in each rack while cluster size less than target size"""
        nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
        current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        if self.is_target_reached(current_cluster_size, cluster_target_size):
            self.log.debug("Cluster has required size, no need to grow")
            return
        InfoEvent(
            message=f"Starting to grow cluster from {current_cluster_size} to {cluster_target_size}").publish()

        add_node_cnt = self.params.get('add_node_cnt')
        try:
            while not self.is_target_reached(current_cluster_size, cluster_target_size):
                for dcx, target in enumerate(cluster_target_size):
                    if current_cluster_size[dcx] >= target:
                        continue
                    add_nodes_num = add_node_cnt if (
                        target - current_cluster_size[dcx]) >= add_node_cnt else target - current_cluster_size[dcx]

                    for rack in range(self.db_cluster.racks_count):
                        added_nodes = []
                        InfoEvent(
                            message=f"Adding next number of nodes {add_nodes_num} to dc_idx {dcx} and rack {rack}").publish()
                        added_nodes.extend(self.db_cluster.add_nodes(
                            count=add_nodes_num, enable_auto_bootstrap=True, dc_idx=dcx, rack=rack))
                        self.monitors.reconfigure_scylla_monitoring()
                        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
                        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
                            self.db_cluster.wait_for_init(
                                node_list=added_nodes, timeout=up_timeout, check_node_health=False)
                        self.db_cluster.wait_for_nodes_up_and_normal(nodes=added_nodes)
                        InfoEvent(f"New nodes up and normal {[node.name for node in added_nodes]}").publish()
                nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
                current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        finally:
            nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
            current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
            InfoEvent(message=f"Grow cluster finished, cluster size is {current_cluster_size}").publish()

    def shrink_to_cluster_target_size(self, cluster_target_size: list[int]):
        """Decommission node in each dc in each rack while cluster size more than target size"""
        nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
        current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        if self.is_target_reached(cluster_target_size, current_cluster_size):
            self.log.debug("Cluster has required size, no need to shrink")
            return
        InfoEvent(
            message=f"Starting to shrink cluster from {current_cluster_size} to {cluster_target_size}").publish()
        try:
            nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
            while not self.is_target_reached(cluster_target_size, current_cluster_size):
                for dcx, _ in enumerate(current_cluster_size):
                    nodes_by_racks = self.db_cluster.get_nodes_per_datacenter_and_rack_idx(nodes_by_dcx[dcx])
                    for nodes in nodes_by_racks.values():
                        decommissioning_node = nodes[-1]
                        decommissioning_node.running_nemesis = "Decommissioning node"
                        self.db_cluster.decommission(node=decommissioning_node, timeout=7200)
                nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
                current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        finally:
            nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
            current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
            InfoEvent(
                message=f"Reached cluster size {current_cluster_size}").publish()

    def create_schema(self):
        number_of_table = self.params.get(
            'user_profile_table_count') or 0
        cs_user_profiles = self.params.get('cs_user_profiles')
        keyspace_num = self.params.get('keyspace_num')
        if not number_of_table and not cs_user_profiles:
            self.log.debug("User schema will not be created")
            return
        if not cs_user_profiles:
            region_dc_names = self.db_cluster.get_datacenter_name_per_region(self.db_cluster.nodes)
            replication_factor = self.db_cluster.racks_count
            InfoEvent("Create keyspace and 100 empty tables").publish()
            for i in range(1, keyspace_num + 1):
                self.create_keyspace(keyspace_name=f"testing_keyspace_{i}", replication_factor={
                    dc_name: replication_factor for dc_name in region_dc_names.values()})
                for j in range(1, number_of_table + 1):
                    self.create_table(name=f"table_{j}", keyspace_name=f"testing_keyspace_{i}")
            InfoEvent(f"{keyspace_num} Keyspaces and {number_of_table} tables were created").publish()
        else:
            self._pre_create_templated_user_schema()

    def test_grow_shrink_cluster(self):
        """
        Test allow to test cluster reaching target size by growing and shrinking.
        1. Create schema if needed
        2. Grow cluster to target size
        3. if bootstrap failed during grow, try to shrink cluster to initial size
        4. If shrink failed during step 3, just log error and finish test

        """
        nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
        init_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        InfoEvent(message=f"Cluster size is {init_cluster_size}").publish()
        self.create_schema()
        try:
            InfoEvent("Start grow cluster").publish()
            self.grow_to_cluster_target_size(self.cluster_target_size)
        except Exception as ex:  # noqa: BLE001
            self.log.error(f"Failed to grow cluster: {ex}")
            InfoEvent(f"Grow cluster failed with error: {ex}", severity=Severity.ERROR).publish()

        try:
            InfoEvent("Start shrink cluster").publish()
            self.shrink_to_cluster_target_size(init_cluster_size)
        except Exception as ex:  # noqa: BLE001
            self.log.error(f"Failed to shrink cluster: {ex}")
            InfoEvent(f"Shrink cluster failed with error: {ex}", severity=Severity.ERROR).publish()
        nodes_by_dcx = group_nodes_by_dc_idx(self.db_cluster.data_nodes)
        current_cluster_size = [len(nodes_by_dcx[dcx]) for dcx in sorted(nodes_by_dcx)]
        InfoEvent(message=f"Cluster size is {current_cluster_size}").publish()

        assert current_cluster_size == init_cluster_size, f"Cluster size {current_cluster_size} is not equal to initial {init_cluster_size}"

    def test_no_workloads_idle_custom_time(self):
        """
        The aim of test is nemesis execution without any workload
        with configured user schema during idle_duration time.
        """
        self.create_schema()
        self.grow_to_cluster_target_size(self.cluster_target_size)
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
        self.db_cluster.start_nemesis()
        duration = self.params.get('idle_duration')
        InfoEvent(f"Wait {duration} minutes while cluster resizing").publish()
        time.sleep(duration * 60)

        self.shrink_to_cluster_target_size(self.params.total_db_nodes)
        InfoEvent("Test done").publish()
