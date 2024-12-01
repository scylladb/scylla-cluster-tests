import time
from functools import partial

from full_storage_utilization_test import FullStorageUtilizationTest
from sdcm.argus_results import disk_usage_to_argus, timer_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.sct_events.system import InfoEvent
from cassandra.cluster import Session
from sdcm.utils.disk import get_cluster_disk_usage
from sdcm.utils.features import is_tablets_feature_enabled
from sdcm.utils.replication_strategy_utils import NetworkTopologyReplicationStrategy
from sdcm.utils.tablets.common import wait_for_tablets_balanced


class FullStorageUtilizationTest2(FullStorageUtilizationTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_removal_action = self.params.get('data_removal_action')
        self.scale_out_instance_type = self.params.get('scale_out_instance_type')
        self.scale_out_n_nodes = self.params.get("scale_out_n_nodes")
        # transform to list
        if self.scale_out_n_nodes is not None:
            self.scale_out_n_nodes = [self.scale_out_n_nodes] if isinstance(self.scale_out_n_nodes, int) else [
                int(i) for i in str(self.scale_out_n_nodes).split()]
        self.keyspaces = []
        self.timer_results_to_argus = partial(timer_results_to_argus, argus_client=self.test_config.argus_client())
        self.initial_dcs = []

    def get_total_free_space(self):
        free = 0
        for node in self.db_cluster.nodes:
            info = self.get_disk_info(node)
            free += info["available"]

        return free

    def get_keyspaces(self, prefix: str):
        rows = self.execute_cql(f"SELECT keyspace_name FROM system_schema.keyspaces")
        return [row.keyspace_name for row in rows if row.keyspace_name.startswith(prefix)]

    def execute_cql(self, query):
        node = self.db_cluster.nodes[0]
        self.log.info(f"Executing {query}")
        with self.db_cluster.cql_connection_patient(node) as session:
            session: Session
            results = session.execute(query, timeout=3600)

        return results

    def remove_data_from_cluster(self, keyspace: str):
        table = "standard1"
        match self.data_removal_action:
            case "drop":
                self.execute_cql(f"DROP TABLE {keyspace}.{table}")
            case "truncate":
                self.execute_cql(f"TRUNCATE TABLE {keyspace}.{table}")
            case "expire":
                raise NotImplementedError("TTL not implemented")
            case _:
                raise ValueError(f"data_removal_action={self.data_removal_action} is not supported!")

    def scale_out(self):
        if len(self.scale_out_n_nodes) == 1:
            # TODO: Find out why we get Critical Error
            # Only happens when adding nodes to another cluster
            # Stress command completed with bad status 1: Failed to connect over JMX; not collecting these stats
            # java.io.IOException: Operation x0 on key(s) [4b3132355032384c4b30]: Data returned was not validated
            self.start_throttle_rw()
        self.log.info("Started scale out")
        start_time = time.time()
        self.add_new_node()
        duration = time.time() - start_time
        self.log.info(f"Scale out finished with time: {duration}")

    def add_new_node(self):
        self.initial_dcs = set(node.datacenter for node in self.db_cluster.nodes)
        with self.timer_results_to_argus("Add new node(s)"):
            new_nodes: list[BaseNode] = []
            for dc_idx, n_nodes in enumerate(self.scale_out_n_nodes):
                new_nodes += self.db_cluster.add_nodes(count=n_nodes, enable_auto_bootstrap=True,
                                                       dc_idx=dc_idx, instance_type=self.scale_out_instance_type)
            self.db_cluster.wait_for_init(node_list=new_nodes)
            self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)
            self.monitors.reconfigure_scylla_monitoring()

        InfoEvent(message=f"New node(s) added").publish()
        self.log.info(f"New node(s) added, total nodes in cluster: {len(self.db_cluster.nodes)}")

        with self.timer_results_to_argus("New node(s) ready"):
            self.update_cluster(new_nodes)
            wait_for_tablets_balanced(self.db_cluster.nodes[0])
        InfoEvent(message=f"New node(s) ready").publish()

    def update_cluster(self, new_nodes: list[BaseNode]):
        """
        Update the cluster's configuration if needed

        https://enterprise.docs.scylladb.com/stable/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc.html
        """
        if len(self.initial_dcs) != 1:
            # already have a setup with multiple dcs
            return
        if new_dcs := set(node.datacenter for node in self.db_cluster.nodes) - self.initial_dcs:
            new_dc = list(new_dcs)[0]
            old_dc = list(self.initial_dcs)[0]
        else:
            # added nodes were in the same dc
            return

        self.reconfigure_keyspaces(old_dc, new_dc)
        self.rebuild_new_nodes(new_nodes)
        self.full_cluster_repair()

    def full_cluster_repair(self):
        self.log.info("Running repair on all nodes")
        for node in self.db_cluster.nodes:
            node.run_nodetool(sub_cmd="repair -pr", publish_event=True)

    def rebuild_new_nodes(self, new_nodes: list[BaseNode]):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            if is_tablets_feature_enabled(session):
                # with tablets, no need to rebuild
                return

        status = self.db_cluster.get_nodetool_status()
        for node in new_nodes:
            self.log.info("Running rebuild on each node in new DC")
            node.run_nodetool(sub_cmd=f"rebuild -- {list(status.keys())[0]}", publish_event=True)

    def reconfigure_keyspaces(self, old_dc: str, new_dc: str):
        system_keyspaces = ["system_distributed", "system_traces"]
        # auth-v2 is used when consistent topology is enabled
        if not self.db_cluster.nodes[0].raft.is_consistent_topology_changes_enabled:
            system_keyspaces.insert(0, "system_auth")

        for rf in range(1, self.scale_out_n_nodes[1]):
            for keyspace in system_keyspaces:
                replication_factors = {old_dc: 3, new_dc: rf}
                cql = f"ALTER KEYSPACE {keyspace} WITH replication = {NetworkTopologyReplicationStrategy(**replication_factors)}"
                self.execute_cql(cql)
                wait_for_tablets_balanced(self.db_cluster.nodes[0])

        for rf in range(1, self.scale_out_n_nodes[1]):
            for keyspace in self.keyspaces:
                replication_factors = {old_dc: 3, new_dc: rf}
                cql = f"ALTER KEYSPACE {keyspace} WITH replication = {NetworkTopologyReplicationStrategy(**replication_factors)}"
                self.execute_cql(cql)
                wait_for_tablets_balanced(self.db_cluster.nodes[0])

    def insert_data(self, dataset_size: int, ks_name: str, ks_num: int):
        stress_cmd = self.prepare_dataset_layout(dataset_size)
        stress_queue = self.run_stress_thread(
            stress_cmd=stress_cmd, keyspace_name=ks_name, stress_num=1, keyspace_num=ks_num)
        self.verify_stress_thread(cs_thread_pool=stress_queue)
        self.get_stress_results(queue=stress_queue)

    def run_stress_until_target(self, target_used_size, target_usage):
        current_usage, current_used = self.get_max_disk_usage()

        # Use smaller chunks (1GB) near threshold, otherwise use 10% of remaining space
        small_chunk = 1
        big_chunk = int((target_used_size - current_used) * 0.1)
        while current_used < target_used_size and current_usage < target_usage:
            # Write smaller dataset near the threshold (15% or 30GB of the target)
            smaller_dataset = (((target_used_size - current_used) < 30) or ((target_usage - current_usage) <= 15))

            dataset_size = small_chunk if smaller_dataset else big_chunk
            num = len(self.keyspaces) + 1
            ks_name = f"keyspace_{'small' if smaller_dataset else 'large'}{num}"
            self.keyspaces.append(ks_name)
            self.log.info(f"Writing chunk of size: {dataset_size} GB in keyspace {ks_name}")
            self.log_disk_usage()
            self.insert_data(dataset_size, ks_name, num)

            self.db_cluster.flush_all_nodes()

            current_usage, current_used = self.get_max_disk_usage()
            self.log.info(f"Wrote chunk of size: {dataset_size} GB in keyspace {ks_name}")
            self.log_disk_usage()
            self.log.info(
                f"Max disk usage after writing to {ks_name}: {current_usage}% ({current_used} GB / {target_used_size} GB)")
            InfoEvent(message=f"{current_usage}% Limit Reached").publish()

    def log_disk_usage(self):
        data = get_cluster_disk_usage(self.db_cluster)

        headers = f"{'Node':<8} {'Total GB':<12} {'Used GB':<12} {'Avail GB':<12} {'Used %':<8}"
        self.log.info(headers)

        for idx in range(1, len(self.db_cluster.nodes) + 1):
            node_data = data[f"node_{idx}"]
            row = f"{idx:<8} {node_data['Total']:<12} {node_data['Used']:<12} {node_data['Available']:<12} {node_data['Usage']:.1f}%"
            self.log.info(row)

        cluster_data = data["cluster"]
        total = f"{'Cluster':<8} {cluster_data['Total']:<12} {cluster_data['Used']:<12} {cluster_data['Available']:<12} {cluster_data['Usage']:.1f}%"
        self.log.info(total)

    def disk_usage_to_argus(self, label: str):
        argus_client = self.test_config.argus_client()
        data = get_cluster_disk_usage(self.db_cluster)
        disk_usage_to_argus(argus_client, label, data)

    def test_reclaim_space(self):
        """
        3 nodes cluster, RF=3.
        Write data until 90% disk usage is reached.
        Sleep for 60 minutes.
        Drop some data and verify space was reclaimed
        """
        self.run_stress(self.softlimit, sleep_time=self.sleep_time_fill_disk)
        self.run_stress(self.hardlimit, sleep_time=self.sleep_time_fill_disk)
        self.disk_usage_to_argus(label="After data insertion")

        free_before = self.get_total_free_space()

        # remove data from 2 large and 2 small keyspaces
        for keyspace in self.get_keyspaces(prefix="keyspace_large")[:2]:
            self.remove_data_from_cluster(keyspace)
            time.sleep(600)
        for keyspace in self.get_keyspaces(prefix="keyspace_small")[:2]:
            self.remove_data_from_cluster(keyspace)
            time.sleep(600)

        # sleep to let compaction happen
        time.sleep(1800)
        self.log_disk_usage()
        self.disk_usage_to_argus(label="After data removal")

        free_after = self.get_total_free_space()
        assert free_after > free_before, "space was not freed after dropping data"

    def test_scale_out(self):
        """
        3 nodes cluster, RF=3.
        Write data until 90% disk usage is reached.
        Sleep for 60 minutes.
        Add  new node(s)

        Configurations:
        - test-cases/scale/full-storage-utilization-scale-out-different-dc.yaml
        - test-cases/scale/full-storage-utilization-scale-out-both-dcs.yaml
        - test-cases/scale/full-storage-utilization-scale-out-larger-instance.yaml
        - test-cases/scale/full-storage-utilization-scale-out-same-instance.yaml
        - test-cases/scale/full-storage-utilization-scale-out-smaller-instance.yaml
        """
        with self.timer_results_to_argus("Soft Limit"):
            self.run_stress(self.softlimit, sleep_time=self.sleep_time_fill_disk)
            self.disk_usage_to_argus(label="Soft Limit")
            InfoEvent(message=f"Solf limit").publish()
        with self.timer_results_to_argus("Hard Limit"):
            self.run_stress(self.hardlimit, sleep_time=self.sleep_time_fill_disk)
            self.disk_usage_to_argus(label="Hard Limit")
            InfoEvent(message=f"Hard limit").publish()

        self.scale_out()
        self.log_disk_usage()
        self.disk_usage_to_argus(label="Scale out")
        InfoEvent(message=f"Scale out").publish()

        time.sleep(1800)
        self.log_disk_usage()
        self.disk_usage_to_argus(label="Final")
