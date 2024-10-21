from enum import Enum
import time
from sdcm.tester import ClusterTester
from sdcm.utils.tablets.common import wait_for_tablets_balanced
from sdcm.utils.full_storage_utils import DiskUtils, StressUtils


class ScalingActionType(Enum):
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"


class FullStorageUtilizationTest(ClusterTester):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num_stress_threads = 10
        self.sleep_seconds_before_scale = 120
        self.sleep_time_fill_disk = 1800
        self.softlimit = self.params.get('diskusage_softlimit')
        self.hardlimit = self.params.get('diskusage_hardlimit')
        self.stress_cmd_w = self.params.get('stress_cmd_w')
        self.stress_cmd_r = self.params.get('stress_cmd_r')
        self.add_node_cnt = self.params.get('add_node_cnt')
        self.scaling_action_type = self.params.get('scaling_action_type')
        self.stress_utils = None

    def setUp(self):
        super().setUp()
        self.start_time = time.time()
        self.stress_utils = StressUtils(
            db_node=self.db_cluster.nodes[0], cluster_tester=self)

    def start_throttle_write(self):
        self.stress_cmd_w = self.stress_cmd_w.replace(
            "<THREADS_PLACE_HOLDER>", str(self.num_stress_threads))
        self.run_stress_thread(stress_cmd=self.stress_cmd_w)
        '''
        During scaling operation, make sure there are some on-going read/write
        operations to simulate real-world. Wait for 2mins so that c-s tool
        is started running.
        '''
        time.sleep(self.sleep_seconds_before_scale)

    def start_throttle_read(self):
        self.stress_cmd_r = self.stress_cmd_r.replace(
            "<THREADS_PLACE_HOLDER>", str(self.num_stress_threads))
        self.run_stress_thread(stress_cmd=self.stress_cmd_r)
        time.sleep(self.sleep_seconds_before_scale)

    def start_throttle_rw(self):
        self.start_throttle_write()
        self.start_throttle_read()

    def scale_out(self):
        self.start_throttle_rw()
        self.log.info("Started adding a new node")
        start_time = time.time()
        self.add_new_nodes()
        duration = time.time() - start_time
        self.log.info(f"Adding a node finished with duration: {duration}")

    def scale_in(self):
        self.start_throttle_rw()
        self.log.info("Started removing a node")
        start_time = time.time()
        self.remove_node()
        duration = time.time() - start_time
        self.log.info(f"Removing a node finished with duration: {duration}")

    def drop_data(self, keyspace_name):
        '''
        Drop keyspace and clear snapshots.
        '''
        node = self.db_cluster.nodes[0]
        self.log.info("Dropping some data")
        query = f"DROP KEYSPACE {keyspace_name}"
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute(query)
            # node.run_nodetool(f"clearsnapshot")
        DiskUtils.log_disk_usage(self.db_cluster.nodes)

    def perform_scale_in(self):
        '''
        If the test was configured to stop populating data at 90% utilization, first scale-out then
        drop 20% of data to make space for scale-in operation.

        If the test was configured to stop populating data at 67% disk utilization, scale-in without scale-out or
        dropping data.
        '''
        if self.hardlimit == 90:
            self.scale_out()
            '''
           Before removing a node, we should make sure
           other nodes has enough space so that they
           can accommodate data from the removed node.
           '''
            # Remove 20% of data from the cluster.
            self.drop_data("keyspace_large1")
            self.drop_data("keyspace_large2")
            self.scale_in()
        elif self.hardlimit == 67:
            self.scale_in()

    def perform_action(self):
        DiskUtils.log_disk_usage(self.db_cluster.nodes)
        # Trigger specific action
        if self.scaling_action_type == ScalingActionType.SCALE_OUT.value:
            self.scale_out()
        elif self.scaling_action_type == ScalingActionType.SCALE_IN.value:
            self.perform_scale_in()
        else:
            self.log.info(f"Invalid ActionType {self.scaling_action_type}")
        DiskUtils.log_disk_usage(self.db_cluster.nodes)

    def test_storage_utilization(self):
        """
        Write data until disk usage reaches specified hardlimit.
        Sleep for few minutes.
        Perform specific action.
        """
        self.run_stress(self.softlimit, sleep_seconds=self.sleep_time_fill_disk)
        self.run_stress(self.hardlimit, sleep_seconds=self.sleep_time_fill_disk)
        self.perform_action()

    def run_stress(self, target_usage, sleep_seconds=600):
        target_used_size = DiskUtils.determine_storage_limit(
            self.db_cluster.nodes, target_usage)
        self.stress_utils.run_stress_until_target(
            target_used_size, target_usage, self.hardlimit, self.softlimit)

        DiskUtils.log_disk_usage(self.db_cluster.nodes)
        self.log.info(f"Wait for {sleep_seconds} seconds")
        time.sleep(sleep_seconds)
        DiskUtils.log_disk_usage(self.db_cluster.nodes)

    def add_new_nodes(self):
        new_nodes = self.db_cluster.add_nodes(
            count=self.add_node_cnt, enable_auto_bootstrap=True)
        self.db_cluster.wait_for_init(node_list=new_nodes)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)
        total_nodes_in_cluster = len(self.db_cluster.nodes)
        self.log.info(
            f"New node added, total nodes in cluster: {total_nodes_in_cluster}")
        self.monitors.reconfigure_scylla_monitoring()
        wait_for_tablets_balanced(self.db_cluster.nodes[0])

    def remove_node(self):
        self.log.info('Removing a second node from the cluster')
        node_to_remove = self.db_cluster.nodes[1]
        self.log.info(f"Node to be removed: {node_to_remove.name}")
        self.db_cluster.decommission(node_to_remove)
        self.log.info(
            f"Node {node_to_remove.name} has been removed from the cluster")
        self.monitors.reconfigure_scylla_monitoring()
        wait_for_tablets_balanced(self.db_cluster.nodes[0])
