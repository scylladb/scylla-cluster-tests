from enum import Enum
import time
from sdcm.tester import ClusterTester
from sdcm.utils.tablets.common import wait_for_tablets_balanced


class ScalingActionType(Enum):
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"


class FullStorageUtilizationTest(ClusterTester):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num_stress_threads = 10
        self.sleep_time_before_scale = 120
        self.sleep_time_fill_disk = 1800
        self.softlimit = self.params.get('diskusage_softlimit')
        self.hardlimit = self.params.get('diskusage_hardlimit')
        self.stress_cmd_w = self.params.get('stress_cmd_w')
        self.stress_cmd_r = self.params.get('stress_cmd_r')
        self.add_node_cnt = self.params.get('add_node_cnt')
        self.scaling_action_type = self.params.get('scaling_action_type')
        self.total_large_ks = 0
        self.total_small_ks = 0

    def prepare_dataset_layout(self, dataset_size, row_size=10240):
        n = dataset_size * 1024 * 1024 * 1024 // row_size
        seq_end = n * 100
        cores = self.db_cluster.nodes[0].cpu_cores
        if not cores:
            self.num_stress_threads = 10
        else:
            self.num_stress_threads = int(cores) * 8

        return f'cassandra-stress write cl=ONE n={n} -mode cql3 native -rate threads={self.num_stress_threads} -pop dist="uniform(1..{seq_end})" ' \
               f'-col "size=FIXED({row_size}) n=FIXED(1)" -schema "replication(strategy=NetworkTopologyStrategy,replication_factor=3)"'

    def setUp(self):
        super().setUp()
        self.start_time = time.time()

    def start_throttle_write(self):
        self.stress_cmd_w = self.stress_cmd_w.replace("<THREADS_PLACE_HOLDER>", str(self.num_stress_threads))
        self.run_stress_thread(stress_cmd=self.stress_cmd_w)
        '''
        During scaling operation, make sure there is some on-going read/write
        operations to simulate real-world. Wait for 2mins so that c-s tool
        is started running.
        '''
        time.sleep(self.sleep_time_before_scale)

    def start_throttle_read(self):
        self.stress_cmd_r = self.stress_cmd_r.replace("<THREADS_PLACE_HOLDER>", str(self.num_stress_threads))
        self.run_stress_thread(stress_cmd=self.stress_cmd_r)
        time.sleep(self.sleep_time_before_scale)

    def start_throttle_rw(self):
        self.start_throttle_write()
        self.start_throttle_read()

    def scale_out(self):
        self.start_throttle_rw()
        self.log.info("Started adding a new node")
        start_time = time.time()
        self.add_new_node()
        duration = time.time() - start_time
        self.log.info(f"Adding a node finished with time: {duration}")

    def scale_in(self):
        self.start_throttle_rw()
        self.log.info("Started removing a node")
        start_time = time.time()
        self.remove_node()
        duration = time.time() - start_time
        self.log.info(f"Removing a node finished with time: {duration}")

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
        self.log_disk_usage()

    def perform_scale_in(self):
        '''
        If we are already at 90% disk utilization, first scale-out then
        drop 20% of data to make space for scale-in operation.

        If we are at 67% disk utilization, scale-in without scale-out or
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
        self.log_disk_usage()
        # Trigger specific action
        if self.scaling_action_type == ScalingActionType.SCALE_OUT.value:
            self.scale_out()
        elif self.scaling_action_type == ScalingActionType.SCALE_IN.value:
            self.perform_scale_in()
        else:
            self.log.info(f"Invalid ActionType {self.scaling_action_type}")
        self.log_disk_usage()

    def test_storage_utilization(self):
        """
        Write data until 90% disk usage is reached.
        Sleep for 60 minutes.
        Perform specific action.
        """
        self.run_stress(self.softlimit, sleep_time=self.sleep_time_fill_disk)
        self.run_stress(self.hardlimit, sleep_time=self.sleep_time_fill_disk)
        self.perform_action()

    def run_stress(self, target_usage, sleep_time=600):
        target_used_size = self.calculate_target_used_size(target_usage)
        self.run_stress_until_target(target_used_size, target_usage)

        self.log_disk_usage()
        self.log.info(f"Wait for {sleep_time} seconds")
        time.sleep(sleep_time)
        self.log_disk_usage()

    def run_stress_until_target(self, target_used_size, target_usage):
        current_usage, current_used = self.get_max_disk_usage()
        smaller_dataset = False

        space_needed = target_used_size - current_used
        # Calculate chunk size as 10% of space needed
        chunk_size = int(space_needed * 0.1)
        while current_used < target_used_size and current_usage < target_usage:
            # Write smaller dataset near the threshold (15% or 30GB of the target)
            smaller_dataset = (((target_used_size - current_used) < 30) or ((target_usage - current_usage) <= 15))
            if not smaller_dataset:
                self.total_large_ks += 1
            else:
                self.total_small_ks += 1

            # Use 1GB chunks near threshold, otherwise use 10% of remaining space
            dataset_size = 1 if smaller_dataset else chunk_size
            ks_name = "keyspace_small" if smaller_dataset else "keyspace_large"
            num = self.total_small_ks if smaller_dataset else self.total_large_ks
            self.log.info(f"Writing chunk of size: {dataset_size} GB")
            stress_cmd = self.prepare_dataset_layout(dataset_size)
            stress_queue = self.run_stress_thread(
                stress_cmd=stress_cmd, keyspace_name=f"{ks_name}{num}", stress_num=1, keyspace_num=num)

            self.verify_stress_thread(cs_thread_pool=stress_queue)
            self.get_stress_results(queue=stress_queue)

            self.db_cluster.flush_all_nodes()
            #time.sleep(60) if smaller_dataset else time.sleep(600)

            current_usage, current_used = self.get_max_disk_usage()
            self.log.info(
                f"Current max disk usage after writing to keyspace{num}: {current_usage}% ({current_used} GB / {target_used_size} GB)")

    def add_new_node(self):
        new_nodes = self.db_cluster.add_nodes(count=self.add_node_cnt, enable_auto_bootstrap=True)
        self.db_cluster.wait_for_init(node_list=new_nodes)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)
        total_nodes_in_cluster = len(self.db_cluster.nodes)
        self.log.info(f"New node added, total nodes in cluster: {total_nodes_in_cluster}")
        self.monitors.reconfigure_scylla_monitoring()
        wait_for_tablets_balanced(self.db_cluster.nodes[0])

    def remove_node(self):
        self.log.info('Removing a second node from the cluster')
        node_to_remove = self.db_cluster.nodes[1]
        self.log.info(f"Node to be removed: {node_to_remove.name}")
        self.db_cluster.decommission(node_to_remove)
        self.log.info(f"Node {node_to_remove.name} has been removed from the cluster")
        self.monitors.reconfigure_scylla_monitoring()
        wait_for_tablets_balanced(self.db_cluster.nodes[0])

    def get_max_disk_usage(self):
        max_usage = 0
        max_used = 0
        for node in self.db_cluster.nodes:
            info = self.get_disk_info(node)
            max_usage = max(max_usage, info["used_percent"])
            max_used = max(max_used, info["used"])
        return max_usage, max_used

    def get_disk_info(self, node):
        result = node.remoter.run(
            "df -h -BG --output=size,used,avail,pcent /var/lib/scylla | sed 1d | sed 's/G//g' | sed 's/%//'")
        size, used, avail, pcent = result.stdout.strip().split()
        return {
            'total': int(size),
            'used': int(used),
            'available': int(avail),
            'used_percent': int(pcent)
        }

    def calculate_target_used_size(self, target_percent):
        max_total = 0
        for node in self.db_cluster.nodes:
            info = self.get_disk_info(node)
            max_total = max(max_total, info['total'])

        target_used_size = (target_percent / 100) * max_total
        current_usage, current_used = self.get_max_disk_usage()
        additional_usage_needed = target_used_size - current_used

        self.log.info(f"Current max disk usage: {current_usage:.2f}%")
        self.log.info(f"Current max used space: {current_used:.2f} GB")
        self.log.info(f"Max total disk space: {max_total:.2f} GB")
        self.log.info(f"Target used space to reach {target_percent}%: {target_used_size:.2f} GB")
        self.log.info(f"Additional space to be used: {additional_usage_needed:.2f} GB")

        return target_used_size

    def log_disk_usage(self):
        for node in self.db_cluster.nodes:
            info = self.get_disk_info(node)
            self.log.info(f"Disk usage for node {node.name}:")
            self.log.info(f"  Total: {info['total']} GB")
            self.log.info(f"  Used: {info['used']} GB")
            self.log.info(f"  Available: {info['available']} GB")
            self.log.info(f"  Used %: {info['used_percent']}%")
