import time
from sdcm.tester import ClusterTester
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP

class FullStorageUtilizationTest(ClusterTester):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def prepare_dataset_layout(self, dataset_size, row_size=10240):
        n = dataset_size * 1024 * 1024 * 1024 // row_size
        seq_end = n * 100

        return f'cassandra-stress write cl=ONE n={n} -mode cql3 native -rate threads=10 -pop dist="uniform(1..{seq_end})" ' \
               f'-col "size=FIXED({row_size}) n=FIXED(1)" -schema "replication(strategy=NetworkTopologyStrategy,replication_factor=3)"'

    def setUp(self):
        super().setUp()
        self.start_time = time.time()

    def test_storage_utilization(self):
        """
        3 nodes cluster, RF=3.
        Write data until 85% disk usage is reached.
        Sleep for 90 minutes.
        Add 4th node.
        """
        self.log.info("Running df command on all nodes:")
        self.get_df_output()
        self.run_stress(85)
        self.run_stress(93)

        self.log.info("Adding a new node")
        self.add_new_node()

        ## TODO: Move below code
        self.log_disk_usage()
        self.log.info("Wait for 60 minutes")
        time.sleep(3600)  
        self.log_disk_usage()

    def run_stress(self, target_usage):
        target_used_size = self.calculate_target_used_size(target_usage)
        self.run_stress_until_target(target_used_size, target_usage)

        self.log_disk_usage()
        self.log.info("Wait for 60 minutes")
        time.sleep(3600)  
        self.log_disk_usage()

    def run_stress_until_target(self, target_used_size, target_usage):
        current_used = self.get_max_disk_used()
        current_usage = self.get_max_disk_usage()
        num = 0
        smaller_dataset = True
        while current_used < target_used_size and current_usage < target_usage:
            num += 1
            
            # Write smaller dataset near the threshold (10% or 20GB of the target)
            smaller_dataset = (((target_used_size - current_used) < 20) or ((target_usage - current_usage) <= 10))

            dataset_size = 1 if smaller_dataset else 10  # 1 GB or 10 GB
            ks_name = "keyspace_small" if smaller_dataset else "keyspace_large"
            stress_cmd = self.prepare_dataset_layout(dataset_size)
            stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, keyspace_name=f"{ks_name}{num}", stress_num=1, keyspace_num=num)

            self.verify_stress_thread(cs_thread_pool=stress_queue)
            self.get_stress_results(queue=stress_queue)

            self.flush_all_nodes()

            current_used = self.get_max_disk_used()
            current_usage = self.get_max_disk_usage()
            self.log.info(f"Current max disk usage after writing to keyspace{num}: {current_usage}% ({current_used} GB / {target_used_size} GB)")



    def add_new_node(self):
        self.get_df_output()
        new_nodes = self.db_cluster.add_nodes(count=1, enable_auto_bootstrap=True)
        self.monitors.reconfigure_scylla_monitoring()
        new_node = new_nodes[0]
        self.db_cluster.wait_for_init(node_list=[new_node], timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP, check_node_health=False)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        total_nodes_in_cluster = len(self.db_cluster.nodes)
        self.log.info(f"New node added, total nodes in cluster: {total_nodes_in_cluster}")
        self.get_df_output()

    def get_df_output(self):
        for node in self.db_cluster.nodes:
            result = node.remoter.run('df -h /var/lib/scylla')
            self.log.info(f"DF output for node {node.name}:\n{result.stdout}")

    def flush_all_nodes(self):
        for node in self.db_cluster.nodes:
            self.log.info(f"Flushing data on node {node.name}")
            node.run_nodetool("flush")

    def get_max_disk_usage(self):
        max_usage = 0
        for node in self.db_cluster.nodes:
            result = node.remoter.run("df -h --output=pcent /var/lib/scylla | sed 1d | sed 's/%//'")
            usage = int(result.stdout.strip())
            max_usage = max(max_usage, usage)
        return max_usage

    def get_max_disk_used(self):
        max_used = 0
        for node in self.db_cluster.nodes:
            result = node.remoter.run("df -h --output=used /var/lib/scylla | sed 1d | sed 's/G//'")
            used = int(result.stdout.strip())
            max_used = max(max_used, used)
        return max_used

    def get_disk_info(self, node):
        result = node.remoter.run("df -h --output=size,used,avail,pcent /var/lib/scylla | sed 1d")
        size, used, avail, pcent = result.stdout.strip().split()
        return {
            'total': int(size.rstrip('G')),
            'used': int(used.rstrip('G')),
            'available': int(avail.rstrip('G')),
            'used_percent': int(pcent.rstrip('%'))
        }

    def calculate_target_used_size(self, target_percent):
        max_total = 0
        for node in self.db_cluster.nodes:
            info = self.get_disk_info(node)
            max_total = max(max_total, info['total'])
        
        target_used_size = (target_percent / 100) * max_total
        current_used = self.get_max_disk_used()
        additional_usage_needed = target_used_size - current_used

        self.log.info(f"Current max disk usage: {self.get_max_disk_usage():.2f}%")
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
