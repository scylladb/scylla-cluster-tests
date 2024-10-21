import logging

LOGGER = logging.getLogger(__name__)


class DiskUtils:
    @staticmethod
    def get_disk_info(node):
        result = node.remoter.run(
            "df -h -BG --output=size,used,avail,pcent /var/lib/scylla | sed 1d | sed 's/G//g' | sed 's/%//'")
        size, used, avail, pcent = result.stdout.strip().split()
        return {
            'total': int(size),
            'used': int(used),
            'available': int(avail),
            'used_percent': int(pcent)
        }

    @staticmethod
    def get_max_disk_usage(nodes):
        max_used_percent = 0
        max_used = 0
        for node in nodes:
            info = DiskUtils.get_disk_info(node)
            max_used_percent = max(max_used_percent, info["used_percent"])
            max_used = max(max_used, info["used"])
        return max_used_percent, max_used

    @staticmethod
    def determine_storage_limit(nodes, target_percent):
        """
        Calculate the target disk usage limit in GB that needs to be reached
        in the cluster.
        """
        max_total = 0
        for node in nodes:
            info = DiskUtils.get_disk_info(node)
            max_total = max(max_total, info['total'])

        target_used_size = (target_percent / 100) * max_total
        current_usage, current_used = DiskUtils.get_max_disk_usage(nodes)
        additional_usage_needed = target_used_size - current_used

        LOGGER.info(f"Current max disk usage: {current_usage:.2f}%")
        LOGGER.info(f"Current max used space: {current_used:.2f} GB")
        LOGGER.info(f"Max total disk space: {max_total:.2f} GB")
        LOGGER.info(
            f"Target harddisk limit to reach {target_percent}% or {target_used_size:.2f} GB")
        LOGGER.info(
            f"Additional space to be populated: {additional_usage_needed:.2f} GB")

        # Target disk usage in GB (either softlimit or hardlimit value)
        return target_used_size

    @staticmethod
    def log_disk_usage(nodes):
        for node in nodes:
            info = DiskUtils.get_disk_info(node)
            LOGGER.info(f"Disk usage for node {node.name}:")
            LOGGER.info(f"  Total: {info['total']} GB")
            LOGGER.info(f"  Used: {info['used']} GB")
            LOGGER.info(f"  Available: {info['available']} GB")
            LOGGER.info(f"  Used %: {info['used_percent']}%")


class StressUtils():
    def __init__(self, db_node, cluster_tester):
        self.db_node = db_node
        self.db_cluster = self.db_node.parent_cluster
        self.cluster_tester = cluster_tester
        self.large_ks_cnt = 0
        self.small_ks_cnt = 0
        cores = self.db_cluster.nodes[0].cpu_cores
        self.num_stress_threads = 8 if not cores else int(cores) * 8

    def prepare_dataset_layout(self, dataset_size_gb, row_size=10240):
        n = dataset_size_gb * 1024 * 1024 * 1024 // row_size
        seq_end = n * 100

        return f'cassandra-stress write cl=ONE n={n} -mode cql3 native -rate threads={self.num_stress_threads} ' \
            f'-pop dist="uniform(1..{seq_end})" ' \
            f'-col "size=FIXED({row_size}) n=FIXED(1)" ' \
            f'-schema "replication(strategy=NetworkTopologyStrategy,replication_factor=3)"'

    def run_stress_until_target(self, target_used_size, target_usage, hardlimit, softlimit):
        current_usage, current_used = DiskUtils.get_max_disk_usage(
            self.db_cluster.nodes)
        smaller_dataset = False

        space_needed = target_used_size - current_used
        # Calculate chunk size as 10% of space needed
        chunk_size = int(space_needed * 0.1)

        while current_used < target_used_size and current_usage < target_usage:
            # Write smaller dataset near the threshold (15% or 30GB of the target)
            smaller_dataset = (((target_used_size - current_used) < 30) or
                               ((target_usage - current_usage) <= (hardlimit - softlimit)))

            if not smaller_dataset:
                self.large_ks_cnt += 1
            else:
                self.small_ks_cnt += 1

            # Use 1GB chunks near threshold, otherwise use 10% of remaining space
            dataset_size_gb = 1 if smaller_dataset else chunk_size
            ks_name = "keyspace_small" if smaller_dataset else "keyspace_large"
            num = self.small_ks_cnt if smaller_dataset else self.large_ks_cnt

            LOGGER.info(f"Writing chunk of size: {dataset_size_gb} GB")
            stress_cmd = self.prepare_dataset_layout(dataset_size_gb)
            stress_queue = self.cluster_tester.run_stress_thread(
                stress_cmd=stress_cmd,
                keyspace_name=f"{ks_name}{num}",
                stress_num=1,
                keyspace_num=num
            )

            self.cluster_tester.verify_stress_thread(
                cs_thread_pool=stress_queue)
            self.cluster_tester.get_stress_results(queue=stress_queue)

            self.db_cluster.flush_all_nodes()
            # time.sleep(60) if smaller_dataset else time.sleep(600)

            current_usage, current_used = DiskUtils.get_max_disk_usage(
                self.db_cluster.nodes)
            LOGGER.info(
                f"Current max disk usage after writing to keyspace{num}: "
                f"{current_usage}% ({current_used} GB / {target_used_size} GB)"
            )
