from functools import partial
import time

from sdcm.tester import ClusterTester
from sdcm.utils.common import ParallelObject
from sdcm.utils.decorators import measure_time

class MixedShardRepair(ClusterTester):

    def setUp(self):
        super().setUp()
        self.populate_data_parallel(size_in_gb=1024, blocking=True)

    @measure_time
    def _run_repair(self, node):
        self.log.info('Running nodetool repair on {}'.format(node.name))
        node.run_nodetool(sub_cmd='repair')

    @measure_time
    def _run_repairs(self):
        triggers = [partial(node.run_nodetool, sub_cmd="repair", args=f"keyspace1 standard1", ) for node
                    in self.db_cluster.nodes]
        ParallelObject(objects=triggers, timeout=1200 * 60).call_objects()

    def test_mixed_shard_repair(self):
        node = self.db_cluster.nodes[0]
        repair_time = self._run_repair(node=node)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair time on node: {} is: {}'.format(node.name, repair_time))

    def test_mixed_shard_repair_on_all_nodes(self):
        repair_time = self._run_repairs()[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair time is: {}'.format(repair_time))

    def test_mixed_shard_bootstrap(self):
        self.log.info('Bootstrapping a new node...')
        new_node = self.db_cluster.add_nodes(count=1)[0]
        self.monitors.reconfigure_scylla_monitoring()
        self.log.info('Waiting for new node to finish initializing...')
        self.db_cluster.wait_for_init(node_list=[new_node])
        self.monitors.reconfigure_scylla_monitoring()

class MixedShardRepairBase(ClusterTester):

    @measure_time
    def _run_repair(self, node):
        node.run_nodetool(sub_cmd='repair')

    @measure_time
    def _run_bootstrap(self):
        new_node = self.db_cluster.add_nodes(count=1)[0]
        self.db_cluster.wait_for_init(node_list=[new_node])
        return new_node

    @measure_time
    def _run_rebuild(self, node):
        node.run_nodetool(sub_cmd='rebuild')

    @measure_time
    def _run_decommission(self, node):
        node.run_nodetool(sub_cmd='decommission')

    def _repair(self, node):
        self.log.info('Running nodetool repair on {}'.format(node.name))
        repair_time = self._run_repair(node=node)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair time on node: {} is: {}'.format(node.name, repair_time))

    def _bootstrap(self):
        self.log.info('Bootstrapping a new node...')
        bootstrap_time, new_node = self._run_bootstrap()
        self.monitors.reconfigure_scylla_monitoring()
        self.log.info('Bootstrap time of node: {} is: {}'.format(new_node.name, bootstrap_time))
        return new_node

    def _rebuild(self, node):
        self.log.info('Rebuilding a node: {}...'.format(node.name))
        rebuild_time = self._run_rebuild(node=node)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Rebuild time of node: {} is: {}'.format(node.name, rebuild_time))

    def _decommission(self, node):
        self.log.info('Decommission of a node: {}...'.format(node.name))
        decommission_time = self._run_decommission(node=node)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Decommission time of node: {} is: {}'.format(node.name, decommission_time))

    def test_mix_shard_ops(self):
        node1 = self.db_cluster.nodes[0]
        node2 = self.db_cluster.nodes[1]
        node3 = self._bootstrap()
        self._repair(node1)
        self._rebuild(node2)
        self._decommission(node3)

class MixedShardRepairDense(MixedShardRepairBase):

    def setUp(self):
        super().setUp()
        self.populate_data_parallel(size_in_gb=1024, replication_factor=3, blocking=True)

class MixedShardRepairSparse(MixedShardRepairBase):

    def setUp(self):
        super().setUp()
        self.populate_data_parallel_mb(size_in_mb=50, replication_factor=3, blocking=True)

    def populate_data_parallel_mb(self, size_in_mb: int, replication_factor: int = 3, blocking=True, read=False):

        # pylint: disable=too-many-locals
        base_cmd = "cassandra-stress write cl=QUORUM "
        if read:
            base_cmd = "cassandra-stress read cl=ONE "
        stress_fixed_params = f" -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor={replication_factor}) " \
                              "compaction(strategy=LeveledCompactionStrategy)' " \
                              "-mode cql3 native -rate threads=200 -col 'size=FIXED(1024) n=FIXED(1)' "
        stress_keys = "n="
        population = " -pop seq="

        total_keys = size_in_mb * 1024
        n_loaders = int(self.params.get('n_loaders'))
        keys_per_node = total_keys // n_loaders

        write_queue = []
        start = 1
        for i in range(1, n_loaders + 1):
            stress_cmd = base_cmd + stress_keys + str(keys_per_node) + population + str(start) + ".." + \
                str(keys_per_node * i) + stress_fixed_params
            start = keys_per_node * i + 1

            write_queue.append(self.run_stress_thread(stress_cmd=stress_cmd, round_robin=True))
            time.sleep(3)

        if blocking:
            for stress in write_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

        return write_queue
