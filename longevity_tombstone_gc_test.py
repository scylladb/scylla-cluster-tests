import datetime
import re
import time
from functools import partial

from longevity_twcs_test import TWCSLongevityTest
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.compaction_ops import CompactionOps
from sdcm.utils.sstable.sstable_utils import SstableUtils


class TombstoneGcLongevityTest(TWCSLongevityTest):
    keyspace = 'scylla_bench'
    table = 'test'
    ks_cf = f'{keyspace}.{table}'
    repair_date = None
    db_node = None

    def setUp(self):
        super().setUp()
        post_prepare_cql_cmds = self.params.get('post_prepare_cql_cmds')
        self.ttl = int(re.search(r'default_time_to_live = (\d+)', post_prepare_cql_cmds).group(1))
        self.propagation_delay = int(
            re.search(r"'propagation_delay_in_seconds':'(\d+)", post_prepare_cql_cmds).group(1))

    def _flush_nodes(self):
        self.log.info('Run a flush for %s on nodes', self.keyspace)
        triggers = [partial(node.run_nodetool, sub_cmd=f"flush -- {self.keyspace}", )
                    for node in self.db_cluster.data_nodes]
        ParallelObject(objects=triggers, timeout=1200).call_objects()

    def _repair_nodes(self):
        self.log.info('Run a repair for %s on nodes', self.ks_cf)
        triggers = [partial(node.run_nodetool, sub_cmd="repair", args=f"-pr {self.keyspace} {self.table}", ) for node
                    in self.db_cluster.data_nodes]
        ParallelObject(objects=triggers, timeout=1200).call_objects()

    def _run_major_compaction(self):
        self.log.info('Run a major compaction for %s on node', self.ks_cf)
        self.db_node.run_nodetool("compact", args=f"{self.keyspace} {self.table}")
        self.wait_no_compactions_running()

    def _run_repair_and_major_compaction(self, wait_propagation_delay: bool = False):
        self._flush_nodes()
        self._repair_nodes()
        if wait_propagation_delay:
            time.sleep(self.propagation_delay)
        self.repair_date = datetime.datetime.now()
        self._run_major_compaction()

    def _drop_and_recreate_keyspace(self):
        self.log.info("Dropping the s-b keyspace before continuing with next tombstone-gc-mode")
        with self.db_cluster.cql_connection_patient(node=self.db_node) as session:
            session.execute(f"DROP KEYSPACE {self.keyspace}")
        self.log.info("Recreating the s-b keyspace and table after it was dropped")
        self.create_tables_for_scylla_bench()

    def test_switch_tombstone_gc_modes(self):
        """
        Test the 4 modes of tombstones-gc.
        Verify the expected tombstones' state before switching to the next GC mode.

        #Refs: https://github.com/scylladb/scylla/commit/a8ad385ecd3e2b372db3c354492dbe57d9d91760

        test steps:
        -----------
        Based on TWCS TTL 48h longevity configuration with a much shorter TTL (few minutes) and same for gc-grace-seconds.
        change gc-grace-seconds back to default of 10 days.
        change gc-mode to repair
        run a repair + wait propagation delay + run a major compaction.
        verify no tombstones exist in post-repair-created sstables.
        change gc-mode to immediate.
        wait for load to end + TTL period.
        Run a major compaction.
        Verify no tombstones.
        change gc-mode to disabled
        wait a duration of TTL.
        count number of tombstones.
        run a repair + a major compaction.
        count again the number of tombstones and verify no smaller number of tombstones is found.
        So it confirms no tombstones were deleted during gc-mode "disabled".
        """

        self.create_tables_for_scylla_bench()
        self.db_node = self.db_cluster.nodes[0]

        # ALTER TABLE scylla_bench.test with (according to yaml post_prepare_cql_cmds):
        # 4 minutes load duration
        # gc_grace_seconds = 240
        # default_time_to_live = 240
        # TimeWindowCompactionStrategy
        # tombstone_gc 'disabled'
        # propagation_delay_in_seconds 240
        self.run_post_prepare_cql_cmds()
        sstable_utils = SstableUtils(db_node=self.db_node, propagation_delay_in_seconds=self.propagation_delay,
                                     ks_cf=self.ks_cf)
        stress_cmd = self.params.get('stress_cmd')

        params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}

        def _run_and_verify_stress():
            self.log.info('Run and verify completion of stress command')
            stress_queue = []
            self._run_all_stress_cmds(stress_queue, params)
            for stress in stress_queue:
                self.verify_stress_thread(thread_pool=stress)

        def wait_for_propagation():
            self.log.info('Wait a duration of propagation_delay_in_seconds')
            time.sleep(self.propagation_delay)

        def wait_for_ttl_and_propagation():
            wait_for_propagation()
            self.log.info('Wait a duration of TTL')
            time.sleep(self.ttl + 60)  # Adding an extra minute sleep for table rows to finish expiry.

        # Testing gc mode 'repair'
        self._drop_and_recreate_keyspace()
        self.log.info("change gc-grace-seconds to default of 10 days and tombstone-gc mode to 'repair'")
        with self.db_cluster.cql_connection_patient(node=self.db_node) as session:
            query = "ALTER TABLE scylla_bench.test with gc_grace_seconds = 864000 " \
                    f"and tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds':'{self.propagation_delay}'}};"
            session.execute(query)
        wait_for_propagation()
        _run_and_verify_stress()
        self._flush_nodes()
        self._run_major_compaction()
        self._repair_nodes()
        wait_for_ttl_and_propagation()
        self._run_major_compaction()
        self.log.info("verify no tombstones exist in post-repair-created sstables")
        table_repair_date, delta_repair_date_minutes = sstable_utils.get_table_repair_date_and_delta_minutes()
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        with compaction_ops.temporarily_disable_autocompaction_on_ks_cf(node=self.db_node, keyspace=self.keyspace,
                                                                        cf=self.table):
            sstables = sstable_utils.get_sstables(from_minutes_ago=delta_repair_date_minutes)
            self.log.debug('Starting sstable dump to verify correctness of tombstones for %s sstables',
                           len(sstables))
            sstable_utils.verify_post_repair_ttl_expired_tombstones(
                table_repair_date=table_repair_date, sstables=sstables)

        # Testing gc mode 'immediate'
        self._drop_and_recreate_keyspace()
        self.log.info("Change tombstone-gc mode to 'immediate'")
        with self.db_cluster.cql_connection_patient(node=self.db_node) as session:
            query = "ALTER TABLE scylla_bench.test with tombstone_gc = {'mode': 'immediate', 'propagation_delay_in_seconds':'300'};"
            session.execute(query)

        self.log.info('Wait a duration of schema-agreement and propagation_delay_in_seconds')
        self.db_cluster.wait_for_schema_agreement()
        wait_for_propagation()
        _run_and_verify_stress()
        wait_for_ttl_and_propagation()
        self.db_node.run_nodetool(f"flush -- {self.keyspace}")
        self.log.info('Run a major compaction for user-table on node')
        self.db_node.run_nodetool("compact", args=f"{self.keyspace} {self.table}")
        self.wait_no_compactions_running()
        self.log.info('Verify no compacted tombstones in sstables')
        sstables = sstable_utils.get_sstables()
        self.log.debug('Starting sstabledump to verify correctness of tombstones for %s sstables',
                       len(sstables))
        for sstable in sstables:
            tombstone_deletion_info = sstable_utils.get_ttl_expired_tombstone_deletion_info(sstable=sstable)
            assert not tombstone_deletion_info, f"Found unexpected existing tombstones: {tombstone_deletion_info} for sstable: {sstable}"

        # Testing gc mode 'disabled'
        self._drop_and_recreate_keyspace()
        self.log.info("change gc-grace-seconds to 240 and tombstone-gc mode to 'disabled'")
        with self.db_cluster.cql_connection_patient(node=self.db_node) as session:
            query = "ALTER TABLE scylla_bench.test with gc_grace_seconds = 240 " \
                    f"and tombstone_gc = {{'mode': 'disabled', 'propagation_delay_in_seconds':'{self.propagation_delay}'}};"
            session.execute(query)
        wait_for_propagation()
        _run_and_verify_stress()
        wait_for_ttl_and_propagation()
        self.db_node.run_nodetool(f"flush -- {self.keyspace}")

        self.log.info('Count the initial number of tombstones')
        tombstone_num_pre_repair = sstable_utils.count_tombstones()
        self._run_repair_and_major_compaction()
        tombstone_num_post_repair = sstable_utils.count_tombstones()
        assert tombstone_num_post_repair >= tombstone_num_pre_repair, \
            f"Found unexpected fewer tombstones: {tombstone_num_post_repair} / {tombstone_num_pre_repair}"
