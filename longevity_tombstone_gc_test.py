import time

from longevity_twcs_test import TWCSLongevityTest
from sdcm.utils.table_utils import wait_until_user_table_exists, get_sstables, count_sstable_tombstones


def count_tombstones(db_node, ks_cf: str):
    sstables = get_sstables(db_node=db_node, ks_cf=ks_cf)
    tombstones_num = 0
    for sstable in sstables:
        tombstones_num += count_sstable_tombstones(db_node=db_node, sstable=sstable)
    return tombstones_num


class TombstoneGcLongevityTest(TWCSLongevityTest):

    def test_switch_tombstone_gc_modes(self):
        """
        test steps:
        -----------
        Based on TWCS TTL 48h longevity configuration with a much shorter TTL (few minutes) and same for gc-grace-seconds.
        Start with tombstone-gc-mode disabled
        wait a duration of TTL * 2.
        count number of tombstones.
        run a repair + a major compaction.
        count again to see not fewer tombstones exit.
        change gc-grace-seconds back to default of 10 days.
        change gc-mode to repair
        run a repair + wait propagation delay + run a major compaction.
        verify no tombstones exist in post-repair-created sstables.
        change gc-mode to immediate.
        wait for load to end + TTL period.
        Run a major compaction.
        Verify no tombstones.
        """

        self.create_tables_for_scylla_bench()
        db_node = self.db_cluster.nodes[0]
        keyspace = 'scylla_bench'
        table = 'test'
        ks_cf = f'{keyspace}.{table}'
        wait_until_user_table_exists(db_node=db_node, table_name=ks_cf)
        self.run_post_prepare_cql_cmds()
        stress_queue = []

        stress_cmd = self.params.get('stress_cmd')
        params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}
        self._run_all_stress_cmds(stress_queue, params)

        self.log.info('Wait a duration of TTL * 2 + propagation_delay_in_seconds')
        wait_for_tombstones = 5 * 60 * 4
        time.sleep(wait_for_tombstones)

        self.log.info('Count the initial number of tombstones')
        tombstone_num_pre_repair = count_tombstones(db_node=db_node, ks_cf=ks_cf)

        self.log.info('Run a repair for user-table on node')
        db_node.run_nodetool(sub_cmd="repair", args=f"-- {keyspace}")

        self.log.info('Run a major compaction for user-table on node')
        db_node.run_nodetool("compact", args=f"{keyspace} {table}")
        self.wait_no_compactions_running()

        tombstone_num_post_repair = count_tombstones(db_node=db_node, ks_cf=ks_cf)
        assert tombstone_num_post_repair >= tombstone_num_pre_repair, \
            f"Found unexpected fewer tombstones: {tombstone_num_post_repair} / {tombstone_num_pre_repair}"
        # TODO: implement

        self.log.info('Wait for s-b load to finish')
        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)
