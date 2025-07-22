import time

from longevity_test import LongevityTest
from sdcm.cluster import BaseNode
from sdcm.utils.alternator.consts import NO_LWT_TABLE_NAME
from sdcm.utils.common import skip_optional_stage


class AlternatorTtlLongevityTest(LongevityTest):

    keyspace = f"alternator_{NO_LWT_TABLE_NAME}"
    full_table_name = f'{keyspace}.{NO_LWT_TABLE_NAME}'

    def _count_sstables_and_partitions(self, node: BaseNode) -> int:
        cfstats = node.get_cfstats(self.full_table_name)
        estimated_num_of_partitions = cfstats['Number of partitions (estimate)']
        num_of_sstables = cfstats['SSTable count']
        self.log.info('Table stats results are: %s sstables, %s estimated partitions',
                      num_of_sstables, estimated_num_of_partitions)

        self.log.info('Run a table scan to count number of existing items')
        with self.db_cluster.cql_connection_patient(node=node, connect_timeout=600) as session:
            result = session.execute(f"SELECT count(*) FROM {self.full_table_name} using timeout 10m")
        partitions_num_result = result.current_rows[0].count
        self.log.info('Number of partitions found: %s', partitions_num_result)
        return partitions_num_result

    def count_sstables_and_partitions_after_major_compaction(self, run_repair: bool = True):
        if run_repair:
            self.log.info('Run a repair on nodes..')
            for node in self.db_cluster.nodes:
                node.run_nodetool(sub_cmd="repair -pr")

        node: BaseNode = self.db_cluster.nodes[0]
        self.log.info('force offstrategy on %s before running running a major compaction on node %s', self.keyspace,
                      node.name)
        node.remoter.run(
            "curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json'"
            f" http://127.0.0.1:10000/storage_service/keyspace_offstrategy_compaction/{self.keyspace}")
        self.log.info('Run a major compaction on node %s', node.name)
        node.run_nodetool("compact")
        self.wait_no_compactions_running()
        return self._count_sstables_and_partitions(node=node)

    def test_count_sstables_after_major_compaction(self):
        """
        This test run the original test_custom_time first.
        Waits a duration of TTL + scan + gc_grace_seconds.
        Assume TTL=36 minutes, scan=8 minutes, gc_grace_seconds=4 minutes: Total sums to: 48 minutes.
        Run a repair on nodes.
        Select a node and run a major compaction.
        Wait for node's compactions to finish.
        count and print number of sstables, partitions and sizes.
        """

        # Run the stress_cmd and wait for complete.
        self.test_custom_time()
        # Wait for all data to be expired.
        # Should wait a total of 48 minutes.
        # splitting to 30 minutes wait before stopping nemesis.
        # Then an additional 18 minutes wait to complete a total of 48 minutes.
        self.log.info('Wait TTL + scan interval + gc_grace_seconds')
        wait_for_expired_items = 30 * 60
        time.sleep(wait_for_expired_items)
        self.stop_nemesis(self.db_cluster)
        wait_for_expired_items = 18 * 60
        time.sleep(wait_for_expired_items)

        # Run a repair + a major compaction and count results.
        self.count_sstables_and_partitions_after_major_compaction()

    def test_custom_time_repeat_stress_cmd(self):
        """
        This test run the original test_custom_time first.
        Then after finished it re-execute the same stress_cmd again.
        An example for a relevant scenario for that is for Alternator TTL that requires overwriting existing TTL data
        after a while, where some items are already expired and/or scanned.
        """

        # Run the stress_cmd a first time
        self.test_custom_time()

        # Rerun the stress_cmd a second time
        stress_cmd = self.params.get('stress_cmd')
        stress_queue = []
        if stress_cmd and not skip_optional_stage('main_load'):
            params = {'stress_cmd': stress_cmd, 'round_robin': self.params.get('round_robin')}
            self._run_all_stress_cmds(stress_queue, params)

            for stress in stress_queue:
                self.verify_stress_thread(stress)

        self.count_sstables_and_partitions_after_major_compaction()

    def test_disable_enable_ttl_scan(self):
        """
        1. Run pre-create schema for Alternator, disabling TTL for tables.
        2. Run the original test_custom_time prepare step.
        3. Enable TTL for tables.
        4. Wait for TTL-scan intervals to run
        5. Run a background read stress while data is being expired.
        """

        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
        stress_queue = []
        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = 1
        self.pre_create_alternator_tables()
        # Disable TTL
        self.alternator.modify_alternator_ttl_spec(enabled=False, node=self.db_cluster.nodes[0])

        # Run write stress
        self.run_prepare_write_cmd()
        stress_cmd = self.params.get('stress_cmd')
        if stress_cmd and not skip_optional_stage('main_load'):
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_cmd,
                      'round_robin': self.params.get('round_robin')}
            self._run_all_stress_cmds(stress_queue, params)

        if not prepare_write_cmd or not self.params.get('nemesis_during_prepare'):
            self.db_cluster.start_nemesis()

        for stress in stress_queue:
            self.verify_stress_thread(stress)

        # Enable TTL
        self.alternator.modify_alternator_ttl_spec(enabled=True, node=self.db_cluster.nodes[0])
        stress_queue = []

        # Run read stress as a background thread while TTL scans are going over existing data.
        stress_read_cmd = self.params.get('stress_read_cmd')
        if stress_read_cmd and not skip_optional_stage('main_load'):
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_read_cmd}
            self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(stress)

        self.count_sstables_and_partitions_after_major_compaction()

    def test_multiple_ttl(self):
        """
        This test run the original test_custom_time first.
        It assumes multiple TTL values to run in stress commands.
        It then waits a TTL+scan-interval duration for the second longest TTL stress:
        2.5 hours (second longer stress TTL) + 4 minutes (scan-interval) + 10 minutes (max estimated scan duration)
        This sums up to: 2 hours and 45 minutes (=165 minutes).
        Then after finished it run a table full scan.
        It validates only the data of longest TTL stress (10 days) exist.
        """

        # Run the stress_cmd and wait for finish
        self.test_custom_time()

        self.log.info('Wait second-longer-stress TTL + scan-interval + max-estimated-scan-duration')
        wait_for_expired_items = 165 * 60
        time.sleep(wait_for_expired_items)

        stress_cmd = self.params.get('stress_cmd')
        longest_ttl_stress_cmd = stress_cmd[-1]
        insert_count = [param for param in longest_ttl_stress_cmd.split() if 'insertcount=' in param]
        insert_count = int(insert_count[0].split('=')[1])
        self.log.info('The longest-TTL-load number of items is: %s', insert_count)

        self.log.info('Run a major compaction on nodes for all expected expired data tombstones to be deleted.')
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")
        self.wait_no_compactions_running()

        self.log.info('Run a table scan to count number of existing items and compare to expected')
        current_partitions_num = self._count_sstables_and_partitions(node=self.db_cluster.nodes[0])
        assert current_partitions_num == insert_count, f"Result: {current_partitions_num}, Expected: {insert_count}"
