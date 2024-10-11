#
# This is stress longevity test that runs scylla-benchtime window load  with different node operations:
# disruptive and not disruptive

from longevity_test import LongevityTest
from sdcm.sct_events.group_common_events import ignore_mutation_write_errors
from sdcm.utils.common import skip_optional_stage


class TWCSLongevityTest(LongevityTest):
    def create_tables_for_scylla_bench(self, window_size=1, ttl=900):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute("""
                CREATE KEYSPACE scylla_bench WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}
                AND durable_writes = true;""")
            session.execute(f"""
                CREATE TABLE scylla_bench.test (
                    pk bigint,
                    ck bigint,
                    v  blob,
                    PRIMARY KEY (pk, ck)
                ) WITH CLUSTERING ORDER BY (ck ASC)
                    AND default_time_to_live = {ttl}
                    AND compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '{window_size}',
                    'compaction_window_unit': 'MINUTES'}}""")

    def run_prepare_write_cmd(self):
        if not skip_optional_stage('prepare_write'):
            self.create_tables_for_scylla_bench()
            if self.params.get("prepare_write_cmd"):
                # run_prepare_write_cmd executes run_post_prepare_cql_cmds
                # it doesn't need to call run_post_prepare_cql_cmds twice.
                with ignore_mutation_write_errors():
                    super().run_prepare_write_cmd()
            else:
                self.run_post_prepare_cql_cmds()

        # Run nemesis during stress as it was stopped before copy expected data
        if self.params.get('nemesis_during_prepare'):
            self.db_cluster.start_nemesis()

    def test_twcs_longevity(self):
        self.test_custom_time()

        # Stop nemesis. Prefer all nodes will be run before collect data for validation
        # Increase timeout to wait for nemesis finish
        if self.db_cluster.nemesis_threads:
            self.db_cluster.stop_nemesis(timeout=300)
