#
# This is stress longevity test that runs light weight transactions in parallel with different node operations:
# disruptive and not disruptive
#
# After the test is finished will be performed the data validation.
import time

from longevity_test import LongevityTest
from sdcm.utils.data_validator import LongevityDataValidator


class TWCSLongevityTest(LongevityTest):
    BASE_TABLE_PARTITION_KEYS = ['domain', 'published_date']

    def __init__(self, *args):
        super(TWCSLongevityTest, self).__init__(*args)


    def create_ks_and_udf(self):
        rf = 1
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:

            session.execute(f"CREATE KEYSPACE IF NOT EXISTS keyspace1"
                            f" WITH replication={{'class':'SimpleStrategy', 'replication_factor':{rf}}}")
            session.execute(f"USE keyspace1")
            session.execute(f"""CREATE OR REPLACE FUNCTION   minutesAgo(ago int, now bigint)
                            RETURNS NULL ON NULL INPUT
                            RETURNS bigint
                            LANGUAGE Lua
                            AS 'return now - 60000 * ago';""")
            session.execute(f"""CREATE OR REPLACE FUNCTION   bucket(now bigint)
                            RETURNS NULL ON NULL INPUT
                            RETURNS bigint
                            LANGUAGE Lua 
                            AS 'return (now / 60000 / 5 + 5) * 60000 * 5';""")


    def run_prepare_write_cmd(self):
        self.create_ks_and_udf()
        super(TWCSLongevityTest, self).run_prepare_write_cmd()


        # Run nemesis during stress as it was stopped before copy expected data
        if self.params.get('nemesis_during_prepare'):
            self.start_nemesis()

    def start_nemesis(self):
        self.db_cluster.start_nemesis()

    def test_twcs_longevity(self):
        self.test_custom_time()

        # Stop nemesis. Prefer all nodes will be run before collect data for validation
        # Increase timeout to wait for nemesis finish
        if self.db_cluster.nemesis_threads:
            self.db_cluster.stop_nemesis(timeout=300)
