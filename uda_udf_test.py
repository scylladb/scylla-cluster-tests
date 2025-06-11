from typing import NamedTuple, Callable

from sdcm.cluster import BaseNode
from sdcm.send_email import LongevityEmailReporter
from sdcm.stress_thread import CassandraStressThread
from sdcm.tester import ClusterTester
from sdcm.utils.udf import UDFS
from sdcm.utils.uda import UDAS


class UDVerification(NamedTuple):
    name: str
    query: str
    verifier_func: Callable


class UDAUDFTest(ClusterTester):
    """
    Test Scylla with User Defined Functions and User Defined Aggregates,
    using cassandra-stress.
    """
    KEYSPACE_NAME = "ks"
    CF_NAME = "uda_udf"

    def setUp(self):
        super().setUp()
        self.email_reporter = LongevityEmailReporter(email_recipients=self.params.get('email_recipients'),
                                                     logdir=self.logdir)

    def test_uda_and_udf(self) -> None:
        self.log.info("Starting UDA/UDF test...")
        self.prewrite_db_with_data()
        node: BaseNode = self.db_cluster.get_node()

        for udf in UDFS.values():
            self.log.info("Creating the following UDF: %s", udf.name)
            cmd = udf.get_create_query(ks=self.KEYSPACE_NAME)
            with self.db_cluster.cql_connection_patient(node=node) as session:
                session.execute(cmd)
            self.log.info("UDF %s created.", udf.name)

        self._verify_udf_functions()

        for uda in UDAS.values():
            cmd = uda.get_create_query_string(ks=self.KEYSPACE_NAME)
            node.run_cqlsh(cmd=cmd)
            self.log.info("UDA %s created.", uda.name)

        self._verify_uda_aggregates()

        uda_udf_thread = self.run_uda_udf_thread()

        # wait for stress to complete
        self.verify_stress_thread(uda_udf_thread)
        self.log.info("Test completed")

    def run_uda_udf_thread(self) -> CassandraStressThread:
        self.log.info("Running mixed workload c-s thread alongside uda/udf stress thread...")
        uda_udf_cmd = self.params.get('stress_cmd')[0]
        uda_udf_thread = self.run_stress_thread(stress_cmd=uda_udf_cmd,
                                                stats_aggregate_cmds=False,
                                                round_robin=False)
        self.log.info("Stress threads started.")
        return uda_udf_thread

    def prewrite_db_with_data(self) -> None:
        self.log.info("Prewriting database...")
        stress_cmd = self.params.get('prepare_write_cmd')
        pre_thread = self.run_stress_thread(stress_cmd=stress_cmd, stats_aggregate_cmds=False, round_robin=False)
        self.verify_stress_thread(pre_thread)
        self.log.info("Database pre write completed")

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = self._get_common_email_data()

        return email_data

    def _verify_udf_functions(self):
        row_query = UDVerification(name="row_query",
                                   query=f"SELECT * FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                                   verifier_func=lambda c2, c3, c7: all([c2, c3, c7]))
        verifications = [
            UDVerification(name="lua_var_length_counter",
                           query=f"SELECT {self.KEYSPACE_NAME}.lua_var_length_counter(c7_text) AS result "
                           f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                           verifier_func=lambda c2, c3, c7, query_response: len(c7) == query_response.result),
            UDVerification(name="wasm_plus",
                           query=f"SELECT {self.KEYSPACE_NAME}.wasm_plus(c2_int, c3_int) AS result "
                           f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                           verifier_func=lambda c2, c3, c7, query_response: c2 + c3 == query_response.result),
            UDVerification(name="wasm_div",
                           query=f"SELECT {self.KEYSPACE_NAME}.wasm_div(c2_int, c3_int) AS result "
                           f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                           verifier_func=lambda c2, c3, c7, query_response: c2 // c3 == query_response.result)
        ]
        self.log.info("Starting UDF verifications...")

        with self.db_cluster.cql_connection_patient(self.db_cluster.get_node(), verbose=False) as session:
            row_result = session.execute(row_query.query).one()
            self.log.info("Row query was: %s", row_result)
            c2_value = row_result.c2_int
            c3_value = row_result.c3_int
            c7_value = row_result.c7_text
            assert row_query.verifier_func(c2_value, c3_value, c7_value), \
                "Expected row values to not be None, at least one them was. " \
                "c2_value: %s, c3_value: %s, c7 value: %s" % (c2_value, c3_value, c7_value)

            for verification in verifications:
                self.log.info("Running UDF verification: %s; query: %s", verification.name, verification.query)
                query_result = session.execute(verification.query).one()
                self.log.info("Verification query result: %s", query_result)
                assert verification.verifier_func(c2_value, c3_value, c7_value, query_result)
            self.log.info("Finished running UDF verifications.")

    def _verify_uda_aggregates(self):
        uda_verification = UDVerification(
            name="my_avg",
            query="SELECT ks.my_avg(c2_int) AS result FROM ks.uda_udf USING TIMEOUT 120s",
            verifier_func=lambda verification_query_result, avg_response: verification_query_result == avg_response)

        self.log.info("Running UDA verification: %s; query: %s...", uda_verification.name, uda_verification.query)
        with self.db_cluster.cql_connection_patient(self.db_cluster.get_node(), verbose=False) as session:
            avg_query = "SELECT AVG (c2_int) FROM ks.uda_udf"
            avg_result = session.execute(avg_query).one()
            verification_query_result = session.execute(uda_verification.query).one()

        assert uda_verification.verifier_func(verification_query_result, avg_result), \
            "UDA verifivation failed. UDA result: %s, Builtin AVG result: %s" % (verification_query_result, avg_result)
        self.log.info("Finished running UDA verifications.")
