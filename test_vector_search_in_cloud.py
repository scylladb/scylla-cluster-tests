import ast
import random
import time
from functools import cached_property
from threading import Thread
from typing import Optional

import cassandra

from sdcm.mgmt.helpers import get_schema_create_statements_from_file, substitute_dc_name_in_ks_statements_if_different
from sdcm.tester import ClusterTester
from sdcm.utils.common import S3Storage
from sdcm.utils.file import File

# Vector Search test data files
BUCKET_NAME = "vector-store-in-cloud"

TEST_DATA_FILENAME = "test_data.txt"
GROUND_TRUTH_FILENAME = "ground_truth.txt"
DATASET_FILENAME = "dataset.txt"
SCHEMA_FILENAME = "schema.json"

DATA_DIR_PATH = "data_dir/vector_search/"
TEST_DATA_FILE_PATH = DATA_DIR_PATH + TEST_DATA_FILENAME
GROUND_TRUTH_FILE_PATH = DATA_DIR_PATH + GROUND_TRUTH_FILENAME
DATASET_FILE_PATH = DATA_DIR_PATH + DATASET_FILENAME
SCHEMA_FILE_PATH = DATA_DIR_PATH + SCHEMA_FILENAME

ANN_SEARCH_CMD_TEMPLATE = "SELECT id FROM vdb_bench.vdb_bench_collection ORDER BY vector ANN OF {vector} LIMIT 10;"

VECTOR_SEARCH_INSTANCE_TYPES = {
    "aws": {"t4g.small": 175, "t4g.medium": 176, "r7g.medium": 177},
    "gce": {"e2-small": 178, "e2-medium": 179, "n4-highmem-2": 180}
}


class VectorSearchInCloudBase(ClusterTester):
    average_recall: float = 0.0

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"average_recall": self.average_recall})

        return email_data

    @staticmethod
    def download_vector_search_test_data_from_s3():
        s3_storage = S3Storage(bucket=BUCKET_NAME)

        base_url = f"https://{BUCKET_NAME}.s3.amazonaws.com"
        s3_storage.download_file(link=f"{base_url}/{TEST_DATA_FILENAME}", dst_dir=DATA_DIR_PATH)
        s3_storage.download_file(link=f"{base_url}/{GROUND_TRUTH_FILENAME}", dst_dir=DATA_DIR_PATH)

        s3_storage.download_file(link=f"{base_url}/{SCHEMA_FILENAME}", dst_dir=DATA_DIR_PATH)
        s3_storage.download_file(link=f"{base_url}/{DATASET_FILENAME}", dst_dir=DATA_DIR_PATH)

    @cached_property
    def test_data(self):
        return File(TEST_DATA_FILE_PATH).readlines()

    @cached_property
    def ground_truth_data(self):
        return File(GROUND_TRUTH_FILE_PATH).readlines()

    def prepare_vector_search_index(self):
        self.log.info("Download Vector Search test data from S3")
        self.download_vector_search_test_data_from_s3()

        self.log.info("Create schema applying CQL statements")
        ks_statements, other_statements = get_schema_create_statements_from_file(schema_file_path=SCHEMA_FILE_PATH)

        # DC name written in the schema file may differ from the one used in the cluster under test
        # if run in region or cloud provider different from ones where backup was taken
        dc_under_test_name = next(iter(self.db_cluster.get_nodetool_status()))
        ks_statements = substitute_dc_name_in_ks_statements_if_different(ks_statements=ks_statements,
                                                                         current_dc_name=dc_under_test_name)

        with self.db_cluster.cql_connection_patient(node=self.db_cluster.nodes[0], verbose=False) as session:
            for cql_stmt in ks_statements + other_statements:
                session.execute(cql_stmt)

            self.log.info("Insert sample data into the table")
            insert_cmd_template = "INSERT INTO vdb_bench.vdb_bench_collection (id, vector) VALUES ({id}, {vector});"
            with open(DATASET_FILE_PATH, encoding="utf-8") as dataset_file:
                for line in dataset_file:
                    id_str, vector_str = line.strip().split(",", maxsplit=1)
                    insert_cmd = insert_cmd_template.format(id=int(id_str), vector=vector_str)
                    session.execute(insert_cmd)

        self.log.info("Create Vector Index")
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            query = ("CREATE CUSTOM INDEX vdb_bench_collection_vector_idx ON vdb_bench.vdb_bench_collection(vector) "
                     "USING 'vector_index'")
            session.execute(query)

        # TODO: rework when it'll be possible to check index readiness status
        self.log.info("Sleep 200 seconds for indexes to be built")
        time.sleep(200)

    def test_vector_search_recall(self, queries_num: Optional[int] = None, duration: Optional[int] = None):

        assert (queries_num is not None) ^ (duration is not None), \
            "Either queries_num or duration must be provided, but not both"

        total_recall = 0

        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0], verbose=False) as session:
            def _run_ann_search():
                nonlocal total_recall
                index = random.randint(0, len(self.test_data) - 1)
                vector_value = self.test_data[index]

                query = ANN_SEARCH_CMD_TEMPLATE.format(vector=vector_value)
                self.log.debug("Running verification request #%d", query_num)
                rows = session.execute(query).all()

                actual_ids = [row.id for row in rows]
                ground_truth_ids = ast.literal_eval(self.ground_truth_data[index])[:10]
                assert len(actual_ids) == len(ground_truth_ids), \
                    f"Mismatch in number of returned IDs ({len(actual_ids)}) and ground truth IDs ({len(ground_truth_ids)})"

                # Count how many elements from actual_ids are present in ground_truth_ids (order doesn't matter)
                elements_present = sum(1 for actual_id in actual_ids if actual_id in ground_truth_ids)

                query_recall = elements_present / len(actual_ids)
                total_recall += query_recall

                self.log.debug("Query #%d: %d/%d elements present, recall factor: %.2f",
                               query_num, elements_present, len(actual_ids), query_recall)

            if queries_num is not None:
                for query_num in range(1, queries_num + 1):
                    _run_ann_search()

                overall_queries_num = queries_num
            else:
                query_num = 0
                end_time = time.time() + duration
                while time.time() < end_time:
                    query_num += 1
                    _run_ann_search()

                overall_queries_num = query_num

        average_recall = round(total_recall / overall_queries_num, 3)
        self.average_recall = average_recall

        self.log.info("Average recall for %d verification queries: %f", overall_queries_num, average_recall)
        assert average_recall > 0.85, f"Average recall {average_recall} is below the expected threshold of 0.85"


class VectorSearchInCloudSanity(VectorSearchInCloudBase):
    def test_vector_search_in_cloud_sanity(self):
        self.log.info("Prepare Vector Search data")
        self.prepare_vector_search_index()

        self.log.info("Run Vector Search verification queries")
        self.test_vector_search_recall(queries_num=100)


class VectorSearchInCloudReplaceNode(VectorSearchInCloudBase):
    def _test_replace_healthy_node(self,
                                   server_id: str,
                                   instance_type_id: Optional[int] = None,
                                   version: Optional[str] = None):
        self.log.debug("Start Vector Search verification queries in background thread")
        recall_test_thread = Thread(target=self.test_vector_search_recall, kwargs={"duration": 300})
        recall_test_thread.start()

        self.log.debug("Replace Vector Search node %s - new instance type ID %s, new version %s",
                       server_id, instance_type_id, version)
        self.db_cluster.replace_vector_search_node(
            server_id=server_id,
            instance_type_id=instance_type_id,
            service_version=version,
        )

        recall_test_thread.join(timeout=300)

    def test_vector_search_in_cloud_replace_healthy_node(self):
        self.log.info("Prepare Vector Search data")
        self.prepare_vector_search_index()

        vs_nodes_server_ids = iter(self.params.get("vector_search_node_ids"))

        test_desc = "Run VS node replacement, no changes to instance type or version"
        with self.subTest(test_desc):
            self.log.info(test_desc)
            self._test_replace_healthy_node(server_id=next(vs_nodes_server_ids))

        vs_instance_type_ids = list(VECTOR_SEARCH_INSTANCE_TYPES[self.params.get("cluster_backend")].values())
        current_vs_instance_type = self.params.get("vector_search_node_type_id")
        vs_instance_type_ids.remove(current_vs_instance_type)  # Avoid replacing with the same type
        for instance_type_id in vs_instance_type_ids:
            test_desc = f"Run VS node replacement changing instance type ID to {instance_type_id}"
            with self.subTest(test_desc):
                self.log.info(test_desc)
                self._test_replace_healthy_node(
                    server_id=next(vs_nodes_server_ids),
                    instance_type_id=instance_type_id,
                )

    def _test_replace_broken_node(self, server_id: str):
        vector_value = random.choice(self.test_data)
        query = ANN_SEARCH_CMD_TEMPLATE.format(vector=vector_value)

        failed_queries_num, passed_queries_num = 0, 0
        stop_thread = False

        def _run_ann_search():
            nonlocal failed_queries_num, passed_queries_num

            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0], verbose=False) as session:
                while not stop_thread:
                    try:
                        session.execute(query)
                        passed_queries_num += 1
                    except cassandra.InvalidRequest as e:
                        self.log.warning(f"ANN search request failed: {e}")
                        failed_queries_num += 1
                    time.sleep(1)

        ann_search_thread = Thread(target=_run_ann_search)
        ann_search_thread.start()

        try:
            self.log.info("vector-store should be stopped MANUALLY on server ID %s, sleeping 10 mins...", server_id)
            # Wait for failures to start occurring (max 10 minutes)
            end_time = time.time() + 600
            while failed_queries_num == 0 and time.time() < end_time:
                time.sleep(10)

            passed_before_replace = passed_queries_num
            failed_before_replace = failed_queries_num
            self.log.debug("Before replacement requests status: passed - %d, failed - %d",
                           passed_before_replace, failed_before_replace)
            assert failed_before_replace > 0, "ANN search queries didn't fail after stopping vector-store service"

            self.log.info("Replace Vector Search node %s", server_id)
            self.db_cluster.replace_vector_search_node(server_id=server_id)

            passed_after_replace = passed_queries_num
            failed_after_replace = failed_queries_num
            self.log.debug("After replacement requests status: passed - %d, failed - %d",
                           passed_after_replace, failed_after_replace)
            assert passed_after_replace > passed_before_replace, "No ANN search queries passed during node replacement"
            assert failed_after_replace > failed_before_replace, "ANN search didn't fail during node replacement"

            self.log.debug("Run ANN queries for 1 more minute after replacement to make sure no failures occur")
            time.sleep(60)
            assert failed_after_replace == failed_queries_num, "ANN search is still failing after node replacement"

        finally:
            stop_thread = True
            ann_search_thread.join(timeout=30)

    def test_vector_search_in_cloud_replace_broken_node(self):
        """
        GIVEN a Cloud cluster with Vector Search nodes
        WHEN replacing a broken Vector Search node (vector-store service stopped)
        THEN the operation is successful and Vector Search is fully functional after replacement;
             during the replacement some availability loss is expected for now (when the request hits the broken node)
        Note: the test scenario requires MANUAL stop of vector-store service on one of the VS nodes since ssh access
              to VS node is not supported by siren-tests + SCT integration at the moment
        """
        self.log.info("Prepare Vector Search data")
        self.prepare_vector_search_index()

        vs_nodes_server_ids = self.params.get("vector_search_node_ids")

        self.log.info("Run VS node replacement for broken instance")
        self._test_replace_broken_node(server_id=vs_nodes_server_ids[0])
