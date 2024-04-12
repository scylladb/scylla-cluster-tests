import os.path
import random
import re
from collections import namedtuple
from typing import Any, List, Iterable

from sdcm.keystore import KeyStore
from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import remote_get_file, LOGGER, RemoteTemporaryFolder
from sdcm.utils.decorators import timeout as timeout_decor
from sdcm.utils.sstable.load_inventory import (TestDataInventory, BIG_SSTABLE_COLUMN_1_DATA, COLUMN_1_DATA,
                                               MULTI_NODE_DATA, BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA)
from sdcm.utils.node import RequestMethods, build_node_api_command
from sdcm.wait import wait_for_log_lines

LOCAL_CMD_RUNNER = LocalCmdRunner()


class SstableLoadUtils:
    LOAD_AND_STREAM_RUN_EXPR = r'(?:storage_service|sstables_loader) - load_and_stream:'
    LOAD_AND_STREAM_DONE_EXPR = (
        r'(?:storage_service|sstables_loader) - '
        r'Done loading new SSTables for keyspace={}, table={}, '
        r'load_and_stream=true.*status=(.*)'
    )

    @staticmethod
    def calculate_columns_count_in_table(target_node, keyspace_name: str = 'keyspace1',
                                         table_name: str = 'standard1') -> int:
        query_cmd = f"SELECT * FROM {keyspace_name}.{table_name} LIMIT 1"
        result = target_node.run_cqlsh(query_cmd)
        return len(re.findall(r"(\| C\d+)", result.stdout))

    @staticmethod
    def get_random_item(items: list, pop: bool = False) -> Any:
        if pop:
            return items.pop(random.randint(0, len(items) - 1))

        return items[random.randint(0, len(items) - 1)]

    @classmethod
    def distribute_test_files_to_cluster_nodes(cls, nodes, test_data: List[TestDataInventory]) -> List:
        """
        Distribute test sstables over cluster nodes for `load-and-stream` test:
        the feature allow loading arbitrary sstables that do not belong to a node into the cluster.
        So we want to distribute sstables randomly.
        Also add case when the 50% sstables of node2 (from test data cluster) will be loaded on the one node
        and other 50% - on another
        """
        map_files_to_node = []
        nodes_bucket = nodes.copy()
        for data in test_data:
            if not nodes_bucket:
                nodes_bucket = nodes.copy()
            map_files_to_node.append([data, nodes_bucket.pop()])

        return map_files_to_node

    @staticmethod
    # pylint: disable=too-many-arguments,too-many-locals
    def upload_sstables(node, test_data: TestDataInventory, keyspace_name: str = 'keyspace1', table_name=None,
                        create_schema: bool = False, is_cloud_cluster=False, **kwargs):
        key_store = KeyStore()
        creds = key_store.get_scylladb_upload_credentials()
        # Download the sstable files from S3
        if is_cloud_cluster:
            local_temp_path = '/tmp/' + os.path.basename(test_data.sstable_file)
            try:
                remote_get_file(LOCAL_CMD_RUNNER, test_data.sstable_url, local_temp_path,
                                hash_expected=test_data.sstable_md5, retries=2,
                                user_agent=creds['user_agent'])
                node.remoter.send_files(local_temp_path, test_data.sstable_file)
            finally:
                LOCAL_CMD_RUNNER.run(f"rm -f {local_temp_path}", ignore_status=True)
        else:
            remote_get_file(node.remoter, test_data.sstable_url, test_data.sstable_file,
                            hash_expected=test_data.sstable_md5, retries=2,
                            user_agent=creds['user_agent'])

        if create_schema:
            with RemoteTemporaryFolder(node=node) as tmp_folder:
                # Extract tarball to temporary folder when test keyspace and table do not exist and need to be created
                node.remoter.run(f'tar xvfz {test_data.sstable_file} -C {tmp_folder.folder_name}/')
                SstableLoadUtils.create_keyspace(node=node,
                                                 replication_factor=kwargs["replication_factor"])

                SstableLoadUtils.create_table_for_load(node=node,
                                                       schema_file_and_path=f"{tmp_folder.folder_name}/schema.cql",
                                                       session=kwargs["session"])

        keyspace_folder = f"/var/lib/scylla/data/{keyspace_name}"
        command = f"ls -t {keyspace_folder}/" if not table_name else f"ls -d {keyspace_folder}/{table_name}-*"
        result = node.remoter.sudo(command)
        if not result:
            LOGGER.debug("Empty result for command: '%s'", command)
            raise NotADirectoryError(f"Not found table folder under '{keyspace_folder}' on the {node.name} node")

        upload_dir = result.stdout.split()[0]
        table_folder = f"/var/lib/scylla/data/{keyspace_name}/{upload_dir}" if not table_name else upload_dir

        # Extract tarball again (in case create_schema=True) directly to Scylla table upload folder to simplify the code
        node.remoter.sudo(f'tar xvfz {test_data.sstable_file} -C {table_folder}/upload/', user='scylla')

        # Scylla Enterprise 2019.1 doesn't support to load schema.cql and manifest.json, let's remove them
        node.remoter.sudo(f'rm -f {table_folder}/upload/schema.cql')
        node.remoter.sudo(f'rm -f {table_folder}/upload/manifest.json')

    @classmethod
    def run_load_and_stream(cls, node,  # pylint: disable=too-many-arguments
                            keyspace_name: str = 'keyspace1', table_name: str = 'standard1',
                            start_timeout=60, end_timeout=300):
        """runs load and stream using API request and waits for it to finish"""
        with wait_for_log_lines(node, start_line_patterns=[cls.LOAD_AND_STREAM_RUN_EXPR],
                                end_line_patterns=[cls.LOAD_AND_STREAM_DONE_EXPR.format(keyspace_name, table_name)],
                                start_timeout=start_timeout, end_timeout=end_timeout):
            LOGGER.info("Running load and stream on the node %s for %s.%s'", node.name, keyspace_name, table_name)

            # `load_and_stream` parameter is not supported by nodetool yet. This is workaround
            # https://github.com/scylladb/scylla-tools-java/issues/253
            path = f'/storage_service/sstables/{keyspace_name}?cf={table_name}&load_and_stream=true'
            load_api_cmd = build_node_api_command(path_url=path, request_method=RequestMethods.POST, silent=False)
            node.remoter.run(load_api_cmd)

    @staticmethod
    def run_refresh(node, test_data: namedtuple) -> Iterable[str]:
        LOGGER.debug('Loading %s keys to %s by refresh', test_data.keys_num, node.name)
        # Resharding of the loaded sstable files is performed before they are moved from upload to the main folder.
        # So we need to validate that resharded files are placed in the "upload" folder before moving.
        # Find the compaction output that reported about the resharding

        system_log_follower = node.follow_system_log(patterns=[r'Resharded.*'])
        node.run_nodetool(sub_cmd="refresh", args="-- keyspace1 standard1")
        return system_log_follower

    @staticmethod
    @timeout_decor(
        timeout=60,
        allowed_exceptions=(AssertionError,),
        message="Waiting for resharding completion message to appear in logs")
    def validate_resharding_after_refresh(node, system_log_follower):
        """
        # Validate that files after resharding were saved in the "upload" folder.
        # Example of compaction output:

        #   scylla[6653]:  [shard 0] compaction - [Reshard keyspace1.standard1 3cad4140-f8c3-11ea-acb1-000000000002]
        #   Resharded 1 sstables to [
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-9-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-10-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-11-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-12-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-13-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-22-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-15-big-Data.db:level=0,
        #   /var/lib/scylla/data/keyspace1/standard1-9fbed8d0f8c211ea9bb1000000000000/upload/md-16-big-Data.db:level=0,
        #   ]. 91MB to 92MB (~100% of original) in 5009ms = 18MB/s. ~370176 total partitions merged to 370150

        Starting with Scylla 4.7 messages have changed to the following:
        [shard 1] sstables_loader - Loading new SSTables for keyspace=keyspace1, table=standard1, ...
        [shard 1] database - Resharding 223kB for keyspace1.standard1
        [shard 1] database - Resharded 223kB for keyspace1.standard1 in 0.14 seconds, 1MB/s
        [shard 1] database - Loaded 16 SSTables into /var/lib/scylla/data/keyspace1/standard1-eb0401905d8311ecb391aa52ebf0b3e1
        [shard 1] sstables_loader - Done loading new SSTables for keyspace=keyspace1, table=standard1, ...

        So, there is no per-file paths anymore for resharding log messages, only root dir path.
        """
        resharding_logs = list(system_log_follower)
        assert resharding_logs, f"Resharding wasn't run on the node {node.name}"
        LOGGER.debug("Found resharding on the node %s: %s", node.name, resharding_logs)

        for line in resharding_logs:
            # Find all files that were created after resharding
            for one_file in re.findall(r"(/var/.*?),", line, re.IGNORECASE):
                # The file path have to include "upload" folder
                assert '/upload/' in one_file, \
                    f"Loaded file was resharded not in 'upload' folder on the node {node.name}"

    @classmethod
    def get_load_test_data_inventory(cls, column_number: int, big_sstable: bool,
                                     load_and_stream: bool) -> List[TestDataInventory]:
        if column_number == 1:
            # Use special schema (one column) for refresh before https://github.com/scylladb/scylla/issues/6617 is fixed
            if big_sstable:
                return BIG_SSTABLE_COLUMN_1_DATA

            return COLUMN_1_DATA

        if column_number >= 5:
            # The snapshot has 5 columns, the snapshot (col=5) can be loaded to table (col > 5).
            # they rest columns will be filled to 'null'.
            if load_and_stream:
                return MULTI_NODE_DATA

            if big_sstable:
                return BIG_SSTABLE_MULTI_COLUMNS_DATA

            return MULTI_COLUMNS_DATA

        return []

    @classmethod
    def create_keyspace(cls, node, keyspace_name: str = "keyspace1", strategy: str = 'NetworkTopologyStrategy',
                        replication_factor: int = 1):
        node.run_cqlsh(
            "CREATE KEYSPACE %s WITH replication = {'class': '%s', 'replication_factor': %s}" % (
                keyspace_name, strategy, replication_factor))

    @classmethod
    def create_table_for_load(cls, node, schema_file_and_path: str, session):
        schema = node.remoter.run(f"cat {schema_file_and_path}").stdout
        session.execute(schema.replace("\n", ""))

    @classmethod
    def validate_data_count_after_upload(cls, node, keyspace_name: str = "keyspace1", table_name: str = 'standard1'):
        result = node.run_cqlsh(f"consistency QUORUM;SELECT COUNT(*) FROM {keyspace_name}.{table_name}")

        next_line_is_result = False
        # Stdout expected example:
        #   'Consistency level set to QUORUM.
        #
        #  count
        # -------
        #      0
        #
        # (1 rows)
        # '
        for line in result.stdout.split("\n"):
            if next_line_is_result:
                LOGGER.debug("Rows amount in the %s.%s table is: %s", keyspace_name, table_name, line.strip())
                return line.strip()

            if "----" in line:
                next_line_is_result = True

        raise ValueError(f"Failed to receive rows count. Stdout: {result.stdout}. Stderr: {result.stderr}")
