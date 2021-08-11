import random
import re
from collections import namedtuple
from typing import Any, List, Iterable

from sdcm.keystore import KeyStore
from sdcm.utils.common import remote_get_file, LOGGER
from sdcm.utils.decorators import timeout as timeout_decor
from sdcm.utils.sstable.load_inventory import (TestDataInventory, BIG_SSTABLE_COLUMN_1_DATA, COLUMN_1_DATA,
                                               MULTI_NODE_DATA, BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA)


class SstableLoadUtils:
    LOAD_AND_STREAM_RUN_EXPR = r'storage_service - load_and_stream:'
    LOAD_AND_STREAM_DONE_EXPR = r'storage_service - Done loading new SSTables for keyspace={}, table={}, ' \
                                r'load_and_stream=true.*status=(.*)'

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
    def upload_sstables(node, test_data: TestDataInventory, keyspace_name: str = 'keyspace1'):
        key_store = KeyStore()
        creds = key_store.get_scylladb_upload_credentials()
        # Download the sstable files from S3
        remote_get_file(node.remoter, test_data.sstable_url, test_data.sstable_file,
                        hash_expected=test_data.sstable_md5, retries=2,
                        user_agent=creds['user_agent'])
        result = node.remoter.sudo(f"ls -t /var/lib/scylla/data/{keyspace_name}/")
        upload_dir = result.stdout.split()[0]
        if node.is_docker():
            node.remoter.run(f'tar xvfz {test_data.sstable_file} -C /'
                             f'var/lib/scylla/data/{keyspace_name}/{upload_dir}/upload/')
        else:
            node.remoter.sudo(
                f'tar xvfz {test_data.sstable_file} -C /var/lib/scylla/data/{keyspace_name}/{upload_dir}/upload/',
                user='scylla')

        # Scylla Enterprise 2019.1 doesn't support to load schema.cql and manifest.json, let's remove them
        node.remoter.sudo(f'rm -f /var/lib/scylla/data/{keyspace_name}/{upload_dir}/upload/schema.cql')
        node.remoter.sudo(f'rm -f /var/lib/scylla/data/{keyspace_name}/{upload_dir}/upload/manifest.json')

    @classmethod
    def run_load_and_stream(cls, node, keyspace_name: str = 'keyspace1', table_name: str = 'standard1'):
        system_log_follower = node.follow_system_log(
            patterns=[cls.LOAD_AND_STREAM_DONE_EXPR.format(keyspace_name, table_name),
                      cls.LOAD_AND_STREAM_RUN_EXPR])
        LOGGER.info("Running load and stream on the node %s for %s.%s'", node.name, keyspace_name, table_name)

        # `load_and_stream` parameter is not supported by nodetool yet. This is workaround
        # https://github.com/scylladb/scylla-tools-java/issues/253
        load_api_cmd = 'curl -X POST --header "Content-Type: application/json" --header ' \
                       f'"Accept: application/json" "http://127.0.0.1:10000/storage_service/sstables/{keyspace_name}?' \
                       f'cf={table_name}&load_and_stream=true"'
        node.remoter.run(load_api_cmd)
        return system_log_follower

    @staticmethod
    def run_refresh(node, test_data: namedtuple) -> Iterable[str]:
        LOGGER.debug('Loading %s keys to %s by refresh', test_data.keys_num, node.name)
        # Resharding of the loaded sstable files is performed before they are moved from upload to the main folder.
        # So we need to validate that resharded files are placed in the "upload" folder before moving.
        # Find the compaction output that reported about the resharding

        system_log_follower = node.follow_system_log(patterns=[r'Resharded.*\[/'])
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
    @timeout_decor(
        timeout=60,
        allowed_exceptions=(AssertionError,),
        message="Waiting for load_and_stream completion message to appear in logs")
    def validate_load_and_stream_status(cls, node, system_log_follower,
                                        keyspace_name='keyspace1', table_name='standard1'):
        """
        Validate that load_and_stream was started and has been completed successfully.
        Search for a messages like:
            storage_service - load_and_stream: ops_uuid=c06c76bb-d178-4e9c-87ff-90df7b80bd5e, ks=keyspace1, table=standard1,
            target_node=10.0.3.31, num_partitions_sent=45721, num_bytes_sent=24140688

           storage_service - Done loading new SSTables for keyspace=keyspace1, table=standard1, load_and_stream=true,
           primary_replica_only=false, status=succeeded
        """
        load_and_stream_messages = list(system_log_follower)
        assert load_and_stream_messages, f"Load and stream wasn't run on the node {node.name}"
        LOGGER.debug("Found load_and_stream messages: %s", load_and_stream_messages)

        load_and_stream_started = False
        load_and_stream_status = ""
        for line in load_and_stream_messages:
            if not load_and_stream_started and re.findall(cls.LOAD_AND_STREAM_RUN_EXPR, line):
                load_and_stream_started = True

            load_and_stream_done = re.search(cls.LOAD_AND_STREAM_DONE_EXPR.format(keyspace_name, table_name), line)
            if load_and_stream_done:
                load_and_stream_status = load_and_stream_done.groups()[0]
                break

        assert load_and_stream_started, f'Load and stream has not been run on the node {node.name}'
        assert load_and_stream_status == 'succeeded', \
            f'Load and stream status  on the node {node.name} is "{load_and_stream_status}". Expected "succeeded"'

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
