import math
import re

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from longevity_test import LongevityTest
from sdcm import wait
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.tablets.common import get_tablets_count
from test_lib.scylla_bench_tools import create_scylla_bench_table_query

KEYSPACE_NAME = 'scylla_bench'
TABLE_NAME = 'test'
KS_CF = f"{KEYSPACE_NAME}.{TABLE_NAME}"


class TabletsSplitMergeTest(LongevityTest):

    def _get_tablets_number(self) -> int:
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            tablets_number = get_tablets_count(keyspace=KEYSPACE_NAME, session=session)
        self.log.debug(f"Tablets number for {KEYSPACE_NAME} is: {tablets_number}")
        return tablets_number

    def _get_max_expected_tablets(self, stress_cmd, target_tablet_size_in_bytes) -> int:
        """
        Example calculation:
        partition_count = 500
        clustering_row_count = 5555
        clustering_row_size = 1884 bytes
        target_tablet_size_in_bytes = 1073741824 bytes

        Total Data Size=500×5555×1884=5,236,230,000 bytes≈5.2GB
        Total Data Size=500×5555×1884=5,236,230,000bytes≈5.2GB

        Now, dividing by the target tablet size of 1 GB:
        Max Expected Tablets=5,236,230,000 / 1,073,741,824 ≈ 4.88 tablets
        Thus, the system should expect 4 or 8 tablets based on this setup.

        """
        # Extract relevant parameters from stress_cmd
        partition_count = int(re.search(r"-partition-count=(\d+)", stress_cmd).group(1))
        clustering_row_count = int(re.search(r"-clustering-row-count=(\d+)", stress_cmd).group(1))
        clustering_row_size = int(re.search(r"-clustering-row-size=(\d+)", stress_cmd).group(1))

        # Calculate total data size (partition_count * clustering_row_count * clustering_row_size)
        total_data_size = partition_count * clustering_row_count * clustering_row_size

        # Calculate the maximum expected tablets by dividing total size by target tablet size
        max_expected_tablets = total_data_size // target_tablet_size_in_bytes
        self.log.debug(f"Calculated dataset size: {total_data_size / (1024 ** 3)} GB")
        self.log.debug(f"Maximum expected tablets number: {max_expected_tablets}")
        return max_expected_tablets

    def _update_target_tablet_size(self, target_tablet_size_in_bytes: int):
        for node in self.db_cluster.nodes:
            self.log.info(f"Updating {node} with new target_tablet_size_in_bytes = {target_tablet_size_in_bytes}")
            append_scylla_yaml = {"target_tablet_size_in_bytes": target_tablet_size_in_bytes}
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.update(append_scylla_yaml)
            self.log.debug(f"Restarting node {node} to apply new target_tablet_size_in_bytes")
            node.restart_scylla_server()

    def _pre_create_large_partitions_schema(self):
        self.run_pre_create_keyspace()
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(create_scylla_bench_table_query())

    def test_tablets_split_merge(self):  # pylint: disable=too-many-locals  # noqa: PLR0914
        """
        (1) writing initial schema and get initial number of tablets.
        (2) start write and read stress simultaneously.
        (3) wait for tablets split.
        (4) wait for stress completion.
        (5) run deletions, nodetool flush and major compaction.
        (6) wait for tablets merge, following a shrunk dataset size.
        (7) redo more such cycles.
        """

        # (1) initiate schema.
        InfoEvent(message=f"Create c-s keyspace with tablets initial value").publish()
        self._pre_create_large_partitions_schema()
        stress_cmd = self.params.get('stress_cmd')
        stress_read_cmd = self.params.get('stress_read_cmd')
        append_scylla_yaml = self.params.get('append_scylla_yaml')
        target_tablet_size_in_bytes = append_scylla_yaml['target_tablet_size_in_bytes']
        max_expected_tablets = self._get_max_expected_tablets(stress_cmd, target_tablet_size_in_bytes)
        tablets_number_split_threshold = max_expected_tablets // 2
        deletion_percentage = 70  # How many of dataset partitions should be deleted.
        non_deleted_percentage = 100 - deletion_percentage
        # In case, for example, 70% of data is deleted, then 30% left.
        # Accordingly, it is expected to have about 30% of the tablets + some small extra (10%)
        # example scenario:
        # max_expected_tablets = 6
        # deletion_percentage = 70, non_deleted_percentage = 30
        # tablets_number_merge_threshold = math.ceil(6 * ((30+10) / 100) ) = 3
        # Meaning there were 6 tablets, then after 70% of data is deleted, we expect 3 or fewer tablets.
        tablets_number_merge_threshold = max_expected_tablets * math.ceil((non_deleted_percentage + 10) / 100)

        # Run prepare stress
        self.run_prepare_write_cmd()

        # Run Read background stress
        stress_read_queue = []
        read_params = {'keyspace_num': 1, 'stress_cmd': stress_read_cmd}
        self._run_all_stress_cmds(stress_read_queue, read_params)
        self.node1 = self.db_cluster.nodes[0]

        # Run cycles of writes and deletions
        cycles_num = 3
        for cycle in range(cycles_num):
            tablets_num = self._get_tablets_number()
            InfoEvent(message=f"Initial tablets number before stress is: {tablets_num}").publish()
            InfoEvent(message=f"Starting write load: {stress_cmd}").publish()
            stress_queue = []
            self.assemble_and_run_all_stress_cmd(stress_queue=stress_queue, stress_cmd=stress_cmd, keyspace_num=1)

            if self._is_stress_finished(stress_queue=stress_read_queue):
                InfoEvent(message="The Read Stress is finished. Rerunning it.").publish()
                self._run_all_stress_cmds(stress_read_queue, read_params)

            InfoEvent(message="Wait for write stress to finish (if running)").publish()
            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

            InfoEvent(message="Wait for tablets split following writes.").publish()
            self._wait_for_more_tablet_than(tablets_number_split_threshold)

            tablets_num = self._get_tablets_number()
            InfoEvent(message=f"Start deletions to trigger a tablets merge following {tablets_num} tablets.").publish()
            self.delete_partitions(deletion_percentage)
            self.node1.run_nodetool(f"flush -- {KEYSPACE_NAME}")
            self.node1.run_nodetool("compact", args=f"{KEYSPACE_NAME} {TABLE_NAME}")

            InfoEvent(message="Wait for tablets merge after deletion is done.").publish()
            self._wait_for_less_or_equal_tablet_than(tablets_number_merge_threshold)

        InfoEvent(message="Wait for read stress to finish.").publish()

        for stress in stress_read_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

    def _is_stress_finished(self, stress_queue) -> bool:
        for index, stress in enumerate(stress_queue):
            self.log.debug(f"Checking stress task {index + 1}/{len(stress_queue)}: {stress}")
            if not all(future.done() for future in stress.results_futures):
                self.log.debug(f"Stress task {index + 1} is still running.")
                return False
            self.log.debug(f"Stress task {index + 1} has completed.")
        self.log.debug("All stress tasks have finished.")
        return True

    def _wait_for_less_or_equal_tablet_than(self, tablets_num: int, timeout_min: int = 30):  # TODO: 30
        text = f"Waiting for a tablets number smaller than {tablets_num}"
        res = wait.wait_for(func=lambda: self._get_tablets_number() <= tablets_num, step=60,
                            text=text, timeout=60 * timeout_min, throw_exc=False)
        if not res:
            InfoEvent(message=f"{text} FAILED.", severity=Severity.ERROR).publish()

    def _wait_for_more_tablet_than(self, tablets_num: int, timeout_min: int = 30):  # TODO: 30
        text = f"Waiting for a tablets number bigger than {tablets_num}"
        res = wait.wait_for(func=lambda: self._get_tablets_number() > tablets_num, step=60,
                            text=text, timeout=60 * timeout_min, throw_exc=False)
        if not res:
            InfoEvent(message=f"{text} FAILED.", severity=Severity.ERROR).publish()

    def delete_partitions(self, deletion_percentage: int):
        """
        Deletes a percentage of table's partitions (from the end of it).
        Example:
            partition_end_range = 10
            max_partitions_to_delete = 100
            deletion_percentage = 70
            ==> 70 partitions from 11 up to 81 will be deleted.
        """
        start_partition = self.partitions_attrs.partition_end_range + 1
        max_partitions_to_delete = self.partitions_attrs.max_partitions_in_test_table - self.partitions_attrs.partition_end_range
        num_of_partitions_to_delete = int(max_partitions_to_delete * deletion_percentage / 100)
        end_partition = start_partition + num_of_partitions_to_delete

        self.log.debug(
            f"Preparing to delete {num_of_partitions_to_delete} partitions from {start_partition} to {end_partition}.")
        for pkey in range(start_partition, end_partition):
            delete_query = f"delete from {KS_CF} where pk = {pkey}"
            self.log.debug(f'delete query: {delete_query}')
            try:
                with self.db_cluster.cql_connection_patient(self.node1, connect_timeout=300) as session:
                    session.execute(SimpleStatement(delete_query, consistency_level=ConsistencyLevel.QUORUM),
                                    timeout=3600)
            except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
                message = f"Failed to execute delete: {e}"
                InfoEvent(message=message, severity=Severity.ERROR).publish()
