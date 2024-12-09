import os
import tempfile
import threading
import time
from functools import partial

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from longevity_test import LongevityTest
from sdcm import wait
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.common import ParallelObject
from sdcm.utils.tablets.common import get_tablets_count
from test_lib.scylla_bench_tools import create_scylla_bench_table_query

KEYSPACE_NAME = 'scylla_bench'
TABLE_NAME = 'test'
KS_CF = f"{KEYSPACE_NAME}.{TABLE_NAME}"
GB_IN_BYTES = 1_073_741_824


class TabletsSplitMergeTest(LongevityTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_tablets_num = 0
        self.query_tablets_num_stop_event = threading.Event()
        self.tablets_num_lock = threading.Lock()  # Lock to protect shared variables

    def _background_query_tablets_num(self):
        """Background thread to query the tablets number."""
        while not self.query_tablets_num_stop_event.is_set():
            try:
                tablets_num = self._get_tablets_number()
                with self.tablets_num_lock:
                    self.max_tablets_num = max(self.max_tablets_num, tablets_num)
                self.log.debug(f"Updated max tablets number: {self.max_tablets_num}")
            except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
                self.log.error(f"Error in background tablet query: {e}")
            time.sleep(15)  # Wait 15 seconds before next query

    def _get_tablets_number(self) -> int:
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            tablets_number = get_tablets_count(keyspace=KEYSPACE_NAME, session=session)
        self.log.debug(f"Tablets number for {KEYSPACE_NAME} is: {tablets_number}")
        return tablets_number

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

    def _start_stress_if_not_running(self, stress_queue, stress_params):
        if self._is_stress_finished(stress_queue=stress_queue):
            InfoEvent(message="The Stress is not running. Rerunning it.").publish()
            self._run_all_stress_cmds(stress_queue, stress_params)

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
        InfoEvent(message=f"Create a keyspace with tablets initial value").publish()
        self._pre_create_large_partitions_schema()
        stress_cmd = self.params.get('stress_cmd')
        stress_read_cmd = self.params.get('stress_read_cmd')
        deletion_percentage = 96  # How many of dataset partitions should be deleted.

        # Run prepare stress
        self.run_prepare_write_cmd()

        # Run Read background stress
        stress_read_queue = []
        read_params = {'keyspace_num': 1, 'stress_cmd': stress_read_cmd}
        self._run_all_stress_cmds(stress_read_queue, read_params)
        self.node1 = self.db_cluster.data_nodes[0]
        list_file_size_script_name = "list_files_and_sizes_to_file.py"
        list_file_size_output_file = "/tmp/files_and_sizes.txt"
        table_dst_path = os.path.join(tempfile.mkdtemp(prefix='tablets-split-merge'), 'files_and_sizes.txt')

        for node in self.db_cluster.data_nodes:  # copy python script of calculating cf dir files size to db nodes.
            node.remoter.send_files(f'./data_dir/{list_file_size_script_name}', '/tmp/')

        def _check_table_file_sizes():
            for node in self.db_cluster.data_nodes:
                InfoEvent(
                    message=f"Checking file sizes of {KS_CF} directory on node {node.name}").publish()
                node.remoter.run(f'python3 /tmp/{list_file_size_script_name}', verbose=True)
                node.remoter.receive_files(src=list_file_size_output_file,
                                           dst=table_dst_path)
                try:
                    with open(table_dst_path, 'r', encoding='utf-8') as file:
                        self.log.debug(f"--- File sizes on node {node.name} ---")
                        for line in file:
                            self.log.debug(line.strip())
                        self.log.debug(f"--- End of file sizes from node {node.name} ---")
                except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
                    self.log.error(
                        f"Failed to read the output file {table_dst_path} from node {node.name}. Error: {e}"
                    )

        InfoEvent(message=f"Starting background thread to query and set max tablets number.").publish()
        background_thread = threading.Thread(target=self._background_query_tablets_num, daemon=True)
        background_thread.start()
        # Run cycles of writes and deletions
        cycles_num = 7
        for cycle in range(cycles_num):
            with self.tablets_num_lock:
                initial_tablets_num = self.max_tablets_num = self._get_tablets_number()
            InfoEvent(
                message=f"Cycle {cycle }, Initial tablets number before stress is: {initial_tablets_num}").publish()
            InfoEvent(message=f"Starting write load: {stress_cmd}").publish()
            stress_queue = []
            self.assemble_and_run_all_stress_cmd(stress_queue=stress_queue, stress_cmd=stress_cmd, keyspace_num=1)

            self._start_stress_if_not_running(stress_read_queue, read_params)

            InfoEvent(message="Wait for write stress to finish (if running)").publish()
            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

            self._start_stress_if_not_running(stress_read_queue, read_params)
            InfoEvent(message=f"Start deletions to trigger a tablets merge.").publish()
            self.delete_partitions_in_batch(deletion_percentage)

            InfoEvent(message=f"Run a flush for {KEYSPACE_NAME} on nodes").publish()
            triggers = [partial(node.run_nodetool, sub_cmd=f"flush -- {KEYSPACE_NAME}", )
                        for node in self.db_cluster.data_nodes]
            ParallelObject(objects=triggers, timeout=2400).call_objects()

            InfoEvent(
                message=f"Checking file sizes of {KS_CF} directory on nodes before major compaction").publish()
            _check_table_file_sizes()

            InfoEvent(message=f"Run a major compaction for {KEYSPACE_NAME} on nodes").publish()
            triggers = [partial(node.run_nodetool, sub_cmd="compact", args=f"{KEYSPACE_NAME} {TABLE_NAME}", ) for
                        node in self.db_cluster.data_nodes]
            ParallelObject(objects=triggers, timeout=2400).call_objects()

            InfoEvent(
                message=f"Checking file sizes of {KS_CF} directory on nodes after major compaction").publish()
            _check_table_file_sizes()

            self._wait_for_tablet_split(tablets_num=initial_tablets_num)
            self.log.debug(f"Final max tablets number: {self.max_tablets_num}")

            if self.max_tablets_num <= initial_tablets_num:
                InfoEvent(
                    message=f"The maximum number of tablets [{self.max_tablets_num}] is not bigger than the initial number [{initial_tablets_num}].", severity=Severity.ERROR).publish()
            InfoEvent(message="Wait for tablets merge after deletion is done.").publish()
            self._wait_for_tablets_merge()

        self.log.debug("Stopping background thread.")
        self.query_tablets_num_stop_event.set()
        background_thread.join()

        InfoEvent(message="Wait for read stress to finish.").publish()

        for stress in stress_read_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

    def _is_stress_finished(self, stress_queue) -> bool:
        for index, stress in enumerate(stress_queue):
            self.log.debug(f"Checking stress task {index + 1}/{len(stress_queue)}: {stress.stress_cmd}")
            if not all(future.done() for future in stress.results_futures):
                self.log.debug(f"Stress task {index + 1} is still running.")
                return False
            self.log.debug(f"Stress task {index + 1} has completed.")
        self.log.debug("All stress tasks have finished.")
        return True

    def _wait_for_tablets_merge(self, timeout_min: int = 15):
        """
        Waits for a tablets number smaller than tablets_num
        """
        text = f"Waiting for a smaller tablets number"
        res = wait.wait_for(func=lambda: self._get_tablets_number() < self.max_tablets_num, step=60,
                            text=text, timeout=60 * timeout_min, throw_exc=False)
        if not res:
            InfoEvent(message=f"{text} FAILED.", severity=Severity.ERROR).publish()

    def _wait_for_tablet_split(self, tablets_num: int, timeout_min: int = 15):
        """
        Waits for tablets number bigger than tablets_num
        """
        text = f"Waiting for a tablets number bigger than {tablets_num}"
        res = wait.wait_for(func=lambda: self.max_tablets_num > tablets_num, step=60,
                            text=text, timeout=60 * timeout_min, throw_exc=False)
        if not res:
            InfoEvent(message=f"{text} FAILED.", severity=Severity.ERROR).publish()

    def delete_partitions(self, deletion_percentage: int):
        """
        Deletes a percentage of table's partitions.
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

    def delete_partitions_in_batch(self, deletion_percentage: int):
        """
        Deletes a percentage of table's partitions using batched queries.
        """
        start_partition = self.partitions_attrs.partition_end_range + 1
        max_partitions_to_delete = self.partitions_attrs.max_partitions_in_test_table - self.partitions_attrs.partition_end_range
        num_of_partitions_to_delete = int(max_partitions_to_delete * deletion_percentage / 100)
        end_partition = start_partition + num_of_partitions_to_delete
        batch_size: int = 1000
        self.log.debug(
            f"Preparing to delete {num_of_partitions_to_delete} partitions from {start_partition} to {end_partition} in batches of {batch_size}.")
        batch = []
        try:
            with self.db_cluster.cql_connection_patient(self.node1, connect_timeout=300) as session:
                for pkey in range(start_partition, end_partition):
                    delete_query = f"DELETE FROM {KS_CF} WHERE pk = {pkey}"
                    batch.append(delete_query)

                    # Execute batch when it reaches the batch size
                    if len(batch) >= batch_size:
                        batch_query = "BEGIN BATCH\n" + "\n".join(batch) + "\nAPPLY BATCH;"
                        session.execute(SimpleStatement(
                            batch_query, consistency_level=ConsistencyLevel.QUORUM), timeout=3600)
                        batch.clear()  # Clear batch after execution

                # Execute any remaining queries in the batch
                if batch:
                    batch_query = "BEGIN BATCH\n" + "\n".join(batch) + "\nAPPLY BATCH;"
                    session.execute(SimpleStatement(
                        batch_query, consistency_level=ConsistencyLevel.QUORUM), timeout=3600)
        except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
            message = f"Failed to execute batch delete: {e}"
            InfoEvent(message=message, severity=Severity.ERROR).publish()
