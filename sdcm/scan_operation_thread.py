import json
import logging
import random
import tempfile
import threading
import time
from abc import abstractmethod
from typing import Optional

from cassandra import ConsistencyLevel
from cassandra.cluster import ResponseFuture  # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from sdcm import wait
from sdcm.cluster import BaseScyllaCluster, BaseCluster
from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import get_partition_keys, get_table_clustering_order
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent, FullPartitionScanReversedOrderEvent, FullPartitionScanEvent

ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout")
LOCAL_CMD_RUNNER = LocalCmdRunner()


# pylint: disable=too-many-instance-attributes
class ScanOperationThread:
    bypass_cache = ' bypass cache'
    basic_query = 'select * from {}'

    # pylint: disable=too-many-arguments,unused-argument
    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], duration: int, interval: int,
                 termination_event: threading.Event, ks_cf: str,
                 scan_event: [FullScanEvent, FullPartitionScanReversedOrderEvent], page_size: int = 10000, **kwargs):
        self.ks_cf = ks_cf
        self.db_cluster = db_cluster
        self.page_size = page_size
        self.duration = duration
        self.interval = interval
        self.number_of_rows_read = 0
        self.db_node = None
        self.read_pages = 0
        self.scans_counter = 0
        self.time_elapsed = 0
        self.total_scan_time = 0
        self.scan_event = scan_event
        self.termination_event = termination_event
        self.log = logging.getLogger(self.__class__.__name__)
        self.user = kwargs.get("user", None)
        self.password = kwargs.get("password", None)
        self._thread = threading.Thread(daemon=True, name=self.__class__.__name__, target=self.run)

    def wait_until_user_table_exists(self, db_node, table_name: str = 'random', timeout_min: int = 20):
        text = f'Waiting until {table_name} user table exists'
        if table_name.lower() == 'random':
            wait.wait_for(func=lambda: len(self.db_cluster.get_non_system_ks_cf_list(db_node)) > 0, step=60,
                          text=text, timeout=60 * timeout_min, throw_exc=True)
        else:
            wait.wait_for(func=lambda: table_name in (self.db_cluster.get_non_system_ks_cf_list(db_node)), step=60,
                          text=text, timeout=60 * timeout_min, throw_exc=True)

    def randomly_bypass_cache(self, cmd: str) -> str:
        if random.choice([True] + [False]):
            cmd += self.bypass_cache
        return cmd

    @staticmethod
    def randomly_add_timeout(cmd) -> str:
        if random.choice([True] * 2 + [False]):
            cql_timeout_seconds = str(random.choice([2, 4, 8, 30, 120, 300]))
            cql_timeout_param = f" USING TIMEOUT {cql_timeout_seconds}s"
            cmd += cql_timeout_param
        return cmd

    @abstractmethod
    def randomly_form_cql_statement(self) -> Optional[str]:
        ...

    def execute_query(self, session, cmd: str):
        self.log.info('Will run command "%s"', cmd)
        return session.execute(SimpleStatement(
            cmd,
            fetch_size=self.page_size,
            consistency_level=ConsistencyLevel.ONE))

    def fetch_result_pages(self, result, read_pages):
        self.log.debug('Will fetch up to %s result pages.."', read_pages)
        pages = 0
        while result.has_more_pages and pages <= read_pages:
            result.fetch_next_page()
            if read_pages > 0:
                pages += 1

    def update_stats(self):
        self.scans_counter += 1
        self.total_scan_time += self.time_elapsed
        self.log.info('Average scan duration of %s scans is: %s', self.scans_counter,
                      (self.total_scan_time / self.scans_counter))

    def run_scan_operation(self, cmd: str = None, update_stats: bool = True):  # pylint: disable=too-many-locals
        db_node = self.db_node
        self.wait_until_user_table_exists(db_node=db_node, table_name=self.ks_cf)
        if self.ks_cf.lower() == 'random':
            self.ks_cf = random.choice(self.db_cluster.get_non_system_ks_cf_list(db_node))
        with self.scan_event(node=db_node.name, ks_cf=self.ks_cf, message="") as operation_event:
            cmd = cmd or self.randomly_form_cql_statement()
            if not cmd:
                return
            with self.db_cluster.cql_connection_patient(node=db_node, connect_timeout=300,
                                                        user=self.user, password=self.password) as session:

                if self.termination_event.is_set():
                    return

                try:
                    start_time = time.time()
                    result = self.execute_query(session=session, cmd=cmd)
                    self.fetch_result_pages(result=result, read_pages=self.read_pages)
                    self.time_elapsed = time.time() - start_time
                    self.update_stats()
                    self.log.debug('[%s] last scan duration of %s rows is: %s', {type(self).__name__},
                                   self.number_of_rows_read, self.time_elapsed)
                    operation_event.message = f"{type(self).__name__} operation ended successfully"
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    msg = f"{msg} while running Nemesis: {db_node.running_nemesis}" if db_node.running_nemesis else msg
                    operation_event.message = msg

                    if db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        operation_event.severity = Severity.WARNING
                    else:
                        operation_event.severity = Severity.ERROR

    def run(self):
        end_time = time.time() + self.duration
        while time.time() < end_time and not self.termination_event.is_set():
            self.db_node = random.choice(self.db_cluster.nodes)
            self.read_pages = random.choice([100, 1000, 0])
            self.run_scan_operation()
            self.log.debug('Executed %s number: %s', self.scan_event.__name__, self.scans_counter)
            time.sleep(self.interval)

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)


class FullScanThread(ScanOperationThread):

    def __init__(self, **kwargs):
        super().__init__(scan_event=FullScanEvent, **kwargs)

    def randomly_form_cql_statement(self) -> Optional[str]:
        cmd = self.randomly_bypass_cache(cmd=self.basic_query).format(self.ks_cf)
        return self.randomly_add_timeout(cmd)


class FullPartitionScanThread(ScanOperationThread):
    """
    Run a full scan of a partition, assuming it has a clustering key and multiple rows.
    It runs a reversed query of a partition, then optionally runs a normal partition scan in order
    to validate the reversed-query output data.

    Should support the following query options:
    1) ck < ?
    2) ck > ?
    3) ck > ? and ck < ?
    4) order by ck desc
    5) limit <int>
    6) paging
    """
    reversed_query_filter_ck_by = {'lt': ' and {} < {}', 'gt': ' and {} > {}', 'lt_and_gt': ' and {} < {} and {} > {}',
                                   'no_filter': ''}

    def __init__(self, **kwargs):
        super().__init__(scan_event=FullPartitionScanReversedOrderEvent, **kwargs)
        self.full_partition_scan_params = kwargs
        self.full_partition_scan_params['validate_data'] = json.loads(
            self.full_partition_scan_params.get('validate_data', 'false'))
        self.pk_name = self.full_partition_scan_params.get('pk_name', 'pk')
        self.ck_name = self.full_partition_scan_params.get('ck_name', 'ck')
        self.data_column_name = self.full_partition_scan_params.get('data_column_name', 'v')
        self.rows_count = self.full_partition_scan_params.get('rows_count', 5000)
        self.table_clustering_order = self.get_table_clustering_order()
        self.reversed_order = 'desc' if self.table_clustering_order.lower() == 'asc' else 'asc'
        self.reversed_query_filter_ck_stats = {'lt': {'count': 0, 'total_scan_duration': 0},
                                               'gt': {'count': 0, 'total_scan_duration': 0},
                                               'lt_and_gt': {'count': 0, 'total_scan_duration': 0},
                                               'no_filter': {'count': 0, 'total_scan_duration': 0}}
        self.ck_filter = ''
        self.limit = ''
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                               encoding='utf-8')

    def get_table_clustering_order(self) -> str:
        for node in self.db_cluster.nodes:
            try:
                with self.db_cluster.cql_connection_patient(node=node, connect_timeout=300) as session:
                    # Using CL ONE. No need for a quorum since querying a constant fixed attribute of a table.
                    session.default_consistency_level = ConsistencyLevel.ONE
                    return get_table_clustering_order(ks_cf=self.ks_cf, ck_name=self.ck_name, session=session)
            except Exception as error:  # pylint: disable=broad-except
                self.log.error('Failed getting table %s clustering order through node %s : %s', self.ks_cf, node.name,
                               error)
        raise Exception('Failed getting table clustering order from all db nodes')

    def randomly_form_cql_statement(self) -> Optional[tuple[str, str]]:  # pylint: disable=too-many-branches
        """
        The purpose of this method is to form a random reversed-query out of all options, like:
            select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc

        This consists of:
        1) Select a random partition found in requested table.
        2) randomly add a CQL LIMIT and 'bypass cache' to query.
        3) Add a random CK filter with random row values.
        :return: a CQL reversed-query
        """
        with self.db_cluster.cql_connection_patient(node=self.db_node, connect_timeout=300) as session:
            ck_name = self.ck_name
            rows_count = self.rows_count
            ck_random_min_value = random.randint(a=1, b=rows_count)
            ck_random_max_value = random.randint(a=ck_random_min_value, b=rows_count)
            self.ck_filter = ck_filter = random.choice(list(self.reversed_query_filter_ck_by.keys()))
            pk_name = self.pk_name
            if pks := get_partition_keys(ks_cf=self.ks_cf, session=session, pk_name=pk_name):
                partition_key = random.choice(pks)
                # Form a random query out of all options, like:
                # select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc
                # limit 3467 bypass cache
                selected_columns = [pk_name, ck_name]
                if self.full_partition_scan_params.get('include_data_column'):
                    selected_columns.append(self.data_column_name)
                reversed_query = f'select {",".join(selected_columns)} from {self.ks_cf}' + \
                    f' where {pk_name} = {partition_key}'
                query_suffix = self.limit = ''

                # Randomly add CK filtering ( less-than / greater-than / both / non-filter )

                # example: rows-count = 20, ck > 10, ck < 15, limit = 3 ==> ck_range = [11..14] = 4
                # ==> limit < ck_range
                # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                # order by ck desc limit 5
                # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                match ck_filter:
                    case 'lt_and_gt':
                        # Example: select * from scylla_bench.test where pk = 1 and ck > 10 and ck < 15 order by ck desc
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                             ck_random_max_value,
                                                                                             ck_name,
                                                                                             ck_random_min_value)

                    case 'gt':
                        # example: rows-count = 20, ck > 10, limit = 5 ==> ck_range = 20 - 10 = 10 ==> limit < ck_range
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                             ck_random_min_value)

                    case 'lt':
                        # example: rows-count = 20, ck < 10, limit = 5 ==> limit < ck_random_min_value (ck_range)
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck < 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck >= 5 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(ck_name,
                                                                                             ck_random_min_value)
                query_suffix = self.randomly_bypass_cache(cmd=query_suffix)
                normal_query = reversed_query + query_suffix
                if random.choice([False] + [True]):  # Randomly add a LIMIT
                    self.limit = random.randint(a=1, b=rows_count)
                    query_suffix = f' limit {self.limit}' + query_suffix
                reversed_query += f' order by {ck_name} {self.reversed_order}' + query_suffix
                self.log.debug('Randomly formed normal query is: %s', normal_query)
                self.log.debug('[scan: %s, type: %s] Randomly formed reversed query is: %s', self.scans_counter,
                               ck_filter, reversed_query)
            else:
                self.log.debug('No partition keys found for table: %s! A reversed query cannot be executed!', self.ks_cf)
                return None
        return normal_query, reversed_query

    def fetch_result_pages(self, result: ResponseFuture, read_pages):
        self.log.debug('Will fetch up to %s result pages.."', read_pages)
        self.number_of_rows_read = 0
        handler = PagedResultHandler(future=result, scan_operation_thread=self)
        handler.finished_event.wait()
        if handler.error:
            self.log.warning("Got a Page Handler error: %s", handler.error)
            raise handler.error
        self.log.debug('Fetched a total of %s pages', handler.current_read_pages)

    def execute_query(self, session, cmd: str):
        self.log.debug('Will run command "%s"', cmd)
        session.default_fetch_size = self.page_size
        session.default_consistency_level = ConsistencyLevel.ONE
        return session.execute_async(cmd)

    def reset_output_files(self):
        self.normal_query_output.close()
        self.reversed_query_output.close()
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,  # pylint: disable=consider-using-with
                                                               encoding='utf-8')

    def _compare_output_files(self):
        self.normal_query_output.flush()
        self.reversed_query_output.flush()

        with tempfile.NamedTemporaryFile(mode='w+', delete=True, encoding='utf-8') as reorder_normal_query_output:
            cmd = f'tac {self.normal_query_output.name}'
            if self.limit:
                cmd += f' | head -n {self.limit}'
            cmd += f' > {reorder_normal_query_output.name}'
            LOCAL_CMD_RUNNER.run(cmd=cmd, ignore_status=True)
            reorder_normal_query_output.flush()
            LOCAL_CMD_RUNNER.run(cmd=f'cp {reorder_normal_query_output.name} {self.normal_query_output.name}',
                                 ignore_status=True)
            self.normal_query_output.flush()

        file_names = f' {self.normal_query_output.name} {self.reversed_query_output.name}'
        diff_cmd = f"diff -y --suppress-common-lines {file_names}"
        self.log.info("Comparing scan queries output files by: %s", diff_cmd)
        result = LOCAL_CMD_RUNNER.run(cmd=diff_cmd, ignore_status=True)
        if not result.stderr:
            if not result.stdout:
                self.log.info("Compared output of normal and reversed queries is identical!")
            else:
                self.log.warning("Normal and reversed queries output differs: \n%s", result.stdout.strip())
                ls_cmd = f"ls -alh {file_names}"
                head_cmd = f"head {file_names}"
                for cmd in [ls_cmd, head_cmd]:
                    result = LOCAL_CMD_RUNNER.run(cmd=cmd, ignore_status=True)
                    if result.stdout:
                        stdout = result.stdout.strip()
                        self.log.info("%s command output is: \n%s", cmd, stdout)
        self.reset_output_files()

    def run_scan_operation(self, cmd: str = None, update_stats: bool = True):  # pylint: disable=too-many-locals
        queries = self.randomly_form_cql_statement()
        if not queries:
            return
        normal_query, reversed_query = queries
        self.scan_event = FullPartitionScanReversedOrderEvent
        super().run_scan_operation(cmd=reversed_query)
        self.reversed_query_filter_ck_stats[self.ck_filter]['count'] += 1
        self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] += self.time_elapsed
        count = self.reversed_query_filter_ck_stats[self.ck_filter]['count']
        average = self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] / count
        self.log.debug('Average %s scans duration of %s executions is: %s', self.ck_filter, count, average)
        if self.full_partition_scan_params.get('validate_data'):
            self.log.debug('Executing the normal query: %s', normal_query)
            self.scan_event = FullPartitionScanEvent
            super().run_scan_operation(cmd=normal_query)
            self._compare_output_files()

    def update_stats(self):
        if self.scan_event == FullPartitionScanReversedOrderEvent:
            super().update_stats()


class PagedResultHandler:

    def __init__(self, future: ResponseFuture, scan_operation_thread: FullPartitionScanThread):
        self.error = None
        self.finished_event = threading.Event()
        self.future = future
        self.max_read_pages = scan_operation_thread.read_pages
        self.current_read_pages = 0
        self.log = logging.getLogger(self.__class__.__name__)
        self.scan_operation_thread = scan_operation_thread
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def _row_to_string(self, row, include_data_column: bool = False) -> str:
        row_string = str(getattr(row, self.scan_operation_thread.pk_name))
        row_string += str(getattr(row, self.scan_operation_thread.ck_name))
        if include_data_column:
            row_string += str(getattr(row, self.scan_operation_thread.data_column_name))
        return f'{row_string}\n'

    def handle_page(self, rows):
        include_data_column = self.scan_operation_thread.full_partition_scan_params.get('include_data_column')
        if self.scan_operation_thread.scan_event == FullPartitionScanEvent:
            for row in rows:
                self.scan_operation_thread.normal_query_output.write(
                    self._row_to_string(row=row, include_data_column=include_data_column))
        elif self.scan_operation_thread.scan_event == FullPartitionScanReversedOrderEvent:
            self.scan_operation_thread.number_of_rows_read += len(rows)
            if self.scan_operation_thread.full_partition_scan_params['validate_data']:
                for row in rows:
                    self.scan_operation_thread.reversed_query_output.write(
                        self._row_to_string(row=row, include_data_column=include_data_column))

        if self.future.has_more_pages and self.current_read_pages <= self.max_read_pages:
            self.log.debug('Will fetch the next page: %s', self.current_read_pages)
            self.future.start_fetching_next_page()
            if self.max_read_pages > 0:
                self.current_read_pages += 1
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.finished_event.set()
