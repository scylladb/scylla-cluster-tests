from __future__ import annotations

import datetime
import logging
import random
import tempfile
import threading
import time
import traceback
from pathlib import Path
from abc import abstractmethod
from string import Template
from typing import Optional, Type, NamedTuple, TYPE_CHECKING
from contextlib import contextmanager

from pytz import utc
from cassandra import ConsistencyLevel
from cassandra.cluster import ResponseFuture, ResultSet
from cassandra.query import SimpleStatement
from cassandra.policies import ExponentialBackoffRetryPolicy

from sdcm.remote import LocalCmdRunner
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent, FullPartitionScanReversedOrderEvent, FullPartitionScanEvent, \
    FullScanAggregateEvent
from sdcm.utils.database_query_utils import get_table_clustering_order, get_partition_keys
from sdcm.utils.operations_thread import OperationThreadStats, OneOperationStat, OperationThread, ThreadParams
from sdcm.db_stats import PrometheusDBStats
from sdcm.test_config import TestConfig
from sdcm.utils.decorators import retrying, Retry
from sdcm.utils.issues import SkipPerIssues

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout", 'host has been marked down or removed')
BYPASS_CACHE_VALUES = [" BYPASS CACHE", ""]
LOCAL_CMD_RUNNER = LocalCmdRunner()


class FullScanCommand(NamedTuple):
    name: str
    base_query: Template


class FullScanAggregateCommands(NamedTuple):
    SELECT_ALL = FullScanCommand("SELECT_ALL", Template("SELECT * from $ks_cf$bypass_cache$timeout"))
    AGG_COUNT_ALL = FullScanCommand("AGG_COUNT_ALL", Template("SELECT count(*) FROM $ks_cf$bypass_cache$timeout"))


class FullscanException(Exception):
    """ Exception during running a fullscan"""


class ScanOperationThread(OperationThread):
    """
    Runs fullscan operations according to the parameters specified in the test
    config yaml files. Has 4 main modes:
    - random: uses a seeded random generator to generate a queue of fullscan
    operations using all the available operation types
    - table: uses only FullScanOperation
    - partition: uses only FullPartitionScanOperation
    - aggregate: uses only FullScanAggregatesOperation

    Check ThreadParams class for parameters to tweak.

    Note: the seeded random generator is shared between the thread class and
    the operations instances. So if the operations queue is  Op1, Op2, Op3],
    all of them will be initialized with the same random generator instance
    as this thread. This way the CQL commands generated by the instances are
    also replicable.
    """

    def __init__(self, thread_params: ThreadParams, thread_name: str = ""):
        super().__init__(thread_params, thread_name)
        full_scan_operation = FullScanOperation(**self.operation_params)
        full_partition_scan_operation = FullPartitionScanOperation(**self.operation_params)
        full_scan_aggregates_operation = FullScanAggregatesOperation(**self.operation_params)

        # create mapping for different scan operations objects,
        # please see usage in get_next_scan_operation()
        self.operation_instance_map = {
            "table_and_aggregate": lambda: self.generator.choice(
                [full_scan_operation, full_scan_aggregates_operation]),
            "random": lambda: self.generator.choice(
                [full_scan_operation, full_scan_aggregates_operation, full_partition_scan_operation]),
            "table": lambda: full_scan_operation,
            "partition": lambda: full_partition_scan_operation,
            "aggregate": lambda: full_scan_aggregates_operation
        }


class FullscanOperationBase:
    def __init__(self, generator: random.Random, thread_params: ThreadParams, thread_stats: OperationThreadStats,
                 scan_event: Type[FullScanEvent] | Type[FullPartitionScanEvent]
                 | Type[FullPartitionScanReversedOrderEvent] | Type[FullScanAggregateEvent]):
        """
        Base class for performing fullscan operations.
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info("FullscanOperationBase starts")
        self.fullscan_params = thread_params
        self.fullscan_stats = thread_stats
        self.scan_event = scan_event
        self.log.info("FullscanOperationBase scan_event: %s", self.scan_event)
        self.termination_event = self.fullscan_params.termination_event
        self.generator = generator
        self.db_node = self._get_random_node()
        self.current_operation_stat = None
        self.log.info("FullscanOperationBase init finished")
        self._exp_backoff_retry_policy_params = {
            "max_num_retries": 15.0, "min_interval": 1.0, "max_interval": 1800.0
        }
        self._request_default_timeout = 1800

    def _get_random_node(self) -> BaseNode:
        return self.generator.choice(self.fullscan_params.db_cluster.data_nodes)

    @abstractmethod
    def randomly_form_cql_statement(self) -> str:
        ...

    def execute_query(
            self, session, cmd: str,
            event: Type[FullScanEvent | FullPartitionScanEvent
                        | FullPartitionScanReversedOrderEvent]) -> ResultSet:
        self.log.debug('Will run command %s', cmd)
        return session.execute(SimpleStatement(
            cmd,
            fetch_size=self.fullscan_params.page_size,
            consistency_level=ConsistencyLevel.ONE)
        )

    def run_scan_event(self, cmd: str,
                       scan_event: Type[FullScanEvent | FullPartitionScanEvent
                                        | FullPartitionScanReversedOrderEvent] = None) -> OneOperationStat:
        scan_event = scan_event or self.scan_event
        cmd = cmd or self.randomly_form_cql_statement()
        with scan_event(node=self.db_node.name, ks_cf=self.fullscan_params.ks_cf, message=f"Will run command {cmd}",
                        user=self.fullscan_params.user,
                        password=self.fullscan_params.user_password) as scan_op_event:

            self.current_operation_stat = OneOperationStat(
                op_type=scan_op_event.__class__.__name__,
                nemesis_at_start=self.db_node.running_nemesis,
                cmd=cmd
            )

            with self.cql_connection(connect_timeout=300) as session:
                try:
                    scan_op_event.message = ''
                    start_time = time.time()
                    result = self.execute_query(session=session, cmd=cmd, event=scan_op_event)
                    if result:
                        self.fetch_result_pages(result=result, read_pages=self.fullscan_stats.read_pages)
                    if not scan_op_event.message:
                        scan_op_event.message = f"{type(self).__name__} operation ended successfully"
                except Exception as exc:  # noqa: BLE001
                    self.log.error(traceback.format_exc())
                    msg = repr(exc)
                    self.current_operation_stat.exceptions.append(repr(exc))
                    msg = f"{msg} while running " \
                          f"Nemesis: {self.db_node.running_nemesis}" if self.db_node.running_nemesis else msg
                    scan_op_event.message = msg

                    if self.db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        scan_op_event.severity = Severity.WARNING
                    else:
                        scan_op_event.severity = Severity.ERROR
                finally:
                    duration = time.time() - start_time
                    self.fullscan_stats.time_elapsed += duration
                    self.fullscan_stats.scans_counter += 1
                    self.current_operation_stat.nemesis_at_end = self.db_node.running_nemesis
                    self.current_operation_stat.duration = duration
                    # success is True if there were no exceptions
                    self.current_operation_stat.success = not bool(self.current_operation_stat.exceptions)
                    self.update_stats(self.current_operation_stat)
                    return self.current_operation_stat

    def update_stats(self, new_stat):
        self.fullscan_stats.stats.append(new_stat)

    def run_scan_operation(self, cmd: str = None) -> OneOperationStat:
        return self.run_scan_event(cmd=cmd or self.randomly_form_cql_statement(), scan_event=self.scan_event)

    def fetch_result_pages(self, result, read_pages):
        self.log.debug('Will fetch up to %s result pages..', read_pages)
        pages = 0
        while result.has_more_pages and pages <= read_pages:
            result.fetch_next_page()
            if read_pages > 0:
                pages += 1

    @contextmanager
    def cql_connection(self, **kwargs):
        node = kwargs.pop("node", self.db_node)
        with self.fullscan_params.db_cluster.cql_connection_patient(
                node=node,
                user=self.fullscan_params.user,
                password=self.fullscan_params.user_password, **kwargs) as session:
            session.cluster.default_retry_policy = ExponentialBackoffRetryPolicy(
                **self._exp_backoff_retry_policy_params)
            session.default_timeout = self._request_default_timeout
            yield session


class FullScanOperation(FullscanOperationBase):
    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullScanEvent, **kwargs)

    def randomly_form_cql_statement(self) -> str:
        base_query = FullScanAggregateCommands.SELECT_ALL.base_query
        cmd = base_query.substitute(ks_cf=self.fullscan_params.ks_cf,
                                    timeout=f" USING TIMEOUT {self.fullscan_params.full_scan_operation_limit}s",
                                    bypass_cache=BYPASS_CACHE_VALUES[0])
        return cmd


class FullPartitionScanOperation(FullscanOperationBase):
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

    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullPartitionScanReversedOrderEvent, **kwargs)
        self.table_clustering_order = ""
        self.reversed_order = 'desc' if self.table_clustering_order.lower() == 'asc' else 'asc'
        self.reversed_query_filter_ck_stats = {'lt': {'count': 0, 'total_scan_duration': 0},
                                               'gt': {'count': 0, 'total_scan_duration': 0},
                                               'lt_and_gt': {'count': 0, 'total_scan_duration': 0},
                                               'no_filter': {'count': 0, 'total_scan_duration': 0}}
        self.ck_filter = ''
        self.limit = ''
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,
                                                               encoding='utf-8')

    def get_table_clustering_order(self) -> str:
        node = self._get_random_node()
        try:
            with self.cql_connection(node=node, connect_timeout=300) as session:
                # Using CL ONE. No need for a quorum since querying a constant fixed attribute of a table.
                session.default_consistency_level = ConsistencyLevel.ONE
                return get_table_clustering_order(ks_cf=self.fullscan_params.ks_cf,
                                                  ck_name=self.fullscan_params.ck_name, session=session)
        except Exception as error:  # noqa: BLE001
            self.log.error(traceback.format_exc())
            self.log.error('Failed getting table %s clustering order through node %s : %s',
                           self.fullscan_params.ks_cf, node.name,
                           error)
        raise Exception('Failed getting table clustering order from all db nodes')

    def randomly_form_cql_statement(self) -> Optional[tuple[str, str]]:
        """
        The purpose of this method is to form a random reversed-query out of all options, like:
            select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc

        This consists of:
        1) Select a random partition found in requested table.
        2) randomly add a CQL LIMIT and 'bypass cache' to query.
        3) Add a random CK filter with random row values.
        :return: a CQL reversed-query
        """
        db_node = self._get_random_node()

        with self.cql_connection(node=db_node, connect_timeout=300) as session:
            ck_random_min_value = self.generator.randint(a=1, b=self.fullscan_params.rows_count)
            ck_random_max_value = self.generator.randint(a=ck_random_min_value, b=self.fullscan_params.rows_count)
            self.ck_filter = ck_filter = self.generator.choice(list(self.reversed_query_filter_ck_by.keys()))

            if pks := get_partition_keys(ks_cf=self.fullscan_params.ks_cf, session=session, pk_name=self.fullscan_params.pk_name):
                partition_key = self.generator.choice(pks)
                # Form a random query out of all options, like:
                # select * from scylla_bench.test where pk = 1234 and ck < 4721 and ck > 2549 order by ck desc
                # limit 3467 bypass cache
                selected_columns = [self.fullscan_params.pk_name, self.fullscan_params.ck_name]
                if self.fullscan_params.include_data_column:
                    selected_columns.append(self.fullscan_params.data_column_name)
                reversed_query = f'select {",".join(selected_columns)} from {self.fullscan_params.ks_cf}' + \
                    f' where {self.fullscan_params.pk_name} = {partition_key}'
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
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_max_value,
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                    case 'gt':
                        # example: rows-count = 20, ck > 10, limit = 5 ==> ck_range = 20 - 10 = 10 ==> limit < ck_range
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck > 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck > 15 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                    case 'lt':
                        # example: rows-count = 20, ck < 10, limit = 5 ==> limit < ck_random_min_value (ck_range)
                        # reversed query is: select * from scylla_bench.test where pk = 1 and ck < 10
                        # order by ck desc limit 5
                        # normal query should be: select * from scylla_bench.test where pk = 1 and ck >= 5 limit 5
                        reversed_query += self.reversed_query_filter_ck_by[ck_filter].format(
                            self.fullscan_params.ck_name,
                            ck_random_min_value
                        )

                query_suffix = f"{query_suffix} {self.generator.choice(BYPASS_CACHE_VALUES)}"
                normal_query = reversed_query + query_suffix
                if random.choice([False] + [True]):  # Randomly add a LIMIT
                    self.limit = random.randint(a=1, b=self.fullscan_params.rows_count)
                    query_suffix = f' limit {self.limit}' + query_suffix
                reversed_query += f' order by {self.fullscan_params.ck_name} {self.reversed_order}' + query_suffix
                self.log.debug('Randomly formed normal query is: %s', normal_query)
                self.log.debug('[scan: %s, type: %s] Randomly formed reversed query is: %s', self.fullscan_stats.scans_counter,
                               ck_filter, reversed_query)
            else:
                self.log.debug('No partition keys found for table: %s! A reversed query cannot be executed!',
                               self.fullscan_params.ks_cf)
                return None
        return normal_query, reversed_query

    def fetch_result_pages(self, result: ResponseFuture, read_pages):
        self.log.debug('Will fetch up to %s result pages.."', read_pages)
        self.fullscan_stats.number_of_rows_read = 0
        handler = PagedResultHandler(future=result, scan_operation=self)
        handler.finished_event.wait()

        if handler.error:
            self.log.warning("Got a Page Handler error: %s", handler.error)
            raise handler.error
        self.log.debug('Fetched a total of %s pages', handler.current_read_pages)

    def execute_query(
            self, session, cmd: str,
            event: Type[FullScanEvent | FullPartitionScanEvent
                        | FullPartitionScanReversedOrderEvent]) -> ResponseFuture:
        self.log.debug('Will run command "%s"', cmd)
        session.default_fetch_size = self.fullscan_params.page_size
        session.default_consistency_level = ConsistencyLevel.ONE
        return session.execute_async(cmd)

    def reset_output_files(self):
        self.normal_query_output.close()
        self.reversed_query_output.close()
        self.reversed_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,
                                                                 encoding='utf-8')
        self.normal_query_output = tempfile.NamedTemporaryFile(mode='w+', delete=False,
                                                               encoding='utf-8')

    def _compare_output_files(self) -> bool:
        self.normal_query_output.flush()
        self.reversed_query_output.flush()
        result = False

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
            self.log.debug("Comparing scan queries output files by: %s", diff_cmd)
            log_file = Path(TestConfig().logdir()) / 'fullscans' / \
                f'partition_range_scan_diff_{datetime.datetime.now(tz=utc).strftime("%Y_%m_%d-%I_%M_%S")}.log'
            log_file.parent.mkdir(parents=True, exist_ok=True)
            result = LOCAL_CMD_RUNNER.run(cmd=diff_cmd, ignore_status=True, verbose=False, log_file=str(log_file))

        if not result.stderr:
            if not result.stdout:
                self.log.debug("Compared output of normal and reversed queries is identical!")
                result = True
            else:
                self.log.warning("Normal and reversed queries output differs: output results in %s", log_file)
                ls_cmd = f"ls -alh {file_names}"
                head_cmd = f"head {file_names}"
                for cmd in [ls_cmd, head_cmd]:
                    result = LOCAL_CMD_RUNNER.run(cmd=cmd, ignore_status=True)
                    if result.stdout:
                        stdout = result.stdout.strip()
                        self.log.debug("%s command output is: \n%s", cmd, stdout)
        else:
            self.log.warning("Comparison of output of normal and reversed queries failed with stderr:\n%s",
                             result.stderr)

        self.reset_output_files()
        return result

    def run_scan_operation(self, cmd: str = None):
        self.table_clustering_order = self.get_table_clustering_order()
        queries = self.randomly_form_cql_statement()

        if not queries:
            return

        normal_query, reversed_query = queries

        full_partition_op_stat = OneOperationStat(
            op_type=self.__class__.__name__,
            nemesis_at_start=self.db_node.running_nemesis,
            cmd=str(queries)
        )

        reversed_op_stat = self.run_scan_event(cmd=reversed_query, scan_event=FullPartitionScanReversedOrderEvent)
        self.reversed_query_filter_ck_stats[self.ck_filter]['count'] += 1
        self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] += self.fullscan_stats.time_elapsed
        count = self.reversed_query_filter_ck_stats[self.ck_filter]['count']
        average = self.reversed_query_filter_ck_stats[self.ck_filter]['total_scan_duration'] / count
        self.log.debug('Average %s scans duration of %s executions is: %s', self.ck_filter, count, average)

        if self.fullscan_params.validate_data:
            self.log.debug('Executing the normal query: %s', normal_query)
            self.scan_event = FullPartitionScanEvent
            regular_op_stat = self.run_scan_event(cmd=normal_query, scan_event=self.scan_event)
            comparison_result = self._compare_output_files()
            full_partition_op_stat.nemesis_at_end = self.db_node.running_nemesis
            full_partition_op_stat.exceptions.append(regular_op_stat.exceptions)
            full_partition_op_stat.exceptions.append(reversed_op_stat.exceptions)
            if comparison_result and not full_partition_op_stat.exceptions:
                full_partition_op_stat.success = True
            else:
                full_partition_op_stat.success = False


class FullScanAggregatesOperation(FullscanOperationBase):
    def __init__(self, generator, **kwargs):
        super().__init__(generator, scan_event=FullScanAggregateEvent, **kwargs)
        self._session_execution_timeout = self.fullscan_params.full_scan_aggregates_operation_limit + 10*60

    def randomly_form_cql_statement(self) -> str:
        cmd = FullScanAggregateCommands.AGG_COUNT_ALL.base_query.substitute(
            ks_cf=self.fullscan_params.ks_cf,
            timeout=f" USING TIMEOUT {self.fullscan_params.full_scan_aggregates_operation_limit}s",
            bypass_cache=BYPASS_CACHE_VALUES[0]
        )
        return cmd

    def execute_query(self, session, cmd: str,
                      event: Type[FullScanEvent | FullPartitionScanEvent
                                  | FullPartitionScanReversedOrderEvent]) -> None:
        self.log.debug('Will run command %s', cmd)
        validate_mapreduce_service_requests_start_time = time.time()
        cmd_result = session.execute(
            query=cmd, trace=False, timeout=self._session_execution_timeout)

        message, severity = self._validate_fullscan_result(cmd_result, validate_mapreduce_service_requests_start_time)
        if not severity:
            event.message = f"{type(self).__name__} operation ended successfully: {message}"
        else:
            event.severity = severity
            event.message = f"{type(self).__name__} operation failed: {message}"

    def _validate_fullscan_result(
            self, cmd_result: ResultSet, validate_mapreduce_service_requests_start_time):
        result = cmd_result.all()

        if not result:
            message = "Fullscan failed - got empty result"
            self.log.warning(message)
            return message, Severity.ERROR
        output = "\n".join([str(i) for i in result])
        if int(result[0].count) <= 0:
            return f"Fullscan failed - count is not bigger than 0: {output}", Severity.ERROR
        self.log.debug("Fullscan aggregation result: %s", output)

        try:
            self.validate_mapreduce_service_requests_dispatched_to_other_nodes(
                validate_mapreduce_service_requests_start_time)
        except Retry as retry_exception:
            self.log.debug("prometheus_mapreduce_service_requests metrics:\n %s",
                           str(retry_exception))
            self.log.debug("Fullscan aggregation result: %s", output)
            severity = Severity.ERROR
            if SkipPerIssues('https://github.com/scylladb/scylladb/issues/21578', TestConfig().tester_obj().params):
                severity = Severity.WARNING
            return "Fullscan failed - 'mapreduce_service_requests_dispatched_to_other_nodes' was not triggered", severity

        return f'result {result[0]}', None

    @retrying(n=6, sleep_time=10, allowed_exceptions=(Retry, ))
    def validate_mapreduce_service_requests_dispatched_to_other_nodes(self, start_time):
        prometheus = PrometheusDBStats(
            TestConfig().tester_obj().monitors.nodes[0].external_address)
        prometheus_mapreduce_service_requests = prometheus.query(
            'scylla_mapreduce_service_requests_dispatched_to_other_nodes',
            start_time, time.time())

        # expected format is :
        # [{'metric': {'__name__': 'scylla_mapreduce_service_requests_dispatched_to_other_nodes',
        # 'instance': '10.4.0.181', 'job': 'scylla', 'shard': '0'}, 'values': [[1690287334.763, '0']]}, ...]
        for metric in prometheus_mapreduce_service_requests:
            mapreduce_service_requests_dispatched_before_query = int(metric["values"][0][1])
            mapreduce_service_requests_dispatched_after_query = int(metric["values"][-1][1])
            if mapreduce_service_requests_dispatched_before_query < mapreduce_service_requests_dispatched_after_query:
                self.log.info('mapreduce_service_requests_dispatched_to_other_nodes was triggered')
                return

        raise Retry(prometheus_mapreduce_service_requests)


class PagedResultHandler:

    def __init__(self, future: ResponseFuture, scan_operation: FullPartitionScanOperation):
        self.error = None
        self.finished_event = threading.Event()
        self.future = future
        self.max_read_pages = scan_operation.fullscan_stats.read_pages
        self.current_read_pages = 0
        self.log = logging.getLogger(self.__class__.__name__)
        self.scan_operation = scan_operation
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error)

    def _row_to_string(self, row, include_data_column: bool = False) -> str:
        row_string = str(getattr(row, self.scan_operation.fullscan_params.pk_name))
        row_string += str(getattr(row, self.scan_operation.fullscan_params.ck_name))
        if include_data_column:
            row_string += str(getattr(row, self.scan_operation.fullscan_params.data_column_name))
        return f'{row_string}\n'

    def handle_page(self, rows):
        include_data_column = self.scan_operation.fullscan_params.include_data_column
        if self.scan_operation.scan_event == FullPartitionScanEvent:
            for row in rows:
                self.scan_operation.normal_query_output.write(
                    self._row_to_string(row=row, include_data_column=include_data_column))
        elif self.scan_operation.scan_event == FullPartitionScanReversedOrderEvent:
            self.scan_operation.fullscan_stats.number_of_rows_read += len(rows)
            if self.scan_operation.fullscan_params.validate_data:
                for row in rows:
                    self.scan_operation.reversed_query_output.write(
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
