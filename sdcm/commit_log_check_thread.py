import traceback
import logging
import time
import re
import threading
from dataclasses import dataclass
from distutils.util import strtobool
from sdcm.sct_events.database import CommitLogCheckErrorEvent, Severity
from sdcm.rest.remote_curl_client import RemoteCurlClient


def get_max_disk_size_metric(db_cluster):
    node = db_cluster.nodes[0]
    node.wait_native_transport()
    return RemoteCurlClient(
        host="localhost:10000", endpoint="commitlog",
        node=node).run_remoter_curl(method="GET", path="metrics/max_disk_size", params=None).stdout


@dataclass
class CommitlogConfigParams:
    def __init__(self, db_cluster, ):
        logger = logging.getLogger(self.__class__.__name__)
        with db_cluster.cql_connection_patient(
                node=db_cluster.data_nodes[0],
                connect_timeout=300,) as session:
            self.use_hard_size_limit = bool(strtobool(session.execute(
                "SELECT value FROM system.config WHERE name='commitlog_use_hard_size_limit'").one().value))
            self.segment_size_in_mb = int(session.execute(
                "SELECT value FROM system.config WHERE name='commitlog_segment_size_in_mb'").one().value)
            self.max_disk_size = int(RemoteCurlClient(
                host="localhost:10000", endpoint="commitlog", node=db_cluster.nodes[0]).run_remoter_curl(
                method="GET", path="metrics/max_disk_size", params=None).stdout)
            self.smp = len(re.findall(
                "shard",
                db_cluster.data_nodes[0].remoter.run('sudo seastar-cpu-map.sh -n scylla').stdout))
            self.total_space = int(self.max_disk_size / self.smp)

            logger.debug("CommitlogConfigParams")
            logger.debug("smp: %s", self.smp)
            logger.debug("max_disk_size: %s", self.max_disk_size)
            logger.debug("total_space: %s", self.total_space)
            logger.debug("use_hard_size_limit: %s", self.use_hard_size_limit)
            logger.debug("segment_size_in_mb: %s", self.segment_size_in_mb)

    smp: int
    total_space: int
    max_disk_size: int
    use_hard_size_limit: bool
    segment_size_in_mb: int


@dataclass
class PrometheusQueries:
    def __init__(self, commitlog_params: CommitlogConfigParams):
        self.overflow_commit_log_directory = self.overflow_commit_log_directory.format(
            commitlog_total_space=commitlog_params.total_space,
            commitlog_segment_size_in_mb=commitlog_params.segment_size_in_mb
        )

        self.zero_free_segments = self.zero_free_segments.format(
            commitlog_total_space=commitlog_params.total_space,
            commitlog_segment_size_in_mb=commitlog_params.segment_size_in_mb
        )
    # Prometheus queries with detailed description below
    overflow_commit_log_directory = ("scylla_commitlog_disk_total_bytes>=({commitlog_total_space}"
                                     "%2B({commitlog_segment_size_in_mb}%2A1048576))")
    """
        returns commitlog total size on disk if commit log directory exceed the limit

        commit_log_directory_query description:
        %2A is *
        %2B is +

        {commitlog_total_space} :
        set to max_disk_size(value from API) divided  to number for shards
        see CommitLogCheckThread.init_commitlog_params_from_db()

        + {commitlog_segment_size_in_mb}*1048576) :
        add size of 1 segment,  Scylla actually stops creating new segments not when the next segment
        would cause us to exceed the value,
        but instead it checks if we've already reached or exceeded the limit.

        scylla_commitlog_disk_total_bytes > :
        filter all good values when scylla_commitlog_disk_total_bytes<= commitlog_total_space + 1 segment
        and return from Prometheus only bad
    """

    zero_free_segments = \
        ("scylla_commitlog_disk_active_bytes>((scylla_commitlog_disk_total_bytes>={commitlog_total_space})"
         "%2D({commitlog_segment_size_in_mb}%2A1048576))")
    """
        returns commitlog active size on disk if number of free segments drop to zero

        free_segments_query description:
        %2A is *
        %2D is -

        {commitlog_total_space} :
        set to max_disk_size(value from API) divided  to number for shards
        see CommitLogCheckThread.init_commitlog_params_from_db()

        scylla_commitlog_disk_total_bytes >= {commitlog_total_space}:
        Scylla has 0 free segments and allocates new segments until exceed limit {commitlog_total_space}
        this condition will ignore result when Scylla does not exceed  segments limit

        - {commitlog_segment_size_in_mb}*1048576) :
        minus size of 1 segment,  to ensure that Scylla has at least 1 free segment number of active_bytes should be
        less then "scylla_commitlog_disk_total_bytes"-"1 segment"

        scylla_commitlog_disk_active_bytes > :
        filter all good values when scylla_commitlog_disk_active_bytes<= scylla_commitlog_disk_total_bytes - 1 segment
        and return from Prometheus only bad
    """


class CommitLogCheckThread:
    """
        if commitlog-use-hard-size-limit is enabled,
        this thread will check the following metrics during the test:

        1) commit log directory does not exceed the limit at any stage of the test.
           if it exceeded - send an error event that fails the test.

        2) free segments don't drop to zero at any stage of the test,
        otherwise, send an error event that fails the test.
    """

    # Prometheus API limit: check_interval/discreteness must be less then 11000
    check_interval = 10 * 60
    discreteness = 1
    # scylla_commitlog_disk_total_bytes can exceed the limit during/after replay. For a short bit.
    exceed_time_interval = 60

    def __init__(self, custer_tester, test_duration, termination_event=None, thread_name: str = ""):
        self.log = logging.getLogger(self.__class__.__name__)
        self.prometheus = custer_tester.prometheus_db
        self.test_duration = test_duration

        self.commitlog_params = CommitlogConfigParams(custer_tester.db_cluster)
        self.prometheus_queries = PrometheusQueries(self.commitlog_params)
        self.start_time = None

        self._thread = threading.Thread(
            daemon=True, name=f"{self.__class__.__name__}_{thread_name}", target=self.run_thread)
        self.termination_event = termination_event
        if not self.termination_event:
            self.termination_event = custer_tester.db_cluster.nemesis_termination_event

    def start(self):
        if self.commitlog_params.use_hard_size_limit:
            self.log.debug("starting CommitLogCheckThread")
            self._thread.start()
        else:
            self.log.debug(
                "CommitLogCheckThread was not started due to commitlog_use_hard_size_limit is %s",
                self.commitlog_params.use_hard_size_limit)

    def join(self, timeout=None):
        return self._thread.join(timeout)

    def run_thread(self):
        self.log.debug("CommitLogCheckThread Started")
        try:
            thread_end_time = time.time() + self.test_duration

            self.start_time = time.time()

            while time.time() < thread_end_time and not self.termination_event.is_set():
                interval_end_time = self.start_time + self.check_interval
                self.termination_event.wait(self.check_interval)
                self.overflow_commit_log_directory_checker(self.start_time, interval_end_time)
                self.zero_free_segments_checker(self.start_time, interval_end_time)

                self.start_time = interval_end_time
        except Exception as exc:  # noqa: BLE001
            trace = traceback.format_exc()
            CommitLogCheckErrorEvent(
                message=f"CommitLogCheckThread failed: {exc.__repr__()} with traceback {trace}").publish()
        self.log.debug("CommitLogCheckThread finished")

    @staticmethod
    def get_commit_log_directory_max_exceed_time(prometheus_response):
        # get max time diff from response per shard(line in response)
        # response example:
        # [{'metric': {'__name__': 'scylla_commitlog_disk_total_bytes',
        #              'instance': '10.0.0.6', 'job': 'scylla', 'shard': '0'},
        #              'values': [[1706415723.153, '54324629504'],  [1706415743.153, '54324629504']]},
        #  {'metric': {'__name__': 'scylla_commitlog_disk_total_bytes',
        #             'instance': '10.0.0.7', 'job': 'scylla', 'shard': '1'},
        #             'values': [[1706415724.153, '54324629504'], [1706415725.153, '54324629504']]}
        # ]
        exceed_time_list = []
        for line in prometheus_response:
            time_list = [item[0] for item in line["values"]]
            time_list.sort()
            exceed_time_list.append(time_list[-1] - time_list[0])
        exceed_time_list.sort()
        return exceed_time_list[-1]

    def overflow_commit_log_directory_checker(self, start_time, end_time):
        response = self.prometheus.query(
            self.prometheus_queries.overflow_commit_log_directory, start_time, end_time, self.discreteness)
        self.log.debug("overflow_commit_log_directory: %s", response)
        if response and self.get_commit_log_directory_max_exceed_time(response) > self.exceed_time_interval:
            CommitLogCheckErrorEvent(
                message=f"commit log directory exceed the limit longer that expected."
                f" Prometheus response: {response}").publish()

    def zero_free_segments_checker(self, start_time, end_time):
        response = self.prometheus.query(
            self.prometheus_queries.zero_free_segments, start_time, end_time, self.discreteness)
        self.log.debug("zero_free_segments: %s", response)
        if response:
            CommitLogCheckErrorEvent(
                message=f"free segments drop to zero. Prometheus response:: {response}").publish()

    @staticmethod
    def run(custer_tester, duration):
        # check known requirements
        if not (custer_tester.monitors and custer_tester.monitors.nodes):
            CommitLogCheckErrorEvent(
                severity=Severity.WARNING,
                message="CommitLogCheckThread will not start due to no monitors in the cluster").publish()
            return

        if "Not found" in get_max_disk_size_metric(custer_tester.db_cluster):
            CommitLogCheckErrorEvent(
                severity=Severity.WARNING,
                message="CommitLogCheckThread will not start due to current scylla version has no "
                        "commitlog/metrics/max_disk_size endpoint ").publish()
            return

        try:
            thread = CommitLogCheckThread(custer_tester, duration)
        except Exception as exc:  # noqa: BLE001
            trace = traceback.format_exc()
            CommitLogCheckErrorEvent(
                message=f"CommitLogCheckThread.__init__ failed with unexpected exception:"
                f" {exc.__repr__()} with traceback {trace}").publish()
        else:
            try:
                thread.start()
            except Exception as exc:  # noqa: BLE001
                trace = traceback.format_exc()
                CommitLogCheckErrorEvent(
                    message=f"CommitLogCheckThread.start failed with unexpected exception:"
                    f" {exc.__repr__()} with traceback {trace}").publish()
