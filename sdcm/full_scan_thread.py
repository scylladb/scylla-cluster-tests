import logging
import random
import threading
import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from sdcm.cluster import BaseNode, BaseScyllaCluster, BaseCluster
from sdcm.sct_events import Severity
from sdcm.sct_events.database import FullScanEvent


ERROR_SUBSTRINGS = ("timed out", "unpack requires", "timeout")


# pylint: disable=too-many-instance-attributes
class FullScanThread:
    query_options = (
        'select * from {}',
        'select * from {} bypass cache'
    )

    # pylint: disable=too-many-arguments
    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], ks_cf: str, duration: int, interval: int,
                 termination_event: threading.Event, page_size: int = 100000):
        self.ks_cf = ks_cf
        self.db_cluster = db_cluster
        self.page_size = page_size
        self.duration = duration
        self.interval = interval
        self.termination_event = termination_event
        self.log = logging.getLogger(self.__class__.__name__)
        self._thread = threading.Thread(daemon=True, name=self.__class__.__name__, target=self.run)

    def get_ks_cs(self, db_node: BaseNode):
        ks_cf_list = self.db_cluster.get_non_system_ks_cf_list(db_node)
        if self.ks_cf not in ks_cf_list:
            self.ks_cf = 'random'

        if 'random' in self.ks_cf.lower():
            return random.choice(ks_cf_list)
        return self.ks_cf

    @staticmethod
    def randomly_add_timeout(cmd) -> str:
        if random.choice([True] * 2 + [False]):
            cql_timeout_seconds = str(random.choice([2, 4, 8, 30, 120, 300]))
            cql_timeout_param = f" USING TIMEOUT {cql_timeout_seconds}s"
            cmd += cql_timeout_param
        return cmd

    def randomly_form_cql_statement(self, ks_cf: str) -> str:
        cmd = random.choice(self.query_options).format(ks_cf)
        return self.randomly_add_timeout(cmd)

    def create_session(self, db_node: BaseNode):
        credentials = self.db_cluster.get_db_auth()
        username, password = credentials if credentials else (None, None)
        return self.db_cluster.cql_connection_patient(db_node, user=username, password=password)

    def run_fullscan(self, db_node: BaseNode):  # pylint: disable=too-many-locals
        ks_cf = self.get_ks_cs(db_node)
        read_pages = random.choice([100, 1000, 0])
        with FullScanEvent(node=db_node.name, ks_cf=ks_cf, message="") as fs_event:
            cmd = self.randomly_form_cql_statement(ks_cf)

            with self.create_session(db_node) as session:

                if self.termination_event.is_set():
                    return

                try:
                    self.log.info('Will run command "%s"', cmd)
                    result = session.execute(SimpleStatement(
                        cmd,
                        fetch_size=self.page_size,
                        consistency_level=ConsistencyLevel.ONE))
                    pages = 0
                    while result.has_more_pages and pages <= read_pages:
                        result.fetch_next_page()
                        if read_pages > 0:
                            pages += 1
                    fs_event.message = "full scan ended successfully"
                except Exception as exc:  # pylint: disable=broad-except
                    msg = str(exc)
                    msg = f"{msg} while running Nemesis: {db_node.running_nemesis}" if db_node.running_nemesis else msg
                    fs_event.message = msg

                    if db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                        fs_event.severity = Severity.WARNING
                    else:
                        fs_event.severity = Severity.ERROR

    def run(self):
        end_time = time.time() + self.duration
        while time.time() < end_time and not self.termination_event.is_set():
            self.run_fullscan(db_node=random.choice(self.db_cluster.nodes))
            time.sleep(self.interval)

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)
