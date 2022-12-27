import datetime
import json
import logging
import random
import threading
import time

from sdcm import wait
from sdcm.cluster import BaseScyllaCluster, BaseCluster
from sdcm.remote import LocalCmdRunner
from sdcm.sct_events import Severity
from sdcm.sct_events.database import TombstoneGcVerificationEvent
from sdcm.utils.decorators import retrying

LOCAL_CMD_RUNNER = LocalCmdRunner()
ERROR_SUBSTRINGS = ("timed out", "timeout")


class NonDeletedTombstonesFound(Exception):
    pass


class NoSstableFound(Exception):
    pass


# pylint: disable=too-many-instance-attributes
class TombstoneGcVerificationThread:

    # pylint: disable=too-many-arguments

    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], duration: int, interval: int,
                 propagation_delay_in_seconds: int, termination_event: threading.Event, ks_cf: str = None, **kwargs):

        self.db_cluster: [BaseScyllaCluster, BaseCluster] = db_cluster
        self.ks_cf = ks_cf or random.choice(self.db_cluster.get_non_system_ks_cf_list(self.db_cluster.nodes[0]))
        self.keyspace, self.table = self.ks_cf.split('.')
        self.duration = duration
        self.interval = interval
        self.propagation_delay_in_seconds = propagation_delay_in_seconds
        self.db_node = None
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

    @retrying(n=10, allowed_exceptions=NoSstableFound)
    def _get_sstables(self, from_minutes_ago: int = 0):
        ks_cf_path = self.ks_cf.replace('.', '/')
        find_cmd = f"find /var/lib/scylla/data/{ks_cf_path}-*/*-big-Data.db -maxdepth 1 -type f"
        if from_minutes_ago:
            find_cmd += f" -cmin -{from_minutes_ago}"
        sstables_res = self.db_node.remoter.sudo(find_cmd, verbose=True)  # TODO: change to verbose=False?
        if sstables_res.stderr:
            raise NoSstableFound(
                'Failed to get sstables for {}. Error: {}'.format(ks_cf_path, sstables_res.stderr))

        selected_sstables = sstables_res.stdout.split()
        self.log.debug('Got %s sstables filtered by last %s minutes', len(selected_sstables),
                       from_minutes_ago)
        return selected_sstables

    def _get_table_repair_date(self) -> str | None:
        """
        Search entries of requested table in system.repair_history
        Return last entry found.
        Example returned value: '2022-12-28 11:53:53'
        """
        with self.db_cluster.cql_connection_patient(node=self.db_node, connect_timeout=300,
                                                    user=self.user, password=self.password) as session:
            try:
                query = f"SELECT repair_time from system.repair_history WHERE keyspace_name = '{self.keyspace}' " \
                        f"AND table_name = '{self.table}' ALLOW FILTERING;"
                results = session.execute(query)
                output = results.all()
                self.log.debug('SELECT repair_time results: %s', output)  # TODO: REMOVE
                if len(output) == 0:
                    self.log.debug('No repair history found for %s.%s', self.keyspace, self.table)
                    return None
                return str(results[-1].repair_time)
            except Exception as exc:  # pylint: disable=broad-except
                self.log.warning('Failed to get repair date of %s.%s. Error: %s', self.keyspace, self.table, exc)
                raise

    @staticmethod
    def get_tombstone_date(tombstone_deletion_info: str) -> datetime.datetime:
        """
        Parse a datetime value out of an sstable tombstone dump.
        Example input is: '{ "name" : "name_list", "deletion_info" : { "marked_deleted" : "2023-01-03T18:06:36.559369Z",
         "local_delete_time" : "2023-01-03T18:06:36Z" } },'
        Example Output:  datetime.datetime(2023, 1, 3, 18, 5, 58)
        """
        tombstone_dict = json.loads(tombstone_deletion_info[:-1])
        deletion_date, deletion_time = tombstone_dict['deletion_info']['marked_deleted'].split('T')
        deletion_hour, deletion_minutes, deletion_seconds = deletion_time.split(':')
        deletion_seconds = deletion_seconds.split('.')[0]
        full_deletion_date = f'{deletion_date} {deletion_hour}:{deletion_minutes}:{deletion_seconds}'
        full_deletion_date_datetime = datetime.datetime.strptime(full_deletion_date, '%Y-%m-%d %H:%M:%S')
        return full_deletion_date_datetime

    def verify_post_repair_sstable_tombstones(self, table_repair_date: datetime, sstable: str):
        non_deleted_tombstones = []
        self.db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=True)
        tombstones_deletion_info = self.db_node.remoter.run(
            'sudo grep marked_deleted /tmp/sstabledump.json').stdout.splitlines()
        self.log.debug('Found %s tombstones for sstable %s', len(tombstones_deletion_info), sstable)
        for tombstone_deletion_info in tombstones_deletion_info:
            tombstone_delete_date = self.get_tombstone_date(tombstone_deletion_info=tombstone_deletion_info)
            if tombstone_delete_date < table_repair_date:
                non_deleted_tombstones.append(tombstone_delete_date)
        if non_deleted_tombstones:
            raise NonDeletedTombstonesFound(
                f"Found pre-repair time ({table_repair_date}) tombstones in a post-repair sstable ({sstable}): {non_deleted_tombstones}")

    def tombstone_gc_verification(self, max_sstable_num: int = 50):
        table_repair_date = self._get_table_repair_date()  # Example: 2022-12-28 11:53:53
        if not table_repair_date:
            return
        table_repair_date = datetime.datetime.strptime(table_repair_date, '%Y-%m-%d %H:%M:%S')
        delta_repair_date_minutes = int(
            (datetime.datetime.now() - table_repair_date).seconds / 60) - self.propagation_delay_in_seconds
        if delta_repair_date_minutes <= 0:
            self.log.debug('Table %s repair date is smaller than propagation delay, aborting.', self.ks_cf)
            return
        sstables = self._get_sstables(from_minutes_ago=delta_repair_date_minutes)
        if not sstables:
            self.log.warning('No sstable with a creation time of last %s minutes found, aborting.',
                             delta_repair_date_minutes)
            return
        self.log.debug('Starting sstabledump to verify correctness of tombstones for %s sstables',
                       len(sstables))
        if max_sstable_num < len(sstables):
            sstables = sstables[:max_sstable_num]
        for sstable in sstables:
            self.verify_post_repair_sstable_tombstones(table_repair_date=table_repair_date, sstable=sstable)

    def run_tombstone_gc_verification(self):
        db_node = self.db_node
        self.wait_until_user_table_exists(db_node=db_node, table_name=self.ks_cf)
        with TombstoneGcVerificationEvent(node=db_node.name, ks_cf=self.ks_cf, message="") as tombstone_event:
            if self.termination_event.is_set():
                return

            try:
                self.tombstone_gc_verification()
                tombstone_event.message = "Tombstone GC verification ended successfully"
            except Exception as exc:  # pylint: disable=broad-except
                msg = str(exc)
                msg = f"{msg} while running Nemesis: {db_node.running_nemesis}" if db_node.running_nemesis else msg
                tombstone_event.message = msg

                if db_node.running_nemesis or any(s in msg.lower() for s in ERROR_SUBSTRINGS):
                    tombstone_event.severity = Severity.WARNING
                else:
                    tombstone_event.severity = Severity.ERROR

    def run(self):
        end_time = time.time() + self.duration
        while time.time() < end_time and not self.termination_event.is_set():
            self.db_node = random.choice(self.db_cluster.nodes)
            self.run_tombstone_gc_verification()
            self.log.debug('Executed %s', TombstoneGcVerificationEvent.__name__)
            time.sleep(self.interval)

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        return self._thread.join(timeout)
