import datetime
import json
import logging
import random

from sdcm.cluster import BaseScyllaCluster, BaseCluster
from sdcm.utils.decorators import retrying


class NoSstableFound(Exception):
    pass


class NonDeletedTombstonesFound(Exception):
    pass


class SstableUtils:

    # pylint: disable=too-many-instance-attributes
    def __init__(self, db_cluster: [BaseScyllaCluster, BaseCluster], propagation_delay_in_seconds: int,
                 ks_cf: str = None, **kwargs):

        self.db_cluster: [BaseScyllaCluster, BaseCluster] = db_cluster
        self.ks_cf = ks_cf or random.choice(self.db_cluster.get_non_system_ks_cf_list(self.db_cluster.nodes[0]))
        self.keyspace, self.table = self.ks_cf.split('.')
        self.propagation_delay_in_seconds = propagation_delay_in_seconds
        self.db_node = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.user = kwargs.get("user", None)
        self.password = kwargs.get("password", None)

    def count_tombstones(self):
        sstables = self.get_sstables()
        tombstones_num = 0
        for sstable in sstables:
            tombstones_num += self.count_sstable_tombstones(sstable=sstable)
        return tombstones_num

    @retrying(n=10, allowed_exceptions=NoSstableFound)
    def get_sstables(self, from_minutes_ago: int = 0):
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

    def count_sstable_tombstones(self, sstable: str):
        self.db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=True)
        tombstones_deletion_info = self.db_node.remoter.run(
            'sudo grep marked_deleted /tmp/sstabledump.json').stdout.splitlines()
        return len(tombstones_deletion_info)

    def get_table_repair_date(self) -> str | None:
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

    def get_table_repair_date_and_delta_minutes(self) -> (datetime.datetime, int):

        table_repair_date = self.get_table_repair_date()
        table_repair_date = datetime.datetime.strptime(table_repair_date, '%Y-%m-%d %H:%M:%S')
        delta_repair_date_minutes = int(
            (datetime.datetime.now() - table_repair_date).seconds / 60) - self.propagation_delay_in_seconds
        return table_repair_date, delta_repair_date_minutes

    def get_tombstone_deletion_info(self, sstable: str):
        self.db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=True)
        tombstones_deletion_info = self.db_node.remoter.run(
            'sudo grep marked_deleted /tmp/sstabledump.json').stdout.splitlines()
        self.log.debug('Found %s tombstones for sstable %s', len(tombstones_deletion_info), sstable)
        return tombstones_deletion_info

    def verify_post_repair_sstable_tombstones(self, table_repair_date: datetime.datetime, sstable: str):
        non_deleted_tombstones = []
        tombstones_deletion_info = self.get_tombstone_deletion_info(sstable=sstable)
        for tombstone_deletion_info in tombstones_deletion_info:
            tombstone_delete_date = self.get_tombstone_date(tombstone_deletion_info=tombstone_deletion_info)
            if tombstone_delete_date < table_repair_date:
                non_deleted_tombstones.append(tombstone_delete_date)
        if non_deleted_tombstones:
            raise NonDeletedTombstonesFound(
                f"Found pre-repair time ({table_repair_date}) tombstones in a post-repair sstable ({sstable}): {non_deleted_tombstones}")

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
