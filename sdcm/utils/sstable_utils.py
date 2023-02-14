import datetime
import json
import logging
import random

from sdcm.cluster import BaseScyllaCluster, BaseCluster, BaseNode


class NonDeletedTombstonesFound(Exception):
    pass


class SstableUtils:

    # pylint: disable=too-many-instance-attributes
    def __init__(self, propagation_delay_in_seconds: int, ks_cf: str = None, **kwargs):

        self.db_node: BaseNode = kwargs.get("db_node", None)
        self.db_cluster: [BaseScyllaCluster, BaseCluster] = self.db_node.parent_cluster if self.db_node else None
        self.ks_cf = ks_cf or random.choice(self.db_cluster.get_non_system_ks_cf_list(self.db_cluster.nodes[0]))
        self.keyspace, self.table = self.ks_cf.split('.')
        self.propagation_delay_in_seconds = propagation_delay_in_seconds
        self.log = logging.getLogger(self.__class__.__name__)
        self.user = kwargs.get("user", None)
        self.password = kwargs.get("password", None)

    def count_tombstones(self):
        sstables = self.get_sstables()
        tombstones_num = 0
        for sstable in sstables:
            tombstones_num += self.count_sstable_tombstones(sstable=sstable)
        self.log.debug('Got %s tombstones for %s', tombstones_num, self.ks_cf)
        return tombstones_num

    def get_sstables(self, from_minutes_ago: int = 0):
        selected_sstables = []
        ks_cf_path = self.ks_cf.replace('.', '/')
        find_cmd = f"find /var/lib/scylla/data/{ks_cf_path}-*/*-big-Data.db -maxdepth 1 -type f"
        if from_minutes_ago:
            find_cmd += f" -cmin -{from_minutes_ago}"
        try:
            sstables_res = self.db_node.remoter.sudo(find_cmd, verbose=True, ignore_status=True)
            if sstables_res.stderr:
                self.log.debug('Failed to get sstables for %s. Error: %s', self.ks_cf, sstables_res.stderr)
            else:
                selected_sstables = sstables_res.stdout.split()
        except Exception as error:  # pylint: disable=broad-except
            self.log.debug('Failed to find sstables for %s: %s', self.ks_cf, error)

        message = f'filtered by last {from_minutes_ago} minutes' if from_minutes_ago else '(not filtered by time)'
        self.log.debug('Got %s sstables %s', len(selected_sstables), message)
        return selected_sstables

    def count_sstable_tombstones(self, sstable: str) -> int:
        try:
            self.db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=False)
            tombstones_deletion_info = self.db_node.remoter.run(
                'sudo egrep \'"expired" : true|marked_deleted\' /tmp/sstabledump.json', verbose=False, ignore_status=True)
            if not tombstones_deletion_info:
                self.log.debug('Got no tombstones for sstable: %s', sstable)
                return 0

            num_tombstones = len(tombstones_deletion_info.stdout.splitlines())
            self.log.debug('Got %s tombstones for sstable: %s', num_tombstones, sstable)
            return num_tombstones
        except Exception as error:  # pylint: disable=broad-except
            self.log.error('count_sstable_tombstones failed with: %s', error)
            return 0

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
                self.log.debug('SELECT repair_time results: %s', output)
                if len(output) == 0:
                    self.log.debug('No repair history found for %s.%s', self.keyspace, self.table)
                    return None
                return str(output[-1].repair_time)
            except Exception as exc:  # pylint: disable=broad-except
                self.log.error('Failed to get repair date of %s.%s. Error: %s', self.keyspace, self.table, exc)
                raise

    def get_table_repair_date_and_delta_minutes(self) -> (datetime.datetime, int):

        table_repair_date = datetime.datetime.strptime(self.get_table_repair_date(), '%Y-%m-%d %H:%M:%S')
        now = datetime.datetime.now()
        delta_repair_date_minutes = ((now - table_repair_date).seconds - self.propagation_delay_in_seconds) // 60
        self.log.debug('Found table-repair-date: %s, Ended %s minutes ago',
                       table_repair_date, delta_repair_date_minutes)
        return table_repair_date, delta_repair_date_minutes

    def get_compacted_tombstone_deletion_info(self, sstable: str) -> list:
        tombstones_deletion_info = []
        try:
            self.db_node.remoter.run(f'sudo sstabledump  {sstable} 1>/tmp/sstabledump.json', verbose=False)
            result = self.db_node.remoter.run('sudo grep marked_deleted /tmp/sstabledump.json', verbose=False,
                                              ignore_status=True)
            if result.ok:
                tombstones_deletion_info = result.stdout.splitlines()
            else:
                self.log.warning('Failed to find compacted tombstones in %s: (%s, %s)',
                                 sstable, result.stdout, result.stderr)
        except Exception as error:  # pylint: disable=broad-except
            self.log.debug('Failed to find compacted tombstones in %s: (%s)', sstable, error)
        self.log.debug('Found %s tombstones for sstable %s', len(tombstones_deletion_info), sstable)
        return tombstones_deletion_info

    def verify_post_repair_sstable_tombstones(self, table_repair_date: datetime.datetime, sstable: str):
        non_deleted_tombstones = []
        tombstones_deletion_info = self.get_compacted_tombstone_deletion_info(sstable=sstable)
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
