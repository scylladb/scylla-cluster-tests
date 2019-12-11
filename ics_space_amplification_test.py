import math
import time

from longevity_ics_test import IcsLongevetyTest
from sdcm.utils.common import retrying

KB_SIZE = 2 ** 10
MB_SIZE = KB_SIZE * 1024
GB_SIZE = MB_SIZE * 1024
MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB = 65


class IcsSpaceAmplificationTest(IcsLongevetyTest):

    def _get_used_capacity_gb(self, node):
        """
        :param node:
        :return: the file-system used-capacity on node (in GB)
        """
        used_capacity = self._get_filesystem_total_size_gb(node=node) - self._get_filesystem_available_size_gb(
            node=node)
        self.log.debug("Node {} used capacity is: {} GB".format(node.private_ip_address, used_capacity))
        return used_capacity

    def _get_prometheus_query_numeric_values_list(self, query, start_time=None):
        start_time = start_time or time.time()
        res = self.prometheus_db.query(query=query, start=start_time, end=time.time())
        return res[0]["values"]

    def _get_prometheus_query_numeric_value_gb(self, query):
        res = self._get_prometheus_query_numeric_values_list(query=query)
        return int(res[0][1]) / GB_SIZE

    def _get_filesystem_available_size_gb(self, node):
        """
        :param node:
        :return:
        """
        filesystem_available_size_query = 'sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
                                          'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_value_gb(query=filesystem_available_size_query)

    def _get_filesystem_available_size_list(self, node, start_time):
        """
        :param node:
        :return:
        """
        filesystem_available_size_query = 'sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
                                          'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_values_list(query=filesystem_available_size_query,
                                                              start_time=start_time)

    def _get_filesystem_total_size_gb(self, node):
        """
        :param node:
        :return:
        """
        filesystem_capacity_query = 'sum(node_filesystem_size{{mountpoint="{0.scylla_dir}", ' \
                                    'instance=~"{1.private_ip_address}"}})'.format(self, node)
        return self._get_prometheus_query_numeric_value_gb(query=filesystem_capacity_query)

    def _get_max_used_capacity_over_time_gb(self, node, start_time):
        """

        :param node:
        :param start_time: the start interval to search max-used-capacity from.
        :return:
        """
        fs_size_gb = self._get_filesystem_total_size_gb(node=node)
        end_time = time.time()
        time_interval_minutes = int(math.ceil((end_time - start_time) / 60))  # convert time to minutes and round up.
        min_available_capacity_gb = min([int(val[1]) for val in self._get_filesystem_available_size_list(node=node,
                                                                                                         start_time=start_time)]) / GB_SIZE
        max_used_capacity_gb = fs_size_gb - min_available_capacity_gb
        self.log.debug("The maximum used filesystem capacity of {} for the last {} minutes is: {} GB/ {} GB".format(
            node.private_ip_address, time_interval_minutes, max_used_capacity_gb, fs_size_gb))
        return max_used_capacity_gb

    def _get_nodes_space_amplification(self, dict_nodes_initial_capacity, start_time) -> dict:
        dict_nodes_space_amplification = {}
        dict_nodes_used_capacity = self._get_nodes_used_capacity()
        for node in self.db_cluster.nodes:
            node_max_used_capacity_gb = self._get_max_used_capacity_over_time_gb(node=node,
                                                                                 start_time=start_time)
            dict_nodes_space_amplification[node.private_ip_address] = node_max_used_capacity_gb - \
                dict_nodes_initial_capacity[
                node.private_ip_address]
            self.log.info(
                f"Node {node.private_ip_address} used capacity changed from {dict_nodes_initial_capacity[node.private_ip_address]} to {dict_nodes_used_capacity[node.private_ip_address]}.")
            self.log.info(f"Space amplification is: {dict_nodes_space_amplification[node.private_ip_address]} GB")
        return dict_nodes_space_amplification

    def _get_nodes_space_amplification_after_write(self, dict_nodes_initial_capacity, written_data_size_gb,
                                                   start_time) -> dict:
        self.log.info(f"Space amplification results after a write of: {written_data_size_gb} are:")
        return self._get_nodes_space_amplification(dict_nodes_initial_capacity=dict_nodes_initial_capacity,
                                                   start_time=start_time)

    def _get_nodes_used_capacity(self) -> dict:
        """

        :rtype: dictionary with capacity per node-ip
        """
        dict_nodes_used_capacity = {}
        for node in self.db_cluster.nodes:
            dict_nodes_used_capacity[node.private_ip_address] = self._get_used_capacity_gb(node=node)
        return dict_nodes_used_capacity

    @retrying(n=80, sleep_time=60, allowed_exceptions=(AssertionError,))
    def wait_no_compactions_running(self):
        compaction_query = "sum(scylla_compaction_manager_compactions{})"
        now = time.time()
        results = self.prometheus_db.query(query=compaction_query, start=now - 60, end=now)
        self.log.debug(f"scylla_compaction_manager_compactions: {results}")
        # if all are zeros the result will be False, otherwise there are still compactions
        if results:
            assert any([float(v[1]) for v in results[0]["values"]]) is False, \
                "Waiting until all compactions settle down"

    def test_ics_space_amplification(self):  # pylint: disable=too-many-locals
        # too-many-branches,too-many-statements
        self._pre_create_schema_with_compaction()
        stress_queue = list()
        write_queue = list()
        verify_queue = list()
        column_size = 200
        num_of_columns = 5
        ops_num = 200200300
        overwrite_ops_num = ops_num // 2
        total_new_data_to_write_gb = ops_num * column_size * num_of_columns / (1024 ** 3)
        total_data_to_overwrite_gb = overwrite_ops_num * column_size * num_of_columns / (1024 ** 3)
        keyspace_num = 1

        self.log.debug('Test Space-amplification on writing new data')
        prepare_write_cmd = "cassandra-stress write cl=ALL n={ops_num}  -schema 'replication(factor=3)" \
                            " compaction(strategy=IncrementalCompactionStrategy)' -port jmx=6868 -mode cql3 native" \
                            " -rate threads=1000 -col 'size=FIXED({column_size}) n=FIXED(num_of_columns)'" \
                            " -pop seq=1..200200300 -log interval=15".format(**locals())
        dict_nodes_initial_capacity = self._get_nodes_used_capacity()
        start_time = time.time()
        self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                       'keyspace_num': keyspace_num})

        # Wait on the queue till all threads come back.
        for stress in write_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

        dict_nodes_space_amplification = self._get_nodes_space_amplification_after_write(
            dict_nodes_initial_capacity=dict_nodes_initial_capacity,
            written_data_size_gb=total_new_data_to_write_gb, start_time=start_time)
        verify_nodes_space_amplification(dict_nodes_space_amplification=dict_nodes_space_amplification)

        self.log.debug('Test Space-amplification on over-write data')
        prepare_overwrite_cmd = "cassandra-stress write cl=ALL  n={overwrite_ops_num} -schema 'replication(factor=3) compaction(strategy=LeveledCompactionStrategy)' -port jmx=6868 -mode cql3 native" \
                                " -rate threads=1000 -col 'size=FIXED({column_size}) n=FIXED(num_of_columns)' -pop 'dist=uniform(1..{overwrite_ops_num})' ".format(
                                    **locals())

        verify_overwrite_queue = list()
        self.log.debug('Total data to write per cycle is: {} GB '.format(total_data_to_overwrite_gb))

        self.wait_no_compactions_running()
        overwrite_cycles_num = 4
        for i in range(1, overwrite_cycles_num + 1):
            self.log.debug('Starting overwrite stress cycle {}..'.format(i))
            dict_nodes_capacity_before_overwrite_data = self._get_nodes_used_capacity()
            start_time = time.time()
            self._run_all_stress_cmds(verify_overwrite_queue, params={'stress_cmd': prepare_overwrite_cmd,
                                                                      'keyspace_num': keyspace_num})
            for stress in verify_overwrite_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

            dict_nodes_space_amplification = self._get_nodes_space_amplification_after_write(
                dict_nodes_initial_capacity=dict_nodes_capacity_before_overwrite_data,
                written_data_size_gb=total_data_to_overwrite_gb, start_time=start_time)
            verify_nodes_space_amplification(dict_nodes_space_amplification=dict_nodes_space_amplification)

        self.log.debug('Test Space-amplification on major compaction')
        dict_nodes_capacity_before_major_compaction = self._get_nodes_used_capacity()
        start_time = time.time()
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")
        self.wait_no_compactions_running()
        dict_nodes_space_amplification = self._get_nodes_space_amplification(
            dict_nodes_initial_capacity=dict_nodes_capacity_before_major_compaction,
            start_time=start_time)
        verify_nodes_space_amplification(dict_nodes_space_amplification=dict_nodes_space_amplification)


def verify_nodes_space_amplification(dict_nodes_space_amplification):
    for node_ip, space_amplification_gb in dict_nodes_space_amplification.items():
        assert space_amplification_gb < MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB, \
            f'Node {node_ip} space amplification of: {space_amplification_gb} exceeds the maximum allowed ({MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB})'
