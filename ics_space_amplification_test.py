import math
import time

from longevity_test import LongevityTest
from sdcm.cluster import SCYLLA_DIR

KB_SIZE = 2 ** 10
MB_SIZE = KB_SIZE * 1024
GB_SIZE = MB_SIZE * 1024
MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB = 65
FS_SIZE_METRIC = 'node_filesystem_size_bytes'
FS_SIZE_METRIC_OLD = 'node_filesystem_size'
AVAIL_SIZE_METRIC = 'node_filesystem_avail_bytes'
AVAIL_SIZE_METRIC_OLD = 'node_filesystem_avail'


class IcsSpaceAmplificationTest(LongevityTest):

    def _get_used_capacity_gb(self, node):  # pylint: disable=too-many-locals
        # example: node_filesystem_size_bytes{mountpoint="/var/lib/scylla", instance=~".*?10.0.79.46.*?"}-node_filesystem_avail_bytes{mountpoint="/var/lib/scylla", instance=~".*?10.0.79.46.*?"}
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)  # pylint: disable=unused-variable
        filesystem_capacity_query = '{FS_SIZE_METRIC}{node_capacity_query_postfix}'.format(
            **dict(locals(), **globals()))
        used_capacity_query = '{filesystem_capacity_query}-{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}'.format(
            **dict(locals(), **globals()))
        self.log.debug("filesystem_capacity_query: {filesystem_capacity_query}".format(**dict(locals(), **globals())))

        fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                               end=int(time.time()))
        assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = '{FS_SIZE_METRIC_OLD}{node_capacity_query_postfix}'.format(
                **dict(locals(), **globals()))
            used_capacity_query = '{filesystem_capacity_query}-{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}'.format(
                **dict(locals(), **globals()))
            self.log.debug("filesystem_capacity_query: {filesystem_capacity_query}".format(
                **dict(locals(), **globals())))
            fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                                   end=int(time.time()))

        assert fs_size_res[0], "Could not resolve capacity query result."
        self.log.debug("used_capacity_query: {}".format(used_capacity_query))
        used_cap_res = self.prometheus_db.query(query=used_capacity_query, start=int(time.time()) - 5,
                                                end=int(time.time()))
        assert used_cap_res, "No results from Prometheus"
        used_size_mb = float(used_cap_res[0]["values"][0][1]) / float(MB_SIZE)
        used_size_gb = round(float(used_size_mb / 1024), 2)
        self.log.debug(
            "The used filesystem capacity on node {} is: {} MB/ {} GB".format(node.public_ip_address, used_size_mb,
                                                                              used_size_gb))
        return used_size_gb

    def _get_filesystem_available_size_list(self, node, start_time):
        """
        :returns a list of file-system free-capacity on point in time from 'start_time' up to now.
        """
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)  # pylint: disable=unused-variable
        available_capacity_query = '{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}'.format(
            **dict(locals(), **globals()))
        available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                      end=int(time.time()))
        assert available_size_res, "No results from Prometheus"
        if not available_size_res[0]:  # if no returned values - try the old metric names.
            available_capacity_query = '{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}'.format(
                **dict(locals(), **globals()))
            available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                          end=int(time.time()))
        assert available_size_res[0], "Could not resolve available-size query result."
        return available_size_res[0]["values"]

    def _get_filesystem_total_size_gb(self, node):
        """
        :param node:
        :return:
        """
        fs_size_metric = 'node_filesystem_size_bytes'  # pylint: disable=unused-variable
        fs_size_metric_old = 'node_filesystem_size'  # pylint: disable=unused-variable
        node_capacity_query_postfix = generate_node_capacity_query_postfix(node)  # pylint: disable=unused-variable
        filesystem_capacity_query = '{fs_size_metric}{node_capacity_query_postfix}'.format(
            **dict(locals(), **globals()))
        fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                               end=int(time.time()))
        assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = '{fs_size_metric_old}{node_capacity_query_postfix}'.format(
                **dict(locals(), **globals()))
            self.log.debug("filesystem_capacity_query: {filesystem_capacity_query}".format(
                **dict(locals(), **globals())))
            fs_size_res = self.prometheus_db.query(query=filesystem_capacity_query, start=int(time.time()) - 5,
                                                   end=int(time.time()))
        assert fs_size_res[0], "Could not resolve capacity query result."
        self.log.debug("fs_size_res: {}".format(fs_size_res))
        fs_size_gb = float(fs_size_res[0]["values"][0][1]) / float(GB_SIZE)
        return fs_size_gb

    def _get_max_used_capacity_over_time_gb(self, node, start_time):
        """

        :param node:
        :param start_time: the start interval to search max-used-capacity from.
        :return:
        """
        fs_size_gb = self._get_filesystem_total_size_gb(node=node)
        end_time = time.time()
        time_interval_minutes = int(math.ceil((end_time - start_time) / 60))  # convert time to minutes and round up.
        min_available_capacity_gb = min(
            [int(val[1]) for val in
             self._get_filesystem_available_size_list(node=node, start_time=start_time)]) / GB_SIZE
        max_used_capacity_gb = fs_size_gb - min_available_capacity_gb
        self.log.debug("The maximum used filesystem capacity of {} for the last {} minutes is: {} GB/ {} GB".format(
            node.private_ip_address, time_interval_minutes, max_used_capacity_gb, fs_size_gb))
        return max_used_capacity_gb

    def _get_nodes_space_ampl_over_time_gb(self, dict_nodes_initial_capacity, start_time,
                                           written_data_size_gb=0):
        dict_nodes_space_amplification = {}
        dict_nodes_used_capacity = self._get_nodes_used_capacity()
        for node in self.db_cluster.nodes:
            node_initial_capacity = dict_nodes_initial_capacity[node.private_ip_address]
            node_used_capacity = dict_nodes_used_capacity[node.private_ip_address]  # pylint: disable=unused-variable
            node_max_used_capacity_gb = self._get_max_used_capacity_over_time_gb(node=node,
                                                                                 start_time=start_time)
            dict_nodes_space_amplification[node.private_ip_address] = round(node_max_used_capacity_gb -
                                                                            written_data_size_gb - node_initial_capacity, 2)
            self.log.info(
                "Node {node.private_ip_address} used capacity changed from {node_initial_capacity} to {node_used_capacity}.".format(
                    **dict(locals(), **globals())))
            self.log.info("Space amplification is: {} GB".format(
                dict_nodes_space_amplification[node.private_ip_address]))
        return dict_nodes_space_amplification

    def _get_nodes_space_amplification_after_write(self, dict_nodes_initial_capacity, written_data_size_gb,
                                                   start_time):
        self.log.info("Space amplification results after a write of: {written_data_size_gb} are:".format(
            **dict(locals(), **globals())))
        return self._get_nodes_space_ampl_over_time_gb(dict_nodes_initial_capacity=dict_nodes_initial_capacity,
                                                       written_data_size_gb=written_data_size_gb,
                                                       start_time=start_time)

    def _get_nodes_used_capacity(self):
        """

        :rtype: dictionary with capacity per node-ip
        """
        dict_nodes_used_capacity = {}
        for node in self.db_cluster.nodes:
            dict_nodes_used_capacity[node.private_ip_address] = self._get_used_capacity_gb(node=node)
        return dict_nodes_used_capacity

    def test_ics_space_amplification(self):  # pylint: disable=too-many-locals
        """
        Includes 3 space amplification test scenarios for ICS:
        (1) writing new data.
        (2) over-writing existing data.
        (3) running a major compaction.
        """
        stress_queue = list()  # pylint: disable=unused-variable
        write_queue = list()
        verify_queue = list()  # pylint: disable=unused-variable
        column_size = 200
        num_of_columns = 5
        # The number of writes for the total new data to test. impacts 'total_new_data_to_write_gb'.
        ops_num = 200200300
        overwrite_ops_num = ops_num // 2  # A half of the data size is used for the over-write data test.
        # The total data size to test. should be large enough to clearly identify space amplification issue.
        total_new_data_to_write_gb = round(ops_num * column_size * num_of_columns / (1024 ** 3), 2)
        total_data_to_overwrite_gb = round(overwrite_ops_num * column_size * num_of_columns / (1024 ** 3), 2)
        keyspace_num = 1

        self.log.debug('Test Space-amplification on writing new data')
        prepare_write_cmd = "cassandra-stress write cl=ALL n={ops_num}  -schema 'replication(factor=3)" \
                            " compaction(strategy=IncrementalCompactionStrategy)' -port jmx=6868 -mode cql3 native" \
                            " -rate threads=1000 -col 'size=FIXED({column_size}) n=FIXED({num_of_columns})'" \
                            " -pop seq=1..{ops_num} -log interval=15".format(**dict(locals(), **globals()))
        dict_nodes_initial_capacity = self._get_nodes_used_capacity()
        start_time = time.time()
        self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                       'keyspace_num': keyspace_num})

        # Wait on the queue till all threads come back.
        for stress in write_queue:
            self.verify_stress_thread(cs_thread_pool=stress)
        self.wait_no_compactions_running()

        dict_nodes_space_amplification = self._get_nodes_space_amplification_after_write(
            dict_nodes_initial_capacity=dict_nodes_initial_capacity,
            written_data_size_gb=total_new_data_to_write_gb, start_time=start_time)
        verify_nodes_space_amplification(dict_nodes_space_amplification=dict_nodes_space_amplification)

        self.log.debug('Test Space-amplification on over-write data')
        prepare_overwrite_cmd = "cassandra-stress write cl=ALL  n={overwrite_ops_num} -schema 'replication(factor=3) compaction(strategy=IncrementalCompactionStrategy)' -port jmx=6868 -mode cql3 native" \
                                " -rate threads=1000 -col 'size=FIXED({column_size}) n=FIXED({num_of_columns})' -pop 'dist=uniform(1..{overwrite_ops_num})' ".format(
                                    **dict(locals(), **globals()))

        verify_overwrite_queue = list()
        self.log.debug('Total data to write per cycle is: {} GB '.format(total_data_to_overwrite_gb))

        overwrite_cycles_num = 4
        for i in range(1, overwrite_cycles_num + 1):
            self.log.debug('Starting overwrite stress cycle {}..'.format(i))
            dict_nodes_capacity_before_overwrite_data = self._get_nodes_used_capacity()  # pylint: disable=invalid-name
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
        dict_nodes_capacity_before_major_compaction = self._get_nodes_used_capacity()  # pylint: disable=invalid-name
        start_time = time.time()
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")
        self.wait_no_compactions_running()
        dict_nodes_space_amplification = self._get_nodes_space_ampl_over_time_gb(
            dict_nodes_initial_capacity=dict_nodes_capacity_before_major_compaction,
            start_time=start_time)
        verify_nodes_space_amplification(dict_nodes_space_amplification=dict_nodes_space_amplification)


def verify_nodes_space_amplification(dict_nodes_space_amplification):  # pylint: disable=invalid-name
    for node_ip, space_amplification_gb in dict_nodes_space_amplification.items():  # pylint: disable=unused-variable
        assert space_amplification_gb < MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB, \
            'Node {node_ip} space amplification of: {space_amplification_gb} exceeds the maximum allowed ({MAX_ICS_SPACE_AMPLIFICATION_ALLOWED_GB})'.format(
                **dict(locals(), **globals()))


def generate_node_capacity_query_postfix(node):  # pylint: disable=invalid-name
    node_ip = node.private_ip_address  # due to pylint bug # pylint: disable=unused-variable
    scylla_dir = SCYLLA_DIR  # due to pylint bug # pylint: disable=unused-variable
    return '{{mountpoint="{scylla_dir}", instance=~".*?{node_ip}.*?"}}'.format(
        **dict(locals(), **globals()))
