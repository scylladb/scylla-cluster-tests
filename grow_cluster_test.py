#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB

import logging
import time

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources
from sdcm.nemesis import Nemesis
from sdcm.nemesis import log_time_elapsed
from sdcm.data_path import get_data_path


class GrowClusterMonkey(Nemesis):

    @log_time_elapsed
    def disrupt(self):
        self._set_current_disruption('Add new node to %s' % self.cluster)
        new_nodes = self.cluster.add_nodes(count=1)
        self.cluster.wait_for_init(node_list=new_nodes)


class GrowClusterTest(ClusterTester):

    """
    Test scylla cluster growth (adding nodes after an initial cluster size).

    :avocado: enable
    """

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        self.monitors = None
        self.custom_cs_command = None
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        # We're starting the cluster with 3 nodes due to
        # replication factor settings we're using with cassandra-stress
        self._cluster_starting_size = 3
        self._cluster_target_size = None
        loader_info = {'n_nodes': 1, 'device_mappings': None,
                       'type': None}
        monitor_info = {'n_nodes': 1, 'device_mappings': None,
                        'type': None}
        db_info = {'n_nodes': self._cluster_starting_size,
                   'device_mappings': None, 'type': None}
        self.init_resources(loader_info=loader_info, db_info=db_info,
                            monitor_info=monitor_info)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        nodes_monitored = [node.public_ip_address for node in self.db_cluster.nodes]
        self.monitors.wait_for_init(targets=nodes_monitored)
        self.stress_thread = None

    def setup_custom_cs_profile(self):
        cs_custom_config = get_data_path('cassandra-stress-custom-mixed-narrow-wide-row.yaml')
        with open(cs_custom_config, 'r') as cs_custom_config_file:
            self.log.info('Using custom cassandra-stress config:')
            self.log.info(cs_custom_config_file.read())
        for node in self.loaders.nodes:
            node.remoter.send_files(cs_custom_config,
                                    '/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml',
                                    verbose=True)
        ip = self.db_cluster.get_node_private_ips()[0]
        self.custom_cs_command = ('cassandra-stress user '
                      'profile=/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml '
                      'ops\(insert=1\) -node %s' % ip)

    def cleanup_custom_cs_profile(self):
        self.custom_cs_command = None

    def get_stress_cmd(self, duration=None, threads=None, population_size=None,
                       mode='write', limit=None, row_size=None):
        """
        Get a cassandra stress cmd string suitable for grow cluster purposes.

        :param duration: Duration of stress (minutes).
        :param threads: Number of threads used by cassandra stress.
        :param population_size: Size of the -pop seq1..%s argument.
        :return: Cassandra stress string
        :rtype: basestring
        """

        if self.custom_cs_command:
            return self.custom_cs_command

        ip = self.db_cluster.get_node_private_ips()[0]
        if population_size is None:
            population_size = 1000000
        if duration is None:
            duration = self.params.get('cassandra_stress_duration')
        if threads is None:
            threads = self.params.get('cassandra_stress_threads')
        return ("cassandra-stress write cl=QUORUM duration=%sm "
                "-schema 'replication(factor=3)' -port jmx=6868 "
                "-mode cql3 native -rate threads=%s "
                "-pop seq=1..%s -node %s" %
                (duration, threads, population_size, ip))

    def grow_cluster(self, cluster_target_size):
        # 60 minutes should be long enough for adding each node
        nodes_to_add = cluster_target_size - self._cluster_starting_size
        duration = 60 * nodes_to_add
        stress_queue = self.run_stress_thread(duration=duration)

        # Wait for cluster is filled with data
        # Set space_node_threshold in config file for the size
        self.db_cluster.wait_total_space_used_per_node()

        self.db_cluster.add_nemesis(GrowClusterMonkey)
        while len(self.db_cluster.nodes) < cluster_target_size:
            # Run GrowClusterMonkey to add one node at a time
            self.db_cluster.start_nemesis(interval=10)
            self.db_cluster.stop_nemesis(timeout=None)

        # Run 2 more minutes before stop c-s
        time.sleep(2 * 60)

        # Kill c-s when decommission is done
        self.kill_stress_thread()

        self.verify_stress_thread(queue=stress_queue)

    def test_grow_3_to_5(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 5 nodes
        """
        self.grow_cluster(cluster_target_size=5)

    def test_grow_3_to_4(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        """
        self.grow_cluster(cluster_target_size=4)

    def test_grow_3_to_30(self):
        """
        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 30 nodes
        """
        self.grow_cluster(cluster_target_size=30)

    def test_grow_3_to_4_large_partition(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        """
        self.setup_custom_cs_profile()
        self.grow_cluster(cluster_target_size=4)
        self.cleanup_custom_cs_profile()

if __name__ == '__main__':
    main()
