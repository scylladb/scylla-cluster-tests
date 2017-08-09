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
import datetime

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources
from sdcm.data_path import get_data_path


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
        self._cluster_starting_size = self.params.get('cluster_initial_size', default=3)
        self._cluster_target_size = self.params.get('cluster_target_size', default=5)
        loader_info = {'n_nodes': 1, 'device_mappings': None, 'disk_size': None, 'disk_type': None,
                       'n_local_ssd': None, 'type': None}
        monitor_info = {'n_nodes': 1, 'device_mappings': None, 'disk_size': None, 'disk_type': None,
                        'n_local_ssd': None, 'type': None}
        db_info = {'n_nodes': self._cluster_starting_size, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                   'device_mappings': None, 'type': None}
        self.init_resources(loader_info=loader_info, db_info=db_info,
                            monitor_info=monitor_info)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        nodes_monitored = [node.private_ip_address for node in self.db_cluster.nodes]
        nodes_monitored += [node.private_ip_address for node in self.loaders.nodes]
        self.monitors.wait_for_init(targets=nodes_monitored)
        self.stress_thread = None

    def get_stress_cmd_profile(self):
        cs_custom_config = get_data_path('cassandra-stress-custom-mixed-narrow-wide-row.yaml')
        with open(cs_custom_config, 'r') as cs_custom_config_file:
            self.log.info('Using custom cassandra-stress config:')
            self.log.info(cs_custom_config_file.read())
        for node in self.loaders.nodes:
            node.remoter.send_files(cs_custom_config,
                                    '/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml',
                                    verbose=True)
        ip = self.db_cluster.get_node_private_ips()[0]
        return ('cassandra-stress user '
                'profile=/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml '
                'ops\(insert=1\) -node %s' % ip)

    def get_stress_cmd(self, mode='write', duration=None):
        """
        Get a cassandra stress cmd string suitable for grow cluster purposes.

        :param duration: Duration of stress (minutes).
        :param threads: Number of threads used by cassandra stress.
        :param population_size: Size of the -pop seq1..%s argument.
        :return: Cassandra stress string
        :rtype: basestring
        """
        ip = self.db_cluster.get_node_private_ips()[0]
        population_size = self.params.get('cassandra_stress_population_size', default=1000000)
        if not duration:
            duration = self.params.get('test_duration', default=60)
        threads = self.params.get('cassandra_stress_threads', default=1000)

        return ("cassandra-stress %s cl=QUORUM duration=%sm "
                "-schema 'replication(factor=3)' -port jmx=6868 -col 'size=FIXED(2) n=FIXED(1)' "
                "-mode cql3 native -rate threads=%s "
                "-pop seq=1..%s -node %s" %
                (mode, duration, threads, population_size, ip))

    def reconfigure_monitor(self):
        targets = [node.private_ip_address for node in self.db_cluster.nodes + self.loaders.nodes]
        for monitoring_node in self.monitors.nodes:
            monitoring_node.reconfigure_prometheus(targets=targets)

    def grow_cluster(self, cluster_target_size, stress_cmd):
        # 60 minutes should be long enough for adding each node
        nodes_to_add = cluster_target_size - self._cluster_starting_size
        duration = 60 * nodes_to_add
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd,
                                              duration=duration)
        # Wait for cluster is filled with data
        # Set space_node_threshold in config file for the size
        self.db_cluster.wait_total_space_used_per_node()

        start = datetime.datetime.now()
        self.log.info('Starting to grow cluster: %s' % str(start))

        add_node_cnt = self.params.get('add_node_cnt', default=1)
        node_cnt = len(self.db_cluster.nodes)
        while node_cnt < cluster_target_size:
            time.sleep(60)
            new_nodes = self.db_cluster.add_nodes(count=add_node_cnt)
            self.db_cluster.wait_for_init(node_list=new_nodes)
            node_cnt = len(self.db_cluster.nodes)
            self.reconfigure_monitor()

        end = datetime.datetime.now()
        self.log.info('Growing cluster finished: %s' % str(end))
        self.log.info('Growing cluster costs: %s' % str(end - start))

        # Run 2 more minutes before stop c-s
        time.sleep(2 * 60)

        # Kill c-s when decommission is done
        self.kill_stress_thread()

        self.verify_stress_thread(queue=stress_queue)
        self.run_stress(stress_cmd=self.get_stress_cmd('read', 10))

    def test_grow_3_to_5(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 5 nodes
        """
        self.grow_cluster(cluster_target_size=5,
                          stress_cmd=self.get_stress_cmd())

    def test_grow_3_to_4(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        """
        self.grow_cluster(cluster_target_size=4,
                          stress_cmd=self.get_stress_cmd())

    def test_grow_3_to_30(self):
        """
        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 30 nodes
        """
        self.grow_cluster(cluster_target_size=30,
                          stress_cmd=self.get_stress_cmd())

    def test_grow_x_to_y(self):
        """
        1) Start a x node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of y nodes
        """
        self.grow_cluster(cluster_target_size=self._cluster_target_size,
                          stress_cmd=self.get_stress_cmd())

    def test_grow_3_to_4_large_partition(self):
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        """
        self.grow_cluster(cluster_target_size=4,
                          stress_cmd=self.get_stress_cmd_profile())


if __name__ == '__main__':
    main()
