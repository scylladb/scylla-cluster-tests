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

import time
import datetime
import random

from sdcm.tester import ClusterTester
from sdcm.utils.common import get_data_dir_path, skip_optional_stage
from sdcm import nemesis
from sdcm import prometheus


class GrowClusterTest(ClusterTester):

    """
    Test scylla cluster growth (adding nodes after an initial cluster size).
    """

    def setUp(self):
        super().setUp()
        self._cluster_starting_size = self.params.get('n_db_nodes')
        self._cluster_target_size = self.params.get('cluster_target_size')
        self.metrics_srv = prometheus.nemesis_metrics_obj()

    def get_stress_cmd_profile(self):
        cs_custom_config = get_data_dir_path('cassandra-stress-custom-mixed-narrow-wide-row.yaml')
        with open(cs_custom_config, encoding="utf-8") as cs_custom_config_file:
            self.log.info('Using custom cassandra-stress config:')
            self.log.info(cs_custom_config_file.read())
        for node in self.loaders.nodes:
            node.remoter.send_files(cs_custom_config,
                                    '/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml',
                                    verbose=True)
        ip = self.db_cluster.get_node_private_ips()[0]
        return (r'cassandra-stress user '
                r'profile=/tmp/cassandra-stress-custom-mixed-narrow-wide-row.yaml '
                r'ops\(insert=1\) -node %s' % ip)

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
        population_size = self.params.get('cassandra_stress_population_size')
        if not duration:
            duration = self.params.get('test_duration')
        threads = self.params.get('cassandra_stress_threads')

        return ("cassandra-stress %s cl=QUORUM duration=%sm "
                "-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -col 'size=FIXED(2) n=FIXED(1)' "
                "-mode cql3 native -rate threads=%s "
                "-pop seq=1..%s -node %s" %
                (mode, duration, threads, population_size, ip))

    def add_nodes(self, add_node_cnt):
        self.metrics_srv.event_start('add_node')
        new_nodes = self.db_cluster.add_nodes(count=add_node_cnt, enable_auto_bootstrap=True)
        self.db_cluster.wait_for_init(node_list=new_nodes)
        self.metrics_srv.event_stop('add_node')
        self.monitors.reconfigure_scylla_monitoring()

    def grow_cluster(self, cluster_target_size, stress_cmd):
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
        # default=1440 min (one day) if test_duration is not defined
        duration = self.params.get('test_duration')
        if not skip_optional_stage('main_load'):
            cs_thread_pool = self.run_stress_thread(stress_cmd=stress_cmd,
                                                    duration=duration)

        time.sleep(2 * 60)

        start = datetime.datetime.now()
        self.log.info('Starting to grow cluster: %s', str(start))

        add_node_cnt = self.params.get('add_node_cnt')
        node_cnt = len(self.db_cluster.nodes)
        while node_cnt < cluster_target_size:
            time.sleep(60)
            self.add_nodes(add_node_cnt)
            node_cnt = len(self.db_cluster.nodes)

        end = datetime.datetime.now()
        self.log.info('Growing cluster finished: %s', str(end))
        self.log.info('Growing cluster costs: %s', str(end - start))

        # Run 2 more minutes before start nemesis
        time.sleep(2 * 60)

        if self.params.get('nemesis_class_name').lower() != 'noopmonkey':
            self.db_cluster.start_nemesis()

        if not skip_optional_stage('main_load'):
            self.verify_stress_thread(cs_thread_pool=cs_thread_pool)

    def test_grow_x_to_y(self):
        """
        1) Start a x node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of y nodes
        """
        self.grow_cluster(cluster_target_size=self._cluster_target_size,
                          stress_cmd=self.get_stress_cmd())

    def test_grow_3_to_4_large_partition(self):  # pylint: disable=invalid-name
        """
        Shorter version of the cluster growth test.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        """
        self.grow_cluster(cluster_target_size=4,
                          stress_cmd=self.get_stress_cmd_profile())

    def test_add_remove_nodes(self):
        """
        1) Start a x node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Decommission random chosen node
        5) Repeat 3) and 4) for number of times
        """
        if not skip_optional_stage('main_load'):
            cs_thread_pool = self.run_stress_thread(stress_cmd=self.get_stress_cmd())

        start = datetime.datetime.now()
        duration = 0
        wait_interval = 60
        max_random_cnt = 3
        self.log.info('Starting to add/remove nodes: %s', str(start))

        while duration <= self._duration:
            time.sleep(wait_interval)
            node_cnt = len(self.db_cluster.nodes)
            max_add_cnt = max_random_cnt if max_random_cnt <= self._cluster_target_size - node_cnt else\
                self._cluster_target_size - node_cnt
            if max_add_cnt >= 1:
                add_cnt = random.randint(1, max_add_cnt)
                self.log.info('Add %s nodes to cluster', add_cnt)
                for _ in range(add_cnt):
                    self.add_nodes(1)
                time.sleep(wait_interval)
            rm_cnt = random.randint(1, max_random_cnt) if len(self.db_cluster.nodes) >= 10 else 0
            if rm_cnt > 0:
                self.log.info('Remove %s nodes from cluster', rm_cnt)
                for _ in range(rm_cnt):
                    decommision_nemesis = nemesis.DecommissionMonkey(
                        tester_obj=self, termination_event=self.db_cluster.nemesis_termination_event)
                    decommision_nemesis.disrupt_nodetool_decommission(add_node=False)
            duration = (datetime.datetime.now() - start).seconds / 60  # current duration in minutes
            self.log.info('Count of nodes in cluster: %s', len(self.db_cluster.nodes))

        end = datetime.datetime.now()
        self.log.info('Add/remove nodes finished: %s', str(end))
        self.log.info('Duration: %s', str(end - start))

        # Run 2 more minutes before stop c-s
        time.sleep(2 * 60)

        if not skip_optional_stage('main_load'):
            # Kill c-s when decommission is done
            self.kill_stress_thread()

            self.verify_stress_thread(cs_thread_pool=cs_thread_pool)
            self.run_stress(stress_cmd=self.get_stress_cmd('read', 10))
