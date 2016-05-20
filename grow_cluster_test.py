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
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        # We're starting the cluster with 3 nodes due to
        # replication factor settings we're using with cassandra-stress
        self._cluster_starting_size = 3
        self._cluster_target_size = None
        self.init_resources(n_db_nodes=self._cluster_starting_size,
                            n_loader_nodes=1)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

    def get_stress_cmd(self, duration=None, threads=None):
        """
        Get a cassandra stress cmd string suitable for grow cluster purposes.

        :param duration: Duration of stress (minutes).
        :param threads: Number of threads used by cassandra stress.
        :return: Cassandra stress string
        :rtype: basestring
        """
        ip = self.db_cluster.get_node_private_ips()[0]
        if duration is None:
            duration = self.params.get('cassandra_stress_duration')
        if threads is None:
            threads = self.params.get('cassandra_stress_threads')
        return ("cassandra-stress write cl=QUORUM duration=%sm "
                "-schema 'replication(factor=3)' -port jmx=6868 "
                "-mode cql3 native -rate threads=%s "
                "-pop seq=1..100000 -node %s" % (duration, threads, ip))

    def grow_cluster(self, cluster_target_size):
        # 60 minutes should be long enough for adding each node
        nodes_to_add = cluster_target_size - self._cluster_starting_size
        duration = 60 * nodes_to_add
        stress_queue = self.run_stress_thread(duration=duration)

        # Wait for cluster is filled with data
        # Set space_node_treshold in config file for the size
        self.db_cluster.wait_total_space_used_per_node()

        self.db_cluster.add_nemesis(GrowClusterMonkey)
        # Have c-s run for 2 + 3 minutes before we start to do decommission
        # TODO: I'm not sure why Asias put those sleeps here
        time.sleep(2 * 60)
        while len(self.db_cluster.nodes) < cluster_target_size:
            # Sleep 3 minutes before adding a new node, so we can see the tps
            # for each new cluster size
            time.sleep(3 * 60)
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

    def test_grow_3_to_4_while_filled(self):
        """
        Grow an already filled cluster.

        This is a regression test for scylla #1157.

        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) wait 10 minutes so that some data can fill the cluster
        3) Add a new node

        :see: https://github.com/scylladb/scylla/issues/1157
        """
        time.sleep(10 * 60)
        self.grow_cluster(cluster_target_size=4)
        time.sleep(10 * 60)

    def test_grow_3_to_30(self):
        """
        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 30 nodes
        """
        self.grow_cluster(cluster_target_size=30)

if __name__ == '__main__':
    main()
