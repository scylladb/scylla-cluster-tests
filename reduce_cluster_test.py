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

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources
from sdcm.nemesis import DecommissionNoAddMonkey
import time


class ReduceClusterTest(ClusterTester):
    """
    Test scylla cluster reduction (removing nodes to shrink cluster from initial cluster size to target cluster size).

    :avocado: enable
    """

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        self.stress_thread = None

    def wait_for_init(self, cluster_starting_size, cluster_target_size):
        # We're starting the cluster with more than 3 nodes due to
        # replication factor settings we're using with cassandra-stress
        self._cluster_starting_size = cluster_starting_size
        self._cluster_target_size = cluster_target_size
        self.init_resources(n_db_nodes=self._cluster_starting_size, n_loader_nodes=1)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()

    def reduce_cluster(self, cluster_starting_size, cluster_target_size=3):
        self.wait_for_init(cluster_starting_size, cluster_target_size)
        nodes_to_remove = self._cluster_starting_size - self._cluster_target_size
        # FIXME: we should find a way to stop c-s when decommission is finished instead of estimating time to run
        duration = 8 * nodes_to_remove
        stress_queue = self.run_stress_thread(duration=duration)
        self.db_cluster.add_nemesis(DecommissionNoAddMonkey)
        # Have c-s run for 2 + 3 minutes before we start to do decommission
        time.sleep(2 * 60)
        while len(self.db_cluster.nodes) > cluster_target_size:
            # Sleep 3 minutes before decommission
            time.sleep(3 * 60)
            # Run DecommissionNoAddMonkey once to decommission one node a time
            self.db_cluster.start_nemesis(interval=10)
            self.db_cluster.stop_nemesis(timeout=None)

        self.verify_stress_thread(queue=stress_queue)

    def test_reduce_5_to_3(self):
        """
        Reduce cluster from 5 to 3

        1) Start a 5 node cluster
        2) Start cassandra-stress on the loader node
        3) Decommission a node
        4) Keep repeating 3) until we get to the target number of 3 nodes
        """
        self.reduce_cluster(cluster_starting_size=5, cluster_target_size=3)

    def test_reduce_10_to_3(self):
        """
        Reduce cluster from 10 to 3

        1) Start a 10 node cluster
        2) Start cassandra-stress on the loader node
        3) Decommission a node
        4) Keep repeating 3) until we get to the target number of 3 nodes
        """
        self.reduce_cluster(cluster_starting_size=10, cluster_target_size=3)

    def test_reduce_30_to_3(self):
        """
        Reduce cluster from 10 to 3

        1) Start a 10 node cluster
        2) Start cassandra-stress on the loader node
        3) Decommission a  node
        4) Keep repeating 3) until we get to the target number of 3 nodes
        """
        self.reduce_cluster(cluster_starting_size=30, cluster_target_size=3)


if __name__ == '__main__':
    main()
