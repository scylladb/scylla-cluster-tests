#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources


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
        # We're starting the cluster with 3 nodes due to
        # replication factor settings we're using with cassandra-stress
        self._cluster_starting_size = 3
        self._cluster_target_size = None
        self.init_resources(n_db_nodes=self._cluster_starting_size,
                            n_loader_nodes=1)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

    def grow_cluster(self, cluster_target_size):
        # Let's estimate 10 minutes of c-s for each new node
        # They initialize in ~ 5 minutes average
        nodes_to_add = cluster_target_size - self._cluster_starting_size
        duration = 10 * nodes_to_add
        stress_queue = self.run_stress_thread(duration=duration)
        while len(self.db_cluster.nodes) < cluster_target_size:
            new_nodes = self.db_cluster.add_nodes(count=1)
            self.db_cluster.wait_for_init(node_list=new_nodes)
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
