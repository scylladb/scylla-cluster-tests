#!/usr/bin/env python

import threading

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
        self.init_resources(n_db_nodes=3, n_loader_nodes=1)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

    def test_grow_one_to_30(self):
        """
        1) Start a 1 node cluster
        2) Start cassandra-stress on the loader node
        3) Add a new node
        4) Keep repeating 3) until we get to the target number of 30 nodes
        """
        # Let's estimate 15 minutes of c-s for each new node
        # They initialize in ~ 5 minutes average
        duration = 15 * 27
        self.stress_thread = threading.Thread(target=self.run_stress,
                                              kwargs={'duration': duration})
        self.stress_thread.start()
        target_size = 30
        while len(self.db_cluster.nodes) < target_size:
            new_nodes = self.db_cluster.add_nodes(count=1)
            self.db_cluster.wait_for_init(node_list=new_nodes)
        self.stress_thread.join()


if __name__ == '__main__':
    main()
