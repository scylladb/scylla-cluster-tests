#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources


class HugeClusterTest(ClusterTester):

    """
    Test a huge Scylla cluster

    :avocado: enable
    """

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        self.init_resources(n_db_nodes=40, n_loader_nodes=1)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

    def test_huge(self):
        """
        Test a huge Scylla cluster
        """
        self.run_stress(duration=20)

if __name__ == '__main__':
    main()
