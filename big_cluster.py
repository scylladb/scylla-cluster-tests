#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester


class HugeClusterTest(ClusterTester):

    """
    Test a huge Scylla cluster

    :avocado: enable
    """

    def test_huge(self):
        """
        Test a huge Scylla cluster
        """
        self.run_stress(duration=20)

if __name__ == '__main__':
    main()
