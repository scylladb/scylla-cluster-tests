#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.nemesis import DrainerMonkey


class MaintainanceTest(ClusterTester):

    """
    Test a Scylla cluster maintainance operations.

    :avocado: enable
    """

    def test_drain(self):
        """
        Drain a node an restart it.
        """
        self.db_cluster.add_nemesis(DrainerMonkey)
        # this nemesis is not periodic and will do
        # the stop and restart
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(duration=20)
