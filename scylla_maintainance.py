#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.nemesis import DrainerMonkey
from sdcm.nemesis import CorruptThenRepairMonkey
from sdcm.nemesis import CorruptThenRebuildMonkey


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

    def test_repair(self):
        """
        Repair a node
        """
        self.db_cluster.add_nemesis(CorruptThenRepairMonkey)
        # this nemesis is not periodic and will do
        # the stop and restart
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(duration=20)

    def test_rebuild(self):
        """
        Rebuild all nodes
        """
        self.db_cluster.add_nemesis(CorruptThenRebuildMonkey)
        # this nemesis is not periodic and will do
        # the stop and restart
        self.db_cluster.start_nemesis(interval=10)
        self.run_stress(duration=20)


if __name__ == '__main__':
    main()
