#!/usr/bin/env python

from avocado import main

from sdcm.tester import ScyllaClusterTester
from sdcm.nemesis import ChaosMonkey


class LongevityTest(ScyllaClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    def test_20_minutes(self):
        """
        Run a very short test, as a config/code sanity check.
        """
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=5)
        self.run_stress(duration=20)

    def test_add_node(self):
        new_nodes = self.db_cluster.add_nodes(count=1)
        self.db_cluster.wait_for_init(new_nodes)

    def test_12_hours(self):
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 12)

    def test_1_day(self):
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24)

    def test_1_week(self):
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24 * 7)

if __name__ == '__main__':
    main()
