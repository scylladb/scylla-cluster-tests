#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.nemesis import StopStartMonkey
from sdcm.nemesis import DecommissionMonkey


class LongevityTest(ClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    def test_20_minutes(self):
        """
        Run cassandra-stress on a cluster for 20 min (smoke check).
        """
        self.db_cluster.add_nemesis(StopStartMonkey)
        self.db_cluster.start_nemesis(interval=5)
        self.run_stress(duration=20)

    def test_12_hours(self):
        """
        Run cassandra-stress on a cluster for 12 hours.
        """
        self.db_cluster.add_nemesis(StopStartMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 12)

    def test_1_day(self):
        """
        Run cassandra-stress on a cluster for 24 hours.
        """
        self.db_cluster.add_nemesis(StopStartMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24)

    def test_1_week(self):
        """
        Run cassandra-stress on a cluster for 1 week.
        """
        self.db_cluster.add_nemesis(StopStartMonkey)
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24 * 7)

if __name__ == '__main__':
    main()
