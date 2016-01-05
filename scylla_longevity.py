#!/usr/bin/env python

from avocado import main

from sdcm.tester import ScyllaClusterTester
from sdcm.nemesis import ChaosMonkey


class LongevityTest(ScyllaClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """
    def test_twenty_minutes(self):
        """
        Run a very short test, as a config/code sanity check.
        """
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=5)
        self.run_stress(duration=20)


if __name__ == '__main__':
    main()
