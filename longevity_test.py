#!/usr/bin/env python

from avocado import main

from sdcm.tester import ClusterTester


class LongevityTest(ClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.run_stress(duration=self.params.get('cassandra_stress_duration'))

    def test_12_hours(self):
        """
        Run cassandra-stress on a cluster for 12 hours.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 12)

    def test_1_day(self):
        """
        Run cassandra-stress on a cluster for 24 hours.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24)

    def test_1_week(self):
        """
        Run cassandra-stress on a cluster for 1 week.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=30)
        self.run_stress(duration=60 * 24 * 7)

if __name__ == '__main__':
    main()
