#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB


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
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.run_stress(duration=60 * 12)

    def test_1_day(self):
        """
        Run cassandra-stress on a cluster for 24 hours.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.run_stress(duration=60 * 24)

    def test_1_week(self):
        """
        Run cassandra-stress on a cluster for 1 week.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.run_stress(duration=60 * 24 * 7)

if __name__ == '__main__':
    main()
