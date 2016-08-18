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

    default_params = {'timeout': 650000}

    def test_row_limit(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        stress_queue = self.run_stress_thread(row_limit=self.params.get('cassandra_row_limit'),
                                              population_size=self.params.get('cassandra_stress_population_size'),
                                              column_per_row=self.params.get('cassandra_column_per_row'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        stress_queue = self.run_stress_thread(duration=self.params.get('cassandra_stress_duration'),
                                              population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)

    def test_12_hours(self):
        """
        Run cassandra-stress on a cluster for 12 hours.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        stress_queue = self.run_stress(duration=60 * 12,
                                       population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)

    def test_1_day(self):
        """
        Run cassandra-stress on a cluster for 24 hours.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        stress_queue = self.run_stress(duration=60 * 24,
                                       population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)

    def test_1_week(self):
        """
        Run cassandra-stress on a cluster for 1 week.
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        stress_queue = self.run_stress(duration=60 * 24 * 7,
                                       population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.verify_stress_thread(queue=stress_queue)


if __name__ == '__main__':
    main()
