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


class SimpleRegressionTest(ClusterTester):

    """
    Test Scylla performance regression with cassandra-stress.

    :avocado: enable
    """

    def test_simple_regression(self):
        """
        Test steps:

        1. Run a write workload
        2. Run a read workload (after the write - cache will contain some data)
        3. Restart node, run a read workload (cache will be empty)
        4. Run a mixed read write workload
        """
        # run a write workload
        stress_queue = self.run_stress_thread(
                         duration=self.params.get('cassandra_stress_duration'),
                         population_size=self.params.get(
                                           'cassandra_stress_population_size'),
                         threads=self.params.get('cassandra_stress_threads'),
                         mode='write')
        write_results = self.get_stress_results(queue=stress_queue)

        # run a read workload
        stress_queue = self.run_stress_thread(
                         duration=self.params.get('cassandra_stress_duration'),
                         population_size=self.params.get(
                                           'cassandra_stress_population_size'),
                         threads=self.params.get('cassandra_stress_threads'),
                         mode='read')
        read_results = self.get_stress_results(queue=stress_queue)

        # restart all the nodes
        for loader in self.db_cluster.nodes:
            loader.restart()

        # run a mixed read write workload
        stress_queue = self.run_stress_thread(
                         duration=self.params.get('cassandra_stress_duration'),
                         population_size=self.params.get(
                                           'cassandra_stress_population_size'),
                         threads=self.params.get('cassandra_stress_threads'),
                         mode='mixed')
        mixed_results = self.get_stress_results(queue=stress_queue)


if __name__ == '__main__':
    main()
