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

import time

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.nemesis import DrainerMonkey
from sdcm.nemesis import CorruptThenRepairMonkey
from sdcm.nemesis import CorruptThenRebuildMonkey


class MaintainanceTest(ClusterTester):

    """
    Test a Scylla cluster maintenance operations.

    :avocado: enable
    """

    def test_drain(self):
        """
        Drain a node an restart it.
        """
        self.db_cluster.add_nemesis(DrainerMonkey)
        stress_queue = self.run_stress_thread(
            duration=240,
            population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=10)
        # Sleep for
        time.sleep(3 * 60 * 60)
        self.kill_stress_thread()
        self.verify_stress_thread(queue=stress_queue)

    def test_repair(self):
        """
        Repair a node
        """
        self.db_cluster.add_nemesis(CorruptThenRepairMonkey)
        stress_queue = self.run_stress_thread(
            duration=240,
            population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=10)
        time.sleep(1 * 60 * 60)
        self.kill_stress_thread()
        self.verify_stress_thread(queue=stress_queue)

    def test_rebuild(self):
        """
        Rebuild all nodes
        """
        self.db_cluster.add_nemesis(CorruptThenRebuildMonkey)
        stress_queue = self.run_stress_thread(
            duration=240,
            population_size=self.params.get('cassandra_stress_population_size'))
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.start_nemesis(interval=10)
        time.sleep(3 * 60 * 60)
        self.kill_stress_thread()
        self.verify_stress_thread(queue=stress_queue)


if __name__ == '__main__':
    main()
