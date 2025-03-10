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
# Copyright (c) 2017 ScyllaDB

from sdcm.tester import ClusterTester
from sdcm import nemesis


class CorruptThenRepair(ClusterTester):

    def test_destroy_data_then_repair_test_nodes(self):  # pylint: disable=invalid-name
        # populates 100GB
        write_queue = self.populate_data_parallel(100, blocking=False)

        self.db_cluster.wait_total_space_used_per_node(50 * (1024 ** 3))  # calculates 50gb in bytes

        # run rebuild
        current_nemesis = nemesis.CorruptThenRepairMonkey(
            tester_obj=self, termination_event=self.db_cluster.nemesis_termination_event)
        current_nemesis.disrupt()

        for stress in write_queue:
            self.verify_stress_thread(stress)
        self.populate_data_parallel(100, blocking=False, read=True)
