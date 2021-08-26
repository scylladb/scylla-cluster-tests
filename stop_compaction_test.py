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
# Copyright (c) 2021 ScyllaDB
import time

from sdcm.tester import ClusterTester


class StopCompactionTest(ClusterTester):
    def setUp(self):
        super().setUp()
        self.disable_autocompaction_on_all_nodes()
        self.populate_data_parallel(size_in_gb=100, blocking=False)

    def disable_autocompaction_on_all_nodes(self):
        for node in self.db_cluster.nodes:
            cmd = "disableautocompaction"
            node.run_nodetool(sub_cmd=cmd)

    def test_stop_major_compaction(self):  # pylint: disable=invalid-name
        # populates 100GB
        # write_queue = self.populate_data_parallel(100, blocking=False)
        #
        # self.db_cluster.wait_total_space_used_per_node(50 * (1024 ** 3))  # calculates 50gb in bytes
        #
        # # run rebuild
        # current_nemesis = nemesis.CorruptThenRepairMonkey(
        #     tester_obj=self, termination_event=self.db_cluster.nemesis_termination_event)
        # current_nemesis.disrupt()
        #
        # for stress in write_queue:
        #     self.verify_stress_thread(cs_thread_pool=stress)
        # self.populate_data_parallel(100, blocking=False, read=True)
        start_time = time.time()
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd')[0])
        self.verify_stress_thread(cs_thread_pool=stress_queue)
        self.verify_no_drops_and_errors(starting_from=start_time)

