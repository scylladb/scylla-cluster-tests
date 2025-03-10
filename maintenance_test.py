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

from sdcm.tester import ClusterTester
from sdcm.nemesis import DrainerMonkey
from sdcm.nemesis import CorruptThenRepairMonkey
from sdcm.nemesis import CorruptThenRebuildMonkey
from sdcm.utils.common import skip_optional_stage


class MaintainanceTest(ClusterTester):

    """
    Test a Scylla cluster maintenance operations.
    """

    def _base_procedure(self, nemesis_class):
        if not skip_optional_stage('main_load'):
            cs_thread_pool = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                                    duration=240)
        self.db_cluster.wait_total_space_used_per_node()
        self.db_cluster.add_nemesis(nemesis=nemesis_class,
                                    tester_obj=self)
        # Wait another 10 minutes
        time.sleep(10 * 60)
        self.db_cluster.start_nemesis(interval=10)
        time.sleep(180 * 60)
        if not skip_optional_stage('main_load'):
            # Kill c-s when done
            self.kill_stress_thread()
            self.verify_stress_thread(cs_thread_pool)

    def test_drain(self):
        """
        Drain a node an restart it.
        """
        self._base_procedure(DrainerMonkey)

    def test_repair(self):
        """
        Repair a node
        """
        self._base_procedure(CorruptThenRepairMonkey)

    def test_rebuild(self):
        """
        Rebuild all nodes
        """
        self._base_procedure(CorruptThenRebuildMonkey)
