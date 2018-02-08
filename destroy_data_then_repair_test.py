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

from avocado import main
from sdcm.tester import ClusterTester
from sdcm import nemesis


class CorruptThenRepair(ClusterTester):
    """
    :avocado: enable
    """
    def test_destroy_data_then_repair_test_nodes(self):

        # run a prepare write workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_write_cmd'),
                                              stress_num=1,
                                              keyspace_num=1)
        #wait for prepare write to finish
        self.db_cluster.wait_total_space_used_per_node()

        # run rebuild
        nm = nemesis.CorruptThenRepairMonkey(self.db_cluster, self.loaders, self.monitors, None)
        nm.disrupt()

        #verify stress
        self.verify_stress_thread(queue=stress_queue)


if __name__ == '__main__':
    main()
