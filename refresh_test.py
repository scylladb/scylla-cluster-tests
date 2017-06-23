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

import time

from avocado import main
from sdcm.tester import ClusterTester
from sdcm.nemesis import RefreshMonkey
from sdcm.nemesis import RefreshBigMonkey


class RefreshTest(ClusterTester):
    """
    Nodetool refresh after uploading lot of data to a cluster with running load in the background.
    :avocado: enable
    """
    def test_refresh_small_node(self):
        self.db_cluster.add_nemesis(nemesis=RefreshMonkey,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors)

        # run a write workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        time.sleep(30)
        self.db_cluster.start_nemesis()
        self.db_cluster.stop_nemesis(timeout=None)

        self.get_stress_results(queue=stress_queue, stress_num=2, keyspace_num=1)

    def test_refresh_big_node(self):
        self.db_cluster.add_nemesis(nemesis=RefreshBigMonkey,
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors)

        # run a write workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        time.sleep(30)
        self.db_cluster.start_nemesis()
        self.db_cluster.stop_nemesis(timeout=None)

        self.get_stress_results(queue=stress_queue, stress_num=2, keyspace_num=1)


if __name__ == '__main__':
    main()
