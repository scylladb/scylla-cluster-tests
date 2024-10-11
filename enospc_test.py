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
from sdcm.nemesis import EnospcAllNodesMonkey
from sdcm.utils.common import skip_optional_stage


class EnospcTest(ClusterTester):
    """
    Cost rest space to trigger ENOSPC error for scylla,
    and then release space to recover scylla service.
    """

    def test_enospc_nodes(self):
        self.db_cluster.add_nemesis(nemesis=EnospcAllNodesMonkey,
                                    tester_obj=self)

        if not skip_optional_stage('main_load'):
            # run a write workload
            stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                                  stress_num=2, keyspace_num=1)

        self.db_cluster.start_nemesis(interval=15)
        self.db_cluster.stop_nemesis(timeout=1000)

        if not skip_optional_stage('main_load'):
            self.get_stress_results(queue=stress_queue)
