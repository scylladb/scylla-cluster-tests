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

# TODO: this test seem to be broken, hence disabling all ruff checks
# ruff: noqa: F821

from sdcm.tester import ClusterTester


class MultipleDcTest(ClusterTester):
    """
    Test with multiple DataCenters
    """

    def stop_scylla(self, node):
        node.stop_scylla(verify_up=True, verify_down=True)

    def start_scylla(self, node):
        node.start_scylla(verify_up=True, verify_down=True)

    def test_shutdown(self):
        """
        https://github.com/scylladb/scylla-enterprise/issues/228
        """

        # run a background workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        nodes = self.db_cluster.nodes

        # add a new node to existing cluster
        new_node = self.add_node(1)[0]

        # stop old node
        old_node = nodes[-1]
        old_node.stop_scylla(verify_up=True, verify_down=True)

        # stop new node
        new_node.stop_scylla(verify_up=True, verify_down=True)

        # restart the service
        old_node.start_scylla(verify_up=True, verify_down=True)
        new_node.start_scylla(verify_up=True, verify_down=True)

        self.get_stress_results(queue=stress_queue)

    def test_series_stop(self):

        # run a background workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        nodes_2nd_dc = [n for n in node_list if n.dc_idx == 1]
        for node in nodes_2nd_dc:
            node.stop_scylla(verify_up=True, verify_down=True)

        for node in nodes_2nd_dc:
            node.start_scylla(verify_up=True, verify_down=True)

        self.get_stress_results(queue=stress_queue)

    def test_parallel_stop(self):
        # run a background workload
        self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                               stress_num=2,
                               keyspace_num=1)
        nodes_2nd_dc = [n for n in node_list if n.dc_idx == 1]

        # stop the services of 2nd dc in parallel
        self.run_func_parallel(self.stop_scylla, node_list=nodes_2nd_dc)

        # restart the services in parallel
        self.run_func_parallel(self.start_scylla, node_list=nodes_2nd_dc)
