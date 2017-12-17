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


class MultipleDcTest(ClusterTester):
    """

    :avocado: enable
    """

    def stop_scylla(self, node):
        node.wait_db_up()
        node.remoter.run('sudo systemctl stop scylla-server.service')
        node.remoter.run('sudo systemctl stop scylla-jmx.service')
        node.wait_db_down()

    def start_scylla(self, node):
        node.wait_db_down()
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.remoter.run('sudo systemctl start scylla-jmx.service')
        node.wait_db_up()

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
        self.stop_scylla(old_node)

        # stop new node
        self.stop_scylla(new_node)

        # restart the service
        self.start_scylla(old_node)
        self.start_scylla(new_node)

        results = self.get_stress_results(queue=stress_queue)

    def test_series_stop(self):

        # run a background workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        nodes_2nd_dc = [n for n in node_list if n.dc_idx == 1]
        for node in nodes_2nd_dc:
            self.stop_scylla(node)

        for node in nodes_2nd_dc:
            self.start_scylla(node)

        results = self.get_stress_results(queue=stress_queue)

    def test_parallel_stop(self):
        # run a background workload
        stress_queue = self.run_stress_thread(stress_cmd=self.params.get('stress_cmd'),
                                              stress_num=2,
                                              keyspace_num=1)
        nodes_2nd_dc = [n for n in node_list if n.dc_idx == 1]

        # stop the services of 2nd dc in parallel
        self.run_func_parallel(self.stop_scylla, node_list=nodes_2nd_dc)

        # restart the services in parallel
        self.run_func_parallel(self.start_scylla, node_list=nodes_2nd_dc)


if __name__ == '__main__':
    main()
