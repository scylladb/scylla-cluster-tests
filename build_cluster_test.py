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


class BuildClusterTest(ClusterTester):

    """
    Build a Scylla cluster with the appropriate parameters.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_build(self):
        """
        Build a Scylla cluster with params defined in data_dir/scylla.yaml
        """
        self.log.info('DB cluster is: %s', self.db_cluster)
        for node in self.db_cluster.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        self.log.info('Loader Nodes: %s', self.loaders)
        for node in self.loaders.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        self.log.info('Monitor Nodes: %s', self.monitors)
        for node in self.monitors.nodes:
            self.log.info(node.remoter.ssh_debug_cmd())
        if self.monitors.nodes:
            self.log.info('Prometheus Web UI: http://%s:9090',
                          self.monitors.nodes[0].public_ip_address)
            self.log.info('Grafana Web UI: http://%s:3000',
                          self.monitors.nodes[0].public_ip_address)

if __name__ == '__main__':
    main()
