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

# TODO: this test seem to totally unused

import logging

from sdcm.tester import ClusterTester
from sdcm.tester import teardown_on_exception


class HugeClusterTest(ClusterTester):

    """
    Test a huge Scylla cluster
    """

    @teardown_on_exception
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        self.monitors = None

        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)

        loader_info = {'n_nodes': 1, 'device_mappings': None, 'type': None}
        db_info = {'n_nodes': 40, 'device_mappings': None, 'type': None}
        monitor_info = {'n_nodes': 1, 'device_mappings': None, 'type': None}
        self.init_resources(loader_info=loader_info, db_info=db_info,
                            monitor_info=monitor_info)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        nodes_monitored = [node.private_ip_address for node in self.db_cluster.nodes]
        nodes_monitored += [node.private_ip_address for node in self.loaders.nodes]
        self.monitors.wait_for_init(targets=nodes_monitored)
        self.stress_thread = None

    def test_huge(self):
        """
        Test a huge Scylla cluster
        """
        self.run_stress(stress_cmd=self.params.get('stress_cmd'), duration=20)
