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

from sdcm.mgmt import HostStatus
from sdcm.utils.decorators import retrying, Retry
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn


class XcloudScaleOutTest(ManagerTestFunctionsMixIn):

    # The expected max time for scale out successful completion is 40 minutes
    @retrying(n=8, sleep_time=5, allowed_exceptions=(Retry,))
    def verify_cluster_scaled_out(self, nodes):
        self.log.info(f"Verify the number of nodes after scale out is 6 and all of them are 'UP'")
        assert len(nodes) == 6, "Initial number of nodes is not 3"
        for node in nodes.values():
            assert node.status == HostStatus.UP, "Not all nodes status is 'UP'"

    def test_xcloud_scale_out(self):
        """
        Test Xcloud cluster automatic scale out triggering upon reaching specified threshold
        """
        self.log.info("Starting Xcloud scale out test")

        initial_nodes = self.db_cluster.get_nodetool_status()

        self.log.info(f"Verify initial number of nodes -3 and all of them are 'UP'")
        assert len(initial_nodes) == 3, "Initial number of nodes is not 3"
        for node in initial_nodes.values():
            assert node.status == HostStatus.UP, "Not all nodes status is 'UP'"

        self.generate_load_and_wait_for_results()

        updated_nodes =self.db_cluster.get_nodetool_status()

        self.verify_cluster_scaled_out(updated_nodes)