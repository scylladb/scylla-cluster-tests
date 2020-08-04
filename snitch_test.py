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
# Copyright (c) 2018 ScyllaDB

from sdcm.tester import ClusterTester


class SnitchTest(ClusterTester):
    """
    Endpoint Snitch Test
    """

    def test_google_cloud_snitch(self):
        """
        Verify the snitch setup, and check if system.peers is empty.
        """
        result = self.db_cluster.nodes[0].run_nodetool("describecluster")
        assert 'GoogleCloudSnitch' in result.stdout, "Cluster doesn't use GoogleCloudSnitch"

        with self.db_cluster.cql_connection_patient_exclusive(self.db_cluster.nodes[0]) as session:
            # pylint: disable=no-member
            result = list(session.execute('select * from system.peers'))
            self.log.debug(result)
            assert result, 'ERROR: system.peers should be not empty.'

        self.log.info("PASS: system.peers isn't empty as expected")

        result = self.db_cluster.nodes[0].check_node_health()
        assert 'Datacenter: us-east1scylla_node_east' in result.stdout
        assert 'Datacenter: us-west1scylla_node_west' in result.stdout

        stress_cmd = self.params.get('stress_cmd')
        cs_thread_pool = self.run_stress_thread(stress_cmd=stress_cmd)
        self.verify_stress_thread(cs_thread_pool=cs_thread_pool)
