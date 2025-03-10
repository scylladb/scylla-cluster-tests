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
from sdcm.utils.common import skip_optional_stage


class SnitchTest(ClusterTester):
    """
    Endpoint Snitch Test
    """

    def test_cloud_snitch(self):
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

        backend = self.params.get("cluster_backend")
        if backend == "gce":
            self.check_nodetool_status_output_gce()
        elif backend == "aws":
            self.check_nodetool_status_output_aws()

        stress_cmd = self.params.get('stress_cmd')
        if not skip_optional_stage('main_load'):
            cs_thread_pool = self.run_stress_thread(stress_cmd=stress_cmd)
            self.verify_stress_thread(cs_thread_pool)

    def check_nodetool_status_output_gce(self):
        result = self.db_cluster.nodes[0].get_nodes_status()
        all_datacenters = {result[node]["dc"] for node in result.keys()}
        assert 'us-east1us_east1' in all_datacenters
        assert 'us-west1us_west1' in all_datacenters

    def check_nodetool_status_output_aws(self):
        pass  # TODO: Add aws snitch test option

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = self._get_common_email_data()

        return email_data
