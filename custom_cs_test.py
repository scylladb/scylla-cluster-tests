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


from sdcm.tester import ClusterTester
from sdcm.utils.common import get_data_dir_path


class CustomCsTest(ClusterTester):
    """
    Run a custom c-s workload (yaml) on a cluster.
    """

    default_params = {"timeout": 650000}

    def test_write_mode(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        cs_custom_config = get_data_dir_path("cassandra-stress-custom.yaml")
        with open(cs_custom_config, encoding="utf-8") as cs_custom_config_file:
            self.log.info("Using custom cassandra-stress config:")
            self.log.info(cs_custom_config_file.read())
        for node in self.loaders.nodes:
            node.remoter.send_files(cs_custom_config, "/tmp/cassandra-stress-custom.yaml", verbose=True)
        ip = self.db_cluster.get_node_private_ips()[0]
        cs_command = (
            r"cassandra-stress user "
            r"profile=/tmp/cassandra-stress-custom.yaml "
            r"ops\(insert=1\) -node %s" % ip
        )
        self.run_stress(stress_cmd=cs_command)
