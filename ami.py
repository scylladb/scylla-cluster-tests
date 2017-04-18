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

import logging

from avocado import main

from sdcm.tester import ClusterTester
from sdcm.tester import clean_aws_resources

class AMIBaseTest(ClusterTester):

    """
    Test a Scylla cluster AMI

    :avocado: enable
    """

    INSTANCE_TYPE = "none"

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        self.init_resources(n_db_nodes=1,
                            n_loader_nodes=1,
                            dbs_type=self.INSTANCE_TYPE,
                            loaders_type=self.INSTANCE_TYPE)
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()
        self.stress_thread = None

    def test_ami(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(self.get_nemesis_class())
        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
        self.run_stress(duration=self.params.get('cassandra_stress_duration'))
