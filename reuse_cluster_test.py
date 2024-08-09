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
# Copyright (c) 2024 ScyllaDB
import logging
import unittest

from sdcm.cluster import TestConfig
from sdcm.sct_events.events_processes import create_default_events_process_registry
from sdcm.tester import ClusterTester
from sdcm.utils.loader_utils import LoaderUtilsMixin


LOGGER = logging.getLogger(__name__)


class ReuseClusterTest(unittest.TestCase):
    """Test ability to reuse existing cluster in tests"""

    # Define reusable equivalent to ClusterTester that can be instantiated multiple times during the test
    ReusableCluster = type('ReusableCluster', (ClusterTester, LoaderUtilsMixin), {})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster = None
        self.ReusableCluster.events_processes_registry = (
            create_default_events_process_registry(log_dir=TestConfig().logdir()))

    def provision_cluster(self, reuse_cluster_id: str = None) -> None:
        """Provision a cluster from scratch or re-provision an existing one"""
        if reuse_cluster_id:
            TestConfig().reuse_cluster(True)
            TestConfig().set_test_id(reuse_cluster_id)
        self.cluster = self.ReusableCluster()
        self.cluster.setUp()

    def check_scylla(self) -> None:
        stress_cmd = 'cassandra-stress {mode} no-warmup cl=QUORUM -mode cql3 native -pop seq=1..10000 -rate threads=10'
        self.cluster.run_stress(stress_cmd.format(mode="write"), duration=1)
        self.cluster.run_stress(stress_cmd.format(mode="mixed 'ratio(write=1,read=2)'"), duration=1)

    def test_reuse_cluster(self):
        LOGGER.info("Initial cluster provisioning")
        self.provision_cluster()
        self.cluster.db_cluster.check_nodes_up_and_normal()
        self.cluster.db_cluster.check_cluster_health()

        test_id = self.cluster.test_config.test_id()
        initial_db_nodes = [i._instance for i in self.cluster.db_cluster.nodes]

        LOGGER.info("Pre-populate DB with data")
        self.cluster.run_prepare_write_cmd()

        LOGGER.info("Reuse the previously provisioned cluster")
        self.provision_cluster(reuse_cluster_id=test_id)
        self.cluster.db_cluster.check_nodes_up_and_normal()
        self.cluster.db_cluster.check_cluster_health()
        self.check_scylla()

        LOGGER.info("Verify that re-provisioned cluster consists of the initial DB nodes")
        self.assertEquals(set([i._instance for i in self.cluster.db_cluster.nodes]), set(initial_db_nodes),
                          "The re-provisioned cluster nodes do not match the initial DB nodes")

    def tearDown(self):
        if self.cluster:
            self.cluster.tearDown()
        super().tearDown()
