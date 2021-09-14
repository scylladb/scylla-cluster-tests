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
# Copyright (c) 2021 ScyllaDB
import re

from sdcm.nemesis import StartStopMajorCompaction, StartStopScrubCompaction, StartStopCleanupCompaction, \
    StartStopValidationCompaction, StartStopUpgradeCompaction, StartStopReshapeCompaction
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.tester import ClusterTester
from sdcm.utils.compaction_ops import CompactionOps


class StopCompactionTest(ClusterTester):
    GREP_PATTERN = r'Compaction for keyspace1/standard1 was stopped due to: user request'

    def setUp(self):
        super().setUp()
        self.node = self.db_cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(self.node)
        self.populate_data_parallel(size_in_gb=10, blocking=True)
        self.disable_autocompaction_on_all_nodes()

    def disable_autocompaction_on_all_nodes(self):
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        compaction_ops.disable_autocompaction_on_ks_cf(node=self.node)

    def test_stop_major_compaction(self):
        """
        Test that we can stop a major compaction with <nodetool stop COMPACTION>.
        1. Use the StopStartMajorCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        COMPACTION>.
        2. Grep the logs for a line informing of the major compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopMajorCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def test_stop_scrub_compaction(self):
        """
        Test that we can stop a scurb compaction with <nodetool stop SCRUB>.
        1. Use the StopStartScrubCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        SCRUB>.
        2. Grep the logs for a line informing of the scrub compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopScrubCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def test_stop_cleanup_compaction(self):
        """
        Test that we can stop a cleanup compaction with <nodetool stop CLEANUP>.
        1. Use the StopStartCleanupCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        CLEANUP>.
        2. Grep the logs for a line informing of the cleanup compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopCleanupCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def test_stop_validation_compaction(self):
        """
        Test that we can stop a validation compaction with
        <nodetool stop VALIDATION>.
        1. Use the StopStartValidationCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        CLEANUP>.
        2. Grep the logs for a line informing of the validation
        compaction being stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopValidationCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def test_stop_upgrade_compaction(self):
        """
        Test that we can stop an upgrade compaction with <nodetool stop UPGRADE>.
        1. Use the StartStopUpgradeCompaction nemesis to trigger the sstables
        upgrade compaction.
        2. Stop the compaction mid-flight with <nodetool stop UPGRADE>.
        3. Grep the logs for a line informing of the upgrade compaction being
        stopped due to a user request.
        4. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopUpgradeCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def test_stop_reshape_compaction(self):
        """
        Test that we can stop a reshape compaction with <nodetool stop RESHAPE>.
        1. Use the StartStopReshapeCompaction nemesis to trigger the reshape
        compaction.
        2. Stop the compaction mid-flight with <nodetool stop RESHAPE>.
        3. Grep the logs for a line informing of the reshape compaction
        being stopped due to a user request.
        4. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopReshapeCompaction(
                tester_obj=self,
                termination_event=self.db_cluster.nemesis_termination_event))

    def _stop_compaction_base_test_scenario(self,
                                            compaction_nemesis):
        compaction_nemesis.disrupt()
        found_grepped_expression = False
        with open(self.node.system_log, "r") as logfile:
            pattern = re.compile(self.GREP_PATTERN)
            for line in logfile.readlines():
                if pattern.findall(line):
                    found_grepped_expression = True

        self.assertTrue(found_grepped_expression, msg=f'Did not find the expected "{self.GREP_PATTERN}" '
                                                      f'expression in the logs.')
