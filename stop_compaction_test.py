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

from sdcm.cluster import BaseNode
from sdcm.nemesis import StartStopMajorCompaction, StartStopScrubCompaction, StartStopCleanupCompaction, \
    StartStopValidationCompaction, StartStopUpgradeCompaction, StartStopReshapeCompaction
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.tester import ClusterTester
from sdcm.utils.compaction_ops import CompactionOps


class StopCompactionTest(ClusterTester):
    GREP_PATTERN = r'Compaction for keyspace1/standard1 was stopped due to: user request'

    def setUp(self):
        super().setUp()
        self.node1: BaseNode = self.db_cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(self.node1)
        self.populate_data_parallel(size_in_gb=10, blocking=True, replication_factor=1)
        self.disable_autocompaction_on_all_nodes()

    def disable_autocompaction_on_all_nodes(self):
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        compaction_ops.disable_autocompaction_on_ks_cf(node=self.node1)

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
            compaction_nemesis=StartStopMajorCompaction(tester_obj=self,
                                                        termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)

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
            compaction_nemesis=StartStopScrubCompaction(tester_obj=self,
                                                        termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)

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
            compaction_nemesis=StartStopCleanupCompaction(tester_obj=self,
                                                          termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)

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
                termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)

    def test_stop_upgrade_compaction(self):
        """
        Test that we can stop an upgrade compaction with <nodetool stop UPGRADE>.
        1. Flush the memtable data to sstables to make sure the data
        is persisted in the default format.
        2. Stop the Scylla server on the target node.
        3. Reset the configurations options for the cluster to include:
        "enable_sstables_mc_format" set to True and
        "enable_sstables_md_format" set to False.
        4. Restart the Scylla server.
        5. Trigger the upgrade sstables compaction.
        6. Stop the update sstables compaction mid-flight.
        7. Grep the logs for a line informing of the validation
        compaction being stopped due to a user request.
        8. Assert that we found the line we were grepping for.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopUpgradeCompaction(tester_obj=self,
                                                          termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)

    def test_stop_reshape_compaction(self):
        """
        Test that we can stop a reshape compaction with <nodetool stop RESHAPE>.
        1. Initialize cluster with 1 node.
        2. Running in parallel:
            2.1 Run refresh and restart. This will populate the db with data
            and flush to sstables using STCS as the compaction mode. Then it
            will alter the compaction mode to TWCS and refresh to trigger the
            reshape compaction.
            2.2 Watch for the first line indicating reshaping to show up in the
            logs and then issue the nodetool stop RESHAPE command.
        3. Grep the logs for a line informing of the compaction being stopped
        due to a user request.
        4. Assert that according to the logs scrubbing was stopped due to a
        user request.
        """
        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopReshapeCompaction(tester_obj=self,
                                                          termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=self.GREP_PATTERN)
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": self._reshape_scenario, "kwargs": {}}
    #     watch_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                   "kwargs": {"node": self.node1,
    #                              "watch_for": "Reshape",
    #                              "timeout": 300,
    #                              "stop_func": compaction_ops.stop_reshape_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #     self._stop_compaction_base_test_scenario(
    #         trigger_func=trigger_func,
    #         watch_func=watch_func,
    #         grep_pattern=grep_pattern)
    #
    # def _reshape_scenario(self):
    #     twcs = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1,
    #             'compaction_window_unit': 'MINUTES', 'max_threshold': 1, 'min_threshold': 1}
    #     self.node1.run_nodetool(sub_cmd="flush")
    #     self.wait_no_compactions_running()
    #     self._copy_files()
    #     cmd = f"ALTER TABLE standard1 WITH compaction={twcs}"
    #     self.node1.run_cqlsh(cmd=cmd, keyspace="keyspace1")
    #     self.node1.run_nodetool("refresh -- keyspace1 standard1")

    # def _copy_files(self, keyspace: str = "keyspace1"):
    #     LOGGER.info("Copying data files to ./staging and ./upload directories...")
    #     keyspace_dir = f'/var/lib/scylla/data/{keyspace}'
    #     cf_data_dir = self.node1.remoter.run(f"ls {keyspace_dir}").stdout.splitlines()[0]
    #     full_dir_path = f"{keyspace_dir}/{cf_data_dir}"
    #     upload_dir = f"{full_dir_path}/upload"
    #     staging_dir = f"{full_dir_path}/staging"
    #     cp_cmd_upload = f"cp -p {full_dir_path}/md-* {upload_dir}"
    #     cp_cmd_staging = f"cp -p {full_dir_path}/md-* {staging_dir}"
    #     self.node1.remoter.sudo(cp_cmd_staging)
    #     self.node1.remoter.sudo(cp_cmd_upload)
    #     LOGGER.info("Finished copying data files to ./staging and ./upload directories.")

    def _stop_compaction_base_test_scenario(self,
                                            compaction_nemesis,
                                            # trigger_func: dict[str, Any],
                                            # watch_func: dict[str, Any],
                                            grep_pattern: str,):
        compaction_nemesis.disrupt()
        found_grepped_expression = False
        with open(self.node1.system_log, "r") as logfile:
            pattern = re.compile(grep_pattern)
            for line in logfile.readlines():
                if pattern.findall(line):
                    found_grepped_expression = True

        self.assertTrue(found_grepped_expression, msg=f'Did not find the expected "{grep_pattern}" '
                                                      f'expression in the logs.')
