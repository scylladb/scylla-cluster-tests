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
import functools
import logging
import re
from functools import partial
from typing import Callable

from sdcm.cluster import BaseNode
from sdcm.nemesis import StartStopMajorCompaction, StartStopScrubCompaction, StartStopCleanupCompaction, \
    StartStopValidationCompaction
from sdcm.rest.compaction_manager_client import CompactionManagerClient
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.sct_events.group_common_events import ignore_compaction_stopped_exceptions
from sdcm.send_email import FunctionalEmailReporter
from sdcm.tester import ClusterTester
from sdcm.utils.common import ParallelObject
from sdcm.utils.compaction_ops import CompactionOps, COMPACTION_TYPES
LOGGER = logging.getLogger(__name__)


def record_sub_test_result(func: Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        test_name = getattr(kwargs["compaction_func"], "__name__", "") or kwargs["compaction_func"].__class__.__name__
        try:
            func(*args, **kwargs)
            return {test_name: ["SUCCESS", []]}
        except Exception as exc:  # noqa: BLE001
            LOGGER.error(exc)
            return {test_name: ["FAILURE", [exc]]}
    return wrapper


class StopCompactionTest(ClusterTester):
    GREP_PATTERN = r"Compaction for ([\w/\d]+) was stopped"

    def setUp(self):
        super().setUp()
        self.node = self.db_cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(self.node)
        self.populate_data_parallel(size_in_gb=10, blocking=True)
        self.disable_autocompaction_on_all_nodes()
        self.test_statuses = {}
        self.email_reporter = FunctionalEmailReporter(email_recipients=self.params.get('email_recipients'),
                                                      logdir=self.logdir)

    def disable_autocompaction_on_all_nodes(self):
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        compaction_ops.disable_autocompaction_on_ks_cf(node=self.node)

    def test_stop_compaction(self):

        # skip due to missing support
        # to be enabled when https://github.com/scylladb/scylla/issues/10676 is fixed
        # with self.subTest("Stop upgrade compaction test"):
        #     self.stop_upgrade_compaction()

        with self.subTest("Stop major compaction test"):
            self.stop_major_compaction()

        with self.subTest("Stop scrub compaction test"):
            self.stop_scrub_compaction()

        with self.subTest("Stop cleanup compaction test"):
            self.stop_cleanup_compaction()

        with self.subTest("Stop validation compaction test"):
            self.stop_validation_compaction()

        with self.subTest("Stop reshape compaction test"):
            self.stop_reshape_compaction()

        with self.subTest("Stop reshape on boot compaction test"):
            self.stop_reshape_compaction(reshape_on_boot=True)

    def test_stop_compaction_ks_cf(self):
        with ignore_compaction_stopped_exceptions():
            with self.subTest("Stop SCRUB compaction on c-s keyspace and cf"):
                compaction_ops = CompactionOps(cluster=self.db_cluster, node=self.node)
                self.test_statuses.update(
                    self._stop_compaction_on_ks_cf_base_scenario(
                        compaction_func=compaction_ops.trigger_scrub_compaction,
                        watcher_expression="Scrubbing in abort mode",
                        compaction_type=COMPACTION_TYPES.SCRUB
                    )
                )

            with self.subTest("Stop CLEANUP compaction on c-s keyspace and cf"):
                compaction_ops = CompactionOps(cluster=self.db_cluster, node=self.node)
                self.test_statuses.update(
                    self._stop_compaction_on_ks_cf_base_scenario(
                        compaction_func=compaction_ops.trigger_cleanup_compaction,
                        watcher_expression="Cleaning",
                        compaction_type=COMPACTION_TYPES.CLEANUP
                    )
                )

            with self.subTest("Stop VALIDATE compaction on c-s keyspace and cf"):
                compaction_ops = CompactionOps(cluster=self.db_cluster, node=self.node)
                self.test_statuses.update(
                    self._stop_compaction_on_ks_cf_base_scenario(
                        compaction_func=compaction_ops.trigger_validation_compaction,
                        watcher_expression="Scrubbing in validate mode",
                        compaction_type=COMPACTION_TYPES.SCRUB
                    )
                )

    def stop_major_compaction(self):
        """
        Test that we can stop a major compaction with <nodetool stop COMPACTION>.
        1. Use the StopStartMajorCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        COMPACTION>.
        2. Grep the logs for a line informing of the major compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self.test_statuses.update(
            self._stop_compaction_base_test_scenario(
                compaction_func=StartStopMajorCompaction(
                    tester_obj=self,
                    termination_event=self.db_cluster.nemesis_termination_event
                )
            )
        )

    def stop_scrub_compaction(self):
        """
        Test that we can stop a scrub compaction with <nodetool stop SCRUB>.
        1. Use the StopStartScrubCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        SCRUB>.
        2. Grep the logs for a line informing of the scrub compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self.test_statuses.update(
            self._stop_compaction_base_test_scenario(
                compaction_func=StartStopScrubCompaction(
                    tester_obj=self,
                    termination_event=self.db_cluster.nemesis_termination_event
                )
            )
        )

    def stop_cleanup_compaction(self):
        """
        Test that we can stop a cleanup compaction with <nodetool stop CLEANUP>.
        1. Use the StopStartCleanupCompaction nemesis to trigger the
        major compaction and stop it mid-flight with <nodetool stop
        CLEANUP>.
        2. Grep the logs for a line informing of the cleanup compaction being
        stopped due to a user request.
        3. Assert that we found the line we were grepping for.
        """
        self.test_statuses.update(
            self._stop_compaction_base_test_scenario(
                compaction_func=StartStopCleanupCompaction(
                    tester_obj=self,
                    termination_event=self.db_cluster.nemesis_termination_event
                )
            )
        )

    def stop_validation_compaction(self):
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
        self.test_statuses.update(
            self._stop_compaction_base_test_scenario(
                compaction_func=StartStopValidationCompaction(
                    tester_obj=self,
                    termination_event=self.db_cluster.nemesis_termination_event
                )
            )
        )

    def stop_upgrade_compaction(self):
        """
        Test that we can stop an upgrade compaction with <nodetool stop UPGRADE>.

        Prerequisite:
        The initial setup in scylla.yaml must include 2 settings:
            enable_sstables_mc_format: true
            enable_sstables_md_format: false
        This is necessary for the test to be able to go from the legacy
        "mc" sstable format to the newer "md" format.

        1. Flush the data from memtables to sstables.
        2. Stop scylla on a given node.
        3. Update the scylla.yaml configuration to enable the mc
        sstable format.
        4. Restart scylla.
        5. Trigger the upgrade compaction using the API request.
        6. Stop the compaction mid-flight with <nodetool stop UPGRADE>.
        7. Grep the logs for a line informing of the upgrade compaction being
        stopped due to a user request.
        8. Assert that we found the line we were grepping for.
        """
        compaction_ops = CompactionOps(cluster=self.db_cluster, node=self.node)
        timeout = 300
        upgraded_configuration_options = {"enable_sstables_mc_format": False,
                                          "enable_sstables_md_format": True}
        trigger_func = partial(compaction_ops.trigger_upgrade_compaction)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=self.node,
                             mark=self.node.mark_log(),
                             watch_for="Upgrade keyspace1.standard1",
                             timeout=timeout,
                             stop_func=compaction_ops.stop_upgrade_compaction)

        def _upgrade_sstables_format(node: BaseNode):
            LOGGER.info("Upgrading sstables format...")
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.update(upgraded_configuration_options)

        try:
            compaction_ops.trigger_flush()
            self.node.stop_scylla()
            _upgrade_sstables_format(self.node)
            self.node.start_scylla()
            self.wait_no_compactions_running()
            ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()
            self._grep_log_and_assert(self.node)
            self.test_statuses.update({"StartStopUpgradeCompaction": ["SUCCESS", []]})
        except self.failureException as exc:
            self.test_statuses.update({"StartStopUpgradeCompaction": ["FAILURE", [exc]]})
            raise exc
        finally:
            self.node.running_nemesis = False

    def stop_reshape_compaction(self, reshape_on_boot: bool = False):
        """
        Test that we can stop a reshape compaction with <nodetool stop RESHAPE>.
        To trigger a reshape compaction, the current CompactionStrategy
        must not be aligned with the shape of the stored sstables. In
        this case we're setting the new strategy to
        TimeWindowCompactionStrategy with a very small time window.

        1. Flush the memtable data to sstables.
        2. Copy sstable files to the upload and staging dirs.
        3. Change the compaction strategy to TimeWindowCompactionStrategy.
        4a. if reshape_on_boot==False: Run <nodetool refresh> command to trigger the compaction.
        4b. if reshape_on_boot==True: restart scylla
        5. Stop the compaction mid-flight with <nodetool stop RESHAPE>.
        6. Grep the logs for a line informing of the reshape compaction
        being stopped due to a user request.
        7. Assert that we found the line we were grepping for.
        """
        node = self.node
        compaction_ops = CompactionOps(cluster=self.db_cluster, node=node)
        compaction_manager_client = CompactionManagerClient(self.node)
        if reshape_on_boot is True:
            # during reshape on boot, JMX port is not open and cannot use nodetool. Using scylla api directly.
            stop_reshape_func = partial(compaction_manager_client.stop_compaction, compaction_type="reshape")
        else:
            stop_reshape_func = compaction_ops.stop_reshape_compaction
        timeout = 900

        def _trigger_reshape(node: BaseNode, tester, keyspace: str = "keyspace1"):

            if reshape_on_boot is True:
                compaction_strategy = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1,
                                       'compaction_window_unit': 'MINUTES', 'max_threshold': 1, 'min_threshold': 1}
            else:
                compaction_strategy = {'class': 'SizeTieredCompactionStrategy'}
            compaction_ops.trigger_flush()
            tester.wait_no_compactions_running()
            LOGGER.info("Copying data files to ./staging and ./upload directories...")
            keyspace_dir = f'/var/lib/scylla/data/{keyspace}'
            cf_data_dir = node.remoter.run(f"ls {keyspace_dir}").stdout.splitlines()[0]
            full_dir_path = f"{keyspace_dir}/{cf_data_dir}"
            upload_dir = f"{full_dir_path}/upload"
            staging_dir = f"{full_dir_path}/staging"
            cp_cmd_upload = f"cp -p {full_dir_path}/m* {upload_dir}"
            cp_cmd_staging = f"cp -p {full_dir_path}/m* {staging_dir}"
            node.remoter.sudo(cp_cmd_staging)
            node.remoter.sudo(cp_cmd_upload)
            LOGGER.info("Finished copying data files to ./staging and ./upload directories.")
            cmd = f"ALTER TABLE standard1 WITH compaction={compaction_strategy}"
            node.run_cqlsh(cmd=cmd, keyspace="keyspace1")
            if reshape_on_boot is True:
                node.restart_scylla(verify_up_after=True)
            else:
                node.run_nodetool("refresh -- keyspace1 standard1")

        trigger_func = partial(_trigger_reshape,
                               node=node,
                               tester=self)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=node,
                             mark=node.mark_log(),
                             watch_for="Reshape keyspace1.standard1",
                             timeout=timeout,
                             stop_func=stop_reshape_func)
        try:
            ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()
            self.test_statuses.update({"StartStopReshapeCompaction": ["SUCCESS", []]})
        except self.failureException as exc:
            self.test_statuses.update({"StartStopReshapeCompaction": ["FAILURE", [exc]]})
            raise exc
        finally:
            self._grep_log_and_assert(node)

    @record_sub_test_result
    def _stop_compaction_base_test_scenario(self,
                                            compaction_func):
        try:
            self.wait_no_compactions_running()
            compaction_func.disrupt()
            node = compaction_func.target_node
            self._grep_log_and_assert(node)
        finally:
            node.running_nemesis = False

    def _grep_log_and_assert(self, node: BaseNode):
        found_grepped_expression = False
        with open(node.system_log, encoding="utf-8") as logfile:
            pattern = re.compile(self.GREP_PATTERN)
            for line in logfile.readlines():
                if pattern.search(line):
                    found_grepped_expression = True

        self.assertTrue(found_grepped_expression, msg=f'Did not find the expected "{self.GREP_PATTERN}" '
                                                      f'expression in the logs.')

    @record_sub_test_result
    def _stop_compaction_on_ks_cf_base_scenario(self,
                                                compaction_func: Callable,
                                                watcher_expression: str,
                                                compaction_type: str):
        c_s_keyspace = "keyspace1"
        c_s_cf = "standard1"
        LOGGER.info("Running a start / stop compaction test for compaction type: %s", compaction_type)
        self.wait_no_compactions_running()
        compaction_ops = CompactionOps(cluster=self.db_cluster, node=self.node)
        compaction_manager_client = CompactionManagerClient(self.node)
        stop_func = partial(compaction_manager_client.stop_keyspace_compaction,
                            keyspace=c_s_keyspace, compaction_type=compaction_type,
                            tables=[c_s_cf])
        timeout = 360
        trigger_func = partial(compaction_func)
        watch_func = partial(compaction_ops.stop_on_user_compaction_logged,
                             node=self.node,
                             watch_for=watcher_expression,
                             timeout=timeout,
                             stop_func=stop_func)

        ParallelObject(objects=[trigger_func, watch_func], timeout=timeout).call_objects()
        self._grep_log_and_assert(self.node)

    def get_email_data(self):
        self.log.info("Prepare data for email")
        email_data = {}

        try:
            email_data = self._get_common_email_data()
        except Exception as error:  # noqa: BLE001
            self.log.error("Error in gathering common email data: Error:\n%s", error)

        email_data.update({"test_statuses": self.test_statuses,
                           "scylla_ami_id": self.params.get("ami_id_db_scylla") or "-", })
        return email_data


class StopCompactionTestICS(ClusterTester):

    def setUp(self):
        super().setUp()
        self.node = self.db_cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(self.node)
        # insert ~10GB of data
        populate_data_cmd = "cassandra-stress write cl=ONE n=2097152" \
                            " -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) " \
                            "compaction(strategy=IncrementalCompactionStrategy)' -mode cql3 native " \
                            "-rate threads=80 -pop seq=1..2097152 -col 'n=FIXED(10) size=FIXED(512)' -log interval=5"
        prepare = self.run_stress_thread(stress_cmd=populate_data_cmd, round_robin=True)
        self.verify_stress_thread(cs_thread_pool=prepare)

    def test_stop_major_compaction(self):
        """Verifying data loss on stopping compaction by reading inserted"""
        self.wait_no_compactions_running()
        compaction_nemesis = StartStopMajorCompaction(
            tester_obj=self,
            termination_event=self.db_cluster.nemesis_termination_event)
        compaction_nemesis.disrupt()
        verify_cmd = "cassandra-stress read cl=ONE -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) " \
                     "compaction(strategy=IncrementalCompactionStrategy)' -mode cql3 native" \
                     " -rate threads=40 -pop seq=1..2097152 -col 'n=FIXED(10) size=FIXED(512)' -log interval=5"

        verify = self.run_stress_thread(stress_cmd=verify_cmd, round_robin=True)
        self.verify_stress_thread(cs_thread_pool=verify)
