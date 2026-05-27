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
# Copyright (c) 2026 ScyllaDB

import re
import time
from enum import StrEnum

from longevity_test import LongevityTest
from sdcm.cluster import DB_LOG_PATTERN_RESHARDING_FINISH, DB_LOG_PATTERN_RESHARDING_START
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.tablets.common import wait_no_tablets_migration_running
from sdcm.wait import wait_for, wait_for_log_lines


class NodeMigrationStatus(StrEnum):
    USES_VNODES = "uses vnodes"
    MIGRATING = "migrating to tablets"
    USES_TABLETS = "uses tablets"


def get_nodetool_migrate_to_tablets_status(node, keyspace: str) -> dict[str, NodeMigrationStatus]:
    """
    Run 'nodetool migrate-to-tablets status <keyspace>' on the given node and return
    migration status for all nodes in the cluster.

    Example output:
        Keyspace: ks
        Status: migrating_to_tablets

        Nodes:
        Host ID                              Status
        dc1c5a03-8878-49da-a6f3-bd27d6a2d85d uses vnodes
        b1428349-2154-41c9-a5c1-dd33c71bd571 migrating to tablets
        017dd39a-3d06-4c8a-8ac4-379f9e595607 uses vnodes

    Returns: dict of host_id -> NodeMigrationStatus.
    """
    res = node.run_nodetool(f"migrate-to-tablets status {keyspace}")
    status: dict[str, NodeMigrationStatus] = {}
    pattern = re.compile(r"(?P<host_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+(?P<status>.+)")
    for line in res.stdout.splitlines():
        match = pattern.match(line.strip())
        if match:
            status[match.group("host_id")] = NodeMigrationStatus(match.group("status").strip())
    return status


class VnodeToTabletMigrationTest(LongevityTest):
    """Test vnode to tablet migration scenarios."""

    def restart_node_after_migration(self, node) -> None:
        """Restart a node after migrate-to-tablets upgrade, waiting for resharding to complete.

        After the upgrade the node reshards all data on startup. For large datasets
        (hundreds of GB) this can take 15+ minutes. This method monitors the scylla
        log for resharding start/finish and logs the elapsed duration so slow reshards
        are visible in test output.

        Args:
            node: The DB node to restart.
        """
        reshard_timeout = 3600
        self.log.info(
            "Restarting node %s after migrate-to-tablets upgrade (reshard_timeout=%ds)", node.name, reshard_timeout
        )
        node.run_nodetool("drain")
        node.stop_scylla(verify_down=True)

        with wait_for_log_lines(
            node=node,
            start_line_patterns=[DB_LOG_PATTERN_RESHARDING_START],
            end_line_patterns=[DB_LOG_PATTERN_RESHARDING_FINISH],
            start_timeout=600,
            end_timeout=reshard_timeout,
            error_msg_ctx=f"Resharding on {node.name} after tablet migration",
        ):
            node.start_scylla(verify_up=False, timeout=reshard_timeout * 2)

        node.wait_db_up(timeout=reshard_timeout)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=[node], timeout=reshard_timeout)
        node.wait_node_fully_start(timeout=reshard_timeout)

    def _perform_migration_steps(self) -> None:
        """Execute the full vnode-to-tablet migration procedure.

        1. Start migration for all user keyspaces.
        2. Rolling restart — for each node: run migrate-to-tablets upgrade, drain/stop/start.
        3. Finalize migration for all user keyspaces.
        4. Wait for all tablet topology operations to quiesce.
        5. Verify migrated keyspaces use tablets via system.tablets.
        """
        coordinator_node = self.db_cluster.data_nodes[0]
        keyspaces = self.db_cluster.get_test_keyspaces()
        if not keyspaces:
            raise AssertionError(
                "No user keyspaces found to migrate. Ensure prepare_write_cmd populates at least one keyspace."
            )
        self.log.info("User keyspaces to migrate to tablets: %s", keyspaces)

        InfoEvent(message=f"Starting tablet migration for keyspaces: {keyspaces}").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets start {ks}")
            migration_status = get_nodetool_migrate_to_tablets_status(coordinator_node, ks)
            for node in self.db_cluster.data_nodes:
                assert migration_status[node.host_id] == NodeMigrationStatus.USES_VNODES, (
                    f"[ks={ks}] Expected {NodeMigrationStatus.USES_VNODES!r} for {node.host_id}, got {migration_status[node.host_id]!r}"
                )

        InfoEvent(message="Rolling restart with migrate-to-tablets upgrade").publish()
        for node in self.db_cluster.data_nodes:
            wait_for(
                func=lambda n=node: not n.running_nemesis,
                step=30,
                timeout=600,
                text=f"Waiting for nemesis on {node.name} to finish before upgrade",
            )
            with self.nemesis_allocator.nodes_running_nemesis(node, "vnode_to_tablet_migration"):
                self.log.info("Preparing node %s (ip=%s) for tablet migration", node.name, node.ip_address)
                node.run_nodetool("migrate-to-tablets upgrade")
                self.log.info("Verify that the node status changed from vnodes to migrating to tablets")
                for ks in keyspaces:
                    migration_status = get_nodetool_migrate_to_tablets_status(node, ks)
                    assert migration_status[node.host_id] == NodeMigrationStatus.MIGRATING, (
                        f"[ks={ks}] Expected {NodeMigrationStatus.MIGRATING!r} for {node.host_id}, got {migration_status[node.host_id]!r}"
                    )
                self.restart_node_after_migration(node)
                self.log.info("Node %s is back up and normal after restart", node.name)

            self.log.info("Waiting for node %s status to change to 'uses tablets'", node.name)
            for ks in keyspaces:
                wait_for(
                    func=lambda n=node, k=ks: get_nodetool_migrate_to_tablets_status(n, k)[n.host_id]
                    == NodeMigrationStatus.USES_TABLETS,
                    step=60,
                    timeout=3600,
                    text=f"Waiting for node {node.name} to reach {NodeMigrationStatus.USES_TABLETS!r} for keyspace {ks}",
                    throw_exc=True,
                )

            self.log.info("Waiting 300 seconds for cluster to stabilize before migrating next node")
            time.sleep(300)

        InfoEvent(message="Finalizing tablet migration").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets finalize {ks}")

        InfoEvent(message="Waiting for tablet migration to fully complete").publish()
        for migration_node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(migration_node, timeout=7200)

        InfoEvent(message="Verifying migrated keyspaces have tablets in system.tablets").publish()
        with self.db_cluster.cql_connection_patient(coordinator_node, connect_timeout=600) as session:
            result = session.execute("SELECT keyspace_name FROM system.tablets")
            tablet_keyspaces = {row.keyspace_name for row in result}
        self.log.info("Keyspaces with tablets: %s", tablet_keyspaces)
        for ks in keyspaces:
            assert ks in tablet_keyspaces, f"Keyspace {ks} not found in system.tablets after migration"

    @latency_calculator_decorator(
        legend="Pre-migration baseline (vnodes)",
        cycle_name="pre_migration",
        workload_type="mixed",
    )
    def run_pre_migration_benchmark(self):
        """Run stress on vnodes to establish a performance baseline before migration.

        The latency_calculator_decorator collects HDR histogram data for this time window
        and reports P90/P99 latencies and throughput to Argus.
        """
        stress_queue = []
        for cmd in self.params.get("stress_cmd"):
            stress_queue.append(self.run_stress_thread(stress_cmd=cmd, duration=60))
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    @latency_calculator_decorator(
        legend="During vnode-to-tablet migration",
        cycle_name="during_migration",
        workload_type="mixed",
    )
    def run_migration(self):
        """Run stress concurrently with migration and measure the performance impact.

        Stress starts first so the cluster is under load throughout the migration.
        The latency_calculator_decorator captures the full migration window in HDR.
        """
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(
            stress_queue, self.params.get("stress_cmd"), self.params.get("keyspace_num")
        )
        self._perform_migration_steps()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    @latency_calculator_decorator(
        legend="Post-migration (tablets)",
        cycle_name="post_migration",
        workload_type="mixed",
    )
    def run_post_migration_benchmark(self):
        """Run stress on tablets after migration to compare with the pre-migration baseline.

        The latency_calculator_decorator collects HDR histogram data for this time window
        and reports P90/P99 latencies and throughput to Argus.
        """
        stress_queue = []
        for cmd in self.params.get("stress_cmd"):
            stress_queue.append(self.run_stress_thread(stress_cmd=cmd, duration=60))
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    def test_vnode_to_tablet_migration(self):
        """
        Test vnode to tablet migration procedure with performance measurement.

        1. Fill the database with data via prepare_write_cmd.
        2. Run pre-migration benchmark: stress on vnodes to establish a baseline.
        3. Run migration with concurrent stress:
               nodetool migrate-to-tablets start <ks>  (once per keyspace)
               rolling restart with migrate-to-tablets upgrade per node
               nodetool migrate-to-tablets finalize <ks>  (once per keyspace)
               wait for all tablet topology operations to quiesce
               verify migrated keyspaces use tablets via system.tablets
        4. Run post-migration benchmark: stress on tablets to compare with baseline.

        Each phase (pre, during, post) is wrapped with latency_calculator_decorator
        which records HDR histograms and publishes P90/P99 latency and throughput
        tables to Argus for before/during/after comparison.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        prepare_write_cmd = self.params.get("prepare_write_cmd")

        InfoEvent(message="Filling database with data before migration").publish()
        self.run_pre_create_keyspace()
        self.run_pre_create_schema()
        self.run_prepare_write_cmd()

        if not prepare_write_cmd or not self.params.get("nemesis_during_prepare"):
            self.db_cluster.start_nemesis()

        self.run_pre_migration_benchmark()
        self.run_migration()
        self.run_post_migration_benchmark()
