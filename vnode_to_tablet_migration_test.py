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
from collections.abc import Callable
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
    MIGRATING_TO_VNODES = "migrating to vnodes"
    USES_TABLETS = "uses tablets"


class TableConvergenceStatus(StrEnum):
    CONVERGING = "converging"
    CONVERGED = "converged"


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


def get_pow2_convergence_status(node, keyspace: str) -> dict[str, TableConvergenceStatus]:
    """
    Run 'nodetool migrate-to-tablets status <keyspace> --with-tablet-status' and return
    per-table pow2 convergence status.

    Only valid after finalization (keyspace Status: tablets). Example output:

        Keyspace: ks
        Status: tablets

        Tablet info:
          Pow2 tablet layout convergence: in progress
          Tables converging: 2/3

        Table   Status       Tablets   Target
        t1      converging   2301      2048
        t2      converging   2176      2048
        t3      converged    2048      -

    Returns: dict of table_name -> TableConvergenceStatus.
    """
    res = node.run_nodetool(f"migrate-to-tablets status {keyspace} --with-tablet-status")
    table_statuses: dict[str, TableConvergenceStatus] = {}
    pattern = re.compile(r"^(?P<table>\S+)\s+(?P<status>converging|converged)\s+\d+\s+\S+$")
    for line in res.stdout.splitlines():
        match = pattern.match(line.strip())
        if match:
            table_statuses[match.group("table")] = TableConvergenceStatus(match.group("status"))
    return table_statuses


class VnodeToTabletMigrationTest(LongevityTest):
    """Test vnode to tablet migration scenarios."""

    def restart_node_after_migration(self, node) -> None:
        """Restart a node after a migrate-to-tablets upgrade or downgrade, waiting for resharding.

        After the upgrade (or rollback) the node reshards all data on startup.  For large
        datasets (hundreds of GB) this can take 15+ minutes.  This method monitors the
        scylla log for resharding start/finish so slow reshards are visible in test output.

        The same resharding log patterns fire for both forward (vnodes→tablets) and
        reverse (tablets→vnodes) resharding, so this method is safe to use in both
        directions.

        Args:
            node: The DB node to restart.
        """
        reshard_timeout = 3600
        self.log.info(
            "Restarting node %s after migration state change (reshard_timeout=%ds)", node.name, reshard_timeout
        )
        node.run_nodetool("drain")
        node.stop_scylla(verify_down=True)

        with wait_for_log_lines(
            node=node,
            start_line_patterns=[DB_LOG_PATTERN_RESHARDING_START],
            end_line_patterns=[DB_LOG_PATTERN_RESHARDING_FINISH],
            start_timeout=600,
            end_timeout=reshard_timeout,
            error_msg_ctx=f"Resharding on {node.name} after migration state change",
        ):
            node.start_scylla(verify_up=False, timeout=reshard_timeout * 2)

        node.wait_db_up(timeout=reshard_timeout)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=[node], timeout=reshard_timeout)
        node.wait_node_fully_start(timeout=reshard_timeout)

    def _start_migration(self, keyspaces: list[str]) -> None:
        """Start tablet migration for all given keyspaces and verify all nodes use vnodes.

        Runs 'migrate-to-tablets start <ks>' for each keyspace from the coordinator
        (data_nodes[0]) and asserts that every node reports USES_VNODES immediately
        after, confirming the migration is in the preparation phase.

        Args:
            keyspaces: User keyspaces to migrate.
        """
        coordinator_node = self.db_cluster.data_nodes[0]
        InfoEvent(message=f"Starting tablet migration for keyspaces: {keyspaces}").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets start {ks}")
            migration_status = get_nodetool_migrate_to_tablets_status(coordinator_node, ks)
            for node in self.db_cluster.data_nodes:
                assert migration_status[node.host_id] == NodeMigrationStatus.USES_VNODES, (
                    f"[ks={ks}] Expected {NodeMigrationStatus.USES_VNODES!r} for {node.host_id}, "
                    f"got {migration_status[node.host_id]!r}"
                )

    def _upgrade_node_to_tablets(self, node, keyspaces: list[str]) -> None:
        """Upgrade a single node from vnodes to tablets storage.

        Waits for any running nemesis to complete, marks the node for upgrade,
        verifies it enters the 'migrating to tablets' state, drains and restarts
        for resharding, then waits for the node to report 'uses tablets' for all
        keyspaces.  Sleeps 300 s afterward to let the cluster stabilize.

        Args:
            node: The DB node to upgrade.
            keyspaces: User keyspaces being migrated.
        """
        wait_for(
            func=lambda n=node: not n.running_nemesis,
            step=30,
            timeout=600,
            text=f"Waiting for nemesis on {node.name} to finish before upgrade",
        )
        with self.nemesis_allocator.nodes_running_nemesis(node, "vnode_to_tablet_migration"):
            self.log.info("Upgrading node %s (ip=%s) to tablets", node.name, node.ip_address)
            node.run_nodetool("migrate-to-tablets upgrade")
            self.log.info("Verifying node %s status changed to 'migrating to tablets'", node.name)
            for ks in keyspaces:
                migration_status = get_nodetool_migrate_to_tablets_status(node, ks)
                assert migration_status[node.host_id] == NodeMigrationStatus.MIGRATING, (
                    f"[ks={ks}] Expected {NodeMigrationStatus.MIGRATING!r} for {node.host_id}, "
                    f"got {migration_status[node.host_id]!r}"
                )
            self.restart_node_after_migration(node)
            self.log.info("Node %s is back up and normal after restart", node.name)

        self.log.info("Waiting for node %s status to reach 'uses tablets' for all keyspaces", node.name)
        for ks in keyspaces:
            wait_for(
                func=lambda n=node, k=ks: get_nodetool_migrate_to_tablets_status(n, k)[n.host_id]
                == NodeMigrationStatus.USES_TABLETS,
                step=60,
                timeout=3600,
                text=f"Waiting for {node.name} to reach {NodeMigrationStatus.USES_TABLETS!r} for ks={ks}",
                throw_exc=True,
            )
        self.log.info("Waiting 300 seconds for cluster to stabilize after upgrading node %s", node.name)
        time.sleep(300)

    def _rollback_node_to_vnodes(self, node, keyspaces: list[str]) -> None:
        """Roll back a single node from tablets back to vnodes storage.

        Sends the downgrade command.  If any keyspace puts the node in
        'migrating to vnodes' state, drains and restarts to complete the reverse
        resharding (the same log patterns fire as for the forward direction).
        Verifies the node reports 'uses vnodes' for all keyspaces and passes a
        health check before returning.

        Args:
            node: The DB node to roll back.
            keyspaces: User keyspaces being migrated.
        """
        self.log.info("Rolling back node %s (ip=%s) to vnodes", node.name, node.ip_address)
        node.run_nodetool("migrate-to-tablets downgrade")

        # A node fully on tablets enters 'migrating to vnodes' and needs a restart.
        # A node mid-upgrade may skip directly to 'uses vnodes' (no restart needed).
        needs_restart = any(
            get_nodetool_migrate_to_tablets_status(node, ks).get(node.host_id)
            == NodeMigrationStatus.MIGRATING_TO_VNODES
            for ks in keyspaces
        )
        if needs_restart:
            self.log.info("Node %s is 'migrating to vnodes' — restarting for reverse resharding", node.name)
            self.restart_node_after_migration(node)

        self.log.info("Waiting for node %s status to reach 'uses vnodes' for all keyspaces", node.name)
        for ks in keyspaces:
            wait_for(
                func=lambda n=node, k=ks: get_nodetool_migrate_to_tablets_status(n, k)[n.host_id]
                == NodeMigrationStatus.USES_VNODES,
                step=60,
                timeout=3600,
                text=f"Waiting for {node.name} to reach {NodeMigrationStatus.USES_VNODES!r} for ks={ks}",
                throw_exc=True,
            )
        node.check_node_health()
        self.log.info("Node %s successfully rolled back to vnodes", node.name)

    def _finalize_migration(self, keyspaces: list[str]) -> None:
        """Finalize a forward tablet migration for all given keyspaces and verify cluster state.

        Sends 'migrate-to-tablets finalize' for each keyspace, waits for all tablet
        topology operations to quiesce on every node, asserts each keyspace appears in
        system.tablets, then waits for pow2 tablet layout convergence to complete.

        Use this only when all nodes have been upgraded to tablets.  For finalizing
        a rollback (cluster returning to vnodes), use _finalize_rollback() instead.

        Args:
            keyspaces: User keyspaces that were migrated.
        """
        coordinator_node = self.db_cluster.data_nodes[0]
        InfoEvent(message="Finalizing tablet migration").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets finalize {ks}")

        InfoEvent(message="Waiting for tablet migration to fully complete").publish()
        for node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(node, timeout=7200)

        InfoEvent(message="Verifying migrated keyspaces have tablets in system.tablets").publish()
        with self.db_cluster.cql_connection_patient(coordinator_node, connect_timeout=600) as session:
            result = session.execute("SELECT keyspace_name FROM system.tablets")
            tablet_keyspaces = {row.keyspace_name for row in result}
        self.log.info("Keyspaces with tablets: %s", tablet_keyspaces)
        for ks in keyspaces:
            assert ks in tablet_keyspaces, f"Keyspace {ks} not found in system.tablets after migration"

        InfoEvent(message="Waiting for pow2 tablet layout convergence to complete").publish()
        for ks in keyspaces:

            def _all_converged(k=ks):
                statuses = get_pow2_convergence_status(coordinator_node, k)
                if not statuses:
                    self.log.warning("No table convergence status found for keyspace %s yet", k)
                    return False
                not_converged = {t: s for t, s in statuses.items() if s != TableConvergenceStatus.CONVERGED}
                if not_converged:
                    self.log.info(
                        "Keyspace %s pow2 convergence in progress: %d/%d tables still converging: %s",
                        k,
                        len(not_converged),
                        len(statuses),
                        not_converged,
                    )
                    return False
                self.log.info("Keyspace %s pow2 convergence complete: all %d tables converged", k, len(statuses))
                return True

            wait_for(
                func=_all_converged,
                step=60,
                timeout=7200,
                text=f"Waiting for pow2 tablet layout convergence on keyspace {ks}",
                throw_exc=True,
            )

    def _finalize_rollback(self, keyspaces: list[str]) -> None:
        """Finalize a rollback for all given keyspaces, returning the cluster to vnodes.

        Required by the migration protocol after rolling back nodes: the finalize
        command clears migration state so the cluster is fully back on vnodes.

        Unlike _finalize_migration(), this method does NOT check system.tablets
        because the cluster is expected to be on vnodes after this call.

        Args:
            keyspaces: User keyspaces whose migration is being rolled back.
        """
        coordinator_node = self.db_cluster.data_nodes[0]
        InfoEvent(message="Finalizing rollback to clear migration state").publish()
        for ks in keyspaces:
            coordinator_node.run_nodetool(f"migrate-to-tablets finalize {ks}")

        InfoEvent(message="Waiting for rollback to fully complete").publish()
        for node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(node, timeout=3600)

        InfoEvent(message="Rollback finalized — cluster is back on vnodes").publish()

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
            stress_queue.append(self.run_stress_thread(stress_cmd=cmd, duration=30))
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    @latency_calculator_decorator(
        legend="During vnode-to-tablet migration",
        cycle_name="during_migration",
        workload_type="mixed",
    )
    def run_migration(self, migration_steps: Callable[[], None]):
        """Run stress concurrently with a migration phase and measure the performance impact.

        Starts the stress workload first so the cluster is under load throughout the
        entire migration window.  The latency_calculator_decorator captures the full
        window in HDR.

        The concurrent mixed CL=QUORUM stress also acts as a continuous
        data-integrity check: any corruption introduced by an upgrade/rollback
        cycle surfaces as a stress error caught by verify_stress_thread().

        Args:
            migration_steps: Zero-argument callable containing the ordered sequence of
                             migration building-block calls (_start_migration,
                             _upgrade_node_to_tablets, _rollback_node_to_vnodes,
                             _finalize_migration, _finalize_rollback, etc.).
        """
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(
            stress_queue, self.params.get("stress_cmd"), self.params.get("keyspace_num")
        )
        migration_steps()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    @latency_calculator_decorator(
        legend="Post-phase benchmark",
        cycle_name="post_migration",
        workload_type="mixed",
    )
    def run_post_migration_benchmark(self):
        """Run stress after the migration phase to compare with the pre-migration baseline.

        For tests that end on tablets this measures tablet performance.
        For tests that end on vnodes (full rollback) this verifies performance
        has returned to the vnodes baseline.

        The latency_calculator_decorator collects HDR histogram data for this time
        window and reports P90/P99 latencies and throughput to Argus.
        """
        stress_queue = []
        for cmd in self.params.get("stress_cmd"):
            stress_queue.append(self.run_stress_thread(stress_cmd=cmd, duration=30))
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        return stress_queue

    def _common_test_setup(self) -> None:
        """Shared setup for all vnode-to-tablet migration test methods"""
        InfoEvent(message="Filling database with data before migration").publish()
        self.run_pre_create_keyspace()
        self.run_pre_create_schema()
        self.run_prepare_write_cmd()

        if not self.params.get("prepare_write_cmd") or not self.params.get("nemesis_during_prepare"):
            self.db_cluster.start_nemesis()

    def test_vnode_to_tablet_migration(self):
        """Test migration with a mid-flight single-node rollback before finalization.

        1. Fill the database via prepare_write_cmd.
        2. Pre-migration benchmark: stress on vnodes to establish a baseline.
        3. Migration phase (concurrent mixed CL=QUORUM stress throughout):
               a. Start migration for all user keyspaces.
               b. Upgrade all nodes to tablets one at a time.
               c. Roll back node[0] to vnodes.
               d. Re-upgrade node[0] to tablets.
               e. Finalize migration (all nodes on tablets).
               f. Wait for pow2 tablet layout convergence.
        4. Post-phase benchmark: stress on tablets to compare with the baseline.

        Each phase (pre, during, post) is wrapped with latency_calculator_decorator
        which records HDR histograms and publishes P90/P99 latency and throughput
        tables to Argus for before/during/after comparison.
        """
        self._common_test_setup()
        keyspaces = self.db_cluster.get_test_keyspaces()
        data_nodes = self.db_cluster.data_nodes

        def migration_steps():
            self._start_migration(keyspaces)
            for node in data_nodes:
                self._upgrade_node_to_tablets(node, keyspaces)

            InfoEvent(message="Rolling back node[0] before finalize, then re-upgrading").publish()
            self._rollback_node_to_vnodes(data_nodes[0], keyspaces)
            self._upgrade_node_to_tablets(data_nodes[0], keyspaces)
            self._finalize_migration(keyspaces)

        self.run_pre_migration_benchmark()
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
        self.run_migration(migration_steps)
        self.run_post_migration_benchmark()

    def test_vnode_to_tablet_migration_full_rollback(self):
        """Test migration reversibility by upgrading all nodes then rolling them all back.

        1. Fill the database via prepare_write_cmd.
        2. Pre-migration benchmark: stress on vnodes to establish a baseline.
        3. Migration phase (concurrent mixed CL=QUORUM stress throughout):
               a. Start migration for all user keyspaces.
               b. Upgrade all nodes to tablets one at a time.
               c. Roll back all nodes to vnodes (last upgraded first).
               d. Finalize rollback — clears migration state, cluster returns to vnodes.
        4. Post-phase benchmark: stress on vnodes, verifying performance returned to baseline.

        Each phase (pre, during, post) is wrapped with latency_calculator_decorator
        which records HDR histograms and publishes P90/P99 latency and throughput
        tables to Argus for before/during/after comparison.
        """
        self._common_test_setup()
        keyspaces = self.db_cluster.get_test_keyspaces()
        data_nodes = self.db_cluster.data_nodes

        def migration_steps():
            self._start_migration(keyspaces)
            for node in data_nodes:
                self._upgrade_node_to_tablets(node, keyspaces)

            InfoEvent(message="All nodes upgraded — rolling back entire cluster to vnodes").publish()
            for node in reversed(data_nodes):
                self._rollback_node_to_vnodes(node, keyspaces)
            self._finalize_rollback(keyspaces)

        self.run_pre_migration_benchmark()
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
        self.run_migration(migration_steps)
        self.run_post_migration_benchmark()
