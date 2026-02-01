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

from collections import defaultdict

from longevity_test import LongevityTest
from sdcm.sct_events.system import InfoEvent
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.aws_utils import get_arch_from_instance_type
from sdcm.utils.cluster_tools import group_nodes_by_dc_idx
from sdcm.utils.features import is_tablets_feature_enabled
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.replication_strategy_utils import DataCenterTopologyRfControl


class PlatformMigrationTest(LongevityTest):
    """
    Tests for platform migration using add-and-replace node cycling.

    Migrates cluster from source platform (e.g., x86) to target platform (e.g., ARM)
    while maintaining continuous workload.
    """

    def setUp(self):
        super().setUp()
        self.source_instance_type = self.params.get("instance_type_db")
        self.target_instance_type = self.params.get("instance_type_db_target")
        self.target_images = self.params.target_db_image_ids

        if not self.target_instance_type:
            raise ValueError("instance_type_db_target is required for platform migration test")

        region = self.params.region_names[0]
        source_arch = get_arch_from_instance_type(self.source_instance_type, region)
        target_arch = get_arch_from_instance_type(self.target_instance_type, region)
        self.log.info(
            "Platform migration test: %s (%s) -> %s (%s)",
            self.source_instance_type,
            source_arch,
            self.target_instance_type,
            target_arch,
        )

    def _get_target_image_for_dc(self, dc_idx: int) -> str:
        """Get target image for specific DC index"""
        if not self.target_images:
            return None
        if len(self.target_images) == 1:
            return self.target_images[0]
        if dc_idx >= len(self.target_images):
            return self.target_images[0]
        return self.target_images[dc_idx]

    def get_nodes_by_instance_type(self, instance_type: str) -> list:
        """Get all nodes with specific instance type."""
        return [n for n in self.db_cluster.nodes if n._instance_type == instance_type]

    def add_nodes_parallel(self) -> list:
        """Add all target nodes simultaneously across all DCs"""
        source_nodes = self.get_nodes_by_instance_type(self.source_instance_type)
        if not source_nodes:
            raise ValueError(f"No nodes found with source instance type: {self.source_instance_type}")

        nodes_by_dc = group_nodes_by_dc_idx(source_nodes)
        all_added = []
        for dc_idx, dc_nodes in nodes_by_dc.items():
            image_id = self._get_target_image_for_dc(dc_idx)
            # group by rack to preserve rack topology
            nodes_by_rack = defaultdict(list)
            for node in dc_nodes:
                nodes_by_rack[node.rack].append(node)
            for rack, rack_nodes in sorted(nodes_by_rack.items()):
                self.log.info(
                    "Adding %d target platform nodes to DC %s rack %s (image: %s)",
                    len(rack_nodes),
                    dc_idx,
                    rack,
                    image_id,
                )
                all_added.extend(
                    self.db_cluster.add_nodes(
                        count=len(rack_nodes),
                        dc_idx=dc_idx,
                        rack=rack,
                        enable_auto_bootstrap=True,
                        instance_type=self.target_instance_type,
                        image_id=image_id,
                    )
                )

        self.log.info("Waiting for %d new nodes to initialize...", len(all_added))
        self._initialize_and_start_nodes(all_added)
        self.monitors.reconfigure_scylla_monitoring()

        self.log.info("Added %d target platform nodes: %s", len(all_added), [n.name for n in all_added])
        return all_added

    def migrate_nodes_batch_by_rack(self) -> list:
        """Migrate rack-by-rack: add new nodes for rack, decommission old ones, next rack"""
        source_nodes = self.get_nodes_by_instance_type(self.source_instance_type)
        if not source_nodes:
            raise ValueError(f"No nodes found with source instance type: {self.source_instance_type}")

        nodes_by_rack = defaultdict(list)
        for node in source_nodes:
            nodes_by_rack[node.rack].append(node)

        all_new_nodes = []
        for rack, rack_nodes in sorted(nodes_by_rack.items()):
            self.log.info("Migrating rack %s (%d nodes)", rack, len(rack_nodes))

            # add target nodes for the rack
            dc_idx = rack_nodes[0].dc_idx
            image_id = self._get_target_image_for_dc(dc_idx)
            added = self.db_cluster.add_nodes(
                count=len(rack_nodes),
                dc_idx=dc_idx,
                rack=rack,
                enable_auto_bootstrap=True,
                instance_type=self.target_instance_type,
                image_id=image_id,
            )

            self._initialize_and_start_nodes(added)
            all_new_nodes.extend(added)

            self.update_seeds_for_migration(all_new_nodes)
            self.log.info("Decommissioning %d source nodes from rack %s", len(rack_nodes), rack)
            self._decommission_nodes(rack_nodes)

        self.monitors.reconfigure_scylla_monitoring()
        self.log.info("Batch migration complete: %d new nodes", len(all_new_nodes))
        return all_new_nodes

    def _initialize_and_start_nodes(self, nodes: list) -> None:
        """Initialize and start the given nodes"""
        with adaptive_timeout(
            Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP
        ):
            self.db_cluster.wait_for_init(
                node_list=nodes, timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP, check_node_health=False
            )
        for node in nodes:
            node.wait_node_fully_start()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=nodes)

    def _run_nodetool_decommission(self, node) -> None:
        """Run nodetool decommission on a single node without verify/terminate."""
        if not node._is_zero_token_node and is_tablets_feature_enabled(node):
            DataCenterTopologyRfControl(target_node=node).decrease_keyspaces_rf()
        with adaptive_timeout(operation=Operations.DECOMMISSION, node=node):
            node.run_nodetool("decommission", long_running=True, retry=0)
        self.log.info("Nodetool decommission completed on %s", node.name)

    def _verify_and_terminate_nodes(self, nodes: list) -> None:
        """Verify decommission and terminate nodes sequentially"""
        live_candidates = [n for n in self.db_cluster.data_nodes if n not in nodes]
        verification_node = live_candidates[0] if live_candidates else None

        for node in nodes:
            if verification_node:
                status = self.db_cluster.get_nodetool_status(verification_node)
                node_ips = {ip for dc_ips in status.values() for ip in dc_ips}
                if node.ip_address in node_ips:
                    raise Exception(
                        f"Decommissioned node {node.name} ({node.ip_address}) still in cluster. Status: {status}"
                    )
            self.log.info("Decommission %s PASS", node.name)
            self.db_cluster.terminate_node(node)
        self.monitors.reconfigure_scylla_monitoring()

    def _decommission_nodes(self, nodes: list) -> None:
        """Decommission a list of nodes"""
        for node in nodes:
            if node.is_seed:
                node.set_seed_flag(False)
                self.db_cluster.update_seed_provider()
                self.log.debug("Removed seed flag from %s", node.name)

        num_workers = None if (self.db_cluster.parallel_node_operations and nodes[0].raft.is_enabled) else 1
        self.log.info(
            "Decommissioning %d nodes (%s): %s",
            len(nodes),
            "parallel" if num_workers is None else "sequential",
            [n.name for n in nodes],
        )

        ParallelObject(objects=nodes, timeout=MAX_TIME_WAIT_FOR_DECOMMISSION, num_workers=num_workers).run(
            self._run_nodetool_decommission, ignore_exceptions=False, unpack_objects=True
        )

        self._verify_and_terminate_nodes(nodes)
        self.db_cluster.wait_all_nodes_un()

    def _decommission_parallel_across_dcs(self) -> None:
        """Decommission one node per DC in parallel"""
        source_nodes = self.get_nodes_by_instance_type(self.source_instance_type)
        if not source_nodes:
            self.log.info("No source platform nodes to decommission")
            return

        nodes_by_dc = group_nodes_by_dc_idx(source_nodes)
        dc_iter = {dc: iter(nodes) for dc, nodes in nodes_by_dc.items()}

        batch_num = 0
        while dc_iter:
            batch_num += 1
            batch = []
            empty_dcs = []
            for dc_idx, node_iter in dc_iter.items():
                try:
                    node = next(node_iter)
                    if node.is_seed:
                        node.set_seed_flag(False)
                        self.db_cluster.update_seed_provider()
                    batch.append(node)
                except StopIteration:
                    empty_dcs.append(dc_idx)

            for dc in empty_dcs:
                del dc_iter[dc]

            if batch:
                self.log.info(
                    "Decommission batch %d: %s (parallel across %d DCs)", batch_num, [n.name for n in batch], len(batch)
                )

                ParallelObject(
                    objects=batch,
                    timeout=MAX_TIME_WAIT_FOR_DECOMMISSION,
                    num_workers=len(batch),
                ).run(self._run_nodetool_decommission, ignore_exceptions=False, unpack_objects=True)

                self._verify_and_terminate_nodes(batch)
                self.db_cluster.wait_all_nodes_un()

        self.monitors.reconfigure_scylla_monitoring()

    def update_seeds_for_migration(self, new_nodes: list) -> None:
        """Ensure at least one new node is a seed before decommissioning old seeds"""
        source_seeds = [
            n for n in self.db_cluster.seed_nodes if getattr(n, "_instance_type", None) == self.source_instance_type
        ]
        self.log.info(
            "Current seeds: %s, source platform seeds: %s",
            [n.name for n in self.db_cluster.seed_nodes],
            [n.name for n in source_seeds],
        )

        if source_seeds and not any(n.is_seed for n in new_nodes):
            new_seed = new_nodes[0]
            new_seed.set_seed_flag(True)
            self.log.debug("Set %s as new seed node", new_seed.name)
            self.db_cluster.update_seed_provider()
            self.log.info("Updated seed provider on all nodes")

    def verify_migration_complete(self) -> None:
        """Assert all nodes are on target platform and node count is correct"""
        self.log.info("Verifying migration completion...")

        # check that all nodes are of the target instance type
        for node in self.db_cluster.nodes:
            if node._instance_type != self.target_instance_type:
                raise AssertionError(
                    f"Node {node.name} has instance type {node._instance_type}, expected {self.target_instance_type}"
                )

        n_db_nodes = self.params.get("n_db_nodes")
        if isinstance(n_db_nodes, str) and " " in n_db_nodes:
            expected_count = sum(int(x) for x in n_db_nodes.split())
        else:
            expected_count = int(n_db_nodes)

        actual_count = len(self.db_cluster.nodes)
        if actual_count != expected_count:
            raise AssertionError(f"Expected {expected_count} nodes after migration, got {actual_count}")

        self.log.info("Migration verification passed: %d nodes on %s", actual_count, self.target_instance_type)

    def run_stress_before_migration(self) -> None:
        if stress_cmd := self.params.get("stress_before_migration"):
            self.verify_stress_thread(self.run_stress_thread(stress_cmd=stress_cmd))

    def run_verify_stress_after_migration(self) -> None:
        if stress_cmd := self.params.get("verify_stress_after_migration"):
            self.verify_stress_thread(self.run_stress_thread(stress_cmd=stress_cmd))

    def test_migrate_platform(self):
        """
        Single-DC platform migration test.

        Flow:
        1. Initial cluster health checks
        2. Prepare data (cassandra-stress + scylla-bench validation dataset)
        3. Start continuous stress workload
        4. Add all target platform nodes in parallel
        5. Update seed configuration
        6. Decommission source nodes sequentially
        7. Verify data integrity and stress results
        """
        InfoEvent(message="Initial cluster health checks").publish()
        self.db_cluster.wait_all_nodes_un()
        self.db_cluster.wait_for_schema_agreement()
        self.log.info("Initial cluster: %d nodes on %s", len(self.db_cluster.nodes), self.source_instance_type)

        InfoEvent(message="Preparing data").publish()
        self.run_prepare_write_cmd()
        self.run_stress_before_migration()

        InfoEvent(message="Starting continuous stress workload").publish()
        stress_queue = []
        if self.params.get("stress_cmd"):
            self._run_all_stress_cmds(
                stress_queue,
                {"stress_cmd": self.params.get("stress_cmd"), "round_robin": self.params.get("round_robin")},
            )
            self.log.info("Started continuous stress workload")
        else:
            self.log.warning("No stress_cmd configured, running migration without workload")

        InfoEvent(message="Adding target platform nodes (parallel)").publish()
        new_nodes = self.add_nodes_parallel()

        InfoEvent(message="Updating seed configuration").publish()
        self.update_seeds_for_migration(new_nodes)

        InfoEvent(message="Decommissioning source platform nodes").publish()
        self._decommission_nodes(self.get_nodes_by_instance_type(self.source_instance_type))
        self.monitors.reconfigure_scylla_monitoring()

        InfoEvent(message="Verifying migration").publish()
        self.kill_stress_thread()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        if stress_queue:
            self.log.info("Stress workload completed successfully")
        self.run_verify_stress_after_migration()
        self.verify_migration_complete()

    def test_migrate_platform_multi_dc(self):
        """
        Multi-DC platform migration test with parallel operations across DCs.

        Flow:
        1. Initial cluster health checks
        2. Prepare data
        3. Start continuous stress workload
        4. Add all target nodes simultaneously across all DCs
        5. Update seed configuration
        6. Decommission source nodes in parallel across DCs (one per DC at a time)
        7. Verify data integrity and stress results
        """
        InfoEvent(message="Initial cluster health checks").publish()
        self.db_cluster.wait_all_nodes_un()
        self.db_cluster.wait_for_schema_agreement()
        self.log.info("Initial cluster: %d nodes on %s", len(self.db_cluster.nodes), self.source_instance_type)

        InfoEvent(message="Preparing data").publish()
        self.run_prepare_write_cmd()
        self.run_stress_before_migration()

        InfoEvent(message="Starting continuous stress workload").publish()
        stress_queue = []
        if self.params.get("stress_cmd"):
            self._run_all_stress_cmds(
                stress_queue,
                {"stress_cmd": self.params.get("stress_cmd"), "round_robin": self.params.get("round_robin")},
            )
            self.log.info("Started continuous stress workload")
        else:
            self.log.warning("No stress_cmd configured, running migration without workload")

        InfoEvent(message="Adding target platform nodes (all DCs in parallel)").publish()
        new_nodes = self.add_nodes_parallel()

        InfoEvent(message="Updating seed configuration").publish()
        self.update_seeds_for_migration(new_nodes)

        InfoEvent(message="Decommissioning source nodes (parallel across DCs)").publish()
        self._decommission_parallel_across_dcs()

        InfoEvent(message="Verifying migration").publish()
        self.kill_stress_thread()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        if stress_queue:
            self.log.info("Stress workload completed successfully")
        self.run_verify_stress_after_migration()
        self.verify_migration_complete()

    def test_migrate_platform_batched(self):
        """
        Batched platform migration test - migrate rack by rack.

        Flow:
        1. Initial cluster health checks
        2. Prepare data
        3. Start continuous stress workload
        4. For each rack:
           a. Add target nodes for rack
           b. Update seeds
           c. Decommission source nodes from rack
        5. Verify data integrity and stress results
        """
        InfoEvent(message="Initial cluster health checks").publish()
        self.db_cluster.wait_all_nodes_un()
        self.db_cluster.wait_for_schema_agreement()
        self.log.info("Initial cluster: %d nodes on %s", len(self.db_cluster.nodes), self.source_instance_type)

        InfoEvent(message="Preparing data").publish()
        self.run_prepare_write_cmd()
        self.run_stress_before_migration()

        InfoEvent(message="Starting continuous stress workload").publish()
        stress_queue = []
        if self.params.get("stress_cmd"):
            self._run_all_stress_cmds(
                stress_queue,
                {"stress_cmd": self.params.get("stress_cmd"), "round_robin": self.params.get("round_robin")},
            )
            self.log.info("Started continuous stress workload")
        else:
            self.log.warning("No stress_cmd configured, running migration without workload")

        InfoEvent(message="Starting batch-by-rack migration").publish()
        self.migrate_nodes_batch_by_rack()  # handles add + decommission per rack

        InfoEvent(message="Verifying migration").publish()
        self.kill_stress_thread()
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        if stress_queue:
            self.log.info("Stress workload completed successfully")
        self.run_verify_stress_after_migration()
        self.verify_migration_complete()
