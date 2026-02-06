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

from longevity_test import LongevityTest
from sdcm.sct_events.system import InfoEvent
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.aws_utils import get_arch_from_instance_type


class PlatformMigrationTest(LongevityTest):
    """
    Test platform migration using add-and-replace node cycling.

    Migrates cluster from source platform (e.g., x86) to target platform (e.g., ARM)
    while maintaining continuous workload.
    """

    def setUp(self):
        super().setUp()
        self.source_instance_type = self.params.get("instance_type_db")
        self.target_instance_type = self.params.get("instance_type_db_target")
        target_ami_param = self.params.get("ami_id_db_scylla_target")
        self.target_amis = target_ami_param.split() if target_ami_param else []

        if not self.target_instance_type:
            raise ValueError("instance_type_db_target is required for platform migration test")
        if not self.target_amis:
            raise ValueError("ami_id_db_scylla_target must be set or auto-discovered")

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

    def get_nodes_by_instance_type(self, instance_type: str) -> list:
        return [n for n in self.db_cluster.nodes if n._instance_type == instance_type]

    def add_target_platform_nodes(self) -> list:
        """Add target platform nodes, one per rack to match existing topology"""
        source_nodes = self.get_nodes_by_instance_type(self.source_instance_type)
        if not source_nodes:
            raise ValueError(f"No nodes found with source instance type: {self.source_instance_type}")

        new_nodes = []
        for source_node in source_nodes:
            dc_idx = source_node.dc_idx
            ami_id = self.target_amis[0] if len(self.target_amis) == 1 else self.target_amis[dc_idx]

            if dc_idx >= len(self.target_amis):
                raise ValueError(f"No target AMI configured for DC index {dc_idx}. Available AMIs: {self.target_amis}")

            self.log.info(
                "Adding target platform node to DC %s, rack %s (replacing %s)",
                dc_idx,
                source_node.rack,
                source_node.name,
            )

            added = self.db_cluster.add_nodes(
                count=1,
                dc_idx=dc_idx,
                rack=source_node.rack,
                enable_auto_bootstrap=True,
                instance_type=self.target_instance_type,
                ami_id=ami_id,
            )

            with adaptive_timeout(
                Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP
            ):
                self.db_cluster.wait_for_init(
                    node_list=added, timeout=MAX_TIME_WAIT_FOR_NEW_NODE_UP, check_node_health=False
                )

            for node in added:
                node.wait_node_fully_start()

            self.db_cluster.wait_for_nodes_up_and_normal(nodes=added)
            new_nodes.extend(added)

        self.monitors.reconfigure_scylla_monitoring()
        self.log.info("Added %d target platform nodes: %s", len(new_nodes), [n.name for n in new_nodes])
        return new_nodes

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

    def decommission_source_platform_nodes(self) -> None:
        """Decommission all source platform nodes one by one"""
        source_nodes = self.get_nodes_by_instance_type(self.source_instance_type)
        if not source_nodes:
            self.log.info("No source platform nodes to decommission")
            return

        for i, node in enumerate(source_nodes, 1):
            self.log.info("Decommissioning source node %d/%d: %s", i, len(source_nodes), node.name)

            if node.is_seed:
                node.set_seed_flag(False)
                self.db_cluster.update_seed_provider()
                self.log.debug("Removed seed flag from %s", node.name)

            self.db_cluster.decommission(node)
            self.log.info("Decommissioned %s (%d/%d)", node.name, i, len(source_nodes))
            self.db_cluster.wait_all_nodes_un()
            self.log.info("Cluster healthy after decommissioning %s", node.name)

        self.monitors.reconfigure_scylla_monitoring()

    def verify_migration_complete(self) -> None:
        """Assert all nodes are on target platform and node count is correct"""
        self.log.info("Verifying migration completion...")

        # check that all nodes are of the target instance type
        for node in self.db_cluster.nodes:
            if node._instance_type != self.target_instance_type:
                raise AssertionError(
                    f"Node {node.name} has instance type {node._instance_type}, expected {self.target_instance_type}"
                )

        # check that node count matches original
        expected_count = self.params.get("n_db_nodes")
        actual_count = len(self.db_cluster.nodes)
        if actual_count != expected_count:
            raise AssertionError(f"Expected {expected_count} nodes after migration, got {actual_count}")

        self.log.info("Migration verification passed: %d nodes on %s", actual_count, self.target_instance_type)

    def test_migrate_platform(self):
        """
        Platform migration test flow:
        1. Initial checks
        2. Prepare data
        3. Start continuous stress workload
        4. Add target platform nodes
        5. Update seed configuration
        6. Decommission source platform nodes
        7. Verify migration and stress results
        """
        InfoEvent(message="Initial checks").publish()
        self.db_cluster.wait_all_nodes_un()
        self.db_cluster.wait_for_schema_agreement()
        self.log.info("Initial cluster: %d nodes on %s", len(self.db_cluster.nodes), self.source_instance_type)

        InfoEvent(message="Prepare data").publish()
        self.run_prepare_write_cmd()

        InfoEvent(message="Starting continuous stress workload").publish()
        stress_cmd = self.params.get("stress_cmd")
        stress_queue = self.run_stress_thread(stress_cmd) if stress_cmd else None
        if stress_queue:
            self.log.info("Started continuous stress workload")
        else:
            self.log.warning("No stress_cmd configured, running migration without workload")

        InfoEvent(message="Adding target platform nodes").publish()
        new_nodes = self.add_target_platform_nodes()
        self.log.info("Added %d target platform nodes", len(new_nodes))

        InfoEvent(message="Updating seed configuration").publish()
        self.update_seeds_for_migration(new_nodes)

        InfoEvent(message="Decommissioning source platform nodes").publish()
        self.decommission_source_platform_nodes()

        InfoEvent(message="Verifying migration").publish()
        if stress_queue:
            self.verify_stress_thread(stress_queue)
            self.log.info("Stress workload completed successfully")
        self.verify_migration_complete()
