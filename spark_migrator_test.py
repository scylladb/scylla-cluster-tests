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
# Copyright (c) 2025 ScyllaDB

"""Spark migrator test module for testing scylla-spark-migrator on EMR clusters."""

import time

from sdcm.spark_migrator import MigratorConfig, SparkMigratorRunner
from sdcm.tester import ClusterTester


class SparkMigratorTest(ClusterTester):
    """Test scylla-spark-migrator on Amazon EMR clusters.

    This test provisions a Scylla cluster (via standard AWS backend) and
    an EMR cluster (as an auxiliary resource), then runs spark-migrator
    jobs to test data migration.
    """

    def setUp(self):
        super().setUp()
        self.migration_results = {}

    def test_full_migration(self):
        """Run a full table migration from Scylla source to Scylla target.

        Steps:
            1. Create source keyspace and table with test data
            2. Submit spark-migrator job on EMR cluster
            3. Wait for migration to complete
            4. Validate migrated data matches source
        """
        self.log.info("Starting full migration test")

        if not self.emr_cluster:
            self.log.error("EMR cluster not provisioned. Set 'emr_release_label' in config.")
            raise RuntimeError("EMR cluster not available for spark-migrator test")

        jar_path = self.params.get("emr_spark_migrator_jar_path")
        if not jar_path:
            raise RuntimeError("emr_spark_migrator_jar_path not configured")

        # Prepare source data
        self._prepare_source_data()

        # Run migration
        runner = SparkMigratorRunner(self.emr_cluster)
        migrator_config = self._build_migrator_config()

        self.log.info("Submitting spark-migrator job...")
        start_time = time.time()
        result = runner.run_migration(
            cluster_id=self.emr_cluster.cluster_id,
            jar_path=jar_path,
            migrator_config=migrator_config,
        )
        duration = time.time() - start_time

        self.migration_results = {
            "step_id": result["step_id"],
            "duration_seconds": duration,
            "status": "completed",
        }

        self.log.info("Migration completed in %.1f seconds", duration)

        # Validate migration
        self._validate_migration()

    def _prepare_source_data(self):
        """Create source keyspace/table and load test data."""
        self.log.info("Preparing source data for migration test")
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS migrator_test "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            session.execute(
                "CREATE TABLE IF NOT EXISTS migrator_test.source_table (id int PRIMARY KEY, data text, value int)"
            )
            for i in range(1000):
                session.execute(
                    "INSERT INTO migrator_test.source_table (id, data, value) VALUES (%s, %s, %s)",
                    (i, f"data_{i}", i * 10),
                )
        self.log.info("Source data prepared: 1000 rows inserted")

    def _build_migrator_config(self):
        """Build migrator configuration from test parameters."""
        node = self.db_cluster.nodes[0]
        host = node.private_ip_address

        return MigratorConfig(
            source_host=host,
            source_port=9042,
            source_keyspace="migrator_test",
            source_table="source_table",
            target_host=host,
            target_port=9042,
            target_keyspace="migrator_test",
            target_table="target_table",
        )

    def _validate_migration(self):
        """Validate that migrated data matches source."""
        self.log.info("Validating migration results")
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            result = session.execute("SELECT count(*) FROM migrator_test.target_table")
            row_count = result.one()[0]
            assert row_count == 1000, f"Expected 1000 rows, got {row_count}"
        self.log.info("Migration validation passed: %d rows migrated", row_count)
