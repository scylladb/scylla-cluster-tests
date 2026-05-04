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

import random
import re
import time
from concurrent.futures import ThreadPoolExecutor

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster as CassandraCluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement

from sdcm.cluster import BaseNode
from sdcm.spark_migrator import (
    MigratorConfig,
    SparkMigratorRunner,
    discover_cs_source_hosts,
    upload_migrator_jar_to_s3,
)
from sdcm.tester import ClusterTester

_SAMPLE_SIZE = 10


class SparkMigratorTest(ClusterTester):
    """Test scylla-spark-migrator on Amazon EMR clusters.

    This test provisions a Scylla cluster (via standard AWS backend) and
    an EMR cluster (as an auxiliary resource), then runs spark-migrator
    jobs to test data migration.
    """

    def setUp(self):
        super().setUp()
        self.migration_results = {}

    def _ensure_migrator_jar(self):
        """Download and upload migrator JAR to S3 if release is configured.

        Sets emr_spark_migrator_jar_path in params if not already set.
        """
        if self.params.get("emr_spark_migrator_jar_path"):
            return

        release_tag = self.params.get("emr_spark_migrator_release")
        if not release_tag:
            return

        region = self.params.region_names[0]
        s3_uri = upload_migrator_jar_to_s3(
            release_tag=release_tag,
            s3_bucket=f"sct-emr-spark-migrator-{region}",
            region_name=region,
        )
        self.log.info("Migrator JAR available at %s", s3_uri)
        self.params["emr_spark_migrator_jar_path"] = s3_uri

    def test_full_migration(self):
        """Run a full table migration from Scylla source to Scylla target.

        Steps:
            1. Create source keyspace and table with test data
            2. Submit spark-migrator job on EMR cluster
            3. Wait for migration to complete
            4. Validate migrated data matches source
        """
        self.log.info("Starting full migration test")

        self._ensure_migrator_jar()

        if not self.emr_cluster:
            self.log.error("EMR cluster not provisioned. Set 'emr_release_label' in config.")
            raise RuntimeError("EMR cluster not available for spark-migrator test")

        jar_path = self.params.get("emr_spark_migrator_jar_path")
        if not jar_path:
            raise RuntimeError("emr_spark_migrator_jar_path not configured")

        self._prepare_source_data()

        runner = SparkMigratorRunner(self.emr_cluster)
        migrator_config = self._build_migrator_config()

        if not migrator_config.config_path:
            region = self.params.region_names[0]
            config_dict = SparkMigratorRunner.generate_migrator_config(migrator_config)
            migrator_config.config_path = runner.upload_config_to_s3(
                config_dict,
                s3_bucket=f"sct-emr-spark-migrator-{region}",
                test_id=self.test_id,
                region_name=region,
            )

        self.log.info("Submitting spark-migrator job...")
        start_time = time.time()
        result = runner.run_migration(
            cluster_id=self.emr_cluster.cluster_id,
            jar_s3_path=jar_path,
            migrator_config=migrator_config,
            timeout_minutes=self.params.get("migrator_step_timeout_minutes") or 360,
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

    def test_migration_from_external_source(self):
        """Migrate a table from an external Cassandra cluster to the Scylla target.

        Source cluster is provided via migrator_source_hosts or auto-discovered via
        migrator_source_test_id (hosts are looked up by EC2 tag). Target is
        self.db_cluster. Validates row count match plus a sample of individual rows.
        """
        self.log.info("Starting external-source migration test")
        self._ensure_migrator_jar()

        if not self.emr_cluster:
            raise RuntimeError("EMR cluster not available — set emr_release_label in config")

        jar_path = self.params.get("emr_spark_migrator_jar_path")
        if not jar_path:
            raise RuntimeError("emr_spark_migrator_jar_path not configured")

        if not (source_hosts := self.params.get("migrator_source_hosts")):
            source_test_id = self.params.get("migrator_source_test_id")
            if not source_test_id:
                raise RuntimeError("set migrator_source_hosts or migrator_source_test_id")
            source_hosts = discover_cs_source_hosts(source_test_id, self.params.region_names[0])
        if isinstance(source_hosts, str):
            source_hosts = [source_hosts]

        source_keyspace = self.params.get("migrator_source_keyspace") or "migrator_test"
        source_table = self.params.get("migrator_source_table") or "source_table"
        target_keyspace = self.params.get("migrator_target_keyspace") or source_keyspace
        target_table = self.params.get("migrator_target_table") or source_table
        target_node = self.db_cluster.nodes[0]

        self._prepare_external_source_data(source_hosts, BaseNode.CQL_PORT, source_keyspace, source_table)

        # scylla-migrator v2.0.x CQL writer requires target keyspace/table to exist
        self._prepare_target_schema(target_keyspace, target_table)
        runner = SparkMigratorRunner(self.emr_cluster)
        migrator_config = MigratorConfig(
            source_type="cassandra",
            source_hosts=source_hosts,
            source_port=BaseNode.CQL_PORT,
            source_keyspace=source_keyspace,
            source_table=source_table,
            target_host=target_node.private_ip_address,
            target_port=BaseNode.CQL_PORT,
            target_keyspace=target_keyspace,
            target_table=target_table,
        )

        region = self.params.region_names[0]
        migrator_config.config_path = runner.upload_config_to_s3(
            SparkMigratorRunner.generate_migrator_config(migrator_config),
            s3_bucket=f"sct-emr-spark-migrator-{region}",
            test_id=self.test_id,
            region_name=region,
        )

        self.log.info(
            "Submitting spark-migrator job (source=%s ks=%s.%s → target=%s ks=%s.%s)",
            source_hosts[0],
            source_keyspace,
            source_table,
            target_node.private_ip_address,
            target_keyspace,
            target_table,
        )
        result = runner.run_migration(
            cluster_id=self.emr_cluster.cluster_id,
            jar_s3_path=jar_path,
            migrator_config=migrator_config,
            timeout_minutes=self.params.get("migrator_step_timeout_minutes") or 360,
        )
        self.log.info("Migration completed in %.1f seconds", result["duration_seconds"])

        self._validate_external_migration(
            source_hosts,
            BaseNode.CQL_PORT,
            source_keyspace,
            source_table,
            target_keyspace,
            target_table,
        )

    def _prepare_target_schema(self, keyspace: str, table: str):
        """Create matching keyspace/table on target Scylla.

        scylla-migrator v2.0.x CQL writer (com.scylladb.migrator.writers.Scylla.writeDataframe)
        requires the target keyspace and table to already exist.
        """
        self.log.info("Preparing target schema %s.%s on Scylla", keyspace, table)
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (id int PRIMARY KEY, data text, value int)")
        self.log.info("Target schema ready: %s.%s", keyspace, table)

    def _prepare_external_source_data(self, hosts: list, port: int, keyspace: str, table: str):
        """Create keyspace/table on external Cassandra cluster and insert 1000 test rows."""
        self.log.info("Preparing source data on external cluster %s", hosts[0])
        cluster = CassandraCluster(contact_points=hosts, port=port)
        try:
            session = cluster.connect()
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (id int PRIMARY KEY, data text, value int)")
            for i in range(1000):
                session.execute(
                    f"INSERT INTO {keyspace}.{table} (id, data, value) VALUES (%s, %s, %s)",
                    (i, f"data_{i}", i * 10),
                )
            self.log.info("Source data prepared: 1000 rows in %s.%s", keyspace, table)
        finally:
            cluster.shutdown()

    def _validate_external_migration(
        self,
        source_hosts: list,
        source_port: int,
        source_keyspace: str,
        source_table: str,
        target_keyspace: str,
        target_table: str,
    ):
        """Validate migrated data: row count match + _SAMPLE_SIZE random row comparison."""
        self.log.info(
            "Validating migration from %s.%s → %s.%s", source_keyspace, source_table, target_keyspace, target_table
        )

        source_cluster = CassandraCluster(contact_points=source_hosts, port=source_port)
        try:
            src_session = source_cluster.connect()
            src_count = src_session.execute(f"SELECT count(*) FROM {source_keyspace}.{source_table}").one()[0]
        finally:
            source_cluster.shutdown()

        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as tgt_session:
            tgt_count = tgt_session.execute(f"SELECT count(*) FROM {target_keyspace}.{target_table}").one()[0]

        assert src_count == tgt_count, f"row count mismatch: source {src_count} vs target {tgt_count}"
        self.log.info("Row count matches: %d rows", src_count)

        # sample-row comparison
        sample_ids = random.sample(range(1000), _SAMPLE_SIZE)
        source_cluster = CassandraCluster(contact_points=source_hosts, port=source_port)
        try:
            src_session = source_cluster.connect()
            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as tgt_session:
                for row_id in sample_ids:
                    src_row = src_session.execute(
                        f"SELECT id, data, value FROM {source_keyspace}.{source_table} WHERE id = %s",
                        (row_id,),
                    ).one()
                    tgt_row = tgt_session.execute(
                        f"SELECT id, data, value FROM {target_keyspace}.{target_table} WHERE id = %s",
                        (row_id,),
                    ).one()
                    assert src_row == tgt_row, f"row mismatch for id={row_id}: source={src_row} target={tgt_row}"
        finally:
            source_cluster.shutdown()

        self.log.info("Sample validation passed: %d/%d rows match", _SAMPLE_SIZE, _SAMPLE_SIZE)

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
            session.execute(
                "CREATE TABLE IF NOT EXISTS migrator_test.target_table (id int PRIMARY KEY, data text, value int)"
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
            source_hosts=[host],
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

    def test_migration_cs_to_scylla_small(self):
        """Migrate 100k rows from a provisioned Cassandra source cluster to the Scylla target.

        Requires db_type: mixed_cassandra and an EMR cluster with the migrator JAR configured.
        Uses dump_schema to replicate the Cassandra schema on Scylla before running the job.
        """
        self.log.info("Starting cs-to-scylla small migration test")

        if self.cs_db_cluster is None:
            raise RuntimeError("cs_db_cluster not provisioned — set db_type: mixed_cassandra in test-case yaml")

        if not self.emr_cluster:
            raise RuntimeError("EMR cluster not available — set emr_release_label")

        self._assert_region_coherence()
        self._ensure_migrator_jar()

        jar_path = self.params.get("emr_spark_migrator_jar_path")
        if not jar_path:
            raise RuntimeError("emr_spark_migrator_jar_path not configured")

        source_keyspace = self.params.get("migrator_source_keyspace") or "migrator_test"
        source_table = self.params.get("migrator_source_table") or "source_table"
        target_keyspace = self.params.get("migrator_target_keyspace") or source_keyspace
        target_table = self.params.get("migrator_target_table") or source_table
        row_count = 100_000

        self._preload_cs_source_data(source_keyspace, source_table, row_count)
        self._apply_source_schema_on_target(source_keyspace, target_keyspace)

        runner = SparkMigratorRunner(self.emr_cluster)
        migrator_config = self._build_cs_to_scylla_migrator_config(
            source_keyspace, source_table, target_keyspace, target_table
        )

        region = self.params.region_names[0]
        migrator_config.config_path = runner.upload_config_to_s3(
            SparkMigratorRunner.generate_migrator_config(migrator_config),
            s3_bucket=f"sct-emr-spark-migrator-{region}",
            test_id=self.test_id,
            region_name=region,
        )

        result = runner.run_migration(
            cluster_id=self.emr_cluster.cluster_id,
            jar_s3_path=jar_path,
            migrator_config=migrator_config,
            timeout_minutes=self.params.get("migrator_step_timeout_minutes") or 360,
        )
        self.log.info("Migration completed in %.1f seconds", result["duration_seconds"])

        self._validate_migration_count_and_sample(
            source_cluster=self.cs_db_cluster,
            source_keyspace=source_keyspace,
            source_table=source_table,
            target_keyspace=target_keyspace,
            target_table=target_table,
            expected_row_count=row_count,
        )

        if self.params.get("migrator_run_validator"):
            self._run_validator(runner, jar_path, migrator_config)

    def _assert_region_coherence(self):
        """Fail fast if EMR cluster is in a different region than the test."""
        if (emr_region := self.emr_cluster.region_name) != (test_region := self.params.region_names[0]):
            raise RuntimeError(
                f"EMR cluster region {emr_region!r} != test region {test_region!r}; "
                "cross-region data transfer is slow + costly — fix the yaml."
            )

    def _run_preload_against_cs_cluster(self):
        """Dispatch each prepare_write_cmd against cs_db_cluster.nodes via the stress wrappers."""
        prepare_cmds = self.params.get("prepare_write_cmd") or []
        if not prepare_cmds:
            raise RuntimeError("scale test requires prepare_write_cmd in the test-case yaml")
        if isinstance(prepare_cmds, str):
            prepare_cmds = [prepare_cmds]

        self.log.info("Starting preload of %d stress chunk(s) against cs_db_cluster", len(prepare_cmds))
        threads = [
            self.run_stress_thread(stress_cmd=cmd, round_robin=True, node_list=self.cs_db_cluster.nodes)
            for cmd in prepare_cmds
        ]
        for t in threads:
            if not self.verify_stress_thread(t):
                raise RuntimeError("preload stress thread reported errors — see sct.log for details")
        self.log.info("Preload complete")

    def _run_validator(self, runner, jar_path, migrator_config):
        """Submit a scylla-migrator validator step + assert zero mismatches.

        The EMR step state is authoritative: COMPLETED = 0 mismatches, FAILED = ≥1 mismatch.
        On FAILED, the step stdout is fetched and dumped to sct.log; it contains the upstream
        "Found N comparison failure(s) in sample: <breakdown>" summary and per-row
        RowComparisonFailure entries for diagnosis.
        """
        self.log.info("Submitting validator step against the just-migrated tables")
        step_id = runner.submit_validator_job(
            cluster_id=self.emr_cluster.cluster_id,
            jar_s3_path=jar_path,
            migrator_config=migrator_config,
        )

        try:
            runner.wait_for_migration(
                self.emr_cluster.cluster_id,
                step_id,
                timeout_minutes=self.params.get("validator_step_timeout_minutes") or 60,
            )
        except AssertionError:
            try:
                step_log = runner.get_step_stdout(self.emr_cluster.cluster_id, step_id)
            except Exception as fetch_err:  # noqa: BLE001
                self.log.warning("Could not fetch validator step log: %s", fetch_err)
                step_log = ""
            self.log.error("Validator step output:\n%s", step_log or "(no log captured)")
            raise AssertionError("validator step FAILED — see stdout dump above") from None

        self.log.info("Validator passed: 0 mismatches across migrated rows")

    def test_migration_cs_to_scylla_scale(self):
        """Scale-out variant of the cs-to-scylla migration test."""
        self.log.info("Starting cs-to-scylla scale migration test")

        if self.cs_db_cluster is None:
            raise RuntimeError("cs_db_cluster not provisioned — set db_type: mixed_cassandra in test-case yaml")
        if not self.emr_cluster:
            raise RuntimeError("EMR cluster not available — set emr_release_label")

        self._assert_region_coherence()
        self._ensure_migrator_jar()

        jar_path = self.params.get("emr_spark_migrator_jar_path")
        if not jar_path:
            raise RuntimeError("emr_spark_migrator_jar_path not configured")

        source_keyspace = self.params.get("migrator_source_keyspace") or "keyspace1"
        source_table = self.params.get("migrator_source_table") or "standard1"
        target_keyspace = self.params.get("migrator_target_keyspace") or source_keyspace
        target_table = self.params.get("migrator_target_table") or source_table

        if not self.params.get("reuse_cluster"):
            self._run_preload_against_cs_cluster()

        self._apply_source_schema_on_target(source_keyspace, target_keyspace)

        runner = SparkMigratorRunner(self.emr_cluster)
        migrator_config = self._build_cs_to_scylla_migrator_config(
            source_keyspace, source_table, target_keyspace, target_table
        )

        region = self.params.region_names[0]
        migrator_config.config_path = runner.upload_config_to_s3(
            SparkMigratorRunner.generate_migrator_config(migrator_config),
            s3_bucket=f"sct-emr-spark-migrator-{region}",
            test_id=self.test_id,
            region_name=region,
        )

        result = runner.run_migration(
            cluster_id=self.emr_cluster.cluster_id,
            jar_s3_path=jar_path,
            migrator_config=migrator_config,
            timeout_minutes=self.params.get("migrator_step_timeout_minutes") or 360,
        )
        self.log.info("Migration completed in %.1f seconds", result["duration_seconds"])

        if self.params.get("migrator_run_validator"):
            self._run_validator(runner, jar_path, migrator_config)

    def _preload_cs_source_data(self, keyspace: str, table: str, row_count: int):
        """Insert row_count rows into cs_db_cluster keyspace.table using concurrent writes."""
        self.log.info("Preloading %d rows into %s.%s on Cassandra source", row_count, keyspace, table)
        with self.cs_db_cluster.cql_connection_patient(self.cs_db_cluster.nodes[0]) as session:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (id int PRIMARY KEY, data text, value int)")
            stmt = session.prepare(f"INSERT INTO {keyspace}.{table} (id, data, value) VALUES (?, ?, ?)")
            params = [(i, f"data_{i}", i * 10) for i in range(row_count)]
            execute_concurrent_with_args(session, stmt, params, concurrency=200, raise_on_first_error=True)
        self.log.info("Preloaded %d rows into %s.%s", row_count, keyspace, table)

    def _apply_source_schema_on_target(self, source_keyspace: str, target_keyspace: str):
        """Dump schema from Cassandra source and apply it on the Scylla target.

        Pre-creates the target keyspace with SimpleStrategy RF=1 (the schema dump
        strips the CREATE KEYSPACE statement by default, and the target may use
        different replication settings anyway).

        For phase 1's simple schemas a plain str.replace is sufficient to rename
        all keyspace-qualified object references when source and target keyspace names differ.
        """
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {target_keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )

        schema_cql = self.cs_db_cluster.dump_schema(source_keyspace, include_keyspace_stmt=False)

        if target_keyspace != source_keyspace:
            schema_cql = schema_cql.replace(f"{source_keyspace}.", f"{target_keyspace}.")

        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for raw_stmt in schema_cql.split(";"):
                cql_stmt = raw_stmt.strip()
                if not cql_stmt:
                    continue
                cql_stmt = self._make_create_idempotent(cql_stmt)
                self.log.info("Applying schema statement: %s", cql_stmt.splitlines()[0])
                session.execute(cql_stmt)

    def _count_rows_parallel(
        self,
        cluster,
        keyspace: str,
        table: str,
        partition_key: str | None = None,
        parallelism: int = 16,
        per_query_timeout: int = 600,
    ) -> int:
        """Count rows by sharding the Murmur3 token ring across parallel concurrent sub-queries."""
        min_token = -(2**63)
        max_token = (2**63) - 1
        step = (max_token - min_token + 1) // parallelism

        boundaries = [min_token + step * i for i in range(parallelism + 1)]
        boundaries[-1] = max_token

        # mark first range as inclusive-low (>=) so MIN_TOKEN is included
        ranges = [(boundaries[i], boundaries[i + 1], i == 0) for i in range(parallelism)]

        with cluster.cql_connection_patient(cluster.nodes[0]) as session:
            if partition_key:
                token_arg = partition_key
            else:
                pk_rows = sorted(
                    (
                        r
                        for r in session.execute(
                            "SELECT column_name, kind, position FROM system_schema.columns "
                            "WHERE keyspace_name=%s AND table_name=%s",
                            (keyspace, table),
                        )
                        if r.kind == "partition_key"
                    ),
                    key=lambda r: r.position,
                )
                if not pk_rows:
                    raise RuntimeError(f"no partition key columns found for {keyspace}.{table}")
                token_arg = ", ".join(r.column_name for r in pk_rows)

            def count_range(args):
                low, high, inclusive_low = args
                op = ">=" if inclusive_low else ">"
                cql = (
                    f"SELECT count(*) FROM {keyspace}.{table} "
                    f"WHERE token({token_arg}) {op} {low} AND token({token_arg}) <= {high}"
                )
                return session.execute(
                    SimpleStatement(cql, consistency_level=ConsistencyLevel.ONE),
                    timeout=per_query_timeout,
                ).one()[0]

            with ThreadPoolExecutor(max_workers=parallelism) as pool:
                total = sum(pool.map(count_range, ranges))

        self.log.info("Parallel count over %s.%s with %d ranges → %d rows", keyspace, table, parallelism, total)
        return total

    @staticmethod
    def _make_create_idempotent(cql_stmt: str) -> str:
        """Rewrite leading `CREATE TABLE|TYPE|INDEX` to `CREATE … IF NOT EXISTS` so the apply
        loop is safe to re-run under SCT_REUSE_CLUSTER. No-op if the statement already contains
        IF NOT EXISTS or does not start with one of the targeted DDL verbs."""
        return re.sub(
            r"^(CREATE\s+(?:TABLE|TYPE|INDEX))(?!\s+IF\s+NOT\s+EXISTS)\b",
            r"\1 IF NOT EXISTS",
            cql_stmt,
            count=1,
            flags=re.IGNORECASE,
        )

    def _build_cs_to_scylla_migrator_config(
        self,
        source_keyspace: str,
        source_table: str,
        target_keyspace: str,
        target_table: str,
    ) -> MigratorConfig:
        """Build a MigratorConfig for cs_db_cluster → db_cluster migration."""
        return MigratorConfig(
            source_type="cassandra",
            source_hosts=[n.private_ip_address for n in self.cs_db_cluster.nodes],
            source_port=BaseNode.CQL_PORT,
            source_keyspace=source_keyspace,
            source_table=source_table,
            target_host=self.db_cluster.nodes[0].private_ip_address,
            target_port=BaseNode.CQL_PORT,
            target_keyspace=target_keyspace,
            target_table=target_table,
        )

    def _validate_migration_count_and_sample(
        self,
        source_cluster,
        source_keyspace: str,
        source_table: str,
        target_keyspace: str,
        target_table: str,
        expected_row_count: int,
        sample_size: int = _SAMPLE_SIZE,
        parallelism: int = 16,
    ):
        """Validate migration: target count == expected, plus optional row sampling.

        Target count uses _count_rows_parallel (token-range sharded) so 100M+ row
        tables finish in ~1–2 min instead of timing out a single coordinator.
        """
        self.log.info(
            "Validating migration %s.%s → %s.%s",
            source_keyspace,
            source_table,
            target_keyspace,
            target_table,
        )

        tgt_count = self._count_rows_parallel(self.db_cluster, target_keyspace, target_table, parallelism=parallelism)
        assert tgt_count == expected_row_count, (
            f"row count mismatch on target {target_keyspace}.{target_table}: "
            f"expected {expected_row_count}, got {tgt_count}"
        )
        self.log.info("Target row count matches expected: %d rows", tgt_count)

        if sample_size <= 0:
            return

        sample_ids = random.sample(range(expected_row_count), sample_size)
        with source_cluster.cql_connection_patient(source_cluster.nodes[0]) as src_session:
            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as tgt_session:
                for row_id in sample_ids:
                    src_row = src_session.execute(
                        f"SELECT id, data, value FROM {source_keyspace}.{source_table} WHERE id = %s",
                        (row_id,),
                    ).one()
                    tgt_row = tgt_session.execute(
                        f"SELECT id, data, value FROM {target_keyspace}.{target_table} WHERE id = %s",
                        (row_id,),
                    ).one()
                    assert src_row is not None, (
                        f"source row missing for id={row_id} in {source_keyspace}.{source_table}"
                    )
                    assert tgt_row is not None, (
                        f"target row missing for id={row_id} in {target_keyspace}.{target_table}"
                    )
                    assert src_row == tgt_row, f"row mismatch for id={row_id}: source={src_row} target={tgt_row}"

        self.log.info("Sample validation passed: %d/%d rows match", sample_size, sample_size)
