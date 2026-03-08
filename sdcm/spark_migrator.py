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

"""Spark migrator job submission and monitoring for EMR clusters."""

import logging
import time
from dataclasses import dataclass, field

LOGGER = logging.getLogger(__name__)


@dataclass
class MigratorConfig:
    """Configuration for a spark-migrator job.

    Args:
        source_host: Source database host (Scylla/Cassandra endpoint).
        source_port: Source database CQL port.
        source_keyspace: Source keyspace name.
        source_table: Source table name.
        target_host: Target database host (Scylla endpoint).
        target_port: Target database CQL port.
        target_keyspace: Target keyspace name.
        target_table: Target table name.
        config_path: S3 path to migrator config YAML (alternative to individual fields).
        spark_executor_memory: Spark executor memory (e.g., '4g').
        spark_executor_cores: Number of Spark executor cores.
        spark_parallelism: Spark default parallelism.
        extra_spark_args: Additional spark-submit arguments.
    """

    source_host: str = ""
    source_port: int = 9042
    source_keyspace: str = ""
    source_table: str = ""
    target_host: str = ""
    target_port: int = 9042
    target_keyspace: str = ""
    target_table: str = ""
    config_path: str = ""
    spark_executor_memory: str = "4g"
    spark_executor_cores: int = 2
    spark_parallelism: int = 200
    extra_spark_args: list = field(default_factory=list)


class SparkMigratorRunner:
    """Submits and monitors spark-migrator jobs on EMR clusters.

    Args:
        emr_provisioner: An EmrClusterProvisioner instance managing the EMR cluster.
    """

    def __init__(self, emr_provisioner):
        self.emr_provisioner = emr_provisioner

    def submit_migration_job(self, cluster_id, jar_path, migrator_config):
        """Submit a spark-migrator job as an EMR step.

        Args:
            cluster_id: EMR cluster ID.
            jar_path: S3 path to the spark-migrator JAR.
            migrator_config: MigratorConfig with job parameters.

        Returns:
            str: Step ID of the submitted job.
        """
        args = self._build_spark_submit_args(migrator_config)
        step_id = self.emr_provisioner.add_step(
            cluster_id=cluster_id,
            step_name="spark-migrator",
            jar_path=jar_path,
            args=args,
        )
        LOGGER.info("Spark migrator job submitted as step %s on cluster %s", step_id, cluster_id)
        return step_id

    def wait_for_migration(self, cluster_id, step_id):
        """Wait for a migration job to complete.

        Args:
            cluster_id: EMR cluster ID.
            step_id: Step ID of the migration job.

        Returns:
            dict: Step status after completion.
        """
        return self.emr_provisioner.wait_for_step_completion(cluster_id, step_id)

    def run_migration(self, cluster_id, jar_path, migrator_config):
        """Submit and wait for a migration job to complete.

        Args:
            cluster_id: EMR cluster ID.
            jar_path: S3 path to the spark-migrator JAR.
            migrator_config: MigratorConfig with job parameters.

        Returns:
            dict: Migration result with step_id, status, and duration.
        """
        start_time = time.time()
        step_id = self.submit_migration_job(cluster_id, jar_path, migrator_config)
        status = self.wait_for_migration(cluster_id, step_id)
        duration = time.time() - start_time

        LOGGER.info("Migration completed in %.1f seconds", duration)
        return {
            "step_id": step_id,
            "status": status,
            "duration_seconds": duration,
        }

    def get_step_logs(self, cluster_id, step_id):
        """Retrieve step logs from the EMR cluster.

        Args:
            cluster_id: EMR cluster ID.
            step_id: Step ID.

        Returns:
            dict: Step status information including any failure details.
        """
        return self.emr_provisioner.get_step_status(cluster_id, step_id)

    @staticmethod
    def _build_spark_submit_args(migrator_config):
        """Build spark-submit arguments from migrator configuration.

        Args:
            migrator_config: MigratorConfig with job parameters.

        Returns:
            list: List of spark-submit argument strings.
        """
        args = [
            "--conf",
            f"spark.executor.memory={migrator_config.spark_executor_memory}",
            "--conf",
            f"spark.executor.cores={migrator_config.spark_executor_cores}",
            "--conf",
            f"spark.default.parallelism={migrator_config.spark_parallelism}",
        ]

        if migrator_config.config_path:
            args.extend(["--conf", f"spark.scylla.config={migrator_config.config_path}"])

        args.extend(migrator_config.extra_spark_args)
        return args
