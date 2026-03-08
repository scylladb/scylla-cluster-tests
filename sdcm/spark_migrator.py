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
import os
import tempfile
import time
from dataclasses import dataclass, field

import boto3
import requests
import yaml
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)

MIGRATOR_GITHUB_REPO = "scylladb/scylla-migrator"
MIGRATOR_JAR_NAME = "scylla-migrator-assembly.jar"


def _ensure_s3_bucket(s3_client, bucket_name, region_name):
    """Create S3 bucket if it does not already exist.

    Args:
        s3_client: boto3 S3 client.
        bucket_name: Name of the bucket to ensure.
        region_name: AWS region for the bucket.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return  # bucket already exists
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise

    LOGGER.info("Creating S3 bucket %s in %s", bucket_name, region_name)
    create_params = {"Bucket": bucket_name}
    if region_name != "us-east-1":
        create_params["CreateBucketConfiguration"] = {"LocationConstraint": region_name}
    s3_client.create_bucket(**create_params)


def get_migrator_download_url(release_tag):
    """Build GitHub release asset URL for scylla-migrator JAR.

    Args:
        release_tag: GitHub release tag (e.g., 'v1.1.2') or full HTTPS URL.

    Returns:
        str: Download URL for the JAR asset.
    """
    if release_tag.startswith("https://"):
        return release_tag
    return f"https://github.com/{MIGRATOR_GITHUB_REPO}/releases/download/{release_tag}/{MIGRATOR_JAR_NAME}"


def upload_migrator_jar_to_s3(release_tag, s3_bucket, region_name, s3_prefix="jars"):
    """Download migrator JAR from GitHub and upload to S3 if not already present.

    Args:
        release_tag: GitHub release tag or direct URL.
        s3_bucket: Target S3 bucket name.
        region_name: AWS region for S3 client.
        s3_prefix: S3 key prefix (default: 'jars').

    Returns:
        str: S3 URI of the uploaded JAR (s3://bucket/prefix/tag/scylla-migrator-assembly.jar).
    """
    s3_key = f"{s3_prefix}/{release_tag}/{MIGRATOR_JAR_NAME}"
    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    s3_client = boto3.client("s3", region_name=region_name)
    _ensure_s3_bucket(s3_client, s3_bucket, region_name)

    # check if the JAR already exists in S3
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        LOGGER.info("Migrator JAR already exists at %s, skipping upload", s3_uri)
        return s3_uri
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "404":
            raise

    download_url = get_migrator_download_url(release_tag)
    LOGGER.info("Downloading migrator JAR from %s", download_url)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, MIGRATOR_JAR_NAME)
        response = requests.get(download_url, stream=True, timeout=300)
        response.raise_for_status()
        with open(local_path, "wb") as jar_file:
            jar_file.writelines(response.iter_content(chunk_size=8192))

        LOGGER.info("Uploading migrator JAR to %s", s3_uri)
        s3_client.upload_file(local_path, s3_bucket, s3_key)

    LOGGER.info("Migrator JAR uploaded to %s", s3_uri)
    return s3_uri


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
    def generate_migrator_config(migrator_config):
        """Generate a scylla-migrator configuration dictionary from MigratorConfig.

        Args:
            migrator_config: MigratorConfig with source/target parameters.

        Returns:
            dict: Configuration dictionary matching scylla-migrator's expected YAML format.
        """
        return {
            "source": {
                "type": "cassandra",
                "host": migrator_config.source_host,
                "port": migrator_config.source_port,
                "keyspace": migrator_config.source_keyspace,
                "table": migrator_config.source_table,
                "consistencyLevel": "LOCAL_QUORUM",
                "preserveTimestamps": True,
                "splitCount": 256,
                "connections": 8,
                "fetchSize": 1000,
            },
            "target": {
                "type": "scylla",
                "host": migrator_config.target_host,
                "port": migrator_config.target_port,
                "keyspace": migrator_config.target_keyspace,
                "table": migrator_config.target_table,
                "consistencyLevel": "LOCAL_QUORUM",
                "connections": 16,
            },
            "savepoints": {
                "path": "/tmp/savepoints",
                "intervalSeconds": 300,
            },
        }

    def upload_config_to_s3(self, config_dict, s3_bucket, test_id, region_name):
        """Upload a migrator config dictionary as YAML to S3.

        Args:
            config_dict: Configuration dictionary to serialize.
            s3_bucket: Target S3 bucket name.
            test_id: Test ID used to namespace the config file.
            region_name: AWS region for S3 client.

        Returns:
            str: S3 URI of the uploaded config file.
        """
        s3_key = f"configs/{test_id}/config.yaml"
        s3_uri = f"s3://{s3_bucket}/{s3_key}"

        s3_client = boto3.client("s3", region_name=region_name)
        _ensure_s3_bucket(s3_client, s3_bucket, region_name)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=yaml.dump(config_dict, default_flow_style=False).encode("utf-8"),
        )

        LOGGER.info("Migrator config uploaded to %s", s3_uri)
        return s3_uri

    @staticmethod
    def _build_spark_submit_args(migrator_config):
        """Build spark-submit arguments from migrator configuration.

        Args:
            migrator_config: MigratorConfig with job parameters.

        Returns:
            list: List of spark-submit argument strings.
        """
        args = [
            "--class",
            "com.scylladb.migrator.Migrator",
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
