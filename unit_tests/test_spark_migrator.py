"""Unit tests for spark migrator job submission module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws

from sdcm.spark_migrator import (
    MigratorConfig,
    SparkMigratorRunner,
    get_migrator_download_url,
    upload_migrator_jar_to_s3,
)


def test_migrator_config_defaults():
    """Test MigratorConfig has sensible defaults."""
    config = MigratorConfig()
    assert config.source_port == 9042
    assert config.target_port == 9042
    assert config.spark_executor_memory == "4g"
    assert config.spark_executor_cores == 2
    assert config.spark_parallelism == 200
    assert config.extra_spark_args == []


def test_migrator_config_custom():
    """Test MigratorConfig with custom values."""
    config = MigratorConfig(
        source_host="10.0.0.1",
        source_keyspace="ks1",
        source_table="table1",
        target_host="10.0.0.2",
        target_keyspace="ks2",
        target_table="table2",
        spark_executor_memory="8g",
        spark_executor_cores=4,
    )
    assert config.source_host == "10.0.0.1"
    assert config.target_host == "10.0.0.2"
    assert config.spark_executor_memory == "8g"
    assert config.spark_executor_cores == 4


def test_build_spark_submit_args_basic():
    """Test building spark-submit arguments from config."""
    config = MigratorConfig(
        spark_executor_memory="4g",
        spark_executor_cores=2,
        spark_parallelism=200,
    )
    args = SparkMigratorRunner._build_spark_submit_args(config)
    assert "--conf" in args
    assert "spark.executor.memory=4g" in args
    assert "spark.executor.cores=2" in args
    assert "spark.default.parallelism=200" in args


def test_build_spark_submit_args_with_config_path():
    """Test building spark-submit args with migrator config path."""
    config = MigratorConfig(
        config_path="s3://bucket/config.yaml",
    )
    args = SparkMigratorRunner._build_spark_submit_args(config)
    assert "spark.scylla.config=s3://bucket/config.yaml" in args


def test_build_spark_submit_args_with_extra_args():
    """Test building spark-submit args with extra arguments."""
    config = MigratorConfig(
        extra_spark_args=["--conf", "spark.yarn.maxAppAttempts=1"],
    )
    args = SparkMigratorRunner._build_spark_submit_args(config)
    assert "--conf" in args
    assert "spark.yarn.maxAppAttempts=1" in args


def test_generate_migrator_config_basic():
    """Test generated config has correct source/target structure."""
    config = MigratorConfig(
        source_host="10.0.0.1",
        source_port=9042,
        source_keyspace="ks_source",
        source_table="tbl_source",
        target_host="10.0.0.2",
        target_port=9042,
        target_keyspace="ks_target",
        target_table="tbl_target",
    )
    result = SparkMigratorRunner.generate_migrator_config(config)

    source, target = result["source"], result["target"]
    assert source["host"] == "10.0.0.1"
    assert source["port"] == 9042
    assert source["keyspace"] == "ks_source"
    assert source["table"] == "tbl_source"
    assert target["host"] == "10.0.0.2"
    assert target["port"] == 9042
    assert target["keyspace"] == "ks_target"
    assert target["table"] == "tbl_target"


def test_generate_migrator_config_defaults():
    """Test generated config has sensible default values."""
    config = MigratorConfig(source_host="10.0.0.1", target_host="10.0.0.2")
    result = SparkMigratorRunner.generate_migrator_config(config)

    source, target, savepoints = result["source"], result["target"], result["savepoints"]

    assert source["type"] == "cassandra"
    assert source["consistencyLevel"] == "LOCAL_QUORUM"
    assert source["preserveTimestamps"] is True
    assert source["splitCount"] == 256
    assert source["connections"] == 8
    assert source["fetchSize"] == 1000

    assert target["type"] == "scylla"
    assert target["consistencyLevel"] == "LOCAL_QUORUM"
    assert target["connections"] == 16

    assert savepoints["path"] == "/tmp/savepoints"
    assert savepoints["intervalSeconds"] == 300


@mock_aws
def test_upload_config_to_s3():
    """Test uploading migrator config to S3 returns correct URI."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")

    config_dict = {
        "source": {"type": "cassandra", "host": "10.0.0.1"},
        "target": {"type": "scylla", "host": "10.0.0.2"},
    }

    runner = SparkMigratorRunner(emr_provisioner=None)
    s3_uri = runner.upload_config_to_s3(
        config_dict=config_dict,
        s3_bucket="test-bucket",
        test_id="abc-123",
        region_name="us-east-1",
    )

    assert s3_uri == "s3://test-bucket/configs/abc-123/config.yaml"

    body = s3_client.get_object(Bucket="test-bucket", Key="configs/abc-123/config.yaml")["Body"].read().decode("utf-8")
    assert "cassandra" in body
    assert "10.0.0.1" in body


def test_build_spark_submit_args_includes_class():
    """Test that --class and Migrator class name are in spark-submit args."""
    args = SparkMigratorRunner._build_spark_submit_args(MigratorConfig())
    class_index = args.index("--class")
    assert args[class_index + 1] == "com.scylladb.migrator.Migrator"


def test_get_migrator_download_url_tag():
    """Test that a release tag produces a correct GitHub download URL."""
    url = get_migrator_download_url("v1.1.2")
    assert url == "https://github.com/scylladb/scylla-migrator/releases/download/v1.1.2/scylla-migrator-assembly.jar"


def test_get_migrator_download_url_full_url():
    """Test that a full HTTPS URL is returned unchanged."""
    url = "https://example.com/custom/scylla-migrator-assembly.jar"
    assert get_migrator_download_url(url) == url


@mock_aws
def test_upload_migrator_jar_to_s3_already_exists():
    """Test that upload is skipped when JAR already exists in S3."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-jar-bucket")
    s3_client.put_object(
        Bucket="test-jar-bucket", Key="jars/v1.1.2/scylla-migrator-assembly.jar", Body=b"fake-jar-content"
    )

    with patch("sdcm.spark_migrator.requests.get") as mock_get:
        result = upload_migrator_jar_to_s3(release_tag="v1.1.2", s3_bucket="test-jar-bucket", region_name="us-east-1")
        mock_get.assert_not_called()

    assert result == "s3://test-jar-bucket/jars/v1.1.2/scylla-migrator-assembly.jar"


@mock_aws
def test_upload_migrator_jar_to_s3_downloads_and_uploads():
    """Test that JAR is downloaded from GitHub and uploaded to S3."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-jar-bucket")

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"fake-jar-bytes"]

    with patch("sdcm.spark_migrator.requests.get", return_value=mock_response) as mock_get:
        result = upload_migrator_jar_to_s3(release_tag="v1.1.2", s3_bucket="test-jar-bucket", region_name="us-east-1")
        assert mock_get.call_count == 1
        call_url = mock_get.call_args[0][0]
        assert "v1.1.2" in call_url
        assert "scylla-migrator-assembly.jar" in call_url

    assert result == "s3://test-jar-bucket/jars/v1.1.2/scylla-migrator-assembly.jar"

    body = s3_client.get_object(Bucket="test-jar-bucket", Key="jars/v1.1.2/scylla-migrator-assembly.jar")["Body"].read()
    assert body == b"fake-jar-bytes"
