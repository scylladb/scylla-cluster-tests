"""Unit tests for spark migrator job submission module."""

from __future__ import annotations

from sdcm.spark_migrator import MigratorConfig, SparkMigratorRunner


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
