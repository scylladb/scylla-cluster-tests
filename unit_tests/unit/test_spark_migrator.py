"""Unit tests for spark migrator job submission module."""

from __future__ import annotations

import gzip
import logging
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from sdcm.spark_migrator import (
    SPARK4_INSTALL_DIR,
    SPARK4_VERSION,
    MigratorConfig,
    SparkMigratorRunner,
    build_spark4_bootstrap_actions,
    discover_cs_source_hosts,
    get_migrator_download_url,
    upload_migrator_jar_to_s3,
    upload_spark4_tarball_to_s3,
)
import spark_migrator_test


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
        source_hosts=["10.0.0.1"],
        source_keyspace="ks1",
        source_table="table1",
        target_host="10.0.0.2",
        target_keyspace="ks2",
        target_table="table2",
        spark_executor_memory="8g",
        spark_executor_cores=4,
    )
    assert config.source_hosts == ["10.0.0.1"]
    assert config.target_host == "10.0.0.2"
    assert config.spark_executor_memory == "8g"
    assert config.spark_executor_cores == 4


def test_generate_migrator_config_basic():
    """Test generated config has correct source/target structure."""
    config = MigratorConfig(
        source_hosts=["10.0.0.1"],
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
    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
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


def test_generate_migrator_config_has_strip_trailing_zeros():
    """Test generated config includes stripTrailingZerosForDecimals in source and target."""
    result = SparkMigratorRunner.generate_migrator_config(
        MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    )

    assert result["source"]["stripTrailingZerosForDecimals"] is False
    assert result["target"]["stripTrailingZerosForDecimals"] is False


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


def test_get_migrator_download_url_tag():
    """Test that a release tag produces a correct GitHub download URL."""
    url = get_migrator_download_url("v2.0.0")
    assert url == "https://github.com/scylladb/scylla-migrator/releases/download/v2.0.0/scylla-migrator-assembly.jar"


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
        Bucket="test-jar-bucket", Key="jars/v2.0.0/scylla-migrator-assembly.jar", Body=b"fake-jar-content"
    )

    with patch("sdcm.spark_migrator.requests.get") as mock_get:
        result = upload_migrator_jar_to_s3(release_tag="v2.0.0", s3_bucket="test-jar-bucket", region_name="us-east-1")
        mock_get.assert_not_called()

    assert result == "s3://test-jar-bucket/jars/v2.0.0/scylla-migrator-assembly.jar"


@mock_aws
def test_upload_migrator_jar_to_s3_downloads_and_uploads():
    """Test that JAR is downloaded from GitHub and uploaded to S3."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-jar-bucket")

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"fake-jar-bytes"]

    with patch("sdcm.spark_migrator.requests.get", return_value=mock_response) as mock_get:
        result = upload_migrator_jar_to_s3(release_tag="v2.0.0", s3_bucket="test-jar-bucket", region_name="us-east-1")
        assert mock_get.call_count == 1
        call_url = mock_get.call_args[0][0]
        assert "v2.0.0" in call_url
        assert "scylla-migrator-assembly.jar" in call_url

    assert result == "s3://test-jar-bucket/jars/v2.0.0/scylla-migrator-assembly.jar"

    body = s3_client.get_object(Bucket="test-jar-bucket", Key="jars/v2.0.0/scylla-migrator-assembly.jar")["Body"].read()
    assert body == b"fake-jar-bytes"


# TODO: Spark 4.x workaround
@mock_aws
def test_build_spark4_bootstrap_actions():
    """Bootstrap actions are built, the Spark tarball is mirrored to S3, and the script pulls it via aws s3 cp."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"fake-spark-tarball"]
    with patch("sdcm.spark_migrator.requests.get", return_value=mock_response):
        actions = build_spark4_bootstrap_actions("us-east-1")

    assert len(actions) == 1
    assert actions[0]["Name"] == f"Install Spark {SPARK4_VERSION}"
    assert actions[0]["ScriptBootstrapAction"]["Path"].startswith("s3://")

    body = (
        s3_client.get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key="scripts/bootstrap-spark4.sh")["Body"]
        .read()
        .decode("utf-8")
    )
    assert SPARK4_VERSION in body
    assert SPARK4_INSTALL_DIR in body
    assert "aws s3 cp" in body
    assert "wget" not in body
    assert "downloads.apache.org" not in body

    tarball = s3_client.get_object(
        Bucket="sct-emr-spark-migrator-us-east-1", Key=f"spark/spark-{SPARK4_VERSION}-bin-hadoop3.tgz"
    )["Body"].read()
    assert tarball == b"fake-spark-tarball"


# TODO: Spark 4.x workaround
@mock_aws
def test_upload_spark4_tarball_to_s3_downloads_from_archive_and_uploads():
    """Spark tarball is downloaded from the Apache archive (not the live mirror) and uploaded to S3 when absent."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"spark-tarball-bytes"]
    with patch("sdcm.spark_migrator.requests.get", return_value=mock_response) as mock_get:
        uri = upload_spark4_tarball_to_s3("sct-emr-spark-migrator-us-east-1", "us-east-1")
        assert mock_get.call_count == 1
        call_url = mock_get.call_args[0][0]
        assert "archive.apache.org" in call_url
        assert SPARK4_VERSION in call_url

    key = f"spark/spark-{SPARK4_VERSION}-bin-hadoop3.tgz"
    assert uri == f"s3://sct-emr-spark-migrator-us-east-1/{key}"
    body = s3_client.get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key=key)["Body"].read()
    assert body == b"spark-tarball-bytes"


@mock_aws
def test_upload_runner_script():
    """Test that the runner script is uploaded with correct spark-submit invocation."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    mock_provisioner = MagicMock()
    mock_provisioner.region_name = "us-east-1"

    runner = SparkMigratorRunner(mock_provisioner)
    uri = runner._upload_runner_script(
        "s3://bucket/jars/v2.0.0/migrator.jar",
        "s3://bucket/configs/test-id/config.yaml",
    )

    assert uri.startswith("s3://")
    body = (
        s3_client.get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key="scripts/run-migrator.sh")["Body"]
        .read()
        .decode("utf-8")
    )
    assert SPARK4_INSTALL_DIR in body
    assert "spark-submit" in body
    assert "--deploy-mode cluster" in body
    assert "s3://bucket/jars/v2.0.0/migrator.jar" in body
    assert "s3://bucket/configs/test-id/config.yaml" in body


@mock_aws
def test_upload_runner_script_cds_to_tmp_so_client_mode_finds_relative_config():
    """In client deploy mode the driver JVM inherits the wrapper script's CWD;
    spark.scylla.config is a relative path, so the script must cd into /tmp
    (where it just downloaded migrator-config.yaml) before invoking spark-submit.
    Without cd, validator main fails with FileNotFoundException: migrator-config.yaml."""
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    runner = SparkMigratorRunner(MagicMock(region_name="us-east-1"))
    runner._upload_runner_script(
        "s3://bucket/jars/v2.0.0/migrator.jar",
        "s3://bucket/configs/test-id/config.yaml",
    )

    body = (
        boto3.client("s3", region_name="us-east-1")
        .get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key="scripts/run-migrator.sh")["Body"]
        .read()
        .decode("utf-8")
    )
    cd_pos = body.find("cd /tmp")
    submit_pos = body.find("spark-submit")
    assert cd_pos != -1, "wrapper script must cd into /tmp before spark-submit"
    assert submit_pos != -1
    assert cd_pos < submit_pos, "cd /tmp must precede spark-submit so driver CWD has the config"


@pytest.mark.parametrize("source_type", ["cassandra", "scylla"])
def test_generate_migrator_config_source_and_target_types(source_type):
    """Test that source.type reflects the configured source_type; target.type is always 'scylla'."""
    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2", source_type=source_type)
    result = SparkMigratorRunner.generate_migrator_config(config)
    assert result["source"]["type"] == source_type
    assert result["target"]["type"] == "scylla"


def test_generate_migrator_config_picks_single_source_host():
    """Test that only the first source host is used; the driver discovers the rest via gossip."""
    config = MigratorConfig(source_hosts=["10.0.0.1", "10.0.0.2", "10.0.0.3"], target_host="10.0.0.4")
    assert SparkMigratorRunner.generate_migrator_config(config)["source"]["host"] == "10.0.0.1"


def _make_discover_mock(instances):
    """Mock EC2 client returning the given instance list."""
    mock_client = MagicMock()
    mock_client.describe_instances.return_value = {"Reservations": [{"Instances": instances}]}
    return mock_client


def test_discover_cs_source_hosts_returns_public_ips():
    """Test that discover_cs_source_hosts returns public IPs from the EC2 response."""
    test_id = "aaaa-bbbb-cccc"
    instances = [
        {"PublicIpAddress": "54.1.2.3", "InstanceId": "i-001"},
        {"PublicIpAddress": "54.4.5.6", "InstanceId": "i-002"},
    ]
    mock_client = _make_discover_mock(instances)

    with patch("sdcm.spark_migrator.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_client
        hosts = discover_cs_source_hosts(test_id, "eu-west-1")

    assert hosts == ["54.1.2.3", "54.4.5.6"]
    mock_client.describe_instances.assert_called_once()
    filters = {f["Name"]: f["Values"] for f in mock_client.describe_instances.call_args[1]["Filters"]}
    assert filters["tag:TestId"] == [test_id]
    assert filters["tag:NodeType"] == ["cs-db"]
    assert filters["instance-state-name"] == ["running"]


def test_discover_cs_source_hosts_falls_back_to_private_ip():
    """Test that discover_cs_source_hosts prefers public IPs but falls back to private when absent."""
    instances = [
        {"PublicIpAddress": "54.1.2.3", "PrivateIpAddress": "10.0.0.1", "InstanceId": "i-001"},
        {"PrivateIpAddress": "10.0.0.2", "InstanceId": "i-002"},  # private-only
    ]
    mock_client = _make_discover_mock(instances)

    with patch("sdcm.spark_migrator.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_client
        hosts = discover_cs_source_hosts("aaaa-bbbb-cccc", "eu-west-1")

    assert hosts == ["54.1.2.3", "10.0.0.2"]


def _make_tester():
    """Instantiate SparkMigratorTest without triggering ClusterTester.__init__."""
    tester = spark_migrator_test.SparkMigratorTest.__new__(spark_migrator_test.SparkMigratorTest)
    tester.log = logging.getLogger("test")
    tester.params = MagicMock()
    tester.params.get.return_value = None
    return tester


def test_build_cs_to_scylla_migrator_config_source_target_selection():
    """_build_cs_to_scylla_migrator_config uses cs_db_cluster for source and db_cluster for target."""
    tester = _make_tester()

    src_node = MagicMock(private_ip_address="10.0.0.1")
    tester.cs_db_cluster = MagicMock(nodes=[src_node])

    tgt_node = MagicMock(private_ip_address="10.0.0.2")
    tester.db_cluster = MagicMock(nodes=[tgt_node])

    config = tester._build_cs_to_scylla_migrator_config("ks_src", "tbl_src", "ks_tgt", "tbl_tgt")

    assert config.source_type == "cassandra"
    assert config.source_hosts == ["10.0.0.1"]
    assert config.target_host == "10.0.0.2"
    assert config.source_keyspace == "ks_src"
    assert config.source_table == "tbl_src"
    assert config.target_keyspace == "ks_tgt"
    assert config.target_table == "tbl_tgt"
    assert config.source_port == 9042
    assert config.target_port == 9042


@pytest.mark.parametrize(
    "stmt,expected",
    [
        (
            "CREATE TABLE ks.t (id int PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS ks.t (id int PRIMARY KEY)",
        ),
        (
            "CREATE TYPE ks.addr (street text)",
            "CREATE TYPE IF NOT EXISTS ks.addr (street text)",
        ),
        (
            "CREATE INDEX idx_data ON ks.t (data)",
            "CREATE INDEX IF NOT EXISTS idx_data ON ks.t (data)",
        ),
    ],
)
def test_make_create_idempotent_inserts_if_not_exists(stmt, expected):
    """CREATE TABLE/TYPE/INDEX all get IF NOT EXISTS inserted after the keyword."""
    assert spark_migrator_test.SparkMigratorTest._make_create_idempotent(stmt) == expected


def test_make_create_idempotent_keyspace_unchanged():
    """CREATE KEYSPACE is not rewritten — keyspace pre-creation is handled separately."""
    stmt = "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    assert spark_migrator_test.SparkMigratorTest._make_create_idempotent(stmt) == stmt


def test_assert_region_coherence_raises_on_mismatch():
    """Test that error is raised when EMR is in a different region than the rest of the test setup."""
    tester = _make_tester()
    tester.emr_cluster = MagicMock(region_name="us-east-1")
    tester.params = MagicMock(region_names=["eu-west-1"])
    with pytest.raises(RuntimeError, match="EMR cluster region 'us-east-1' != test region 'eu-west-1'"):
        tester._assert_region_coherence()


@mock_aws
def test_submit_validator_job_uploads_validator_script_with_validator_main_class():
    """Test that submit_validator_job uploads a separate wrapper script invoking Validator main class."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    mock_provisioner = MagicMock(region_name="us-east-1", params={"emr_install_spark4_via_bootstrap": True})
    mock_provisioner.add_step.return_value = "s-VALIDATOR123"

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    config.config_path = "s3://bucket/configs/test-id/config.yaml"

    runner = SparkMigratorRunner(mock_provisioner)
    step_id = runner.submit_validator_job(
        cluster_id="j-CLUSTER",
        jar_s3_path="s3://bucket/jars/v2.0.1/scylla-migrator-assembly.jar",
        migrator_config=config,
    )

    assert step_id == "s-VALIDATOR123"

    body = (
        s3_client.get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key="scripts/run-validator.sh")["Body"]
        .read()
        .decode("utf-8")
    )
    assert "com.scylladb.migrator.Validator" in body
    assert "com.scylladb.migrator.Migrator" not in body  # regression guard
    assert "spark-submit" in body
    assert "s3://bucket/jars/v2.0.1/scylla-migrator-assembly.jar" in body


@mock_aws
def test_submit_validator_job_uses_distinct_step_name_and_script_runner():
    """Test that submit_validator_job names the EMR step distinctly + uses script-runner.jar."""
    boto3.client("s3", region_name="eu-west-1").create_bucket(
        Bucket="sct-emr-spark-migrator-eu-west-1",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    mock_provisioner = MagicMock(region_name="eu-west-1", params={"emr_install_spark4_via_bootstrap": True})
    mock_provisioner.add_step.return_value = "s-VAL"

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    config.config_path = "s3://bucket/configs/test-id/config.yaml"

    SparkMigratorRunner(mock_provisioner).submit_validator_job("j-CLUSTER", "s3://bucket/jars/x/migrator.jar", config)

    add_step_kwargs = mock_provisioner.add_step.call_args[1]
    assert add_step_kwargs["step_name"] == "spark-migrator-validator"
    assert "script-runner.jar" in add_step_kwargs["jar"]
    assert "eu-west-1.elasticmapreduce" in add_step_kwargs["jar"]
    assert add_step_kwargs["args"][0].endswith("/scripts/run-validator.sh")


@mock_aws
def test_submit_migration_job_still_uses_cluster_deploy_mode():
    """Migration job remains in cluster deploy mode (default); deploy_mode refactor must not regress it."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="sct-emr-spark-migrator-us-east-1")

    mock_provisioner = MagicMock(
        region_name="us-east-1", params={"emr_install_spark4_via_bootstrap": True}, **{"add_step.return_value": "s-MIG"}
    )

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    config.config_path = "s3://bucket/configs/test-id/config.yaml"

    SparkMigratorRunner(mock_provisioner).submit_migration_job("j-CLUSTER", "s3://bucket/jars/x/migrator.jar", config)

    body = (
        s3_client.get_object(Bucket="sct-emr-spark-migrator-us-east-1", Key="scripts/run-migrator.sh")["Body"]
        .read()
        .decode("utf-8")
    )
    assert "--deploy-mode cluster" in body


def _build_native_runner_and_config(step_return_value):
    provisioner = MagicMock(region_name="us-east-1", params={"emr_install_spark4_via_bootstrap": False})
    provisioner.add_step.return_value = step_return_value

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    config.config_path = "s3://bucket/configs/test-id/config.yaml"

    return SparkMigratorRunner(provisioner), provisioner, config


def test_submit_migration_native_uses_command_runner_with_s3_files():
    """Native migration (cluster mode): command-runner.jar spark-submit, config read via --files from S3.

    In cluster mode YARN localizes --files into the driver container CWD, so a relative
    spark.scylla.config basename resolves — no local staging needed.
    """
    runner, provisioner, config = _build_native_runner_and_config("s-MIG")

    with patch.object(SparkMigratorRunner, "_upload_runner_script") as mock_upload:
        step_id = runner.submit_migration_job("j-CLUSTER", "s3://bucket/jars/x/migrator.jar", config)

    assert step_id == "s-MIG"
    mock_upload.assert_not_called()  # native path uploads no /opt/spark4 wrapper

    kwargs = provisioner.add_step.call_args[1]
    assert kwargs["step_name"] == "spark-migrator"
    assert kwargs["jar"] == "command-runner.jar"
    assert kwargs["args"] == [
        "spark-submit",
        "--deploy-mode",
        "cluster",
        "--master",
        "yarn",
        "--class",
        "com.scylladb.migrator.Migrator",
        "--conf",
        "spark.scylla.config=config.yaml",
        "--files",
        "s3://bucket/configs/test-id/config.yaml",
        "s3://bucket/jars/x/migrator.jar",
    ]


def test_submit_validator_native_stages_config_locally_for_client_mode():
    """Native validator (client mode) must stage config to /tmp and use local spark.scylla.config path."""
    runner, provisioner, config = _build_native_runner_and_config("s-VAL")
    jar_s3_path = "s3://bucket/jars/x/migrator.jar"

    expected_args = [
        "bash",
        "-c",
        "aws s3 cp s3://bucket/configs/test-id/config.yaml /tmp/config.yaml && "
        "spark-submit --deploy-mode client --master yarn "
        "--class com.scylladb.migrator.Validator "
        "--conf spark.scylla.config=/tmp/config.yaml "
        "--files /tmp/config.yaml s3://bucket/jars/x/migrator.jar",
    ]

    with patch.object(SparkMigratorRunner, "_upload_runner_script") as mock_upload:
        step_id = runner.submit_validator_job("j-CLUSTER", jar_s3_path, config)

    assert step_id == "s-VAL"
    mock_upload.assert_not_called()

    add_step_kwargs = provisioner.add_step.call_args.kwargs
    assert add_step_kwargs["step_name"] == "spark-migrator-validator"
    assert add_step_kwargs["jar"] == "command-runner.jar"
    assert add_step_kwargs["args"] == expected_args


@mock_aws
def test_get_step_stdout_concatenates_stdout_and_stderr():
    """stdout.gz + stderr.gz are downloaded, gunzipped, decoded, and concatenated."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="emr-logs")

    key_prefix = "logs/j-CLUSTER/steps/s-STEP"
    s3_client.put_object(Bucket="emr-logs", Key=f"{key_prefix}/stdout.gz", Body=gzip.compress(b"out-line\n"))
    s3_client.put_object(Bucket="emr-logs", Key=f"{key_prefix}/stderr.gz", Body=gzip.compress(b"err-line\n"))

    mock_provisioner = MagicMock()
    mock_provisioner.region_name = "us-east-1"
    mock_provisioner.emr_client.describe_cluster.return_value = {"Cluster": {"LogUri": "s3://emr-logs/logs/"}}

    text = SparkMigratorRunner(mock_provisioner).get_step_stdout("j-CLUSTER", "s-STEP")

    assert "out-line" in text
    assert "err-line" in text


@mock_aws
def test_get_step_stdout_accepts_s3n_log_uri_scheme():
    """EMR rewrites s3:// LogUri to s3n:// (legacy Hadoop S3 protocol); accept both."""
    cluster_id, step_id = "j-CLUSTER", "s-STEP"

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="emr-logs")
    s3_client.put_object(
        Bucket="emr-logs",
        Key=f"emr-logs/{cluster_id}/steps/{step_id}/stderr.gz",
        Body=gzip.compress(b"validator-line\n"),
    )

    mock_provisioner = MagicMock(region_name="us-east-1")
    mock_provisioner.emr_client.describe_cluster.return_value = {"Cluster": {"LogUri": "s3n://emr-logs/emr-logs/"}}

    assert "validator-line" in SparkMigratorRunner(mock_provisioner).get_step_stdout(cluster_id, step_id)


@mock_aws
def test_get_step_stdout_skips_missing_log_files():
    """NoSuchKey on either stdout.gz or stderr.gz is silently skipped."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="emr-logs")
    s3_client.put_object(
        Bucket="emr-logs",
        Key="logs/j-CLUSTER/steps/s-STEP/stderr.gz",  # only stderr present
        Body=gzip.compress(b"err-only\n"),
    )

    mock_provisioner = MagicMock()
    mock_provisioner.region_name = "us-east-1"
    mock_provisioner.emr_client.describe_cluster.return_value = {"Cluster": {"LogUri": "s3://emr-logs/logs/"}}

    assert "err-only" in SparkMigratorRunner(mock_provisioner).get_step_stdout("j-CLUSTER", "s-STEP")


def test_run_validator_passes_on_completed_step():
    """COMPLETED EMR step → _run_validator returns silently, no log fetch."""
    tester = _make_tester()
    tester.emr_cluster = MagicMock(cluster_id="j-CLUSTER")

    runner = MagicMock()
    runner.submit_validator_job.return_value = "s-VAL"

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
    config.config_path = "s3://bucket/cfg.yaml"

    tester._run_validator(runner, "s3://bucket/jar", config)

    runner.submit_validator_job.assert_called_once_with(
        cluster_id="j-CLUSTER", jar_s3_path="s3://bucket/jar", migrator_config=config
    )
    runner.wait_for_migration.assert_called_once_with("j-CLUSTER", "s-VAL", timeout_minutes=60)
    runner.get_step_stdout.assert_not_called()


def test_run_validator_raises_when_step_failed():
    """FAILED EMR step (validator System.exit(1)) → fetch stdout, dump, raise."""
    tester = _make_tester()
    tester.emr_cluster = MagicMock(cluster_id="j-CLUSTER")

    runner = MagicMock()
    runner.submit_validator_job.return_value = "s-VAL"
    runner.wait_for_migration.side_effect = AssertionError("EMR step s-VAL failed with state: FAILED. Reason: unknown")
    runner.get_step_stdout.return_value = (
        "ERROR Migrator: Found the following comparison failures:\nRowComparisonFailure: Row(id=42) value differs\n"
    )

    config = MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")

    with pytest.raises(AssertionError, match="validator step FAILED"):
        tester._run_validator(runner, "s3://bucket/jar", config)
    runner.get_step_stdout.assert_called_once_with("j-CLUSTER", "s-VAL")


def test_run_validator_raises_when_step_failed_and_log_fetch_raises():
    """Log-fetch failure on a FAILED step still raises with the FAILED message."""
    tester = _make_tester()
    tester.emr_cluster = MagicMock(cluster_id="j-CLUSTER")

    runner = MagicMock()
    runner.submit_validator_job.return_value = "s-VAL"
    runner.wait_for_migration.side_effect = AssertionError("EMR step s-VAL failed")
    runner.get_step_stdout.side_effect = RuntimeError("S3 access denied")

    with pytest.raises(AssertionError, match="validator step FAILED"):
        tester._run_validator(
            runner, "s3://bucket/jar", MigratorConfig(source_hosts=["10.0.0.1"], target_host="10.0.0.2")
        )
