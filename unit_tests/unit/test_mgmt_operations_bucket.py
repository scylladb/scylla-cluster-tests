"""Unit tests for BucketOperations download of backup files from S3 (nodetool refresh restore)."""

import configparser
from pathlib import Path
from unittest.mock import Mock

from sdcm.mgmt.operations import OCI_AWS_CLI_CONFIG, BucketOperations

OCI_ENDPOINT = "https://ns.compat.objectstorage.us-phoenix-1.oci.customer-oci.com"
OCI_CREDS = {"access_key_id": "SENTINEL_OCI_ACCESS_KEY_ID", "secret_access_key": "SENTINEL_OCI_SECRET_ACCESS_KEY"}


def _bucket_ops(params: dict) -> BucketOperations:
    ops = BucketOperations.__new__(BucketOperations)
    ops.params = params
    ops.test_config = Mock(backup_oci_credentials=OCI_CREDS)
    return ops


def test_download_from_s3_uses_plain_aws_cli_outside_oci():
    ops = _bucket_ops({"cluster_backend": "aws"})
    node = Mock()
    ops.install_awscli_dependencies(node)
    ops.download_from_s3(node, source="s3://bucket/key", destination="/upload")

    node.remoter.send_files.assert_not_called()
    assert node.remoter.sudo.call_args.args[0] == "aws s3 cp 's3://bucket/key' '/upload'"


def test_download_from_s3_on_oci_uses_endpoint_and_installed_config():
    ops = _bucket_ops(
        {
            "cluster_backend": "oci",
            "append_scylla_yaml": {"object_storage_endpoints": [{"name": OCI_ENDPOINT, "aws_region": "us-phoenix-1"}]},
        }
    )
    node = Mock()
    sent = []
    node.remoter.send_files.side_effect = lambda src, dst: sent.append(Path(src).read_text())
    ops.install_awscli_dependencies(node)
    ops.download_from_s3(node, source="s3://bucket/key", destination="/upload")

    config = configparser.ConfigParser()
    config.read_string(sent[0])
    assert dict(config["default"]) == {
        "aws_access_key_id": OCI_CREDS["access_key_id"],
        "aws_secret_access_key": OCI_CREDS["secret_access_key"],
        "region": "us-phoenix-1",
        "s3": "\naddressing_style = path",
    }
    sudo_cmds = [call.args[0] for call in node.remoter.sudo.call_args_list]
    assert any(OCI_AWS_CLI_CONFIG in cmd and "0600" in cmd for cmd in sudo_cmds)
    assert sudo_cmds[-1] == (
        f"env AWS_CONFIG_FILE={OCI_AWS_CLI_CONFIG} aws --endpoint-url {OCI_ENDPOINT} s3 cp 's3://bucket/key' '/upload'"
    )
    for cmd in sudo_cmds:
        assert OCI_CREDS["access_key_id"] not in cmd
        assert OCI_CREDS["secret_access_key"] not in cmd
