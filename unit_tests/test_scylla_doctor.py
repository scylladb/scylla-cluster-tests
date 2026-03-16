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
# Copyright (c) 2020 ScyllaDB

import datetime
from unittest.mock import MagicMock, patch

import pytest

from utils.scylla_doctor import ScyllaDoctor, ScyllaDoctorException


@pytest.fixture()
def mock_test_config():
    """Create a mock test_config with configurable params."""
    test_config = MagicMock()
    params = {}
    test_config.tester_obj.return_value.params.get = lambda key, default=None: params.get(key, default)
    return test_config, params


@pytest.fixture()
def mock_node():
    """Create a mock node with remoter."""
    node = MagicMock(
        spec=[
            "remoter",
            "is_nonroot_install",
            "parent_cluster",
            "install_package",
            "public_dns_name",
            "distro",
            "is_enterprise",
            "name",
        ]
    )
    node.is_nonroot_install = False
    node.parent_cluster.cluster_backend = "aws"
    run_result = MagicMock()
    run_result.ok = True
    run_result.stdout = "/home/testuser\n"
    node.remoter.run.return_value = run_result
    return node


@pytest.fixture()
def doctor(mock_node, mock_test_config):
    """Create a ScyllaDoctor instance with mocked dependencies."""
    test_config, _params = mock_test_config
    return ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)


# --- configured_edition tests ---


def test_configured_edition_defaults_to_basic(mock_node, mock_test_config):
    """When scylla_doctor_edition is not set, configured_edition should default to 'basic'."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.configured_edition == "basic"


def test_configured_edition_full(mock_node, mock_test_config):
    """When scylla_doctor_edition is 'full', configured_edition should return 'full'."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.configured_edition == "full"


def test_configured_edition_basic_explicit(mock_node, mock_test_config):
    """When scylla_doctor_edition is explicitly 'basic', configured_edition should return 'basic'."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "basic"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.configured_edition == "basic"


# --- locate_full_scylla_doctor_package tests ---


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_locate_full_package_found(mock_keystore_cls, mock_boto_client, doctor):
    """When the full edition bucket has matching packages, the latest should be returned."""
    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/scylla-doctor/tar/",
    }
    mock_keystore_cls.return_value = mock_ks

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {
        "Contents": [
            {
                "Key": "releases/scylla-doctor/tar/scylla-doctor-1.9.0.tar.gz",
                "LastModified": now - datetime.timedelta(days=2),
            },
            {"Key": "releases/scylla-doctor/tar/scylla-doctor-1.9.1.tar.gz", "LastModified": now},
        ]
    }
    mock_boto_client.return_value = mock_s3

    package, bucket = doctor.locate_full_scylla_doctor_package(version="1.9")

    assert package is not None
    assert package["Key"] == "releases/scylla-doctor/tar/scylla-doctor-1.9.1.tar.gz"
    assert bucket == "private-sd-bucket"


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_locate_full_package_not_found(mock_keystore_cls, mock_boto_client, doctor):
    """When the full edition bucket is empty, (None, None) should be returned."""
    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/scylla-doctor/tar/",
    }
    mock_keystore_cls.return_value = mock_ks

    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {}
    mock_boto_client.return_value = mock_s3

    package, bucket = doctor.locate_full_scylla_doctor_package()

    assert package is None
    assert bucket is None


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_locate_full_package_version_not_found(mock_keystore_cls, mock_boto_client, doctor):
    """When the requested version doesn't exist, (None, None) should be returned."""
    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/scylla-doctor/tar/",
    }
    mock_keystore_cls.return_value = mock_ks

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {
        "Contents": [
            {"Key": "releases/scylla-doctor/tar/scylla-doctor-1.8.0.tar.gz", "LastModified": now},
        ]
    }
    mock_boto_client.return_value = mock_s3

    package, bucket = doctor.locate_full_scylla_doctor_package(version="2.0")

    assert package is None
    assert bucket is None


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_locate_full_package_latest_when_no_version(mock_keystore_cls, mock_boto_client, doctor):
    """When no version is specified, the latest package by LastModified should be returned."""
    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/",
    }
    mock_keystore_cls.return_value = mock_ks

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {
        "Contents": [
            {"Key": "releases/scylla-doctor-1.8.tar.gz", "LastModified": now - datetime.timedelta(days=5)},
            {"Key": "releases/scylla-doctor-2.0.tar.gz", "LastModified": now},
        ]
    }
    mock_boto_client.return_value = mock_s3

    package, bucket = doctor.locate_full_scylla_doctor_package()

    assert package["Key"] == "releases/scylla-doctor-2.0.tar.gz"
    assert bucket == "private-sd-bucket"


# --- download_full_scylla_doctor tests ---


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_download_full_generates_presigned_url(mock_keystore_cls, mock_boto_client, mock_node, mock_test_config):
    """Verify that a presigned URL is generated and used with curl on the node."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    params["scylla_doctor_version"] = "1.9"

    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/",
    }
    mock_keystore_cls.return_value = mock_ks

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    # We need two separate mock S3 clients: one for list_objects, one for presigned URL
    mock_s3_list = MagicMock()
    mock_s3_list.list_objects.return_value = {
        "Contents": [
            {"Key": "releases/scylla-doctor-1.9.0.tar.gz", "LastModified": now},
        ]
    }
    mock_s3_presign = MagicMock()
    mock_s3_presign.generate_presigned_url.return_value = "https://private-sd-bucket.s3.amazonaws.com/signed-url"

    # boto3.client is called twice: first in locate, then in download
    mock_boto_client.side_effect = [mock_s3_list, mock_s3_presign]

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    doc.download_full_scylla_doctor()

    mock_s3_presign.generate_presigned_url.assert_called_once_with(
        ClientMethod="get_object",
        Params={"Bucket": "private-sd-bucket", "Key": "releases/scylla-doctor-1.9.0.tar.gz"},
        ExpiresIn=300,
    )

    # Verify curl was called on the node with the presigned URL
    curl_calls = [
        call for call in mock_node.remoter.run.call_args_list if "curl" in str(call) and "signed-url" in str(call)
    ]
    assert len(curl_calls) == 1


@patch("utils.scylla_doctor.boto3.client")
@patch("utils.scylla_doctor.KeyStore")
def test_download_full_raises_when_package_not_found(mock_keystore_cls, mock_boto_client, mock_node, mock_test_config):
    """When no package is found in the full edition bucket, ScyllaDoctorException should be raised."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"

    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "private-sd-bucket",
        "prefix": "releases/",
    }
    mock_keystore_cls.return_value = mock_ks

    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {}
    mock_boto_client.return_value = mock_s3

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with pytest.raises(ScyllaDoctorException, match="Unable to find full scylla-doctor"):
        doc.download_full_scylla_doctor()


# --- download_scylla_doctor dispatch tests ---


@patch.object(ScyllaDoctor, "download_full_scylla_doctor")
def test_download_dispatches_to_full_when_edition_is_full(mock_download_full, mock_node, mock_test_config):
    """When configured_edition is 'full', download_scylla_doctor should call download_full_scylla_doctor."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    doc.download_scylla_doctor()

    mock_download_full.assert_called_once()


@patch("utils.scylla_doctor.boto3.client")
@patch.object(
    ScyllaDoctor, "download_full_scylla_doctor", side_effect=ScyllaDoctorException("keystore unavailable")
)
def test_download_fallback_to_basic_on_full_failure(
    mock_download_full, mock_boto_client, mock_node, mock_test_config
):
    """When full download fails, download_scylla_doctor should fall back to basic edition."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    params["scylla_doctor_version"] = "1.9"

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {
        "Contents": [
            {"Key": "downloads/scylla-doctor/tar/scylla-doctor-1.9.0.tar.gz", "LastModified": now},
        ]
    }
    mock_boto_client.return_value = mock_s3

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    doc.download_scylla_doctor()

    # Full was attempted and failed
    mock_download_full.assert_called_once()

    # Basic download path was exercised — curl with the public URL was called
    curl_calls = [call for call in mock_node.remoter.run.call_args_list if "downloads.scylladb.com" in str(call)]
    assert len(curl_calls) == 1


@patch("utils.scylla_doctor.boto3.client")
def test_download_basic_edition_skips_full(mock_boto_client, mock_node, mock_test_config):
    """When configured_edition is 'basic', download_scylla_doctor should not attempt full download."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "basic"
    params["scylla_doctor_version"] = "1.9"

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    mock_s3 = MagicMock()
    mock_s3.list_objects.return_value = {
        "Contents": [
            {"Key": "downloads/scylla-doctor/tar/scylla-doctor-1.9.0.tar.gz", "LastModified": now},
        ]
    }
    mock_boto_client.return_value = mock_s3

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with patch.object(ScyllaDoctor, "download_full_scylla_doctor") as mock_full:
        doc.download_scylla_doctor()
        mock_full.assert_not_called()

    # Basic path was used — curl with the public URL
    curl_calls = [call for call in mock_node.remoter.run.call_args_list if "downloads.scylladb.com" in str(call)]
    assert len(curl_calls) == 1
