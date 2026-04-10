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
from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

import pytest

from utils.scylla_doctor import ScyllaDoctor, ScyllaDoctorException


@dataclass
class FakeParentCluster:
    cluster_backend: str = "aws"


class FakeNode:
    """Minimal fake node for ScyllaDoctor unit tests."""

    def __init__(self):
        self.name = "test-node"
        self.is_nonroot_install = False
        self.public_dns_name = "test-node.local"
        self.is_enterprise = False
        self.distro = MagicMock()
        self.parent_cluster = FakeParentCluster()
        self.remoter = MagicMock()
        run_result = MagicMock()
        run_result.ok = True
        run_result.stdout = "/home/testuser\n"
        self.remoter.run.return_value = run_result

    def install_package(self, pkg):
        pass


@dataclass
class FakeTester:
    """Minimal fake tester for ScyllaDoctor unit tests."""

    params: dict = field(default_factory=dict)


@pytest.fixture()
def mock_test_config():
    """Create a fake test_config with a configurable params dict."""
    params = {}
    tester = FakeTester(params=params)
    test_config = MagicMock()
    test_config.tester_obj.return_value = tester
    return test_config, params


@pytest.fixture()
def mock_node():
    return FakeNode()


@pytest.fixture()
def doctor(mock_node, mock_test_config):
    """Create a ScyllaDoctor instance with fake dependencies."""
    test_config, _params = mock_test_config
    return ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)


# --- configured_edition tests ---


def test_configured_edition_not_set_returns_none(mock_node, mock_test_config):
    """When scylla_doctor_edition is not set in params, configured_edition returns None.

    The 'basic' default is enforced by defaults/test_default.yaml, not by this property.
    """
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.configured_edition is None


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
        "bucket": "private-sd-bucket-us-east-1",
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
    mock_s3_presign.generate_presigned_url.return_value = (
        "https://private-sd-bucket-us-east-1.s3.amazonaws.com/signed-url"
    )

    # boto3.client is called twice: first in locate, then in download (with region)
    mock_boto_client.side_effect = [mock_s3_list, mock_s3_presign]

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    with patch.object(doc, "_download_and_extract_tarball") as mock_extract:
        doc.download_full_scylla_doctor()

    mock_s3_presign.generate_presigned_url.assert_called_once_with(
        ClientMethod="get_object",
        Params={"Bucket": "private-sd-bucket-us-east-1", "Key": "releases/scylla-doctor-1.9.0.tar.gz"},
        ExpiresIn=300,
    )

    # Verify the presigned URL was passed to the download method
    mock_extract.assert_called_once_with(
        "https://private-sd-bucket-us-east-1.s3.amazonaws.com/signed-url",
        description="full scylla-doctor (scylla-doctor-1.9.0.tar.gz)",
    )


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
@patch.object(ScyllaDoctor, "download_full_scylla_doctor", side_effect=ScyllaDoctorException("keystore unavailable"))
def test_download_full_failure_propagates(mock_download_full, mock_boto_client, mock_node, mock_test_config):
    """When full download fails, download_scylla_doctor should propagate the exception (no fallback)."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    params["scylla_doctor_version"] = "1.9"

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with pytest.raises(ScyllaDoctorException, match="keystore unavailable"):
        doc.download_scylla_doctor()

    mock_download_full.assert_called_once()


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
        with patch.object(doc, "_download_and_extract_tarball") as mock_extract:
            doc.download_scylla_doctor()
        mock_full.assert_not_called()

    # Basic path was used — _download_and_extract_tarball was called with the public URL
    mock_extract.assert_called_once()
    call_url = mock_extract.call_args[0][0]
    assert "downloads.scylladb.com" in call_url


# --- _get_bucket_region tests ---


@pytest.mark.parametrize(
    "bucket_name,expected_region",
    [
        ("fe-artifacts-297607762119-eu-central-1", "eu-central-1"),
        ("my-bucket-us-west-2", "us-west-2"),
        ("something-ap-southeast-1", "ap-southeast-1"),
        ("bucket-sa-east-1", "sa-east-1"),
    ],
)
def test_get_bucket_region_from_name(bucket_name, expected_region):
    """Region should be extracted from bucket name when it ends with a valid AWS region."""
    assert ScyllaDoctor._get_bucket_region(bucket_name) == expected_region


@patch("utils.scylla_doctor.boto3.client")
def test_get_bucket_region_fallback_to_api(mock_boto_client):
    """When bucket name doesn't contain a region, fall back to get_bucket_location API."""
    mock_s3 = MagicMock()
    mock_s3.get_bucket_location.return_value = {"LocationConstraint": "eu-west-1"}
    mock_boto_client.return_value = mock_s3

    assert ScyllaDoctor._get_bucket_region("my-plain-bucket") == "eu-west-1"


# --- _download_and_extract_tarball tests ---


def test_download_and_extract_tarball_rejects_non_gzip(mock_node, mock_test_config):
    """When downloaded file is not a gzip tarball, ScyllaDoctorException should be raised."""
    test_config, _params = mock_test_config

    # Make the file check return non-gzip
    check_result = MagicMock()
    check_result.ok = True
    check_result.stdout = "ASCII text"

    head_result = MagicMock()
    head_result.stdout = "<Error><Code>AccessDenied</Code></Error>"

    rm_result = MagicMock()
    rm_result.ok = True

    # First call: curl download (succeeds), second: file check, third: head, fourth: rm
    mock_node.remoter.run.side_effect = [
        MagicMock(ok=True),  # curl download
        check_result,  # file check
        head_result,  # head -c 500
        rm_result,  # rm -f
    ]

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with pytest.raises(ScyllaDoctorException, match="not a valid gzip tarball"):
        doc._download_and_extract_tarball("https://example.com/bad.tar.gz")


# --- _full_edition_downloaded tests ---


def test_full_edition_downloaded_flag_set_on_success(mock_node, mock_test_config):
    """After successful full download, _full_edition_downloaded should be True."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    assert not getattr(doc, "_full_edition_downloaded", False)

    with patch.object(doc, "download_full_scylla_doctor") as mock_full:

        def set_flag():
            doc._full_edition_downloaded = True

        mock_full.side_effect = set_flag
        doc.download_scylla_doctor()

    assert doc._full_edition_downloaded is True
