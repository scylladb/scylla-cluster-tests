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
import json
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
    """Verify that a presigned URL is generated with the correct bucket region."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    params["scylla_doctor_version"] = "1.9"

    mock_ks = MagicMock()
    mock_ks.get_scylla_doctor_full_bucket_config.return_value = {
        "bucket": "fe-artifacts-297607762119-eu-central-1",
        "prefix": "releases/",
    }
    mock_keystore_cls.return_value = mock_ks

    now = datetime.datetime.now(tz=datetime.timezone.utc)

    # boto3.client("s3") is called 2 times:
    #   1) locate: list_objects
    #   2) download: generate_presigned_url (with region_name extracted from bucket name)
    # _get_bucket_region extracts "eu-central-1" from the bucket name — no API call needed
    mock_s3_list = MagicMock()
    mock_s3_list.list_objects.return_value = {
        "Contents": [
            {"Key": "releases/scylla-doctor-1.9.0.tar.gz", "LastModified": now},
        ]
    }
    mock_s3_presign = MagicMock()
    mock_s3_presign.generate_presigned_url.return_value = "https://fe-artifacts.s3.eu-central-1.amazonaws.com/signed"

    mock_boto_client.side_effect = [mock_s3_list, mock_s3_presign]

    # _download_and_extract_tarball calls remoter.run multiple times:
    # 1) curl --version, 2) curl -fSL download, 3) test -s && file, 4) tar -xvzf
    # The `file` check needs "gzip" in stdout to pass validation.
    def _run_side_effect(cmd, **kwargs):
        result = MagicMock()
        result.ok = True
        if "file " in cmd:
            result.stdout = "/tmp/scylla_doctor_download.tar.gz: gzip compressed data\n"
        else:
            result.stdout = "/home/testuser\n"
        return result

    mock_node.remoter.run.side_effect = _run_side_effect

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)
    doc.download_full_scylla_doctor()

    # Verify the S3 client for presigned URL was created with the region extracted from bucket name
    assert mock_boto_client.call_args_list[1] == (("s3",), {"region_name": "eu-central-1"})

    mock_s3_presign.generate_presigned_url.assert_called_once_with(
        ClientMethod="get_object",
        Params={"Bucket": "fe-artifacts-297607762119-eu-central-1", "Key": "releases/scylla-doctor-1.9.0.tar.gz"},
        ExpiresIn=300,
    )

    # Verify curl download was called with the presigned URL
    curl_calls = [
        call for call in mock_node.remoter.run.call_args_list if "curl" in str(call) and "eu-central-1" in str(call)
    ]
    assert len(curl_calls) == 1

    # Verify the full edition flag was set
    assert doc._full_edition_downloaded is True


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
    """Region should be extracted from the bucket name suffix without any API call."""
    assert ScyllaDoctor._get_bucket_region(bucket_name) == expected_region


@patch("utils.scylla_doctor.boto3.client")
def test_get_bucket_region_fallback_to_api(mock_boto_client):
    """When the bucket name has no region suffix, fall back to get_bucket_location API."""
    mock_s3 = MagicMock()
    mock_s3.get_bucket_location.return_value = {"LocationConstraint": "eu-west-1"}
    mock_boto_client.return_value = mock_s3

    assert ScyllaDoctor._get_bucket_region("my-plain-bucket") == "eu-west-1"
    mock_s3.get_bucket_location.assert_called_once_with(Bucket="my-plain-bucket")


def test_download_and_extract_tarball_rejects_non_gzip(mock_node, mock_test_config):
    """When the downloaded file is not a valid gzip tarball (e.g. S3 error XML), raise ScyllaDoctorException."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    call_count = 0

    def _run_side_effect(cmd, **kwargs):
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        result.ok = True
        if "file " in cmd:
            # S3 returned an XML error page, not gzip
            result.stdout = "/tmp/scylla_doctor_download.tar.gz: XML document text\n"
        elif "head -c" in cmd:
            result.stdout = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code></Error>'
        else:
            result.stdout = ""
        return result

    mock_node.remoter.run.side_effect = _run_side_effect

    with pytest.raises(ScyllaDoctorException, match="not a valid gzip tarball"):
        doc._download_and_extract_tarball("https://example.com/bad.tar.gz", description="test")


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


@patch.object(ScyllaDoctor, "download_full_scylla_doctor", side_effect=ScyllaDoctorException("keystore unavailable"))
def test_download_full_failure_raises(mock_download_full, mock_node, mock_test_config):
    """When full download fails, download_scylla_doctor should raise — no silent fallback to basic."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with pytest.raises(ScyllaDoctorException, match="keystore unavailable"):
        doc.download_scylla_doctor()


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

    def _run_side_effect(cmd, **kwargs):
        result = MagicMock()
        result.ok = True
        if "file " in cmd:
            result.stdout = "/tmp/scylla_doctor_download.tar.gz: gzip compressed data\n"
        else:
            result.stdout = "/home/testuser\n"
        return result

    mock_node.remoter.run.side_effect = _run_side_effect

    doc = ScyllaDoctor(node=mock_node, test_config=test_config, offline_install=True)

    with patch.object(ScyllaDoctor, "download_full_scylla_doctor") as mock_full:
        doc.download_scylla_doctor()
        mock_full.assert_not_called()

    # Basic path was used — curl with the public URL
    curl_calls = [call for call in mock_node.remoter.run.call_args_list if "downloads.scylladb.com" in str(call)]
    assert len(curl_calls) == 1


# --- is_full_edition tests ---


def test_is_full_edition_true(mock_node, mock_test_config):
    """When edition is 'full' AND the full binary was downloaded, is_full_edition should return True."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._full_edition_downloaded = True
    assert doc.is_full_edition is True


def test_is_full_edition_false_when_download_failed(mock_node, mock_test_config):
    """When edition is 'full' but the download fell back to basic, is_full_edition should return False."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    # _full_edition_downloaded defaults to False
    assert doc.is_full_edition is False


def test_is_full_edition_false_basic(mock_node, mock_test_config):
    """When scylla_doctor_edition is 'basic', is_full_edition should return False."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "basic"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.is_full_edition is False


def test_is_full_edition_false_none(mock_node, mock_test_config):
    """When scylla_doctor_edition is not set, is_full_edition should return False."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    assert doc.is_full_edition is False


# --- run_analysis_phase tests ---


def test_run_analysis_phase_skipped_for_basic(mock_node, mock_test_config):
    """When edition is basic, run_analysis_phase should skip without running anything."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "basic"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.json_result_file = "test.vitals.json"

    with patch.object(doc, "run") as mock_run:
        doc.run_analysis_phase()
        mock_run.assert_not_called()


def test_run_analysis_phase_skipped_when_no_vitals(mock_node, mock_test_config):
    """When no vitals file is available, run_analysis_phase should skip."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._full_edition_downloaded = True
    doc.json_result_file = ""

    with patch.object(doc, "run") as mock_run:
        doc.run_analysis_phase()
        mock_run.assert_not_called()


def test_run_analysis_phase_full_edition_success(mock_node, mock_test_config):
    """When edition is full, run_analysis_phase should run analysis and store the report file."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._full_edition_downloaded = True
    doc.json_result_file = "test-node.local.vitals.json"
    doc.scylla_doctor_exec = "/home/testuser/scylla_doctor.pyz"

    sudo_result = MagicMock()
    sudo_result.stdout = "Analysis complete"
    mock_node.remoter.sudo.return_value = sudo_result

    check_result = MagicMock()
    check_result.ok = True
    mock_node.remoter.run.return_value = check_result

    doc.run_analysis_phase()

    # Verify sudo was called with tee to save output directly to file
    mock_node.remoter.sudo.assert_called_once()
    sudo_cmd = mock_node.remoter.sudo.call_args[0][0]
    assert "--load-vitals test-node.local.vitals.json" in sudo_cmd
    assert "--verbose" not in sudo_cmd
    assert "tee" in sudo_cmd
    assert "test-node.local.analysis.json" in sudo_cmd

    assert doc.analysis_report_file == "test-node.local.analysis.json"


def test_run_analysis_phase_report_not_created(mock_node, mock_test_config):
    """When the analysis report file is not created, ScyllaDoctorException should be raised."""
    test_config, params = mock_test_config
    params["scylla_doctor_edition"] = "full"
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._full_edition_downloaded = True
    doc.json_result_file = "test-node.local.vitals.json"
    doc.scylla_doctor_exec = "/home/testuser/scylla_doctor.pyz"

    sudo_result = MagicMock()
    sudo_result.stdout = ""
    mock_node.remoter.sudo.return_value = sudo_result

    # test -s check fails — file was not created
    check_result = MagicMock()
    check_result.ok = False
    mock_node.remoter.run.return_value = check_result

    with pytest.raises(ScyllaDoctorException, match="Analysis report file"):
        doc.run_analysis_phase()


# --- analyze_and_verify_analysis_results tests ---


def test_verify_analysis_results_all_pass(mock_node, mock_test_config):
    """When all analyzers pass, analyze_and_verify_analysis_results should not raise."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = "test.analysis.json"

    analysis_json = json.dumps(
        {
            "ConfigAnalyzer": {"status": 0, "findings": []},
            "PerformanceAnalyzer": {"status": 0, "findings": []},
        }
    )
    sudo_result = MagicMock()
    sudo_result.stdout = analysis_json
    mock_node.remoter.sudo.return_value = sudo_result

    doc.analyze_and_verify_analysis_results()  # Should not raise


def test_verify_analysis_results_failed_analyzer(mock_node, mock_test_config):
    """When an analyzer fails, analyze_and_verify_analysis_results should raise AssertionError."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = "test.analysis.json"

    analysis_json = json.dumps(
        {
            "ConfigAnalyzer": {"status": 0, "findings": []},
            "PerformanceAnalyzer": {"status": 1, "findings": [{"severity": "error", "message": "High latency"}]},
        }
    )
    sudo_result = MagicMock()
    sudo_result.stdout = analysis_json
    mock_node.remoter.sudo.return_value = sudo_result

    # Need to mock version property to avoid running remote command
    with patch.object(type(doc), "version", new_callable=lambda: property(lambda self: "1.10")):
        with pytest.raises(AssertionError, match="Failed analyzers"):
            doc.analyze_and_verify_analysis_results()


def test_verify_analysis_results_skipped_analyzer(mock_node, mock_test_config):
    """Analyzers with status 2 (skipped) should not cause a failure."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = "test.analysis.json"

    analysis_json = json.dumps(
        {
            "ConfigAnalyzer": {"status": 0, "findings": []},
            "SkippedAnalyzer": {"status": 2},
        }
    )
    sudo_result = MagicMock()
    sudo_result.stdout = analysis_json
    mock_node.remoter.sudo.return_value = sudo_result

    doc.analyze_and_verify_analysis_results()  # Should not raise


def test_verify_analysis_no_report_file(mock_node, mock_test_config):
    """When no analysis report file exists, the method should return early without error."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = ""

    doc.analyze_and_verify_analysis_results()  # Should not raise
    mock_node.remoter.sudo.assert_not_called()


def test_verify_analysis_results_non_json_output(mock_node, mock_test_config):
    """When analysis file contains only human-readable text (no JSON), should log warning and return."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = "test.analysis.json"

    # Simulate the exact output from scylla-doctor basic edition: ASCII banner + table, no JSON
    human_readable = (
        "__            _ _            ___           _\n"
        "/ _\\ ___ _   _   __ _     /   \\___   ___ _ ___  _ __\n"
        "+ Data collection\n"
        "  - CPUScalingCollector   PASSED   Data collected\n"
        "+ Data analysis\n"
    )
    sudo_result = MagicMock()
    sudo_result.stdout = human_readable
    mock_node.remoter.sudo.return_value = sudo_result

    doc.analyze_and_verify_analysis_results()  # Should not raise — gracefully skips


# --- _extract_json_from_output tests ---


def test_extract_json_pure_json():
    """Pure JSON content should be parsed directly."""
    content = json.dumps({"ConfigAnalyzer": {"status": 0}})
    result = ScyllaDoctor._extract_json_from_output(content)
    assert result == {"ConfigAnalyzer": {"status": 0}}


def test_extract_json_with_preamble():
    """JSON preceded by non-JSON text (banner, progress messages) should still be extracted."""
    banner = (
        "__            _ _            ___           _\n"
        "/ _\\ ___ _   _   __ _     /   \\___   ___ _ ___  _ __\n"
        "+ Data collection\n"
        "+ Data analysis\n"
    )
    payload = {"ConfigAnalyzer": {"status": 0, "findings": []}}
    content = banner + json.dumps(payload)
    result = ScyllaDoctor._extract_json_from_output(content)
    assert result == payload


def test_extract_json_empty_content():
    """Empty content should raise ScyllaDoctorException."""
    with pytest.raises(ScyllaDoctorException, match="empty"):
        ScyllaDoctor._extract_json_from_output("")


def test_extract_json_no_json_at_all():
    """Content with no JSON at all should raise ScyllaDoctorException."""
    with pytest.raises(ScyllaDoctorException, match="No JSON object found"):
        ScyllaDoctor._extract_json_from_output("Just some text\nwithout any JSON")


def test_extract_json_invalid_json_after_brace():
    """Content with a '{' but invalid JSON should raise ScyllaDoctorException."""
    with pytest.raises(ScyllaDoctorException, match="Failed to parse JSON"):
        ScyllaDoctor._extract_json_from_output("preamble {not valid json at all")


def test_extract_json_with_trailing_text():
    """JSON followed by trailing text should still be extracted."""
    payload = {"PerformanceAnalyzer": {"status": 1, "findings": [{"msg": "slow"}]}}
    content = json.dumps(payload) + "\n\nSome trailing text"
    result = ScyllaDoctor._extract_json_from_output(content)
    assert result == payload


# --- analyze_and_verify_analysis_results with non-JSON preamble ---


def test_verify_analysis_results_with_banner_preamble(mock_node, mock_test_config):
    """When analysis file has a banner before JSON, results should still be parsed correctly."""
    test_config, _params = mock_test_config
    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc.analysis_report_file = "test.analysis.json"

    banner = "Scylla Doctor Banner\n+ Data analysis\n"
    analysis_data = {
        "ConfigAnalyzer": {"status": 0, "findings": []},
        "PerformanceAnalyzer": {"status": 0, "findings": []},
    }
    sudo_result = MagicMock()
    sudo_result.stdout = banner + json.dumps(analysis_data)
    mock_node.remoter.sudo.return_value = sudo_result

    doc.analyze_and_verify_analysis_results()  # Should not raise


# --- _ensure_pyyaml_installed tests ---


def test_ensure_pyyaml_installed_rhel_like(mock_node, mock_test_config):
    """On RHEL-like distros, python3-pyyaml should be installed via install_package."""
    test_config, _params = mock_test_config
    mock_node.distro.is_rhel_like = True
    mock_node.distro.is_sles = False
    mock_node.distro.is_debian_like = False
    mock_node.is_nonroot_install = False
    mock_node.install_package = MagicMock()

    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._ensure_pyyaml_installed()

    mock_node.install_package.assert_called_once_with("python3-pyyaml")


def test_ensure_pyyaml_installed_debian_like(mock_node, mock_test_config):
    """On Debian-like distros, python3-yaml should be installed via install_package."""
    test_config, _params = mock_test_config
    mock_node.distro.is_rhel_like = False
    mock_node.distro.is_sles = False
    mock_node.distro.is_debian_like = True
    mock_node.is_nonroot_install = False
    mock_node.install_package = MagicMock()

    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._ensure_pyyaml_installed()

    mock_node.install_package.assert_called_once_with("python3-yaml")


def test_ensure_pyyaml_installed_sles(mock_node, mock_test_config):
    """On SLES distros, python3-PyYAML should be installed via zypper."""
    test_config, _params = mock_test_config
    mock_node.distro.is_rhel_like = False
    mock_node.distro.is_sles = True
    mock_node.distro.is_debian_like = False
    mock_node.is_nonroot_install = False

    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._ensure_pyyaml_installed()

    mock_node.remoter.sudo.assert_called_once_with("zypper install -y python3-PyYAML", retry=3)


def test_ensure_pyyaml_installed_unknown_distro(mock_node, mock_test_config):
    """On unknown distros, pip install should be attempted as fallback."""
    test_config, _params = mock_test_config
    mock_node.distro.is_rhel_like = False
    mock_node.distro.is_sles = False
    mock_node.distro.is_debian_like = False
    mock_node.is_nonroot_install = False

    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._ensure_pyyaml_installed()

    mock_node.remoter.sudo.assert_called_once_with("python3 -m pip install pyyaml", ignore_status=True)


def test_ensure_pyyaml_skipped_for_nonroot(mock_node, mock_test_config):
    """For nonroot installs, PyYAML installation should be skipped (bundled Python used)."""
    test_config, _params = mock_test_config
    mock_node.is_nonroot_install = True
    mock_node.install_package = MagicMock()

    doc = ScyllaDoctor(node=mock_node, test_config=test_config)
    doc._ensure_pyyaml_installed()

    mock_node.install_package.assert_not_called()
    mock_node.remoter.sudo.assert_not_called()
