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
# Copyright (c) 2026 ScyllaDB

"""Unit tests for sdcm/keystore.py.

Tests all public methods, error paths, edge cases, and thread safety
using moto for AWS S3 mocking.  No real AWS calls are made.
"""

import hashlib
import io
import json
import logging
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor
from unittest.mock import MagicMock, PropertyMock, patch

import boto3
import paramiko
import pytest
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from moto import mock_aws

from sdcm.keystore import KEYSTORE_S3_BUCKET, KeyStore, SSHKey, pub_key_from_private_key_file


# ---------------------------------------------------------------------------
# Override the session-scoped mock_cloud_services fixture from conftest.py.
#
# The conftest fixture patches KeyStore methods *at the class level* via
# ``patch.object(KeyStore, ...)``.  Under pytest-xdist, another test module
# in the same worker may trigger the session-scoped fixture first, making
# those class-level patches global for the rest of the session.
#
# A simple no-op module fixture would prevent fixture injection for our
# tests, but cannot undo patches that are already active.  So we save the
# original class-dict entries at **import time** (before any fixture runs)
# and explicitly restore them while our tests execute.
# ---------------------------------------------------------------------------

# Captured at import time — before conftest's session fixture patches the class.
_KEYSTORE_ORIGINALS = {
    name: KeyStore.__dict__[name]
    for name in (
        "__init__",
        "s3",
        "s3_client",
        "get_file_contents",
        "_fetch_content",
        "_fetch_from_s3",
        "_fetch_from_secrets_manager",
        "get_json",
        "get_ssh_key_pair",
        "download_file",
        "sync",
        "get_obj_if_needed",
        "get_sm_version_id",
        "clear_cache",
    )
    if name in KeyStore.__dict__
}


@pytest.fixture(scope="module")
def mock_cloud_services():
    """Restore original KeyStore methods so moto S3 is used."""
    saved = {name: KeyStore.__dict__.get(name) for name in _KEYSTORE_ORIGINALS}
    for name, original in _KEYSTORE_ORIGINALS.items():
        setattr(KeyStore, name, original)
    yield
    for name, value in saved.items():
        if value is not None:
            setattr(KeyStore, name, value)


# --- Test data constants ---------------------------------------------------

FAKE_EMAIL_CONFIG = {"user": "test@example.com", "password": "secret"}
FAKE_GCP_CREDENTIALS = {"project_id": "gcp-sct-project-1", "type": "service_account"}
FAKE_GCP_DBAASLAB = {"project_id": "gcp-scylladbaaslab", "type": "service_account"}
FAKE_GCP_SERVICE_ACCOUNTS = [
    {"email": "svc@project.iam.gserviceaccount.com", "scopes": ["https://www.googleapis.com/auth/compute"]}
]
FAKE_SCYLLADB_UPLOAD = {"bucket": "uploads", "key": "fake-key"}
FAKE_QA_USERS = [{"username": "qa_user_1", "password": "pass1"}]
FAKE_ACL_GRANTEES = [{"id": "user1", "permission": "FULL_CONTROL"}]
FAKE_HOUSEKEEPING_DB = {"host": "db.example.com", "user": "admin", "password": "dbpass"}
FAKE_LDAP_MS_AD = {"host": "ldap.example.com", "user": "admin", "password": "ldappass"}
FAKE_BACKUP_AZURE_BLOB = {"account_name": "sctbackup", "account_key": "fake-key"}
FAKE_DOCKER_HUB = {"username": "docker_user", "password": "docker_pass"}
FAKE_AZURE_CREDS = {"subscription_id": "sub-123", "tenant_id": "tenant-456"}
FAKE_OCI_CREDS = {
    "tenancy": "ocid1.tenancy",
    "user": "ocid1.user",
    "fingerprint": "aa:bb:cc",
    "key_content": "fake-pem",
    "region": "us-phoenix-1",
    "compartment_id": "ocid1.compartment",
}
FAKE_AZURE_KMS = {"vault_name": "sct-kms-vault", "key_name": "sct-key"}
FAKE_GCP_KMS = {"project": "gcp-sct-project-1", "location": "global"}
FAKE_ARGUS_CREDS = {"token": "argus-token", "url": "https://argus.example.com"}
FAKE_ARGUS_AWS_CREDS = {"token": "argus-aws-token"}
FAKE_BAREMETAL = {"nodes": ["192.168.1.1"]}
FAKE_CLOUD_REST = {"api_key": "cloud-api-key"}
FAKE_JIRA_CREDS = {"token": "jira-token"}

FAKE_SSH_PRIVATE_KEY = b"fake-openssh-private-key-content-for-testing\n"
FAKE_SSH_PUBLIC_KEY = b"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA scylla_test\n"


# --- Helpers ---------------------------------------------------------------


def _populate_bucket(s3_resource):
    """Put all test objects into the mocked S3 bucket."""
    s3_resource.create_bucket(Bucket=KEYSTORE_S3_BUCKET)

    json_objects = {
        "email_config.json": FAKE_EMAIL_CONFIG,
        "gcp-sct-project-1.json": FAKE_GCP_CREDENTIALS,
        "gcp-scylladbaaslab.json": FAKE_GCP_DBAASLAB,
        "gcp-sct-project-1_service_accounts.json": FAKE_GCP_SERVICE_ACCOUNTS,
        "scylladb_upload.json": FAKE_SCYLLADB_UPLOAD,
        "qa_users.json": FAKE_QA_USERS,
        "bucket-users.json": FAKE_ACL_GRANTEES,
        "housekeeping-db.json": FAKE_HOUSEKEEPING_DB,
        "ldap_ms_ad.json": FAKE_LDAP_MS_AD,
        "backup_azure_blob.json": FAKE_BACKUP_AZURE_BLOB,
        "docker.json": FAKE_DOCKER_HUB,
        "azure.json": FAKE_AZURE_CREDS,
        "oci.json": FAKE_OCI_CREDS,
        "azure_kms_config.json": FAKE_AZURE_KMS,
        "gcp_kms_config.json": FAKE_GCP_KMS,
        "argus_rest_credentials.json": FAKE_ARGUS_CREDS,
        "argus_rest_credentials_sct_aws.json": FAKE_ARGUS_AWS_CREDS,
        "baremetal_config.json": FAKE_BAREMETAL,
        "scylla_cloud_sct_api_creds_lab.json": FAKE_CLOUD_REST,
        "scylladb_jira.json": FAKE_JIRA_CREDS,
    }
    for key, data in json_objects.items():
        s3_resource.Object(KEYSTORE_S3_BUCKET, key).put(Body=json.dumps(data).encode())

    s3_resource.Object(KEYSTORE_S3_BUCKET, "scylla_test_id_ed25519").put(Body=FAKE_SSH_PRIVATE_KEY)
    s3_resource.Object(KEYSTORE_S3_BUCKET, "scylla_test_id_ed25519.pub").put(Body=FAKE_SSH_PUBLIC_KEY)


# --- Fixtures --------------------------------------------------------------


@pytest.fixture
def mocked_s3():
    """Provide a moto-mocked S3 environment with a populated bucket."""
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        _populate_bucket(s3)
        yield s3


@pytest.fixture
def ks(mocked_s3):
    """Return a fresh KeyStore inside the moto context."""
    return KeyStore()


# ---------------------------------------------------------------------------
# get_file_contents
# ---------------------------------------------------------------------------


class TestGetFileContents:
    def test_returns_bytes(self, ks):
        assert isinstance(ks.get_file_contents("email_config.json"), bytes)

    def test_returns_correct_json_bytes(self, ks):
        assert json.loads(ks.get_file_contents("email_config.json")) == FAKE_EMAIL_CONFIG

    def test_returns_binary_content(self, ks):
        assert ks.get_file_contents("scylla_test_id_ed25519") == FAKE_SSH_PRIVATE_KEY

    def test_missing_key_raises_client_error(self, ks):
        with pytest.raises(ClientError) as exc_info:
            ks.get_file_contents("nonexistent.json")
        assert "NoSuchKey" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_json
# ---------------------------------------------------------------------------


class TestGetJson:
    def test_returns_dict(self, ks):
        assert ks.get_json("email_config.json") == FAKE_EMAIL_CONFIG

    def test_returns_list(self, ks):
        assert ks.get_json("qa_users.json") == FAKE_QA_USERS

    def test_invalid_json_raises(self, ks, mocked_s3):
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "bad.json").put(Body=b"{not json")
        with pytest.raises(json.JSONDecodeError):
            ks.get_json("bad.json")

    def test_empty_object_is_valid(self, ks, mocked_s3):
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "empty.json").put(Body=b"{}")
        assert ks.get_json("empty.json") == {}


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile:
    def test_writes_json_to_disk(self, ks, tmp_path):
        dest = str(tmp_path / "out.json")
        ks.download_file("email_config.json", dest)
        with open(dest, "rb") as fh:
            assert json.loads(fh.read()) == FAKE_EMAIL_CONFIG

    def test_writes_binary_to_disk(self, ks, tmp_path):
        dest = str(tmp_path / "key")
        ks.download_file("scylla_test_id_ed25519", dest)
        with open(dest, "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY


# ---------------------------------------------------------------------------
# Credential getter methods
# ---------------------------------------------------------------------------


class TestCredentialGetters:
    def test_email(self, ks):
        assert ks.get_email_credentials() == FAKE_EMAIL_CONFIG

    def test_gcp_default_project(self, ks):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SCT_GCE_PROJECT", None)
            assert ks.get_gcp_credentials() == FAKE_GCP_CREDENTIALS

    def test_gcp_custom_project(self, ks, mocked_s3):
        custom = {"project_id": "custom"}
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "my-project.json").put(Body=json.dumps(custom).encode())
        with patch.dict(os.environ, {"SCT_GCE_PROJECT": "my-project"}):
            assert ks.get_gcp_credentials() == custom

    def test_dbaaslab_gcp(self, ks):
        assert ks.get_dbaaslab_gcp_credentials() == FAKE_GCP_DBAASLAB

    def test_gcp_service_accounts_adds_cloud_platform_scope(self, ks):
        result = ks.get_gcp_service_accounts()
        assert "https://www.googleapis.com/auth/cloud-platform" in result[0]["scopes"]
        assert "https://www.googleapis.com/auth/compute" in result[0]["scopes"]

    def test_gcp_service_accounts_no_duplicate_scope(self, ks, mocked_s3):
        sa_data = [{"email": "x", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}]
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "gcp-sct-project-1_service_accounts.json").put(
            Body=json.dumps(sa_data).encode()
        )
        result = ks.get_gcp_service_accounts()
        assert result[0]["scopes"].count("https://www.googleapis.com/auth/cloud-platform") == 1

    def test_scylladb_upload(self, ks):
        assert ks.get_scylladb_upload_credentials() == FAKE_SCYLLADB_UPLOAD

    def test_qa_users(self, ks):
        assert ks.get_qa_users() == FAKE_QA_USERS

    def test_acl_grantees(self, ks):
        assert ks.get_acl_grantees() == FAKE_ACL_GRANTEES

    def test_housekeeping_db(self, ks):
        assert ks.get_housekeeping_db_credentials() == FAKE_HOUSEKEEPING_DB

    def test_ldap_ms_ad(self, ks):
        assert ks.get_ldap_ms_ad_credentials() == FAKE_LDAP_MS_AD

    def test_backup_azure_blob(self, ks):
        assert ks.get_backup_azure_blob_credentials() == FAKE_BACKUP_AZURE_BLOB

    def test_docker_hub(self, ks):
        assert ks.get_docker_hub_credentials() == FAKE_DOCKER_HUB

    def test_azure(self, ks):
        assert ks.get_azure_credentials() == FAKE_AZURE_CREDS

    def test_azure_kms(self, ks):
        assert ks.get_azure_kms_config() == FAKE_AZURE_KMS

    def test_gcp_kms(self, ks):
        assert ks.get_gcp_kms_config() == FAKE_GCP_KMS

    def test_baremetal(self, ks):
        assert ks.get_baremetal_config("baremetal_config") == FAKE_BAREMETAL

    def test_cloud_rest(self, ks):
        assert ks.get_cloud_rest_credentials(environment="lab") == FAKE_CLOUD_REST

    def test_jira(self, ks):
        assert ks.get_jira_credentials() == FAKE_JIRA_CREDS


# ---------------------------------------------------------------------------
# SSH key methods
# ---------------------------------------------------------------------------


class TestSSHKeys:
    def test_get_ssh_key_pair_returns_namedtuple(self, ks):
        key = ks.get_ssh_key_pair("scylla_test_id_ed25519")
        assert isinstance(key, SSHKey)
        assert key.name == "scylla_test_id_ed25519"
        assert key.public_key == FAKE_SSH_PUBLIC_KEY
        assert key.private_key == FAKE_SSH_PRIVATE_KEY

    def test_qa_ssh_keys(self, ks):
        result = ks.get_qa_ssh_keys()
        assert len(result) == 1
        assert isinstance(result[0], SSHKey)

    def test_missing_ssh_key_raises(self, ks):
        with pytest.raises(ClientError):
            ks.get_ssh_key_pair("nonexistent_key")


# ---------------------------------------------------------------------------
# OCI credentials (local config fallback)
# ---------------------------------------------------------------------------


class TestOCICredentials:
    def test_from_s3_when_no_env_var(self, ks):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SCT_OCI_CONFIG_PATH", None)
            assert ks.get_oci_credentials() == FAKE_OCI_CREDS

    def test_from_local_config(self, ks, tmp_path):
        key_file = tmp_path / "key.pem"
        key_file.write_text("fake-pem-key-content-for-testing\n")
        config = tmp_path / "config"
        config.write_text(
            f"[DEFAULT]\ntenancy=local-t\nuser=local-u\nfingerprint=xx:yy\n"
            f"key_file={key_file}\nregion=eu-frankfurt-1\ncompartment_id=local-c\n"
        )
        with patch.dict(os.environ, {"SCT_OCI_CONFIG_PATH": str(config)}):
            result = ks.get_oci_credentials()
            assert result["tenancy"] == "local-t"
            assert result["region"] == "eu-frankfurt-1"

    def test_nonexistent_config_falls_back_to_s3(self, ks):
        with patch.dict(os.environ, {"SCT_OCI_CONFIG_PATH": "/no/such/file"}):
            assert ks.get_oci_credentials() == FAKE_OCI_CREDS


# ---------------------------------------------------------------------------
# _parse_local_oci_config (static method)
# ---------------------------------------------------------------------------


class TestParseLocalOCIConfig:
    def test_parses_all_fields(self, tmp_path):
        key_file = tmp_path / "key.pem"
        key_file.write_text("pem-content")
        config = tmp_path / "config"
        config.write_text(
            f"[DEFAULT]\ntenancy=t\nuser=u\nfingerprint=fp\nkey_file={key_file}\nregion=r\ncompartment_id=c\n"
        )
        assert KeyStore._parse_local_oci_config(str(config)) == {
            "tenancy": "t",
            "user": "u",
            "fingerprint": "fp",
            "key_content": "pem-content",
            "region": "r",
            "compartment_id": "c",
        }

    def test_compartment_defaults_to_tenancy(self, tmp_path):
        key_file = tmp_path / "key.pem"
        key_file.write_text("pem")
        config = tmp_path / "config"
        config.write_text(f"[DEFAULT]\ntenancy=my-tenancy\nuser=u\nfingerprint=fp\nkey_file={key_file}\nregion=r\n")
        assert KeyStore._parse_local_oci_config(str(config))["compartment_id"] == "my-tenancy"

    def test_missing_profile_raises(self, tmp_path):
        config = tmp_path / "config"
        config.write_text("[DEFAULT]\ntenancy=t\n")
        with pytest.raises(ValueError, match="Profile 'NONEXISTENT' not found"):
            KeyStore._parse_local_oci_config(str(config), profile="NONEXISTENT")

    def test_custom_profile(self, tmp_path):
        key_file = tmp_path / "key.pem"
        key_file.write_text("custom-pem")
        config = tmp_path / "config"
        config.write_text(f"[CUSTOM]\ntenancy=ct\nuser=cu\nfingerprint=cf\nkey_file={key_file}\nregion=cr\n")
        result = KeyStore._parse_local_oci_config(str(config), profile="CUSTOM")
        assert result["tenancy"] == "ct"

    def test_missing_key_file_raises(self, tmp_path):
        config = tmp_path / "config"
        config.write_text("[DEFAULT]\ntenancy=t\nuser=u\nfingerprint=f\nkey_file=/no/key\nregion=r\n")
        with pytest.raises(ValueError, match="OCI key file not found"):
            KeyStore._parse_local_oci_config(str(config))


# ---------------------------------------------------------------------------
# Argus credentials (Jenkins / provider logic)
# ---------------------------------------------------------------------------


class TestArgusCredentials:
    def test_jenkins_with_matching_provider(self, ks):
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="aws"):
                assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_AWS_CREDS

    def test_jenkins_fallback_on_missing_provider_file(self, ks):
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="no_such_provider"):
                assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_CREDS

    def test_outside_jenkins(self, ks):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("JOB_NAME", None)
            assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_CREDS

    def test_explicit_provider_overrides_detection(self, ks):
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            result = ks.get_argus_rest_credentials_per_provider(cloud_provider="aws")
            assert result == FAKE_ARGUS_AWS_CREDS

    def test_jenkins_non_nosuchkey_error_propagates(self, ks):
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="aws"):
                with patch.object(ks, "get_json") as mock_gj:
                    error_resp = {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}
                    mock_gj.side_effect = ClientError(error_resp, "GetObject")
                    with pytest.raises(ClientError) as exc_info:
                        ks.get_argus_rest_credentials_per_provider()
                    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


# ---------------------------------------------------------------------------
# calculate_s3_etag (static method)
# ---------------------------------------------------------------------------


class TestCalculateS3Etag:
    def test_single_part(self):
        data = b"hello world"
        expected = '"{}"'.format(hashlib.md5(data).hexdigest())
        assert KeyStore.calculate_s3_etag(io.BytesIO(data)) == expected

    def test_multi_part_format(self):
        chunk = 8
        data = b"a" * 20  # 3 chunks: 8 + 8 + 4
        result = KeyStore.calculate_s3_etag(io.BytesIO(data), chunk_size=chunk)
        assert result.startswith('"') and result.endswith('"')
        assert "-3" in result

    def test_multi_part_correctness(self):
        chunk = 8
        data = b"abcdefghijklmnop"  # 2 chunks of 8
        md5_parts = [hashlib.md5(data[i : i + chunk]).digest() for i in range(0, len(data), chunk)]
        combined_md5 = hashlib.md5(b"".join(md5_parts)).hexdigest()
        expected = f'"{combined_md5}-{len(md5_parts)}"'
        assert KeyStore.calculate_s3_etag(io.BytesIO(data), chunk_size=chunk) == expected

    def test_empty_file(self):
        # Empty file has 0 chunks → md5 of empty bytes with "-0" suffix
        result = KeyStore.calculate_s3_etag(io.BytesIO(b""))
        expected_md5 = hashlib.md5(b"").hexdigest()
        assert result == f'"{expected_md5}-0"'


# ---------------------------------------------------------------------------
# get_object_etag
# ---------------------------------------------------------------------------


class TestGetObjectEtag:
    def test_returns_quoted_string(self, ks):
        etag = ks.get_object_etag("email_config.json")
        assert etag.startswith('"') and etag.endswith('"')

    def test_missing_key_raises(self, ks):
        with pytest.raises(ClientError):
            ks.get_object_etag("nonexistent")


# ---------------------------------------------------------------------------
# get_obj_if_needed / sync
# ---------------------------------------------------------------------------


class TestGetObjIfNeeded:
    def test_downloads_missing_file(self, ks, tmp_path):
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert (tmp_path / "email_config.json").exists()

    def test_skips_when_etag_matches(self, ks, tmp_path):
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        first = (tmp_path / "email_config.json").read_bytes()
        # Second call — file unchanged, ETag matches, no re-download
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert (tmp_path / "email_config.json").read_bytes() == first

    def test_redownloads_when_content_changed(self, ks, tmp_path):
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        (tmp_path / "email_config.json").write_bytes(b"tampered")
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert json.loads((tmp_path / "email_config.json").read_bytes()) == FAKE_EMAIL_CONFIG

    def test_creates_subdirectories(self, ks, tmp_path):
        deep = str(tmp_path / "a" / "b")
        ks.get_obj_if_needed("email_config.json", deep, 0o644)
        assert os.path.exists(os.path.join(deep, "email_config.json"))

    def test_sets_permissions(self, ks, tmp_path):
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o600)
        mode = os.stat(tmp_path / "email_config.json").st_mode & 0o777
        assert mode == 0o600


class TestSync:
    def test_downloads_multiple_keys(self, ks, tmp_path):
        ks.sync(["email_config.json", "scylla_test_id_ed25519"], str(tmp_path), 0o600)
        assert (tmp_path / "email_config.json").exists()
        assert (tmp_path / "scylla_test_id_ed25519").exists()

    def test_applies_permissions(self, ks, tmp_path):
        ks.sync(["email_config.json"], str(tmp_path), 0o600)
        mode = os.stat(tmp_path / "email_config.json").st_mode & 0o777
        assert mode == 0o600


# ---------------------------------------------------------------------------
# S3 properties
# ---------------------------------------------------------------------------


class TestS3Properties:
    def test_s3_resource(self, ks):
        assert ks.s3 is not None

    def test_s3_client(self, ks):
        assert ks.s3_client is not None


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_get_file_contents(self, ks):
        errors = []

        def fetch(name):
            try:
                ks.get_file_contents(name)
            except ClientError as exc:
                errors.append(exc)

        names = ["email_config.json", "azure.json", "docker.json", "oci.json"]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(fetch, names * 5))
        assert not errors

    def test_concurrent_s3_client_creation(self, ks):
        clients = []

        def get_client():
            clients.append(ks.s3_client)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: get_client(), range(16)))
        assert len(clients) == 16


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestCaching:
    def test_cache_hit_returns_same_value(self, ks):
        first = ks.get_file_contents("email_config.json")
        second = ks.get_file_contents("email_config.json")
        assert first == second

    def test_cache_hit_does_not_call_s3(self, ks):
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json")
            mock_fetch.assert_not_called()

    def test_different_keys_cached_independently(self, ks):
        email = ks.get_file_contents("email_config.json")
        azure = ks.get_file_contents("azure.json")
        assert email != azure
        assert ks.get_file_contents("email_config.json") == email
        assert ks.get_file_contents("azure.json") == azure

    def test_bypass_cache_forces_fetch(self, ks):
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json", bypass_cache=True)
            mock_fetch.assert_called_once_with("email_config.json")

    def test_bypass_cache_updates_cached_value(self, ks, mocked_s3):
        ks.get_file_contents("email_config.json")
        new_data = json.dumps({"updated": True}).encode()
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "email_config.json").put(Body=new_data)
        result = ks.get_file_contents("email_config.json", bypass_cache=True)
        assert result == new_data
        # Subsequent normal call returns the updated value
        assert ks.get_file_contents("email_config.json") == new_data

    def test_clear_cache_empties_all(self, ks):
        ks.get_file_contents("email_config.json")
        ks.get_file_contents("azure.json")
        ks.clear_cache()
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json")
            ks.get_file_contents("azure.json")
            assert mock_fetch.call_count == 2

    def test_cache_is_per_instance(self, mocked_s3):
        ks1 = KeyStore()
        ks2 = KeyStore()
        ks1.get_file_contents("email_config.json")
        with patch.object(ks2, "_fetch_from_s3", wraps=ks2._fetch_from_s3) as mock_fetch:
            ks2.get_file_contents("email_config.json")
            mock_fetch.assert_called_once()

    def test_cache_thread_safe(self, ks):
        results = []

        def fetch_and_record(name):
            results.append(ks.get_file_contents(name))

        names = ["email_config.json", "azure.json", "docker.json"]
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(fetch_and_record, names * 10))
        assert len(results) == 30

    def test_get_json_uses_cache(self, ks):
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3") as mock_fetch:
            result = ks.get_json("email_config.json")
            mock_fetch.assert_not_called()
        assert result == FAKE_EMAIL_CONFIG


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetry:
    @staticmethod
    def _make_flaky_s3(error, succeed_after, real_data):
        """Build a mock S3 resource whose Object().get() fails N times then succeeds."""
        call_count = 0

        def fake_get():
            nonlocal call_count
            call_count += 1
            if call_count <= succeed_after:
                raise error
            body = MagicMock()
            body.read.return_value = real_data
            return {"Body": body}

        mock_obj = MagicMock()
        mock_obj.get.side_effect = fake_get
        mock_s3 = MagicMock()
        mock_s3.Object.return_value = mock_obj
        return mock_s3, mock_obj

    def test_retries_transient_slowdown(self, ks):
        error = ClientError({"Error": {"Code": "SlowDown", "Message": "Slow down"}}, "GetObject")
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=2, real_data=data)

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            result = ks.get_file_contents("email_config.json")
        assert json.loads(result) == FAKE_EMAIL_CONFIG
        assert mock_obj.get.call_count == 3

    def test_retries_transient_internal_error(self, ks):
        error = ClientError({"Error": {"Code": "InternalError", "Message": "Internal"}}, "GetObject")
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=1, real_data=data)

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            result = ks.get_file_contents("email_config.json")
        assert json.loads(result) == FAKE_EMAIL_CONFIG
        assert mock_obj.get.call_count == 2

    def test_no_retry_on_nosuchkey(self, ks):
        error = ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=99, real_data=b"")

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            with pytest.raises(ClientError) as exc_info:
                ks.get_file_contents("missing.json")
            assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
        assert mock_obj.get.call_count == 1

    def test_no_retry_on_access_denied(self, ks):
        error = ClientError({"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}, "GetObject")
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=99, real_data=b"")

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            with pytest.raises(ClientError) as exc_info:
                ks.get_file_contents("secret.json")
            assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
        assert mock_obj.get.call_count == 1


# ---------------------------------------------------------------------------
# Secrets Manager backend (SCT_KEYSTORE_BACKEND=secretsmanager)
# ---------------------------------------------------------------------------


class TestSecretsManagerBackend:
    """Tests for the Secrets Manager backend behind SCT_KEYSTORE_BACKEND."""

    @pytest.fixture
    def sm_ks(self, mocked_s3):
        """KeyStore configured with secretsmanager backend inside mock_aws."""
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        # Populate SM with the same test data as S3
        json_secrets = {
            "email_config.json": FAKE_EMAIL_CONFIG,
            "azure.json": FAKE_AZURE_CREDS,
            "docker.json": FAKE_DOCKER_HUB,
        }
        for key, data in json_secrets.items():
            sm.create_secret(Name=f"sct/{key}", SecretString=json.dumps(data))
        # Binary secret
        sm.create_secret(Name="sct/scylla_test_id_ed25519", SecretBinary=FAKE_SSH_PRIVATE_KEY)
        sm.create_secret(Name="sct/scylla_test_id_ed25519.pub", SecretBinary=FAKE_SSH_PUBLIC_KEY)

        with patch.dict(
            os.environ,
            {
                "SCT_KEYSTORE_BACKEND": "secretsmanager",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            yield KeyStore()

    def test_get_json_from_sm(self, sm_ks):
        assert sm_ks.get_json("email_config.json") == FAKE_EMAIL_CONFIG

    def test_get_binary_from_sm(self, sm_ks):
        assert sm_ks.get_file_contents("scylla_test_id_ed25519") == FAKE_SSH_PRIVATE_KEY

    def test_get_ssh_key_pair_from_sm(self, sm_ks):
        key = sm_ks.get_ssh_key_pair("scylla_test_id_ed25519")
        assert isinstance(key, SSHKey)
        assert key.private_key == FAKE_SSH_PRIVATE_KEY
        assert key.public_key == FAKE_SSH_PUBLIC_KEY

    def test_caching_works_with_sm(self, sm_ks):
        sm_ks.get_file_contents("email_config.json")
        with patch.object(sm_ks, "_fetch_from_secrets_manager", wraps=sm_ks._fetch_from_secrets_manager) as mock:
            sm_ks.get_file_contents("email_config.json")
            mock.assert_not_called()

    def test_missing_secret_raises(self, sm_ks):
        with pytest.raises(ClientError):
            sm_ks.get_file_contents("nonexistent.json")

    def test_custom_prefix(self, mocked_s3):
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(Name="custom/email_config.json", SecretString=json.dumps(FAKE_EMAIL_CONFIG))
        with patch.dict(
            os.environ,
            {
                "SCT_KEYSTORE_BACKEND": "secretsmanager",
                "SCT_KEYSTORE_SM_PREFIX": "custom/",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            ks = KeyStore()
            assert ks.get_json("email_config.json") == FAKE_EMAIL_CONFIG

    def test_s3_backend_is_default(self, mocked_s3):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SCT_KEYSTORE_BACKEND", None)
            ks = KeyStore()
            assert ks._backend == "s3"

    def test_download_file_from_sm(self, sm_ks, tmp_path):
        dest = str(tmp_path / "key")
        sm_ks.download_file("scylla_test_id_ed25519", dest)
        with open(dest, "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY

    def test_sync_from_sm(self, sm_ks, tmp_path):
        sm_ks.sync(["scylla_test_id_ed25519"], str(tmp_path), 0o600)
        assert (tmp_path / "scylla_test_id_ed25519").exists()
        with open(tmp_path / "scylla_test_id_ed25519", "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY
        # Version sidecar written alongside the key
        assert (tmp_path / "scylla_test_id_ed25519.version").exists()

    def test_get_sm_version_id_returns_awscurrent(self, sm_ks):
        version_id = sm_ks.get_sm_version_id("scylla_test_id_ed25519")
        assert version_id  # non-empty UUID-like string

    def test_get_obj_if_needed_skips_when_version_unchanged(self, sm_ks, tmp_path):
        # First call downloads + writes sidecar
        sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
        first_mtime = os.stat(tmp_path / "scylla_test_id_ed25519").st_mtime

        # Second call with same version — no re-download
        with patch.object(sm_ks, "download_file") as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_not_called()
        assert os.stat(tmp_path / "scylla_test_id_ed25519").st_mtime == first_mtime

    def test_get_obj_if_needed_redownloads_when_version_changes(self, sm_ks, tmp_path):
        sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
        # Tamper the sidecar to simulate a stale/rotated version
        (tmp_path / "scylla_test_id_ed25519.version").write_text("old-version-id")

        with patch.object(sm_ks, "download_file", wraps=sm_ks.download_file) as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_called_once()

    def test_get_obj_if_needed_downloads_when_sidecar_missing(self, sm_ks, tmp_path):
        # Create the key file but no sidecar — should re-download
        (tmp_path / "scylla_test_id_ed25519").write_bytes(FAKE_SSH_PRIVATE_KEY)
        with patch.object(sm_ks, "download_file", wraps=sm_ks.download_file) as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_called_once()
        assert (tmp_path / "scylla_test_id_ed25519.version").exists()


# ---------------------------------------------------------------------------
# Access logging
# ---------------------------------------------------------------------------


class TestAccessLogging:
    def test_cache_hit_logs_debug(self, ks, caplog):
        ks.get_file_contents("email_config.json")
        with caplog.at_level(logging.DEBUG, logger="sdcm.keystore"):
            ks.get_file_contents("email_config.json")
        assert any("cache hit" in r.message and "email_config.json" in r.message for r in caplog.records)
        assert any(r.levelno == logging.DEBUG for r in caplog.records if "cache hit" in r.message)

    def test_fetch_logs_info(self, ks, caplog):
        with caplog.at_level(logging.INFO, logger="sdcm.keystore"):
            ks.get_file_contents("email_config.json")
        assert any("fetched" in r.message and "email_config.json" in r.message for r in caplog.records)
        assert any(r.levelno == logging.INFO for r in caplog.records if "fetched" in r.message)

    def test_slow_fetch_logs_warning(self, ks, caplog):
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        body = MagicMock()
        body.read.return_value = data
        mock_obj = MagicMock()
        mock_obj.get.side_effect = lambda: (time.sleep(0.05) or {"Body": body})  # force small delay
        mock_s3 = MagicMock()
        mock_s3.Object.return_value = mock_obj

        with caplog.at_level(logging.WARNING, logger="sdcm.keystore"):
            # Patch the threshold check: temporarily lower by mocking time.monotonic
            t = [0.0]

            def fake_monotonic():
                t[0] += 1.5  # each call advances 1.5s → elapsed = 3s > 2s threshold
                return t[0]

            with patch("sdcm.keystore.time.monotonic", side_effect=fake_monotonic):
                with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
                    ks.get_file_contents("slow_file.json")
        assert any(r.levelno == logging.WARNING and "slow fetch" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    def test_get_file_contents_missing_key(self, ks):
        with pytest.raises(ClientError):
            ks.get_file_contents("does_not_exist")

    def test_get_json_invalid_content(self, ks, mocked_s3):
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "bad.json").put(Body=b"{{bad")
        with pytest.raises(json.JSONDecodeError):
            ks.get_json("bad.json")

    def test_get_ssh_key_pair_missing_key(self, ks):
        with pytest.raises(ClientError):
            ks.get_ssh_key_pair("nonexistent_key")


# ---------------------------------------------------------------------------
# SSHKey namedtuple
# ---------------------------------------------------------------------------


class TestSSHKeyNamedTuple:
    def test_fields(self):
        key = SSHKey(name="n", public_key=b"pub", private_key=b"priv")
        assert key.name == "n"
        assert key.public_key == b"pub"
        assert key.private_key == b"priv"

    def test_tuple_fields(self):
        assert SSHKey._fields == ("name", "public_key", "private_key")


# ---------------------------------------------------------------------------
# pub_key_from_private_key_file
# ---------------------------------------------------------------------------


class TestPubKeyFromPrivateKeyFile:
    def test_rsa_key(self, tmp_path):
        key = paramiko.RSAKey.generate(2048)
        path = str(tmp_path / "id_rsa")
        key.write_private_key_file(path)
        base64_pub, key_type = pub_key_from_private_key_file(path)
        assert key_type == "ssh-rsa"
        assert base64_pub == key.get_base64()

    def test_ed25519_key(self, tmp_path):
        private_key = Ed25519PrivateKey.generate()
        pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.OpenSSH,
            serialization.NoEncryption(),
        )
        path = tmp_path / "id_ed25519"
        path.write_bytes(pem)
        base64_pub, key_type = pub_key_from_private_key_file(str(path))
        assert key_type == "ssh-ed25519"
        assert len(base64_pub) > 0
