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
import stat
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

from sdcm import keystore as keystore_module
from sdcm.keystore import (
    KEYSTORE_S3_BUCKET,
    KEYSTORE_SM_REGION,
    KeyStore,
    SSHKey,
    is_not_found_error,
    materialize_ssh_key,
    pub_key_from_private_key_file,
)

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


@pytest.fixture(scope="module", autouse=True)
def mock_cloud_services():
    """Restore original KeyStore methods so moto S3 is used.

    The unit_tests/unit/conftest.py defines a session-scoped autouse fixture
    that patches KeyStore at the class level. Under pytest-xdist, if this
    module ends up on the same worker as tests from unit_tests/unit/, those
    patches are active. This autouse module fixture undoes them so our tests
    exercise the real KeyStore implementation against moto.
    """
    saved = {name: KeyStore.__dict__.get(name) for name in _KEYSTORE_ORIGINALS}
    for name, original in _KEYSTORE_ORIGINALS.items():
        setattr(KeyStore, name, original)
    yield
    for name, value in saved.items():
        if value is not None:
            setattr(KeyStore, name, value)


@pytest.fixture(autouse=True)
def default_to_s3_backend():
    """Pin the S3 backend for the legacy-path tests in this module.

    `secretsmanager` is the real default, but most tests here exercise the S3
    code path against the moto bucket -- including ones that never touch a
    KeyStore directly (e.g. `materialize_ssh_key`), so they can't opt in
    themselves. Tests that care about the true default or about Secrets Manager
    override this env var explicitly.
    """
    with patch.dict(os.environ, {"SCT_KEYSTORE_BACKEND": "s3"}):
        yield


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


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)


@pytest.fixture(scope="module")
def mocked_s3():
    """Provide a moto S3 resource with the keystore bucket populated with the fake credential objects."""
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        _populate_bucket(s3)
        yield s3


@pytest.fixture(autouse=True)
def _clear_keystore_cache():
    """The cache is process-wide, so it has to be dropped between tests.

    Without this, `test_bypass_cache_updates_cached_value` (which rewrites `email_config.json` in
    the bucket and then restores it) leaves the rewritten bytes cached for every later test.
    """
    KeyStore.clear_cache(KeyStore())
    yield
    KeyStore.clear_cache(KeyStore())


@pytest.fixture
def ks(mocked_s3):
    """Return a fresh KeyStore inside the moto context (S3 backend, see autouse fixture)."""
    return KeyStore()


# ---------------------------------------------------------------------------
# get_file_contents
# ---------------------------------------------------------------------------


class TestGetFileContents:
    """Cover KeyStore.get_file_contents reading raw object bytes from S3."""

    def test_returns_bytes(self, ks):
        """get_file_contents returns the object content as bytes."""
        assert isinstance(ks.get_file_contents("email_config.json"), bytes)

    def test_returns_correct_json_bytes(self, ks):
        """get_file_contents returns the exact JSON bytes stored in the bucket."""
        assert json.loads(ks.get_file_contents("email_config.json")) == FAKE_EMAIL_CONFIG

    def test_returns_binary_content(self, ks):
        """get_file_contents returns binary objects such as private keys byte for byte."""
        assert ks.get_file_contents("scylla_test_id_ed25519") == FAKE_SSH_PRIVATE_KEY

    def test_missing_key_raises_client_error(self, ks):
        """Reading a missing object raises ClientError with the NoSuchKey code."""
        with pytest.raises(ClientError) as exc_info:
            ks.get_file_contents("nonexistent.json")
        assert "NoSuchKey" in str(exc_info.value)


# ---------------------------------------------------------------------------
# get_json
# ---------------------------------------------------------------------------


class TestGetJson:
    """Cover KeyStore.get_json decoding keystore objects as JSON."""

    def test_returns_dict(self, ks):
        """get_json decodes a JSON object into a dict."""
        assert ks.get_json("email_config.json") == FAKE_EMAIL_CONFIG

    def test_returns_list(self, ks):
        """get_json decodes a JSON array into a list."""
        assert ks.get_json("qa_users.json") == FAKE_QA_USERS

    def test_invalid_json_raises(self, ks, mocked_s3):
        """get_json raises JSONDecodeError for malformed JSON content."""
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "bad.json").put(Body=b"{not json")
        with pytest.raises(json.JSONDecodeError):
            ks.get_json("bad.json")

    def test_empty_object_is_valid(self, ks, mocked_s3):
        """get_json decodes an empty JSON object into an empty dict."""
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "empty.json").put(Body=b"{}")
        assert ks.get_json("empty.json") == {}


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile:
    """Cover KeyStore.download_file writing keystore objects to local files."""

    def test_writes_json_to_disk(self, ks, tmp_path):
        """download_file writes a JSON object to the destination path."""
        dest = str(tmp_path / "out.json")
        ks.download_file("email_config.json", dest)
        with open(dest, "rb") as fh:
            assert json.loads(fh.read()) == FAKE_EMAIL_CONFIG

    def test_writes_binary_to_disk(self, ks, tmp_path):
        """download_file writes a binary object to the destination path byte for byte."""
        dest = str(tmp_path / "key")
        ks.download_file("scylla_test_id_ed25519", dest)
        with open(dest, "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY


# ---------------------------------------------------------------------------
# materialize_ssh_key
# ---------------------------------------------------------------------------


class TestMaterializeSshKey:
    """Cover materialize_ssh_key writing a private key to a temporary file for the block's lifetime."""

    def test_writes_key_bytes(self, mocked_s3):
        """The materialized key file holds the private key bytes from the keystore."""
        with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
            assert key_path.read_bytes() == FAKE_SSH_PRIVATE_KEY

    def test_sets_0600_perms(self, mocked_s3):
        """The materialized key file has 0600 permissions, as ssh requires."""
        with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
            mode = key_path.stat().st_mode & 0o777
        assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    def test_removes_file_on_exit(self, mocked_s3):
        """The materialized key file is removed when the context exits normally."""
        with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
            captured = key_path
            assert captured.exists()
        assert not captured.exists()

    def test_removes_file_on_exception(self, mocked_s3):
        """The materialized key file is removed when the block raises an exception."""
        key_path_holder = []
        with pytest.raises(RuntimeError, match="boom"):
            with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
                key_path_holder.append(key_path)
                raise RuntimeError("boom")
        assert key_path_holder and not key_path_holder[0].exists()

    def test_prefers_tmpfs_when_available(self, mocked_s3):
        """On Linux with /dev/shm available, keys land on tmpfs."""
        if not os.path.isdir("/dev/shm"):
            pytest.skip("no /dev/shm on this platform")
        with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
            assert str(key_path).startswith("/dev/shm/"), f"expected /dev/shm placement, got {key_path}"

    def test_falls_back_when_tmpfs_missing(self, mocked_s3, monkeypatch):
        """Falls back to the system temp dir when /dev/shm is not a directory."""
        fake_tmpfs = keystore_module.Path("/nonexistent-sct-tmpfs-xyz")
        monkeypatch.setattr(keystore_module, "_TMPFS_PATH", fake_tmpfs)
        with materialize_ssh_key("scylla_test_id_ed25519") as key_path:
            assert not str(key_path).startswith("/dev/shm/")
            assert key_path.read_bytes() == FAKE_SSH_PRIVATE_KEY


# ---------------------------------------------------------------------------
# Credential getter methods
# ---------------------------------------------------------------------------


class TestCredentialGetters:
    """Cover the KeyStore getters that return each service's credentials from their keystore objects."""

    def test_email(self, ks):
        """get_email_credentials returns the email_config.json content."""
        assert ks.get_email_credentials() == FAKE_EMAIL_CONFIG

    def test_gcp_default_project(self, ks, monkeypatch):
        """get_gcp_credentials reads gcp-sct-project-1.json when SCT_GCE_PROJECT is unset."""
        monkeypatch.delenv("SCT_GCE_PROJECT", raising=False)
        assert ks.get_gcp_credentials() == FAKE_GCP_CREDENTIALS

    def test_gcp_custom_project(self, ks, mocked_s3):
        """get_gcp_credentials reads <project>.json for the project named by SCT_GCE_PROJECT."""
        custom = {"project_id": "custom"}
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "my-project.json").put(Body=json.dumps(custom).encode())
        with patch.dict(os.environ, {"SCT_GCE_PROJECT": "my-project"}):
            assert ks.get_gcp_credentials() == custom

    def test_dbaaslab_gcp(self, ks):
        """get_dbaaslab_gcp_credentials returns the gcp-scylladbaaslab.json content."""
        assert ks.get_dbaaslab_gcp_credentials() == FAKE_GCP_DBAASLAB

    def test_gcp_service_accounts_adds_cloud_platform_scope(self, ks):
        """get_gcp_service_accounts adds the cloud-platform scope while keeping the existing scopes."""
        result = ks.get_gcp_service_accounts()
        assert "https://www.googleapis.com/auth/cloud-platform" in result[0]["scopes"]
        assert "https://www.googleapis.com/auth/compute" in result[0]["scopes"]

    def test_gcp_service_accounts_no_duplicate_scope(self, ks, mocked_s3):
        """get_gcp_service_accounts keeps a single cloud-platform scope when it is already present."""
        sa_data = [{"email": "x", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}]
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "gcp-sct-project-1_service_accounts.json").put(
            Body=json.dumps(sa_data).encode()
        )
        result = ks.get_gcp_service_accounts()
        assert result[0]["scopes"].count("https://www.googleapis.com/auth/cloud-platform") == 1

    def test_scylladb_upload(self, ks):
        """get_scylladb_upload_credentials returns the scylladb_upload.json content."""
        assert ks.get_scylladb_upload_credentials() == FAKE_SCYLLADB_UPLOAD

    def test_qa_users(self, ks):
        """get_qa_users returns the qa_users.json content."""
        assert ks.get_qa_users() == FAKE_QA_USERS

    def test_acl_grantees(self, ks):
        """get_acl_grantees returns the bucket-users.json content."""
        assert ks.get_acl_grantees() == FAKE_ACL_GRANTEES

    def test_ldap_ms_ad(self, ks):
        """get_ldap_ms_ad_credentials returns the ldap_ms_ad.json content."""
        assert ks.get_ldap_ms_ad_credentials() == FAKE_LDAP_MS_AD

    def test_backup_azure_blob(self, ks):
        """get_backup_azure_blob_credentials returns the backup_azure_blob.json content."""
        assert ks.get_backup_azure_blob_credentials() == FAKE_BACKUP_AZURE_BLOB

    def test_docker_hub(self, ks):
        """get_docker_hub_credentials returns the docker.json content."""
        assert ks.get_docker_hub_credentials() == FAKE_DOCKER_HUB

    def test_azure(self, ks):
        """get_azure_credentials returns the azure.json content."""
        assert ks.get_azure_credentials() == FAKE_AZURE_CREDS

    def test_azure_kms(self, ks):
        """get_azure_kms_config returns the azure_kms_config.json content."""
        assert ks.get_azure_kms_config() == FAKE_AZURE_KMS

    def test_gcp_kms(self, ks):
        """get_gcp_kms_config returns the gcp_kms_config.json content."""
        assert ks.get_gcp_kms_config() == FAKE_GCP_KMS

    def test_baremetal(self, ks):
        """get_baremetal_config returns the content of the named <config>.json object."""
        assert ks.get_baremetal_config("baremetal_config") == FAKE_BAREMETAL

    def test_cloud_rest(self, ks):
        """get_cloud_rest_credentials returns the credentials for the requested environment."""
        assert ks.get_cloud_rest_credentials(environment="lab") == FAKE_CLOUD_REST

    def test_jira(self, ks):
        """get_jira_credentials returns the scylladb_jira.json content."""
        assert ks.get_jira_credentials() == FAKE_JIRA_CREDS


# ---------------------------------------------------------------------------
# SSH key methods
# ---------------------------------------------------------------------------


class TestSSHKeys:
    """Cover KeyStore SSH key pair retrieval."""

    def test_get_ssh_key_pair_returns_namedtuple(self, ks):
        """get_ssh_key_pair returns an SSHKey with the name and the public and private key bytes."""
        key = ks.get_ssh_key_pair("scylla_test_id_ed25519")
        assert isinstance(key, SSHKey)
        assert key.name == "scylla_test_id_ed25519"
        assert key.public_key == FAKE_SSH_PUBLIC_KEY
        assert key.private_key == FAKE_SSH_PRIVATE_KEY

    def test_qa_ssh_keys(self, ks):
        """get_qa_ssh_keys returns the QA key pair as a list of SSHKey."""
        result = ks.get_qa_ssh_keys()
        assert len(result) == 1
        assert isinstance(result[0], SSHKey)

    def test_missing_ssh_key_raises(self, ks):
        """get_ssh_key_pair raises ClientError for a key missing from the keystore."""
        with pytest.raises(ClientError):
            ks.get_ssh_key_pair("nonexistent_key")


# ---------------------------------------------------------------------------
# OCI credentials (local config fallback)
# ---------------------------------------------------------------------------


class TestOCICredentials:
    """Cover get_oci_credentials choosing between the keystore and a local OCI config file."""

    def test_from_s3_when_no_env_var(self, ks, monkeypatch):
        """Without SCT_OCI_CONFIG_PATH, OCI credentials come from oci.json in the keystore."""
        monkeypatch.delenv("SCT_OCI_CONFIG_PATH", raising=False)
        assert ks.get_oci_credentials() == FAKE_OCI_CREDS

    def test_from_local_config(self, ks, tmp_path):
        """With SCT_OCI_CONFIG_PATH set, OCI credentials are parsed from that local config file."""
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
        """A SCT_OCI_CONFIG_PATH pointing to a missing file falls back to the keystore credentials."""
        with patch.dict(os.environ, {"SCT_OCI_CONFIG_PATH": "/no/such/file"}):
            assert ks.get_oci_credentials() == FAKE_OCI_CREDS


# ---------------------------------------------------------------------------
# _parse_local_oci_config (static method)
# ---------------------------------------------------------------------------


class TestParseLocalOCIConfig:
    """Cover KeyStore._parse_local_oci_config turning an OCI CLI config into a credentials dict."""

    def test_parses_all_fields(self, tmp_path):
        """All profile fields are parsed and key_file is replaced by the key file's content."""
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
        """compartment_id defaults to the tenancy when the profile omits it."""
        key_file = tmp_path / "key.pem"
        key_file.write_text("pem")
        config = tmp_path / "config"
        config.write_text(f"[DEFAULT]\ntenancy=my-tenancy\nuser=u\nfingerprint=fp\nkey_file={key_file}\nregion=r\n")
        assert KeyStore._parse_local_oci_config(str(config))["compartment_id"] == "my-tenancy"

    def test_missing_profile_raises(self, tmp_path):
        """Requesting a profile absent from the config raises ValueError."""
        config = tmp_path / "config"
        config.write_text("[DEFAULT]\ntenancy=t\n")
        with pytest.raises(ValueError, match="Profile 'NONEXISTENT' not found"):
            KeyStore._parse_local_oci_config(str(config), profile="NONEXISTENT")

    def test_custom_profile(self, tmp_path):
        """The requested non-default profile section is the one parsed."""
        key_file = tmp_path / "key.pem"
        key_file.write_text("custom-pem")
        config = tmp_path / "config"
        config.write_text(f"[CUSTOM]\ntenancy=ct\nuser=cu\nfingerprint=cf\nkey_file={key_file}\nregion=cr\n")
        result = KeyStore._parse_local_oci_config(str(config), profile="CUSTOM")
        assert result["tenancy"] == "ct"

    def test_missing_key_file_raises(self, tmp_path):
        """A key_file path that does not exist raises ValueError."""
        config = tmp_path / "config"
        config.write_text("[DEFAULT]\ntenancy=t\nuser=u\nfingerprint=f\nkey_file=/no/key\nregion=r\n")
        with pytest.raises(ValueError, match="OCI key file not found"):
            KeyStore._parse_local_oci_config(str(config))


# ---------------------------------------------------------------------------
# Argus credentials (Jenkins / provider logic)
# ---------------------------------------------------------------------------


class TestArgusCredentials:
    """Cover get_argus_rest_credentials_per_provider choosing per-provider or generic Argus credentials."""

    def test_jenkins_with_matching_provider(self, ks):
        """In Jenkins, the credentials of the detected cloud provider are returned."""
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="aws"):
                assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_AWS_CREDS

    def test_jenkins_fallback_on_missing_provider_file(self, ks):
        """In Jenkins, a provider without its own credentials file falls back to the generic credentials."""
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="no_such_provider"):
                assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_CREDS

    def test_outside_jenkins(self, ks, monkeypatch):
        """Outside Jenkins the generic Argus credentials are returned."""
        monkeypatch.delenv("JOB_NAME", raising=False)
        assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_CREDS

    def test_explicit_provider_overrides_detection(self, ks):
        """An explicit cloud_provider argument selects that provider's credentials."""
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            result = ks.get_argus_rest_credentials_per_provider(cloud_provider="aws")
            assert result == FAKE_ARGUS_AWS_CREDS

    def test_jenkins_non_nosuchkey_error_propagates(self, ks):
        """Errors other than not-found, such as AccessDenied, propagate from the per-provider lookup."""
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="aws"):
                with patch.object(ks, "get_json") as mock_gj:
                    error_resp = {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}
                    mock_gj.side_effect = ClientError(error_resp, "GetObject")
                    with pytest.raises(ClientError) as exc_info:
                        ks.get_argus_rest_credentials_per_provider()
                    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"

    def test_jenkins_fallback_on_secrets_manager_resource_not_found(self, ks):
        """A provider with no dedicated secret must fall back, not blow up.

        Secrets Manager reports a missing secret as ResourceNotFoundException
        rather than S3's NoSuchKey. Only recognising NoSuchKey made every
        non-AWS provider fail to initialise its Argus client.
        """
        with patch.dict(os.environ, {"JOB_NAME": "test-job"}):
            with patch("sdcm.keystore.provider", return_value="gce"):
                real_get_json = ks.get_json
                error_resp = {"Error": {"Code": "ResourceNotFoundException", "Message": "can't find secret"}}

                def fake_get_json(name):
                    if name == "argus_rest_credentials_sct_gce.json":
                        raise ClientError(error_resp, "GetSecretValue")
                    return real_get_json(name)

                with patch.object(ks, "get_json", side_effect=fake_get_json):
                    assert ks.get_argus_rest_credentials_per_provider() == FAKE_ARGUS_CREDS


class TestIsNotFoundError:
    """Cover is_not_found_error recognising missing-entry errors from the S3 and Secrets Manager backends."""

    @pytest.mark.parametrize("code", ["NoSuchKey", "404", "ResourceNotFoundException"])
    def test_recognises_missing_entry_codes_from_both_backends(self, code):
        """NoSuchKey, 404 and ResourceNotFoundException are all recognised as not-found errors."""
        err = ClientError({"Error": {"Code": code, "Message": "missing"}}, "Get")
        assert is_not_found_error(err) is True

    @pytest.mark.parametrize("code", ["AccessDenied", "ThrottlingException", "InternalError"])
    def test_other_codes_are_not_not_found(self, code):
        """Other ClientError codes are reported as something other than not-found."""
        err = ClientError({"Error": {"Code": code, "Message": "nope"}}, "Get")
        assert is_not_found_error(err) is False

    def test_non_client_error_is_not_not_found(self):
        """Exceptions other than ClientError are reported as something other than not-found."""
        assert is_not_found_error(ValueError("boom")) is False


# ---------------------------------------------------------------------------
# calculate_s3_etag (static method)
# ---------------------------------------------------------------------------


class TestCalculateS3Etag:
    """Cover KeyStore.calculate_s3_etag computing S3-compatible ETags."""

    def test_single_part(self):
        """Data within one chunk gets the quoted plain MD5 hex digest as its ETag."""
        data = b"hello world"
        expected = f'"{hashlib.md5(data).hexdigest()}"'
        assert KeyStore.calculate_s3_etag(io.BytesIO(data)) == expected

    def test_multi_part_format(self):
        """Multi-chunk data gets a quoted ETag with a -<part count> suffix."""
        chunk = 8
        data = b"a" * 20  # 3 chunks: 8 + 8 + 4
        result = KeyStore.calculate_s3_etag(io.BytesIO(data), chunk_size=chunk)
        assert result.startswith('"') and result.endswith('"')
        assert "-3" in result

    def test_multi_part_correctness(self):
        """A multi-part ETag is the MD5 of the concatenated part digests plus the part count."""
        chunk = 8
        data = b"abcdefghijklmnop"  # 2 chunks of 8
        md5_parts = [hashlib.md5(data[i : i + chunk]).digest() for i in range(0, len(data), chunk)]
        combined_md5 = hashlib.md5(b"".join(md5_parts)).hexdigest()
        expected = f'"{combined_md5}-{len(md5_parts)}"'
        assert KeyStore.calculate_s3_etag(io.BytesIO(data), chunk_size=chunk) == expected

    def test_empty_file(self):
        """An empty file gets the quoted MD5 of empty bytes with a -0 suffix."""
        # Empty file has 0 chunks → md5 of empty bytes with "-0" suffix
        result = KeyStore.calculate_s3_etag(io.BytesIO(b""))
        expected_md5 = hashlib.md5(b"").hexdigest()
        assert result == f'"{expected_md5}-0"'


# ---------------------------------------------------------------------------
# get_object_etag
# ---------------------------------------------------------------------------


class TestGetObjectEtag:
    """Cover KeyStore.get_object_etag reading an object's ETag from S3."""

    def test_returns_quoted_string(self, ks):
        """get_object_etag returns the ETag as a quoted string."""
        etag = ks.get_object_etag("email_config.json")
        assert etag.startswith('"') and etag.endswith('"')

    def test_missing_key_raises(self, ks):
        """get_object_etag raises ClientError for a missing object."""
        with pytest.raises(ClientError):
            ks.get_object_etag("nonexistent")


# ---------------------------------------------------------------------------
# get_obj_if_needed / sync
# ---------------------------------------------------------------------------


class TestGetObjIfNeeded:
    """Cover KeyStore.get_obj_if_needed downloading an object only when the local copy is stale."""

    def test_downloads_missing_file(self, ks, tmp_path):
        """A file missing locally is downloaded into the target directory."""
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert (tmp_path / "email_config.json").exists()

    def test_skips_when_etag_matches(self, ks, tmp_path):
        """A local file whose ETag matches the object is left as it is."""
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        first = (tmp_path / "email_config.json").read_bytes()
        # Second call — file unchanged, ETag matches, no re-download
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert (tmp_path / "email_config.json").read_bytes() == first

    def test_redownloads_when_content_changed(self, ks, tmp_path):
        """A local file whose content differs from the object is downloaded again."""
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        (tmp_path / "email_config.json").write_bytes(b"tampered")
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o644)
        assert json.loads((tmp_path / "email_config.json").read_bytes()) == FAKE_EMAIL_CONFIG

    def test_creates_subdirectories(self, ks, tmp_path):
        """Missing parent directories of the target directory are created."""
        deep = str(tmp_path / "a" / "b")
        ks.get_obj_if_needed("email_config.json", deep, 0o644)
        assert os.path.exists(os.path.join(deep, "email_config.json"))

    def test_sets_permissions(self, ks, tmp_path):
        """The downloaded file gets the requested permissions."""
        ks.get_obj_if_needed("email_config.json", str(tmp_path), 0o600)
        mode = os.stat(tmp_path / "email_config.json").st_mode & 0o777
        assert mode == 0o600


class TestSync:
    """Cover KeyStore.sync downloading a list of keystore objects into a directory."""

    def test_downloads_multiple_keys(self, ks, tmp_path):
        """KeyStore.sync downloads every listed object into the target directory."""
        ks.sync(["email_config.json", "scylla_test_id_ed25519"], str(tmp_path), 0o600)
        assert (tmp_path / "email_config.json").exists()
        assert (tmp_path / "scylla_test_id_ed25519").exists()

    def test_applies_permissions(self, ks, tmp_path):
        """KeyStore.sync applies the requested permissions to the downloaded files."""
        ks.sync(["email_config.json"], str(tmp_path), 0o600)
        mode = os.stat(tmp_path / "email_config.json").st_mode & 0o777
        assert mode == 0o600


# ---------------------------------------------------------------------------
# S3 properties
# ---------------------------------------------------------------------------


class TestS3Properties:
    """Cover the lazily created S3 resource and client properties."""

    def test_s3_resource(self, ks):
        """The s3 property returns a boto3 S3 resource."""
        assert ks.s3 is not None

    def test_s3_client(self, ks):
        """The s3_client property returns a boto3 S3 client."""
        assert ks.s3_client is not None


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Cover concurrent use of a single KeyStore from multiple threads."""

    def test_concurrent_get_file_contents(self, ks):
        """Concurrent get_file_contents calls on several objects all succeed."""
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
        """Concurrent access to s3_client returns a client to every caller."""
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
    """Cover the process-wide KeyStore content cache and its keys."""

    def test_cache_hit_returns_same_value(self, ks):
        """Repeated reads of an object return the same content."""
        first = ks.get_file_contents("email_config.json")
        second = ks.get_file_contents("email_config.json")
        assert first == second

    def test_cache_hit_does_not_call_s3(self, ks):
        """A cached object is served without another S3 fetch."""
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json")
            mock_fetch.assert_not_called()

    def test_different_keys_cached_independently(self, ks):
        """Different objects are cached under separate entries."""
        email = ks.get_file_contents("email_config.json")
        azure = ks.get_file_contents("azure.json")
        assert email != azure
        assert ks.get_file_contents("email_config.json") == email
        assert ks.get_file_contents("azure.json") == azure

    def test_bypass_cache_forces_fetch(self, ks):
        """bypass_cache=True fetches the object from S3 even when it is cached."""
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json", bypass_cache=True)
            mock_fetch.assert_called_once_with("email_config.json")

    def test_bypass_cache_updates_cached_value(self, ks, mocked_s3):
        """A bypass_cache fetch replaces the cached value with the new content."""
        ks.get_file_contents("email_config.json")
        new_data = json.dumps({"updated": True}).encode()
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "email_config.json").put(Body=new_data)
        result = ks.get_file_contents("email_config.json", bypass_cache=True)
        assert result == new_data
        assert ks.get_file_contents("email_config.json") == new_data
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "email_config.json").put(Body=json.dumps(FAKE_EMAIL_CONFIG).encode())

    def test_clear_cache_empties_all(self, ks):
        """clear_cache drops every cached entry, so later reads fetch again."""
        ks.get_file_contents("email_config.json")
        ks.get_file_contents("azure.json")
        ks.clear_cache()
        with patch.object(ks, "_fetch_from_s3", wraps=ks._fetch_from_s3) as mock_fetch:
            ks.get_file_contents("email_config.json")
            ks.get_file_contents("azure.json")
            assert mock_fetch.call_count == 2

    def test_cache_is_shared_between_instances(self, mocked_s3):
        """The whole point: nothing in SCT keeps a KeyStore, so a per-instance cache never hit."""
        ks1 = KeyStore()
        ks2 = KeyStore()
        ks1.get_file_contents("email_config.json")
        with patch.object(ks2, "_fetch_from_s3", wraps=ks2._fetch_from_s3) as mock_fetch:
            ks2.get_file_contents("email_config.json")
            mock_fetch.assert_not_called()

    def test_the_real_call_shape_only_fetches_once(self, mocked_s3):
        """`KeyStore().get_json(...)` repeated is how every call site in SCT is actually written."""
        with patch.object(KeyStore, "_fetch_from_s3", autospec=True, side_effect=KeyStore._fetch_from_s3) as fetch:
            for _ in range(4):
                KeyStore().get_json("email_config.json")
        assert fetch.call_count == 1

    def test_backends_do_not_serve_each_others_bytes(self, mocked_s3, monkeypatch):
        """A name means different content per backend, so the key cannot be the name alone."""
        s3_store = KeyStore(backend="s3")
        s3_store.get_file_contents("email_config.json")

        sm_store = KeyStore(backend="secretsmanager")
        with patch.object(sm_store, "_fetch_from_secrets_manager", return_value=b"{}") as sm_fetch:
            sm_store.get_file_contents("email_config.json")
            sm_fetch.assert_called_once(), "the S3 entry must not satisfy a Secrets Manager read"

    def test_prefix_is_part_of_the_key(self, mocked_s3, monkeypatch):
        """`SCT_KEYSTORE_SM_PREFIX` selects a different secret for the same file name."""
        monkeypatch.setenv("SCT_KEYSTORE_BACKEND", "secretsmanager")
        monkeypatch.setenv("SCT_KEYSTORE_SM_PREFIX", "prod/")
        with patch.object(KeyStore, "_fetch_from_secrets_manager", return_value=b"{}") as fetch:
            KeyStore().get_file_contents("email_config.json")
            monkeypatch.setenv("SCT_KEYSTORE_SM_PREFIX", "staging/")
            KeyStore().get_file_contents("email_config.json")
        assert fetch.call_count == 2

    def test_endpoint_url_is_part_of_the_key(self, mocked_s3, monkeypatch):
        """Bytes read through a moto/emulator endpoint must not be served once it is unset.

        The integration suite seeds a moto keystore with fake credentials; with the endpoint out of
        the key, every later module got those fakes and sent them to real AWS and GCE.
        """
        monkeypatch.setenv("SCT_KEYSTORE_BACKEND", "secretsmanager")
        with patch.object(KeyStore, "_fetch_from_secrets_manager", side_effect=[b"fake", b"real"]) as fetch:
            monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:5000")
            assert KeyStore().get_file_contents("aws_images_role.json") == b"fake"
            monkeypatch.delenv("AWS_ENDPOINT_URL")
            assert KeyStore().get_file_contents("aws_images_role.json") == b"real"
        assert fetch.call_count == 2

    def test_cache_thread_safe(self, ks):
        """Concurrent cached reads from many threads all return content."""
        results = []

        def fetch_and_record(name):
            results.append(ks.get_file_contents(name))

        names = ["email_config.json", "azure.json", "docker.json"]
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(fetch_and_record, names * 10))
        assert len(results) == 30

    def test_get_json_uses_cache(self, ks):
        """get_json decodes the cached content without fetching from S3."""
        ks.get_file_contents("email_config.json")
        with patch.object(ks, "_fetch_from_s3") as mock_fetch:
            result = ks.get_json("email_config.json")
            mock_fetch.assert_not_called()
        assert result == FAKE_EMAIL_CONFIG


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetry:
    """Cover the retry policy of S3 fetches."""

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
        """A SlowDown error is retried until the fetch succeeds."""
        error = ClientError({"Error": {"Code": "SlowDown", "Message": "Slow down"}}, "GetObject")
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=2, real_data=data)

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            result = ks.get_file_contents("email_config.json")
        assert json.loads(result) == FAKE_EMAIL_CONFIG
        assert mock_obj.get.call_count == 3

    def test_retries_transient_internal_error(self, ks):
        """An InternalError is retried until the fetch succeeds."""
        error = ClientError({"Error": {"Code": "InternalError", "Message": "Internal"}}, "GetObject")
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=1, real_data=data)

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            result = ks.get_file_contents("email_config.json")
        assert json.loads(result) == FAKE_EMAIL_CONFIG
        assert mock_obj.get.call_count == 2

    def test_no_retry_on_nosuchkey(self, ks):
        """A NoSuchKey error is raised after a single attempt."""
        error = ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
        mock_s3, mock_obj = self._make_flaky_s3(error, succeed_after=99, real_data=b"")

        with patch.object(type(ks), "s3", new_callable=PropertyMock, return_value=mock_s3):
            with pytest.raises(ClientError) as exc_info:
                ks.get_file_contents("missing.json")
            assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
        assert mock_obj.get.call_count == 1

    def test_no_retry_on_access_denied(self, ks):
        """An AccessDenied error is raised after a single attempt."""
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
    """Cover the AWS Secrets Manager keystore backend."""

    @pytest.fixture(scope="module")
    def _sm_secrets(self, mocked_s3):
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        json_secrets = {
            "email_config.json": FAKE_EMAIL_CONFIG,
            "azure.json": FAKE_AZURE_CREDS,
            "docker.json": FAKE_DOCKER_HUB,
        }
        for key, data in json_secrets.items():
            sm.create_secret(Name=f"sct/{key}", SecretString=json.dumps(data))
        sm.create_secret(Name="sct/scylla_test_id_ed25519", SecretBinary=FAKE_SSH_PRIVATE_KEY)
        sm.create_secret(Name="sct/scylla_test_id_ed25519.pub", SecretBinary=FAKE_SSH_PUBLIC_KEY)

    @pytest.fixture
    def sm_ks(self, _sm_secrets):
        """Provide a KeyStore that uses the Secrets Manager backend with the test secrets created."""
        with patch.dict(
            os.environ,
            {
                "SCT_KEYSTORE_BACKEND": "secretsmanager",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
        ):
            yield KeyStore()

    def test_get_json_from_sm(self, sm_ks):
        """get_json decodes a JSON secret stored as SecretString."""
        assert sm_ks.get_json("email_config.json") == FAKE_EMAIL_CONFIG

    def test_get_binary_from_sm(self, sm_ks):
        """get_file_contents returns a binary secret stored as SecretBinary."""
        assert sm_ks.get_file_contents("scylla_test_id_ed25519") == FAKE_SSH_PRIVATE_KEY

    def test_get_ssh_key_pair_from_sm(self, sm_ks):
        """get_ssh_key_pair reads both halves of the key pair from Secrets Manager."""
        key = sm_ks.get_ssh_key_pair("scylla_test_id_ed25519")
        assert isinstance(key, SSHKey)
        assert key.private_key == FAKE_SSH_PRIVATE_KEY
        assert key.public_key == FAKE_SSH_PUBLIC_KEY

    def test_caching_works_with_sm(self, sm_ks):
        """A cached secret is served without another Secrets Manager fetch."""
        sm_ks.get_file_contents("email_config.json")
        with patch.object(sm_ks, "_fetch_from_secrets_manager", wraps=sm_ks._fetch_from_secrets_manager) as mock:
            sm_ks.get_file_contents("email_config.json")
            mock.assert_not_called()

    def test_missing_secret_raises(self, sm_ks):
        """Reading a secret that does not exist raises ClientError."""
        with pytest.raises(ClientError):
            sm_ks.get_file_contents("nonexistent.json")

    def test_custom_prefix(self, mocked_s3):
        """SCT_KEYSTORE_SM_PREFIX replaces the default secret name prefix."""
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

    def test_sm_backend_is_default(self, mocked_s3, monkeypatch):
        """Secrets Manager is the backend when SCT_KEYSTORE_BACKEND is unset."""
        monkeypatch.delenv("SCT_KEYSTORE_BACKEND", raising=False)
        ks = KeyStore()
        assert ks._backend == "secretsmanager"

    def test_s3_backend_via_explicit_override(self, mocked_s3, monkeypatch):
        """SCT_KEYSTORE_BACKEND=s3 selects the S3 backend."""
        monkeypatch.setenv("SCT_KEYSTORE_BACKEND", "s3")
        ks = KeyStore()
        assert ks._backend == "s3"

    @pytest.mark.parametrize("env_backend", ["secretsmanager", "s3", None])
    def test_constructor_backend_wins_over_env(self, mocked_s3, monkeypatch, env_backend):
        """A constructor-pinned backend ignores SCT_KEYSTORE_BACKEND entirely.

        The `issues/` bulk cache exists in S3 only -- it is too large for a
        Secrets Manager secret and is written by a workflow that uploads to S3
        -- so its readers pin `backend="s3"` and must not follow the config.
        """
        if env_backend is None:
            monkeypatch.delenv("SCT_KEYSTORE_BACKEND", raising=False)
        else:
            monkeypatch.setenv("SCT_KEYSTORE_BACKEND", env_backend)
        assert KeyStore(backend="s3")._backend == "s3"

    def test_s3_pinned_instance_reads_from_s3_under_sm_default(self, mocked_s3, monkeypatch):
        """The pin has to change where bytes actually come from, not just `_backend`."""
        monkeypatch.delenv("SCT_KEYSTORE_BACKEND", raising=False)
        boto3.client("s3").put_object(
            Bucket=KEYSTORE_S3_BUCKET, Key="issues/scylladb_scylladb.csv", Body=b"1,open,bug,title\n"
        )
        # No sct/issues/... secret exists, so an unpinned KeyStore would miss here.
        assert KeyStore(backend="s3").get_file_contents("issues/scylladb_scylladb.csv") == b"1,open,bug,title\n"

    def test_sm_region_defaults_without_aws_region_env(self, mocked_s3, monkeypatch):
        """Secrets Manager is regional; the client must not depend on AWS_DEFAULT_REGION.

        `boto3.client("s3")` silently falls back to us-east-1, but Secrets Manager
        raises NoRegionError, so the region has to be pinned by KeyStore itself.
        """
        monkeypatch.delenv("SCT_KEYSTORE_BACKEND", raising=False)
        monkeypatch.delenv("SCT_KEYSTORE_SM_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("AWS_REGION", raising=False)
        ks = KeyStore()
        assert ks._sm_region == KEYSTORE_SM_REGION
        assert ks.sm_client.meta.region_name == KEYSTORE_SM_REGION

    def test_sm_region_env_override(self, mocked_s3, monkeypatch):
        """SCT_KEYSTORE_SM_REGION sets the region of the Secrets Manager client."""
        monkeypatch.setenv("SCT_KEYSTORE_SM_REGION", "eu-west-1")
        ks = KeyStore()
        assert ks.sm_client.meta.region_name == "eu-west-1"

    def test_download_file_from_sm(self, sm_ks, tmp_path):
        """download_file writes a Secrets Manager binary secret to the destination path."""
        dest = str(tmp_path / "key")
        sm_ks.download_file("scylla_test_id_ed25519", dest)
        with open(dest, "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY

    def test_sync_from_sm(self, sm_ks, tmp_path):
        """KeyStore.sync writes the secret to disk together with a .version sidecar file."""
        sm_ks.sync(["scylla_test_id_ed25519"], str(tmp_path), 0o600)
        assert (tmp_path / "scylla_test_id_ed25519").exists()
        with open(tmp_path / "scylla_test_id_ed25519", "rb") as fh:
            assert fh.read() == FAKE_SSH_PRIVATE_KEY
        # Version sidecar written alongside the key
        assert (tmp_path / "scylla_test_id_ed25519.version").exists()

    def test_sync_from_sm_applies_permissions_to_sidecar(self, sm_ks, tmp_path):
        """The sidecar must not inherit the umask - ssh rejects a group/world readable ~/.ssh."""
        sm_ks.sync(["scylla_test_id_ed25519"], str(tmp_path), 0o600)
        for name in ("scylla_test_id_ed25519", "scylla_test_id_ed25519.version"):
            assert stat.S_IMODE((tmp_path / name).stat().st_mode) == 0o600, f"{name} has wrong permissions"

    def test_sync_from_sm_repairs_sidecar_permissions_on_cache_hit(self, sm_ks, tmp_path):
        """A sidecar left too permissive by an earlier sync is repaired without a re-download."""
        sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
        sidecar = tmp_path / "scylla_test_id_ed25519.version"
        sidecar.chmod(0o664)

        # Version is unchanged, so this must fix the permissions without downloading again
        with patch.object(sm_ks, "download_file") as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_not_called()
        assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600

    def test_get_sm_version_id_returns_awscurrent(self, sm_ks):
        """get_sm_version_id returns the version id of the AWSCURRENT secret version."""
        version_id = sm_ks.get_sm_version_id("scylla_test_id_ed25519")
        assert version_id  # non-empty UUID-like string

    def test_get_obj_if_needed_skips_when_version_unchanged(self, sm_ks, tmp_path):
        """get_obj_if_needed skips the download when the sidecar version matches the secret's."""
        # First call downloads + writes sidecar
        sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
        first_mtime = os.stat(tmp_path / "scylla_test_id_ed25519").st_mtime

        # Second call with same version — no re-download
        with patch.object(sm_ks, "download_file") as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_not_called()
        assert os.stat(tmp_path / "scylla_test_id_ed25519").st_mtime == first_mtime

    def test_get_obj_if_needed_redownloads_when_version_changes(self, sm_ks, tmp_path):
        """get_obj_if_needed downloads again when the sidecar holds a different version id."""
        sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
        # Tamper the sidecar to simulate a stale/rotated version
        (tmp_path / "scylla_test_id_ed25519.version").write_text("old-version-id")

        with patch.object(sm_ks, "download_file", wraps=sm_ks.download_file) as mock_dl:
            sm_ks.get_obj_if_needed("scylla_test_id_ed25519", str(tmp_path), 0o600)
            mock_dl.assert_called_once()

    def test_get_obj_if_needed_downloads_when_sidecar_missing(self, sm_ks, tmp_path):
        """get_obj_if_needed downloads and writes the sidecar when the local key has no sidecar."""
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
    """Cover the log records KeyStore emits on fetches and cache hits."""

    def test_cache_hit_logs_debug(self, ks, caplog):
        """A cache hit is logged at DEBUG level with the object name."""
        ks.get_file_contents("email_config.json")
        with caplog.at_level(logging.DEBUG, logger="sdcm.keystore"):
            ks.get_file_contents("email_config.json")
        assert any("cache hit" in r.message and "email_config.json" in r.message for r in caplog.records)
        assert any(r.levelno == logging.DEBUG for r in caplog.records if "cache hit" in r.message)

    def test_fetch_logs_info(self, ks, caplog):
        """A fetch is logged at INFO level with the object name."""
        with caplog.at_level(logging.INFO, logger="sdcm.keystore"):
            ks.get_file_contents("email_config.json")
        assert any("fetched" in r.message and "email_config.json" in r.message for r in caplog.records)
        assert any(r.levelno == logging.INFO for r in caplog.records if "fetched" in r.message)

    def test_slow_fetch_logs_warning(self, ks, caplog):
        """A fetch slower than the threshold is logged as a 'slow fetch' warning."""
        data = json.dumps(FAKE_EMAIL_CONFIG).encode()
        body = MagicMock()
        body.read.return_value = data
        mock_obj = MagicMock()
        mock_obj.get.side_effect = lambda: time.sleep(0.05) or {"Body": body}  # force small delay
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
    """Cover errors raised for missing or malformed keystore content."""

    def test_get_file_contents_missing_key(self, ks):
        """get_file_contents raises ClientError for a missing object."""
        with pytest.raises(ClientError):
            ks.get_file_contents("does_not_exist")

    def test_get_json_invalid_content(self, ks, mocked_s3):
        """get_json raises JSONDecodeError for malformed JSON content."""
        mocked_s3.Object(KEYSTORE_S3_BUCKET, "bad.json").put(Body=b"{{bad")
        with pytest.raises(json.JSONDecodeError):
            ks.get_json("bad.json")

    def test_get_ssh_key_pair_missing_key(self, ks):
        """get_ssh_key_pair raises ClientError for a missing key."""
        with pytest.raises(ClientError):
            ks.get_ssh_key_pair("nonexistent_key")


# ---------------------------------------------------------------------------
# SSHKey namedtuple
# ---------------------------------------------------------------------------


class TestSSHKeyNamedTuple:
    """Cover the SSHKey named tuple."""

    def test_fields(self):
        """SSHKey exposes the name, public_key and private_key it was built with."""
        key = SSHKey(name="n", public_key=b"pub", private_key=b"priv")
        assert key.name == "n"
        assert key.public_key == b"pub"
        assert key.private_key == b"priv"

    def test_tuple_fields(self):
        """SSHKey's fields are name, public_key and private_key, in that order."""
        assert SSHKey._fields == ("name", "public_key", "private_key")


# ---------------------------------------------------------------------------
# pub_key_from_private_key_file
# ---------------------------------------------------------------------------


class TestPubKeyFromPrivateKeyFile:
    """Cover pub_key_from_private_key_file deriving the public key from a private key file."""

    def test_rsa_key(self, tmp_path):
        """An RSA private key yields its base64 public key and the ssh-rsa type."""
        key = paramiko.RSAKey.generate(2048)
        path = str(tmp_path / "id_rsa")
        key.write_private_key_file(path)
        base64_pub, key_type = pub_key_from_private_key_file(path)
        assert key_type == "ssh-rsa"
        assert base64_pub == key.get_base64()

    def test_ed25519_key(self, tmp_path):
        """An OpenSSH Ed25519 private key yields a base64 public key and the ssh-ed25519 type."""
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
