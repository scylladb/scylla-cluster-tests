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

"""Fixtures scoped to unit tests only (unit_tests/unit/).

- ``fake_remoter``: ensures all unit tests use FakeRemoter, not real SSH.
- ``mock_cloud_services``: mocks AWS/GCE/Azure APIs so no real credentials
  are needed.  Both are autouse so unit tests get them automatically without
  any marker or explicit declaration.
"""

import json
import os
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

from sdcm.keystore import KeyStore, SSHKey
from sdcm.remote import RemoteCmdRunnerBase

from unit_tests.lib.fake_remoter import FakeRemoter


@pytest.fixture(autouse=True)
def fake_remoter():
    """Ensure all unit tests use FakeRemoter instead of real SSH remoters."""
    RemoteCmdRunnerBase.set_default_remoter_class(FakeRemoter)
    return FakeRemoter


@pytest.fixture(scope="session", autouse=True)
def mock_cloud_services(tmp_path_factory):
    """Prevent unit tests from making real AWS/GCE/Azure API calls.

    This session-scoped fixture mocks cloud service calls that would otherwise
    require real credentials. It covers:
    - convert_name_to_ami_if_needed: resolves 'resolve:ssm:' AMI patterns via AWS SSM
    - find_scylla_repo: calls get_s3_scylla_repos_mapping which lists S3 buckets
    - get_s3_scylla_repos_mapping: lists S3 buckets for version-to-repo mapping
    - get_arch_from_instance_type: calls EC2 DescribeInstanceTypes
    - KeyStore: accesses S3 for credentials and SSH keys
    - _file validator: SCTConfiguration uses ExistingFile (pydantic BeforeValidator)
      to check user_credentials_path exists on disk
    """
    # Redirect HOME to a temp directory so dummy SSH key files are created there
    # instead of polluting the real home directory.
    fake_home = tmp_path_factory.mktemp("home")
    ssh_dir = fake_home / ".ssh"
    ssh_dir.mkdir()
    orig_home = os.environ.get("HOME")
    os.environ["HOME"] = str(fake_home)
    for key_name in ("scylla_test_id_ed25519", "scylla-test"):
        (ssh_dir / key_name).touch(mode=0o600)

    def fake_find_scylla_repo(scylla_version, dist_type="centos", dist_version=None):
        """Return a plausible repo URL without accessing S3."""
        bucket = "downloads.scylladb.com"
        version_prefix = scylla_version.split(":")[0] if ":" in scylla_version else scylla_version
        parts = version_prefix.split(".")
        if len(parts) > 2:
            version_prefix = ".".join(parts[:2])
        if dist_type in ("centos", "rocky", "rhel"):
            return f"https://s3.amazonaws.com/{bucket}/rpm/centos/scylla-{version_prefix}.repo"
        elif dist_type in ("ubuntu", "debian"):
            return f"https://s3.amazonaws.com/{bucket}/deb/debian/scylla-{version_prefix}.list"
        raise ValueError(f"repo for scylla version {scylla_version} wasn't found")

    def fake_get_file_contents(self, file_name):
        """Return fake content for common KeyStore files."""
        defaults = {
            "email_config.json": b'{"user": "test", "password": "test"}',
            "azure.json": b'{"subscription_id": "test", "tenant_id": "test", "client_id": "test", "client_secret": "test"}',
            "backup_azure_blob.json": b'{"account": "test", "key": "test"}',
            "azure_kms_config.json": b'{"shared_vault_name": "test-vault", "resource_group": "test-rg", "identity_name": "test-identity", "managed_identity_principal_id": "test-principal-id", "sct_service_principal_id": "test-sct-principal", "num_of_keys": 1}',
        }
        return defaults.get(file_name, b"{}")

    def fake_get_json(self, json_file):
        return json.loads(fake_get_file_contents(self, json_file))

    mock_ssh_key = SSHKey(
        name="scylla_test_id_ed25519",
        public_key=b"ssh-rsa AAAA fake-public-key scylla_test_id_ed25519\n",
        private_key=b"dummy-key\n",
    )

    with (
        patch("sdcm.utils.common.convert_name_to_ami_if_needed", side_effect=lambda param, region_names: param),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=lambda param, region_names: param),
        patch("sdcm.utils.version_utils.find_scylla_repo", side_effect=fake_find_scylla_repo),
        patch("sdcm.sct_config.find_scylla_repo", side_effect=fake_find_scylla_repo),
        patch("sdcm.mgmt.common.find_scylla_repo", side_effect=fake_find_scylla_repo),
        patch("sdcm.utils.version_utils.get_s3_scylla_repos_mapping", return_value={}),
        patch("sdcm.utils.aws_utils.get_arch_from_instance_type", return_value="x86_64"),
        patch("sdcm.sct_config.get_arch_from_instance_type", return_value="x86_64"),
        patch.object(KeyStore, "get_file_contents", fake_get_file_contents),
        patch.object(KeyStore, "get_json", fake_get_json),
        patch.object(KeyStore, "get_ssh_key_pair", return_value=mock_ssh_key),
        patch.object(KeyStore, "download_file", return_value=None),
        patch.object(KeyStore, "sync", return_value=None),
        patch.object(KeyStore, "s3", new_callable=PropertyMock, return_value=MagicMock()),
        patch.object(KeyStore, "s3_client", new_callable=PropertyMock, return_value=MagicMock()),
    ):
        yield

    if orig_home is not None:
        os.environ["HOME"] = orig_home
    else:
        os.environ.pop("HOME", None)
