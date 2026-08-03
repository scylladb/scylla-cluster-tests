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
"""Unit tests for AWS KMS / encryption-at-rest configuration.

Covers:
- ClusterTester.prepare_kms_host: KMS is configured for the tested (enterprise) cluster,
  including mixed_scylla setups, and is skipped when disabled.
- BaseNode.proposed_scylla_yaml: KMS keys are applied to the tested cluster only and are
  always stripped for the Oracle cluster (KMS is a Scylla-only, enterprise feature).
"""

import copy
import unittest.mock
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseNode
from sdcm.tester import ClusterTester
from unit_tests.lib.dot_dict import DotDict
from unit_tests.lib.dummy_remote import DummyRemote
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode


# ---------------------------------------------------------------------------
# Shared test data and fixtures
# ---------------------------------------------------------------------------
# KMS / encryption-at-rest is a Scylla-only, enterprise feature. It is injected into every node's
# scylla.yaml via the global `append_scylla_yaml` param, so the Oracle cluster (mixed_scylla /
# mixed_cassandra) must be explicitly excluded from it.
KMS_APPEND_SCYLLA_YAML = {
    "kms_hosts": {
        "auto": {
            "master_key": "alias/testid-xxx",
            "aws_use_ec2_credentials": True,
            "aws_region": "us-east-1",
        },
    },
    "user_info_encryption": {"enabled": True, "key_provider": "KmsKeyProviderFactory", "kms_host": "auto"},
    "system_info_encryption": {"enabled": True, "key_provider": "KmsKeyProviderFactory", "kms_host": "auto"},
}


@pytest.fixture
def kms_dummy_node(tmp_path):
    node = DummyNode(
        name="test_node",
        parent_cluster=None,
        base_logdir=str(tmp_path),
        ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519"),
    )
    node.parent_cluster = DummyDbCluster(nodes=[node])
    node.init()
    node.remoter = DummyRemote()
    node.parent_cluster.params["append_scylla_yaml"] = copy.deepcopy(KMS_APPEND_SCYLLA_YAML)
    return node


def _make_kms_tester_self(**param_overrides):
    """Build a fake ClusterTester self for prepare_kms_host testing."""
    params = DotDict(
        {
            "cluster_backend": "aws",
            "is_enterprise": True,
            "artifact_scylla_version": "2026.2.0",
            "region_names": ["us-east-1"],
            "enterprise_disable_kms": False,
            "db_type": "mixed_scylla",
            "scylla_encryption_options": None,
            "append_scylla_yaml": None,
        }
    )
    params.update(param_overrides)
    fake_self = MagicMock()
    fake_self.params = params
    fake_self.test_config.test_id.return_value = "test-id-123"
    return fake_self


# ---------------------------------------------------------------------------
# ClusterTester.prepare_kms_host
# ---------------------------------------------------------------------------


def test_prepare_kms_host_configures_kms_for_mixed_scylla():
    """mixed_scylla enterprise tests must still get KMS configured for the tested cluster."""
    fake_self = _make_kms_tester_self()
    with patch("sdcm.tester.AwsKms") as aws_kms_mock, patch("sdcm.tester.SkipPerIssues", return_value=False):
        ClusterTester.prepare_kms_host(fake_self)

    aws_kms_mock.return_value.create_alias.assert_called_once()
    append_scylla_yaml = fake_self.params["append_scylla_yaml"]
    assert append_scylla_yaml["kms_hosts"]["auto"]["master_key"] == "alias/testid-test-id-123"
    assert append_scylla_yaml["user_info_encryption"]["enabled"] is True
    assert append_scylla_yaml["system_info_encryption"]["enabled"] is True


def test_prepare_kms_host_skips_when_kms_disabled():
    """enterprise_disable_kms must short-circuit KMS configuration."""
    fake_self = _make_kms_tester_self(enterprise_disable_kms=True)
    with patch("sdcm.tester.AwsKms") as aws_kms_mock, patch("sdcm.tester.SkipPerIssues", return_value=False):
        ClusterTester.prepare_kms_host(fake_self)

    aws_kms_mock.assert_not_called()
    assert fake_self.params["append_scylla_yaml"] is None


# ---------------------------------------------------------------------------
# BaseNode.proposed_scylla_yaml (KMS applied to tested cluster only)
# ---------------------------------------------------------------------------


def test_proposed_scylla_yaml_includes_kms_for_tested_cluster(kms_dummy_node):
    """The tested (scylla-db) cluster gets the KMS / encryption-at-rest config from append_scylla_yaml."""
    with (
        unittest.mock.patch.object(
            BaseNode, "_proposed_scylla_yaml_properties", new_callable=unittest.mock.PropertyMock, return_value={}
        ),
        unittest.mock.patch("sdcm.cluster.install_encryption_at_rest_files") as install_mock,
    ):
        scylla_yaml = kms_dummy_node.proposed_scylla_yaml

    assert scylla_yaml.kms_hosts == KMS_APPEND_SCYLLA_YAML["kms_hosts"]
    assert scylla_yaml.user_info_encryption == KMS_APPEND_SCYLLA_YAML["user_info_encryption"]
    assert scylla_yaml.system_info_encryption == KMS_APPEND_SCYLLA_YAML["system_info_encryption"]
    install_mock.assert_called_once()


def test_proposed_scylla_yaml_excludes_kms_for_oracle_cluster(kms_dummy_node):
    """The Oracle cluster must never run with KMS, even if append_scylla_yaml carries it."""
    kms_dummy_node.parent_cluster.node_type = "oracle-db"

    with (
        unittest.mock.patch.object(
            BaseNode, "_proposed_scylla_yaml_properties", new_callable=unittest.mock.PropertyMock, return_value={}
        ),
        unittest.mock.patch("sdcm.cluster.install_encryption_at_rest_files") as install_mock,
    ):
        scylla_yaml = kms_dummy_node.proposed_scylla_yaml

    assert scylla_yaml.kms_hosts is None
    assert scylla_yaml.user_info_encryption is None
    assert scylla_yaml.system_info_encryption is None
    install_mock.assert_not_called()
