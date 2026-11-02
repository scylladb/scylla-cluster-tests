"""Tests for sdcm.nemesis.monkey.encryption module."""

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from sdcm.exceptions import SstablesNotFound, UnsupportedNemesis
from sdcm.nemesis.monkey import encryption as enc_module
from sdcm.nemesis.monkey.encryption import (
    EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey,
    EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey,
)
from sdcm.nemesis.utils.node_allocator import AllNodesRunNemesisError

_MODULE = "sdcm.nemesis.monkey.encryption"

pytestmark = pytest.mark.usefixtures("events")


def _make_scylla_yaml(user_info_encryption_enabled=False):
    scylla_yaml = MagicMock()
    scylla_yaml.kms_hosts = {}
    scylla_yaml.user_info_encryption = (
        {"enabled": user_info_encryption_enabled} if user_info_encryption_enabled else False
    )
    return scylla_yaml


def sstable_utils_per_node(mock_sstable_cls, sstables_by_node):
    """Make ``SstableUtils(db_node=...)`` return a per-node mock whose ``get_sstables`` returns the given list."""
    utils = {}
    for node, sstables in sstables_by_node.items():
        utils[node] = MagicMock()
        utils[node].get_sstables.return_value = sstables
    mock_sstable_cls.side_effect = lambda db_node, ks_cf: utils[db_node]
    return utils


def node_allocator_mock(busy_nodes=()):
    """``node_allocator`` whose ``run_nemesis`` reserves the requested node, unless it's in ``busy_nodes``."""

    @contextmanager
    def run_nemesis(nemesis_label, node_list):
        if node_list[0] in busy_nodes:
            raise AllNodesRunNemesisError(f"{nemesis_label}: node allocation failed")
        yield node_list[0]

    allocator = MagicMock()
    allocator.run_nemesis.side_effect = run_nemesis
    return allocator


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` configured with an AWS backend."""
    params = {
        "cluster_backend": "aws",
        "compaction_strategy": "SizeTieredCompactionStrategy",
    }
    base_runner.cluster.params = MagicMock()
    base_runner.cluster.params.get.side_effect = lambda key, default=None: params.get(key, default)
    base_runner.cluster.params.region_names = ["us-east-1"]
    base_runner.cluster.test_config.test_id.return_value = "test-id-123"
    base_runner.cluster.get_test_keyspaces.return_value = ["ks1"]
    base_runner.cluster.nodes = base_runner.cluster.data_nodes
    base_runner._nemesis_stress_failure_handler = MagicMock()
    base_runner.node_allocator = node_allocator_mock()

    scylla_yaml = _make_scylla_yaml()
    for node in base_runner.cluster.data_nodes:
        node.remote_scylla_yaml.return_value.__enter__.return_value = scylla_yaml
        node.region = "us-east-1"

    return base_runner


@pytest.fixture(autouse=True)
def mocked_deps():
    """Skip real sleeps and heavy AWS/sstable interactions used by every test."""
    with (
        patch(f"{_MODULE}.time") as mock_time,
        patch(f"{_MODULE}.AwsKms") as mock_aws_kms_cls,
        patch(f"{_MODULE}.SstableUtils") as mock_sstable_cls,
    ):
        yield mock_time, mock_aws_kms_cls, mock_sstable_cls


@pytest.mark.parametrize(
    "monkey_class",
    [
        pytest.param(EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey, id="with-rotation"),
        pytest.param(EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey, id="without-rotation"),
    ],
)
def test_raises_when_not_aws_backend(runner, monkey_class):
    """Both nemesis are supported only on the AWS cluster backend."""
    runner.cluster.params.get.side_effect = lambda key, default=None: {
        "cluster_backend": "gce",
    }.get(key, default)
    with pytest.raises(UnsupportedNemesis, match="AWS cluster backend"):
        monkey_class(runner).disrupt()


def test_with_rotation_full_flow(runner):
    """The rotation nemesis writes/reads data twice, rotates the KMS key once, and
    disables encryption again (creating, enabling, rotating, disabling, dropping).
    """
    monkey = EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey(runner)
    monkey.disrupt()

    executed = runner.executed
    assert any("CREATE TABLE IF NOT EXISTS tmp_encrypted_table" in stmt for stmt in executed)
    assert any("scylla_encryption_options = {'key_provider': 'none'}" in stmt for stmt in executed)
    assert executed[-1] == "DROP TABLE tmp_encrypted_table;"

    enc_module.AwsKms.assert_called_once_with(region_names=["us-east-1"])
    aws_kms = enc_module.AwsKms.return_value
    aws_kms.create_alias.assert_called_once_with("alias/testid-test-id-123")
    aws_kms.rotate_kms_key.assert_called_once_with("alias/testid-test-id-123")

    sstable_util = enc_module.SstableUtils.return_value
    sstables = sstable_util.get_sstables.return_value
    sstable_util.check_that_sstables_are_encrypted.assert_has_calls(
        [call(sstables=sstables, expected_bool_value=True), call(sstables=sstables, expected_bool_value=False)]
    )

    # write+read happen twice (rotation loop) plus reread+rewrite+reread during the disable phase
    assert runner.tester.run_stress_thread.call_count == 7


def test_without_rotation_never_rotates_key(runner):
    """The non-rotation nemesis only goes through the write/read cycle once and never rotates the key."""
    monkey = EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner)
    monkey.disrupt()

    aws_kms = enc_module.AwsKms.return_value
    aws_kms.rotate_kms_key.assert_not_called()

    # write+read once, plus reread+rewrite+reread during the disable phase
    assert runner.tester.run_stress_thread.call_count == 5


def test_skips_disable_when_user_info_encryption_enabled(runner):
    """If user_info_encryption is already enabled cluster-wide, encryption can't be disabled."""
    scylla_yaml = _make_scylla_yaml(user_info_encryption_enabled=True)
    for node in runner.cluster.data_nodes:
        node.remote_scylla_yaml.return_value.__enter__.return_value = scylla_yaml

    monkey = EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner)
    monkey.disrupt()

    executed = runner.executed
    assert not any("key_provider': 'none'" in stmt for stmt in executed)
    assert executed[-1] == "DROP TABLE tmp_encrypted_table;"
    # only the initial write+read, no reread/rewrite/reread
    assert runner.tester.run_stress_thread.call_count == 2


def test_kms_host_reconfiguration_only_restarts_nodes_missing_it(runner):
    """AWS KMS host config is written per-node; only nodes missing the kms_host
    entry get restarted, and nodes that already have it are left untouched.
    """
    node1, node2 = runner.cluster.data_nodes

    yaml1 = _make_scylla_yaml()
    yaml2 = _make_scylla_yaml()
    yaml2.kms_hosts = {"kms-host": {"master_key": "pre-existing-alias"}}
    node1.remote_scylla_yaml.return_value.__enter__.return_value = yaml1
    node2.remote_scylla_yaml.return_value.__enter__.return_value = yaml2

    monkey = EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner)
    monkey.disrupt()

    assert yaml1.kms_hosts == {
        "kms-host": {
            "master_key": "alias/testid-test-id-123",
            "aws_region": "us-east-1",
            "aws_use_ec2_credentials": True,
        }
    }
    node1.restart_scylla.assert_called_once()

    assert yaml2.kms_hosts == {"kms-host": {"master_key": "pre-existing-alias"}}
    node2.restart_scylla.assert_not_called()


def test_drops_table_even_when_stress_fails(runner):
    """The temporary encrypted table must be dropped (the 'finally' cleanup)
    even when something in the write/read flow raises.
    """
    runner.tester.verify_stress_thread.side_effect = RuntimeError("boom")

    monkey = EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner)
    with pytest.raises(RuntimeError, match="boom"):
        monkey.disrupt()

    assert runner.executed[-1] == "DROP TABLE tmp_encrypted_table;"


def test_checks_encryption_on_node_holding_sstables(runner, mocked_deps):
    """The target node may hold no replica of the table (tablets, RF < nodes);
    the check must run on a reserved node that actually has sstables."""
    node1, node2 = runner.cluster.data_nodes
    runner.target_node = node1
    utils = sstable_utils_per_node(mocked_deps[2], {node1: [], node2: ["/var/lib/scylla/data/x-Data.db"]})

    EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner).disrupt()

    utils[node1].check_that_sstables_are_encrypted.assert_not_called()
    runner.node_allocator.run_nemesis.assert_called_with(nemesis_label="TableEncryptionCheck", node_list=[node2])
    utils[node2].check_that_sstables_are_encrypted.assert_has_calls(
        [
            call(sstables=["/var/lib/scylla/data/x-Data.db"], expected_bool_value=True),
            call(sstables=["/var/lib/scylla/data/x-Data.db"], expected_bool_value=False),
        ]
    )


def test_prefers_target_node_for_encryption_check(runner, mocked_deps):
    """When the target node holds sstables, the check runs there and other nodes aren't probed."""
    node1, node2 = runner.cluster.data_nodes
    runner.target_node = node2
    utils = sstable_utils_per_node(mocked_deps[2], {node1: ["a-Data.db"], node2: ["b-Data.db"]})

    EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner).disrupt()

    utils[node1].get_sstables.assert_not_called()
    runner.node_allocator.run_nemesis.assert_not_called()
    assert utils[node2].check_that_sstables_are_encrypted.call_count == 2


def test_raises_when_no_node_holds_sstables(runner, mocked_deps):
    """If no data node has sstables of the table, SstablesNotFound is raised (after retries) and the table is dropped."""
    node1, node2 = runner.cluster.data_nodes
    sstable_utils_per_node(mocked_deps[2], {node1: [], node2: []})

    with (
        patch("sdcm.utils.decorators.time.sleep"),
        pytest.raises(SstablesNotFound, match="weren't found on any of the nodes"),
    ):
        EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner).disrupt()

    assert runner.executed[-1] == "DROP TABLE tmp_encrypted_table;"


def test_unsupported_when_free_nodes_hold_no_sstables(runner, mocked_deps):
    """Nodes busy with other nemeses aren't probed; if the free ones have no sstables,
    the nemesis is skipped naming the busy nodes, and the table is dropped."""
    node1, node2 = runner.cluster.data_nodes
    node2.running_nemesis = "OtherNemesis"
    runner.node_allocator = node_allocator_mock(busy_nodes={node2})
    utils = sstable_utils_per_node(mocked_deps[2], {node1: [], node2: ["b-Data.db"]})

    with pytest.raises(UnsupportedNemesis, match=r"busy: \['node2 \(OtherNemesis\)'\]"):
        EnableDisableTableEncryptionAwsKmsProviderWithoutRotationMonkey(runner).disrupt()

    utils[node2].get_sstables.assert_not_called()
    assert runner.executed[-1] == "DROP TABLE tmp_encrypted_table;"
