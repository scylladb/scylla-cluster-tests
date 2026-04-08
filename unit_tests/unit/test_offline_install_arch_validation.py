"""Tests for architecture validation in offline_install_scylla."""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseNode, NodeSetupFailed


_BASE_URL = (
    "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/scylla-unified-2026.1.1~dev-0.20260326"
)


@pytest.fixture
def make_node():
    """Factory fixture: create a mock node whose ``uname -m`` returns *uname_output*."""

    def _make(uname_output: str):
        node = MagicMock()
        uname_result = MagicMock()
        uname_result.stdout = uname_output
        node.remoter.run.return_value = uname_result
        return node

    return _make


def test_aarch64_package_on_x86_node_raises(make_node):
    """aarch64 unified package on an x86_64 node must raise NodeSetupFailed."""
    node = make_node("x86_64\n")
    unified_package = f"{_BASE_URL}.aarch64.tar.gz"

    with pytest.raises(NodeSetupFailed, match="Architecture mismatch.*aarch64.*x86_64"):
        BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)


def test_x86_package_on_aarch64_node_raises(make_node):
    """x86_64 unified package on an aarch64 node must raise NodeSetupFailed."""
    node = make_node("aarch64\n")
    unified_package = f"{_BASE_URL}.x86_64.tar.gz"

    with pytest.raises(NodeSetupFailed, match="Architecture mismatch.*x86_64.*aarch64"):
        BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)


def test_matching_x86_package_on_x86_node_passes(make_node):
    """x86_64 unified package on an x86_64 node should pass the arch check."""
    node = make_node("x86_64\n")
    unified_package = f"{_BASE_URL}.x86_64.tar.gz"

    # The method should proceed past arch validation and call curl to download.
    # We don't need it to succeed fully, just not raise NodeSetupFailed.
    try:
        BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)
    except NodeSetupFailed:
        pytest.fail("NodeSetupFailed raised for matching architecture (x86_64)")
    except Exception:  # noqa: BLE001
        # Any other exception is fine – it means we passed the arch check
        pass


def test_matching_aarch64_package_on_aarch64_node_passes(make_node):
    """aarch64 unified package on an aarch64 node should pass the arch check."""
    node = make_node("aarch64\n")
    unified_package = f"{_BASE_URL}.aarch64.tar.gz"

    try:
        BaseNode.offline_install_scylla(node, unified_package=unified_package, nonroot=False)
    except NodeSetupFailed:
        pytest.fail("NodeSetupFailed raised for matching architecture (aarch64)")
    except Exception:  # noqa: BLE001
        pass
