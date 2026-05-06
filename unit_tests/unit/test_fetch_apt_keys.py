from unittest import mock

import pytest
from invoke.runners import Result

from sdcm.cluster import BaseNode, NodeSetupFailed


def test_fetch_apt_keys_first_keyserver_succeeds():
    """Test that first HKP keyserver succeeds and no fallback is attempted."""
    node = mock.MagicMock()
    node.parent_cluster.params.get.return_value = ["AABBCCDD11223344"]

    # Mock successful response from first keyserver
    success_result = mock.MagicMock(spec=Result)
    success_result.ok = True

    node.remoter.sudo.side_effect = [
        None,  # mkdir
        success_result,  # gpg recv-keys from first keyserver
        None,  # export
        None,  # cleanup
    ]

    BaseNode.fetch_apt_keys(node)

    # Verify sudo was called 4 times (mkdir, recv-keys, export, cleanup)
    assert node.remoter.sudo.call_count == 4

    # Verify the first keyserver was used
    calls = node.remoter.sudo.call_args_list
    assert "hkp://keyserver.ubuntu.com:80" in calls[1][0][0]


def test_fetch_apt_keys_first_fails_second_succeeds():
    """Test that first HKP keyserver fails, second HKP keyserver succeeds."""
    node = mock.MagicMock()
    node.parent_cluster.params.get.return_value = ["AABBCCDD11223344"]

    # Mock failed response from first keyserver, success from second
    fail_result = mock.MagicMock(spec=Result)
    fail_result.ok = False

    success_result = mock.MagicMock(spec=Result)
    success_result.ok = True

    node.remoter.sudo.side_effect = [
        None,  # mkdir
        fail_result,  # gpg recv-keys from first keyserver (fails)
        success_result,  # gpg recv-keys from second keyserver (succeeds)
        None,  # export
        None,  # cleanup
    ]

    BaseNode.fetch_apt_keys(node)

    # Verify sudo was called 5 times (mkdir, recv-keys x2, export, cleanup)
    assert node.remoter.sudo.call_count == 5

    # Verify both keyservers were tried
    calls = node.remoter.sudo.call_args_list
    assert "hkp://keyserver.ubuntu.com:80" in calls[1][0][0]
    assert "hkp://keys.openpgp.org" in calls[2][0][0]


def test_fetch_apt_keys_all_hkp_fail_https_succeeds():
    """Test that all HKP keyservers fail, HTTPS fallback succeeds."""
    node = mock.MagicMock()
    node.parent_cluster.params.get.return_value = ["AABBCCDD11223344"]

    # Mock failed responses from all HKP keyservers, success from HTTPS
    fail_result = mock.MagicMock(spec=Result)
    fail_result.ok = False

    success_result = mock.MagicMock(spec=Result)
    success_result.ok = True

    node.remoter.sudo.side_effect = [
        None,  # mkdir
        fail_result,  # gpg recv-keys from first keyserver (fails)
        fail_result,  # gpg recv-keys from second keyserver (fails)
        fail_result,  # gpg recv-keys from third keyserver (fails)
        success_result,  # curl + gpg import from HTTPS (succeeds)
        None,  # export
        None,  # cleanup
    ]

    BaseNode.fetch_apt_keys(node)

    # Verify sudo was called 7 times (mkdir, recv-keys x3, https, export, cleanup)
    assert node.remoter.sudo.call_count == 7

    # Verify all three HKP keyservers were tried
    calls = node.remoter.sudo.call_args_list
    assert "hkp://keyserver.ubuntu.com:80" in calls[1][0][0]
    assert "hkp://keys.openpgp.org" in calls[2][0][0]
    assert "hkp://pgp.mit.edu" in calls[3][0][0]

    # Verify HTTPS fallback was used
    assert "https://keyserver.ubuntu.com/pks/lookup" in calls[4][0][0]


def test_fetch_apt_keys_all_sources_fail_raises_exception():
    """Test that all sources fail and exception is raised."""
    node = mock.MagicMock()
    node.parent_cluster.params.get.return_value = ["AABBCCDD11223344"]

    # Mock failed responses from all sources
    fail_result = mock.MagicMock(spec=Result)
    fail_result.ok = False
    fail_result.stderr = "Key not found"

    node.remoter.sudo.side_effect = [
        None,  # mkdir
        fail_result,  # gpg recv-keys from first keyserver (fails)
        fail_result,  # gpg recv-keys from second keyserver (fails)
        fail_result,  # gpg recv-keys from third keyserver (fails)
        fail_result,  # curl + gpg import from HTTPS (fails)
        None,  # cleanup (in finally block)
    ]

    with pytest.raises(NodeSetupFailed):
        BaseNode.fetch_apt_keys(node)

    # Verify cleanup was still called
    calls = node.remoter.sudo.call_args_list
    assert "rm -f /tmp/temp-" in calls[-1][0][0]
