from unittest import mock

import pytest
from invoke.runners import Result

from sdcm.cluster import BaseNode, NodeSetupFailed
from sdcm.remote import shell_script_cmd


APT_KEY = "AABBCCDD11223344"
HKP_KEYSERVERS = [
    "hkp://keyserver.ubuntu.com:80",
    "hkp://keys.openpgp.org",
    "hkp://pgp.mit.edu",
]


@pytest.fixture()
def node():
    n = mock.MagicMock()
    n.parent_cluster.params.get.return_value = [APT_KEY]
    return n


def _make_result(ok):
    r = mock.MagicMock(spec=Result)
    r.ok = ok
    return r


def _gpg_recv_call(temp_keyring, keyserver, apt_key):
    return mock.call(
        f"gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --keyserver {keyserver} --keyserver-options timeout=10 --recv-keys {apt_key}",
        retry=1,
        ignore_status=True,
    )


def _https_fallback_call(temp_keyring, apt_key):
    https_url = f"https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x{apt_key}&options=mr"
    return mock.call(
        shell_script_cmd(
            f"curl --retry 3 --retry-max-time 60 --connect-timeout 10 -fsSL '{https_url}' | gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --import"
        ),
        retry=1,
        ignore_status=True,
    )


def _export_call(temp_keyring):
    return mock.call(
        shell_script_cmd(
            f"gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --export --armor | gpg --dearmor > /etc/apt/keyrings/scylladb.gpg"
        ),
        retry=3,
    )


def test_fetch_apt_keys_first_keyserver_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(True),  # first keyserver succeeds
        None,  # export
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            _export_call(temp_keyring),
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 4


def test_fetch_apt_keys_first_fails_second_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        _make_result(True),  # second keyserver succeeds
        None,  # export
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            _export_call(temp_keyring),
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 5


def test_fetch_apt_keys_all_hkp_fail_https_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        _make_result(False),  # second keyserver fails
        _make_result(False),  # third keyserver fails
        _make_result(True),  # HTTPS fallback succeeds
        None,  # export
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            _https_fallback_call(temp_keyring, APT_KEY),
            _export_call(temp_keyring),
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 7


def test_fetch_apt_keys_all_sources_fail_raises_exception(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        _make_result(False),  # second keyserver fails
        _make_result(False),  # third keyserver fails
        _make_result(False),  # HTTPS fallback fails
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        with pytest.raises(NodeSetupFailed):
            BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            _https_fallback_call(temp_keyring, APT_KEY),
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 6
