from pathlib import Path
from unittest import mock

import pytest
import yaml
from invoke.runners import Result

from sdcm.cluster import SCYLLA_APT_KEY_URLS, BaseNode, NodeSetupFailed
from sdcm.remote import shell_script_cmd
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.curl import curl_with_retry

APT_KEY = "AABBCCDD11223344"
HKP_KEYSERVERS = [
    "hkp://keyserver.ubuntu.com:80",
    "hkp://keys.openpgp.org",
    "hkp://pgp.mit.edu",
]
GPG_KILL_CALL = mock.call("gpgconf --homedir /tmp --kill all", ignore_status=True, verbose=False, timeout=60)


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
        f"timeout 30 gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --keyserver {keyserver} --keyserver-options timeout=10 --recv-keys {apt_key}",
        retry=1,
        ignore_status=True,
        timeout=120,
    )


def _https_import_call(temp_keyring, url, apt_key):
    curl_cmd = curl_with_retry(
        f"'{url}'", silent=True, follow_redirects=True, fail_early=True, retry=3, retry_max_time=60, extra_flags="-S"
    )
    gpg_temp = f"gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring}"
    return mock.call(
        shell_script_cmd(f"{curl_cmd} | {gpg_temp} --import && {gpg_temp} --list-keys {apt_key}"),
        retry=1,
        ignore_status=True,
        timeout=120,
    )


def _https_fallback_call(temp_keyring, apt_key):
    return _https_import_call(
        temp_keyring, f"https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x{apt_key}&options=mr", apt_key
    )


def _export_call(temp_keyring, apt_keys=(APT_KEY,)):
    return mock.call(
        shell_script_cmd(
            f"gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --export --armor {' '.join(apt_keys)} | gpg --dearmor > /etc/apt/keyrings/scylladb.gpg"
        ),
        retry=3,
    )


def test_fetch_apt_keys_first_keyserver_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(True),  # first keyserver succeeds
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            _export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 6


def test_fetch_apt_keys_first_fails_second_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        _make_result(True),  # second keyserver succeeds
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            _export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 8


def test_fetch_apt_keys_all_hkp_fail_https_succeeds(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        _make_result(False),  # second keyserver fails
        None,  # kill gpg daemons
        _make_result(False),  # third keyserver fails
        None,  # kill gpg daemons
        _make_result(True),  # HTTPS fallback succeeds
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            GPG_KILL_CALL,
            _https_fallback_call(temp_keyring, APT_KEY),
            GPG_KILL_CALL,
            _export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 12


def test_fetch_apt_keys_all_sources_fail_raises_exception(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        _make_result(False),  # second keyserver fails
        None,  # kill gpg daemons
        _make_result(False),  # third keyserver fails
        None,  # kill gpg daemons
        _make_result(False),  # HTTPS fallback fails
        None,  # kill gpg daemons
        None,  # kill gpg daemons (finally)
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
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            GPG_KILL_CALL,
            _https_fallback_call(temp_keyring, APT_KEY),
            GPG_KILL_CALL,
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 11


def test_fetch_apt_keys_reaps_gpg_daemons_when_export_fails(node):
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(True),  # first keyserver succeeds
        None,  # kill gpg daemons
        RuntimeError("export failed"),  # export raises
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        with pytest.raises(RuntimeError):
            BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    assert node.remoter.sudo.mock_calls[-2:] == [
        GPG_KILL_CALL,
        mock.call(f"rm -f {temp_keyring}", ignore_status=True),
    ]


PUBLISHED_KEY = "C503C686B007F39E"
PUBLISHED_URL = SCYLLA_APT_KEY_URLS[PUBLISHED_KEY]


def test_fetch_apt_keys_published_key_skips_keyservers(node):
    node.parent_cluster.params.get.return_value = [PUBLISHED_KEY]
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(True),  # download + import + key id check succeeds
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            _https_import_call(temp_keyring, PUBLISHED_URL, PUBLISHED_KEY),
            GPG_KILL_CALL,
            _export_call(temp_keyring, [PUBLISHED_KEY]),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 6


def test_fetch_apt_keys_published_key_failure_falls_back_to_keyserver(node):
    node.parent_cluster.params.get.return_value = [PUBLISHED_KEY]
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(False),  # download or key id check fails
        None,  # kill gpg daemons
        _make_result(True),  # first keyserver succeeds
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            _https_import_call(temp_keyring, PUBLISHED_URL, PUBLISHED_KEY),
            GPG_KILL_CALL,
            _gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], PUBLISHED_KEY),
            GPG_KILL_CALL,
            _export_call(temp_keyring, [PUBLISHED_KEY]),
        ]
    )
    assert node.remoter.sudo.call_count == 8


def test_every_default_apt_key_has_a_published_url():
    """A default key without a published URL puts every node back on the public keyservers."""
    defaults = yaml.safe_load((Path(get_sct_root_path()) / "defaults" / "test_default.yaml").read_text())
    assert set(defaults["scylla_apt_keys"]) <= set(SCYLLA_APT_KEY_URLS)


def test_fetch_apt_keys_exports_only_the_requested_keys(node):
    two_keys = [APT_KEY, PUBLISHED_KEY]
    node.parent_cluster.params.get.return_value = two_keys
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        _make_result(True),  # APT_KEY from the first keyserver
        None,  # kill gpg daemons
        _make_result(True),  # PUBLISHED_KEY from its published URL
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    assert _export_call("/tmp/temp-test-uuid.gpg", two_keys) in node.remoter.sudo.mock_calls
