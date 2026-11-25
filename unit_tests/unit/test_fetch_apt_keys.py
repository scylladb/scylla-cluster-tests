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
PUBLISHED_KEY = "C503C686B007F39E"
PUBLISHED_URL = SCYLLA_APT_KEY_URLS[PUBLISHED_KEY]
HKP_KEYSERVERS = [
    "hkp://keyserver.ubuntu.com:80",
    "hkp://keys.openpgp.org",
    "hkp://pgp.mit.edu",
]
GPG_KILL_CALL = mock.call("gpgconf --homedir /tmp --kill all", ignore_status=True, verbose=False, timeout=60)


@pytest.fixture()
def node():
    """Mock node whose cluster lists APT_KEY as the scylla apt key."""
    n = mock.MagicMock()
    n.parent_cluster.params.get.return_value = [APT_KEY]
    return n


def make_result(ok):
    """Return a mock invoke Result with the given ``ok`` flag."""
    r = mock.MagicMock(spec=Result)
    r.ok = ok
    return r


def gpg_recv_call(temp_keyring, keyserver, apt_key):
    """Expected remoter.sudo call that receives apt_key from a keyserver into temp_keyring."""
    return mock.call(
        f"timeout 30 gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --keyserver {keyserver} --keyserver-options timeout=10 --recv-keys {apt_key}",
        retry=1,
        ignore_status=True,
        timeout=120,
    )


def https_import_call(temp_keyring, url, apt_key):
    """Expected remoter.sudo call that downloads url, imports it into temp_keyring and checks apt_key."""
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


def https_fallback_call(temp_keyring, apt_key):
    """Expected HTTPS fallback call that fetches apt_key from keyserver.ubuntu.com."""
    return https_import_call(
        temp_keyring, f"https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x{apt_key}&options=mr", apt_key
    )


def export_call(temp_keyring, apt_keys=(APT_KEY,)):
    """Expected remoter.sudo call that exports apt_keys to the scylladb keyring file."""
    return mock.call(
        shell_script_cmd(
            f"gpg --homedir /tmp --no-default-keyring --keyring {temp_keyring} --export --armor {' '.join(apt_keys)} | gpg --dearmor > /etc/apt/keyrings/scylladb.gpg"
        ),
        retry=3,
    )


def test_fetch_apt_keys_first_keyserver_succeeds(node):
    """fetch_apt_keys stops at the first keyserver when it returns the key."""
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(True),  # first keyserver succeeds
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
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 6


def test_fetch_apt_keys_first_fails_second_succeeds(node):
    """fetch_apt_keys tries the next keyserver after the first one fails."""
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        make_result(True),  # second keyserver succeeds
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
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 8


def test_fetch_apt_keys_all_hkp_fail_https_succeeds(node):
    """fetch_apt_keys falls back to the HTTPS download when all HKP keyservers fail."""
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        make_result(False),  # second keyserver fails
        None,  # kill gpg daemons
        make_result(False),  # third keyserver fails
        None,  # kill gpg daemons
        make_result(True),  # HTTPS fallback succeeds
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
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            GPG_KILL_CALL,
            https_fallback_call(temp_keyring, APT_KEY),
            GPG_KILL_CALL,
            export_call(temp_keyring),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 12


def test_fetch_apt_keys_all_sources_fail_raises_exception(node):
    """fetch_apt_keys raises NodeSetupFailed when every keyserver and the HTTPS fallback fail."""
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(False),  # first keyserver fails
        None,  # kill gpg daemons
        make_result(False),  # second keyserver fails
        None,  # kill gpg daemons
        make_result(False),  # third keyserver fails
        None,  # kill gpg daemons
        make_result(False),  # HTTPS fallback fails
        None,  # kill gpg daemons
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"), pytest.raises(NodeSetupFailed):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    node.remoter.sudo.assert_has_calls(
        [
            mock.call("mkdir -m 0755 -p /etc/apt/keyrings"),
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], APT_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[1], APT_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[2], APT_KEY),
            GPG_KILL_CALL,
            https_fallback_call(temp_keyring, APT_KEY),
            GPG_KILL_CALL,
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 11


def test_fetch_apt_keys_reaps_gpg_daemons_when_export_fails(node):
    """fetch_apt_keys kills gpg daemons and removes the temp keyring when the export raises."""
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(True),  # first keyserver succeeds
        None,  # kill gpg daemons
        RuntimeError("export failed"),  # export raises
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with (
        mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"),
        pytest.raises(RuntimeError, match="export failed"),
    ):
        BaseNode.fetch_apt_keys(node)

    temp_keyring = "/tmp/temp-test-uuid.gpg"
    assert node.remoter.sudo.mock_calls[-2:] == [
        GPG_KILL_CALL,
        mock.call(f"rm -f {temp_keyring}", ignore_status=True),
    ]


def test_fetch_apt_keys_published_key_skips_keyservers(node):
    """fetch_apt_keys imports a key with a published URL without querying the keyservers."""
    node.parent_cluster.params.get.return_value = [PUBLISHED_KEY]
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(True),  # download + import + key id check succeeds
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
            https_import_call(temp_keyring, PUBLISHED_URL, PUBLISHED_KEY),
            GPG_KILL_CALL,
            export_call(temp_keyring, [PUBLISHED_KEY]),
            GPG_KILL_CALL,
            mock.call(f"rm -f {temp_keyring}", ignore_status=True),
        ]
    )
    assert node.remoter.sudo.call_count == 6


def test_fetch_apt_keys_published_key_failure_falls_back_to_keyserver(node):
    """fetch_apt_keys queries the keyservers when a key's published URL fails."""
    node.parent_cluster.params.get.return_value = [PUBLISHED_KEY]
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(False),  # download or key id check fails
        None,  # kill gpg daemons
        make_result(True),  # first keyserver succeeds
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
            https_import_call(temp_keyring, PUBLISHED_URL, PUBLISHED_KEY),
            GPG_KILL_CALL,
            gpg_recv_call(temp_keyring, HKP_KEYSERVERS[0], PUBLISHED_KEY),
            GPG_KILL_CALL,
            export_call(temp_keyring, [PUBLISHED_KEY]),
        ]
    )
    assert node.remoter.sudo.call_count == 8


def test_every_default_apt_key_has_a_published_url():
    """A default key without a published URL puts every node back on the public keyservers."""
    defaults = yaml.safe_load((Path(get_sct_root_path()) / "defaults" / "test_default.yaml").read_text())
    # 2025.1 still needs the 2022 key (upgrades from 2024.1); Releng doesn't publish it yet (RELENG-1172)
    not_published = {"D0A112E067426AB2"}
    assert set(defaults["scylla_apt_keys"]) - not_published <= set(SCYLLA_APT_KEY_URLS)


def test_fetch_apt_keys_exports_only_the_requested_keys(node):
    """fetch_apt_keys exports every requested key, not only the ones fetched from keyservers."""
    two_keys = [APT_KEY, PUBLISHED_KEY]
    node.parent_cluster.params.get.return_value = two_keys
    node.remoter.sudo.side_effect = [
        None,  # mkdir
        make_result(True),  # APT_KEY from the first keyserver
        None,  # kill gpg daemons
        make_result(True),  # PUBLISHED_KEY from its published URL
        None,  # kill gpg daemons
        None,  # export
        None,  # kill gpg daemons (finally)
        None,  # cleanup
    ]

    with mock.patch("sdcm.cluster.uuid.uuid4", return_value="test-uuid"):
        BaseNode.fetch_apt_keys(node)

    assert export_call("/tmp/temp-test-uuid.gpg", two_keys) in node.remoter.sudo.mock_calls
