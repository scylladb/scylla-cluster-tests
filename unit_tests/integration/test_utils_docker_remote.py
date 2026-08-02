"""Integration tests for sdcm.utils.docker_remote.RemoteDocker file transfers.

External services: Docker (Scylla container, via the 'docker_scylla' fixture).

'send_files()' and 'receive_files()' stage the payload through a 'mktemp' file on the container's
host. The unit tests can only assert that an 'rm' was issued; these run the real transfers against a
real container and check the host afterwards. Before the cleanup existed, every transfer left one
staging copy of the payload behind.

'receive_files()' is only checked for that cleanup, not for delivering the file: its 'docker cp' runs
under 'sudo', which leaves the staging file owned by root, and the unprivileged local copy that
follows cannot read it. That is a pre-existing failure unrelated to the staging file -- see
'test_receive_files_removes_the_staging_file_even_when_the_copy_out_fails'.
"""

import contextlib

import pytest

PAYLOAD = "staging-cleanup-probe\n" * 1000
CONTAINER_PATH = "/staging-probe/payload.txt"


@pytest.fixture(name="staging_dir")
def fixture_staging_dir(monkeypatch, tmp_path):
    """Point the host's 'mktemp' at a private directory.

    'mktemp' honours TMPDIR and the remoter runs it in a subprocess that inherits this environment,
    so the staging file lands somewhere no concurrent test can add to -- globbing /tmp would be racy
    under xdist.
    """
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    monkeypatch.setenv("TMPDIR", str(staging_dir))
    return staging_dir


@pytest.fixture(name="payload_in_container")
def fixture_payload_in_container(docker_scylla, staging_dir, tmp_path):
    """A payload delivered into the container, with the host staging directory left clean."""
    src = tmp_path / "payload.txt"
    src.write_text(PAYLOAD)
    assert docker_scylla.send_files(str(src), CONTAINER_PATH) is True
    return src


@pytest.mark.integration
def test_send_files_delivers_the_payload_and_removes_the_staging_file(docker_scylla, staging_dir, payload_in_container):
    """A real send_files() puts the file in the container and leaves nothing on its host."""
    assert docker_scylla.run(f"wc -c < {CONTAINER_PATH}").stdout.strip() == str(len(PAYLOAD))
    assert list(staging_dir.iterdir()) == []


@pytest.mark.integration
def test_receive_files_removes_the_staging_file_even_when_the_copy_out_fails(
    docker_scylla, staging_dir, payload_in_container, tmp_path
):
    """The staging file is gone whether or not the transfer itself succeeded.

    Deliberately asserts nothing about the outcome: 'docker cp' runs under 'sudo' and hands back a
    root owned staging file that the unprivileged copy which follows cannot read, so today this
    raises. Suppressing that keeps the test honest now and still correct if it is ever fixed -- and
    it exercises the real 'finally' path against a real failure, which the unit tests can only mock.
    """
    with contextlib.suppress(Exception):
        docker_scylla.receive_files(CONTAINER_PATH, str(tmp_path / "roundtrip.txt"))

    assert list(staging_dir.iterdir()) == []


@pytest.mark.integration
def test_unremovable_staging_file_is_logged_and_swallowed(docker_scylla, caplog):
    """A staging file that cannot be removed is reported, not raised.

    '/proc/self/cmdline' exists but cannot be unlinked, which is the only reliable way to make 'rm'
    fail -- 'rm -f' exits 0 on a path that is merely missing.
    """
    with caplog.at_level("WARNING"):
        docker_scylla._remove_staging_file("/proc/self/cmdline")

    assert "Failed to remove the staging file '/proc/self/cmdline'" in caplog.text
