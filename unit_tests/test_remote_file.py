"""Integration tests for sdcm.remote.remote_file.remote_file() over a real SSH connection.

External services: Docker (an sshd container with passwordless sudo, via the 'ssh_server_container' fixture).

The unit tests fake the remoter, so they can't show that the real 'test -e', 'stat', 'chown' and file
transfers agree with each other. These run them against a real host, as a non-root user with sudo,
like a test node.
"""

import uuid

import pytest

from sdcm.remote.remote_base import RemoteCmdRunnerBase
from sdcm.remote.remote_file import remote_file

pytestmark = pytest.mark.integration


@pytest.fixture(name="remoter", scope="module")
def fixture_remoter(ssh_server_container):
    # the autouse fake_remoter fixture leaves FakeRemoter as the default class, pick the real transport
    RemoteCmdRunnerBase.set_default_ssh_transport("libssh2")
    return RemoteCmdRunnerBase.create_remoter(**ssh_server_container)


@pytest.fixture(name="scylla_d")
def fixture_scylla_d(remoter):
    """A fresh root-owned directory, like /etc/scylla.d on an image without an io preset."""
    path = f"/etc/scylla.d-{uuid.uuid4().hex[:8]}"
    remoter.sudo(f"mkdir -p {path}")
    yield path
    remoter.sudo(f"rm -rf {path}")


def owner_and_mode(remoter, path):
    return remoter.sudo(f"stat -c '%U:%G %a' {path}").stdout.strip()


def test_missing_file_is_created_with_sudo(remoter, scylla_d):
    path = f"{scylla_d}/io_properties.yaml"
    with remote_file(remoter=remoter, remote_path=path, sudo=True) as fobj:
        assert fobj.getvalue() == ""
        fobj.write("disks: []\n")
    assert remoter.sudo(f"cat {path}").stdout == "disks: []\n"
    assert owner_and_mode(remoter, path) == "root:root 644"


def test_existing_file_keeps_its_owner_and_mode(remoter, scylla_d):
    path = f"{scylla_d}/io_properties.yaml"
    remoter.sudo(f"sh -c 'echo old > {path}'")
    remoter.sudo(f"chown test:test {path}")
    remoter.sudo(f"chmod 600 {path}")
    with remote_file(remoter=remoter, remote_path=path, sudo=True) as fobj:
        assert fobj.getvalue() == "old\n"
        fobj.write("new\n")
    assert remoter.sudo(f"cat {path}").stdout == "new\n"
    assert owner_and_mode(remoter, path) == "test:test 600"


def test_reading_a_missing_file_does_not_create_it(remoter, scylla_d):
    path = f"{scylla_d}/io_properties.yaml"
    with remote_file(remoter=remoter, remote_path=path, sudo=True) as fobj:
        assert fobj.getvalue() == ""
    assert remoter.run(f"test -e {path}", ignore_status=True).exited == 1


def test_existing_path_with_a_space_is_edited(remoter):
    path = f"/home/test/config copy {uuid.uuid4().hex[:8]}.yaml"
    remoter.run(f"echo old > '{path}'")
    with remote_file(remoter=remoter, remote_path=path) as fobj:
        assert fobj.getvalue() == "old\n"
        fobj.write("new\n")
    assert remoter.run(f"cat '{path}'").stdout == "new\n"
