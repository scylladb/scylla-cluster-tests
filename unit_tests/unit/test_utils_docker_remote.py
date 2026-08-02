"""Unit tests for sdcm.utils.docker_remote.RemoteDocker.

Covers the host-side staging file that 'send_files()' and 'receive_files()' create with 'mktemp' to
move data in and out of the container.  Both must remove it on every path, or each transfer leaks a
full copy of the payload into the /tmp of the node hosting the container -- a loader instance on the
cloud backends, the machine running the test on the docker one.
"""

from unittest.mock import MagicMock, patch

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.remote.base import RetryableNetworkException
from sdcm.utils.docker_remote import RemoteDocker


@pytest.fixture(name="remote_docker")
def fixture_remote_docker():
    """A RemoteDocker whose container host is a mock remoter, built without starting a container."""
    remote_docker = RemoteDocker.__new__(RemoteDocker)  # __init__ would 'docker run' over ssh
    remote_docker.log = MagicMock()
    remote_docker.docker_id = "dummy-container-id"
    remote_docker.sudo_needed = ""

    remote_docker.node = MagicMock()
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpXXXXXX\n", ok=True)
    remote_docker.node.remoter.send_files.return_value = True
    remote_docker.node.remoter.receive_files.return_value = True

    # 'send_files()' runs 'mkdir -p' inside the container through RemoteDocker.run()
    with patch.object(RemoteDocker, "run", return_value=MagicMock(ok=True)):
        yield remote_docker


def _failing_docker_cp(tempfile_path):
    """Return a remoter.run() side effect where 'mktemp' succeeds and 'docker cp' exits non-zero."""

    def run(cmd, *_args, **_kwargs):
        result = MagicMock(ok=True)
        if cmd.startswith("mktemp"):
            result.stdout = f"{tempfile_path}\n"
        elif "docker cp" in cmd:
            result.ok = False
        return result

    return run


def _cleanup_commands(remote_docker):
    """The 'rm -f' commands the container host was asked to run, with the 'sudo_needed' padding
    normalized away."""
    return [
        " ".join(call.args[0].split())
        for call in remote_docker.node.remoter.run.call_args_list
        if "rm -f" in call.args[0]
    ]


def _cleanup_ran_last(remote_docker):
    """Whether the cleanup was the transfer's final command."""
    return "rm -f" in remote_docker.node.remoter.run.call_args_list[-1].args[0]


# --------------------------------------------------------------------------------------------------
# send_files
# --------------------------------------------------------------------------------------------------


def test_send_files_success_removes_staging_file(remote_docker):
    """A completed transfer removes the staging file and still reports success."""
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpABC\n", ok=True)

    result = remote_docker.send_files("/local/file.tsv", "/container/dst/file.tsv")

    assert result is True
    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpABC"]
    assert _cleanup_ran_last(remote_docker)


def test_send_files_docker_cp_failure_removes_staging_file(remote_docker):
    """A failed 'docker cp' still removes the staging file, and is still reported as a failure."""
    remote_docker.node.remoter.run.side_effect = _failing_docker_cp("/tmp/tmpFAIL")

    result = remote_docker.send_files("/local/file.tsv", "/container/dst/file.tsv")

    assert result is False
    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpFAIL"]


def test_send_files_transfer_exception_removes_staging_file(remote_docker):
    """A raising transfer removes the staging file and propagates the original exception."""
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpRSYNC\n", ok=True)
    remote_docker.node.remoter.send_files.side_effect = UnexpectedExit(MagicMock())

    with pytest.raises(UnexpectedExit):
        remote_docker.send_files("/local/file.tsv", "/container/dst/file.tsv")

    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpRSYNC"]


# --------------------------------------------------------------------------------------------------
# receive_files
# --------------------------------------------------------------------------------------------------


def test_receive_files_success_removes_staging_file(remote_docker):
    """A completed transfer removes the staging file and still reports success."""
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpRCV\n", ok=True)

    result = remote_docker.receive_files("/container/src/file.tsv", "/local/dst/file.tsv")

    assert result is True
    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpRCV"]
    assert _cleanup_ran_last(remote_docker)


def test_receive_files_docker_cp_failure_removes_staging_file(remote_docker):
    """A failed 'docker cp' still removes the staging file, and is still reported as a failure."""
    remote_docker.node.remoter.run.side_effect = _failing_docker_cp("/tmp/tmpRCVFAIL")

    result = remote_docker.receive_files("/container/src/file.tsv", "/local/dst/file.tsv")

    assert result is False
    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpRCVFAIL"]


def test_receive_files_transfer_exception_removes_staging_file(remote_docker):
    """A raising transfer removes the staging file and propagates the original exception."""
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpRCVEXC\n", ok=True)
    remote_docker.node.remoter.receive_files.side_effect = OSError("disk full")

    with pytest.raises(OSError):
        remote_docker.receive_files("/container/src/file.tsv", "/local/dst/file.tsv")

    assert _cleanup_commands(remote_docker) == ["rm -f -- /tmp/tmpRCVEXC"]


# --------------------------------------------------------------------------------------------------
# Both transfers
# --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("transfer", ["send_files", "receive_files"])
def test_cleanup_uses_the_same_sudo_as_docker_cp(transfer, remote_docker):
    """The removal runs with the privileges 'docker cp' had -- see '_remove_staging_file()'."""
    remote_docker.sudo_needed = "sudo "
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/tmpSUDO\n", ok=True)

    getattr(remote_docker, transfer)("/some/src", "/some/dst")

    assert _cleanup_commands(remote_docker) == ["sudo rm -f -- /tmp/tmpSUDO"]


@pytest.mark.parametrize("transfer", ["send_files", "receive_files"])
def test_cleanup_failure_does_not_mask_the_transfer_error(transfer, remote_docker):
    """A staging file that cannot be removed must not replace the error that broke the transfer."""
    transfer_error = UnexpectedExit(MagicMock())

    def run(cmd, *_args, **_kwargs):
        if "rm -f" in cmd:
            raise RetryableNetworkException("connection lost", original=OSError("connection lost"))
        return MagicMock(stdout="/tmp/tmpMASK\n", ok=True)

    remote_docker.node.remoter.run.side_effect = run
    remote_docker.node.remoter.send_files.side_effect = transfer_error
    remote_docker.node.remoter.receive_files.side_effect = transfer_error

    with pytest.raises(UnexpectedExit):
        getattr(remote_docker, transfer)("/some/src", "/some/dst")

    remote_docker.log.warning.assert_called_once()


def test_cleanup_quotes_a_staging_path_that_needs_it(remote_docker):
    """'mktemp' builds the path from TMPDIR, so it is only as tame as the caller's environment.

    Unquoted, a directory with a space splits into arguments and 'rm -f' exits 0 on a path that does
    not exist -- a silent leak, in the one case this cleanup exists to prevent.
    """
    remote_docker.node.remoter.run.return_value = MagicMock(stdout="/tmp/a b/tmp.X$(id)\n", ok=True)

    remote_docker.send_files("/local/file.tsv", "/container/dst/file.tsv")

    cleanup = remote_docker.node.remoter.run.call_args_list[-1].args[0]
    assert cleanup.strip() == "rm -f -- '/tmp/a b/tmp.X$(id)'"


def test_cleanup_failure_is_logged_and_keeps_a_good_transfer_successful(remote_docker):
    """A staging file that cannot be removed is logged, but does not fail a transfer that worked."""

    def run(cmd, *_args, **_kwargs):
        if "rm -f" in cmd:
            raise UnexpectedExit(MagicMock(exited=1, stderr="rm: cannot remove: Operation not permitted"))
        return MagicMock(stdout="/tmp/tmpNOPERM\n", ok=True)

    remote_docker.node.remoter.run.side_effect = run

    assert remote_docker.send_files("/local/file.tsv", "/container/dst/file.tsv") is True
    remote_docker.log.warning.assert_called_once()
    assert "/tmp/tmpNOPERM" in str(remote_docker.log.warning.call_args)
