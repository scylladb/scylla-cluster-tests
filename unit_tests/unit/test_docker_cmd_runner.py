"""Tests for sdcm.remote.docker_cmd_runner.DockerCmdRunner."""

import tarfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from invoke.exceptions import UnexpectedExit
from invoke.watchers import StreamWatcher

from sdcm.remote.docker_cmd_runner import DockerCmdRunner


class DummyNode:
    """Node stub that exposes only the name DockerCmdRunner reads."""

    name = "dummy-node"


class TestWatcher(StreamWatcher):
    """StreamWatcher that records every output chunk it receives."""

    __test__ = False

    def __init__(self):
        self.submissions = []

    def submit(self, output):
        """Record one output chunk."""
        self.submissions.append(output)


@pytest.fixture()
def node():
    """DummyNode the runner under test is bound to."""
    return DummyNode()


@pytest.fixture()
def docker_runner(node):
    """DockerCmdRunner on ``node`` with a mocked container and logger."""
    runner = DockerCmdRunner(node)
    runner._container = MagicMock()
    runner.log = MagicMock()
    return runner


@patch("sdcm.remote.docker_cmd_runner.ContainerManager")
def test_get_container_no_cache(mock_cm, node):
    """_get_container asks ContainerManager for the node container when none is cached."""
    mock_container = MagicMock()
    mock_cm.get_container.return_value = mock_container

    runner = DockerCmdRunner(node)
    runner._container = None
    result = runner._get_container()

    assert result == runner._container
    mock_cm.get_container.assert_called_once_with(node, "node")


@patch("sdcm.remote.docker_cmd_runner.ContainerManager")
def test_get_container_cached_running(mock_cm, node):
    """_get_container reuses a cached container that is still running."""
    mock_container = MagicMock(status="running")

    runner = DockerCmdRunner(node)
    runner._container = mock_container
    result = runner._get_container()

    assert result == mock_container
    mock_container.reload.assert_called_once_with()
    mock_cm.get_container.assert_not_called()


@patch("sdcm.remote.docker_cmd_runner.ContainerManager")
def test_get_container_cached_not_running(mock_cm, node):
    """_get_container replaces a cached container that is no longer running."""
    old_container = MagicMock(status="exited")
    new_container = MagicMock()
    mock_cm.get_container.return_value = new_container

    runner = DockerCmdRunner(node)
    runner._container = old_container
    result = runner._get_container()

    assert result == new_container
    assert result == runner._container
    old_container.reload.assert_called_once_with()
    mock_cm.get_container.assert_called_once_with(node, "node")


@patch("sdcm.remote.docker_cmd_runner.ContainerManager")
def test_get_container_not_found(mock_cm, node):
    """_get_container raises RuntimeError when ContainerManager finds no container."""
    mock_cm.get_container.return_value = None

    runner = DockerCmdRunner(node)
    runner._container = None

    with pytest.raises(RuntimeError, match="could not be resolved"):
        runner._get_container()

    mock_cm.get_container.assert_called_once_with(node, "node")


def test_connection(docker_runner):
    """Connection returns the container from _get_container."""
    with patch.object(docker_runner, "_get_container") as mock_get:
        mock_container = MagicMock()
        mock_get.return_value = mock_container

        result = docker_runner.connection

        assert result == mock_container
        mock_get.assert_called_once_with()


@patch("sdcm.remote.docker_cmd_runner.ContainerManager")
def test_container_is_up(mock_cm, node, docker_runner):
    """is_up reflects ContainerManager.is_running for the node."""
    mock_cm.is_running.return_value = True
    assert docker_runner.is_up()
    mock_cm.is_running.assert_called_once_with(node, "node")

    mock_cm.is_running.return_value = False
    assert not docker_runner.is_up()


def test_execute_command_success(docker_runner):
    """_execute_command returns the exec output as a Result."""
    mock_container = MagicMock(status="running")
    mock_container.exec_run.return_value = MagicMock(exit_code=0, output=(b"hello world", b""))

    with patch.object(docker_runner, "_get_container", return_value=mock_container):
        result = docker_runner._execute_command("echo hello", 30, False, True, [])

    assert result.exited == 0
    assert result.stdout == "hello world"
    assert result.stderr == ""
    mock_container.exec_run.assert_called_once_with(
        ["sh", "-c", "echo hello"], tty=False, demux=True, stream=False, user=""
    )


def test_execute_command_with_stderr(docker_runner):
    """_execute_command raises UnexpectedExit when the command exits non-zero."""
    mock_container = MagicMock(status="running")
    mock_container.exec_run.return_value = MagicMock(exit_code=1, output=(b"", b"error message"))

    with (
        patch.object(docker_runner, "_get_container", return_value=mock_container),
        pytest.raises(UnexpectedExit),
    ):
        docker_runner._execute_command("false", 30, False, True, [])


@patch("sdcm.remote.docker_cmd_runner.retrying")
def test_run(mock_retrying, docker_runner):
    """Run forwards the command to _execute_command and prints its result."""
    mock_retrying.return_value = lambda f: f  # No-op decorator

    with (
        patch.object(docker_runner, "_execute_command") as mock_exec,
        patch.object(docker_runner, "_setup_watchers") as mock_setup,
        patch.object(docker_runner, "_print_command_results") as mock_print,
    ):
        mock_result = MagicMock()
        mock_exec.return_value = mock_result
        mock_setup.return_value = []
        result = docker_runner.run("echo hello", timeout=30, verbose=True)

    assert result == mock_result
    mock_setup.assert_called_once_with(True, None, None, timestamp_logs=False)
    mock_exec.assert_called_once_with("echo hello", 30, False, True, [], "")
    mock_print.assert_called_once_with(mock_result, True, False)


@patch("sdcm.remote.docker_cmd_runner.retrying")
def test_send_files(mock_retrying, docker_runner, tmp_path):
    """send_files puts a tar of the source under the destination's parent directory."""
    mock_retrying.return_value = lambda f: f

    mock_container = MagicMock()
    mock_container.name = "test-container"
    source = tmp_path / "scylla.yaml"
    source.write_text("test data")
    mock_tar_stream = BytesIO(b"tar data")

    with (
        patch.object(docker_runner, "_get_container", return_value=mock_container),
        patch.object(docker_runner, "run") as mock_run,
        patch.object(docker_runner, "_create_tar_stream") as mock_create_tar,
    ):
        mock_create_tar.return_value = mock_tar_stream
        mock_run.return_value = MagicMock(ok=True)

        result = docker_runner.send_files(str(source), "/etc/scylla/scylla.yaml")

    assert result
    mock_container.put_archive.assert_called_once_with(path="/etc/scylla", data=mock_tar_stream)
    mock_create_tar.assert_called_once_with(str(source), "/etc/scylla/scylla.yaml")


@patch("sdcm.remote.docker_cmd_runner.retrying")
def test_receive_files(mock_retrying, docker_runner, tmp_path):
    """receive_files extracts the container archive into the local destination file."""
    mock_retrying.return_value = lambda f: f

    tar_stream = BytesIO()
    with tarfile.open(fileobj=tar_stream, mode="w") as tar:
        info = tarfile.TarInfo("file.txt")
        content = b"hello world"
        info.size = len(content)
        tar.addfile(info, BytesIO(content))
    tar_stream.seek(0)

    mock_container = MagicMock()
    mock_container.name = "test-container"
    tar_data = tar_stream.getvalue()
    mock_container.get_archive.return_value = ([tar_data], {})
    local_file = tmp_path / "file.txt"

    with patch.object(docker_runner, "_get_container", return_value=mock_container):
        result = docker_runner.receive_files("/src/file.txt", str(local_file))

    assert result
    assert local_file.read_text() == "hello world"


def test_sudo_with_non_root_user(docker_runner):
    """Sudo runs the command through run with the given non-root user."""
    with patch.object(docker_runner, "run", return_value=MagicMock()) as mock_run:
        cmd = "ls -la"
        result = docker_runner.sudo(cmd, user="testuser")

        assert result == mock_run.return_value
        mock_run.assert_called_once_with(
            cmd=f"{cmd}",
            timeout=None,
            ignore_status=False,
            verbose=True,
            new_session=False,
            log_file=None,
            retry=1,
            watchers=None,
            user="testuser",
        )
