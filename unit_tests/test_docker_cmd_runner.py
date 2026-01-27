import tarfile
import tempfile
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
import docker
from docker.errors import NotFound
from invoke.exceptions import UnexpectedExit
from invoke.watchers import StreamWatcher

from sdcm.remote.docker_cmd_runner import DockerCmdRunner


class DummyNode:
    name = "dummy-node"


class TestWatcher(StreamWatcher):
    __test__ = False

    def __init__(self):
        self.submissions = []

    def submit(self, output):
        self.submissions.append(output)


class TestDockerCmdRunner(unittest.TestCase):
    def setUp(self):
        self.node = DummyNode()
        self.runner = DockerCmdRunner(self.node)
        self.runner._container = MagicMock()
        self.runner.log = MagicMock()

    @patch("sdcm.remote.docker_cmd_runner.ContainerManager")
    def test_get_container_no_cache(self, mock_cm):
        mock_container = MagicMock()
        mock_cm.get_container.return_value = mock_container

        runner = DockerCmdRunner(self.node)
        runner._container = None
        result = runner._get_container()

        self.assertEqual(result, runner._container)
        mock_cm.get_container.assert_called_once_with(self.node, "node")

    @patch("sdcm.remote.docker_cmd_runner.ContainerManager")
    def test_get_container_cached_running(self, mock_cm):
        mock_container = MagicMock(status="running")

        runner = DockerCmdRunner(self.node)
        runner._container = mock_container
        result = runner._get_container()

        self.assertEqual(result, mock_container)
        mock_container.reload.assert_called_once()
        mock_cm.get_container.assert_not_called()

    @patch("sdcm.remote.docker_cmd_runner.ContainerManager")
    def test_get_container_cached_not_running(self, mock_cm):
        old_container = MagicMock(status="exited")
        new_container = MagicMock()
        mock_cm.get_container.return_value = new_container

        runner = DockerCmdRunner(self.node)
        runner._container = old_container
        result = runner._get_container()

        self.assertEqual(result, new_container)
        self.assertEqual(result, runner._container)
        old_container.reload.assert_called_once()
        mock_cm.get_container.assert_called_once_with(self.node, "node")

    @patch("sdcm.remote.docker_cmd_runner.ContainerManager")
    def test_get_container_not_found(self, mock_cm):
        mock_cm.get_container.return_value = None

        runner = DockerCmdRunner(self.node)
        runner._container = None

        with self.assertRaises(RuntimeError, msg="could not be resolved"):
            runner._get_container()

        mock_cm.get_container.assert_called_once_with(self.node, "node")

    def test_connection(self):
        with patch.object(self.runner, "_get_container") as mock_get:
            mock_container = MagicMock()
            mock_get.return_value = mock_container

            result = self.runner.connection

            self.assertEqual(result, mock_container)
            mock_get.assert_called_once()

    @patch("sdcm.remote.docker_cmd_runner.ContainerManager")
    def test_container_is_up(self, mock_cm):
        mock_cm.is_running.return_value = True
        self.assertTrue(self.runner.is_up())
        mock_cm.is_running.assert_called_once_with(self.node, "node")

        mock_cm.is_running.return_value = False
        self.assertFalse(self.runner.is_up())

    def test_execute_command_success(self):
        """Test _execute_command with successful execution"""
        mock_container = MagicMock(status="running")
        mock_container.exec_run.return_value = MagicMock(exit_code=0, output=(b"hello world", b""))

        with patch.object(self.runner, "_get_container", return_value=mock_container):
            result = self.runner._execute_command("echo hello", 30, False, True, [])

        self.assertEqual(result.exited, 0)
        self.assertEqual(result.stdout, "hello world")
        self.assertEqual(result.stderr, "")
        self.assertGreater(result.duration, 0)
        mock_container.exec_run.assert_called_once_with(
            ["sh", "-c", "echo hello"], tty=False, demux=True, stream=False, user=""
        )

    def test_execute_command_with_stderr(self):
        mock_container = MagicMock(status="running")
        mock_container.exec_run.return_value = MagicMock(exit_code=1, output=(b"", b"error message"))

        with patch.object(self.runner, "_get_container", return_value=mock_container):
            with self.assertRaises(UnexpectedExit):
                self.runner._execute_command("false", 30, False, True, [])

    @patch("sdcm.remote.docker_cmd_runner.retrying")
    def test_run(self, mock_retrying):
        mock_retrying.return_value = lambda f: f  # No-op decorator

        with (
            patch.object(self.runner, "_execute_command") as mock_exec,
            patch.object(self.runner, "_setup_watchers") as mock_setup,
            patch.object(self.runner, "_print_command_results") as mock_print,
        ):
            mock_result = MagicMock()
            mock_exec.return_value = mock_result
            mock_setup.return_value = []
            result = self.runner.run("echo hello", timeout=30, verbose=True)

        self.assertEqual(result, mock_result)
        mock_setup.assert_called_once_with(True, None, None, timestamp_logs=False)
        mock_exec.assert_called_once_with("echo hello", 30, False, True, [], "")
        mock_print.assert_called_once_with(mock_result, True, False)

    @patch("sdcm.remote.docker_cmd_runner.retrying")
    def test_send_files(self, mock_retrying):
        mock_retrying.return_value = lambda f: f

        mock_container = MagicMock()
        mock_container.name = "test-container"

        with (
            tempfile.NamedTemporaryFile(mode="w", delete=True) as temp_file,
            patch.object(self.runner, "_get_container", return_value=mock_container),
            patch.object(self.runner, "run") as mock_run,
            patch.object(self.runner, "_create_tar_stream") as mock_create_tar,
        ):
            temp_file.write("test data")
            temp_file.flush()

            mock_tar_stream = BytesIO(b"tar data")
            mock_create_tar.return_value = mock_tar_stream
            mock_run.return_value = MagicMock(ok=True)

            result = self.runner.send_files(temp_file.name, "/dest/path")

        self.assertTrue(result)
        mock_container.put_archive.assert_called_once()
        mock_create_tar.assert_called_once_with(temp_file.name, "/dest/path")

    @patch("sdcm.remote.docker_cmd_runner.retrying")
    def test_receive_files(self, mock_retrying):
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

        with (
            tempfile.NamedTemporaryFile(mode="w+", delete=True) as temp_file,
            patch.object(self.runner, "_get_container", return_value=mock_container),
        ):
            result = self.runner.receive_files("/src/file.txt", temp_file.name)

            self.assertTrue(result)
            temp_file.seek(0)
            self.assertEqual(temp_file.read(), "hello world")

    def test_sudo_with_non_root_user(self):
        with patch.object(self.runner, "run", return_value=MagicMock()) as mock_run:
            cmd = "ls -la"
            result = self.runner.sudo(cmd, user="testuser")

            self.assertEqual(result, mock_run.return_value)
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


@pytest.fixture
def docker_node_with_name(docker_scylla):
    """Fixture that adds required attributes to docker_scylla for DockerCmdRunner compatibility."""
    # Ensure _containers dict exists (required by ContainerManager)
    if not hasattr(docker_scylla, "_containers"):
        docker_scylla._containers = {}

    # Get the actual Docker container instance
    if "node" not in docker_scylla._containers:
        docker_client = docker.from_env()
        try:
            container = docker_client.containers.get(docker_scylla.docker_id)
            docker_scylla._containers["node"] = container
        except NotFound:
            # Container doesn't exist yet, this is fine - it will be created when needed
            pass

    return docker_scylla


@pytest.mark.integration
@pytest.mark.docker_scylla_args()
def test_send_files_integration_with_real_docker(docker_node_with_name, tmp_path):
    """
    Integration test: send files to a real Docker container.

    This test uses an actual Docker container running Scylla.
    It verifies that files can be successfully copied from the host
    to the container using the DockerCmdRunner.
    """
    runner = DockerCmdRunner(docker_node_with_name)

    # Create a temporary file with test data
    test_file = tmp_path / "test_file.txt"
    test_content = "This is a test file for Docker integration testing\n"
    test_file.write_text(test_content)

    # Send file to container
    remote_path = "/tmp/test_file.txt"
    result = runner.send_files(str(test_file), remote_path)

    # Verify file was sent successfully
    assert result is True, "Failed to send file to Docker container"

    # Verify file content in container
    check_cmd = f"cat {remote_path}"
    cmd_result = runner.run(check_cmd, verbose=False)
    assert cmd_result.ok, f"Failed to read file from container: {cmd_result.stderr}"
    assert test_content.strip() in cmd_result.stdout, "File content mismatch in container"

    # Cleanup
    runner.run(f"rm -f {remote_path}", ignore_status=True)


@pytest.mark.integration
@pytest.mark.docker_scylla_args()
def test_send_directory_integration_with_real_docker(docker_node_with_name, tmp_path):
    """
    Integration test: send an entire directory to a real Docker container.

    This test creates a local directory structure with multiple files
    and subdirectories, then copies the entire structure to the container.
    It verifies that the directory hierarchy and all files are preserved.
    """
    runner = DockerCmdRunner(docker_node_with_name)

    # Create directory structure
    src_dir = tmp_path / "source_directory"
    src_dir.mkdir()

    # Create files in root of source directory
    (src_dir / "file1.txt").write_text("Content of file 1\n")
    (src_dir / "file2.txt").write_text("Content of file 2\n")

    # Create subdirectories with files
    subdir1 = src_dir / "subdir1"
    subdir1.mkdir()
    (subdir1 / "nested_file1.txt").write_text("Nested file 1 content\n")

    subdir2 = src_dir / "subdir2"
    subdir2.mkdir()
    (subdir2 / "nested_file2.txt").write_text("Nested file 2 content\n")

    # Create a deeper nested structure
    deep_dir = subdir1 / "deep"
    deep_dir.mkdir()
    (deep_dir / "deep_file.txt").write_text("Deep file content\n")

    # Remote destination
    remote_dest = "/tmp/copied_directory"
    runner.run(f"mkdir -p {remote_dest}", ignore_status=True)
    # Send entire directory to container
    result = runner.send_files(str(src_dir), remote_dest)

    # Verify directory was sent successfully
    assert result is True, "Failed to send directory to Docker container"

    # Verify directory structure and files in container
    # Check if source_directory exists
    check_dir = runner.run(f"test -d {remote_dest} && echo 'exists'", ignore_status=True)
    assert "exists" in check_dir.stdout, f"Remote directory not found: {remote_dest}"

    # Verify files exist
    test_cases = [
        (f"{remote_dest}/file1.txt", "Content of file 1"),
        (f"{remote_dest}/file2.txt", "Content of file 2"),
        (f"{remote_dest}/subdir1/nested_file1.txt", "Nested file 1 content"),
        (f"{remote_dest}/subdir2/nested_file2.txt", "Nested file 2 content"),
        (f"{remote_dest}/subdir1/deep/deep_file.txt", "Deep file content"),
    ]

    for remote_file, expected_content in test_cases:
        # Check file exists
        exists_result = runner.run(f"test -f {remote_file} && echo 'exists'", ignore_status=True)
        assert "exists" in exists_result.stdout, f"Remote file not found: {remote_file}"

        # Verify file content
        content_result = runner.run(f"cat {remote_file}", verbose=False)
        assert content_result.ok, f"Failed to read file: {remote_file}"
        assert expected_content in content_result.stdout, (
            f"Content mismatch for {remote_file}. Expected: {expected_content}, Got: {content_result.stdout}"
        )

    # Verify directory structure using find command
    find_result = runner.run(f"find {remote_dest} -type f | wc -l", verbose=False)
    assert find_result.ok, "Failed to count files in remote directory"
    file_count = int(find_result.stdout.strip())
    assert file_count == 5, f"Expected 5 files, but found {file_count} in {remote_dest}"

    # Cleanup
    runner.run(f"rm -rf {remote_dest}", ignore_status=True)


@pytest.mark.integration
@pytest.mark.docker_scylla_args()
def test_receive_files_integration_with_real_docker(docker_node_with_name, tmp_path):
    """
    Integration test: receive files from a real Docker container.

    This test creates files in the container, then retrieves them
    back to the host using the DockerCmdRunner.
    """
    runner = DockerCmdRunner(docker_node_with_name)

    # Create a temporary file in the container
    remote_file = "/tmp/test_receive_file.txt"
    test_content = "This is content created in the container\n"
    create_cmd = f"echo '{test_content.strip()}' > {remote_file}"
    runner.run(create_cmd)

    # Create a temporary file on host to receive the content
    local_file = tmp_path / "received_file.txt"

    # Receive file from container
    result = runner.receive_files(remote_file, str(local_file))

    # Verify file was received successfully
    assert result is True, "Failed to receive file from Docker container"

    # Verify file content on host
    received_content = local_file.read_text()
    assert test_content.strip() in received_content, "File content mismatch after receive"

    # Cleanup
    runner.run(f"rm -f {remote_file}", ignore_status=True)
