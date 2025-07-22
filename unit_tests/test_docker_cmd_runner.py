import tarfile
import tempfile
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from invoke.exceptions import UnexpectedExit
from invoke.watchers import StreamWatcher

from sdcm.remote.docker_cmd_runner import DockerCmdRunner


class DummyNode:
    name = 'dummy-node'


class TestWatcher(StreamWatcher):
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

    @patch('sdcm.remote.docker_cmd_runner.ContainerManager')
    def test_get_container_no_cache(self, mock_cm):
        mock_container = MagicMock()
        mock_cm.get_container.return_value = mock_container

        runner = DockerCmdRunner(self.node)
        runner._container = None
        result = runner._get_container()

        self.assertEqual(result, runner._container)
        mock_cm.get_container.assert_called_once_with(self.node, "node")

    @patch('sdcm.remote.docker_cmd_runner.ContainerManager')
    def test_get_container_cached_running(self, mock_cm):
        mock_container = MagicMock(status='running')

        runner = DockerCmdRunner(self.node)
        runner._container = mock_container
        result = runner._get_container()

        self.assertEqual(result, mock_container)
        mock_container.reload.assert_called_once()
        mock_cm.get_container.assert_not_called()

    @patch('sdcm.remote.docker_cmd_runner.ContainerManager')
    def test_get_container_cached_not_running(self, mock_cm):
        old_container = MagicMock(status='exited')
        new_container = MagicMock()
        mock_cm.get_container.return_value = new_container

        runner = DockerCmdRunner(self.node)
        runner._container = old_container
        result = runner._get_container()

        self.assertEqual(result, new_container)
        self.assertEqual(result, runner._container)
        old_container.reload.assert_called_once()
        mock_cm.get_container.assert_called_once_with(self.node, "node")

    @patch('sdcm.remote.docker_cmd_runner.ContainerManager')
    def test_get_container_not_found(self, mock_cm):
        mock_cm.get_container.return_value = None

        runner = DockerCmdRunner(self.node)
        runner._container = None

        with self.assertRaises(RuntimeError, msg="could not be resolved"):
            runner._get_container()

        mock_cm.get_container.assert_called_once_with(self.node, "node")

    def test_connection(self):
        with patch.object(self.runner, '_get_container') as mock_get:
            mock_container = MagicMock()
            mock_get.return_value = mock_container

            result = self.runner.connection

            self.assertEqual(result, mock_container)
            mock_get.assert_called_once()

    @patch('sdcm.remote.docker_cmd_runner.ContainerManager')
    def test_container_is_up(self, mock_cm):
        mock_cm.is_running.return_value = True
        self.assertTrue(self.runner.is_up())
        mock_cm.is_running.assert_called_once_with(self.node, "node")

        mock_cm.is_running.return_value = False
        self.assertFalse(self.runner.is_up())

    def test_execute_command_success(self):
        """Test _execute_command with successful execution"""
        mock_container = MagicMock(status='running')
        mock_container.exec_run.return_value = MagicMock(
            exit_code=0,
            output=(b'hello world', b''))

        with patch.object(self.runner, '_get_container', return_value=mock_container):
            result = self.runner._execute_command('echo hello', 30, False, True, [])

        self.assertEqual(result.exited, 0)
        self.assertEqual(result.stdout, 'hello world')
        self.assertEqual(result.stderr, '')
        self.assertGreater(result.duration, 0)
        mock_container.exec_run.assert_called_once_with(
            ["sh", "-c", "echo hello"], tty=False, demux=True, stream=False, user='')

    def test_execute_command_with_stderr(self):
        mock_container = MagicMock(status='running')
        mock_container.exec_run.return_value = MagicMock(
            exit_code=1,
            output=(b'', b'error message'))

        with patch.object(self.runner, '_get_container', return_value=mock_container):
            with self.assertRaises(UnexpectedExit):
                self.runner._execute_command('false', 30, False, True, [])

    @patch('sdcm.remote.docker_cmd_runner.retrying')
    def test_run(self, mock_retrying):
        mock_retrying.return_value = lambda f: f  # No-op decorator

        with (patch.object(self.runner, '_execute_command') as mock_exec,
              patch.object(self.runner, '_setup_watchers') as mock_setup,
              patch.object(self.runner, '_print_command_results') as mock_print):
            mock_result = MagicMock()
            mock_exec.return_value = mock_result
            mock_setup.return_value = []
            result = self.runner.run('echo hello', timeout=30, verbose=True)

        self.assertEqual(result, mock_result)
        mock_setup.assert_called_once_with(True, None, None)
        mock_exec.assert_called_once_with('echo hello', 30, False, True, [], '')
        mock_print.assert_called_once_with(mock_result, True, False)

    @patch('sdcm.remote.docker_cmd_runner.retrying')
    def test_send_files(self, mock_retrying):
        mock_retrying.return_value = lambda f: f

        mock_container = MagicMock()
        mock_container.name = 'test-container'

        with (tempfile.NamedTemporaryFile(mode='w', delete=True) as temp_file,
              patch.object(self.runner, '_get_container', return_value=mock_container),
              patch.object(self.runner, 'run') as mock_run,
              patch.object(self.runner, '_create_tar_stream') as mock_create_tar):
            temp_file.write('test data')
            temp_file.flush()

            mock_tar_stream = BytesIO(b'tar data')
            mock_create_tar.return_value = mock_tar_stream
            mock_run.return_value = MagicMock(ok=True)

            result = self.runner.send_files(temp_file.name, '/dest/path')

        self.assertTrue(result)
        mock_container.put_archive.assert_called_once()
        mock_create_tar.assert_called_once_with(temp_file.name, '/dest/path')

    @patch('sdcm.remote.docker_cmd_runner.retrying')
    def test_receive_files(self, mock_retrying):
        mock_retrying.return_value = lambda f: f

        tar_stream = BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            info = tarfile.TarInfo('file.txt')
            content = b'hello world'
            info.size = len(content)
            tar.addfile(info, BytesIO(content))
        tar_stream.seek(0)

        mock_container = MagicMock()
        mock_container.name = 'test-container'
        tar_data = tar_stream.getvalue()
        mock_container.get_archive.return_value = ([tar_data], {})

        with (tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file,
              patch.object(self.runner, '_get_container', return_value=mock_container)):
            result = self.runner.receive_files('/src/file.txt', temp_file.name)

            self.assertTrue(result)
            temp_file.seek(0)
            self.assertEqual(temp_file.read(), 'hello world')

    def test_sudo_with_non_root_user(self):
        with patch.object(self.runner, 'run', return_value=MagicMock()) as mock_run:
            cmd = 'ls -la'
            result = self.runner.sudo(cmd, user='testuser')

            self.assertEqual(result, mock_run.return_value)
            mock_run.assert_called_once_with(
                cmd=f'{cmd}', timeout=None, ignore_status=False, verbose=True,
                new_session=False, log_file=None, retry=1, watchers=None,  user='testuser')
