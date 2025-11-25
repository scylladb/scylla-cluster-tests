import time
import tarfile
from io import BytesIO
from typing import TYPE_CHECKING
from shlex import quote
from pathlib import Path
import shutil

from invoke.runners import Result
from invoke.exceptions import UnexpectedExit
from invoke.watchers import StreamWatcher

from sdcm.remote.base import CommandRunner, RetryableNetworkException
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import ContainerManager


if TYPE_CHECKING:
    from sdcm.cluster_docker import DockerNode


class DockerCmdRunner(CommandRunner):
    """Command runner for executing commands inside a Docker container on the node"""

    def __init__(self, node: 'DockerNode'):
        super().__init__(user='scylla-test', hostname=node.name)
        self.node = node
        self._container = None

    def _get_container(self):
        """Retrieve and cache the Docker container associated with the node"""
        if self._container:
            try:
                self._container.reload()
                if self._container.status != 'running':
                    # reset cache
                    self._container = None
            except Exception:  # noqa: BLE001
                self._container = None

        if not self._container:
            self._container = ContainerManager.get_container(self.node, "node")
            if not self._container:
                raise RuntimeError(f"Container for node {self.node.name} could not be resolved by ContainerManager.")

        return self._container

    @property
    def connection(self):
        return self._get_container()

    def _create_connection(self):
        self._get_container()

    def is_up(self, timeout: float | None = None) -> bool:
        """Check if the Docker container associated with the node is running"""
        try:
            return ContainerManager.is_running(self.node, "node")
        except Exception:  # noqa: BLE001
            return False

    def run(
        self, cmd: str, timeout: float | None = None, ignore_status: bool = False, verbose: bool = True,
        new_session: bool = False, log_file: str | None = None, retry: int = 1,
        watchers: list[StreamWatcher] | None = None, change_context: bool = False, user: str | None = '',
        timestamp_logs: bool = False
    ) -> Result:
        """Execute a command inside a Docker container"""
        watchers_list = self._setup_watchers(verbose, log_file, watchers, timestamp_logs=timestamp_logs)

        @retrying(n=retry, sleep_time=3, allowed_exceptions=(RetryableNetworkException,))
        def _run():
            return self._execute_command(cmd, timeout, ignore_status, verbose, watchers_list, user)

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        return result

    def _execute_command(
        self, cmd: str, timeout: float | None, ignore_status: bool, verbose: bool,
        watchers: list[StreamWatcher], user: str | None = ''
    ) -> Result:
        start_time = time.perf_counter()

        if verbose:
            self.log.debug('<%s>: Docker exec command: "%s"', self.node.name, cmd)

        container = self._get_container()

        if not container or container.status != 'running':
            err_msg = f"Container {container.name if container else 'unknown'} for node {self.node.name} is not running."
            self.log.error(err_msg)
            result = Result(command=cmd, exited=127, stdout="", stderr=err_msg)
            result.duration = time.perf_counter() - start_time
            if not ignore_status:
                raise UnexpectedExit(result)
            return result

        try:
            exec_output = container.exec_run(["sh", "-c", cmd], tty=False, demux=True, stream=False, user=user)
            stdout_bytes, stderr_bytes = exec_output.output if isinstance(exec_output.output, tuple) else (b"", b"")

            result = Result(
                command=cmd,
                exited=exec_output.exit_code,
                stdout=stdout_bytes.decode(errors='replace') if stdout_bytes else "",
                stderr=stderr_bytes.decode(errors='replace') if stderr_bytes else "")
            result.duration = time.perf_counter() - start_time
            result.exit_status = exec_output.exit_code

            if verbose:
                self.log.debug('<%s>: Docker exec result (status=%d, duration=%.2fs)\nStdout:\n%s\nStderr:\n%s',
                               self.node.name, result.exited, result.duration, result.stdout, result.stderr)
            if result.exited != 0 and not ignore_status:
                raise UnexpectedExit(result)

            return result

        except UnexpectedExit:
            raise
        except Exception as exc:  # noqa: BLE001
            duration = time.perf_counter() - start_time
            self.log.error("Exception during docker exec on %s for command '%s': %s",
                           self.node.name, cmd, str(exc), exc_info=True)
            result = Result(command=cmd, exited=255, stdout="", stderr=str(exc))
            result.duration = duration
            if not ignore_status:
                raise UnexpectedExit(result) from exc
            return result

    @staticmethod
    def _create_tar_stream(src: str, dst: str) -> BytesIO:
        """Create a tar stream from a file or directory"""
        tar_stream = BytesIO()
        src_path = Path(src)
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            if src_path.is_dir():
                for file_path in src_path.rglob('*'):
                    if file_path.is_file():
                        tar.add(str(file_path), arcname=str(file_path.relative_to(src_path.parent)))
            else:
                arcname = Path(dst).name if not dst.endswith('/') else src_path.name
                tar.add(str(src_path), arcname=arcname)
        tar_stream.seek(0)
        return tar_stream

    @staticmethod
    def _extract_tar_stream(tar_bytes: BytesIO, dst: str):
        """Extract a tar stream to the destination path"""
        dst_path = Path(dst)
        with tarfile.open(fileobj=tar_bytes, mode='r') as tar:
            if dst_path.is_dir() or str(dst).endswith('/'):
                dst_path.mkdir(parents=True, exist_ok=True)
                tar.extractall(path=str(dst_path))
            else:
                members = tar.getmembers()
                if len(members) == 1 and members[0].isfile():
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(dst_path, 'wb') as out_f:
                        tar_member = tar.extractfile(members[0])
                        if tar_member:
                            out_f.write(tar_member.read())
                else:
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    tar.extractall(path=str(dst_path.parent))

    @retrying(n=3, sleep_time=5, allowed_exceptions=(Exception,))
    def send_files(
        self, src: str, dst: str, delete_dst: bool = False, preserve_symlinks: bool = False,
        verbose: bool = False, timeout: float = 300, **kwargs
    ) -> bool:
        """Sends files from the local filesystem to a specified path inside a Docker container"""
        container = self._get_container()

        if verbose:
            self.log.debug("Sending local '%s' to container '%s:%s'", src, container.name, dst)

        try:
            if delete_dst:
                self.run(f"rm -rf {quote(dst)}", ignore_status=True, verbose=False)

            tar_stream = self._create_tar_stream(src, dst)
            dst_path = Path(dst)
            extraction_dir = dst if dst.endswith('/') or not dst_path.suffix else str(dst_path.parent)
            self.run(f"mkdir -p {quote(extraction_dir)}", ignore_status=True, verbose=False)
            container.put_archive(path=extraction_dir, data=tar_stream)

            if verbose:
                self.log.info("Sent '%s' to '%s:%s'", src, container.name, dst)
            return True

        except Exception as e:
            self.log.error("Failed to send '%s' to container '%s:%s': %s",
                           src, container.name, dst, str(e), exc_info=True)
            return False

    @retrying(n=3, sleep_time=5, allowed_exceptions=(Exception,))
    def receive_files(
        self, src: str, dst: str, delete_dst: bool = False, preserve_perm: bool = True,
        preserve_symlinks: bool = False, timeout: float = 300, **kwargs
    ) -> bool:
        """Receives files from a specified path inside a Docker container to the local filesystem"""
        container = self._get_container()

        try:
            dst_path = Path(dst)
            if delete_dst and dst_path.exists():
                if dst_path.is_dir():
                    shutil.rmtree(dst_path)
                else:
                    dst_path.unlink()

            tar_stream_bits, _ = container.get_archive(src)
            tar_bytes = BytesIO()
            for chunk in tar_stream_bits:
                tar_bytes.write(chunk)
            tar_bytes.seek(0)

            self._extract_tar_stream(tar_bytes, dst)
            self.log.info("Received '%s:%s' to local '%s'", container.name, src, dst)
            return True

        except Exception as e:
            self.log.error("Failed to receive from container '%s:%s' to local '%s': %s",
                           container.name, src, dst, str(e), exc_info=True)
            return False

    def sudo(
        self, cmd: str, timeout: float | None = None, ignore_status: bool = False, verbose: bool = True,
        new_session: bool = False, log_file: str | None = None, retry: int = 1,
        watchers: list[StreamWatcher] | None = None, user: str | None = 'root'
    ) -> Result:
        return self.run(cmd=cmd, timeout=timeout, ignore_status=ignore_status,
                        verbose=verbose, new_session=new_session, log_file=log_file,
                        retry=retry, watchers=watchers, user=user)

    def file_exists(self, file_path: str) -> bool:
        result = self.run(f"test -e {quote(file_path)}", ignore_status=True, verbose=False)
        return result.ok

    def get_init_arguments(self) -> dict:
        return {'node_name': self.node.name, 'type': 'DockerCmdRunner'}

    def __del__(self):
        self._container = None
