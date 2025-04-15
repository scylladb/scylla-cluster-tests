import io
import concurrent.futures
import docker
import logging
import tarfile
import time
from pathlib import Path
from typing import Any

from docker.errors import DockerException, APIError
from docker.models.containers import Container

LOGGER = logging.getLogger(__name__)


class DockerAPIClient:
    """Wrapper for Docker API interactions in SCT"""

    def __init__(self, base_url: str = 'unix://var/run/docker.sock'):
        """Initialize Docker client.

        :param base_url: str, Docker socket URL.
            If None, environment variables will be used (defaults to 'unix://var/run/docker.sock')
        """
        self.client = docker.DockerClient(base_url=base_url) if base_url else docker.from_env()
        self._base_url = base_url
        self.log = LOGGER

    def resolve_container(self, container: Container | str) -> Container:
        """Resolve container reference to a Container object"""
        if isinstance(container, str):
            container = self.get_container(container)
        if not container:
            raise ValueError(f"Container {container} not found")
        return container

    @staticmethod
    def prepare_command(command: str | list[str]) -> list[str]:
        return ["/bin/sh", "-c", command] if isinstance(command, str) else command

    def get_container(self, container_id_or_name: str) -> Container | None:
        """Get container by id or name"""
        try:
            return self.client.containers.get(container_id_or_name)
        except DockerException:
            self.log.error("Container %s not found", container_id_or_name)
            return None

    def execute_command(self,
                        container: Container | str,
                        command: str,
                        user: str = '',
                        detach: bool = False,
                        environment: dict[str, str] | None = None,
                        workdir: str | None = None,
                        privileged: bool = False) -> tuple:
        """Execute command in container.

        :param container: Container | str, container object or ID/name
        :param command: str, command to execute
        :param user: str, user to run command
        :param detach: bool, run in background
        :param environment: dict | None, environment variables
        :param workdir: str | None, working directory
        :param privileged: bool, run with elevated privileges

        :returns tuple:
        """
        container = self.resolve_container(container)
        cmd_list = self.prepare_command(command)
        try:
            self.log.debug("Executing command in container %s: %s", container.name, command)
            result = container.exec_run(
                cmd=cmd_list,
                stdin=False,
                detach=detach,
                user=user,
                environment=environment,
                workdir=workdir,
                privileged=privileged,
                tty=False,
                demux=True)
            if detach:
                return 0, result.output, ''
            stdout_bytes, stderr_bytes = result.output
            stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ''
            stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ''
            return result.exit_code, stdout, stderr
        except APIError as exc:
            self.log.error("API error starting command in %s: %s", container.name, exc)
            raise DockerException(f"API error starting exec: {exc}") from exc
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Unexpected error starting command in %s: %s", container.name, exc, exc_info=True)
            raise DockerException(f"Unexpected error starting exec: {exc}") from exc

    def copy_to_container(self, container: Container | str, local_path: str | Path, container_path: str | Path) -> None:
        """Copy file or directory to container"""
        container = self.resolve_container(container)
        try:
            self.log.debug("Copying %s to %s:%s", local_path, container.name, container_path)
            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                tar.add(local_path, arcname=container_path.name)
            tar_buffer.seek(0)
            container.put_archive(str(container_path.parent), tar_buffer.read())
        except DockerException as exc:
            self.log.error("Error copying file to container %s: %s", container.name, exc)
            raise

    def copy_from_container(self, container: Container | str, container_path: str | Path, local_path: str | Path) -> None:
        """Copy file or directory from container"""
        container = self.resolve_container(container)
        try:
            self.log.debug("Copying %s:%s to %s", container.name, container_path, local_path)
            bits, _ = container.get_archive(container_path)
            tar_stream = io.BytesIO()
            for chunk in bits:
                tar_stream.write(chunk)
            tar_stream.seek(0)

            with tarfile.open(fileobj=tar_stream) as tar:
                member = tar.getmember(container_path.name)
                if not member:
                    raise DockerException(f"No file {container_path} found in container {container.name}")

                extracted_file = tar.extractfile(member)
                if not extracted_file:
                    raise DockerException(f"Failed to extract file {container_path} from container {container.name}")
                with open(local_path, 'wb') as dest_file:
                    dest_file.write(extracted_file.read())
        except DockerException as exc:
            self.log.error("Error copying file from container %s: %s", container.name, exc)
            raise

    def list_containers(self, all_containers: bool = False, filters: dict[str, Any] | None = None) -> list[Container]:
        return self.client.containers.list(all=all_containers, filters=filters)

    def remove_container(self, container: Container | str, force: bool = False, remove_volumes: bool = False) -> None:
        container = self.resolve_container(container)
        self.log.debug("Removing container %s", container.name)
        container.remove(force=force, v=remove_volumes)

    def __enter__(self) -> 'DockerAPIClient':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            self.client.close()
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("Error closing Docker client: %s", exc)
        return False


class DockerCmdResult:
    """Represents the result of a docker command execution"""

    def __init__(self, exit_code: int, stdout: str, stderr: str = "", command: str = None):
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.command = command

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    def __str__(self) -> str:
        return f"Command: {self.command}\nExit status: {self.exit_code}"


class DockerCmdExecutor:
    """Execute commands in Docker containers, with interface compatible with existing RemoterCmdRunner"""

    def __init__(self, docker_client: DockerAPIClient | None = None):
        self.docker_client = docker_client or DockerAPIClient()
        self.log = LOGGER

    def run(self,
            container: Container | str,
            cmd: str,
            timeout: int | None = None,
            detach: bool = False,
            ignore_status: bool = False,
            verbose: bool = False,
            retry: int = 0,
            interval: int = 2,
            **kwargs) -> DockerCmdResult | None:
        """Run command in container.

        :param container: Container | str, container object or ID/name
        :param cmd: str, command to execute
        :param timeout: int | None, command timeout in seconds
        :param detach: bool, run in background
        :param ignore_status: bool, don't raise exception on non-zero exit code
        :param verbose: bool, log command output
        :param retry: int, number of retries on failure (defaults to 0)
            **kwargs: Additional arguments for docker_client.execute_command
        :param interval: int, wait time between retries in seconds

        :returns DockerCmdResult:
        """
        if verbose:
            self.log.debug("Running command in container %s: %s", container, cmd)
        attempt = 0
        while True:
            attempt += 1
            try:
                if timeout is None:
                    exit_code, stdout, stderr = self.docker_client.execute_command(
                        container=container, command=cmd, detach=detach, **kwargs)
                    return DockerCmdResult(exit_code=exit_code, stdout=stdout, stderr=stderr, command=cmd)

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        self.docker_client.execute_command,
                        container=container, command=cmd, detach=detach, **kwargs)
                    try:
                        exit_code, stdout, stderr = future.result(timeout=timeout)
                        return DockerCmdResult(exit_code=exit_code, stdout=stdout, stderr=stderr, command=cmd)
                    except concurrent.futures.TimeoutError:
                        self.log.warning(
                            "Command timed out after %ss in container %s: %s", timeout, container, cmd)
                        raise TimeoutError(f"Command timed out after {timeout}s: {cmd}") from None
                    except Exception as exc:
                        self.log.error("Timed exec command failed in thread for %s: %s", container, exc)
                        raise DockerException(f"Unexpected error in exec with timeout thread: {exc}") from exc
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                if attempt <= retry:
                    self.log.warning("Command execution error, retrying (%s/%s): %s", attempt, retry, str(exc))
                    time.sleep(interval)
                    continue
                if ignore_status:
                    self.log.warning("Command execution failed, but ignoring due to ignore_status=True: %s", str(exc))
                    return DockerCmdResult(exit_code=1, stdout="", stderr=str(exc), command=cmd)
                raise

    def sudo(self, container: Container | str, cmd: str, **kwargs) -> DockerCmdResult:
        return self.run(container, f"sudo {cmd}", **kwargs)

    def is_up(self, container: Container | str, timeout: int = 10) -> bool:
        try:
            result = self.run(container, "echo CONTAINER_IS_UP", timeout=timeout, ignore_status=True)
            return result.ok and "CONTAINER_IS_UP" in result.stdout
        except Exception:  # pylint: disable=broad-except  # noqa: BLE001
            return False

    def receive_files(self, container: Container | str, src: str, dst: str) -> None:
        self.docker_client.copy_from_container(container, src, dst)

    def send_files(self, container: Container | str, src: str, dst: str) -> None:
        self.docker_client.copy_to_container(container, src, dst)
