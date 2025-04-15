import logging
import time

from docker.models.containers import Container
from sdcm.utils.docker_api_client import DockerAPIClient, DockerCmdExecutor

LOGGER = logging.getLogger(__name__)


class DockerServiceManager:
    """Service manager for direct service management in Docker containers"""

    SERVICE_PROCESS_MAPPING = {
        'scylla': {
            'start_cmd': '/opt/scylladb/supervisor/scylla-server.sh',
            'process_pattern': '/opt/scylladb/libexec/[s]cylla',
            'user': 'scylla'
        },
        'scylla-housekeeping': {
            'start_cmd': '/scylla-housekeeping-service.sh',
            'process_pattern': '/[s]cylla-housekeeping-service.sh',
            'user': 'scylla'
        },
        'scylla-manager': {
            'start_cmd': '/usr/bin/scylla-manager --developer-mode',
            'process_pattern': '/usr/bin/[s]cylla-manager'
        },
        'sshd': {
            'start_cmd': '/sshd-service.sh',
            'process_pattern': '/usr/sbin/[s]shd'
        },
        'node_exporter': {
            'start_cmd': '/opt/scylladb/supervisor/scylla-node-exporter.sh',
            'process_pattern': '/opt/scylladb/node_exporter/[n]ode_exporter'
        }
    }

    def __init__(self, container: Container, docker_client: DockerAPIClient | None = None):
        self.container = container
        self.docker_client = docker_client or DockerAPIClient()
        self.cmd_executor = DockerCmdExecutor(docker_client)
        self.log = LOGGER
        self.service_commands = self._discover_service_commands()

    def _discover_service_commands(self) -> dict[str, dict[str, str]]:
        """
        Inspects the container to find the proper commands to start, stop, and check
        status for each known service.

        :returns dict: mapping of service names to their command details
        """
        kill_cmd = self._get_available_kill_command()
        service_commands = {}
        for service_name, service_attrs in self.SERVICE_PROCESS_MAPPING.items():
            service_commands[service_name] = {
                'start_cmd': service_attrs['start_cmd'],
                'stop_cmd': f"{kill_cmd} '{service_name}'",
                'restart_cmd': f"{kill_cmd} '{service_name}' && sleep 2 && {service_attrs['start_cmd']}",
                # to check if service is running we can:
                # - for process that is running as binary, one of /proc/[pid]/exe will point to the service binary
                # - for process that is running as script in a shell interpreter, one of /proc/[pid]/cmdline will
                #   contain the script name that started the process
                'status_cmd': (f"readlink /proc/[0-9]*/exe 2>/dev/null | grep '{service_attrs['process_pattern']}' || "
                               f"grep -l '{service_attrs['process_pattern']}' /proc/[0-9]*/cmdline 2>/dev/null"),
                'process_pattern': service_attrs['process_pattern'],
                'type': 'process',
                'user': service_attrs.get('user', '')
            }
        return service_commands

    def _get_available_kill_command(self) -> str:
        if self._check_command_exists("killall"):
            return "killall -TERM"
        if self._check_command_exists("pkill"):
            return "pkill -TERM -f"

    def _check_command_exists(self, command: str) -> bool:
        result = self.cmd_executor.run(container=self.container, cmd=f"command -v {command}", ignore_status=True)
        return result.ok

    def _wait_for_service_status(
            self, service_name: str, service_running: bool, timeout: int, interval: int, user: str | None = None) -> bool:
        end_time = time.monotonic() + timeout
        while time.monotonic() < end_time:
            if self.is_service_running(service_name, user=user) == service_running:
                return True
            time.sleep(interval)
        self.log.warning(
            "Timeout waiting for service %s to %s", service_name, "start" if service_running else "stop")
        return False

    def start_service(self, service_name: str, timeout: int = 60, interval: int = 2, user: str | None = None) -> bool:
        if service_name not in self.service_commands:
            self.log.error("Unknown service: %s", service_name)
            return False

        service_info = self.service_commands[service_name]
        user = user or self.service_commands[service_name].get('user', '')
        self.log.debug("Starting service %s with command: %s", service_name, service_info['start_cmd'])
        try:
            self.cmd_executor.run(
                container=self.container, cmd=service_info['start_cmd'], detach=True, verbose=True, timeout=None, user=user)
            if not self._wait_for_service_status(service_name, service_running=True, timeout=timeout, interval=interval):
                self.log.error("Failed to start service %s", service_name)
                return False
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Error starting service %s: %s", service_name, str(exc))
            return False

        self.log.info("Service %s started successfully", service_name)
        return True

    def stop_service(self, service_name: str, timeout: int = 60, interval: int = 2, user: str | None = None) -> bool:
        if service_name not in self.service_commands:
            self.log.error("Unknown service: %s", service_name)
            return False

        service_info = self.service_commands[service_name]
        user = user or self.service_commands[service_name].get('user', '')
        self.log.debug("Stopping service %s with command: %s", service_name, service_info['stop_cmd'])
        try:
            self.cmd_executor.run(
                container=self.container, cmd=service_info['stop_cmd'], verbose=True,
                timeout=None, ignore_status=True, user=user)
            if not self._wait_for_service_status(
                    service_name, service_running=False, timeout=timeout, interval=interval, user=user):
                self.log.error("Failed to stop service %s", service_name)
                return False
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Error stopping service %s: %s", service_name, str(exc))
            return False

        self.log.info("Service %s stopped successfully", service_name)
        return True

    def restart_service(
            self, service_name: str, timeout: int = 120, interval: int = 2, user: str | None = None) -> bool:
        if service_name not in self.service_commands:
            self.log.error("Unknown service: %s", service_name)
            return False

        service_info = self.service_commands[service_name]
        user = user or self.service_commands[service_name].get('user', '')
        self.log.debug("Restarting service %s", service_name)
        try:
            self.cmd_executor.run(
                container=self.container, cmd=service_info['stop_cmd'], verbose=True, timeout=None, user=user)
            if not self._wait_for_service_status(service_name, service_running=False, timeout=timeout, interval=interval):
                self.log.error("Failed to stop service %s", service_name)
                return False
            self.cmd_executor.run(
                container=self.container, cmd=service_info['start_cmd'], detach=True, verbose=True, timeout=None, user=user)
            if not self._wait_for_service_status(service_name, service_running=True, timeout=timeout, interval=interval):
                self.log.error("Failed to start service %s", service_name)
                return False
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Error restarting service %s: %s", service_name, str(exc))
            return False

        self.log.info("Service %s restarted successfully", service_name)
        return True

    def get_service_status(self, service_name: str, user: str | None = None) -> str:
        if service_name not in self.service_commands:
            self.log.error("Unknown service: %s", service_name)
            return "unknown"

        service_info = self.service_commands[service_name]
        user = user or self.service_commands[service_name].get('user', '')
        try:
            result = self.cmd_executor.run(
                container=self.container, cmd=service_info['status_cmd'], ignore_status=True, verbose=False, user=user)
            return "running" if result.stdout else "stopped"
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Error getting status for service %s: %s", service_name, str(exc))
            return "unknown"

    def is_service_running(self, service_name: str, user: str | None = None) -> bool:
        return self.get_service_status(service_name, user=user) not in ["stopped", "unknown"]


def create_service_manager(container: Container, docker_client: DockerAPIClient | None = None) -> DockerServiceManager:
    """Creates a service manager for the given container"""
    return DockerServiceManager(container, docker_client)
