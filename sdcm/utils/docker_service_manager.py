import logging
import time

from sdcm.utils.docker_remote import RemoteDocker

LOGGER = logging.getLogger(__name__)


class DockerServiceManager:
    """Service manager for direct service management in Docker containers"""

    SERVICE_PROCESS_MAPPING = {
        'scylla': {
            'start_cmd': '/usr/bin/scylla',
            'user': 'scylla'
        },
    }

    def __init__(self, remote_docker: RemoteDocker):
        self.cmd_runner = remote_docker
        self.log = LOGGER
        self.service_commands = self._discover_service_commands()

    def _discover_service_commands(self) -> dict[str, dict[str, str]]:
        """
        Maps the known services to their respective commands for starting, stopping and checking status.

        :returns dict: mapping of service names to their command details
        """
        kill_cmd = self._get_available_kill_command()
        service_commands = {}
        for service_name, service_attrs in self.SERVICE_PROCESS_MAPPING.items():
            service_commands[service_name] = {
                'start_cmd': service_attrs['start_cmd'],
                'stop_cmd': f"{kill_cmd} '{service_name}'",
                'restart_cmd': f"{kill_cmd} '{service_name}' && sleep 2 && {service_attrs['start_cmd']}",
                'status_cmd': f"ps -ef | grep {service_name} | grep -v grep",
                'user': service_attrs.get('user', '')
            }
        return service_commands

    def _get_available_kill_command(self) -> str:
        if self._check_command_exists("killall"):
            return "killall -TERM"
        return "pkill -TERM -f"

    def _check_command_exists(self, command: str) -> bool:
        result = self.cmd_runner.run(f"command -v {command}", ignore_status=True)
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

    @staticmethod
    def _compose_user_command(cmd: str, user: str | None) -> str:
        return f"su - {user} -c '{cmd}'" if (user and user != 'root') else cmd

    def start_service(self, service_name: str, timeout: int = 60, interval: int = 2, user: str | None = None) -> bool:
        if service_name not in self.service_commands:
            self.log.error("Unknown service: %s", service_name)
            return False

        service_info = self.service_commands[service_name]
        user = user or service_info.get('user', '')
        self.log.debug("Starting service %s with command: %s", service_name, service_info['start_cmd'])
        try:
            start_cmd = self._compose_user_command(service_info['start_cmd'], user)
            self.cmd_runner.run(start_cmd, ignore_status=False)
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
        user = user or service_info.get('user', '')
        self.log.debug("Stopping service %s with command: %s", service_name, service_info['stop_cmd'])
        try:
            stop_cmd = self._compose_user_command(service_info['stop_cmd'], user)
            self.cmd_runner.run(stop_cmd, ignore_status=True)
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

        user = user or self.service_commands[service_name].get('user', '')
        self.log.debug("Restarting service %s", service_name)
        try:
            if not self.stop_service(service_name, timeout=timeout//2, interval=interval, user=user):
                self.log.error("Failed to stop service %s", service_name)
                return False
            if not self.start_service(service_name, timeout=timeout//2, interval=interval, user=user):
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
            status_cmd = self._compose_user_command(service_info['status_cmd'], user)
            result = self.cmd_runner.run(status_cmd, ignore_status=True, verbose=False)
            return "running" if result.stdout else "stopped"
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.error("Error getting status for service %s: %s", service_name, str(exc))
            return "unknown"

    def is_service_running(self, service_name: str, user: str | None = None) -> bool:
        return self.get_service_status(service_name, user=user) not in ["stopped", "unknown"]


def create_service_manager(cmd_runner: RemoteDocker) -> DockerServiceManager:
    """Creates a service manager for the given docker node"""
    return DockerServiceManager(cmd_runner)
