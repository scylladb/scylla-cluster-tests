from functools import cached_property
from abc import abstractmethod
from datetime import datetime
from typing import Union, Optional
import subprocess

from sdcm import wait
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.services.base import NodeProcessService, NodeThreadService


class SSHLoggerBase(NodeProcessService):
    _retrieve_message = "Reading Scylla logs from {since}"
    _remoter = None
    _interval = 0

    def __init__(self, node, target_log_file: str, log_records: bool = True):
        self._target_log_file = target_log_file
        self._remoter_params = node.remoter.get_init_arguments()
        self._read_from_timestamp = None
        self._log_records = log_records
        super().__init__(node)

    @property
    @abstractmethod
    def _logger_cmd(self) -> str:
        pass

    def _file_exists(self, file_path):
        try:
            result = self._remoter.run('sudo test -e %s' % file_path, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            self._log.error('Error checking if file %s exists: %s',
                            file_path, details)

    def _log_retrieve(self, since):
        if not since:
            since = 'the beginning'
        self._log.debug(self._retrieve_message.format(since=since))

    def _retrieve(self, since):
        since = '--since "{}" '.format(since) if since else ""
        self._remoter.run(self._logger_cmd.format(since=since),
                          verbose=self._log_records, ignore_status=True,
                          log_file=self._target_log_file)

    def _service_body(self):
        if self._remoter is None:
            self._remoter = RemoteCmdRunnerBase.create_remoter(**self._remoter_params)
        self._log_retrieve(self._read_from_timestamp)
        self._retrieve(self._read_from_timestamp)
        self._read_from_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    @property
    def log_records(self) -> bool:
        """
        Return True if this thread log (LOGGER.debug(rec)) records gathered from remote endpoint
        :return:
        :rtype: bool
        """
        return self._log_records


class SSHScyllaSystemdLogger(SSHLoggerBase):
    @property
    def _logger_cmd(self) -> str:
        return 'sudo journalctl -f --no-tail --no-pager --utc {since} ' \
               '-u scylla-ami-setup.service ' \
               '-u scylla-image-setup.service ' \
               '-u scylla-io-setup.service ' \
               '-u scylla-server.service ' \
               '-u scylla-jmx.service'


class SSHGeneralSystemdLogger(SSHLoggerBase):
    @property
    def _logger_cmd(self) -> str:
        return 'sudo journalctl -f --no-tail --no-pager --utc {since} '


class SSHScyllaFileLogger(SSHLoggerBase):
    @property
    def _logger_cmd(self) -> str:
        return 'sudo tail -f /var/log/syslog | grep scylla'

    def _retrieve(self, since):
        wait.wait_for(self._file_exists, step=10, timeout=600, throw_exc=True,
                      file_path='/var/log/syslog')
        super()._retrieve(since)


class SSHGeneralFileLogger(SSHLoggerBase):
    @property
    def _logger_cmd(self) -> str:
        return 'sudo tail -f /var/log/syslog'

    def _retrieve(self, since):
        wait.wait_for(self._file_exists, step=10, timeout=600, throw_exc=True,
                      file_path='/var/log/syslog')
        super()._retrieve(since)


class tmpclass():
    def fileno(self):
        return 0

    def __getattribute__(self, item):
        print(item)


class CommandLoggerBase(NodeThreadService):
    _interval = 0
    _cached_logger_cmd = None
    _child_process = None

    def __init__(self, node, target_log_file: str):
        self._target_log_file = target_log_file
        super().__init__(node)

    @property
    @abstractmethod
    def _logger_cmd(self) -> str:
        pass

    def _service_body(self):
        self._child_process = subprocess.Popen(
            self._logger_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        self._child_process.wait()

    def stop(self):
        super().stop()
        if self._child_process:
            try:
                self._child_process.kill()
            except:
                pass

    @property
    def log_records(self) -> bool:
        """
        Return True if this thread log (LOGGER.debug(rec)) records gathered from remote endpoint
        :return:
        :rtype: bool
        """
        return False


class DockerScyllaLogger(CommandLoggerBase):

    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} 2>&1 | grep scylla >>{self._target_log_file}'


class DockerGeneralLogger(CommandLoggerBase):

    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} >>{self._target_log_file} 2>&1'


class KubectlScyllaLogger(CommandLoggerBase):

    @property
    def _logger_cmd(self) -> str:
        pc = self._node.parent_cluster
        return f"kubectl -s {pc.k8s_cluster.k8s_server_url} -n {pc.namespace} " \
               f"logs -f {self._node.name} -c {pc.container} 2>&1 | grep scylla >>{self._target_log_file}"


class KubectlGeneralLogger(CommandLoggerBase):

    @property
    def _logger_cmd(self) -> str:
        pc = self._node.parent_cluster
        return f"kubectl -s {pc.k8s_cluster.k8s_server_url} -n {pc.namespace} " \
               f"logs -f {self._node.name} -c {pc.container} >> {self._target_log_file} 2>&1"


class CertManagerLogger(CommandLoggerBase):

    @property
    def _logger_cmd(self) -> str:
        return f"kubectl -s {self._node.k8s_server_url} -n cert-manager " \
               "logs -l app.kubernetes.io/instance=cert-manager" \
               f" --all-containers=true -f >> {self._target_log_file} 2>&1"


class ScyllaOperatorLogger(CommandLoggerBase):

    @property
    def _logger_cmd(self) -> str:
        return f"kubectl -s {self._node.k8s_server_url} -n scylla-operator-system  " \
               f"logs scylla-operator-controller-manager-0 --all-containers=true -f >> {self._target_log_file} 2>&1"


class ScyllaManagerSystemdLogger(SSHLoggerBase):
    @property
    def _logger_cmd(self) -> str:
        return 'sudo journalctl -f -u scylla-manager --no-tail --no-pager --utc {since} '


def get_system_logging_thread(logs_transport, node, target_log_file) \
        -> Optional[Union[SSHLoggerBase, CommandLoggerBase]]:  # pylint: disable=too-many-return-statements
    if logs_transport == 'docker':
        if 'db-node' in node.name:
            return DockerScyllaLogger(node, target_log_file)
        return DockerGeneralLogger(node, target_log_file)
    if logs_transport == 'kubectl':
        if 'db-node' in node.name:
            return KubectlScyllaLogger(node, target_log_file)
        return KubectlGeneralLogger(node, target_log_file)
    if logs_transport == 'ssh':
        if node.init_system == 'systemd':
            if 'db-node' in node.name:
                return SSHScyllaSystemdLogger(node, target_log_file)
            return SSHGeneralSystemdLogger(node, target_log_file)
        if 'db-node' in node.name:
            return SSHScyllaFileLogger(node, target_log_file)
        else:
            return SSHGeneralFileLogger(node, target_log_file)
    return None
