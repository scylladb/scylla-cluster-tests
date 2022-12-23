# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import time
import logging
import subprocess
from abc import abstractmethod, ABCMeta
from datetime import datetime
from functools import cached_property
from threading import Thread, Event as ThreadEvent
from multiprocessing import Process, Event
from textwrap import dedent

from sdcm import wait
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.decorators import raise_event_on_failure
from sdcm.utils.k8s import KubernetesOps
from sdcm.utils.decorators import retrying


class LoggerBase(metaclass=ABCMeta):
    def __init__(self, target_log_file: str):
        self._target_log_file = target_log_file
        self._log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self, timeout=None):
        pass


class NodeLoggerBase(LoggerBase, metaclass=ABCMeta):
    def __init__(self, node, target_log_file: str):
        self._node = node
        super().__init__(target_log_file=target_log_file)


class SSHLoggerBase(NodeLoggerBase):
    _retrieve_message = "Reading Scylla logs from {since}"

    def __init__(self, node, target_log_file: str):
        super().__init__(node, target_log_file)
        self._termination_event = Event()
        self.node = node
        self._remoter = None
        self._remoter_params = node.remoter.get_init_arguments()
        self._child_process = Process(target=self._journal_thread, daemon=True)

    @property
    @abstractmethod
    def _logger_cmd(self) -> str:
        pass

    def _file_exists(self, file_path):
        try:
            result = self._remoter.run('sudo test -e %s' % file_path,
                                       ignore_status=True)
            return result.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            self._log.error('Error checking if file %s exists: %s',
                            file_path, details)
        return False

    def _log_retrieve(self, since):
        if not since:
            since = 'the beginning'
        self._log.debug(self._retrieve_message.format(since=since))

    def _retrieve(self, since):
        since = '--since "{}" '.format(since) if since else ""
        self._remoter.run(self._logger_cmd.format(since=since),
                          verbose=True, ignore_status=True,
                          log_file=self._target_log_file)

    def _retrieve_journal(self, since):
        try:
            self._log_retrieve(since)
            self._retrieve(since)
        except Exception as details:  # pylint: disable=broad-except
            self._log.error('Error retrieving remote node DB service log: %s', details)

    @raise_event_on_failure
    def _journal_thread(self):
        # NOTE: K8S and docker backends use separate non-SSH remoters
        #       where each new call is separate process.
        #       So, reuse the remoter class we already have defined in the node.
        if self.node.is_docker():
            self._remoter = self.node.remoter
        else:
            self._remoter = RemoteCmdRunnerBase.create_remoter(**self._remoter_params)
        read_from_timestamp = None
        while not self._termination_event.is_set():
            self._wait_ssh_up(verbose=False)
            self._retrieve_journal(since=read_from_timestamp)
            read_from_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _wait_ssh_up(self, verbose=True, timeout=500):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self._remoter.is_up, step=10, text=text, timeout=timeout, throw_exc=True)

    def start(self):
        self._child_process.start()

    def stop(self, timeout=None):
        self._child_process.terminate()
        self._child_process.join(timeout)
        if self._child_process.is_alive():
            self._child_process.kill()  # pylint: disable=no-member


class SSHScyllaSystemdLogger(SSHLoggerBase):

    def _wait_ssh_up(self, *args, **kwargs):
        super()._wait_ssh_up(*args, **kwargs)
        self.wait_for_python_to_available()

    @retrying(n=30, sleep_time=15)
    def wait_for_python_to_available(self):
        assert self.node.remoter.sudo("which python3", ignore_status=True).ok

    @staticmethod
    def reformat_output_command(cmd):
        """
        Wrapping journalctl -f command with a python program
        that read a s json stream, and add the level/priority that
        is missing from regular output
        """
        return dedent("""
            PYTHON_PROG="
            import json,sys, datetime

            priorities = \\"emerg,alert,critical,error,warning,notice,info,debug\\"
            prio_map = {{str(i) : str(prio).upper() for i, prio in enumerate(priorities.split(','))}}

            for line in iter(sys.stdin.readline, b''):
                d = json.loads(line)
                o = str(datetime.datetime.fromtimestamp(int(d.get('__REALTIME_TIMESTAMP', '1000')) / 1000**2).isoformat(timespec='milliseconds'))
                o += f\\" {{d.get('_HOSTNAME', 'unknown')}}\\"
                o += f\\" !{{prio_map.get(d.get('PRIORITY', '7'), '???')}} |\\"
                o += f\\" {{d.get('SYSLOG_IDENTIFIER', 'unknown')}}[{{d.get('_PID', '0')}}]:\\"
                o += f\\" {{d.get('MESSAGE', '')}}\\"
                print(o)
            "
                      """
                      f'{cmd} -o json | python3 -c "$PYTHON_PROG"'
                      )

    @property
    def _logger_cmd(self) -> str:
        return self.reformat_output_command(
            f'{self.node.journalctl} -f --no-tail --no-pager '
            '--utc {since} '
            '-u scylla-ami-setup.service '
            '-u scylla-image-setup.service '
            '-u scylla-io-setup.service '
            '-u scylla-server.service '
            '-u scylla-jmx.service '
            '-u scylla-housekeeping-restart.service '
            '-u scylla-housekeeping-daily.service'
        )


class SSHNonRootScyllaSystemdLogger(SSHLoggerBase):
    """
    In NonRoot installation, scylla-server log is redirected a log file in install directory.
    Related commit: https://github.com/scylladb/scylla/commit/0f786f05fed41be94b09e33aa34a767074a14ec1
    """
    @property
    def _logger_cmd(self) -> str:
        scylla_log_file = '~/scylladb/scylla-server.log'
        return f'mkdir -p ~/scylladb && touch {scylla_log_file} && tail -F {scylla_log_file}'


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


class CommandLoggerBase(LoggerBase):
    _child_process = None
    restart_delay = 0.1

    def __init__(self, target_log_file: str):
        super().__init__(target_log_file)
        self._thread = Thread(target=self._thread_body, daemon=True)
        self._termination_event = ThreadEvent()
        # When first started it picks up logs for an hour before
        # to cover cases when logger is started long after node is started
        self._last_time_completed = time.time() - 60*60

    @property
    @abstractmethod
    def _logger_cmd(self) -> str:
        pass

    def _thread_body(self):
        while not self._termination_event.wait(self.restart_delay):
            try:
                # pylint: disable=consider-using-with
                self._child_process = subprocess.Popen(self._logger_cmd, shell=True)
                started = False
                try:
                    self._child_process.wait(10)
                except subprocess.TimeoutExpired:
                    # Assume that command successfully started if it is running for more than 10 seconds
                    started = True
                self._child_process.wait()
                if started:
                    # Update last time only if command successfully started
                    self._last_time_completed = time.time()
            except Exception:  # pylint: disable=broad-except
                pass

    def start(self):
        self._termination_event.clear()
        self._thread.start()

    def stop(self, timeout=None):
        self._termination_event.set()
        if self._child_process:
            self._child_process.kill()
        self._thread.join(timeout)

    @property
    def time_delta(self):
        return time.time() - self._last_time_completed


class CommandClusterLoggerBase(CommandLoggerBase, metaclass=ABCMeta):
    def __init__(self, cluster, target_log_file: str):
        self._cluster = cluster
        super().__init__(target_log_file)


class CommandNodeLoggerBase(CommandLoggerBase, metaclass=ABCMeta):
    def __init__(self, node, target_log_file: str):
        self._node = node
        super().__init__(target_log_file)


class DockerScyllaLogger(CommandNodeLoggerBase):
    # pylint: disable=invalid-overridden-method
    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} 2>&1 | grep scylla >>{self._target_log_file}'


class DockerGeneralLogger(CommandNodeLoggerBase):
    # pylint: disable=invalid-overridden-method
    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} >>{self._target_log_file} 2>&1'


class KubectlGeneralLogger(CommandNodeLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        parent_cluster = self._node.parent_cluster
        cmd = parent_cluster.k8s_cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s", self._node.name, "-c",
            parent_cluster.container, namespace=parent_cluster.namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"  # pylint: disable=protected-access


class KubectlClusterEventsLogger(CommandClusterLoggerBase):
    restart_delay = 30

    def __init__(self, *args, namespace=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd("get events -w", namespace=self.namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class CertManagerLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/instance=cert-manager",
            namespace="cert-manager")
        return f"{cmd} >> {self._target_log_file} 2>&1"  # pylint: disable=protected-access


class ScyllaManagerLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/instance=scylla-manager",
            namespace=self._cluster._scylla_manager_namespace)  # pylint: disable=protected-access
        return f"{cmd} >> {self._target_log_file} 2>&1"


class ScyllaOperatorLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/instance=scylla-operator",
            namespace=self._cluster._scylla_operator_namespace)  # pylint: disable=protected-access
        return f"{cmd} >> {self._target_log_file} 2>&1"


class KubernetesWrongSchedulingLogger(CommandClusterLoggerBase):
    restart_delay = 120
    WRONG_SCHEDULED_PODS_MESSAGE = "Not allowed pods are scheduled on Scylla node found"

    @property
    def _logger_cmd(self) -> str:
        if not self._cluster.allowed_labels_on_scylla_node:
            return ''

        wrong_scheduled_pods_on_scylla_node, node_names = [], []
        if self._cluster.SCYLLA_POOL_NAME in self._cluster.pools:
            node_names = [
                node.metadata.name
                for node in self._cluster.pools[self._cluster.SCYLLA_POOL_NAME].nodes.items]
        else:
            self._log.warning(
                "'%s' pool is not registered. Can not get node names to check pods scheduling",
                self._cluster.SCYLLA_POOL_NAME)
            return ''
        try:
            for pod in KubernetesOps.list_pods(self._cluster):
                if pod.spec.node_name not in node_names:
                    continue
                for key, value in self._cluster.allowed_labels_on_scylla_node:
                    if (key, value) in pod.metadata.labels.items():
                        break
                else:
                    wrong_scheduled_pods_on_scylla_node.append(
                        f"{pod.metadata.name} ({pod.spec.node_name} node)")
        except Exception as details:  # pylint: disable=broad-except
            self._log.warning("Failed to get pods list: %s", str(details))

        if not wrong_scheduled_pods_on_scylla_node:
            return ''

        joined_info = ', '.join(wrong_scheduled_pods_on_scylla_node)
        message = f"{self.WRONG_SCHEDULED_PODS_MESSAGE}: {joined_info}"
        return f"echo \"I`date -u +\"%m%d %H:%M:%S\"`              {message}\" >> {self._target_log_file} 2>&1"


def get_system_logging_thread(logs_transport, node, target_log_file):  # pylint: disable=too-many-return-statements
    if logs_transport == 'docker':
        if 'db-node' in node.name:
            return DockerScyllaLogger(node, target_log_file)
        return DockerGeneralLogger(node, target_log_file)
    if logs_transport == 'kubectl':
        return KubectlGeneralLogger(node, target_log_file)
    if logs_transport == 'ssh':
        if node.init_system == 'systemd':
            if 'db-node' in node.name and node.is_nonroot_install and node.remoter.run(
                    'sudo test -e /var/log/journal', ignore_status=True).exit_status != 0:
                return SSHNonRootScyllaSystemdLogger(node, target_log_file)
            if 'db-node' in node.name:
                return SSHScyllaSystemdLogger(node, target_log_file)
            return SSHGeneralSystemdLogger(node, target_log_file)
        if 'db-node' in node.name:
            return SSHScyllaFileLogger(node, target_log_file)
        else:
            return SSHGeneralFileLogger(node, target_log_file)
    return None
