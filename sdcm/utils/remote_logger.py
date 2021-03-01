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

from sdcm import wait
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.decorators import raise_event_on_failure


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
        super(NodeLoggerBase, self).__init__(target_log_file=target_log_file)


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
    @property
    def _logger_cmd(self) -> str:
        return f'{self.node.journalctl} -f --no-tail --no-pager ' \
               '--utc {since} ' \
               '-u scylla-ami-setup.service ' \
               '-u scylla-image-setup.service ' \
               '-u scylla-io-setup.service ' \
               '-u scylla-server.service ' \
               '-u scylla-jmx.service'


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
            except:  # pylint: disable=bare-except
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

    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} 2>&1 | grep scylla >>{self._target_log_file}'


class DockerGeneralLogger(CommandNodeLoggerBase):

    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} >>{self._target_log_file} 2>&1'


class KubectlGeneralLogger(CommandNodeLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        pc = self._node.parent_cluster
        cmd = pc.k8s_cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s", self._node.name, "-c", pc.container,
            namespace=pc.namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class KubectlClusterEventsLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd("get events -w", namespace="scylla")
        return f"{cmd} >> {self._target_log_file} 2>&1"


class CertManagerLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/instance=cert-manager",
            namespace="cert-manager")
        return f"{cmd} >> {self._target_log_file} 2>&1"


class ScyllaManagerLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app=scylla-manager",
            namespace=self._cluster._scylla_manager_namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class ScyllaOperatorLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs  --previous=false -f --since={int(self.time_delta)}s scylla-operator-controller-manager-0 "
            "--all-containers=true",
            namespace=self._cluster._scylla_operator_namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


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
