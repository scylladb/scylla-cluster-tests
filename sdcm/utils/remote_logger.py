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

from __future__ import annotations

import os
import time
import socket
import logging
import subprocess
from abc import abstractmethod, ABCMeta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import cached_property
from threading import Lock, Thread, Event as ThreadEvent
from typing import TYPE_CHECKING
from multiprocessing import Process, Event
from textwrap import dedent

import kubernetes as k8s
from dateutil.parser import isoparse
from urllib3.exceptions import (
    MaxRetryError,
    ProtocolError,
    ReadTimeoutError,
)

from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events import Severity
from sdcm.sct_events.decorators import raise_event_on_failure
from sdcm.sct_events.loaders import HDRFileMissed
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.k8s import KubernetesOps
from sdcm.utils.decorators import retrying

if TYPE_CHECKING:
    from typing import Generator

    from sdcm.cluster import BaseNode


logging.getLogger('parso.python.diff').setLevel(logging.WARNING)


LOGGER = logging.getLogger(__name__)


class LoggerBase(metaclass=ABCMeta):
    def __init__(self, target_log_file: str):
        self._target_log_file = target_log_file
        self._log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def stop(self, timeout: float | None = None) -> None:
        ...


class SSHLoggerBase(LoggerBase):
    RETRIEVE_LOG_MESSAGE_TEMPLATE = "SSHLogger reading {log_file} from {since}"
    VERBOSE_RETRIEVE = True
    READINESS_CHECK_DELAY = 10  # seconds

    def __init__(self, node: BaseNode, target_log_file: str):
        super().__init__(target_log_file=target_log_file)
        self._node = node
        self._termination_event = Event()
        self._child_process = Process(target=self._journal_thread, daemon=True)

    def start(self) -> None:
        self._termination_event.clear()
        self._child_process.start()

    def stop(self, timeout: float | None = None) -> None:
        self._termination_event.set()
        self._child_process.terminate()
        self._child_process.join(timeout=timeout)
        if self._child_process.is_alive():
            self._child_process.kill()

    @raise_event_on_failure
    def _journal_thread(self) -> None:
        read_from_timestamp = None
        while not self._termination_event.is_set():
            if self._is_ready_to_retrieve():
                self._retrieve(since=read_from_timestamp)
                read_from_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            else:
                time.sleep(self.READINESS_CHECK_DELAY)

    def _is_ready_to_retrieve(self) -> bool:
        return self._remoter.is_up()

    def _retrieve(self, since: str) -> None:
        self._log.debug(self.RETRIEVE_LOG_MESSAGE_TEMPLATE.format(
            log_file=self._target_log_file, since=since or "the beginning"))
        try:
            self._remoter.run(
                cmd=self._logger_cmd_template.format(since=f'--since "{since}" ' if since else ""),
                verbose=self.VERBOSE_RETRIEVE,
                ignore_status=True,
                log_file=self._target_log_file,
            )
        except Exception as details:  # noqa: BLE001
            self._log.error("Error retrieving remote node DB service log: %s", details)

    @cached_property
    def _remoter(self) -> RemoteCmdRunnerBase:
        if self._node.is_docker():
            # NOTE: K8S and docker backends use separate non-SSH remoters
            #       where each new call is a separate process.
            #       So, reuse the remoter class we already have defined in the node.
            return self._node.remoter
        return RemoteCmdRunnerBase.create_remoter(**self._node.remoter.get_init_arguments())

    @property
    @abstractmethod
    def _logger_cmd_template(self) -> str:  # The `since' parameter will be passed to a returned template.
        ...


class HDRHistogramFileLogger(SSHLoggerBase):
    VERBOSE_RETRIEVE = False

    def __init__(self, node: BaseNode, remote_log_file: str, target_log_file: str):
        super().__init__(node=node, target_log_file=target_log_file)
        self._child_process = None
        self._remote_log_file = remote_log_file
        self.target_log_file = target_log_file
        self._child_thread = ThreadPoolExecutor(max_workers=1)
        self._thread = None
        self._lock = Lock()
        self._started = False

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            LOGGER.debug("Start to read target_log_file: %s", self.target_log_file)
            self._termination_event.clear()
            self._thread = self._child_thread.submit(self._journal_thread)
            self._started = True
            LOGGER.debug("Journal thread started for target_log_file: %s", self.target_log_file)

    def stop(self, timeout: float | None = None) -> None:
        with self._lock:
            self._termination_event.set()
            # ensure the tail command to be stopped
            self._remoter.run(f"pkill -f '{self._remote_log_file}'", ignore_status=True)
            if self._thread.running():
                self._thread.cancel()
            self._started = False

    @cached_property
    def _logger_cmd_template(self) -> str:
        return f"tail -f {self._remote_log_file}"

    def validate_and_collect_hdr_file(self):
        """
        Validate that HDR file exists on the SCT runner.
        If it does not exist check if the file was created on the loader.
        If the HDR file found on the loader, try to copy to the runner.
        If the file is missed even on the loader - print error event.
        """
        if os.path.exists(self._target_log_file):
            return

        LOGGER.debug("'%s' file is not found on the runner. Try to find it on the loader %s",
                     self._target_log_file, self._node.name)
        HDRFileMissed(
            message=f"'{self._remote_log_file}' HDR file was not copied to the runner from loader",
            severity=Severity.WARNING).publish()
        result = self._node.remoter.run(f"test -f {self._remote_log_file}", ignore_status=True)
        if not result.ok:
            HDRFileMissed(
                message=f"'{self._remote_log_file}' HDR file was not created on the loader {self._node.name}",
                severity=Severity.ERROR).publish()
        try:
            LOGGER.debug("The '%s' file found on the loader %s", self._remote_log_file, self._node.name)
            self._node.remoter.receive_files(src=self._remote_log_file, dst=self._target_log_file)
        except Exception:  # noqa: BLE001
            HDRFileMissed(
                message=f"'{self._remote_log_file}' HDR file couldn't copied from loader {self._node.name}",
                severity=Severity.ERROR).publish()

    # @raise_event_on_failure
    def _journal_thread(self) -> None:
        LOGGER.debug("Start journal thread. %s", self._remote_log_file)
        read_from_timestamp = None
        te_is_set = self._termination_event.is_set()
        while not te_is_set:
            LOGGER.debug("Start check if remoter ready. %s", self._remote_log_file)
            if self._is_ready_to_retrieve():
                LOGGER.debug("Remoter ready. %s", self._remote_log_file)
                self._retrieve(since=read_from_timestamp)
                LOGGER.debug("Retrieve finished. %s", self._remote_log_file)
                read_from_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            else:
                LOGGER.debug("Remoter is not ready. %s", self._remote_log_file)
                time.sleep(self.READINESS_CHECK_DELAY)
            te_is_set = self._termination_event.is_set()
            LOGGER.debug("_termination_event is set?: %s. %s", te_is_set, self._remote_log_file)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.validate_and_collect_hdr_file()
        self.stop()


class SSHScyllaSystemdLogger(SSHLoggerBase):
    def _is_ready_to_retrieve(self) -> bool:
        return super()._is_ready_to_retrieve() and self._remoter.sudo(cmd="which python3", ignore_status=True).ok

    @staticmethod
    def reformat_output_command(cmd: str) -> str:
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

    @cached_property
    def _logger_cmd_template(self) -> str:
        return self.reformat_output_command(
            f'{self._node.journalctl} -f --no-tail --no-pager '
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
    SCYLLA_LOG_FILE = "~/scylladb/scylla-server.log"

    @cached_property
    def _logger_cmd_template(self) -> str:
        return f"mkdir -p ~/scylladb && touch {self.SCYLLA_LOG_FILE} && tail -F {self.SCYLLA_LOG_FILE}"


class SSHGeneralSystemdLogger(SSHLoggerBase):
    @cached_property
    def _logger_cmd_template(self) -> str:
        return 'sudo journalctl -f --no-tail --no-pager --utc {since} '


class SSHGeneralFileLogger(SSHLoggerBase):
    REMOTE_LOG_PATH = "/var/log/syslog"

    def _is_ready_to_retrieve(self) -> bool:
        return super()._is_ready_to_retrieve() and self._is_file_exist(file_path=self.REMOTE_LOG_PATH)

    def _is_file_exist(self, file_path: str) -> bool:
        try:
            return self._remoter.run(cmd=f"sudo test -e {file_path}", ignore_status=True).ok
        except Exception as details:  # noqa: BLE001
            self._log.error("Error checking if file %s exists: %s", file_path, details)
        return False

    @cached_property
    def _logger_cmd_template(self) -> str:
        return f"sudo tail -f {self.REMOTE_LOG_PATH}"


class SSHScyllaFileLogger(SSHGeneralFileLogger):
    @cached_property
    def _logger_cmd_template(self) -> str:
        return f"{super()._logger_cmd_template} | grep scylla"


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
            except Exception:  # noqa: BLE001
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


class DockerGeneralLogger(CommandNodeLoggerBase):

    @cached_property
    def _logger_cmd(self) -> str:
        return f'docker logs -f {self._node.name} >>{self._target_log_file} 2>&1'


class KubectlGeneralLogger(CommandNodeLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        parent_cluster = self._node.parent_cluster
        cmd = self._node.k8s_cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s", self._node.name, "-c",
            parent_cluster.container, namespace=parent_cluster.namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class K8sClientLogger(LoggerBase):
    READ_REQUEST_TIMEOUT = 1200  # 20 minutes
    RECONNECT_DELAY = 30
    CHUNK_SIZE = 64

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", target_log_file: str):  # noqa: F821
        """
        Reads logs from a k8s pod using k8s client API and forwards it to a file.

        It is self-healing and will re-open the stream if it is closed and continue reading logs from place it stopped.
        """
        super().__init__(target_log_file)
        self._pod_name = pod.name
        self._parent_cluster = pod.parent_cluster
        self._k8s_cluster = pod.k8s_cluster
        self._last_log_timestamp = ""
        self._last_read_timestamp = None
        self._k8s_core_v1_api = KubernetesOps.core_v1_api(self._k8s_cluster.get_api_client())
        self._termination_event = ThreadEvent()
        self._file_object = open(self._target_log_file, "a", encoding="utf-8")
        self._log_reader = None
        self._stream = None
        self._thread = Thread(target=self._log_loop)

    def start(self):
        self._log.info("Starting logger for pod %s", self._pod_name)
        self._thread.start()

    def stop(self, timeout=None):
        if not self._thread.is_alive():
            self._log.info("Logger for pod %s is already stopped. Ignoring.", self._pod_name)
            return
        self._log.info("Stopping logger for pod %s", self._pod_name)
        self._termination_event.set()
        if self._stream:
            # NOTE: Use 'socket' lib to close the logs watcher forcibly.
            #       It is needed because the 'self._stream.close()' will wait
            #       until the 'self.READ_REQUEST_TIMEOUT' ends and so in each of the pod threads
            #       which get closed serially.
            try:
                sock_obj = socket.fromfd(self._stream.fileno(), socket.AF_INET, socket.SOCK_STREAM)
                sock_obj.shutdown(socket.SHUT_RDWR)
                sock_obj.close()
            except AttributeError as exc:
                # NOTE: try-block is added to cover cases when following bug gets reproduced (rare):
                #       https://github.com/scylladb/scylla-cluster-tests/issues/6191
                self._log.warning("Closing of the socket for the 'self._stream' object has failed: %s", exc)
        self._thread.join(timeout)
        self._file_object.close()

    def _wait_for_pod_not_pending_or_unknown(self, namespace, pod_name, timeout=60):
        self._log.debug("Waiting for pod %s to be in not pending/unknown state", self._pod_name)
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pod = self._k8s_core_v1_api.read_namespaced_pod(name=pod_name, namespace=namespace)
                pod_status = pod.status.phase
                if pod_status.lower() not in ("pending", "unknown"):
                    self._log.debug("Pod %s is in '%s' state", self._pod_name, pod_status)
                    return True
            except k8s.client.exceptions.ApiException as exc:
                self._log.debug("Error occurred while fetching pod %s status: %s", self._pod_name, exc)
            time.sleep(5)
        self._log.debug("Timeout waiting for pod: '{pod_name}' is still in '{pod_status}' state.")
        return False

    @retrying(n=20, sleep_time=3, allowed_exceptions=(ConnectionError, ))
    def _open_stream(self) -> None:
        if self._stream:
            self._stream.close()
            self._stream = None
        if self._termination_event.is_set():
            return
        self._wait_for_pod_not_pending_or_unknown(self._parent_cluster.namespace, self._pod_name)
        self._log.debug("Opening log reader stream for pod %s", self._pod_name)
        since_seconds = round(time.time() - self._last_read_timestamp + 1) if self._last_read_timestamp else None
        try:
            self._stream = self._k8s_core_v1_api.read_namespaced_pod_log(
                name=self._pod_name,
                namespace=self._parent_cluster.namespace,
                container=self._parent_cluster.container,
                timestamps=True,
                follow=True,
                since_seconds=since_seconds,
                # NOTE: need to set a timeout, because GKE's 'pod_log' API tends to hang
                _request_timeout=self.READ_REQUEST_TIMEOUT,
                _preload_content=False
            )

            self._log_reader = self._read_log_lines(self._stream)
            self._reread_logs_till_last_logged_timestamp()
        except (k8s.client.rest.ApiException, StopIteration) as exc:
            self._log.warning(
                "'_open_stream()': failed to open pod log stream:\n%s", exc)
            # NOTE: following is workaround for the error 401 which may happen due to
            #       some config data corruption during the forced socket connection failure
            self._k8s_core_v1_api = KubernetesOps.core_v1_api(self._k8s_cluster.get_api_client())
            raise ConnectionError(str(exc)) from None

    def _reread_logs_till_last_logged_timestamp(self):
        """Upon reconnection, reread the log till last logged timestamp is found to avoid duplication"""
        if not self._last_log_timestamp:
            return
        log_timestamp, line = next(self._log_reader)
        if isoparse(log_timestamp) >= isoparse(self._last_log_timestamp):
            self._log.debug("Current timestap is greater than last logged timestamp. No need to reread logs")
            self._write_log_line(log_timestamp, line)
            return
        self._log.debug("Rereading logs till %s", self._last_log_timestamp)
        while True:
            for _ in range(1000):
                log_timestamp = next(self._log_reader)[0]
                if log_timestamp == self._last_log_timestamp:
                    return
            # each 1000 lines, check if we didn't pass the date, this should not happen and possibly we can drop this code after some time
            if isoparse(log_timestamp) >= isoparse(self._last_log_timestamp):
                TestFrameworkEvent(source="K8sClientLogger",
                                   message=f"Failed to find last log timestamp in the log stream for {self._pod_name} after reconnection."
                                           f" Possibly some log lines are missed or duplicated. Last reread timestamp: {log_timestamp}",
                                   severity=Severity.ERROR).publish()
                return

    def _write_log_line(self, timestamp, line):
        self._last_read_timestamp = time.time()
        self._last_log_timestamp = timestamp
        self._file_object.write(line)

    def _read_log_lines(self, stream) -> Generator[tuple[str, str], None, None]:
        """Reads log lines. Returns a tuple of timestamp and log line"""
        buffer = ''
        while not self._termination_event.is_set():
            chunk = stream.read(self.CHUNK_SIZE).decode("utf-8")
            if not chunk:
                break
            buffer += chunk
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                try:
                    timestamp, line = line.split(maxsplit=1)
                    yield timestamp, line + "\n"
                except ValueError:
                    # sometimes returns empty line without timestamp, ignore it
                    pass
                except StopIteration:
                    return

    def _log_loop(self):
        self._open_stream()
        while not self._termination_event.is_set():
            try:
                timestamp, line = next(self._log_reader)
                self._write_log_line(timestamp, line)
            except StopIteration:
                # pod probably has been deleted, waiting for a while and trying to reconnect
                self._log.debug("Stream from pod %s logs has been closed, "
                                "waiting for %s seconds and trying to reconnect",
                                self._pod_name, self.RECONNECT_DELAY)
                self._stream = None
                if self._termination_event.is_set():
                    return
                time.sleep(self.RECONNECT_DELAY)
                self._open_stream()
            except (MaxRetryError, ProtocolError, ReadTimeoutError, TimeoutError, AttributeError) as exc:
                self._log.debug(
                    "'_read_log_line()': failed to read from pod %s log stream:%s", self._pod_name, exc)
                self._open_stream()
            except Exception as exc:  # noqa: BLE001
                self._log.error(
                    "'_read_log_line()': failed to read from pod %s log stream:%s", self._pod_name, exc)
                self._open_stream()


class KubectlClusterEventsLogger(CommandClusterLoggerBase):
    restart_delay = 30

    def __init__(self, *args, namespace=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace

    @property
    def _logger_cmd(self) -> str:
        cmd = (
            "get events -w -o custom-columns="
            "FirstSeen:.firstTimestamp,LastSeen:.lastTimestamp,"
            "Count:.count,From:.source.component,Type:.type,"
            "Reason:.reason,Message:.message"
        )
        cmd = self._cluster.kubectl_cmd(cmd, namespace=self.namespace)
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
            "-l app.kubernetes.io/instance=scylla-manager",
            namespace=self._cluster._scylla_manager_namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class ScyllaOperatorLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/instance=scylla-operator",
            namespace=self._cluster._scylla_operator_namespace)
        return f"{cmd} >> {self._target_log_file} 2>&1"


class HaproxyIngressLogger(CommandClusterLoggerBase):
    restart_delay = 30

    @property
    def _logger_cmd(self) -> str:
        cmd = self._cluster.kubectl_cmd(
            f"logs --previous=false -f --since={int(self.time_delta)}s --all-containers=true "
            "-l app.kubernetes.io/name=haproxy-ingress",
            namespace="haproxy-controller")
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
        except Exception as details:  # noqa: BLE001
            self._log.warning("Failed to get pods list: %s", str(details))

        if not wrong_scheduled_pods_on_scylla_node:
            return ''

        joined_info = ', '.join(wrong_scheduled_pods_on_scylla_node)
        message = f"{self.WRONG_SCHEDULED_PODS_MESSAGE}: {joined_info}"
        return f"echo \"I`date -u +\"%m%d %H:%M:%S\"`              {message}\" >> {self._target_log_file} 2>&1"


def get_system_logging_thread(logs_transport, node, target_log_file):  # noqa: PLR0911
    if logs_transport == 'docker':
        return DockerGeneralLogger(node, target_log_file)
    if logs_transport == 'kubectl':
        return KubectlGeneralLogger(node, target_log_file)
    if logs_transport == 'k8s_client':
        return K8sClientLogger(node, target_log_file)
    if logs_transport == 'ssh':
        if node.distro.uses_systemd:
            if 'db-node' in node.name and node.is_nonroot_install and not node.is_scylla_logging_to_journal:
                return SSHNonRootScyllaSystemdLogger(node, target_log_file)
            if 'db-node' in node.name:
                return SSHScyllaSystemdLogger(node, target_log_file)
            return SSHGeneralSystemdLogger(node, target_log_file)
        if 'db-node' in node.name:
            return SSHScyllaFileLogger(node, target_log_file)
        else:
            return SSHGeneralFileLogger(node, target_log_file)
    return None


class DockerComposeLogger(CommandClusterLoggerBase):

    @cached_property
    def _logger_cmd(self) -> str:
        return f"{self._cluster.compose_context} logs --no-color --tail=1000 >>{self._target_log_file}"
