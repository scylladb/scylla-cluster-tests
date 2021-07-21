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

import inspect
import threading
from typing import Optional, Callable, Iterator, List

import kubernetes as k8s
from invoke import Runner, Context, Config
from invoke.exceptions import ThreadException
from urllib3.exceptions import MaxRetryError

from sdcm.utils.k8s import KubernetesOps
from sdcm.utils.common import deprecation
from sdcm.utils.decorators import retrying

from .base import RetryableNetworkException
from .remote_base import RemoteCmdRunnerBase, StreamWatcher


class KubernetesRunner(Runner):
    read_timeout = 0.1

    def __init__(self, context: Context) -> None:
        super().__init__(context)

        self.process = None
        self._k8s_core_v1_api = KubernetesOps.core_v1_api(context.k8s_kluster.get_api_client())
        self._ws_lock = threading.RLock()

    def should_use_pty(self, pty: bool, fallback: bool) -> bool:
        return False

    def read_proc_output(self, reader: Callable[[int], str]) -> Iterator[str]:
        while self.process.is_open():
            yield reader(self.read_chunk_size)

    def read_proc_stdout(self, num_bytes: int) -> str:
        with self._ws_lock:
            return self.process.read_stdout(self.read_timeout)

    def read_proc_stderr(self, num_bytes: int) -> str:
        with self._ws_lock:
            return self.process.read_stderr(self.read_timeout)

    def _write_proc_stdin(self, data: str) -> None:
        with self._ws_lock:
            self.process.write_stdin(data)

    def close_proc_stdin(self) -> None:
        pass

    def start(self, command: str, shell: str, env: dict) -> None:
        with self._ws_lock:
            try:
                self.process = k8s.stream.stream(
                    self._k8s_core_v1_api.connect_get_namespaced_pod_exec,
                    name=self.context.config.k8s_pod,
                    container=self.context.config.k8s_container,
                    namespace=self.context.config.k8s_namespace,
                    command=[shell, "-c", command],
                    stderr=True,
                    stdin=True,
                    stdout=True,
                    tty=False,
                    _preload_content=False)
            except k8s.client.rest.ApiException as exc:
                raise ConnectionError(str(exc)) from None

    def kill(self) -> None:
        self.stop()

    @property
    def process_is_finished(self) -> bool:
        return not self.process.is_open()

    def returncode(self) -> Optional[int]:
        try:
            return self.process.returncode
        except (TypeError, KeyError, ):
            return None

    def stop(self) -> None:
        with self._ws_lock:
            if self.process:
                self.process.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class KubernetesCmdRunner(RemoteCmdRunnerBase):
    exception_retryable = (ConnectionError, MaxRetryError, ThreadException)
    default_run_retry = 8

    def __init__(self, kluster, pod: str, container: Optional[str] = None, namespace: str = "default") -> None:
        self.kluster = kluster
        self.pod = pod
        self.container = container
        self.namespace = namespace

        super().__init__(hostname=f"{pod}/{container}")

    def get_init_arguments(self) -> dict:
        return {
            "kluster": self.kluster,
            "pod": self.pod,
            "container": self.container,
            "namespace": self.namespace,
        }

    @property
    def connection(self):
        raise RuntimeError('KubernetesCmdRunner does not hold any connection, use _create_connection instead')

    def is_up(self, timeout=None) -> bool:
        return True

    def _create_connection(self):
        return KubernetesRunner(Context(Config(overrides={"k8s_kluster": self.kluster,
                                                          "k8s_pod": self.pod,
                                                          "k8s_container": self.container,
                                                          "k8s_namespace": self.namespace, })))

    # pylint: disable=too-many-arguments
    def _run_execute(self, cmd: str, timeout: Optional[float] = None,  # pylint: disable=too-many-arguments
                     ignore_status: bool = False, verbose: bool = True, new_session: bool = False,
                     watchers: Optional[List[StreamWatcher]] = None):
        # TODO: This should be removed than sudo calls will be done in more organized way.
        tmp = cmd.split(maxsplit=3)
        if tmp[0] == 'sudo':
            deprecation("Using `sudo' in cmd string is deprecated.  Use `remoter.sudo()' instead.")
            frame = inspect.stack()[1]
            self.log.error("Cut off `sudo' from the cmd string: %s (%s:%s: %s)",
                           cmd, frame.filename, frame.lineno, frame.code_context[0].rstrip())
            if tmp[1] == '-u':
                cmd = tmp[3]
            else:
                cmd = cmd[cmd.find('sudo') + 5:]
        # Session should be created for each run
        return super()._run_execute(cmd, timeout=timeout, ignore_status=ignore_status, verbose=verbose,
                                    new_session=True, watchers=watchers)

    # pylint: disable=too-many-arguments,unused-argument
    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException, ))
    def receive_files(self, src, dst, delete_dst=False, preserve_perm=True, preserve_symlinks=False, timeout=300):
        KubernetesOps.copy_file(self.kluster, f"{self.namespace}/{self.pod}:{src}", dst,
                                container=self.container, timeout=timeout)
        return True

    # pylint: disable=too-many-arguments,unused-argument
    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException, ))
    def send_files(self, src, dst, delete_dst=False, preserve_symlinks=False, verbose=False):
        KubernetesOps.copy_file(self.kluster, src, f"{self.namespace}/{self.pod}:{dst}",
                                container=self.container, timeout=300)
        return True

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool) -> bool:
        self.log.error(exc)
        if isinstance(exc, self.exception_retryable):
            raise RetryableNetworkException(str(exc), original=exc)
        return True

    def _close_connection(self):
        # Websocket connection is getting closed when run is ended, so nothing is needed to be done here
        pass

    def _open_connection(self):
        # Websocket connection is getting opened for each run, so nothing is needed to be done here
        pass

    def stop(self):
        # Websocket connection is getting closed when run is ended, so nothing is needed to be done here
        pass

    def _reconnect(self):
        # Websocket connection is getting closed when run is ended, so nothing is needed to be done here
        pass
