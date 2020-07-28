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
import threading
from typing import Optional, Callable, Iterator

import kubernetes as k8s
from invoke import Runner, Context, Config
from invoke.exceptions import UnexpectedExit, Failure

from sdcm.utils.k8s import KubernetesOps
from sdcm.utils.decorators import retrying

from .base import CommandRunner, RetryableNetworkException


class KubernetesRunner(Runner):
    read_timeout = 0.1

    def __init__(self, context: Context) -> None:
        super().__init__(context)

        self.process = None
        self._k8s_core_v1_api = KubernetesOps.core_v1_api(context.config)
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
            self.process = k8s.stream.stream(self._k8s_core_v1_api.connect_get_namespaced_pod_exec,
                                             name=self.context.config.k8s_pod,
                                             container=self.context.config.k8s_container,
                                             namespace=self.context.config.k8s_namespace,
                                             command=[shell, "-c", command],
                                             stderr=True,
                                             stdin=True,
                                             stdout=True,
                                             tty=False,
                                             _preload_content=False)

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


class KubernetesCmdRunner(CommandRunner):

    # pylint: disable=too-many-arguments
    def __init__(self, pod: str, container: Optional[str] = None, namespace: str = "default",
                 k8s_server_url: Optional[str] = None,
                 k8s_configuration: Optional[k8s.client.Configuration] = None) -> None:
        self.pod = pod
        self.container = container
        self.namespace = namespace
        if k8s_configuration is None:
            self.k8s_server_url = k8s_server_url
            k8s_configuration = KubernetesOps.create_k8s_configuration(self)
        else:
            if k8s_server_url is not None:
                self.log.warning("`k8s_configuration' is not None, `k8s_server_url' parameter ignored")
            self.k8s_server_url = k8s_configuration.host
        self.k8s_configuration = k8s_configuration

        super().__init__(hostname=f"{pod}/{container}")

    def get_init_arguments(self) -> dict:
        return {
            "pod": self.pod,
            "container": self.container,
            "namespace": self.namespace,
            "k8s_configuration": self.k8s_configuration,
        }

    def is_up(self, timeout=None) -> bool:
        return True

    def _create_connection(self):
        return KubernetesRunner(Context(Config(overrides={"k8s_pod": self.pod,
                                                          "k8s_container": self.container,
                                                          "k8s_namespace": self.namespace,
                                                          "k8s_configuration": self.k8s_configuration, })))

    # pylint: disable=too-many-arguments
    def run(self, cmd, timeout=300, ignore_status=False, verbose=True, new_session=False,
            log_file=None, retry=1, watchers=None):
        watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        @retrying(n=retry)
        def _run():
            start_time = time.perf_counter()
            if verbose:
                self.log.debug('Running command "{}"...'.format(cmd))
            try:
                connection = self._create_connection() if new_session else self.connection
                res = connection.run(command=cmd, warn=ignore_status, hide=True, watchers=watchers, timeout=timeout)
                res.duration = time.perf_counter() - start_time
                res.exit_status = res.exited
                return res
            except (Failure, UnexpectedExit) as details:
                if hasattr(details, "result"):
                    self._print_command_results(details.result, verbose, ignore_status)
                raise

        result = _run()
        self._print_command_results(result, verbose, ignore_status)

        return result

    # pylint: disable=too-many-arguments,unused-argument
    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException, ))
    def receive_files(self, src, dst, delete_dst=False, preserve_perm=True, preserve_symlinks=False, timeout=300):
        KubernetesOps.copy_file(self, f"{self.namespace}/{self.pod}:{src}", dst,
                                container=self.container, timeout=timeout)
        return True

    # pylint: disable=too-many-arguments,unused-argument
    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException, ))
    def send_files(self, src, dst, delete_dst=False, preserve_symlinks=False, verbose=False, timeout=300):
        KubernetesOps.copy_file(self, src, f"{self.namespace}/{self.pod}:{dst}",
                                container=self.container, timeout=timeout)
        return True
