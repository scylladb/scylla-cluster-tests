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

# pylint: disable=too-many-arguments

import os
import time
import queue
import logging
import threading
import multiprocessing
import contextlib
from typing import Optional
from functools import cached_property

import kubernetes as k8s
from urllib3.util.retry import Retry

from sdcm import sct_abs_path
from sdcm.remote import LOCALRUNNER
from sdcm.utils.decorators import timeout as timeout_decor
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container
from sdcm.wait import wait_for


KUBECTL_BIN = "kubectl"
HELM_IMAGE = "alpine/helm:3.3.4"

KUBECTL_TIMEOUT = 300  # seconds

K8S_CONFIGS_PATH_SCT = sct_abs_path("sdcm/k8s_configs")
K8S_CONFIGS_PATH_IN_CONTAINER = "/apps/k8s_configs"

JSON_PATCH_TYPE = "application/json-patch+json"

LOGGER = logging.getLogger(__name__)

logging.getLogger("kubernetes.client.rest").setLevel(logging.INFO)


class ApiLimiterClient(k8s.client.ApiClient):
    _api_rate_limiter: 'ApiCallRateLimiter' = None

    def call_api(self, *args, **kwargs):
        if self._api_rate_limiter:
            self._api_rate_limiter.wait()
        return super().call_api(*args, **kwargs)

    def bind_api_limiter(self, instance: 'ApiCallRateLimiter'):
        self._api_rate_limiter = instance


class ApiLimiterRetry(Retry):
    _api_rate_limiter: 'ApiCallRateLimiter' = None

    def sleep(self, *args, **kwargs):
        super().sleep(*args, **kwargs)
        if self._api_rate_limiter:
            self._api_rate_limiter.wait()

    def new(self, *args, **kwargs):
        result = super().new(*args, **kwargs)
        if self._api_rate_limiter:
            ApiLimiterRetry.bind_api_limiter(result, self._api_rate_limiter)
        return result

    def bind_api_limiter(self, instance: 'ApiCallRateLimiter'):
        self._api_rate_limiter = instance


class ApiCallRateLimiter(threading.Thread):
    """Simple and not very accurate rate limiter.

    Allow 1 call each `1 / rate_limit' seconds interval.
    If some call not able to start after `queue_size / rate_limit' seconds then raise `queue.Full' for caller.
    """

    def __init__(self, rate_limit: float, queue_size: int, urllib_retry: int):
        super().__init__(name=type(self).__name__, daemon=True)
        self._lock = multiprocessing.Semaphore(value=1)
        self._requests_pause_event = multiprocessing.Event()
        self.release_requests_pause()
        self.rate_limit = rate_limit  # ops/s
        self.queue_size = queue_size
        self.urllib_retry = urllib_retry
        self.running = threading.Event()

    def put_requests_on_pause(self):
        self._requests_pause_event.clear()

    def release_requests_pause(self):
        self._requests_pause_event.set()

    @property
    @contextlib.contextmanager
    def pause(self):
        self.put_requests_on_pause()
        yield None
        self.release_requests_pause()

    def wait(self):
        self._requests_pause_event.wait(15 * 60)
        if not self._lock.acquire(timeout=self.queue_size / self.rate_limit):  # deepcode ignore E1123: deepcode error
            LOGGER.error("k8s API call rate limiter queue size limit has been reached")
            raise queue.Full

    def _api_test(self, kluster):
        logging.getLogger('urllib3.connectionpool').disabled = True
        try:
            KubernetesOps.core_v1_api(
                KubernetesOps.api_client(KubernetesOps.create_k8s_configuration(kluster))
            ).list_pod_for_all_namespaces(watch=False)
        finally:
            logging.getLogger('urllib3.connectionpool').disabled = False

    def wait_till_api_become_not_operational(self, kluster, num_requests=10, max_waiting_time=360):
        wait_for(
            self.check_if_api_not_operational,
            timeout=max_waiting_time,
            kluster=kluster,
            num_requests=num_requests
        )

    def wait_till_api_become_stable(self, kluster, num_requests=20, max_waiting_time=1200):
        wait_for(
            self.check_if_api_stable,
            timeout=max_waiting_time,
            kluster=kluster,
            num_requests=num_requests,
            throw_exc=True
        )

    def check_if_api_stable(self, kluster, num_requests=20):
        for n in range(num_requests):
            self._api_test(kluster)
        return True

    def check_if_api_not_operational(self, kluster, num_requests=20):
        passed = 0
        for n in range(num_requests):
            try:
                self._api_test(kluster)
                passed += 1
            except Exception:  # pylint: disable=broad-except
                time.sleep(1 / self.rate_limit)
        return passed > num_requests / 0.8

    def stop(self):
        self.running.clear()
        self.join()

    def run(self) -> None:
        LOGGER.info("k8s API call rate limiter started: rate_limit=%s, queue_size=%s",
                    self.rate_limit, self.queue_size)
        self.running.set()
        while self.running.is_set():
            if self._lock.get_value() == 0:
                self._lock.release()
            time.sleep(1 / self.rate_limit)

    def get_k8s_configuration(self, kluster) -> k8s.client.Configuration:
        output = KubernetesOps.create_k8s_configuration(kluster)
        output.retries = ApiLimiterRetry(self.urllib_retry)
        output.retries.bind_api_limiter(self)
        return output

    def get_api_client(self, k8s_configuration: k8s.client.Configuration) -> ApiLimiterClient:
        output = ApiLimiterClient(k8s_configuration)
        output.bind_api_limiter(self)
        return output


class KubernetesOps:

    @staticmethod
    def create_k8s_configuration(kluster) -> k8s.client.Configuration:
        k8s_configuration = k8s.client.Configuration()
        if kluster.k8s_server_url:
            k8s_configuration.host = kluster.k8s_server_url
        else:
            k8s.config.load_kube_config(
                config_file=os.environ.get('KUBECONFIG', '~/.kube/config'),
                client_configuration=k8s_configuration)
        return k8s_configuration

    @classmethod
    def api_client(cls, k8s_configuration: k8s.client.Configuration) -> k8s.client.ApiClient:
        return k8s.client.ApiClient(k8s_configuration)

    @classmethod
    def dynamic_client(cls, api_client: k8s.client.ApiClient) -> k8s.dynamic.DynamicClient:
        return k8s.dynamic.DynamicClient(api_client)

    @classmethod
    def dynamic_api(cls, dynamic_client: k8s.dynamic.DynamicClient, api_version, kind):
        return dynamic_client.resources.get(api_version=api_version, kind=kind)

    @classmethod
    def apps_v1_api(cls, api_client: k8s.client.ApiClient) -> k8s.client.AppsV1Api:
        return k8s.client.AppsV1Api(api_client)

    @classmethod
    def core_v1_api(cls, api_client: k8s.client.ApiClient) -> k8s.client.CoreV1Api:
        return k8s.client.CoreV1Api(api_client)

    @classmethod
    @timeout_decor(timeout=600)
    def list_statefulsets(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return kluster.k8s_apps_v1_api.list_stateful_set_for_all_namespaces(watch=False, **kwargs).items
        return kluster.k8s_apps_v1_api.list_namespaced_stateful_set(namespace=namespace, watch=False, **kwargs).items

    @classmethod
    @timeout_decor(timeout=600)
    def list_pods(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return kluster.k8s_core_v1_api.list_pod_for_all_namespaces(watch=False, **kwargs).items
        return kluster.k8s_core_v1_api.list_namespaced_pod(namespace=namespace, watch=False, **kwargs).items

    @classmethod
    @timeout_decor(timeout=600)
    def list_services(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return kluster.k8s_core_v1_api.list_service_all_namespaces(watch=False, **kwargs).items
        return kluster.k8s_core_v1_api.list_namespaced_service(namespace=namespace, watch=False, **kwargs).items

    @staticmethod
    def kubectl_cmd(kluster, *command, namespace=None, ignore_k8s_server_url=False):
        cmd = [KUBECTL_BIN, ]
        if not ignore_k8s_server_url and kluster.k8s_server_url is not None:
            cmd.append(f"--server={kluster.k8s_server_url}")
        if namespace:
            cmd.append(f"--namespace={namespace}")
        cmd.extend(command)
        return " ".join(cmd)

    @classmethod
    def kubectl(cls, kluster, *command, namespace: Optional[str] = None, timeout: int = KUBECTL_TIMEOUT,
                remoter: Optional['KubernetesCmdRunner'] = None, ignore_status: bool = False, verbose: bool = True):
        cmd = cls.kubectl_cmd(kluster, *command, namespace=namespace, ignore_k8s_server_url=bool(remoter))
        if remoter is None:
            remoter = LOCALRUNNER
        return remoter.run(cmd, timeout=timeout, ignore_status=ignore_status, verbose=verbose)

    @classmethod
    def kubectl_multi_cmd(cls, kluster, *command, namespace: Optional[str] = None, timeout: int = KUBECTL_TIMEOUT,
                          remoter: Optional['KubernetesCmdRunner'] = None, ignore_status: bool = False,
                          verbose: bool = True):
        total_command = ' '.join(command)
        final_command = []
        for cmd in total_command.split(' '):
            if cmd == 'kubectl':
                final_command.append(
                    cls.kubectl_cmd(kluster, namespace=namespace, ignore_k8s_server_url=bool(remoter)))
            else:
                final_command.append(cmd)
        if remoter is None:
            remoter = LOCALRUNNER
        final_command = ' '.join(final_command)
        return remoter.run(final_command, timeout=timeout, ignore_status=ignore_status, verbose=verbose)

    @classmethod
    def apply_file(cls, kluster, config_path, namespace=None, timeout=KUBECTL_TIMEOUT, envsubst=True):
        if envsubst:
            config_path = f"<(envsubst<{config_path})"
        cls.kubectl(kluster, "apply", "-f",  config_path, namespace=namespace, timeout=timeout)

    @classmethod
    def copy_file(cls, kluster, src, dst, container=None, timeout=KUBECTL_TIMEOUT):
        command = ["cp", src, dst]
        if container:
            command.extend(("-c", container))
        cls.kubectl(kluster, *command, timeout=timeout)

    @classmethod
    def expose_pod_ports(cls, kluster, pod_name, ports, labels=None, selector=None, namespace=None, timeout=KUBECTL_TIMEOUT):
        command = ["expose pod", pod_name, "--type=LoadBalancer",
                   "--port", ",".join(map(str, ports)),
                   f"--name={pod_name}-loadbalancer", ]
        if labels:
            command.extend(("--labels", labels))
        if selector:
            command.extend(("--selector", selector))
        cls.kubectl(kluster, *command, namespace=namespace, timeout=timeout)

    @classmethod
    def unexpose_pod_ports(cls, kluster, pod_name, namespace=None, timeout=KUBECTL_TIMEOUT):
        cls.kubectl(kluster, f"delete service {pod_name}-loadbalancer", namespace=namespace, timeout=timeout)


class HelmContainerMixin:
    def helm_container_run_args(self) -> dict:
        kube_config_path = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))
        helm_config_path = os.path.expanduser(os.environ.get('HELM_CONFIG_HOME', '~/.helm'))
        volumes = {
            os.path.dirname(kube_config_path): {"bind": "/root/.kube", "mode": "rw"},
            helm_config_path: {"bind": "/root/.helm", "mode": "rw"},
            K8S_CONFIGS_PATH_SCT: {"bind": K8S_CONFIGS_PATH_IN_CONTAINER, "mode": "ro"},
            '/tmp': {"bind": "/tmp", "mode": "rw"},
        }
        return dict(image=HELM_IMAGE,
                    entrypoint="/bin/cat",
                    tty=True,
                    name=f"{self.name}-helm",
                    network_mode="host",
                    volumes=volumes)

    @cached_property
    def _helm_container(self) -> Container:
        container = ContainerManager.run_container(self, "helm")

        # NOTE: Install 'gettext' package that contains 'envsubst' binary
        # needed for installation of helm charts uisng "values.yaml" files.
        res = container.exec_run(["sh", "-c", "apk add gettext"])
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")

        return container

    def helm(self, kluster, *command: str, namespace: Optional[str] = None,
             prepend_command=None) -> str:
        cmd = ["helm", ]
        if prepend_command:
            if isinstance(prepend_command, list):
                cmd = prepend_command + cmd
            else:
                raise TypeError("'prepend_cmd' param expected to be 'list'")
        if kluster.k8s_server_url:
            cmd.extend(("--kube-apiserver", kluster.k8s_server_url, ))
        if namespace:
            cmd.extend(("--namespace", namespace, ))
        cmd.extend(command)
        cmd = " ".join(cmd)
        environment = [f"SCT_{k.upper()}={v}" for k, v in kluster.params.items()]

        LOGGER.debug("Execute `%s'", cmd)
        res = self._helm_container.exec_run(["sh", "-c", cmd], environment=environment)
        if res.exit_code:
            raise DockerException(f"{self._helm_container}: {res.output.decode('utf-8')}")
        return res.output.decode("utf-8")

    def helm_install(self, kluster,
                     target_chart_name: str,
                     source_chart_name: str,
                     version: str = "",
                     use_devel: bool = False,
                     set_options: str = "",
                     values_file_path: str = "",
                     namespace: Optional[str] = None) -> str:
        command = ["install", target_chart_name, source_chart_name]
        prepend_command = []
        if version:
            command.extend(("--version", version))
        if use_devel:
            command.extend(("--devel",))
        if set_options:
            command.extend(("--set", set_options))
        if values_file_path:
            # NOTE: It must look like following:
            # $ envsubst < {values_file_path} | helm install \
            #     scylla scylla-operator/scylla --version {v} --devel --values -
            command.extend(("--values", "-"))
            prepend_command = ["envsubst", "<", values_file_path, "|"]

        return self.helm(
            kluster,
            *command,
            prepend_command=prepend_command,
            namespace=namespace,
        )
