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


import abc
import json
import os
import time
import queue
import logging
import re
import threading
import multiprocessing
import contextlib
from tempfile import NamedTemporaryFile
from typing import Iterator, Optional, Union, Callable, List
from functools import cached_property, partialmethod
from pathlib import Path

import kubernetes as k8s
import yaml
from paramiko.config import invoke
from urllib3.util.retry import Retry
from urllib3.exceptions import (
    IncompleteRead,
    ProtocolError,
    ReadTimeoutError,
)

from sdcm.log import SDCMAdapter
from sdcm import sct_abs_path
from sdcm.remote import LOCALRUNNER
from sdcm.utils.common import walk_thru_data
from sdcm.utils.decorators import timeout as timeout_decor, retrying
from sdcm.utils.docker_utils import ContainerManager, Container
from sdcm.wait import wait_for


KUBECTL_BIN = "kubectl"
HELM_IMAGE = "alpine/helm:3.8.0"

KUBECTL_TIMEOUT = 300  # seconds

K8S_CONFIGS_PATH_SCT = sct_abs_path("sdcm/k8s_configs")

JSON_PATCH_TYPE = "application/json-patch+json"

LOGGER = logging.getLogger(__name__)
K8S_MEM_CPU_RE = re.compile('^([0-9]+)([a-zA-Z]*)$')
K8S_MEM_CONVERSION_MAP = {
    'e': lambda x: x * 1073741824,
    'p': lambda x: x * 1048576,
    't': lambda x: x * 1024,
    'g': lambda x: x,
    'm': lambda x: x / 1024,
    'k': lambda x: x / 1048576,
    '': lambda x: x,
}
K8S_CPU_CONVERSION_MAP = {
    'm': lambda x: x / 1000,
    '': lambda x: x,
}

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

    def __init__(self, rate_limit: float, queue_size: int, urllib_retry: int, urllib_backoff_factor: float):
        super().__init__(name=type(self).__name__, daemon=True)
        self._lock = multiprocessing.Semaphore(value=1)
        self._requests_pause_event = multiprocessing.Event()
        self.release_requests_pause()
        self.rate_limit = rate_limit  # ops/s
        self.queue_size = queue_size
        self.urllib_retry = urllib_retry
        self.urllib_backoff_factor = urllib_backoff_factor
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
            num_requests=num_requests,
            throw_exc=False
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
        for _ in range(num_requests):
            self._api_test(kluster)
        return True

    def check_if_api_not_operational(self, kluster, num_requests=20):
        passed = 0
        for _ in range(num_requests):
            try:
                self._api_test(kluster)
                passed += 1
            except Exception:  # noqa: BLE001
                time.sleep(1 / self.rate_limit)
        return passed < num_requests * 0.8

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
        output.retries = ApiLimiterRetry(self.urllib_retry, backoff_factor=self.urllib_backoff_factor)
        output.retries.bind_api_limiter(self)
        return output

    def get_api_client(self, k8s_configuration: k8s.client.Configuration) -> ApiLimiterClient:
        output = ApiLimiterClient(k8s_configuration)
        output.bind_api_limiter(self)
        return output


class CordonNodes:
    def __init__(self, kubectl_method, selector):
        self.kubectl = kubectl_method
        self.cordon_cmd = f"cordon -l '{selector}'"

    def __enter__(self):
        result = self.kubectl(self.cordon_cmd)
        # NOTE: sleep for some time to make kube scheduler fit the cordon nodes time range
        time.sleep(5)
        return result

    def __exit__(self, *exc):
        # NOTE: sleep for some time to make kube scheduler fit the cordon nodes time range
        time.sleep(5)
        self.kubectl(f"un{self.cordon_cmd}")


class KubernetesOps:

    @staticmethod
    def create_k8s_configuration(kluster) -> k8s.client.Configuration:
        k8s_configuration = k8s.client.Configuration()
        if kluster.k8s_server_url:
            k8s_configuration.host = kluster.k8s_server_url
        else:
            k8s.config.load_kube_config(
                config_file=kluster.kube_config_path, client_configuration=k8s_configuration)
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
    def get_node(cls, kluster, name, **kwargs):
        return kluster.k8s_core_v1_api.read_node(name, **kwargs)

    @classmethod
    @timeout_decor(timeout=600)
    def list_services(cls, kluster, namespace=None, **kwargs):
        if namespace is None:
            return kluster.k8s_core_v1_api.list_service_for_all_namespaces(watch=False, **kwargs).items
        return kluster.k8s_core_v1_api.list_namespaced_service(namespace=namespace, watch=False, **kwargs).items

    @staticmethod
    def kubectl_cmd(kluster, *command, namespace=None, ignore_k8s_server_url=False):
        cmd = [KUBECTL_BIN]
        if getattr(kluster, "kube_config_path", None) is not None:
            cmd.append(f"--kubeconfig={kluster.kube_config_path}")
        if sct_test_logdir := os.environ.get('_SCT_TEST_LOGDIR'):
            cmd.append(
                f"--cache-dir={Path(sct_test_logdir) / '.kube/http-cache'}--{kluster.short_cluster_name}")
        if not ignore_k8s_server_url and getattr(kluster, "k8s_server_url", None) is not None:
            cmd.append(f"--server={kluster.k8s_server_url}")
        if namespace:
            cmd.append(f"--namespace={namespace}")
        cmd.extend(command)
        return " ".join(cmd)

    @classmethod
    def kubectl(cls, kluster, *command, namespace: Optional[str] = None, timeout: int = KUBECTL_TIMEOUT,
                remoter: Optional['KubernetesCmdRunner'] = None,  # noqa: F821
                ignore_status: bool = False, verbose: bool = True):
        cmd = cls.kubectl_cmd(kluster, *command, namespace=namespace, ignore_k8s_server_url=bool(remoter))
        if remoter is None:
            remoter = LOCALRUNNER
        return remoter.run(cmd, timeout=timeout, ignore_status=ignore_status, verbose=verbose)

    @classmethod
    def kubectl_multi_cmd(cls, kluster, *command, namespace: Optional[str] = None, timeout: int = KUBECTL_TIMEOUT,
                          remoter: Optional['KubernetesCmdRunner'] = None, ignore_status: bool = False,  # noqa: F821
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
    def apply_file(cls, kluster, config_path, namespace=None,
                   timeout=KUBECTL_TIMEOUT, environ=None, envsubst=True,
                   modifiers: List[Callable] = None, server_side=False):
        if environ:
            environ_str = (' '.join([f'{name}="{value}"' for name, value in environ.items()])) + ' '
        else:
            environ_str = ''
        server_side_arg = "--server-side" if server_side else ""

        config_paths = []
        if os.path.isdir(config_path):
            for root, _, subfiles in os.walk(config_path):
                for subfile in subfiles:
                    if not subfile.endswith('yaml'):
                        continue
                    config_paths.append(os.path.join(root, subfile))
        else:
            config_paths.append(config_path)

        for current_config_path in sorted(config_paths):
            LOGGER.debug("Processing '%s' file.", current_config_path,
                         extra={'prefix': kluster.region_name})
            with NamedTemporaryFile(mode='tw') as temp_file:
                resulted_content = []
                if envsubst:
                    data = LOCALRUNNER.run(
                        f'{environ_str}envsubst<{current_config_path}', verbose=False).stdout
                else:
                    with open(current_config_path, encoding="utf-8") as config_file_stream:
                        data = config_file_stream.read()
                file_content = yaml.safe_load_all(data)

                for doc in file_content:
                    if modifiers:
                        for modifier in modifiers:
                            modifier(doc)
                    resulted_content.append(doc)
                temp_file.write(yaml.safe_dump_all(resulted_content))
                temp_file.flush()

                @retrying(n=0, sleep_time=5, timeout=timeout, allowed_exceptions=RuntimeError)
                def run_kubectl(file_name):
                    try:
                        cls.kubectl(kluster, "apply", server_side_arg, "-f", file_name,
                                    namespace=namespace, timeout=timeout)
                    except invoke.exceptions.UnexpectedExit as exc:
                        if 'did you specify the right host or port' in exc.result.stderr:
                            raise RuntimeError(str(exc)) from None
                        raise

                run_kubectl(temp_file.name)

    @classmethod
    def copy_file(cls, kluster, src, dst, container=None, timeout=KUBECTL_TIMEOUT, sudo=False):
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

    @staticmethod
    def get_kubectl_auth_config(kluster, config):
        for user in config["users"]:
            if kluster.short_cluster_name not in user["name"]:
                continue
            for auth_type in ['exec', 'auth-provider']:
                if auth_type in user["user"]:
                    return auth_type, user["user"][auth_type]
        return None, None

    @classmethod
    def patch_kubectl_auth_config(cls, config, auth_type, cmd: str, args: list):
        if auth_type == 'exec':
            config['command'] = cmd
            config['args'] = args
        elif auth_type == 'auth-provider':
            config['config']['cmd-args'] = ' '.join(args)
            config['config']['cmd-path'] = cmd
        else:
            raise ValueError(f'Unknown auth-type {auth_type}')

    @staticmethod
    def wait_for_pods_with_condition(kluster, condition: str, total_pods: Union[int, Callable],
                                     timeout: float, namespace: str,
                                     selector: str = '',
                                     sleep_between_retries: int = 10):
        assert isinstance(total_pods, (int, float)) or callable(total_pods), (
            "total_pods should be number or callable")
        waiter_message = (
            f"{kluster.region_name}: Wait for the '{total_pods}' pod(s) "
            f"from the '{namespace}' namespace with '{condition}' condition to be true...")

        @timeout_decor(message=waiter_message, timeout=timeout * 60, sleep_time=sleep_between_retries)
        def wait_for_condition(kluster, condition, total_pods, timeout, namespace, selector):
            # To make it more informative in worst case scenario made it repeat 5 times, by readiness_timeout // 5
            if selector:
                selector = selector if selector.startswith("--selector=") else f'--selector={selector}'
            result = kluster.kubectl(
                f"wait {selector} --timeout={timeout // 5}m --all --for={condition} pod",
                namespace=namespace,
                timeout=timeout * 60 // 5 + 10)
            count = result.stdout.count('condition met')
            if isinstance(total_pods, (int, float)):
                if total_pods != count:
                    raise RuntimeError('Not all pods reported')
            elif callable(total_pods):
                if not total_pods(count):
                    raise RuntimeError('Not all pods reported')
            else:
                raise ValueError(f"total_pods should be number or callable and not {type(total_pods)}")

        wait_for_condition(kluster, condition, total_pods, timeout, namespace, selector)

    @staticmethod
    def wait_for_pods_readiness(kluster, total_pods: Union[int, Callable], readiness_timeout: float,
                                namespace: str, selector: str = '',
                                sleep_between_retries: int = 10):
        KubernetesOps.wait_for_pods_with_condition(kluster, condition='condition=Ready',
                                                   total_pods=total_pods,
                                                   timeout=readiness_timeout,
                                                   namespace=namespace,
                                                   selector=selector,
                                                   sleep_between_retries=sleep_between_retries)

    @staticmethod
    def wait_for_pods_running(kluster, total_pods: Union[int, Callable], timeout: float,
                              namespace: str, selector: str = '',
                              sleep_between_retries: int = 10):
        KubernetesOps.wait_for_pods_with_condition(kluster, condition="jsonpath='{.status.phase}'=Running",
                                                   total_pods=total_pods,
                                                   timeout=timeout,
                                                   namespace=namespace,
                                                   selector=selector,
                                                   sleep_between_retries=sleep_between_retries)

    @staticmethod
    def wait_for_pod_readiness(kluster, pod_name: str, namespace: str,
                               pod_readiness_timeout_minutes: int):
        timeout = pod_readiness_timeout_minutes or 5

        def _wait_for_pod_readiness():
            result = kluster.kubectl(
                f"wait --timeout={timeout // 3}m --for=condition=Ready pod {pod_name}",
                namespace=namespace,
                timeout=timeout // 3 * 60 + 10)
            if result.stdout.count('condition met') != 1:
                raise RuntimeError(f"'{pod_name}' pod is not ready")
            return True

        wait_for(_wait_for_pod_readiness,
                 text=f"Wait for {pod_name} pod to be ready...",
                 timeout=timeout * 60,
                 throw_exc=True)

    @staticmethod
    def patch_kube_config(kluster, static_token_path, kube_config_path: str = None) -> None:
        # It assumes that config is already created by gcloud
        # It patches kube config so that instead of running gcloud each time
        # we will get it's output from the cache file located at gcloud_token_path
        # To keep this cache file updated we run GcloudTokenUpdateThread thread
        if kube_config_path is None:
            kube_config_path = kluster.kube_config_path
        LOGGER.debug(
            "Patch %s to use file token %s", kube_config_path, static_token_path,
            extra={'prefix': kluster.region_name})

        with open(kube_config_path, encoding="utf-8") as kube_config:
            data = yaml.safe_load(kube_config)
        auth_type, user_config = KubernetesOps.get_kubectl_auth_config(kluster, data)

        if user_config is None:
            raise RuntimeError("Unable to find user configuration at the '%s'" % kube_config_path)
        KubernetesOps.patch_kubectl_auth_config(user_config, auth_type, "cat", [static_token_path])

        with open(kube_config_path, "w", encoding="utf-8") as kube_config:
            yaml.safe_dump(data, kube_config)

        LOGGER.debug('Patched kubectl config at %s with static kubectl token from %s',
                     kube_config_path, static_token_path, extra={'prefix': kluster.region_name})

    @classmethod
    def watch_events(cls, k8s_core_v1_api: k8s.client.CoreV1Api, name: str = None, namespace: str = None,
                     timeout: int = None):
        field_selector = f'involvedObject.name={name}' if name is not None else None
        if namespace is None:
            return k8s.watch.Watch().stream(k8s_core_v1_api.list_event_for_all_namespaces,
                                            field_selector=field_selector, timeout_seconds=timeout)
        return k8s.watch.Watch().stream(k8s_core_v1_api.list_namespaced_event, namespace=namespace,
                                        field_selector=field_selector, timeout_seconds=timeout)

    @classmethod
    def gather_k8s_logs_by_operator(cls, kluster, logdir_path=None):
        # Docs: https://github.com/scylladb/scylla-operator/blob/master/docs/source/support/must-gather.md
        logdir_path = logdir_path or kluster.logdir
        kubeconfig = kluster.kube_config_path
        operator_bin_path = f"/tmp/scylla-operator-{kluster.shortid}"
        operator_image = kluster.get_operator_image()
        LOCALRUNNER.run(
            f"timeout -v 15m docker cp $(docker create --name mustgather {operator_image}):"
            f"/usr/bin/scylla-operator {operator_bin_path} && docker rm mustgather")
        LOCALRUNNER.run(f"chmod +x {operator_bin_path}")
        gather_logs_cmd = (
            f'{operator_bin_path} must-gather --all-resources --loglevel=2'
            f' --dest-dir="{logdir_path}/must-gather" --kubeconfig={kubeconfig}')
        LOGGER.info(
            "K8S-LOGS: START running 'must-gather' scylla-operator command",
            extra={'prefix': kluster.region_name})
        try:
            # NOTE: We should create the 'must-gather' dir ourselves to avoid following error:
            #   Error: destination directory "%logdir_path%/must-gather" doesn't exist
            # Also we should keep it empty to avoid following error:
            #   Error: destination directory "%logdir_path%/must-gather" is not empty
            LOCALRUNNER.run(f"mkdir -p {logdir_path}/must-gather && rm -rf {logdir_path}/must-gather/*")
            LOCALRUNNER.run(gather_logs_cmd)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Failed to run scylla-operator's 'must gather' command: %s", exc,
                extra={'prefix': kluster.region_name})
        else:
            LOGGER.info(
                "K8S-LOGS: END running 'must-gather' scylla-operator command",
                extra={'prefix': kluster.region_name})
        try:
            LOCALRUNNER.run(f"rm {operator_bin_path}")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Failed to delete the the scylla-operator binary located at '%s': %s",
                operator_bin_path, exc, extra={'prefix': kluster.region_name})

    @classmethod
    def gather_k8s_logs(cls, logdir_path, kubectl=None, namespaces=None) -> None:
        # NOTE: reuse data where possible to minimize spent time due to API limiter restrictions
        LOGGER.info("K8S-LOGS: starting logs gathering")
        logdir = Path(logdir_path)
        kubectl = kubectl or (lambda *args, **kwargs: KubernetesOps.kubectl(None, *args, **kwargs))
        kubectl(f"version > {logdir / 'kubectl.version'} 2>&1", ignore_status=True)

        # Gather cluster-scoped resources info
        LOGGER.info("K8S-LOGS: gathering cluster scoped resources")
        cluster_scope_dir = "cluster-scoped-resources"
        os.makedirs(logdir / cluster_scope_dir, exist_ok=True)
        if namespaces:
            # NOTE: gather only small set of the cluster-wide objects which are needed for specific namespaces
            cluster_wide_resource_types = ("namespaces", "nodes", "persistentvolumes")
        else:
            cluster_wide_resource_types = kubectl(
                "api-resources --namespaced=false --verbs=list -o name").stdout.split()
        for resource_type in cluster_wide_resource_types:
            for output_format in ("yaml", "wide"):
                logfile = logdir / cluster_scope_dir / f"{resource_type}.{output_format}"
                kubectl(f"get {resource_type} -o {output_format} > {logfile}", ignore_status=True)
        kubectl(f"describe nodes > {logdir / cluster_scope_dir / 'nodes.desc'}",
                timeout=600, ignore_status=True)

        if not namespaces:
            # Read all the namespaces from already saved file
            with open(logdir / cluster_scope_dir / "namespaces.wide",
                      mode="r", encoding="utf-8") as namespaces_file:
                # Reverse order of namespaces because preferred ones are there
                namespaces = [n.split()[0] for n in namespaces_file.readlines()[1:]][::-1]
        elif isinstance(namespaces, str):
            namespaces = [namespaces]

        # Gather namespace-scoped resources info
        LOGGER.info("K8S-LOGS: gathering namespace scoped resources. list of namespaces: %s",
                    ', '.join(namespaces))
        namespace_scope_dir = "namespace-scoped-resources"
        os.makedirs(logdir / namespace_scope_dir, exist_ok=True)
        for resource_type in kubectl(
                "api-resources --namespaced=true --verbs=get,list -o name").stdout.split():
            LOGGER.info("K8S-LOGS: gathering '%s' resources", resource_type)
            logfile = logdir / namespace_scope_dir / f"{resource_type}.{output_format}"
            resources_wide = kubectl(
                f"get {resource_type} -A -o wide 2>&1 | tee {logfile}", ignore_status=True).stdout
            if resource_type.startswith("events"):
                # NOTE: skip both kinds on 'events' available in k8s
                continue
            for namespace in namespaces:
                if not re.search(f"\n{namespace} ", resources_wide):
                    # NOTE: move to the next namespace because such resources are absent here
                    continue
                LOGGER.info(
                    "K8S-LOGS: gathering '%s' resources in the '%s' namespace",
                    resource_type, namespace)
                resource_dir = logdir / namespace_scope_dir / namespace / resource_type
                os.makedirs(resource_dir, exist_ok=True)
                for _res in resources_wide.split("\n"):
                    if not re.match(f"{namespace} ", _res):
                        continue
                    res = _res.split()[1]
                    logfile = resource_dir / f"{res}.yaml"
                    res_stdout = kubectl(
                        f"get {resource_type}/{res} -o yaml 2>&1 | tee {logfile}",
                        namespace=namespace).stdout
                    if resource_type != "pods":
                        continue
                    try:
                        container_names = [
                            c["name"] for c in yaml.safe_load(res_stdout)["spec"]["containers"]]
                    except KeyError:
                        # NOTE: pod could be in 'deleting' state during 'list' command
                        # and be absent during 'get' command. So, just skip it.
                        continue
                    os.makedirs(resource_dir / res, exist_ok=True)
                    for container_name in container_names:
                        logfile = resource_dir / res / f"{container_name}"
                        # NOTE: ignore status because it may fail when pod is not ready/running
                        kubectl(f"logs pod/{res} -c={container_name} > {logfile}.log",
                                namespace=namespace, ignore_status=True)
                        kubectl(f"logs pod/{res} -c={container_name} --previous=true > "
                                f"{logfile}-previous.log",
                                namespace=namespace, ignore_status=True)

                        # NOTE: pick up Scylla container-specific files
                        if container_name != 'scylla':
                            continue
                        scylla_container_files_to_copy = (
                            ('/var/lib/scylla/io_properties.yaml', logfile / 'io_properties.yaml'),
                            ('/etc/scylla.d/', logfile / 'etc-scylla-d'),
                            ('/etc/scylla/', logfile / 'etc-scylla'),
                        )
                        for src_path, dst_path in scylla_container_files_to_copy:
                            kubectl(f"cp {res}:{src_path} {dst_path} -c {container_name}",
                                    namespace=namespace, ignore_status=True)


class HelmException(Exception):
    ...


class HelmContainerMixin:
    def helm_container_run_args(self) -> dict:
        user_home = os.path.expanduser("~")
        volumes = {
            user_home: {"bind": user_home, "mode": "rw"},
            sct_abs_path(""): {"bind": sct_abs_path(""), "mode": "ro"},
            '/tmp': {"bind": "/tmp", "mode": "rw"},
        }
        return dict(image=HELM_IMAGE,
                    entrypoint="/bin/cat",
                    tty=True,
                    name=f"{self.name}-helm",
                    network_mode="host",
                    volumes=volumes,
                    environment={},
                    )

    @cached_property
    def _helm_container(self) -> Container:
        return ContainerManager.run_container(self, "helm")

    def helm(self, kluster, *command: str, namespace: Optional[str] = None, values: 'HelmValues' = None, prepend_command=None) -> str:
        cmd = [
            f"HELM_CONFIG_HOME={kluster.helm_dir_path}",
            "helm",
            f"--kubeconfig={kluster.kube_config_path}"
        ]
        if prepend_command:
            if isinstance(prepend_command, list):
                cmd = prepend_command + cmd
            else:
                raise TypeError("'prepend_cmd' param expected to be 'list'")
        if kluster.k8s_server_url:
            cmd.extend(("--kube-apiserver", kluster.k8s_server_url, ))
        if namespace:
            cmd.extend(("--namespace", namespace, ))
        values_file = None
        cmd.extend(command)

        if values:
            helm_values_file = NamedTemporaryFile(mode='tw')
            helm_values_file.write(yaml.safe_dump(values.as_dict()))
            helm_values_file.flush()
            cmd.extend(("-f", helm_values_file.name))
            values_file = helm_values_file

        cmd = " ".join(cmd)

        LOGGER.debug("Execute `%s'", cmd, extra={'prefix': kluster.region_name})
        try:
            result = LOCALRUNNER.run(cmd)
            output = result.stdout.strip()
            if result.exited:
                raise HelmException(f"helm: {output}")
            return output
        finally:
            if values_file:
                values_file.close()

    def _helm_install_or_upgrade(self,
                                 operation_type: str,
                                 kluster,
                                 target_chart_name: str,
                                 source_chart_name: str,
                                 version: str = "",
                                 use_devel: bool = False,
                                 debug: bool = True,
                                 values: 'HelmValues' = None,
                                 namespace: Optional[str] = None,
                                 atomic: bool = False,
                                 timeout: Optional[str] = None) -> str:
        command = [operation_type, target_chart_name, source_chart_name]
        prepend_command = []
        if version:
            command.extend(("--version", version))
        if use_devel:
            command.extend(("--devel",))
        if debug:
            command.extend(("--debug",))
        if atomic:
            # if set, the installation process deletes the installation on failure
            # or upgrade process rolls back changes made in case of failed upgrade
            # waits for operation completion - so should specify timeout.
            command.extend(("--atomic",))
        if timeout is not None:
            # timeout in Go duration format
            command.extend(("--timeout", timeout))
        return self.helm(
            kluster,
            *command,
            prepend_command=prepend_command,
            namespace=namespace,
            values=values
        )

    helm_install = partialmethod(_helm_install_or_upgrade, "install")
    helm_upgrade = partialmethod(_helm_install_or_upgrade, "upgrade")


class TokenUpdateThread(threading.Thread, metaclass=abc.ABCMeta):
    update_period = 1800

    def __init__(self, kubectl_token_path: str):
        self._kubectl_token_path = kubectl_token_path
        self._termination_event = threading.Event()
        super().__init__(daemon=True, name=self.__class__.name)

    def run(self):
        wait_time = 0.01
        while not self._termination_event.wait(wait_time):
            try:
                self._get_token_and_save_to_temporary_location()
                self._check_token_validity_in_temporary_location()
                self._replace_active_token_by_token_from_temporary_location()
                LOGGER.debug('Cloud token has been updated and stored at %s', self._kubectl_token_path)
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug('Failed to update cloud token: %s', exc)
                wait_time = 5
            else:
                wait_time = self.update_period
            finally:
                self._clean_up_token_in_temporary_location()

    @cached_property
    def _temporary_token_path(self):
        return self._kubectl_token_path + ".tmp"

    def _clean_up_token_in_temporary_location(self):
        try:
            if os.path.exists(self._temporary_token_path):
                os.unlink(self._temporary_token_path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug('Failed to cleanup temporary token: %s', exc)

    def _check_token_validity_in_temporary_location(self):
        with open(self._temporary_token_path, encoding="utf-8") as gcloud_config_file:
            json.load(gcloud_config_file)

    def _get_token_and_save_to_temporary_location(self):
        token = self.get_token()
        with open(self._temporary_token_path, 'w+', encoding="utf-8") as gcloud_config_file:
            gcloud_config_file.write(token)
            gcloud_config_file.flush()

    def _replace_active_token_by_token_from_temporary_location(self):
        os.rename(self._temporary_token_path, self._kubectl_token_path)

    @abc.abstractmethod
    def get_token(self) -> str:
        pass

    def stop(self, timeout=None):
        self._termination_event.set()
        self.join(timeout)


class ScyllaPodsIPChangeTrackerThread(threading.Thread):
    """It tracks changes of Scylla pods IP addresses in whole K8S cluster and updates mapping.

    When a new IP is detected ...
    - the mapping gets updated
    - 'info' log message gets created
    - 'info' event gets created.
    - the monitoring node related to a Scylla pod gets reconfigured.

    'mapper_dict' has following structure:
    {
        'scylla': {
            'sct-cluster-dc-1-kind-0': {
                'current_ip': '10.0.0.4',
                'old_ips': ['10.0.0.2', '10.0.0.3'],
            },
            ...
        },
        'scylla-manager': {
            'scylla-manager-manager-dc-manager-rack-0': {
                'current_ip': '10.0.2.2',
                'old_ips': [],
            },
            ...
        },
        ...
    }
    """

    SCYLLA_PODS_SELECTOR = "app.kubernetes.io/name=scylla"
    SCYLLA_PODS_EXPECTED_LABEL_KEYS = ("scylla/datacenter", "scylla/rack")
    # NOTE: Knowing that in common case a Scylla pod gets up in 3-4 minutes,
    #       make the timeout be about it.
    READ_REQUEST_TIMEOUT = 180

    def __init__(self, k8s_kluster, mapper_dict):
        self._termination_event = threading.Event()
        super().__init__(daemon=True, name=self.__class__.name)
        self.k8s_kluster = k8s_kluster
        self.mapper_dict = mapper_dict
        self.watcher = None
        self._k8s_core_v1_api = KubernetesOps.core_v1_api(self.k8s_kluster.get_api_client())
        self.log = SDCMAdapter(LOGGER, extra={'prefix': k8s_kluster.region_name})

    @retrying(n=3600, sleep_time=1, allowed_exceptions=(ConnectionError, ))
    def _open_stream(self, cache={}) -> None:  # noqa: B006
        try:
            now = time.time()
            if cache.get("last_call_at", 0) + 5 > now:
                # NOTE: sleep for some time if it is called too often
                time.sleep(5)
                now = time.time()
            cache["last_call_at"] = now

            self.watcher = self._k8s_core_v1_api.list_endpoints_for_all_namespaces(
                label_selector=self.SCYLLA_PODS_SELECTOR,
                watch=True,
                async_req=False,
                _request_timeout=self.READ_REQUEST_TIMEOUT,
                _preload_content=False)
        except k8s.client.rest.ApiException as exc:
            self.log.warning("'_open_stream()': failed to open stream:\n%s", exc)
            # NOTE: following is workaround for the error 401 which may happen due to
            #       some config data corruption during the forced socket connection failure
            self._k8s_core_v1_api = KubernetesOps.core_v1_api(self.k8s_kluster.get_api_client())
            raise ConnectionError(str(exc)) from None

    @retrying(n=3600, sleep_time=1,  allowed_exceptions=(
        ProtocolError, IncompleteRead, ReadTimeoutError, TimeoutError, ValueError))
    def _read_stream(self) -> Iterator[str]:
        while not self._termination_event.wait(0.01):
            if not self.watcher or self.watcher.closed:
                self._open_stream()
            line = self.watcher.readline()
            yield line.decode("utf-8") if isinstance(line, bytes) else line

    # NOTE: we want it to run and retry always.
    #       If it crashes then whole test run will become a mess.
    @retrying(n=987654, sleep_time=10, allowed_exceptions=(Exception, ))
    def run(self) -> None:
        while not self._termination_event.wait(0.01):
            try:
                for line in self._read_stream():
                    self._process_line(line)
            except Exception:  # noqa: BLE001
                if not self._termination_event.wait(0.01):
                    raise
                self.log.info("Scylla pods IP change tracker thread has been stopped")

    def stop(self, timeout=None) -> None:
        self.log.warning("Stopping Scylla pods IP change tracker thread")
        self._termination_event.set()
        if hasattr(self.watcher, 'close') and not self.watcher.closed:
            self.watcher.close()
        self.join(timeout)

    def _process_line(self, line: str) -> None:
        # NOTE: line is expected to have following structure:
        # {"type": "ADDED",
        #  "object": {
        #      "kind": "Endpoints",
        #      "apiVersion": "v1",
        #      "metadata": {
        #          "name":"sct-cluster-dc-1-kind-0",
        #          "namespace": "scylla",
        #          ...
        #      },
        #      "subsets": [{
        #          "addresses": [
        #              {"ip": "10.244.218.195", ... },
        #              ...
        #          ],
        #          "ports": [...]
        #      }]
        #  }
        # }
        data = {}
        try:
            self.log.debug("Processing following line: %s", line)
            data = yaml.safe_load(line) or {}
            metadata = data.get('object', {}).get('metadata', {})
            namespace = metadata.get('namespace')
            if not namespace:
                raise KeyError("Cannot find 'namespace' in the line: %s" % line)
            if namespace not in self.mapper_dict:
                self.mapper_dict[namespace] = {}
            # NOTE: following condition allows to skip processing of all the
            #       load-balancer/headless endpoints that do not refer to any Scylla pod.
            labels = metadata.get('labels', {})
            if not all((key in labels.keys() for key in self.SCYLLA_PODS_EXPECTED_LABEL_KEYS)):
                return None

            if name := metadata.get('name'):
                if name not in self.mapper_dict[namespace]:
                    self.mapper_dict[namespace][name] = {}
                if 'current_ip' not in self.mapper_dict[namespace][name]:
                    self.mapper_dict[namespace][name]['current_ip'] = None
                if 'old_ips' not in self.mapper_dict[namespace][name]:
                    self.mapper_dict[namespace][name]['old_ips'] = []
            for subset in data.get('object', {}).get('subsets', []):
                for address in subset.get('addresses', []):
                    current_ip_address = address['ip']
                    if current_ip_address == self.mapper_dict[namespace][name]['current_ip']:
                        continue
                    old_ip_candidate = self.mapper_dict[namespace][name]['current_ip']
                    self.mapper_dict[namespace][name]['current_ip'] = current_ip_address
                    if not old_ip_candidate:
                        break
                    self.mapper_dict[namespace][name]['old_ips'].append(old_ip_candidate)
                    self.log.info(
                        "'%s/%s' node has changed it's pod IP address from '%s' to '%s'. "
                        "All old IPs: %s",
                        namespace, name, old_ip_candidate, current_ip_address,
                        ', '.join(self.mapper_dict[namespace][name]['old_ips']))
                    self._call_callbacks(namespace, name)
                    break
                else:
                    break
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "Failed to parse following line: %s\nerr: %s", line, exc)

    def _call_callbacks(self, namespace: str, pod_name: str) -> None:
        # TODO: run callbacks in parallel on per-namespace basis and serially inside a
        #       single namespace.

        def process_callback(callback, namespace, pod_name=None, add_pod_name_as_kwarg=False):
            suffix = f"(namespace={namespace}, pod_name={pod_name})"
            func = callback[0]
            args = callback[1]
            kwargs = {} | callback[2]
            if add_pod_name_as_kwarg:
                kwargs['pod_name'] = pod_name
            self.log.debug("Calling '%s' callback %s", func.__name__, suffix)
            try:
                func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                self.log.warning("Callback call failed %s: %s", suffix, str(exc))

        data = self.mapper_dict.get(namespace, {})
        for dict_key in (pod_name, '__each__'):
            callbacks = data.get(dict_key, {}).get('callbacks', [])
            if dict_key == '__each__':
                # NOTE: when 'add_pod_name_as_kwarg' is True it means
                #       we run a pod-specific function
                #       and it also means we should run it before namespace-specific ones.
                callbacks = sorted(callbacks, key=lambda c: c[-1], reverse=True)
            for callback in callbacks:
                process_callback(
                    callback=callback[:-1], namespace=namespace, pod_name=pod_name,
                    add_pod_name_as_kwarg=callback[-1])

    def register_callbacks(self, callbacks: Union[Callable, list[Callable]],
                           namespace: str, pod_name: str = '__each__',
                           add_pod_name_as_kwarg: bool = False) -> None:
        """Register callbacks to be called after a Scylla pod IP change.

        Callbacks may be of the following types:
        - called after a specific Scylla pod IP change
        - called after any Scylla pod IP change in a specific namespace
        - called after any Scylla pod IP change in a specific namespace
          getting 'pod_name=%pod_name%' kwarg.
          Useful for the objects which will be created later in the future.

        Examples of usages:

        # Per-pod
        - register_callbacks(func1,
                             namespace=namespace, pod_name='pod-name-1')
        - register_callbacks([[func2, args, kwargs]],
                             namespace=namespace, pod_name='pod-name-2')
        - register_callbacks([func3, [func3, args2, kwargs2]],
                             namespace=namespace, pod_name='pod-name-3')

        # Per-pod but with automatic addition of a pod name as a kwarg where the key is 'pod_name'
        - register_callbacks(func,
                             namespace=namespace,
                             add_pod_name_as_kwarg=True)

        # Per-namespace
        - register_callbacks([[func, args, kwargs], [func2, args, kwargs]],
                             namespace=namespace)

        Per-pod callbacks get run first, then per-namespace ones.
        """
        if not callbacks:
            self.log.warning(
                "No callbacks are provided. Nothing to register. "
                "namespace='%s', pod_name='%s'",
                namespace, pod_name)
            return
        elif callable(callbacks):
            callbacks = [callbacks]

        if namespace not in self.mapper_dict:
            self.mapper_dict[namespace] = {}
        if not pod_name:
            pod_name = '__each__'
        if pod_name not in self.mapper_dict[namespace]:
            self.mapper_dict[namespace][pod_name] = {'callbacks': []}

        for callback in callbacks:
            if callable(callback):
                callback = [callback, [], {}]  # noqa: PLW2901
            if (isinstance(callback, (tuple, list))
                    and len(callback) == 3
                    and callable(callback[0])
                    and isinstance(callback[1], (tuple, list))
                    and isinstance(callback[2], dict)):
                self.mapper_dict[namespace][pod_name]['callbacks'].append(
                    (callback[0], callback[1], callback[2], add_pod_name_as_kwarg))
            else:
                self.log.warning(
                    "Unexpected type (%s) of the callback: %s. Skipping", type(callback), callback)


def convert_cpu_units_to_k8s_value(cpu: Union[float, int]) -> str:
    if isinstance(cpu, float):
        if not cpu.is_integer():
            return f'{int(cpu * 1000)}m'
    return f'{int(cpu)}'


def convert_memory_units_to_k8s_value(memory: Union[float, int]) -> str:
    if isinstance(memory, float):
        if not memory.is_integer():
            return f'{int(memory * 1024)}Mi'
    return f'{int(memory)}Gi'


def convert_memory_value_from_k8s_to_units(memory: str) -> float:
    match = K8S_MEM_CPU_RE.match(memory).groups()
    if len(match) == 1:
        value = int(match[0])
        units = 'gb'
    else:
        value = int(match[0])
        units = match[1].lower().rstrip('ib')
    convertor = K8S_MEM_CONVERSION_MAP.get(units)
    if convertor is None:
        raise ValueError(f'Unknown memory units {units}')
    return float(convertor(value))


def convert_cpu_value_from_k8s_to_units(cpu: str) -> float:
    match = K8S_MEM_CPU_RE.match(cpu).groups()
    if len(match) == 1:
        value = float(match[0])
        units = ''
    else:
        value = float(match[0])
        units = match[1].lower()
    convertor = K8S_CPU_CONVERSION_MAP.get(units)
    if convertor is None:
        raise ValueError(f'Unknown cpu units {units}')
    return float(convertor(value))


def add_pool_node_affinity(value, pool_label_name, pool_name):
    value['nodeAffinity'] = node_affinity = value.get('nodeAffinity', {})
    node_affinity['requiredDuringSchedulingIgnoredDuringExecution'] = required_during = (
        node_affinity.get('requiredDuringSchedulingIgnoredDuringExecution', {}))
    required_during['nodeSelectorTerms'] = node_selectors = required_during.get(
        'nodeSelectorTerms', [])

    for node_selector in node_selectors:
        if 'matchExpressions' not in node_selector:
            continue
        for match_expression in node_selector['matchExpressions']:
            if match_expression['key'] != pool_label_name:
                continue
            if pool_name not in match_expression['values']:
                match_expression['values'].append(pool_name)
            break
        else:
            continue
        break
    else:
        node_selectors.append({'matchExpressions': [{
            'operator': 'In',
            'key': pool_label_name,
            'values': [pool_name],
        }]})

    return value


def get_preferred_pod_anti_affinity_values(name: str) -> dict:
    return {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [{
        "weight": 1,
        "podAffinityTerm": {
            "topologyKey": "kubernetes.io/hostname",
            "labelSelector": {"matchLabels": {
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/instance": name,
            }},
        },
    }]}}


def get_helm_pool_affinity_values(pool_label_name, pool_name):
    return {'affinity': add_pool_node_affinity({}, pool_label_name, pool_name)}


def get_pool_affinity_modifiers(pool_label_name, pool_name):
    def add_pod_owner_pool_affinity(obj):
        if obj['kind'] in ('StatefulSet', 'DaemonSet', 'Deployment', 'Job'):
            obj['spec']['template']['spec']['affinity'] = add_pool_node_affinity(
                obj['spec']['template']['spec'].get('affinity', {}),
                pool_label_name,
                pool_name)

    def add_pod_pool_affinity(obj):
        if obj['kind'] == 'Pod':
            obj['spec']['affinity'] = add_pool_node_affinity(
                obj['spec'].get('affinity', {}),
                pool_label_name,
                pool_name)

    def add_scylla_cluster_pool_affinity(obj):
        if obj['kind'] == 'ScyllaCluster':
            for rack in obj['spec']['datacenter']['racks']:
                rack['placement'] = add_pool_node_affinity(
                    rack.get('placement', {}),
                    pool_label_name,
                    pool_name)

    def add_node_config_pool_affinity(obj):
        if obj['kind'] == 'NodeConfig':
            obj['spec']['placement']['affinity'] = add_pool_node_affinity(
                obj['spec']['placement'].get('affinity', {}),
                pool_label_name,
                pool_name)

    def add_scylla_cluster_monitoring_pool_affinity(obj):
        if obj['kind'] == 'ScyllaDBMonitoring':
            node_affinity = add_pool_node_affinity({}, pool_label_name, pool_name)
            obj['spec']['components']['prometheus']['placement'] = node_affinity
            obj['spec']['components']['grafana']['placement'] = node_affinity

    return [
        add_pod_owner_pool_affinity,
        add_pod_pool_affinity,
        add_scylla_cluster_pool_affinity,
        add_node_config_pool_affinity,
        add_scylla_cluster_monitoring_pool_affinity,
    ]


class HelmValues:
    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], dict):
            self._data = args[0]
        else:
            self._data = dict(**kwargs)

    def get(self, path):
        return walk_thru_data(data=self._data, path=path, separator=".")

    def _path_to_dict(self, path, value):
        keys = path.split(".")
        if len(keys) > 1:
            value = self._path_to_dict(".".join(keys[1:]), value)
        if "[" in keys[0] or "]" in keys[0]:
            raise ValueError("List items ref is not supported. Only dict keys ref is allowed")
        return {keys[0]: value}

    def _merge_dicts(self, destination_dict, patch_dict):
        for key, value in patch_dict.items():
            if isinstance(value, dict):
                destination_dict[key] = self._merge_dicts(destination_dict.get(key, {}), value)
            else:
                destination_dict[key] = value
        return destination_dict

    def set(self, path, value):
        patch_d = self._path_to_dict(path, value)
        self._merge_dicts(self._data, patch_d)

    def delete(self, path):
        path = path.split('.')
        last = path.pop()
        parent = '.'.join(path)
        if not parent:
            parent = self._data
        else:
            parent = self.get(parent)
        if parent is None:
            return
        if last[0] == '[' and last[-1] == ']':
            last = int(last[1:-1])
        try:
            del parent[last]
        except Exception:  # noqa: BLE001
            pass

    def as_dict(self):
        return self._data

    def __hash__(self):
        return hash(frozenset(self._data.items()))

    def __eq__(self, other: Union['HelmValues', dict]):
        if isinstance(other, HelmValues):
            return self._data == other._data
        return self._data == other

    def __ne__(self, other):
        return not self.__eq__(other)
