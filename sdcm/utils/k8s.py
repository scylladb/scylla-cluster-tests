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
import abc
import os
import time
import queue
import logging
import re
import threading
import multiprocessing
import contextlib
from tempfile import NamedTemporaryFile
from typing import Optional, Union, Callable, List
from functools import cached_property

import kubernetes as k8s
import yaml
from kubernetes.client import V1ObjectMeta, V1Service, V1ServiceSpec, V1ContainerPort, \
    V1ServicePort
from paramiko.config import invoke
from urllib3.util.retry import Retry

from sdcm import sct_abs_path
from sdcm.remote import LOCALRUNNER
from sdcm.utils.decorators import timeout as timeout_decor, timeout, retrying
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container
from sdcm.wait import wait_for


KUBECTL_BIN = "kubectl"
HELM_IMAGE = "alpine/helm:3.3.4"

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


class PortExposeService:
    service_hostname: str = None
    service_ip: str = None

    def __init__(self,
                 name: str,
                 selector_value: str,
                 ports: List[V1ServicePort] = None,
                 selector_key: str = 'statefulset.kubernetes.io/pod-name',
                 namespace: str = 'default',
                 core_v1_api: k8s.client.CoreV1Api = None,
                 pod_container_name: str = None,
                 resolver: Callable = None
                 ):
        if core_v1_api is None:
            k8s_configuration = k8s.client.Configuration()
            k8s.config.load_kube_config(
                config_file=os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config')),
                client_configuration=k8s_configuration)
            self.core_v1_api = KubernetesOps.core_v1_api(KubernetesOps.api_client(k8s_configuration))
        else:
            self.core_v1_api = core_v1_api
        self.ports = ports
        self.selector_key = selector_key
        self.selector_value = selector_value
        self.namespace = namespace
        self.name = name
        self.pod_container_name = pod_container_name
        self.resolver = resolver
        self.is_deployed = False

    @property
    def target_pod(self):
        for pod in self.core_v1_api.list_namespaced_pod(self.namespace).items:
            if pod.metadata.labels.get(self.selector_key) == self.selector_value:
                return pod
        raise ValueError("Can't find target pod")

    @property
    def pod_ports(self) -> List[V1ContainerPort]:
        ports = []
        for container in self.target_pod.spec.containers:
            if self.pod_container_name and container.name != self.pod_container_name:
                continue
            if container.ports:
                ports.extend(container.ports)
        return ports

    @property
    def ports_to_expose(self) -> List[V1ServicePort]:
        if self.ports:
            return self.ports
        ports = []
        for port in self.pod_ports:
            if port.container_port is None:
                continue
            ports.append(V1ServicePort(
                port=port.container_port,
                target_port=port.container_port,
                name=port.name,
                protocol=port.protocol
            ))
        return ports

    @property
    def service_definition(self):
        return V1Service(
            metadata=V1ObjectMeta(
                name=self.name,
                namespace=self.namespace
            ),
            spec=V1ServiceSpec(
                ports=self.ports_to_expose,
                publish_not_ready_addresses=True,
                session_affinity=None,
                type='LoadBalancer',
                selector={self.selector_key: self.selector_value}
            )
        )

    def deploy(self):
        events = KubernetesOps.watch_events(self.core_v1_api, name=self.name, namespace=self.namespace)
        self.core_v1_api.create_namespaced_service(namespace=self.namespace, body=self.service_definition)
        service_hostname = wait_for(self.get_service_hostname, timeout=300, throw_exc=False)
        if not service_hostname:
            error_message = "Failed to create load balancer %s, \n" \
                            "it can happen due to the lack of ip addresses in subnet, \n" \
                            "or due to the reaching limits on load balancers quota"
            if events:
                error_message += ', last events:\n' + ('\n'.join([event['object'].message for event in events]))
            raise RuntimeError(error_message, self.name)
        service_ip = self.get_service_ip()
        if not service_ip:
            raise RuntimeError("Failed to resolve hostname %s for load balancer %s", service_hostname, self.name)

        self.service_hostname = service_hostname
        self.service_ip = service_ip
        self.is_deployed = True

    @property
    def service(self) -> V1Service:
        services = self.core_v1_api.list_namespaced_service(
            namespace=self.namespace,
            field_selector=f"metadata.name={self.name}").items
        if not services:
            raise RuntimeError(f"No service with name %s found under namespace %s", self.name, self.namespace)
        return services[0]

    def get_service_hostname(self) -> str:
        try:
            return self.service.status.load_balancer.ingress[0].hostname
        except Exception:
            return ''

    def get_service_ip(self):
        if self.resolver is None:
            raise ValueError("In order to get ip address you need to provide resolver")
        return self.resolver(self.get_service_hostname(), timeout=300)


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
                config_file=os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config')),
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
    def apply_file(cls, kluster, config_path, namespace=None, timeout=KUBECTL_TIMEOUT, environ=None, envsubst=True,
                   modifiers: List[Callable] = None):
        if environ:
            environ_str = (' '.join([f'{name}="{value}"' for name, value in environ.items()])) + ' '
        else:
            environ_str = ''

        with NamedTemporaryFile(mode='tw') as temp_file:
            resulted_content = []
            if envsubst:
                data = LOCALRUNNER.run(f'{environ_str}envsubst<{config_path}', verbose=False).stdout
            else:
                with open(config_path, 'r') as file:
                    data = file.read()
            file_content = yaml.load_all(data)

            for doc in file_content:
                if modifiers:
                    for modifier in modifiers:
                        modifier(doc)
                resulted_content.append(doc)
            temp_file.write(yaml.safe_dump_all(resulted_content))
            temp_file.flush()

            @retrying(n=0, sleep_time=5, timeout=timeout, allowed_exceptions=RuntimeError)
            def run_kubectl():
                try:
                    cls.kubectl(kluster, "apply", "-f",  temp_file.name, namespace=namespace, timeout=timeout)
                except invoke.exceptions.UnexpectedExit as exc:
                    if 'did you specify the right host or port' in exc.result.stderr:
                        raise RuntimeError(str(exc)) from None
                    else:
                        raise

            run_kubectl()

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

    @classmethod
    def get_kubectl_auth_config_for_first_user(cls, config):
        for user in config["users"]:
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
            raise ValueError('Unknown auth-type %s', auth_type)

    @staticmethod
    def wait_for_pods_readiness(kluster, total_pods: Union[int, Callable], readiness_timeout: float, namespace: str,
                                sleep: int = 10):
        @timeout(message=f"Wait for {total_pods} pod(s) from {namespace} namespace to become ready...",
                 timeout=readiness_timeout * 60,
                 sleep_time=sleep)
        def wait_cluster_is_ready():
            # To make it more informative in worst case scenario made it repeat 5 times, by readiness_timeout // 5
            result = kluster.kubectl(
                f"wait --timeout={readiness_timeout // 5}m --all --for=condition=Ready pod",
                namespace=namespace,
                timeout=readiness_timeout // 5 * 60 + 10)
            count = result.stdout.count('condition met')
            if isinstance(total_pods, (int, float)):
                if total_pods != count:
                    raise RuntimeError('Not all pods reported')
            elif callable(total_pods):
                if not total_pods(count):
                    raise RuntimeError('Not all pods reported')

        wait_cluster_is_ready()

    @classmethod
    def patch_kube_config(cls, static_token_path, kube_config_path: str = None) -> None:
        # It assumes that config is already created by gcloud
        # It patches kube config so that instead of running gcloud each time
        # we will get it's output from the cache file located at gcloud_token_path
        # To keep this cache file updated we run GcloudTokenUpdateThread thread
        if kube_config_path is None:
            kube_config_path = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))
        LOGGER.debug("Patch %s to use file token %s", kube_config_path, static_token_path)

        with open(kube_config_path) as kube_config:
            data = yaml.safe_load(kube_config)
        auth_type, user_config = KubernetesOps.get_kubectl_auth_config_for_first_user(data)

        if user_config is None:
            raise RuntimeError(f"Unable to find user configuration in ~/.kube/config")
        KubernetesOps.patch_kubectl_auth_config(user_config, auth_type, "cat", [static_token_path])

        with open(kube_config_path, "w") as kube_config:
            yaml.safe_dump(data, kube_config)

        LOGGER.debug(f'Patched kubectl config at {kube_config_path} with static kubectl token from {static_token_path}')

    @classmethod
    def watch_events(cls, k8s_core_v1_api: k8s.client.CoreV1Api, name: str = None, namespace: str = None,
                     timeout: int = None):
        field_selector = f'involvedObject.name={name}' if name is not None else None
        if namespace is None:
            return k8s.watch.Watch().stream(k8s_core_v1_api.list_event_for_all_namespaces,
                                            field_selector=field_selector, timeout_seconds=timeout)
        return k8s.watch.Watch().stream(k8s_core_v1_api.list_namespaced_event, namespace=namespace,
                                        field_selector=field_selector, timeout_seconds=timeout)


class HelmContainerMixin:
    def helm_container_run_args(self) -> dict:
        kube_config_path = os.environ.get('KUBECONFIG', '~/.kube/config')
        kube_config_dir_path = os.path.expanduser(kube_config_path)
        helm_config_path = os.path.expanduser(os.environ.get('HELM_CONFIG_HOME', '~/.helm'))
        volumes = {
            os.path.dirname(kube_config_dir_path): {"bind": os.path.dirname(kube_config_dir_path), "mode": "rw"},
            helm_config_path: {"bind": "/root/.helm", "mode": "rw"},
            sct_abs_path(""): {"bind": sct_abs_path(""), "mode": "ro"},
            '/tmp': {"bind": "/tmp", "mode": "rw"},
        }
        return dict(image=HELM_IMAGE,
                    entrypoint="/bin/cat",
                    tty=True,
                    name=f"{self.name}-helm",
                    network_mode="host",
                    volumes=volumes,
                    environment={'KUBECONFIG': kube_config_path},
                    )

    @cached_property
    def _helm_container(self) -> Container:
        return ContainerManager.run_container(self, "helm")

    def helm(self, kluster, *command: str, namespace: Optional[str] = None, values: 'HelmValues' = None, prepend_command=None) -> str:
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
        values_file = None
        cmd.extend(command)

        if values:
            helm_values_file = NamedTemporaryFile(mode='tw')
            helm_values_file.write(yaml.safe_dump(values.as_dict()))
            helm_values_file.flush()
            cmd.extend(("-f", helm_values_file.name))
            values_file = helm_values_file

        cmd = " ".join(cmd)

        LOGGER.debug("Execute `%s'", cmd)
        try:
            res = self._helm_container.exec_run(["sh", "-c", cmd])
            if res.exit_code:
                raise DockerException(f"{self._helm_container}: {res.output.decode('utf-8')}")
            return res.output.decode("utf-8")
        finally:
            if values_file:
                values_file.close()

    def helm_install(self, kluster,
                     target_chart_name: str,
                     source_chart_name: str,
                     version: str = "",
                     use_devel: bool = False,
                     values: 'HelmValues' = None,
                     namespace: Optional[str] = None) -> str:
        command = ["install", target_chart_name, source_chart_name]
        prepend_command = []
        if version:
            command.extend(("--version", version))
        if use_devel:
            command.extend(("--devel",))

        return self.helm(
            kluster,
            *command,
            prepend_command=prepend_command,
            namespace=namespace,
            values=values
        )


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
                mode = 'r+' if os.path.exists(self._kubectl_token_path) else 'w'
                with open(self._kubectl_token_path, mode) as gcloud_config_file:
                    gcloud_config_file.write(self.get_token())
                    gcloud_config_file.flush()
                LOGGER.debug('Cloud token has been updated and stored at %s', self._kubectl_token_path)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.debug('Failed to read gcloud config: %s', exc)
                wait_time = 5
            else:
                wait_time = self.update_period

    @abc.abstractmethod
    def get_token(self) -> str:
        pass

    def stop(self, timeout=None):
        self._termination_event.set()
        self.join(timeout)


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
        raise ValueError('Unknown memory units %s', units)
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
        raise ValueError('Unknown cpu units %s', units)
    return float(convertor(value))


def add_pool_node_affinity(value, pool_label_name, pool_name):
    target_selector_term = {'matchExpressions': [{'operator': 'In', 'values': [pool_name], 'key': pool_label_name}]}
    value['nodeAffinity'] = node_affinity = value.get('nodeAffinity', {})
    node_affinity['requiredDuringSchedulingIgnoredDuringExecution'] = required_during = \
        node_affinity.get('requiredDuringSchedulingIgnoredDuringExecution', {})
    required_during['nodeSelectorTerms'] = node_selectors = required_during.get('nodeSelectorTerms', [])

    exists = False
    for node_selector in node_selectors:
        if node_selector == target_selector_term:
            exists = True
            break

    if not exists:
        node_selectors.append(target_selector_term)

    return value


def get_helm_pool_affinity_values(pool_label_name, pool_name):
    return {'affinity': add_pool_node_affinity({}, pool_label_name, pool_name)}


def get_pool_affinity_modifiers(pool_label_name, pool_name):
    def add_statefulset_or_daemonset_pool_affinity(x):
        if x['kind'] in ['StatefulSet', 'DaemonSet']:
            x['spec']['template']['spec']['affinity'] = add_pool_node_affinity(
                x['spec']['template']['spec'].get('affinity', {}),
                pool_label_name,
                pool_name)

    def add_scylla_cluster_pool_affinity(x):
        if x['kind'] == 'ScyllaCluster':
            for rack in x['spec']['datacenter']['racks']:
                rack['placement'] = add_pool_node_affinity(
                    rack.get('placement', {}),
                    pool_label_name,
                    pool_name)

    return [add_statefulset_or_daemonset_pool_affinity, add_scylla_cluster_pool_affinity]


class HelmValues:
    def __init__(self, *args, **kwargs):
        if len(args) == 1 and type(args[0]) is dict:
            self._data = args[0]
        else:
            self._data = dict(**kwargs)

    def get(self, path):
        current = self._data
        for name in path.split('.'):
            if current is None:
                return None
            if name.isalnum() and isinstance(current, (list, tuple, set)):
                try:
                    current = current[int(name)]
                except Exception:
                    current = None
                continue
            current = current.get(name, None)
        return current

    def set(self, path, value):
        current = self._data
        chain = []
        types = []
        for attr in path.split('.'):
            types.append(dict)
            if attr[-1] == ']':
                idx = attr.find('[')
                chain.extend([attr[0:idx], int(attr[idx + 1:-1])])
                types.append(list)
                continue
            chain.append(attr)

        # last_item = chain.pop()
        types.pop(0)
        types.append(None)

        for num, (attr, next_item_type) in enumerate(zip(chain, types)):
            try:
                if isinstance(attr, int) and isinstance(current, dict):
                    raise TypeError()
                attr_value = current[attr]
            except (KeyError, IndexError):
                attr_value = None
            except TypeError:
                raise ValueError("Wrong type provided at section")

            if None is attr_value:
                if next_item_type is None:
                    attr_value = value
                else:
                    attr_value = next_item_type()
                if type(current) is dict:
                    current[attr] = attr_value
                else:
                    if attr > len(current):
                        raise ValueError("Can add items to tail of the list only")
                    elif attr == len(current):
                        current.append(attr_value)
                    else:
                        current[attr] = attr_value

            current = attr_value

    def as_dict(self):
        return self._data
