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

from __future__ import annotations

import os
import re
import abc
import json
import time
import base64
import random
import socket
import logging
import traceback
import contextlib
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from difflib import unified_diff
from functools import cached_property, partialmethod, partial
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from threading import RLock
from typing import Optional, Union, List, Dict, Any, ContextManager, Type, Tuple, Callable

import yaml
import kubernetes as k8s
from kubernetes.client import V1Container, V1ResourceRequirements, V1ConfigMap
from kubernetes.dynamic.resource import Resource, ResourceField, ResourceInstance, ResourceList, Subresource
from invoke.exceptions import CommandTimedOut

from sdcm import sct_abs_path, cluster, cluster_docker
from sdcm.cluster import DeadNode
from sdcm.test_config import TestConfig
from sdcm.db_stats import PrometheusDBStats
from sdcm.remote import NETWORK_EXCEPTIONS
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.coredump import CoredumpExportFileThread
from sdcm.mgmt import AnyManagerCluster
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils import properties
from sdcm.utils.common import download_from_github, shorten_cluster_name, walk_thru_data
from sdcm.utils.k8s import (
    add_pool_node_affinity,
    convert_cpu_units_to_k8s_value,
    convert_cpu_value_from_k8s_to_units,
    convert_memory_units_to_k8s_value,
    convert_memory_value_from_k8s_to_units,
    get_helm_pool_affinity_values,
    get_pool_affinity_modifiers,
    ApiCallRateLimiter,
    CordonNodes,
    JSON_PATCH_TYPE,
    KubernetesOps,
    KUBECTL_TIMEOUT,
    HelmValues,
    PortExposeService,
    TokenUpdateThread,
)
from sdcm.utils.decorators import log_run_info, retrying, timeout
from sdcm.utils.remote_logger import get_system_logging_thread, CertManagerLogger, ScyllaOperatorLogger, \
    KubectlClusterEventsLogger, ScyllaManagerLogger
from sdcm.utils.version_utils import get_git_tag_from_helm_chart_version
from sdcm.wait import wait_for
from sdcm.cluster_k8s.operator_monitoring import ScyllaOperatorLogMonitoring


ANY_KUBERNETES_RESOURCE = Union[Resource, ResourceField, ResourceInstance, ResourceList, Subresource]

CERT_MANAGER_TEST_CONFIG = sct_abs_path("sdcm/k8s_configs/cert-manager-test.yaml")
LOADER_CLUSTER_CONFIG = sct_abs_path("sdcm/k8s_configs/loaders.yaml")
LOCAL_PROVISIONER_DIR = sct_abs_path("sdcm/k8s_configs/provisioner")
LOCAL_MINIO_DIR = sct_abs_path("sdcm/k8s_configs/minio")

SCYLLA_API_VERSION = "scylla.scylladb.com/v1"
SCYLLA_CLUSTER_RESOURCE_KIND = "ScyllaCluster"
DEPLOY_SCYLLA_CLUSTER_DELAY = 15  # seconds
SCYLLA_OPERATOR_NAMESPACE = "scylla-operator-system"
SCYLLA_MANAGER_NAMESPACE = "scylla-manager-system"
SCYLLA_NAMESPACE = "scylla"
MINIO_NAMESPACE = "minio"
SCYLLA_CONFIG_NAME = "scylla-config"

# Resources that are used by container deployed by scylla-operator on scylla nodes
OPERATOR_CONTAINERS_RESOURCES = {
    'cpu': 0.05,
    'memory': 0.01,
}

# Resources that are used by side-car injected by sct into scylla-operator statefulset
# Look at ScyllaPodCluster.add_sidecar_injection()
SIDECAR_CONTAINERS_RESOURCES = {
    'cpu': 0.01,
    'memory': 0.05,
}

# Other common resources which get deployed on each scylla node such as 'kube-proxy'
# EKS: between 100m-200m CPU
# GKE: between 200m-300m CPU and 250Mi RAM
# Above numbers are "explicit" reservations. So, reserve a bit more for other common pods.
COMMON_CONTAINERS_RESOURCES = {
    'cpu': 0.51,
    'memory': 0.51,
}

LOGGER = logging.getLogger(__name__)


class DnsPodResolver:
    DNS_RESOLVER_CONFIG = sct_abs_path("sdcm/k8s_configs/dns-resolver-pod.yaml")

    def __init__(self, k8s_cluster: KubernetesCluster, pod_name: str, pool: CloudK8sNodePool = None):
        self.pod_name = pod_name
        self.pool = pool
        self.k8s_cluster = k8s_cluster

    def deploy(self):
        if self.pool:
            affinity_modifiers = self.pool.affinity_modifiers
        else:
            affinity_modifiers = []

        self.k8s_cluster.apply_file(
            self.DNS_RESOLVER_CONFIG,
            modifiers=affinity_modifiers,
            environ={'POD_NAME': self.pod_name})

    def resolve(self, hostname: str) -> str:
        result = self.k8s_cluster.kubectl(f'exec {self.pod_name} -- host {hostname}', verbose=False).stdout
        tmp = result.split()
        # result = self.k8s_cluster.kubectl(f'exec {self.pod_name} -- dig +short {hostname}').stdout.splitlines()
        if len(tmp) < 4:
            raise RuntimeError("Got wrong result: %s", result)
        return tmp[3]


class CloudK8sNodePool(metaclass=abc.ABCMeta):
    def __init__(
            self,
            k8s_cluster: 'KubernetesCluster',
            name: str,
            num_nodes: int,
            instance_type: str,
            image_type: str,
            disk_size: int = None,
            disk_type: str = None,
            labels: dict = None,
            tags: dict = None,
            is_deployed: bool = False):
        self.k8s_cluster = k8s_cluster
        self.name = name
        self.num_nodes = int(num_nodes)
        self.instance_type = instance_type
        self.disk_size = disk_size
        self.disk_type = disk_type
        self.image_type = image_type
        self.labels = labels
        self.tags = tags
        self.is_deployed = is_deployed

    @abc.abstractmethod
    def deploy(self):
        pass

    def deploy_and_wait_till_ready(self):
        if not self.is_deployed:
            self.deploy()
        self.wait_for_nodes_readiness()

    @abc.abstractmethod
    def undeploy(self):
        pass

    @abc.abstractmethod
    def resize(self, num_nodes: int):
        pass

    def __str__(self):
        data = [f'name="{self.name}"', *[f'{param}="{value}"' for param, value in self.__dict__.items() if
                                         param not in ['name', 'k8s_cluster']]]
        return f"<{self.__class__.__name__}:{', '.join(data)}>"

    @cached_property
    def affinity_modifiers(self) -> List[Callable]:
        return get_pool_affinity_modifiers(self.pool_label_name, self.name)

    @cached_property
    def helm_affinity_values(self) -> HelmValues:
        return HelmValues(get_helm_pool_affinity_values(self.pool_label_name, self.name))

    @cached_property
    def pool_label_name(self) -> str:
        return self.k8s_cluster.POOL_LABEL_NAME

    @cached_property
    def cpu_and_memory_capacity(self) -> Tuple[float, float]:
        for el in self.k8s_cluster.k8s_core_v1_api.list_node().items:
            if el.metadata.labels.get(self.pool_label_name, '') == self.name:
                capacity = el.status.allocatable
                return convert_cpu_value_from_k8s_to_units(capacity['cpu']), convert_memory_value_from_k8s_to_units(
                    capacity['memory'])
        raise RuntimeError("Can't find any node for pool '%s'", self.name)

    @property
    def cpu_capacity(self) -> float:
        return self.cpu_and_memory_capacity[0]

    @property
    def memory_capacity(self) -> float:
        return self.cpu_and_memory_capacity[1]

    @property
    def readiness_timeout(self) -> int:
        return 10 + (10 * self.num_nodes)

    def wait_for_nodes_readiness(self):
        readiness_timeout = self.readiness_timeout

        @timeout(
            message=f"Wait for {self.num_nodes} node(s) in pool {self.name} to be ready...",
            sleep_time=30,
            timeout=readiness_timeout * 60)
        def wait_nodes_are_ready():
            # To make it more informative in worst case scenario made it repeat 5 times, by readiness_timeout // 5
            result = self.k8s_cluster.kubectl_no_wait(
                f"wait --timeout={self.readiness_timeout // 5}m -l {self.pool_label_name}={self.name} "
                f"--for=condition=Ready node",
                timeout=readiness_timeout // 5 * 60 + 10, verbose=False)
            if result.stdout.count('condition met') != self.num_nodes:
                raise RuntimeError('Not all nodes reported')

        wait_nodes_are_ready()


class KubernetesCluster(metaclass=abc.ABCMeta):
    AUXILIARY_POOL_NAME = 'auxiliary-pool'
    SCYLLA_POOL_NAME = 'scylla-pool'
    MONITORING_POOL_NAME = 'monitoring-pool'
    LOADER_POOL_NAME = 'loader-pool'
    DNS_RESOLVER_POD_NAME = 'dns-resolver-pod'
    POOL_LABEL_NAME: str = None
    USE_POD_RESOLVER = False
    USE_MONITORING_EXPOSE_SERVICE = False

    api_call_rate_limiter: Optional[ApiCallRateLimiter] = None
    k8s_monitoring_prometheus_expose_service: Optional[PortExposeService] = None

    datacenter = ()
    _cert_manager_journal_thread: Optional[CertManagerLogger] = None
    _scylla_manager_journal_thread: Optional[ScyllaManagerLogger] = None
    _scylla_operator_journal_thread: Optional[ScyllaOperatorLogger] = None
    _scylla_cluster_events_thread: Optional[KubectlClusterEventsLogger] = None

    _scylla_operator_log_monitor_thread: Optional[ScyllaOperatorLogMonitoring] = None
    _token_update_thread: Optional[TokenUpdateThread] = None
    pools: Dict[str, CloudK8sNodePool]

    def __init__(self, params: dict, user_prefix: str = '', region_name: str = None, cluster_uuid: str = None):
        self.pools = {}
        if cluster_uuid is None:
            self.uuid = TestConfig.test_id()
        else:
            self.uuid = cluster_uuid
        self.region_name = region_name
        self.shortid = str(self.uuid)[:8]
        self.name = '%s-%s' % (user_prefix, self.shortid)
        self.params = params
        self.api_call_rate_limiter = None

    # NOTE: Following class attr(s) are defined for consumers of this class
    #       such as 'sdcm.utils.remote_logger.ScyllaOperatorLogger'.
    _scylla_operator_namespace = SCYLLA_OPERATOR_NAMESPACE
    _scylla_manager_namespace = SCYLLA_MANAGER_NAMESPACE
    _scylla_namespace = SCYLLA_NAMESPACE

    @property
    def k8s_server_url(self) -> Optional[str]:
        return None

    @cached_property
    def short_cluster_name(self):
        return shorten_cluster_name(self.name, 40).replace('_', '-')

    kubectl_cmd = partialmethod(KubernetesOps.kubectl_cmd)
    apply_file = partialmethod(KubernetesOps.apply_file)

    def kubectl_no_wait(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                        verbose=True):
        return KubernetesOps.kubectl(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                     ignore_status=ignore_status, verbose=verbose)

    def kubectl(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                verbose=True):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return KubernetesOps.kubectl(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                     ignore_status=ignore_status, verbose=verbose)

    def kubectl_multi_cmd(self, *command, namespace=None, timeout=KUBECTL_TIMEOUT, remoter=None, ignore_status=False,
                          verbose=True):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return KubernetesOps.kubectl_multi_cmd(self, *command, namespace=namespace, timeout=timeout, remoter=remoter,
                                               ignore_status=ignore_status, verbose=verbose)

    @property
    def helm(self):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return partial(cluster.TestConfig.tester_obj().localhost.helm, self)

    @property
    def helm_install(self):
        if self.api_call_rate_limiter:
            self.api_call_rate_limiter.wait()
        return partial(cluster.TestConfig.tester_obj().localhost.helm_install, self)

    @cached_property
    def kubectl_token_path(self):
        return os.path.join(os.path.dirname(
            os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))), 'kubectl.token')

    @cached_property
    def cert_manager_log(self) -> str:
        return os.path.join(self.logdir, "cert_manager.log")

    @cached_property
    def scylla_manager_log(self) -> str:
        return os.path.join(self.logdir, "scylla_manager.log")

    def start_cert_manager_journal_thread(self) -> None:
        self._cert_manager_journal_thread = CertManagerLogger(self, self.cert_manager_log)
        self._cert_manager_journal_thread.start()

    def start_scylla_manager_journal_thread(self):
        self._scylla_manager_journal_thread = ScyllaManagerLogger(self, self.scylla_manager_log)
        self._scylla_manager_journal_thread.start()

    def set_nodeselector_for_kubedns(self, pool_name):
        # EKS and GKE deploy kube-dns pods, so make it be deployed on default nodes, not any other
        data = {"spec": {"template": {"spec": {"nodeSelector": {
            self.POOL_LABEL_NAME: pool_name,
        }}}}}
        deployment_name = self.kubectl(
            "get deployments -l k8s-app=kube-dns --no-headers -o custom-columns=:.metadata.name",
            namespace="kube-system").stdout.strip()
        self.kubectl(
            f"patch deployments {deployment_name} -p '{json.dumps(data)}'",
            namespace="kube-system")

    @log_run_info
    def deploy_cert_manager(self, pool_name: str = None) -> None:
        if not self.params.get('reuse_cluster'):
            if pool_name is None:
                pool_name = self.AUXILIARY_POOL_NAME

            LOGGER.info("Deploy cert-manager")
            self.kubectl("create namespace cert-manager", ignore_status=True)
            LOGGER.debug(self.helm("repo add jetstack https://charts.jetstack.io"))

            if pool_name:
                values_dict = get_helm_pool_affinity_values(self.POOL_LABEL_NAME, pool_name)
                values_dict["cainjector"] = {"affinity": values_dict["affinity"]}
                values_dict["webhook"] = {"affinity": values_dict["affinity"]}
                helm_values = HelmValues(values_dict)
            else:
                helm_values = HelmValues()

            helm_values.set('installCRDs', True)

            LOGGER.debug(self.helm(
                f"install cert-manager jetstack/cert-manager --version v{self.params.get('k8s_cert_manager_version')}",
                namespace="cert-manager", values=helm_values))
            time.sleep(10)

        self.kubectl("wait --timeout=10m --all --for=condition=Ready pod", namespace="cert-manager")
        wait_for(
            self.check_if_cert_manager_fully_functional,
            text='Waiting for cert-manager to become fully operational',
            timeout=10 * 60,
            step=10,
            throw_exc=True)

        self.start_cert_manager_journal_thread()

    @cached_property
    def _scylla_operator_chart_version(self):
        LOGGER.debug(self.helm(
            f"repo add scylla-operator {self.params.get('k8s_scylla_operator_helm_repo')}"))

        # NOTE: 'scylla-operator' and 'scylla-manager' chart versions are always the same.
        #       So, we can reuse one for another.
        chart_version = self.params.get(
            "k8s_scylla_operator_chart_version").strip().lower()
        if chart_version in ("", "latest"):
            latest_version_raw = self.helm(
                "search repo scylla-operator/scylla-operator --devel -o yaml")
            latest_version = yaml.safe_load(latest_version_raw)
            assert isinstance(
                latest_version, list), f"Expected list of data, got: {type(latest_version)}"
            assert len(latest_version) == 1, "Expected only one element in the list of versions"
            assert "version" in latest_version[0], "Expected presence of 'version' key"
            chart_version = latest_version[0]["version"].strip()
            LOGGER.info(f"Using automatically found following "
                        f"latest scylla-operator chart version: {chart_version}")
        else:
            LOGGER.info(f"Using following predefined scylla-operator "
                        f"chart version: {chart_version}")
        return chart_version

    @log_run_info
    def deploy_scylla_manager(self, pool_name: str = None) -> None:
        # Calculate options values which must be set
        #
        # image.tag                  -> self.params.get('mgmt_docker_image').split(':')[-1]
        # controllerImage.repository -> self.params.get(
        #                                   'k8s_scylla_operator_docker_image').split('/')[0]
        # controllerImage.tag        -> self.params.get(
        #                                   'k8s_scylla_operator_docker_image').split(':')[-1]
        if not self.params.get('reuse_cluster'):
            LOGGER.info("Deploy scylla-manager")

            helm_affinity = get_helm_pool_affinity_values(
                self.POOL_LABEL_NAME, pool_name) if pool_name else {}
            values = HelmValues(**helm_affinity)
            values.set("controllerAffinity", helm_affinity.get("affinity", {}))
            values.set("scylla", {
                "developerMode": True,
                "datacenter": "manager-dc",
                "racks": [{
                    "name": "manager-rack",
                    "members": 1,
                    # TODO: uncomment 'placement' field when it is allowed to be provided
                    #       as part of the scylla-manager helm chart.
                    #       https://github.com/scylladb/scylla-operator/issues/631
                    # "placement": {"nodeAffinity": helm_affinity["affinity"]},
                    "storage": {"capacity": "10Gi"},
                    "resources": {
                        "limits": {"cpu": 1, "memory": "200Mi"},
                        "requests": {"cpu": 1, "memory": "200Mi"},
                    },
                }],
            })

            mgmt_docker_image_tag = self.params.get('mgmt_docker_image').split(':')[-1]
            if mgmt_docker_image_tag:
                values.set('image.tag', mgmt_docker_image_tag)

            scylla_operator_repo_base = self.params.get(
                'k8s_scylla_operator_docker_image').split('/')[0]
            if scylla_operator_repo_base:
                values.set('controllerImage.repository', scylla_operator_repo_base)

            scylla_operator_image_tag = self.params.get(
                'k8s_scylla_operator_docker_image').split(':')[-1]
            if scylla_operator_image_tag:
                values.set('controllerImage.tag', scylla_operator_image_tag)

            self.kubectl(f'create namespace {SCYLLA_MANAGER_NAMESPACE}')

            # TODO: usage of 'cordon' feature below is a workaround for the scylla-operator issue
            #       https://github.com/scylladb/scylla-operator/issues/631
            #       where it is not possible to provide node/pod affinity for the scylla server
            #       which gets installed for the scylla-manager deployed by scylla-operator.
            to_cordon = ", ".join(['loader-pool', 'monitoring-pool', 'scylla-pool'])
            with CordonNodes(self.kubectl, f"{self.POOL_LABEL_NAME} in ({to_cordon})"):
                # Install and wait for initialization of the Scylla Manager chart
                LOGGER.debug(self.helm_install(
                    target_chart_name="scylla-manager",
                    source_chart_name="scylla-operator/scylla-manager",
                    version=self._scylla_operator_chart_version,
                    use_devel=True,
                    values=values,
                    namespace=SCYLLA_MANAGER_NAMESPACE,
                ))

        self.kubectl("wait --timeout=10m --all --for=condition=Ready pod",
                     namespace=SCYLLA_MANAGER_NAMESPACE)
        self.start_scylla_manager_journal_thread()

    def check_if_cert_manager_fully_functional(self) -> bool:
        # Cert-manager readiness status does not guarantee that it is fully operational
        # This function checks it if is operational via deploying ca and issuing certificate
        try:
            self.apply_file(CERT_MANAGER_TEST_CONFIG)
            return True
        finally:
            self.kubectl(f'delete -f {CERT_MANAGER_TEST_CONFIG}', ignore_status=True)

    @property
    def scylla_operator_log(self) -> str:
        return os.path.join(self.logdir, "scylla_operator.log")

    @property
    def scylla_cluster_event_log(self) -> str:
        return os.path.join(self.logdir, "scylla_cluster_events.log")

    def start_scylla_operator_journal_thread(self) -> None:
        self._scylla_operator_journal_thread = ScyllaOperatorLogger(self, self.scylla_operator_log)
        self._scylla_operator_journal_thread.start()
        self._scylla_operator_log_monitor_thread = ScyllaOperatorLogMonitoring(self)
        self._scylla_operator_log_monitor_thread.start()

    def start_scylla_cluster_events_thread(self) -> None:
        self._scylla_cluster_events_thread = KubectlClusterEventsLogger(self, self.scylla_cluster_event_log)
        self._scylla_cluster_events_thread.start()

    @log_run_info
    def deploy_scylla_operator(self, pool_name: str = None) -> None:
        if not self.params.get('reuse_cluster'):
            if pool_name is None:
                pool_name = self.AUXILIARY_POOL_NAME

            values = HelmValues(**get_helm_pool_affinity_values(self.POOL_LABEL_NAME, pool_name) if pool_name else {})

            # Calculate options values which must be set
            #
            # image.repository -> self.params.get('k8s_scylla_operator_docker_image').split('/')[0]
            # image.tag        -> self.params.get('k8s_scylla_operator_docker_image').split(':')[-1]

            scylla_operator_repo_base = self.params.get(
                'k8s_scylla_operator_docker_image').split('/')[0]
            if scylla_operator_repo_base:
                values.set('image.repository', scylla_operator_repo_base)

            scylla_operator_image_tag = self.params.get(
                'k8s_scylla_operator_docker_image').split(':')[-1]
            if scylla_operator_image_tag:
                values.set('image.tag', scylla_operator_image_tag)

            # Install and wait for initialization of the Scylla Operator chart
            LOGGER.info("Deploy Scylla Operator")
            self.kubectl(f'create namespace {SCYLLA_OPERATOR_NAMESPACE}')
            LOGGER.debug(self.helm_install(
                target_chart_name="scylla-operator",
                source_chart_name="scylla-operator/scylla-operator",
                version=self._scylla_operator_chart_version,
                use_devel=True,
                namespace=SCYLLA_OPERATOR_NAMESPACE,
                values=values
            ))

            time.sleep(10)
            KubernetesOps.wait_for_pods_readiness(
                kluster=self,
                total_pods=lambda pods: pods > 0,
                readiness_timeout=5*60,
                namespace=SCYLLA_OPERATOR_NAMESPACE
            )
        # Start the Scylla Operator logging thread
        self.start_scylla_operator_journal_thread()

    @log_run_info
    def deploy_minio_s3_backend(self):
        if not self.params.get('reuse_cluster'):
            LOGGER.info('Deploy minio s3-like backend server')
            self.kubectl(f"create namespace {MINIO_NAMESPACE}")
            LOGGER.debug(self.helm_install(
                target_chart_name="minio",
                source_chart_name=LOCAL_MINIO_DIR,
                namespace=MINIO_NAMESPACE,
            ))

        wait_for(lambda: self.minio_ip_address, text='Waiting for minio pod to popup',
                 timeout=120, throw_exc=True)
        self.kubectl("wait --timeout=10m -l app=minio --for=condition=Ready pod",
                     timeout=605, namespace=MINIO_NAMESPACE)

    def get_scylla_cluster_helm_values(self, cpu_limit, memory_limit, pool_name: str = None) -> HelmValues:
        return HelmValues({
            'nameOverride': '',
            'fullnameOverride': self.params.get('k8s_scylla_cluster_name'),
            'scyllaImage': {
                'repository': self.params.get('docker_image'),
                'tag': self.params.get('scylla_version')
            },
            'agentImage': {
                'repository': 'scylladb/scylla-manager-agent',
                'tag': self.params.get('scylla_mgmt_agent_version')
            },
            'serviceAccount': {
                'create': True,
                'annotations': {},
                'name': f"{self.params.get('k8s_scylla_cluster_name')}-member"
            },
            'alternator': {
                'enabled': False,
                'port': 8000,
                'writeIsolation': 'always'
            },
            'developerMode': False,
            'cpuset': True,
            'hostNetworking': True,
            'automaticOrphanedNodeCleanup': True,
            'sysctls': ["fs.aio-max-nr=2097152"],
            'serviceMonitor': {
                'create': False
            },
            'datacenter': self.params.get('k8s_scylla_datacenter'),
            'racks': [
                {
                    'name': self.params.get('k8s_scylla_rack'),
                    'scyllaConfig': SCYLLA_CONFIG_NAME,
                    'scyllaAgentConfig': 'scylla-agent-config',
                    'members': 0,
                    'storage': {
                        'storageClassName': self.params.get('k8s_scylla_disk_class'),
                        'capacity': f"{self.params.get('k8s_scylla_disk_gi')}Gi"
                    },
                    'resources': {
                        'limits': {
                            'cpu': cpu_limit,
                            'memory': memory_limit
                        },
                        'requests': {
                            'cpu': cpu_limit,
                            'memory': memory_limit
                        },
                    },
                    'placement': add_pool_node_affinity({}, self.POOL_LABEL_NAME, pool_name) if pool_name else {}
                }
            ]
        })

    def wait_till_cluster_is_operational(self):
        if self.api_call_rate_limiter:
            with self.api_call_rate_limiter.pause:
                self.api_call_rate_limiter.wait_till_api_become_not_operational(self)
                self.api_call_rate_limiter.wait_till_api_become_stable(self)
        self.wait_all_node_pools_to_be_ready()

    def create_scylla_manager_agent_config(self):
        # Create kubernetes secret that holds scylla manager agent configuration
        self.update_secret_from_data('scylla-agent-config', SCYLLA_NAMESPACE, {
            'scylla-manager-agent.yaml': {
                's3': {
                    'provider': 'Minio',
                    'endpoint': self.s3_provider_endpoint,
                    'access_key_id': 'minio_access_key',
                    'secret_access_key': 'minio_secret_key'
                }
            }
        })

    @log_run_info
    def deploy_scylla_cluster(self,  node_pool: CloudK8sNodePool = None, node_prepare_config: str = None) -> None:
        if self.params.get('reuse_cluster'):
            try:
                self.wait_till_cluster_is_operational()
                LOGGER.debug("Check Scylla cluster")
                self.kubectl("get scyllaclusters.scylla.scylladb.com", namespace=SCYLLA_NAMESPACE)
                self.start_scylla_cluster_events_thread()
                return
            except:
                raise RuntimeError("SCT_REUSE_CLUSTER is set, but target scylla cluster is unhealthy")
        LOGGER.info("Create and initialize a Scylla cluster")
        self.kubectl(f"create namespace {SCYLLA_NAMESPACE}")

        if self.params['use_mgmt']:
            self.create_scylla_manager_agent_config()

        affinity_modifiers = []

        if node_pool:
            affinity_modifiers.extend(node_pool.affinity_modifiers)
            if node_prepare_config:
                LOGGER.info("Install DaemonSets required by scylla nodes")
                self.apply_file(node_prepare_config, modifiers=affinity_modifiers, envsubst=False)

            LOGGER.info("Install local volume provisioner")
            self.helm(f"install local-provisioner {LOCAL_PROVISIONER_DIR}", values=node_pool.helm_affinity_values)

            # Calculate cpu and memory limits to occupy all available amounts by scylla pods
            cpu_limit, memory_limit = node_pool.cpu_and_memory_capacity
            # TBD: Remove reduction logic after https://github.com/scylladb/scylla-operator/issues/384 is fixed
            cpu_limit = int(
                cpu_limit
                - OPERATOR_CONTAINERS_RESOURCES['cpu']
                - SIDECAR_CONTAINERS_RESOURCES['cpu']
                - COMMON_CONTAINERS_RESOURCES['cpu']
            )
            memory_limit = int(
                memory_limit
                - OPERATOR_CONTAINERS_RESOURCES['memory']
                - SIDECAR_CONTAINERS_RESOURCES['memory']
                - COMMON_CONTAINERS_RESOURCES['memory']
            )
        else:
            cpu_limit = 1
            memory_limit = 2

        cpu_limit = int(cpu_limit)
        memory_limit = convert_memory_units_to_k8s_value(memory_limit)

        # Deploy scylla cluster
        LOGGER.debug(self.helm_install(
            target_chart_name="scylla",
            source_chart_name="scylla-operator/scylla",
            version=self._scylla_operator_chart_version,
            use_devel=True,
            values=self.get_scylla_cluster_helm_values(
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                pool_name=node_pool.name if node_pool else None),
            namespace=SCYLLA_NAMESPACE,
        ))

        self.wait_till_cluster_is_operational()

        LOGGER.debug("Check Scylla cluster")
        self.kubectl("get scyllaclusters.scylla.scylladb.com", namespace=SCYLLA_NAMESPACE)
        LOGGER.debug("Wait for %d secs before we start to apply changes to the cluster", DEPLOY_SCYLLA_CLUSTER_DELAY)
        time.sleep(DEPLOY_SCYLLA_CLUSTER_DELAY)
        self.start_scylla_cluster_events_thread()

    @log_run_info
    def deploy_loaders_cluster(self, config: str, node_pool: CloudK8sNodePool = None) -> None:
        LOGGER.info("Create and initialize a loaders cluster")
        if node_pool:
            self.deploy_node_pool(node_pool)
            cpu_limit, memory_limit = node_pool.cpu_and_memory_capacity
            cpu_limit, memory_limit = cpu_limit - 1, memory_limit - 1
            affinity_modifiers = node_pool.affinity_modifiers
        else:
            cpu_limit = 2
            memory_limit = 4
            affinity_modifiers = []

        cpu_limit = convert_cpu_units_to_k8s_value(cpu_limit)
        memory_limit = convert_memory_units_to_k8s_value(memory_limit)

        self.apply_file(config, environ={'CPU_LIMIT': cpu_limit, 'MEMORY_LIMIT': memory_limit},
                        modifiers=affinity_modifiers)
        LOGGER.debug("Check the loaders cluster")
        self.kubectl("get statefulset", namespace="sct-loaders")
        self.kubectl("get pods", namespace="sct-loaders")

    @log_run_info
    def deploy_monitoring_cluster(
            self, scylla_operator_tag: str, namespace: str = "monitoring", is_manager_deployed: bool = False,
            node_pool: CloudK8sNodePool = None) -> None:
        """
        This procedure comes from scylla-operator repo:
        https://github.com/scylladb/scylla-operator/blob/master/docs/source/generic.md#setting-up-monitoring

        If it fails please consider reporting and fixing issue in scylla-operator repo too
        """
        if self.params.get('reuse_cluster'):
            LOGGER.info("Reusing existing monitoring cluster")
            self.check_k8s_monitoring_cluster_health(namespace=namespace)
            return
        LOGGER.info("Create and initialize a monitoring cluster")
        if scylla_operator_tag in ('nightly', 'latest'):
            scylla_operator_tag = 'master'
        elif scylla_operator_tag == '':
            scylla_operator_tag = get_git_tag_from_helm_chart_version(
                self._scylla_operator_chart_version)

        with TemporaryDirectory() as tmp_dir_name:
            scylla_operator_dir = os.path.join(tmp_dir_name, 'scylla-operator')
            scylla_monitoring_dir = os.path.join(tmp_dir_name, 'scylla-monitoring')
            LOGGER.info("Download scylla-operator sources")
            download_from_github(
                repo='scylladb/scylla-operator',
                tag=scylla_operator_tag,
                dst_dir=scylla_operator_dir)
            LOGGER.info("Download scylla-monitoring sources")
            download_from_github(
                repo='scylladb/scylla-monitoring',
                tag='scylla-monitoring-3.6.0',
                dst_dir=scylla_monitoring_dir)

            values_filepath = os.path.join(
                scylla_operator_dir, "examples", "common", "monitoring", "values.yaml")
            with open(values_filepath, "r") as values_stream:
                values_data = yaml.safe_load(values_stream)
                # NOTE: we need to unset all the tags because latest chart version may be
                # incompatible with old versions of apps.
                # for example 'prometheus-operator' v0.48.0 compatible with
                # 'kube-prometheus-stack' v16.1.2+
                for values_key in values_data.keys():
                    values_data[values_key].get("image", {}).pop("tag", "")
            if node_pool:
                self.deploy_node_pool(node_pool)
                monitoring_affinity_rules = get_helm_pool_affinity_values(
                    node_pool.pool_label_name, node_pool.name)
            else:
                monitoring_affinity_rules = {}

            helm_values = HelmValues(values_data)

            # Additional values to be set:
            #   nodeExporter.enabled = False
            #   alertmanager.alertmanagerSpec.affinity
            #   prometheusOperator.affinity
            #   prometheus.prometheusSpec.affinity
            #   prometheus.prometheusSpec.service
            #   grafana.affinity
            helm_values.set('nodeExporter.enabled', False)
            helm_values.set('alertmanager.alertmanagerSpec', monitoring_affinity_rules)
            helm_values.set('prometheusOperator', monitoring_affinity_rules)
            prometheus_values = {
                'prometheusSpec': {
                    'affinity': monitoring_affinity_rules.get('affinity', {}),
                    # NOTE: set following values the same as in SCT (standalone) monitoring
                    'scrapeInterval': '20s',
                    'scrapeTimeout': '15s',
                },
                'service': {
                    # NOTE: required for out-of-K8S-cluster access
                    # nodeIp:30090 will redirect traffic to prometheusPod:9090
                    'type': 'NodePort',
                    'nodePort': self.k8s_prometheus_external_port,
                },
            }
            helm_values.set('prometheus', prometheus_values)
            grafana_values = {
                'affinity': monitoring_affinity_rules.get('affinity', {}),
                'service': {
                    # NOTE: required for out-of-K8S-cluster access
                    # k8sMonitoringNodeIp:30000 (30 thousands) will redirect traffic to
                    # grafanaPod:3000 (3 thousands).
                    'type': 'NodePort',
                    'nodePort': self.k8s_grafana_external_port,
                    'port': self.k8s_grafana_external_port,
                },
                'grafana.ini': {
                    'users': {'viewers_can_edit': True},
                    'auth': {'disable_login_form': True, 'disable_signout_menu': True},
                    'auth.anonymous': {'enabled': True, 'org_role': 'Editor'},
                },
            }
            helm_values.set('grafana', grafana_values)
            LOGGER.debug(f"Monitoring helm chart values are following: {helm_values.as_dict()}")

            repo_name = "prometheus-community"
            source_chart_name = f"{repo_name}/kube-prometheus-stack"
            LOGGER.info(f"Install {source_chart_name} helm chart")
            self.kubectl(f'create namespace {namespace}', ignore_status=True)
            self.helm(f'repo add {repo_name} https://prometheus-community.github.io/helm-charts')
            self.helm('repo update')
            LOGGER.debug(self.helm_install(
                target_chart_name="monitoring",
                source_chart_name=source_chart_name,
                use_devel=False,
                values=helm_values,
                namespace=namespace,
            ))

            LOGGER.info("Install scylla-monitoring dashboards and monitoring services for scylla")
            self.apply_file(os.path.join(scylla_operator_dir, "examples", "common", "monitoring",
                                         "scylla-service-monitor.yaml"))
            self.kubectl(
                f'create configmap scylla-dashboards --from-file={scylla_monitoring_dir}/grafana/build/ver_4.3',
                namespace=namespace)
            self.kubectl(
                "patch configmap scylla-dashboards -p '{\"metadata\":{\"labels\":{\"grafana_dashboard\": \"1\"}}}'",
                namespace=namespace)

            if is_manager_deployed:
                LOGGER.info("Install monitoring services for scylla-manager")
                self.apply_file(os.path.join(scylla_operator_dir, "examples", "common", "monitoring",
                                             "scylla-manager-service-monitor.yaml"))
                self.kubectl(
                    f'create configmap scylla-manager-dashboards '
                    f'--from-file={scylla_monitoring_dir}/grafana/build/manager_2.2',
                    namespace=namespace)
                self.kubectl(
                    "patch configmap scylla-manager-dashboards "
                    "-p '{\"metadata\":{\"labels\":{\"grafana_dashboard\": \"1\"}}}'",
                    namespace=namespace)
        time.sleep(10)
        self.check_k8s_monitoring_cluster_health(namespace=namespace)
        LOGGER.info("K8S Prometheus is available at "
                    f"{self.k8s_monitoring_node_ip}:{self.k8s_prometheus_external_port}")
        LOGGER.info("K8S Grafana is available at "
                    f"{self.k8s_monitoring_node_ip}:{self.k8s_grafana_external_port}")

    @property
    def k8s_monitoring_node_ip(self):
        for ip_type in ("ExternalIP", "InternalIP"):
            cmd = (
                f"get node --no-headers "
                f"-l {self.POOL_LABEL_NAME}={self.MONITORING_POOL_NAME} "
                "-o jsonpath='{.items[*].status.addresses[?(@.type==\"" + ip_type + "\")].address}'"
            )
            if ip := self.kubectl(cmd, namespace="monitoring").stdout.strip():
                return ip
        # Must not be reached but exists for safety of code logic
        return "no_ip_detected"

    @property
    def k8s_prometheus_external_port(self) -> int:
        # NOTE: '30090' is node's port that redirects traffic to pod's port 9090
        return 30090

    @property
    def k8s_grafana_external_port(self) -> int:
        # NOTE: '30000' is node's port that redirects traffic to pod's port 3000
        return 30000

    def check_k8s_monitoring_cluster_health(self, namespace: str):
        LOGGER.info("Check the monitoring cluster")
        self.kubectl("wait --timeout=15m --all --for=condition=Ready pod", timeout=1000, namespace=namespace)
        if self.USE_MONITORING_EXPOSE_SERVICE:
            LOGGER.info("Expose ports for prometheus of the monitoring cluster")
            self.k8s_monitoring_prometheus_expose_service = PortExposeService(
                name='prometheus-expose-ports-service',
                namespace='monitoring',
                selector_key='operator.prometheus.io/name',
                selector_value='monitoring-kube-prometheus-prometheus',
                core_v1_api=self.k8s_core_v1_api,
                resolver=self.resolve_dns_to_ip
            )
            self.k8s_monitoring_prometheus_expose_service.deploy()

        self.kubectl("get statefulset", namespace=namespace)
        self.kubectl("get pods", namespace=namespace)

    @log_run_info
    def gather_k8s_logs(self) -> None:
        # NOTE: reuse data where possible to minimize spent time due to API limiter restrictions
        LOGGER.info("K8S-LOGS: starting logs gathering")
        logdir = Path(self.logdir)
        self.kubectl(f"version > {logdir / 'kubectl.version'} 2>&1")

        # Gather cluster-scoped resources info
        LOGGER.info("K8S-LOGS: gathering cluster scoped resources")
        cluster_scope_dir = "cluster-scoped-resources"
        os.makedirs(logdir / cluster_scope_dir, exist_ok=True)
        for resource_type in self.kubectl(
                "api-resources --namespaced=false --verbs=list -o name").stdout.split():
            for output_format in ("yaml", "wide"):
                logfile = logdir / cluster_scope_dir / f"{resource_type}.{output_format}"
                self.kubectl(f"get {resource_type} -o {output_format} > {logfile}")
        self.kubectl(f"describe nodes > {logdir / cluster_scope_dir / 'nodes.desc'}")

        # Read all the namespaces from already saved file
        with open(logdir / cluster_scope_dir / "namespaces.wide", mode="r") as f:
            # Reverse order of namespaces because preferred once are there
            namespaces = [n.split()[0] for n in f.readlines()[1:]][::-1]

        # Gather namespace-scoped resources info
        LOGGER.info("K8S-LOGS: gathering namespace scoped resources. "
                    f"list of namespaces: {', '.join(namespaces)}")
        os.makedirs(logdir / "namespaces", exist_ok=True)
        namespaced_resource_types = self.kubectl(
            "api-resources --namespaced=true --verbs=get,list -o name").stdout.split()
        for resource_type in namespaced_resource_types:
            LOGGER.info(f"K8S-LOGS: gathering '{resource_type}' resources")
            logfile = logdir / "namespaces" / f"{resource_type}.{output_format}"
            resources_wide = self.kubectl(
                f"get {resource_type} -A -o wide 2>&1 | tee {logfile}").stdout
            if resource_type.startswith("events"):
                # NOTE: skip both kinds on 'events' available in k8s
                continue
            for namespace in namespaces:
                if not re.search(f"\n{namespace} ", resources_wide):
                    # NOTE: move to the next namespace because such resources are absent here
                    continue
                LOGGER.info(
                    f"K8S-LOGS: gathering '{resource_type}' resources in the '{namespace}' "
                    "namespace")
                resource_dir = logdir / "namespaces" / namespace / resource_type
                os.makedirs(resource_dir, exist_ok=True)
                for res in resources_wide.split("\n"):
                    if not re.match(f"{namespace} ", res):
                        continue
                    res = res.split()[1]
                    logfile = resource_dir / f"{res}.yaml"
                    res_stdout = self.kubectl(
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
                        logfile = resource_dir / res / f"{container_name}.log"
                        # NOTE: ignore status because it may fail when pod is not ready/running
                        self.kubectl(f"logs pod/{res} -c={container_name} > {logfile}",
                                     namespace=namespace, ignore_status=True)

    @log_run_info
    def stop_k8s_task_threads(self, timeout=10):
        LOGGER.info("Stop k8s task threads")
        if self._scylla_manager_journal_thread:
            self._scylla_manager_journal_thread.stop(timeout)
        if self._cert_manager_journal_thread:
            self._cert_manager_journal_thread.stop(timeout)
        if self._scylla_operator_log_monitor_thread:
            self._scylla_operator_log_monitor_thread.stop()
        if self._scylla_operator_journal_thread:
            self._scylla_operator_journal_thread.stop(timeout)
        if self._scylla_cluster_events_thread:
            self._scylla_cluster_events_thread.stop(timeout)

    @property
    def minio_pod(self) -> Resource:
        for pod in KubernetesOps.list_pods(self, namespace=MINIO_NAMESPACE):
            found = False
            for container in pod.spec.containers:
                if any([portRec for portRec in container.ports
                        if hasattr(portRec, 'container_port') and str(portRec.container_port) == '9000']):
                    found = True
                    break
            if found:
                return pod
        raise RuntimeError("Can't find minio pod")

    @property
    def minio_ip_address(self) -> str:
        return self.minio_pod.status.pod_ip

    @property
    def s3_provider_endpoint(self) -> str:
        return f"http://{self.minio_ip_address}:9000"

    @property
    def operator_pod_status(self):
        pods = KubernetesOps.list_pods(self, namespace=SCYLLA_OPERATOR_NAMESPACE)
        return pods[0].status if pods else None

    @cached_property
    def tags(self) -> Dict[str, str]:
        return get_tags_from_params(self.params)

    @cached_property
    def logdir(self) -> str:
        assert '_SCT_TEST_LOGDIR' in os.environ
        logdir = os.path.join(os.environ['_SCT_TEST_LOGDIR'], self.name)
        os.makedirs(logdir, exist_ok=True)
        return logdir

    @property
    def k8s_core_v1_api(self) -> k8s.client.CoreV1Api:
        return KubernetesOps.core_v1_api(self.api_client)

    @property
    def k8s_apps_v1_api(self) -> k8s.client.AppsV1Api:
        return KubernetesOps.apps_v1_api(self.api_client)

    @property
    def k8s_configuration(self) -> k8s.client.Configuration:
        if self.api_call_rate_limiter:
            return self.api_call_rate_limiter.get_k8s_configuration(self)
        return KubernetesOps.create_k8s_configuration(self)

    @property
    def api_client(self) -> k8s.client.ApiClient:
        return self.get_api_client()

    def get_api_client(self) -> k8s.client.ApiClient:
        if self.api_call_rate_limiter:
            return self.api_call_rate_limiter.get_api_client(self.k8s_configuration)
        return KubernetesOps.api_client(self.k8s_configuration)

    @property
    def dynamic_client(self) -> k8s.dynamic.DynamicClient:
        return KubernetesOps.dynamic_client(self.api_client)

    @cached_property
    def scylla_manager_cluster(self) -> 'ManagerPodCluser':
        return ManagerPodCluser(
            k8s_cluster=self,
            namespace=SCYLLA_MANAGER_NAMESPACE,
            container='scylla-manager',
            cluster_prefix='mgr-',
            node_prefix='mgr-node-',
            n_nodes=1
        )

    def create_secret_from_data(self, secret_name: str, namespace: str, data: dict, secret_type: str = 'Opaque'):
        prepared_data = {key: base64.b64encode(json.dumps(value).encode('utf-8')).decode('utf-8')
                         for key, value in data.items()}
        secret = k8s.client.V1Secret(
            'v1',
            prepared_data,
            'Secret',
            {'name': secret_name, 'namespace': namespace},
            type=secret_type)
        self.k8s_core_v1_api.create_namespaced_secret(namespace, secret)

    def update_secret_from_data(self, secret_name: str, namespace: str, data: dict, secret_type: str = 'Opaque'):
        existing = None
        for secret in self.k8s_core_v1_api.list_namespaced_secret(namespace).items:
            if secret.metadata.name == secret_name:
                existing = secret
                break
        if not existing:
            self.create_secret_from_data(
                secret_name=secret_name, namespace=namespace, data=data, secret_type=secret_type)
            return

        for key, value in data.items():
            existing.data[key] = base64.b64encode(json.dumps(value).encode('utf-8')).decode('utf-8')
        self.k8s_core_v1_api.patch_namespaced_secret(secret_name, namespace, existing)

    def create_secret_from_directory(self, secret_name: str, path: str, namespace: str, secret_type: str = 'generic',
                                     only_files: List[str] = None):
        files = [fname for fname in os.listdir(path) if os.path.isfile(os.path.join(path, fname)) and
                 (not only_files or fname in only_files)]
        cmd = f'create secret {secret_type} {secret_name} ' + \
              ' '.join([f'--from-file={fname}={os.path.join(path, fname)}' for fname in files])
        self.kubectl(cmd, namespace=namespace)

    def patch_kubectl_config(self):
        """
        Patched kubectl config so that it will obtain cloud token from cache file
         that is kept update by token update thread
        """
        self.create_kubectl_config()
        self.start_token_update_thread()
        KubernetesOps.patch_kube_config(self.kubectl_token_path)
        wait_for(self.check_if_token_is_valid, timeout=120, throw_exc=True)

    def check_if_token_is_valid(self) -> bool:
        with open(self.kubectl_token_path, mode='r') as token_file:
            return bool(json.load(token_file))

    def start_token_update_thread(self):
        if os.path.exists(self.kubectl_token_path):
            os.unlink(self.kubectl_token_path)
        self._token_update_thread = self.create_token_update_thread()
        self._token_update_thread.start()
        # Wait till GcloudTokenUpdateThread get tokens and dump them to gcloud_token_path
        wait_for(os.path.exists, timeout=30, step=5, text="Wait for gcloud token", throw_exc=True,
                 path=self.kubectl_token_path)

    def stop_token_update_thread(self):
        if self._token_update_thread and self._token_update_thread.is_alive():
            self._token_update_thread.stop()

    def _add_pool(self, pool: CloudK8sNodePool) -> None:
        if pool.name not in self.pools:
            self.pools[pool.name] = pool

    def wait_all_node_pools_to_be_ready(self):
        for node_pool in self.pools.values():
            node_pool.wait_for_nodes_readiness()

    def resolve_dns_to_ip(self, hostname: str, timeout: int = None, step: int = 1) -> str:

        def resolve_ip():
            try:
                return self._resolve_dns_to_ip(hostname)
            except Exception as exc:
                raise RuntimeError("Failed to resolve %s due to the %s", hostname, exc) from None

        if not timeout:
            return resolve_ip()

        return wait_for(resolve_ip, timeout=timeout, step=step, throw_exc=True)

    def _resolve_dns_to_ip(self, hostname: str) -> str:
        if self.USE_POD_RESOLVER:
            return self._resolve_dns_to_ip_via_resolver(hostname)
        return self._resolve_dns_to_ip_directly(hostname)

    def _resolve_dns_to_ip_directly(self, hostname: str) -> str:
        ip_address = socket.gethostbyname(hostname)
        if ip_address == '0.0.0.0':
            raise RuntimeError('Failed to resolve')
        return ip_address

    def _resolve_dns_to_ip_via_resolver(self, hostname: str) -> str:
        return self.dns_resolver.resolve(hostname)

    @cached_property
    def dns_resolver(self):
        resolver = DnsPodResolver(
            k8s_cluster=self,
            pod_name=self.DNS_RESOLVER_POD_NAME,
            pool=self.pools.get(self.AUXILIARY_POOL_NAME, None)
        )
        resolver.deploy()
        return resolver

    @abc.abstractmethod
    def deploy(self):
        pass

    @abc.abstractmethod
    def create_kubectl_config(self):
        pass

    @abc.abstractmethod
    def create_token_update_thread(self) -> TokenUpdateThread:
        pass

    @abc.abstractmethod
    def deploy_node_pool(self, pool: CloudK8sNodePool, wait_till_ready=True) -> None:
        pass


class BasePodContainer(cluster.BaseNode):
    parent_cluster: PodCluster
    expose_ports_service: Optional[PortExposeService] = None
    public_ip_via_service: bool = False

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 5  # minutes
    pod_terminate_timeout = 5  # minutes

    def __init__(self, name: str, parent_cluster: PodCluster, node_prefix: str = "node", node_index: int = 1,
                 base_logdir: Optional[str] = None, dc_idx: int = 0, rack=0):
        self.node_index = node_index
        cluster.BaseNode.__init__(
            self, name=name,
            parent_cluster=parent_cluster,
            base_logdir=base_logdir,
            node_prefix=node_prefix,
            dc_idx=dc_idx,
            rack=rack)

    @cached_property
    def pod_replace_timeout(self) -> int:
        return self.pod_terminate_timeout + self.pod_readiness_timeout

    @staticmethod
    def is_docker() -> bool:
        return True

    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):
        self.remoter = KubernetesCmdRunner(kluster=self.parent_cluster.k8s_cluster,
                                           pod=self.name,
                                           container=self.parent_cluster.container,
                                           namespace=self.parent_cluster.namespace)

    def _init_port_mapping(self):
        pass

    def init(self) -> None:
        if self.public_ip_via_service:
            self.expose_ports()
        super().init()

    @property
    def system_log(self):
        return os.path.join(self.logdir, "system.log")

    @property
    def region(self):
        return self.parent_cluster.k8s_cluster.datacenter[0]  # TODO: find the node and return it's region.

    def start_journal_thread(self):
        self._journal_thread = get_system_logging_thread(logs_transport="kubectl",
                                                         node=self,
                                                         target_log_file=self.system_log)
        if self._journal_thread:
            self.log.info("Use %s as logging daemon", type(self._journal_thread).__name__)
            self._journal_thread.start()
        else:
            TestFrameworkEvent(source=self.__class__.__name__,
                               source_method="start_journal_thread",
                               message="Got no logging daemon by unknown reason").publish()

    def check_spot_termination(self):
        pass

    @property
    def _pod(self):
        pods = KubernetesOps.list_pods(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                       field_selector=f"metadata.name={self.name}")
        return pods[0] if pods else None

    @property
    def _pod_status(self):
        if pod := self._pod:
            return pod.status
        return None

    @property
    def _node(self):
        return KubernetesOps.get_node(self.parent_cluster, self.node_name)

    @property
    def _cluster_ip_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _svc(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _container_status(self):
        pod_status = self._pod_status
        if pod_status:
            return next((x for x in pod_status.container_statuses if x.name == self.parent_cluster.container), None)
        return None

    def _refresh_instance_state(self):
        public_ips = []
        private_ips = []

        if self.expose_ports_service and self.expose_ports_service.is_deployed:
            public_ips.append(self.expose_ports_service.service_ip)

        if cluster_ip_service := self._cluster_ip_service:
            private_ips.append(cluster_ip_service.spec.cluster_ip)

        if pod_status := self._pod_status:
            public_ips.append(pod_status.host_ip)
            private_ips.append(pod_status.pod_ip)
        return (public_ips or [None, ], private_ips or [None, ])

    @property
    def k8s_pod_uid(self) -> str:
        try:
            return str(self._pod.metadata.uid)
        except Exception:
            return ''

    @property
    def k8s_pod_name(self) -> str:
        return str(self._pod.metadata.name)

    def expose_ports(self):
        self.expose_ports_service = PortExposeService(
            name=f'{self.k8s_pod_name}-lbc',
            namespace='scylla',
            selector_value=self.k8s_pod_name,
            core_v1_api=self.parent_cluster.k8s_cluster.k8s_core_v1_api,
            resolver=self.parent_cluster.k8s_cluster.resolve_dns_to_ip,
        )
        self.expose_ports_service.deploy()

    def wait_till_k8s_pod_get_uid(self, timeout: int = None, ignore_uid=None) -> str:
        """
        Wait till pod get any valid uid.
        If ignore_uid is provided it wait till any valid uid different from ignore_uid
        """
        if timeout is None:
            timeout = self.pod_replace_timeout
        wait_for(lambda: self.k8s_pod_uid and self.k8s_pod_uid != ignore_uid, timeout=timeout,
                 text=f"Wait till host {self} get uid")
        return self.k8s_pod_uid

    def wait_for_k8s_node_readiness(self):
        wait_for(self._wait_for_k8s_node_readiness,
                 text=f"Wait for k8s host {self.node_name} to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 step=5,
                 throw_exc=True)

    def _wait_for_k8s_node_readiness(self):
        if self.node_name is None:
            raise RuntimeError(f"Can't find node for pod {self.name}")
        result = self.parent_cluster.k8s_cluster.kubectl(
            f"wait node --timeout={self.pod_readiness_timeout // 3}m --for=condition=Ready {self.node_name}",
            namespace=self.parent_cluster.namespace,
            timeout=self.pod_readiness_timeout // 3 * 60 + 10
        )
        if result.stdout.count('condition met') != 1:
            raise RuntimeError('Node is not reported as ready')
        return True

    def wait_for_pod_to_appear(self):
        wait_for(self._wait_for_pod_to_appear,
                 text="Wait for pod to appear...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_pod_to_appear(self):
        if self._pod is None:
            raise RuntimeError('Pod is not there')
        return True

    def wait_for_pod_readiness(self):
        time.sleep(self.pod_readiness_delay)

        # To make it more informative in worst case scenario it repeat waiting text 5 times
        wait_for(self._wait_for_pod_readiness,
                 text=f"Wait for {self.name} pod to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_pod_readiness(self):
        result = self.parent_cluster.k8s_cluster.kubectl(
            f"wait --timeout={self.pod_readiness_timeout // 3}m --for=condition=Ready pod {self.name}",
            namespace=self.parent_cluster.namespace,
            timeout=self.pod_readiness_timeout // 3 * 60 + 10)
        if result.stdout.count('condition met') != 1:
            raise RuntimeError('Pod is not ready')
        return True

    @property
    def image(self) -> str:
        return self._container_status.image

    def _get_ipv6_ip_address(self):
        self.log.warning("We don't support IPv6 for k8s-* backends")
        return ""

    def restart(self):
        raise NotImplementedError("Not implemented yet")  # TODO: implement this method.

    def hard_reboot(self):
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete pod {self.name} --now',
            namespace='scylla',
            timeout=self.pod_terminate_timeout * 60 + 10)

    def soft_reboot(self):
        # Kubernetes brings pods back to live right after it is deleted
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete pod {self.name} --grace-period={self.pod_terminate_timeout * 60}',
            namespace='scylla',
            timeout=self.pod_terminate_timeout * 60 + 10)

    # On kubernetes there is no stop/start, closest analog of node restart would be soft_restart
    restart = soft_reboot

    @property
    def uptime(self):
        # update -s from inside of docker containers shows docker host uptime
        return datetime.fromtimestamp(int(self.remoter.run('stat -c %Y /proc/1', ignore_status=True).stdout.strip()))

    def start_coredump_thread(self):
        self._coredump_thread = CoredumpExportFileThread(
            self, self._maximum_number_of_cores_to_publish, ['/var/lib/scylla/coredumps'])
        self._coredump_thread.start()

    @cached_property
    def node_name(self) -> str:
        return self._pod.spec.node_name

    @property
    def instance_name(self) -> str:
        return self.node_name

    def terminate_k8s_node(self):
        """
        Delete kubernetes node, which will terminate scylla node that is running on it
        """
        self.parent_cluster.k8s_cluster.kubectl(
            f'delete node {self.node_name} --now',
            timeout=self.pod_terminate_timeout * 60 + 10)

    def terminate_k8s_host(self):
        """
        Terminate kubernetes node via cloud API (like GCE/EC2), which will terminate scylla node that is running on it
        """
        raise NotImplementedError("To be overridden in child class")

    @staticmethod
    def is_kubernetes() -> bool:
        return True


class BaseScyllaPodContainer(BasePodContainer):
    parent_cluster: ScyllaPodCluster

    @contextlib.contextmanager
    def remote_scylla_yaml(self) -> ContextManager:
        """Update scylla.yaml, k8s way

        Scylla Operator handles scylla.yaml updates using ConfigMap resource and we don't need to update it
        manually on each node.  Just collect all required changes to parent_cluster.scylla_yaml dict and if it
        differs from previous one, set parent_cluster.scylla_yaml_update_required flag.  No actual changes done here.
        Need to do cluster rollout restart.

        More details here: https://github.com/scylladb/scylla-operator/blob/master/docs/generic.md#configure-scylla
        """
        with self.parent_cluster.scylla_yaml_lock:
            # TBD: Interfere with remote_cassandra_rackdc_properties, needed to be addressed
            scylla_yaml_copy = deepcopy(self.parent_cluster.scylla_yaml)
            yield self.parent_cluster.scylla_yaml
            if scylla_yaml_copy == self.parent_cluster.scylla_yaml:
                LOGGER.debug("%s: scylla.yaml hasn't been changed", self)
                return
            original = yaml.safe_dump(scylla_yaml_copy).splitlines(keepends=True)
            changed = yaml.safe_dump(self.parent_cluster.scylla_yaml).splitlines(keepends=True)
            diff = "".join(unified_diff(original, changed))
            LOGGER.debug("%s: scylla.yaml requires to be updated with:\n%s", self, diff)
            self.parent_cluster.scylla_yaml_update_required = True

    @cluster.log_run_info
    def start_scylla_server(self, verify_up=True, verify_down=False,
                            timeout=500, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run('supervisorctl start scylla', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

    @cluster.log_run_info
    @retrying(n=3, sleep_time=5, allowed_exceptions=NETWORK_EXCEPTIONS + (CommandTimedOut, ),
              message="Failed to stop scylla.server, retrying...")
    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300,
                           ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('supervisorctl stop scylla',
                         timeout=timeout, ignore_status=ignore_status)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    @property
    def node_name(self) -> str:
        return self._pod.spec.node_name

    def restart_scylla_server(self, verify_up_before=False, verify_up_after=True, timeout=300, ignore_status=False):
        if verify_up_before:
            self.wait_db_up(timeout=timeout)
        self.remoter.run("supervisorctl restart scylla", timeout=timeout)
        if verify_up_after:
            self.wait_db_up(timeout=timeout)

    @cluster.log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=300):
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)

    @property
    def scylla_listen_address(self):
        pod_status = self._pod_status
        return pod_status and pod_status.pod_ip

    def init(self) -> None:
        super().init()
        if self.distro.is_rhel_like:
            self.remoter.sudo("rpm -q iproute || yum install -y iproute")  # need this because of scylladb/scylla#7560
        self.remoter.sudo('mkdir -p /var/lib/scylla/coredumps', ignore_status=True)

    def drain_k8s_node(self):
        """
        Gracefully terminating kubernetes host and return it back to life.
        It terminates scylla node that is running on it
        """
        self.log.info('drain_k8s_node: kubernetes node will be drained, the following is affected :\n' + dedent('''
            GCE instance  -
            K8s node      X  <-
            Scylla Pod    X
            Scylla node   X
            '''))
        k8s_node_name = self.node_name
        self.parent_cluster.k8s_cluster.kubectl(
            f'drain {k8s_node_name} -n scylla --ignore-daemonsets --delete-local-data')
        time.sleep(5)
        self.parent_cluster.k8s_cluster.kubectl(f'uncordon {k8s_node_name}')

    def _restart_node_with_resharding(self, murmur3_partitioner_ignore_msb_bits: int = 12):
        # Change murmur3_partitioner_ignore_msb_bits parameter to cause resharding.
        self.stop_scylla()
        with self.remote_scylla_yaml() as scylla_yml:
            scylla_yml["murmur3_partitioner_ignore_msb_bits"] = murmur3_partitioner_ignore_msb_bits
        self.parent_cluster.update_scylla_config()
        self.soft_reboot()
        search_reshard = self.follow_system_log(patterns=['Reshard', 'Reshap'])
        self.wait_db_up(timeout=self.pod_readiness_timeout * 60)
        return search_reshard

    @property
    def is_seed(self) -> bool:
        try:
            return 'scylla/seed' in self._svc.metadata.labels
        except Exception:
            return False

    @is_seed.setter
    def is_seed(self, value):
        pass

    def mark_to_be_replaced(self, overwrite: bool = False):
        if self.is_seed:
            raise RuntimeError("Scylla-operator does not support seed nodes replacement")
        # Mark pod with label that is going to be picked up by scylla-operator, pod to be removed and reconstructed
        cmd = f'label svc {self.name} scylla/replace=""'
        if overwrite:
            cmd += ' --overwrite'
        self.parent_cluster.k8s_cluster.kubectl(cmd, namespace=self.parent_cluster.namespace, ignore_status=True)

    def wait_for_svc(self):
        wait_for(self._wait_for_svc,
                 text=f"Wait for k8s service {self.name} to be ready...",
                 timeout=self.pod_readiness_timeout * 60,
                 throw_exc=True)

    def _wait_for_svc(self):
        self.parent_cluster.k8s_cluster.kubectl(
            f"get svc {self.name}", namespace=self.parent_cluster.namespace, verbose=False)
        return True

    def refresh_ip_address(self):
        # Invalidate ip address cache
        old_ip_info = (self.public_ip_address, self.private_ip_address)
        self._private_ip_address_cached = self._public_ip_address_cached = self._ipv6_ip_address_cached = None

        if old_ip_info == (self.public_ip_address, self.private_ip_address):
            return

        self._init_port_mapping()

    def fstrim_scylla_disks(self):
        # NOTE: to be able to run 'fstrim' command in a pod, it must have direct device mount and
        # appropriate priviledges.
        # Both requirements are satisfied by 'local-volume-provisioner' pods which provide disks
        # for scylla pods.
        # So, we run this command not on 'scylla' pods but on 'local-volume-provisioner'
        # ones on each K8S node dedicated for scylla pods.
        podname_path_list = self.parent_cluster.k8s_cluster.kubectl(
            "get pod -l app=local-volume-provisioner "
            f"--field-selector spec.nodeName={self.node_name} "
            "-o jsonpath='{range .items[*]}{.metadata.name} {.spec.volumes[?(@.name==\""
            f"{self.parent_cluster.params.get('k8s_scylla_disk_class')}"
            "\")].hostPath.path}{\"\\n\"}'",
            namespace="default").stdout.strip().split("\n")
        if len(podname_path_list) == 0:
            self.log.warning(f"Could not find scylla disks path on '{self.node_name}'")
        podname, path = podname_path_list[0].split()
        self.parent_cluster.k8s_cluster.kubectl(
            f"exec -ti {podname} -- sh -c 'fstrim -v {path}/*'",
            namespace="default")


class PodCluster(cluster.BaseCluster):
    PodContainerClass: Type[BasePodContainer] = BasePodContainer

    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 namespace: str = "default",
                 container: Optional[str] = None,
                 cluster_uuid: Optional[str] = None,
                 cluster_prefix: str = "cluster",
                 node_prefix: str = "node",
                 node_type: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None,
                 node_pool: Optional[dict] = None,
                 ) -> None:
        self.k8s_cluster = k8s_cluster
        self.namespace = namespace
        self.container = container
        self.node_pool = node_pool

        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=k8s_cluster.datacenter,
                         node_type=node_type)

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Namespace: {self.namespace}"

    @property
    def k8s_apps_v1_api(self):
        return self.k8s_cluster.k8s_apps_v1_api

    @property
    def k8s_core_v1_api(self):
        return self.k8s_cluster.k8s_core_v1_api

    @cached_property
    def pool_name(self):
        return self.node_pool.get('name', None)

    @property
    def statefulsets(self):
        return KubernetesOps.list_statefulsets(self.k8s_cluster, namespace=self.namespace)

    def _create_node(self, node_index: int, pod_name: str, dc_idx: int, rack: int) -> BasePodContainer:
        node = self.PodContainerClass(parent_cluster=self,
                                      name=pod_name,
                                      base_logdir=self.logdir,
                                      node_prefix=self.node_prefix,
                                      node_index=node_index,
                                      dc_idx=dc_idx,
                                      rack=rack)
        node.init()
        return node

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:

        # Wait while whole cluster (on all racks) including new nodes are up and running
        self.wait_for_pods_readiness(pods_to_wait=count, total_pods=len(self.nodes) + count)

        # Register new nodes and return whatever was registered
        k8s_pods = KubernetesOps.list_pods(self, namespace=self.namespace)
        nodes = []
        for pod in k8s_pods:
            if not any((x for x in pod.status.container_statuses if x.name == self.container)):
                continue
            is_already_registered = False
            for node in self.nodes:
                if node.name == pod.metadata.name:
                    is_already_registered = True
                    break
            if is_already_registered:
                continue
            # TBD: A rack validation might be needed
            # Register a new node
            node = self._create_node(len(self.nodes), pod.metadata.name, dc_idx, rack)
            nodes.append(node)
            self.nodes.append(node)
        if len(nodes) != count:
            raise RuntimeError(
                f'Requested {count} number of nodes to add, while only {len(nodes)} new nodes where found')
        return nodes

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Derived class must implement 'get_node_ips_param' method!")

    def wait_for_init(self):
        raise NotImplementedError("Derived class must implement 'wait_for_init' method!")

    def get_nodes_reboot_timeout(self, count) -> Union[float, int]:
        """
        Return readiness timeout (in minutes) for case when nodes are restarted
        sums out readiness and terminate timeouts for given nodes
        """
        return count * self.PodContainerClass.pod_readiness_timeout

    @cached_property
    def get_nodes_readiness_delay(self) -> Union[float, int]:
        return self.PodContainerClass.pod_readiness_delay

    def wait_for_pods_readiness(self, pods_to_wait: int, total_pods: int):
        time.sleep(self.get_nodes_readiness_delay)
        KubernetesOps.wait_for_pods_readiness(
            kluster=self.k8s_cluster,
            total_pods=total_pods,
            readiness_timeout=self.get_nodes_reboot_timeout(pods_to_wait),
            namespace=self.namespace
        )

    def expose_ports(self, nodes=None):
        if nodes is None:
            nodes = self.nodes
        for node in nodes:
            node.expose_ports()


class ScyllaPodCluster(cluster.BaseScyllaCluster, PodCluster):
    NODE_PREPARE_FILE = None
    node_setup_requires_scylla_restart = False
    node_terminate_methods: List[str] = None

    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 scylla_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None,
                 node_pool: CloudK8sNodePool = None,
                 ) -> None:
        k8s_cluster.deploy_scylla_cluster(
            node_pool=node_pool,
            node_prepare_config=self.NODE_PREPARE_FILE
        )
        self.scylla_yaml_lock = RLock()
        self.scylla_yaml = {}
        self.scylla_yaml_update_required = False
        self.scylla_cluster_name = scylla_cluster_name
        super().__init__(k8s_cluster=k8s_cluster,
                         namespace="scylla",
                         container="scylla",
                         cluster_prefix=cluster.prepend_user_prefix(user_prefix, 'db-cluster'),
                         node_prefix=cluster.prepend_user_prefix(user_prefix, 'db-node'),
                         node_type="scylla-db",
                         n_nodes=n_nodes,
                         params=params,
                         node_pool=node_pool)

    get_scylla_args = cluster_docker.ScyllaDockerCluster.get_scylla_args

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, *_, **__):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)
        if self.scylla_yaml_update_required:
            self.update_scylla_config()
            time.sleep(30)
            self.add_sidecar_injection()
            self.restart_scylla()
            self.scylla_yaml_update_required = False
            self.wait_for_nodes_up_and_normal(nodes=node_list)

    @property
    def _k8s_scylla_cluster_api(self) -> Resource:
        return KubernetesOps.dynamic_api(self.k8s_cluster.dynamic_client,
                                         api_version=SCYLLA_API_VERSION,
                                         kind=SCYLLA_CLUSTER_RESOURCE_KIND)

    def replace_scylla_cluster_value(self, path: str, value: Any) -> Optional[ANY_KUBERNETES_RESOURCE]:
        LOGGER.debug("Replace `%s' with `%s' in %s's spec", path, value, self.scylla_cluster_name)
        return self._k8s_scylla_cluster_api.patch(body=[{"op": "replace", "path": path, "value": value}],
                                                  name=self.scylla_cluster_name,
                                                  namespace=self.namespace,
                                                  content_type=JSON_PATCH_TYPE)

    def get_scylla_cluster_value(self, path: str) -> Optional[ANY_KUBERNETES_RESOURCE]:
        """
        Get scylla cluster value from kubernetes API.
        """
        cluster_data = self._k8s_scylla_cluster_api.get(namespace=self.namespace, name=self.scylla_cluster_name)
        return walk_thru_data(cluster_data, path)

    def get_scylla_cluster_plain_value(self, path: str) -> Union[Dict, List, str, None]:
        """
        Get scylla cluster value from kubernetes API and converts result to basic python data types.
        Use it if you are going to modify the data.
        """
        cluster_data = self._k8s_scylla_cluster_api.get(
            namespace=self.namespace, name=self.scylla_cluster_name).to_dict()
        return walk_thru_data(cluster_data, path)

    def add_scylla_cluster_value(self, path: str, element: Any):
        init = self.get_scylla_cluster_value(path) is None
        if path.endswith('/'):
            path = path[0:-1]
        if init:
            # You can't add to empty array, so you need to replace it
            op = "replace"
            path = path
            value = [element]
        else:
            op = "add"
            path = path + "/-"
            value = element
        self._k8s_scylla_cluster_api.patch(
            body=[
                {
                    "op": op,
                    "path": path,
                    "value": value
                }
            ],
            name=self.scylla_cluster_name,
            namespace=self.namespace,
            content_type=JSON_PATCH_TYPE
        )

    @property
    def scylla_config_map(self):
        try:
            return self.k8s_cluster.k8s_core_v1_api.read_namespaced_config_map(
                name=SCYLLA_CONFIG_NAME, namespace=self.namespace)
        except:
            return None

    @contextlib.contextmanager
    def remote_cassandra_rackdc_properties(self) -> ContextManager:
        """Update cassandra-rackdc.properties, k8s way

        Scylla Operator handles cassandra-rackdc.properties updates using ConfigMap resource
        and we don't need to update it manually on each node.
        Only rollout restart stateful set is needed to get it updated.

        More details here: https://github.com/scylladb/scylla-operator/blob/master/docs/generic.md#configure-scylla
        """
        with self.scylla_yaml_lock:
            scylla_config_map = self.scylla_config_map
            if scylla_config_map is None:
                old_rack_properties = {}
            else:
                old_rack_properties = properties.deserialize(
                    scylla_config_map.data.get('cassandra-rackdc.properties', ""))
            if not old_rack_properties:
                if len(self.nodes) > 1:
                    # If there is not config_map than get properties from the very first node in the cluster
                    with self.nodes[0].remote_cassandra_rackdc_properties() as node_rack_properties:
                        old_rack_properties = node_rack_properties
            new_rack_properties = deepcopy(old_rack_properties)
            yield new_rack_properties
            if new_rack_properties == old_rack_properties:
                LOGGER.debug("%s: cassandra-rackdc.properties hasn't been changed", self)
                return
            original = properties.serialize(old_rack_properties).splitlines(keepends=True)
            changed_bare = properties.serialize(new_rack_properties)
            changed = changed_bare.splitlines(keepends=True)
            diff = "".join(unified_diff(original, changed))
            LOGGER.debug("%s: cassandra-rackdc.properties has been updated:\n%s", self, diff)
            config_map = self.scylla_config_map
            if config_map:
                config_map.data['cassandra-rackdc.properties'] = changed_bare
                self.k8s_cluster.k8s_core_v1_api.patch_namespaced_config_map(
                    name=SCYLLA_CONFIG_NAME,
                    namespace=self.namespace,
                    body=config_map)
            else:
                self.k8s_cluster.k8s_core_v1_api.create_namespaced_config_map(
                    namespace=self.namespace,
                    body=V1ConfigMap(
                        data={'cassandra-rackdc.properties': changed_bare},
                        metadata={'name': SCYLLA_CONFIG_NAME}
                    )
                )
            self.restart_scylla()
            self.wait_for_nodes_up_and_normal()

    @property
    def scylla_cluster_spec(self) -> ResourceField:
        return self.get_scylla_cluster_value('/spec')

    def update_seed_provider(self):
        pass

    def install_scylla_manager(self, node):
        pass

    def node_setup(self, node: BaseScyllaPodContainer, verbose: bool = False, timeout: int = 3600):
        self.get_scylla_version()
        if TestConfig.BACKTRACE_DECODING:
            node.install_scylla_debuginfo()

        if TestConfig.MULTI_REGION:
            node.datacenter_setup(self.datacenter)  # pylint: disable=no-member
        self.node_config_setup(
            node,
            seed_address=','.join(self.seed_nodes_ips),
            endpoint_snitch=self.get_endpoint_snitch(),
        )

    @cached_property
    def scylla_manager_cluster_name(self):
        return f"{self.namespace}/{self.scylla_cluster_name}"

    @property
    def scylla_manager_node(self):
        return self.k8s_cluster.scylla_manager_cluster.nodes[0]

    def get_cluster_manager(self, create_cluster_if_not_exists: bool = False) -> AnyManagerCluster:
        return super().get_cluster_manager(create_cluster_if_not_exists=create_cluster_if_not_exists)

    def create_cluster_manager(self, cluster_name: str, manager_tool=None, host_ip=None):
        self.log.info('Scylla manager should not be manipulated on kubernetes manually')
        self.log.info('Instead of creating new cluster we will wait for 5 minutes till it get registered automatically')
        raise NotImplementedError('Scylla manager should not be manipulated on kubernetes manually')

    def node_config_setup(self,
                          node,
                          seed_address=None,
                          endpoint_snitch=None,
                          murmur3_partitioner_ignore_msb_bits=None,
                          client_encrypt=None):  # pylint: disable=too-many-arguments,invalid-name
        if client_encrypt is None:
            client_encrypt = self.params.get("client_encrypt")

        if client_encrypt:
            raise NotImplementedError("client_encrypt is not supported by k8s-* backends yet")

        if self.get_scylla_args():
            raise NotImplementedError("custom SCYLLA_ARGS is not supported by k8s-* backends yet")

        if self.params.get("server_encrypt"):
            raise NotImplementedError("server_encrypt is not supported by k8s-* backends yet")

        append_scylla_yaml = self.params.get("append_scylla_yaml")

        if append_scylla_yaml:
            unsupported_options = ("system_key_directory", "system_info_encryption", "kmip_hosts:", )
            if any(substr in append_scylla_yaml for substr in unsupported_options):
                raise NotImplementedError(
                    f"{unsupported_options} are not supported in append_scylla_yaml by k8s-* backends yet")

        node.config_setup(enable_exp=self.params.get("experimental"),
                          endpoint_snitch=endpoint_snitch,
                          authenticator=self.params.get("authenticator"),
                          server_encrypt=self.params.get("server_encrypt"),
                          append_scylla_yaml=append_scylla_yaml,
                          hinted_handoff=self.params.get("hinted_handoff"),
                          authorizer=self.params.get("authorizer"),
                          alternator_port=self.params.get("alternator_port"),
                          murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits,
                          alternator_enforce_authorization=self.params.get("alternator_enforce_authorization"),
                          internode_compression=self.params.get("internode_compression"),
                          ldap=self.params.get('use_ldap_authorization'))

    def validate_seeds_on_all_nodes(self):
        pass

    def set_seeds(self, wait_for_timeout=300, first_only=False):
        assert self.nodes, "DB cluster should have at least 1 node"
        self.nodes[0].is_seed = True

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        self._create_k8s_rack_if_not_exists(rack)
        current_members = len(self.get_rack_nodes(rack))
        self.replace_scylla_cluster_value(f"/spec/datacenter/racks/{rack}/members", current_members + count)

        return super().add_nodes(count=count,
                                 ec2_user_data=ec2_user_data,
                                 dc_idx=dc_idx,
                                 rack=rack,
                                 enable_auto_bootstrap=enable_auto_bootstrap)

    def _create_k8s_rack_if_not_exists(self, rack: int):
        if self.get_scylla_cluster_value(f'/spec/datacenter/racks/{rack}') is not None:
            return
        # Create new rack of very first rack of the cluster
        new_rack = self.get_scylla_cluster_plain_value('/spec/datacenter/racks/0')
        new_rack['members'] = 0
        new_rack['name'] = f'{new_rack["name"]}-{rack}'
        self.add_scylla_cluster_value('/spec/datacenter/racks', new_rack)

    def _delete_k8s_rack(self, rack: int):
        racks = self.get_scylla_cluster_plain_value(f'/spec/datacenter/racks/')
        if len(racks) == 1:
            return
        racks.pop(rack)
        self.replace_scylla_cluster_value('/spec/datacenter/racks', racks)

    def terminate_node(self, node: BasePodContainer, scylla_shards=""):
        """Terminate node.

        :param node: 'node' object to be processed.
        :param scylla_shards: expected to be the same as 'node.scylla_shards'.
            Used to avoid remoter calls to the target node.
            Useful when the node is unreachable by SSH on the moment of this method call.
        """
        if node.ip_address not in self.dead_nodes_ip_address_list:
            self.dead_nodes_list.append(DeadNode(
                name=node.name,
                public_ip=node.public_ip_address,
                private_ip=node.private_ip_address,
                ipv6_ip=node.ipv6_ip_address if TestConfig.IP_SSH_CONNECTIONS == "ipv6" else '',
                ip_address=node.ip_address,
                shards=scylla_shards or node.scylla_shards,
                termination_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                terminated_by_nemesis=node.running_nemesis,
            ))
        if node in self.nodes:
            self.nodes.remove(node)
        node.destroy()

    def decommission(self, node):
        rack = node.rack
        rack_nodes = self.get_rack_nodes(rack)
        assert rack_nodes[-1] == node, "Can withdraw the last node only"
        current_members = len(rack_nodes)

        # NOTE: "scylla_shards" property uses remoter calls and we save it's result before
        # the target scylla node gets killed using kubectl command which precedes the target GCE
        # node deletion using "terminate_node" command.
        scylla_shards = node.scylla_shards

        self.replace_scylla_cluster_value(f"/spec/datacenter/racks/{rack}/members", current_members - 1)
        self.k8s_cluster.kubectl(f"wait --timeout={node.pod_terminate_timeout}m --for=delete pod {node.name}",
                                 namespace=self.namespace,
                                 timeout=node.pod_terminate_timeout * 60 + 10)
        self.terminate_node(node, scylla_shards=scylla_shards)
        # TBD: Enable rack deletion after https://github.com/scylladb/scylla-operator/issues/287 is resolved
        # if current_members - 1 == 0:
        #     self.delete_rack(rack)

        if monitors := cluster.TestConfig.tester_obj().monitors:
            monitors.reconfigure_scylla_monitoring()

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.replace_scylla_cluster_value("/spec/version", new_version)
        new_image = f"{self.params.get('docker_image')}:{new_version}"

        if not self.nodes:
            return True

        @timeout(timeout=self.nodes[0].pod_replace_timeout * 2 * 60)
        def wait_till_any_node_get_new_image(nodes_with_old_image: list):
            for node in nodes_with_old_image.copy():
                if node.image == new_image:
                    nodes_with_old_image.remove(node)
                    return True
            time.sleep(self.PodContainerClass.pod_readiness_delay)
            raise RuntimeError('No node was upgraded')

        nodes = self.nodes.copy()
        while nodes:
            wait_till_any_node_get_new_image(nodes)

        self.wait_for_pods_readiness(len(self.nodes), len(self.nodes))

    def update_scylla_config(self):
        with self.scylla_yaml_lock:
            with NamedTemporaryFile("w", delete=False) as tmp:
                tmp.write(yaml.safe_dump(self.scylla_yaml))
                tmp.flush()
                self.k8s_cluster.kubectl_multi_cmd(
                    f'kubectl create configmap {SCYLLA_CONFIG_NAME} --from-file=scylla.yaml={tmp.name} ||'
                    f'kubectl create configmap {SCYLLA_CONFIG_NAME} --from-file=scylla.yaml={tmp.name} -o yaml '
                    '--dry-run=client | kubectl replace -f -',
                    namespace=self.namespace
                )
            os.remove(tmp.name)

    def add_sidecar_injection(self) -> bool:
        result = False
        for statefulset in self.k8s_apps_v1_api.list_namespaced_stateful_set(namespace=self.namespace).items:
            is_owned_by_scylla_cluster = False
            for owner_reference in statefulset.metadata.owner_references:
                if owner_reference.kind == 'ScyllaCluster' and owner_reference.name == self.scylla_cluster_name:
                    is_owned_by_scylla_cluster = True
                    break

            if not is_owned_by_scylla_cluster:
                self.log.debug(f"add_sidecar_injection: statefulset {statefulset.metadata.name} skipped")
                continue

            if any([container for container in statefulset.spec.template.spec.containers if
                    container.name == 'injected-busybox-sidecar']):
                self.log.debug(
                    f"add_sidecar_injection: statefulset {statefulset.metadata.name} sidecar is already injected")
                continue

            result = True
            statefulset.spec.template.spec.containers.insert(
                0,
                V1Container(
                    command=['/bin/sh', '-c', 'while true; do sleep 900 ; done'],
                    image='busybox:1.32.0',
                    name='injected-busybox-sidecar',
                    resources=V1ResourceRequirements(
                        limits={
                            'cpu': f"{int(SIDECAR_CONTAINERS_RESOURCES['cpu'] * 1000)}m",
                            'memory': f"{int(SIDECAR_CONTAINERS_RESOURCES['memory'] * 1000)}Mi",
                        },
                        requests={
                            'cpu': f"{int(SIDECAR_CONTAINERS_RESOURCES['cpu'] * 1000)}m",
                            'memory': f"{int(SIDECAR_CONTAINERS_RESOURCES['memory'] * 1000)}Mi",
                        }
                    )
                )
            )

            self.k8s_apps_v1_api.patch_namespaced_stateful_set(
                statefulset.metadata.name, self.namespace,
                {
                    'spec': {
                        'template': {
                            'spec': {
                                'containers': statefulset.spec.template.spec.containers
                            }
                        }
                    }
                }
            )
            self.log.info(f"add_sidecar_injection: statefulset {statefulset.metadata.name} sidecar has been injected")
        return result

    def check_cluster_health(self):
        if self.params.get('k8s_deploy_monitoring'):
            self._check_kubernetes_monitoring_health()
        super().check_cluster_health()

    def _check_kubernetes_monitoring_health(self):
        self.log.debug('Check kubernetes monitoring health')
        try:
            kubernetes_prometheus = PrometheusDBStats(
                host=self.k8s_cluster.k8s_monitoring_node_ip,
                port=self.k8s_cluster.k8s_prometheus_external_port,
            )
        except Exception as exc:
            ClusterHealthValidatorEvent.MonitoringStatus(
                error=f'Failed to connect to kubernetes prometheus server at '
                      f'{self.k8s_cluster.k8s_monitoring_node_ip}:'
                      f'{self.k8s_cluster.k8s_prometheus_external_port},'
                      f' due to the: \n'
                      ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            ).publish()
        ClusterHealthValidatorEvent.Done(
            message="Kubernetes monitoring health check finished").publish()

    def restart_scylla(self, nodes=None, random_order=False):
        # TODO: add support for the "nodes" param to have compatible logic with
        # other backends.
        self.k8s_cluster.kubectl("rollout restart statefulset", namespace=self.namespace)
        readiness_timeout = self.get_nodes_reboot_timeout(len(self.nodes))
        statefulsets = self.statefulsets
        if random_order:
            random.shuffle(statefulsets)
        for statefulset in statefulsets:
            self.k8s_cluster.kubectl(
                f"rollout status statefulset/{statefulset.metadata.name} "
                f"--watch=true --timeout={readiness_timeout}m",
                namespace=self.namespace,
                timeout=readiness_timeout * 60 + 10)


class ManagerPodCluser(PodCluster):
    def wait_for_pods_readiness(self, pods_to_wait: int, total_pods: int):
        time.sleep(self.get_nodes_readiness_delay)
        KubernetesOps.wait_for_pods_readiness(
            kluster=self.k8s_cluster,
            total_pods=lambda x: x > 1,
            readiness_timeout=self.get_nodes_reboot_timeout(pods_to_wait),
            namespace=self.namespace
        )


class LoaderPodCluster(cluster.BaseLoaderSet, PodCluster):
    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 loader_cluster_config: str,
                 loader_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None,
                 node_pool: CloudK8sNodePool = None,
                 ) -> None:

        self.loader_cluster_config = loader_cluster_config
        self.loader_cluster_name = loader_cluster_name
        self.loader_cluster_created = False

        cluster.BaseLoaderSet.__init__(self, params=params)
        PodCluster.__init__(self,
                            k8s_cluster=k8s_cluster,
                            namespace="sct-loaders",
                            container="cassandra-stress",
                            cluster_prefix=cluster.prepend_user_prefix(user_prefix, "loader-set"),
                            node_prefix=cluster.prepend_user_prefix(user_prefix, "loader-node"),
                            node_type="loader",
                            n_nodes=n_nodes,
                            params=params,
                            node_pool=node_pool,
                            )

    def node_setup(self,
                   node: BasePodContainer,
                   verbose: bool = False,
                   db_node_address: Optional[str] = None,
                   **kwargs) -> None:

        if 'scylla-bench' in self.params.list_of_stress_tools:
            self.install_scylla_bench(node)

        if self.params.get('client_encrypt'):
            node.config_client_encrypt()

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:

        if self.loader_cluster_created:
            raise NotImplementedError("Changing number of nodes in LoaderPodCluster is not supported.")

        self.k8s_cluster.deploy_loaders_cluster(self.loader_cluster_config, node_pool=self.node_pool)
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)
        self.loader_cluster_created = True

        return new_nodes


def get_tags_from_params(params: dict) -> Dict[str, str]:
    behaviors = ['keep', 'keep-on-failure', 'destroy']
    picked_behavior_idx = 2
    for node_type in ['db', 'loader', 'monitor']:
        post_behavior_idx = behaviors.index(params.get(f"post_behavior_{node_type}_nodes").lower())
        picked_behavior_idx = min(post_behavior_idx, picked_behavior_idx)
    picked_behavior = behaviors[picked_behavior_idx]
    return {**TestConfig.common_tags(), "keep_action": "terminate" if picked_behavior == "destroy" else "", }
