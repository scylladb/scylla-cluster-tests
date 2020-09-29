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
import logging
import contextlib
from datetime import datetime
from copy import deepcopy
from typing import Optional, Union, List, Dict, Any, ContextManager, Type
from difflib import unified_diff
from tempfile import NamedTemporaryFile
from functools import cached_property, partialmethod
from threading import RLock

import yaml
from kubernetes.dynamic.resource import Resource, ResourceField, ResourceInstance

from sdcm import cluster, cluster_docker
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.coredump import CoredumpExportFileThread
from sdcm.sct_config import sct_abs_path
from sdcm.sct_events import TestFrameworkEvent
from sdcm.utils.k8s import KubernetesOps, JSON_PATCH_TYPE
from sdcm.utils.decorators import log_run_info, timeout
from sdcm.utils.remote_logger import get_system_logging_thread, CertManagerLogger, ScyllaOperatorLogger
from sdcm.cluster_k8s.operator_monitoring import ScyllaOperatorLogMonitoring, ScyllaOperatorStatusMonitoring


SCYLLA_OPERATOR_CONFIG = sct_abs_path("sdcm/k8s_configs/operator.yaml")
SCYLLA_API_VERSION = "scylla.scylladb.com/v1alpha1"
SCYLLA_CLUSTER_RESOURCE_KIND = "Cluster"
SCYLLA_POD_READINESS_DELAY = 30  # seconds
SCYLLA_POD_READINESS_TIMEOUT = 5  # minutes
SCYLLA_POD_TERMINATE_TIMEOUT = 30  # minutes

LOGGER = logging.getLogger(__name__)


class KubernetesCluster:  # pylint: disable=too-few-public-methods
    datacenter = ()
    _cert_manager_journal_thread: Optional[CertManagerLogger] = None
    _scylla_operator_journal_thread: Optional[ScyllaOperatorLogger] = None
    _scylla_operator_log_monitor_thread: Optional[ScyllaOperatorLogMonitoring] = None
    _scylla_operator_status_monitor_thread: Optional[ScyllaOperatorStatusMonitoring] = None

    @property
    def k8s_server_url(self) -> Optional[str]:
        return None

    kubectl_cmd = partialmethod(KubernetesOps.kubectl_cmd)
    kubectl = partialmethod(KubernetesOps.kubectl)
    apply_file = partialmethod(KubernetesOps.apply_file)

    @cached_property
    def helm(self):
        return cluster.Setup.tester_obj().localhost.helm

    @property
    def cert_manager_log(self) -> str:
        return os.path.join(self.logdir, "cert_manager.log")

    def start_cert_manager_journal_thread(self) -> None:
        self._cert_manager_journal_thread = CertManagerLogger(self, self.cert_manager_log)
        self._cert_manager_journal_thread.start()

    @log_run_info
    def deploy_cert_manager(self) -> None:
        LOGGER.info("Deploy cert-manager")
        self.kubectl("create namespace cert-manager")
        LOGGER.debug(self.helm("repo add jetstack https://charts.jetstack.io"))
        LOGGER.debug(self.helm(f"install cert-manager jetstack/cert-manager "
                               f"--version v{self.params.get('k8s_cert_manager_version')} --set installCRDs=true",
                               namespace="cert-manager", k8s_server_url=self.k8s_server_url))
        time.sleep(10)
        self.kubectl("wait --timeout=1m --all --for=condition=Ready pod", namespace="cert-manager")
        self.start_cert_manager_journal_thread()

    @property
    def scylla_operator_log(self) -> str:
        return os.path.join(self.logdir, "scylla_operator.log")

    def start_scylla_operator_journal_thread(self) -> None:
        self._scylla_operator_journal_thread = ScyllaOperatorLogger(self, self.scylla_operator_log)
        self._scylla_operator_journal_thread.start()
        self._scylla_operator_log_monitor_thread = ScyllaOperatorLogMonitoring(self)
        self._scylla_operator_log_monitor_thread.start()
        self._scylla_operator_status_monitor_thread = ScyllaOperatorStatusMonitoring(self)
        self._scylla_operator_status_monitor_thread.start()

    @log_run_info
    def deploy_scylla_operator(self) -> None:
        LOGGER.info("Deploy Scylla Operator")
        self.apply_file(SCYLLA_OPERATOR_CONFIG)
        time.sleep(10)
        self.kubectl("wait --timeout=1m --all --for=condition=Ready pod", namespace="scylla-operator-system")
        self.start_scylla_operator_journal_thread()

    @log_run_info
    def deploy_scylla_cluster(self, config: str) -> None:
        LOGGER.info("Create and initialize a Scylla cluster")
        self.apply_file(config)

        LOGGER.debug("Check Scylla cluster")
        self.kubectl("get clusters.scylla.scylladb.com", namespace="scylla")
        self.kubectl("get pods", namespace="scylla")

    @log_run_info
    def stop_k8s_task_threads(self, timeout=10):
        LOGGER.info("Stop k8s task threads")
        if self._cert_manager_journal_thread:
            self._cert_manager_journal_thread.stop(timeout)
        if self._scylla_operator_log_monitor_thread:
            self._scylla_operator_log_monitor_thread.stop()
        if self._scylla_operator_status_monitor_thread:
            self._scylla_operator_status_monitor_thread.stop()
        if self._scylla_operator_journal_thread:
            self._scylla_operator_journal_thread.stop(timeout)

    @property
    def operator_pod_status(self):
        pods = KubernetesOps.list_pods(self, namespace='scylla-operator-system')
        return pods[0].status if pods else None


class BasePodContainer(cluster.BaseNode):
    parent_cluster: 'PodCluster'

    def __init__(self, name: str, parent_cluster: "PodCluster", node_prefix: str = "node", node_index: int = 1,
                 base_logdir: Optional[str] = None, dc_idx: int = 0):
        self.node_index = node_index
        super().__init__(name=name,
                         parent_cluster=parent_cluster,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix,
                         dc_idx=dc_idx)

    def init(self) -> None:
        super().init()
        self.remoter.run('mkdir -p /var/lib/scylla/coredumps', ignore_status=True)

    @staticmethod
    def is_docker() -> bool:
        return True

    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _init_remoter(self, ssh_login_info):
        self.remoter = KubernetesCmdRunner(pod=self.name,
                                           container=self.parent_cluster.container,
                                           namespace=self.parent_cluster.namespace,
                                           k8s_server_url=self.parent_cluster.k8s_cluster.k8s_server_url)

    def _init_port_mapping(self):
        pass

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
    def scylla_listen_address(self):
        pod_status = self._pod_status
        return pod_status and pod_status.pod_ip

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
    def _cluster_ip_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}")
        return services[0] if services else None

    @property
    def _loadbalancer_service(self):
        services = KubernetesOps.list_services(self.parent_cluster, namespace=self.parent_cluster.namespace,
                                               field_selector=f"metadata.name={self.name}-loadbalancer")
        return services[0] if services else None

    @property
    def _container_status(self):
        pod_status = self._pod_status
        if pod_status:
            return next((x for x in pod_status.container_statuses if x.name == self.parent_cluster.container), None)
        return None

    def _refresh_instance_state(self):
        cluster_ip_service = self._cluster_ip_service
        cluster_ip = cluster_ip_service and cluster_ip_service.spec.cluster_ip
        pod_status = self._pod_status
        return ([cluster_ip, ], [cluster_ip, pod_status and pod_status.pod_ip, ], )

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run("supervisorctl start scylla", timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

    @cluster.log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300, ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('supervisorctl stop scylla', timeout=timeout)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

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
    def image(self) -> str:
        return self._container_status.image

    @property
    def ipv6_ip_address(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError("Not implemented yet")  # TODO: implement this method.

    def hard_reboot(self):
        self.parent_cluster.k8s_cluster.kubectl(f'delete pod {self.name} --now', namespace='scylla')

    def soft_reboot(self):
        # Kubernetes brings pods back to live right after it is deleted
        self.parent_cluster.k8s_cluster.kubectl(f'delete pod {self.name} --grace-period=300', namespace='scylla')

    # On kubernetes there is no stop/start, closest analog of node restart would be soft_restart
    restart = soft_reboot

    @property
    def uptime(self):
        # update -s from inside of docker containers shows docker host uptime
        return datetime.fromtimestamp(int(self.remoter.run('stat -c %Y /proc/1', ignore_status=True).stdout.strip()))

    @contextlib.contextmanager
    def remote_scylla_yaml(self, path: str = cluster.SCYLLA_YAML_PATH) -> ContextManager:
        """Update scylla.yaml, k8s way

        Scylla Operator handles scylla.yaml updates using ConfigMap resource and we don't need to update it
        manually on each node.  Just collect all required changes to parent_cluster.scylla_yaml dict and if it
        differs from previous one, set parent_cluster.scylla_yaml_update_required flag.  No actual changes done here.
        Need to do cluster rollout restart.

        More details here: https://github.com/scylladb/scylla-operator/blob/master/docs/generic.md#configure-scylla
        """
        with self.parent_cluster.scylla_yaml_lock:
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

    def start_coredump_thread(self):
        self._coredump_thread = CoredumpExportFileThread(
            self, self._maximum_number_of_cores_to_publish, ['/var/lib/scylla/coredumps'])
        self._coredump_thread.start()


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
                 params: Optional[dict] = None) -> None:
        self.k8s_cluster = k8s_cluster
        self.namespace = namespace
        self.container = container

        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=k8s_cluster.datacenter,
                         node_type=node_type)

    @cached_property
    def _k8s_core_v1_api(self):
        return KubernetesOps.core_v1_api(self.k8s_cluster)

    def _create_node(self, node_index: int, pod_name: str) -> BasePodContainer:
        node = self.PodContainerClass(parent_cluster=self,
                                      name=pod_name,
                                      base_logdir=self.logdir,
                                      node_prefix=self.node_prefix,
                                      node_index=node_index)
        node.init()
        return node

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        pods = KubernetesOps.list_pods(self, namespace=self.namespace)

        self.log.debug("Numbers of pods: %s", len(pods))
        assert count <= len(pods), "You can't alter number of pods here"

        nodes = []
        for node_index, pod in enumerate(pods[-count:]):
            node = self._create_node(node_index, pod.metadata.name)
            nodes.append(node)
            self.nodes.append(node)

        return nodes

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Derived class must implement 'get_node_ips_param' method!")

    def wait_for_init(self):
        raise NotImplementedError("Derived class must implement 'wait_for_init' method!")


class ScyllaPodCluster(cluster.BaseScyllaCluster, PodCluster):
    node_setup_requires_scylla_restart = False

    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 scylla_cluster_config: str,
                 scylla_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None) -> None:
        k8s_cluster.deploy_scylla_cluster(scylla_cluster_config)
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
                         params=params)

    get_scylla_args = cluster_docker.ScyllaDockerCluster.get_scylla_args

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, *_, **__):
        node_list = node_list if node_list else self.nodes
        self.wait_for_nodes_up_and_normal(nodes=node_list)
        if self.update_scylla_config():
            self.wait_for_nodes_up_and_normal(nodes=node_list)

    @cached_property
    def _k8s_scylla_cluster_api(self) -> Resource:
        return KubernetesOps.dynamic_api(self.k8s_cluster,
                                         api_version=SCYLLA_API_VERSION,
                                         kind=SCYLLA_CLUSTER_RESOURCE_KIND)

    def replace_scylla_cluster_value(self, path: str, value: Any) -> ResourceInstance:
        LOGGER.debug("Replace `%s' with `%s' in %s's spec", path, value, self.scylla_cluster_name)
        return self._k8s_scylla_cluster_api.patch(body=[{"op": "replace", "path": path, "value": value}],
                                                  name=self.scylla_cluster_name,
                                                  namespace=self.namespace,
                                                  content_type=JSON_PATCH_TYPE)

    @property
    def scylla_cluster_spec(self) -> ResourceField:
        return self._k8s_scylla_cluster_api.get(namespace=self.namespace, name=self.scylla_cluster_name).spec

    def update_seed_provider(self):
        pass

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
                          internode_compression=self.params.get("internode_compression"))

    def validate_seeds_on_all_nodes(self):
        pass

    def set_seeds(self, wait_for_timeout=300, first_only=False):
        assert self.nodes, "DB cluster should have at least 1 node"
        self.nodes[0].is_seed = True

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[BasePodContainer]:
        current_members = self.scylla_cluster_spec.datacenter.racks[0].members
        self.replace_scylla_cluster_value("/spec/datacenter/racks/0/members", current_members+count)
        self.wait_for_pods_readiness(count)
        return super().add_nodes(count=count,
                                 ec2_user_data=ec2_user_data,
                                 dc_idx=dc_idx,
                                 enable_auto_bootstrap=enable_auto_bootstrap)

    def terminate_node(self, node):
        assert self.nodes[-1] == node, "Can withdraw the last node only"
        current_members = self.scylla_cluster_spec.datacenter.racks[0].members
        self.replace_scylla_cluster_value("/spec/datacenter/racks/0/members", current_members-1)
        super().terminate_node(node)
        self.k8s_cluster.kubectl(f"wait --timeout={SCYLLA_POD_TERMINATE_TIMEOUT}m --for=delete pod {node.name}",
                                 namespace=self.namespace,
                                 timeout=SCYLLA_POD_TERMINATE_TIMEOUT*60+10)

    def decommission(self, node):
        self.terminate_node(node)

        if monitors := cluster.Setup.tester_obj().monitors:
            monitors.reconfigure_scylla_monitoring()

    @timeout(message="Wait for pod(s) to be ready...", timeout=600)
    def wait_for_pods_readiness(self, count: Optional[int] = None):
        if count is None:
            count = len(self.nodes)
        for _ in range(count):
            time.sleep(SCYLLA_POD_READINESS_DELAY)
            self.k8s_cluster.kubectl(f"wait --timeout={SCYLLA_POD_READINESS_TIMEOUT}m --all --for=condition=Ready pod",
                                     namespace=self.namespace,
                                     timeout=SCYLLA_POD_READINESS_TIMEOUT*60+10)

    def upgrade_scylla_cluster(self, new_version: str) -> None:
        self.replace_scylla_cluster_value("/spec/version", new_version)
        self.rollout_restart()

    def update_scylla_config(self) -> bool:
        with self.scylla_yaml_lock:
            if not self.scylla_yaml_update_required:
                return False
            with NamedTemporaryFile("w", delete=False) as tmp:
                tmp.write(yaml.safe_dump(self.scylla_yaml))
            self.k8s_cluster.kubectl(f"create configmap scylla-config --from-file=scylla.yaml={tmp.name}",
                                     namespace=self.namespace)
            time.sleep(30)
            self.rollout_restart()
            os.remove(tmp.name)
            self.scylla_yaml_update_required = False
        return True

    def rollout_restart(self):
        self.k8s_cluster.kubectl("rollout restart statefulset", namespace=self.namespace)
        self.wait_for_pods_readiness()
