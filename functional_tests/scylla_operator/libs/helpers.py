#!/usr/bin/env python

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
# Copyright (c) 2021 ScyllaDB
# pylint: disable=import-error
import re
from enum import Enum

import yaml

from sdcm.cluster_k8s import SCYLLA_NAMESPACE, SCYLLA_OPERATOR_NAMESPACE, ScyllaPodCluster


class PodStatuses(Enum):
    RUNNING = 'Running'
    CRASH_LOOP_BACK_OFF = 'CrashLoopBackOff'


class ReadLogException(Exception):
    pass


def get_orphaned_services(db_cluster):
    pod_names = scylla_pod_names(db_cluster)
    services_names = scylla_services_names(db_cluster)
    return list(set(services_names) - set(pod_names))


def scylla_pod_names(db_cluster):
    pods = db_cluster.k8s_cluster.kubectl(f"get pods -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                          f"-o=custom-columns='NAME:.metadata.name'")

    return [name for name in pods.stdout.split() if name != 'NAME']


def scylla_services_names(db_cluster):
    services = db_cluster.k8s_cluster.kubectl(f"get svc -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                              f"-o=custom-columns='NAME:.metadata.name'")
    return [name for name in services.stdout.split() if name not in ('NAME', 'sct-cluster-client')]


def rollout_restart(db_cluster: ScyllaPodCluster, namespace: str, deployment: str = None):
    db_cluster.k8s_cluster.kubectl(f"rollout restart deployment -n {namespace} {deployment if deployment else ''}")


def scylla_operator_rollout_restart(db_cluster: ScyllaPodCluster):
    rollout_restart(db_cluster, namespace=SCYLLA_OPERATOR_NAMESPACE)


def get_deployment_rollout_status(db_cluster: ScyllaPodCluster, namespace: str, deployment: str, timeout: str):
    return db_cluster.k8s_cluster.kubectl(f"rollout status deployment -n {namespace} {deployment} --timeout={timeout}")


def wait_for_scylla_operator_rollout_complete(db_cluster: ScyllaPodCluster, timeout: str = '60s'):
    status = []
    for deployment in ['scylla-operator', 'webhook-server']:
        deployment_rollout_status = get_deployment_rollout_status(db_cluster, namespace=SCYLLA_OPERATOR_NAMESPACE,
                                                                  deployment=deployment, timeout=timeout)
        if f"deployment \"{deployment}\" successfully rolled out" not in deployment_rollout_status.stdout:
            status.append(f"{deployment}: {deployment_rollout_status.stdout}")
    return status


def get_namespace_pods(db_cluster, namespace, label):
    return db_cluster.k8s_cluster.kubectl(f"get pods -n {namespace} {'-l ' + label if label else ''} -o yaml")


def scylla_operator_pods_and_statuses(db_cluster):
    pods = get_namespace_pods(db_cluster, namespace=SCYLLA_OPERATOR_NAMESPACE,
                              label='app.kubernetes.io/instance=scylla-operator')

    return [[pod["metadata"]["name"], pod["status"]["phase"]] for pod in yaml.load(pods.stdout)["items"] if pod]


def pods_and_statuses(db_cluster, namespace, label=None):
    pods = get_namespace_pods(db_cluster, namespace=namespace, label=label)
    return [[pod["metadata"]["name"], pod["status"]["phase"]] for pod in yaml.load(pods.stdout)["items"] if pod]


def namespace_persistent_volume(db_cluster, namespace, label=None):
    label = " -l " + label if label else ''
    return db_cluster.k8s_cluster.kubectl(f"get pvc -n {namespace}{label} -o yaml")


def pod_storage_capacity(db_cluster, namespace, pod_name=None, label=None):
    pods_storage_capacity = []
    persistent_volume_info = namespace_persistent_volume(db_cluster, namespace, label)
    for pod in yaml.load(persistent_volume_info.stdout)["items"]:
        if pod_name and pod_name not in pod["metadata"]["name"]:
            continue

        pods_storage_capacity.append([pod["metadata"]["name"], pod["status"]["capacity"]["storage"]])

    return pods_storage_capacity


def scylla_storage_capacity(db_cluster, namespace=SCYLLA_NAMESPACE, pod_name=None):
    return pod_storage_capacity(db_cluster, namespace=namespace, pod_name=pod_name,
                                label="app.kubernetes.io/name=scylla")


def get_container_log(db_cluster, namespace, pod_name, container_name):
    return db_cluster.k8s_cluster.kubectl(f"logs {pod_name} -c {container_name}", namespace=namespace)


def watch_container_log_for_string(db_cluster, search_str, namespace, pod_name, container_name):
    log_text = get_container_log(db_cluster, namespace, pod_name, container_name)
    if log_text.return_code != 0 or log_text.stderr:
        raise ReadLogException(f"Failed to read log of {pod_name}, container {container_name}. "
                               f"Status: {log_text.return_code}. Error: {log_text.stderr}")

    search_pattern = re.compile(search_str, re.IGNORECASE)
    if search_pattern.search(log_text.stdout):
        return True
    return False


def watch_scylla_container_log_for_string(db_cluster, search_str, namespace, pod_name):
    return watch_container_log_for_string(db_cluster, search_str, namespace, pod_name, container_name='scylla')
