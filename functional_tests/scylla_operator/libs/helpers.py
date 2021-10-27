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
from enum import Enum
import yaml

from kubernetes.client import exceptions as k8s_exceptions

from sdcm.cluster_k8s import SCYLLA_NAMESPACE, ScyllaPodCluster
from sdcm.utils.decorators import log_run_info, retrying
from sdcm.wait import wait_for


class PodStatuses(Enum):
    RUNNING = 'Running'
    CRASH_LOOP_BACK_OFF = 'CrashLoopBackOff'


def get_scylla_sysctl_value(db_cluster: ScyllaPodCluster, sysctl_name: str) -> int:
    sysctls = db_cluster.get_scylla_cluster_plain_value('/spec/sysctls')
    for sysctl in sysctls:
        if sysctl.startswith(f"{sysctl_name}="):
            return int(sysctl.split("=")[-1])
    raise ValueError(f"Cannot find '{sysctl_name}' sysctl")


@log_run_info
@retrying(n=10, sleep_time=2, allowed_exceptions=(k8s_exceptions.ApiException, ),
          message="Failed to set scylla sysctl value...")
def set_scylla_sysctl_value(db_cluster: ScyllaPodCluster, sysctl_name, sysctl_value: str) -> None:
    sysctls = db_cluster.get_scylla_cluster_plain_value('/spec/sysctls')
    sysctl_to_set = f"{sysctl_name}={sysctl_value}"
    for i, _ in enumerate(sysctls):
        if sysctls[i].startswith(f"{sysctl_name}="):
            sysctls[i] = sysctl_to_set
            break
    else:
        sysctls.append(sysctl_to_set)
    db_cluster.replace_scylla_cluster_value("/spec/sysctls", sysctls)


def get_orphaned_services(db_cluster):
    pod_names = scylla_pod_names(db_cluster)
    services_names = scylla_services_names(db_cluster)
    return list(set(services_names) - set(pod_names))


def get_pods_without_probe(db_cluster: ScyllaPodCluster,
                           probe_type: str, selector: str, container_name: str) -> list:
    pods = db_cluster.k8s_cluster.kubectl(f'get pods -A -l "{selector}" -o yaml')
    pods_without_probes = []
    for pod in yaml.safe_load(pods.stdout)["items"]:
        for container in pod.get("spec", {}).get("containers", []):
            if container['name'] == container_name and not container.get(probe_type):
                pods_without_probes.append(
                    f"pod: {pod['metadata']['name']}, container: {container['name']}")
    return pods_without_probes


def scylla_pod_names(db_cluster: ScyllaPodCluster) -> list:
    pods = db_cluster.k8s_cluster.kubectl(
        f"get pods -n {SCYLLA_NAMESPACE} --no-headers "
        f"-l scylla/cluster={db_cluster.params.get('k8s_scylla_cluster_name')} "
        f"-o=custom-columns='NAME:.metadata.name'")
    return pods.stdout.split()


def scylla_services_names(db_cluster: ScyllaPodCluster) -> list:
    scylla_cluster_name = db_cluster.params.get('k8s_scylla_cluster_name')
    services = db_cluster.k8s_cluster.kubectl(
        f"get svc -n {SCYLLA_NAMESPACE} --no-headers "
        f"-l scylla/cluster={scylla_cluster_name} "
        f"-o=custom-columns='NAME:.metadata.name'")
    return [name for name in services.stdout.split()
            if name not in ('NAME', f"{scylla_cluster_name}-client")]


def wait_for_resource_absence(db_cluster: ScyllaPodCluster,
                              resource_type: str, resource_name: str,
                              step: int = 2, timeout: int = 60) -> None:
    def resource_is_absent() -> bool:
        all_resources = db_cluster.k8s_cluster.kubectl(
            f"get {resource_type} -o=custom-columns=:.metadata.name",
            namespace=SCYLLA_NAMESPACE,
        ).stdout.split()
        return resource_name not in all_resources

    wait_for(resource_is_absent, step=step, timeout=timeout, throw_exc=True,
             text=f"Waiting for the '{resource_name}' {resource_type} be deleted")


def get_pods_and_statuses(db_cluster: ScyllaPodCluster, namespace: str, label: str = None):
    pods = db_cluster.k8s_cluster.kubectl(f"get pods {'-l ' + label if label else ''} -o yaml", namespace=namespace)
    return [{"name": pod["metadata"]["name"], "status": pod["status"]["phase"]} for pod in
            yaml.safe_load(pods.stdout)["items"] if pod]


def get_pod_storage_capacity(db_cluster: ScyllaPodCluster, namespace: str, pod_name: str = None, label: str = None):
    pods_storage_capacity = []
    label = " -l " + label if label else ''
    persistent_volume_info = db_cluster.k8s_cluster.kubectl(f"get pvc {label} -o yaml", namespace=namespace)
    for pod in yaml.safe_load(persistent_volume_info.stdout)["items"]:
        if pod_name and pod_name not in pod["metadata"]["name"]:
            continue

        pods_storage_capacity.append({"name": pod["metadata"]["name"],
                                      "capacity": pod["status"]["capacity"]["storage"]})

    return pods_storage_capacity
