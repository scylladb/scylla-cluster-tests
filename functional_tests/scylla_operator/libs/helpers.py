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
import yaml

from sdcm.cluster_k8s import SCYLLA_NAMESPACE, SCYLLA_OPERATOR_NAMESPACE, ScyllaPodCluster
from sdcm.wait import wait_for


def get_scylla_sysctl_value(db_cluster: ScyllaPodCluster, sysctl_name: str) -> int:
    sysctls = db_cluster.get_scylla_cluster_plain_value('/spec/sysctls')
    for sysctl in sysctls:
        if sysctl.startswith(f"{sysctl_name}="):
            return int(sysctl.split("=")[-1])
    raise ValueError(f"Cannot find '{sysctl_name}' sysctl")


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


def scylla_pod_names(db_cluster):
    pods = db_cluster.k8s_cluster.kubectl(f"get pods -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                          f"-o=custom-columns='NAME:.metadata.name'")

    return [name for name in pods.stdout.split() if name != 'NAME']


def scylla_services_names(db_cluster):
    services = db_cluster.k8s_cluster.kubectl(f"get svc -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                              f"-o=custom-columns='NAME:.metadata.name'")
    return [name for name in services.stdout.split() if name not in ('NAME', 'sct-cluster-client')]


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


def scylla_operator_pods_and_statuses(db_cluster):
    pods = db_cluster.k8s_cluster.kubectl(f"get pods -n {SCYLLA_OPERATOR_NAMESPACE} "
                                          f"-l app.kubernetes.io/instance=scylla-operator "
                                          f"-o=custom-columns='NAME:.metadata.name,STATUS:.status.phase' -o yaml")

    return [[pod["metadata"]["name"], pod["status"]["phase"]] for pod in yaml.load(pods.stdout)["items"] if pod]
