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
import logging
import time
from typing import Union
import yaml

from kubernetes.client import exceptions as k8s_exceptions

from sdcm.cluster import (
    DB_LOG_PATTERN_RESHARDING_START,
    DB_LOG_PATTERN_RESHARDING_FINISH,
)
from sdcm.cluster_k8s import (
    SCYLLA_MANAGER_NAMESPACE,
    SCYLLA_NAMESPACE,
    ScyllaPodCluster,
)
from sdcm.utils.decorators import log_run_info, retrying
from sdcm.utils.k8s import HelmValues
from sdcm.wait import wait_for

log = logging.getLogger()


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
        f"get pods -n {db_cluster.namespace} --no-headers "
        f"-l scylla/cluster={db_cluster.params.get('k8s_scylla_cluster_name')} "
        f"-o=custom-columns='NAME:.metadata.name'")
    return pods.stdout.split()


def scylla_services_names(db_cluster: ScyllaPodCluster) -> list:
    scylla_cluster_name = db_cluster.params.get('k8s_scylla_cluster_name')
    services = db_cluster.k8s_cluster.kubectl(
        f"get svc -n {db_cluster.namespace} --no-headers "
        f"-l scylla/cluster={scylla_cluster_name} "
        f"-o=custom-columns='NAME:.metadata.name'")
    return [name for name in services.stdout.split()
            if name not in ('NAME', f"{scylla_cluster_name}-client")]


def wait_for_resource_absence(db_cluster: ScyllaPodCluster,
                              resource_type: str, resource_name: str,
                              namespace: str = SCYLLA_NAMESPACE,
                              step: int = 2, timeout: int = 60) -> None:
    def resource_is_absent() -> bool:
        all_resources = db_cluster.k8s_cluster.kubectl(
            f"get {resource_type} -o=custom-columns=:.metadata.name", namespace=namespace,
        ).stdout.split()
        return not all_resources or resource_name not in all_resources

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


def reinstall_scylla_manager(db_cluster: ScyllaPodCluster, manager_version: str):
    values = HelmValues(yaml.safe_load(db_cluster.k8s_cluster.helm(
        "get values scylla-manager -o yaml", namespace=SCYLLA_MANAGER_NAMESPACE)))
    if values.get("image.tag") != manager_version:
        log.info(
            "Scylla Manager '%s' is going to be installed instead of the '%s' one.",
            manager_version, values.get("image.tag"))
        values.set('image.tag', manager_version)
        values.set('scylla.agentImage.tag', manager_version)

        # Delete the current scylla-manager completely because otherwise
        # we won't be able to migrate DB state from a newer version to older one.
        db_cluster.k8s_cluster.helm(
            "uninstall scylla-manager --wait", namespace=SCYLLA_MANAGER_NAMESPACE)
        wait_for_resource_absence(
            db_cluster=db_cluster, step=1, timeout=120, resource_name="",
            resource_type="pod", namespace=SCYLLA_MANAGER_NAMESPACE)
        db_cluster.k8s_cluster.kubectl(
            "delete pvc --all --wait=true", namespace=SCYLLA_MANAGER_NAMESPACE)

        # Deploy Scylla Manager of the specified version
        log.debug(db_cluster.k8s_cluster.helm_install(
            target_chart_name="scylla-manager",
            source_chart_name="scylla-operator/scylla-manager",
            version=db_cluster.k8s_cluster.scylla_operator_chart_version,
            use_devel=True,
            values=values,
            namespace=SCYLLA_MANAGER_NAMESPACE,
        ))
        time.sleep(5)
        db_cluster.k8s_cluster.kubectl_wait(
            "--all --for=condition=Ready pod", namespace=SCYLLA_MANAGER_NAMESPACE, timeout=600)
        log.info("Scylla Manager '%s' has successfully been installed", manager_version)


def verify_resharding_on_k8s(db_cluster: ScyllaPodCluster, cpus: Union[str, int, float]):
    nodes_data = []
    for node in reversed(db_cluster.nodes):
        liveness_probe_failures = node.follow_system_log(
            patterns=["healthz probe: can't connect to JMX"])
        resharding_start = node.follow_system_log(patterns=[DB_LOG_PATTERN_RESHARDING_START])
        resharding_finish = node.follow_system_log(patterns=[DB_LOG_PATTERN_RESHARDING_FINISH])
        nodes_data.append((node, liveness_probe_failures, resharding_start, resharding_finish))

    log.info(
        "Update the cpu count to '%s' CPUs to make Scylla start "
        "the resharding process on all the nodes 1 by 1", cpus)
    db_cluster.replace_scylla_cluster_value(
        "/spec/datacenter/racks/0/resources", {
            "limits": {
                "cpu": cpus,
                "memory": db_cluster.k8s_cluster.scylla_memory_limit,
            },
            "requests": {
                "cpu": cpus,
                "memory": db_cluster.k8s_cluster.scylla_memory_limit,
            },
        })

    # Wait for the start of the resharding.
    # In K8S it starts from the last node of a rack and then goes to previous ones.
    # One resharding with 100Gb+ may take about 3-4 minutes. So, set 5 minutes timeout per node.
    for node, liveness_probe_failures, resharding_start, resharding_finish in nodes_data:
        assert wait_for(
            func=lambda: list(resharding_start),
            step=1, timeout=600, throw_exc=False,
            text=f"Waiting for the start of resharding on the '{node.name}' node.",
        ), f"Start of resharding hasn't been detected on the '{node.name}' node."
        resharding_started = time.time()
        log.debug("Resharding has been started on the '%s' node.", node.name)

        # Wait for the end of resharding
        assert wait_for(
            func=lambda: list(resharding_finish),
            step=3, timeout=600, throw_exc=False,
            text=f"Waiting for the finish of resharding on the '{node.name}' node.",
        ), f"Finish of the resharding hasn't been detected on the '{node.name}' node."
        log.debug("Resharding has been finished successfully on the '%s' node.", node.name)

        # Calculate the time spent for resharding. We need to have it be bigger than 2minutes
        # because it is the timeout of the liveness probe for Scylla pods.
        resharding_time = time.time() - resharding_started
        if resharding_time < 120:
            log.warning(
                "Resharding was too fast - '%s's (<120s) on the '%s' node",
                resharding_time, node.name)
        else:
            log.info(
                "Resharding has taken '%s's on the '%s' node", resharding_time, node.name)

        # Check that liveness probe didn't report any errors
        # https://github.com/scylladb/scylla-operator/issues/894
        liveness_probe_failures_list = list(liveness_probe_failures)
        assert not liveness_probe_failures_list, (
            f"liveness probe has failures: {liveness_probe_failures_list}")
    log.info("Resharding has successfully ended on whole Scylla cluster.")
