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

import logging
import os
import random
import threading
import time
import ssl
import base64
import invoke
import path

import pytest
import yaml
from cassandra.cluster import (
    Cluster,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
)
from cassandra.policies import WhiteListRoundRobinPolicy

from sdcm.cluster_k8s import (
    ScyllaPodCluster,
    SCYLLA_NAMESPACE,
    SCYLLA_MANAGER_NAMESPACE,
    SCYLLA_OPERATOR_NAMESPACE
)
from sdcm.mgmt import TaskStatus
from sdcm.utils.parallel_object import ParallelObject
from sdcm.utils.k8s import (
    convert_cpu_units_to_k8s_value,
    convert_cpu_value_from_k8s_to_units,
    HelmValues,
    KubernetesOps,
)
from sdcm.utils.k8s.chaos_mesh import PodFailureExperiment

from functional_tests.scylla_operator.libs.helpers import (
    get_scylla_sysctl_value,
    get_orphaned_services,
    get_pods_without_probe,
    get_pods_and_statuses,
    get_pod_storage_capacity,
    PodStatuses,
    reinstall_scylla_manager,
    set_scylla_sysctl_value,
    verify_resharding_on_k8s,
    wait_for_resource_absence,
)

log = logging.getLogger()

# TODO: add support for multiDC setups


@pytest.mark.readonly
def test_single_operator_image_tag_is_everywhere(db_cluster):
    expected_operator_tag = db_cluster.k8s_cluster.get_operator_image().split(":")[-1]
    pods_with_wrong_image_tags = []

    # NOTE: operator's image is used in many places. So, walk through all of the related namespaces
    for namespace in (SCYLLA_NAMESPACE, SCYLLA_MANAGER_NAMESPACE, SCYLLA_OPERATOR_NAMESPACE):
        pods = yaml.safe_load(db_cluster.k8s_cluster.kubectl(
            "get pods -o yaml", namespace=namespace).stdout)["items"]
        for pod in pods:
            for container_type in ("c", "initC"):
                for container in pod.get("status", {}).get(f"{container_type}ontainerStatuses", []):
                    image = container["image"].split("/")[-1]
                    if image.startswith("scylla-operator:") and image.split(":")[-1] != expected_operator_tag:
                        pods_with_wrong_image_tags.append({
                            "namespace": namespace,
                            "pod_name": pod["metadata"]["name"],
                            "container_name": container["name"],
                            "image": image,
                        })

    assert not pods_with_wrong_image_tags, (
        f"Found pods that have unexpected scylla-operator image tags.\n"
        f"Expected is '{expected_operator_tag}'.\n"
        f"Pods: {yaml.safe_dump(pods_with_wrong_image_tags, indent=2)}")


@pytest.mark.required_operator("v1.11.0")
def test_deploy_quasi_multidc_db_cluster(db_cluster: ScyllaPodCluster):  # noqa: PLR0914
    """
    Deploy 2 'ScyllaCluster' K8S objects in 2 different namespaces in the single K8S cluster
    and combine them into a single DB cluster.
    Combining is done by using 'externalSeeds' config option and single 'cluster name' value.
    Only PodIPs are used for the connectivity.
    """
    cluster_name, target_chart_name, namespace = ("t-podip-quasi-multidc", ) * 3
    target_chart_name2, namespace2 = (f"{cluster_name}-2", ) * 2
    dc_name, dc_name2 = (f"quasi-dc-{i}" for i in range(1, 3))
    k8s_cluster, kubectl = db_cluster.k8s_cluster, db_cluster.k8s_cluster.kubectl
    operator_version = k8s_cluster.scylla_operator_chart_version
    need_to_collect_logs = True
    logdir = f"{os.path.join(k8s_cluster.logdir, 'test_deploy_quasi_multidc_db_cluster')}"
    values = HelmValues({
        'exposeOptions': {
            'nodeService': {'type': 'Headless'},
            'broadcastOptions': {key: {'type': 'PodIP'} for key in ('nodes', 'clients')},
        },
        'developerMode': True,
        'fullnameOverride': cluster_name,
        'scyllaImage': {
            'repository': k8s_cluster.params.get('docker_image'),
            'tag': k8s_cluster.params.get('scylla_version'),
        },
        'agentImage': {'tag': k8s_cluster.params.get('scylla_mgmt_agent_version')},
        'serviceAccount': {'create': True, 'annotations': {}, 'name': f"{cluster_name}-member"},
        'serviceMonitor': {'create': False},
        'sysctls': ["fs.aio-max-nr=300000000"],
        'datacenter': dc_name,
        'racks': [{
            'name': k8s_cluster.rack_name,
            'members': 3, 'storage': {'capacity': '2Gi'},
            'resources': {
                'requests': {'cpu': '200m', 'memory': "200Mi"},
                'limits': {'cpu': '500m', 'memory': "400Mi"},
            }
        }]
    })

    k8s_cluster.create_namespace(namespace=namespace)
    k8s_cluster.create_scylla_manager_agent_config(namespace=namespace)

    def get_pod_names_and_ips(cluster_name: str, namespace: str):
        pod_names_and_ips = kubectl(
            "get pods --no-headers -o=custom-columns=':.metadata.name,:.status.podIP'"
            f" -l scylla/cluster={cluster_name},app.kubernetes.io/name=scylla",
            namespace=namespace).stdout.split("\n")
        pod_names_and_ips = [row.strip() for row in pod_names_and_ips if row.strip()]
        assert pod_names_and_ips
        assert len(pod_names_and_ips) == 3
        pod_data = {namespace: {}}
        for pod_name_and_ip in pod_names_and_ips:
            pod_name, pod_ip = pod_name_and_ip.split()
            pod_data[namespace][pod_name.strip()] = pod_ip.strip()
            assert pod_ip not in ("", "None"), "Pod IPs were expected to be set"
        return pod_data

    log.info('Deploy first ScyllaCluster')
    try:
        log.debug(k8s_cluster.helm_install(
            target_chart_name=target_chart_name, source_chart_name="scylla-operator/scylla",
            version=operator_version, use_devel=True, values=values, namespace=namespace))
        k8s_cluster.kubectl_wait("--all --for=condition=Ready pod", namespace=namespace, timeout=1200)

        # NOTE: check that Scylla services are headless - no IPs are set
        svc_ips = kubectl(
            "get svc --no-headers -o=custom-columns=':.spec.clusterIP'"
            f" -l scylla/cluster={cluster_name} -l scylla-operator.scylladb.com/scylla-service-type=member",
            namespace=namespace).stdout.split()
        assert svc_ips
        assert len(svc_ips) == 3
        assert all(svc_ip in ('', 'None') for svc_ip in svc_ips), "SVC IPs were expected to be absent"

        # NOTE: read Scylla pods IPs
        pods_data = get_pod_names_and_ips(cluster_name=cluster_name, namespace=namespace)
        try:
            k8s_cluster.create_namespace(namespace=namespace2)
            k8s_cluster.create_scylla_manager_agent_config(namespace=namespace2)

            log.info('Deploy second ScyllaCluster')
            values.set("datacenter", dc_name2)
            values.set("externalSeeds", list(pods_data[namespace].values()))
            log.debug(k8s_cluster.helm_install(
                target_chart_name=target_chart_name2, source_chart_name="scylla-operator/scylla",
                version=operator_version, use_devel=True, values=values, namespace=namespace2))
            k8s_cluster.kubectl_wait(
                "--all --for=condition=Ready pod", namespace=namespace2, timeout=1200)
            pods_data |= get_pod_names_and_ips(cluster_name=cluster_name, namespace=namespace2)
            ip_to_dc_map = {}
            for current_namespace in pods_data.keys():
                for pod_ip in pods_data[current_namespace].values():
                    ip_to_dc_map[pod_ip] = dc_name if current_namespace == namespace else dc_name2

            log.info("Verify DB cluster's peers info")
            for current_namespace in pods_data.keys():
                for pod_name, pod_ip in pods_data[current_namespace].items():
                    cqlsh_cmd = "SELECT JSON peer, data_center, rack, rpc_address FROM system.peers;"
                    cqlsh_results = kubectl(
                        f"exec {pod_name} -- /bin/cqlsh -e \"{cqlsh_cmd}\"",
                        namespace=current_namespace).stdout.split("---\n")[-1].split("\n")
                    table_rows = [yaml.safe_load(row) for row in cqlsh_results if "{" in row]
                    assert len(table_rows) == 5, "Expected 5 peers"
                    for row in table_rows:
                        assert row["peer"] == row["rpc_address"]
                        assert row["peer"] != pod_ip
                        assert row["peer"] in ip_to_dc_map
                        assert row["rack"] == k8s_cluster.rack_name
                        assert row["data_center"] == ip_to_dc_map[row["peer"]]
            need_to_collect_logs = False
            log.info("DB cluster's peers info is correct")
            return None
        finally:
            if need_to_collect_logs:
                KubernetesOps.gather_k8s_logs(
                    logdir_path=logdir, kubectl=kubectl, namespaces=[namespace, namespace2])
                need_to_collect_logs = False
            k8s_cluster.helm(f"uninstall {target_chart_name2} --timeout 120s", namespace=namespace2)
            try:
                kubectl(f"delete namespace {namespace2}", ignore_status=True, timeout=120)
            except invoke.exceptions.CommandTimedOut as exc:
                log.warning("Deletion of the '%s' namespace timed out: %s", namespace2, exc)

    finally:
        if need_to_collect_logs:
            KubernetesOps.gather_k8s_logs(
                logdir_path=logdir, kubectl=kubectl, namespaces=[namespace, namespace2])
        k8s_cluster.helm(f"uninstall {target_chart_name} --timeout 120s", namespace=namespace)
        try:
            kubectl(f"delete namespace {namespace}", ignore_status=True, timeout=120)
        except invoke.exceptions.CommandTimedOut as exc:
            log.warning("Deletion of the '%s' namespace timed out: %s", namespace, exc)


@pytest.mark.restart_is_used
@pytest.mark.skip("Disabled due to the https://github.com/scylladb/scylla-operator/issues/797")
def test_cassandra_rackdc(db_cluster, cassandra_rackdc_properties):
    """
    Test of applying cassandra-rackdc.properties via configmap
    """

    with cassandra_rackdc_properties() as props:
        config_map_props = props
    with db_cluster.nodes[0].actual_cassandra_rackdc_properties() as props:
        original_prefer_local = props.get('prefer_local')

    log.info("configMap's cassandra-rackdc.properties = %s", config_map_props)

    if original_prefer_local == 'false':
        new_prefer_local = 'true'
    elif original_prefer_local == 'true':
        new_prefer_local = 'false'
    else:
        assert False, f"cassandra-rackdc.properties have unexpected prefer_local value: {original_prefer_local}"

    with cassandra_rackdc_properties() as props:
        props['prefer_local'] = new_prefer_local
    db_cluster.restart_scylla()
    for node in db_cluster.nodes:
        with node.actual_cassandra_rackdc_properties() as props:
            assert props.get('prefer_local') == new_prefer_local
    # NOTE: check nodes states from all the nodes, because it is possible to have it be inconsistent
    for node in db_cluster.nodes:
        db_cluster.wait_for_nodes_up_and_normal(nodes=db_cluster.nodes, verification_node=node)

    with cassandra_rackdc_properties() as props:
        assert new_prefer_local == props.get('prefer_local')
        if 'prefer_local' not in config_map_props:
            props.pop('prefer_local', None)
        else:
            props['prefer_local'] = config_map_props['prefer_local']
    db_cluster.restart_scylla()
    # NOTE: check nodes states from all the nodes, because it is possible to have it be inconsistent
    for node in db_cluster.nodes:
        with node.actual_cassandra_rackdc_properties() as props:
            assert props.get('prefer_local') == original_prefer_local
    for node in db_cluster.nodes:
        db_cluster.wait_for_nodes_up_and_normal(nodes=db_cluster.nodes, verification_node=node)

    with cassandra_rackdc_properties() as props:
        assert config_map_props == props


@pytest.mark.restart_is_used
def test_rolling_restart_cluster(db_cluster):
    old_force_redeployment_reason = db_cluster.get_scylla_cluster_value("/spec/forceRedeploymentReason")
    db_cluster.restart_scylla()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))
    new_force_redeployment_reason = db_cluster.get_scylla_cluster_value("/spec/forceRedeploymentReason")

    assert old_force_redeployment_reason != new_force_redeployment_reason, (
        f"'{old_force_redeployment_reason}' must be different than '{new_force_redeployment_reason}'")

    # check iotune isn't called after restart
    pods_name_and_status = get_pods_and_statuses(db_cluster, namespace=db_cluster.namespace)
    for pod_name_and_status in pods_name_and_status:
        scylla_log = db_cluster.k8s_cluster.kubectl(f"logs {pod_name_and_status['name']} "
                                                    f"-c scylla", namespace=db_cluster.namespace)
        assert "scylla_io_setup" not in scylla_log.stdout, \
            f"iotune was run after reboot on {pod_name_and_status['name']}"


@pytest.mark.required_operator("v1.10.0")
def test_add_new_node_and_check_old_nodes_are_cleaned_up(db_cluster):
    log_followers, need_to_collect_logs, stop_ks_creation = {}, True, False
    k8s_cluster = db_cluster.k8s_cluster
    logdir = f"{os.path.join(k8s_cluster.logdir, 'test_add_new_node_and_check_old_nodes_are_cleaned_up')}"
    for node in db_cluster.nodes:
        for keyspace in db_cluster.nodes[0].run_cqlsh('describe keyspaces').stdout.split():
            log_followers[f"{node.name}--{keyspace}"] = node.follow_system_log(patterns=[
                f"api - force_keyspace_cleanup: keyspace={keyspace} ",
                f"api - Keyspace {keyspace} does not require cleanup"])

    def wait_for_cleanup_logs(log_follower_name, log_follower, db_cluster):
        db_rf = len(db_cluster.nodes)
        while not (list(log_follower) or stop_ks_creation):
            # NOTE: log lines located as last may not be caught fast enough because of the chunk-size limitation.
            #       So, perform additional DB actions to create log noise which will cause log reader moving on.
            current_ks_name = f"ks_db_log_population_{int(time.time())}"
            cql_create_ks_cmd = (
                f"CREATE KEYSPACE IF NOT EXISTS {current_ks_name}"
                f" WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor' : {db_rf}}}")
            try:
                db_cluster.nodes[0].run_cqlsh(cmd=cql_create_ks_cmd, timeout=60)
                time.sleep(4)
                db_cluster.nodes[0].run_cqlsh(cmd=f"DROP KEYSPACE IF EXISTS {current_ks_name}", timeout=60)
                time.sleep(4)
            except Exception as exc:  # noqa: BLE001
                # NOTE: we don't care if some of the queries fail.
                #       At first, there are redundant ones and, at second, they are utilitary.
                log.warning("Utilitary CQL query has failed: %s", exc)
        if stop_ks_creation:
            log.warning("%s: log search timed out", log_follower_name)
        else:
            log.info("%s: log search succeeded", log_follower_name)

    new_nodes_count = 1
    new_nodes = db_cluster.add_nodes(count=new_nodes_count, dc_idx=0, rack=0, enable_auto_bootstrap=True)
    try:
        db_cluster.wait_for_pods_readiness(pods_to_wait=new_nodes_count, total_pods=len(db_cluster.nodes))
        object_set = ParallelObject(
            objects=[[name, log_follower, db_cluster] for name, log_follower in log_followers.items()],
            num_workers=min(32, len(log_followers)),
            timeout=600,
        )
        object_set.run(func=wait_for_cleanup_logs, unpack_objects=True, ignore_exceptions=False)
        need_to_collect_logs = False
    finally:
        stop_ks_creation = True
        if need_to_collect_logs:
            KubernetesOps.gather_k8s_logs(
                logdir_path=logdir, kubectl=k8s_cluster.kubectl, namespaces=[SCYLLA_NAMESPACE])
        for new_node in new_nodes:
            db_cluster.decommission(new_node)


def _scylla_cluster_monitoring_ckecks(db_cluster: ScyllaPodCluster, monitoring_type: str):
    k8s_cluster = db_cluster.k8s_cluster
    cluster_name, namespace = db_cluster.scylla_cluster_name, db_cluster.namespace

    # NOTE: deploy prometheus operator if absent
    k8s_cluster.deploy_prometheus_operator()

    # NOTE: remove existing ScyllaDBMonitoring if exists
    k8s_cluster.delete_scylla_cluster_monitoring(namespace=namespace)
    try:
        # NOTE: deploy ScyllaDBMonitoring of the requested type
        k8s_cluster.deploy_scylla_cluster_monitoring(
            cluster_name=cluster_name, namespace=namespace, monitoring_type=monitoring_type)

        # NOTE: add SCT dashboard to the Grafana service
        api_call_return_code = k8s_cluster.register_sct_grafana_dashboard(
            cluster_name=cluster_name, namespace=namespace)
        assert api_call_return_code == '200', "SCT dashboard upload failed"

        is_passed = db_cluster.check_kubernetes_monitoring_health()
        assert is_passed, "K8S monitoring health checks have failed"

        # TODO: add tmp DB member and check the prometheus config for presence of that new tmp DB member
    finally:
        # NOTE: remove monitoring
        k8s_cluster.delete_scylla_cluster_monitoring(namespace=namespace)


@pytest.mark.required_operator("v1.9.0")
def test_scylla_cluster_monitoring_type_saas(db_cluster: ScyllaPodCluster):
    _scylla_cluster_monitoring_ckecks(db_cluster=db_cluster, monitoring_type="SaaS")


@pytest.mark.required_operator("v1.9.0")
def test_scylla_cluster_monitoring_type_platform(db_cluster: ScyllaPodCluster):
    _scylla_cluster_monitoring_ckecks(db_cluster=db_cluster, monitoring_type="Platform")


# NOTE: Scylla manager versions notes:
#       - '2.6.3' is broken: https://github.com/scylladb/scylla-manager/issues/3156
#       - '2.5.4' is broken: https://github.com/scylladb/scylla-manager/issues/3147
#       - '2.5.3' is broken: https://github.com/scylladb/scylla-manager/issues/3150
#       - '2.5.2' is broken: https://github.com/scylladb/scylla-manager/issues/3150
#       - '2.3.x' will fail with following error:
#         invalid character '\\x1f' looking for beginning of value
#       - '2.3.x' and ''2.4.x' are not covered as old ones.
@pytest.mark.requires_mgmt
@pytest.mark.parametrize("manager_version", (
    "3.2.6",
))
def test_mgmt_repair(db_cluster, manager_version):
    reinstall_scylla_manager(db_cluster, manager_version)

    # Run manager repair operation
    mgr_cluster = db_cluster.get_cluster_manager()
    mgr_task = mgr_cluster.create_repair_task()
    assert mgr_task, "Failed to create repair task"
    task_final_status = mgr_task.wait_and_get_final_status(timeout=86400)  # timeout is 24 hours
    assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(
        mgr_task.id, str(mgr_task.status))

    mgr_cluster.delete_task(task=mgr_task)


# NOTE: Scylla manager versions notes:
#       - '2.6.3' is broken: https://github.com/scylladb/scylla-manager/issues/3156
#       - '2.5.4' is broken: https://github.com/scylladb/scylla-manager/issues/3147
#       - '2.5.3' is broken: https://github.com/scylladb/scylla-manager/issues/3150
#       - '2.5.2' is broken: https://github.com/scylladb/scylla-manager/issues/3150
#       - '2.3.x' will fail with following error:
#         invalid character '\\x1f' looking for beginning of value
#       - '2.3.x' and ''2.4.x' are not covered as old ones.
@pytest.mark.requires_mgmt
@pytest.mark.parametrize("manager_version", (
    "3.2.6",
))
def test_mgmt_backup(db_cluster, manager_version):
    reinstall_scylla_manager(db_cluster, manager_version)

    # Run manager backup operation
    mgr_cluster = db_cluster.get_cluster_manager()
    region = next(iter(db_cluster.params.region_names), '')
    backup_bucket_location = db_cluster.params.get('backup_bucket_location').format(region=region)
    bucket_name = f"s3:{backup_bucket_location.split()[0]}"
    mgr_task = mgr_cluster.create_backup_task(location_list=[bucket_name, ])
    assert mgr_task, "Failed to create backup task"
    status = mgr_task.wait_and_get_final_status(timeout=7200, step=5, only_final=True)
    assert TaskStatus.DONE == status

    mgr_cluster.delete_task(task=mgr_task)


def test_drain_kubernetes_node_then_replace_scylla_node(db_cluster):
    target_node = random.choice(db_cluster.non_seed_nodes)
    old_uid = target_node.k8s_pod_uid
    log.info("Drain K8S node that hosts '%s' scylla node (uid=%s)", target_node, old_uid)
    target_node.drain_k8s_node()
    target_node.mark_to_be_replaced()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    target_node.wait_for_pod_readiness()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


def test_drain_kubernetes_node_then_wait_and_replace_scylla_node(db_cluster):
    target_node = random.choice(db_cluster.non_seed_nodes)
    old_uid = target_node.k8s_pod_uid
    log.info("Drain K8S node that hosts '%s' scylla node (uid=%s)", target_node, old_uid)
    target_node.drain_k8s_node()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    old_uid = target_node.k8s_pod_uid
    log.info('Mark %s (uid=%s) to be replaced', target_node, old_uid)
    target_node.mark_to_be_replaced()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    target_node.wait_for_pod_readiness()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


def test_drain_kubernetes_node_then_decommission_and_add_scylla_node(db_cluster):
    target_rack = random.choice([*db_cluster.racks])
    target_node = db_cluster.get_rack_nodes(target_rack)[-1]
    log.info("Drain K8S node that hosts '%s' scylla node not waiting for pod absence", target_node)
    target_node.drain_k8s_node()
    db_cluster.decommission(target_node)
    db_cluster.add_nodes(count=1, dc_idx=0, enable_auto_bootstrap=True, rack=0)
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


@pytest.mark.readonly
def test_listen_address(db_cluster):
    """
    Issues: https://github.com/scylladb/scylla-operator/issues/484
            https://github.com/scylladb/scylla/issues/8381
    Fix commit: https://github.com/scylladb/scylla-operator/pull/529
    """
    all_errors = []
    for node in db_cluster.nodes:
        result = node.remoter.run("ps aux | grep docker-entrypoint.py | grep -Po '\\-\\-listen-address=[^ ]+' | "
                                  "sed -r 's/--listen-address[= ]([^ ]+)/\\1/'", ignore_status=True)
        # Check in command line first
        if result.ok:
            if result.stdout.strip() != '0.0.0.0':
                all_errors.append(f"Node {node.name} has wrong listen-address argument {result.stdout}")
            continue
        # If no --listen-address in command line then proceed with scylla.yaml
        with node.actual_scylla_yaml() as scylla_yaml:
            listen_address = scylla_yaml.listen_address
            if not listen_address:
                all_errors.append(f"Not found listen_address flag in the {node.name} scylla.yaml")
            elif listen_address != '0.0.0.0':
                all_errors.append(f'Node {node.name} has wrong listen_address "{listen_address}" in scylla.yaml')

    assert not all_errors, "Following errors found:\n{'\n'.join(errors)}"


@pytest.mark.restart_is_used
def test_check_operator_operability_when_scylla_crd_is_incorrect(db_cluster):
    """Covers https://github.com/scylladb/scylla-operator/issues/447"""

    # NOTE: Create invalid ScyllaCluster which must be failed but not block operator.
    log.info("DEBUG: test_check_operator_operability_when_scylla_crd_is_incorrect")
    cluster_name, target_chart_name, namespace = ("test-empty-storage-capacity",) * 3
    values = HelmValues({
        'nameOverride': '',
        'fullnameOverride': cluster_name,
        'scyllaImage': {
            'repository': db_cluster.k8s_cluster.params.get('docker_image'),
            'tag': db_cluster.k8s_cluster.params.get('scylla_version'),
        },
        'agentImage': {
            'repository': 'scylladb/scylla-manager-agent',
            'tag': db_cluster.k8s_cluster.params.get('scylla_mgmt_agent_version'),
        },
        'serviceAccount': {
            'create': True,
            'annotations': {},
            'name': f"{cluster_name}-member"
        },
        'developerMode': True,
        'sysctls': ["fs.aio-max-nr=1048576"],
        'serviceMonitor': {'create': False},
        'datacenter': db_cluster.k8s_cluster.region_name,
        'racks': [{
            'name': db_cluster.k8s_cluster.rack_name,
            'members': 1,
            'storage': {},
            'resources': {
                'limits': {'cpu': 1, 'memory': "200Mi"},
                'requests': {'cpu': 1, 'memory': "200Mi"},
            },
        }]
    })
    db_cluster.k8s_cluster.create_namespace(namespace=namespace)
    db_cluster.k8s_cluster.helm_install(
        target_chart_name=target_chart_name,
        source_chart_name="scylla-operator/scylla",
        version=db_cluster.k8s_cluster.scylla_operator_chart_version,
        use_devel=True,
        values=values,
        namespace=namespace)
    try:
        db_cluster.k8s_cluster.wait_till_cluster_is_operational()

        # NOTE: Check that new cluster is non-working. Statefulset must be absent always.
        #       So, sleep for some time and make sure that it is absent.
        time.sleep(30)
        invalid_cluster_sts = db_cluster.k8s_cluster.kubectl(
            f"get sts -n {namespace} -l scylla/cluster={cluster_name}",
            ignore_status=True)
        assert 'No resources found' in invalid_cluster_sts.stderr, (
            f"Expected {cluster_name} not to have statefulset created.\n"
            f"stdout: {invalid_cluster_sts.stdout}\n"
            f"stderr: {invalid_cluster_sts.stderr}")

        # NOTE: Any change to the working ScyllaCluster going to trigger rollout.
        #       And rollout is enough for us to make sure that operator still works
        #       having invalid clusters. So, just run rollout restart which updates
        #       ScyllaCluster CRD.
        db_cluster.restart_scylla()
    finally:
        db_cluster.k8s_cluster.helm(
            f"uninstall {target_chart_name}", namespace=namespace)


def test_orphaned_services_after_shrink_cluster(db_cluster):
    """ Issue https://github.com/scylladb/scylla-operator/issues/514 """
    log.info("Add node to the rack 0")
    new_node = db_cluster.add_nodes(count=1, dc_idx=0, enable_auto_bootstrap=True, rack=0)[0]
    svc_name = new_node.name

    log.info("Decommission newly added node from the rack 0")
    db_cluster.decommission(new_node)
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))

    log.info("Wait for deletion of the '%s' svc for just deleted pod", svc_name)
    wait_for_resource_absence(
        db_cluster=db_cluster, resource_type="svc", resource_name=svc_name, step=2, timeout=60)
    assert not get_orphaned_services(db_cluster), "Orphaned services were found after decommission"


# NOTE: version limitation is caused by the following:
#       - https://github.com/scylladb/scylla-enterprise/issues/3211
#       - https://github.com/scylladb/scylladb/issues/14184
@pytest.mark.requires_scylla_versions(("5.2.7", None), ("2023.1.1", None))
def test_orphaned_services_multi_rack(db_cluster):
    """ Issue https://github.com/scylladb/scylla-operator/issues/514 """
    log.info("Add node to the rack 1")
    new_node = db_cluster.add_nodes(count=1, dc_idx=0, enable_auto_bootstrap=True, rack=1)[0]

    log.info("Decommission newly added node from the rack 1")
    svc_name = new_node.name
    db_cluster.decommission(new_node)
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))

    log.info("Wait for deletion of the '%s' svc for just deleted pod", svc_name)
    wait_for_resource_absence(
        db_cluster=db_cluster, resource_type="svc", resource_name=svc_name, step=2, timeout=60)
    assert not get_orphaned_services(db_cluster), "Orphaned services were found after decommission"


def test_nodetool_drain(db_cluster):
    """Covers https://github.com/scylladb/scylla-enterprise/issues/2808"""
    target_node = random.choice(db_cluster.non_seed_nodes)

    target_node.run_nodetool("drain", timeout=15*60, coredump_on_timeout=True)
    target_node.run_nodetool("status", ignore_status=True, warning_event_on_exception=(Exception,))
    target_node.stop_scylla_server(verify_up=False, verify_down=True, ignore_status=True)
    target_node.start_scylla_server(verify_up=True, verify_down=False)

    for node in db_cluster.nodes:
        db_cluster.wait_for_nodes_up_and_normal(nodes=db_cluster.nodes, verification_node=node)


# NOTE: may fail from time to time due to the https://github.com/scylladb/scylla-operator/issues/791
def test_ha_update_spec_while_rollout_restart(db_cluster: ScyllaPodCluster):
    """
    Cover the issue https://github.com/scylladb/scylla-operator/issues/410
    Validate that cluster resources can be updated while the webhook-server is rolling out
    having scylla-operator not operational at all.
    - update cluster specification a few time
    - start rollout restart in parallel with the update
    - validate that the cluster specification has been updated
    """
    sysctl_name = "fs.aio-max-nr"
    log.info("Get existing value of the '%s' sysctl", sysctl_name)
    original_aio_max_nr_value = expected_aio_max_nr_value = get_scylla_sysctl_value(
        db_cluster, sysctl_name)
    terminate_change_spec_thread, crd_update_errors = threading.Event(), []

    def change_cluster_spec() -> None:
        nonlocal expected_aio_max_nr_value, crd_update_errors
        while not terminate_change_spec_thread.wait(0.1):
            try:
                set_scylla_sysctl_value(db_cluster, sysctl_name, expected_aio_max_nr_value + 1)
                # NOTE: increase the value only when the sysctl spec update is successful
                #       to avoid false negative results in further assertions
                expected_aio_max_nr_value += 1
            except Exception as error:  # noqa: BLE001
                str_error = str(error)
                log.debug("Change /spec/sysctls value to %d failed. Error: %s",
                          expected_aio_max_nr_value, str_error)
                crd_update_errors.append(str_error)

    log.info("Start update of the Scylla cluster sysctl specification")
    change_cluster_spec_thread = threading.Thread(target=change_cluster_spec, daemon=True)
    change_cluster_spec_thread.start()
    change_cluster_spec_thread_stopped = False

    patch_operator_replicas_cmd = (
        """patch deployment scylla-operator -p '{"spec": {"replicas": %d}}'""")
    try:
        log.info("Bring down scylla-operator pods to avoid triggering of the Scylla pods rollout")
        db_cluster.k8s_cluster.kubectl(
            patch_operator_replicas_cmd % 0, namespace=SCYLLA_OPERATOR_NAMESPACE)
        db_cluster.k8s_cluster.kubectl(
            "wait -l app.kubernetes.io/name=scylla-operator --for=delete pod",
            namespace=SCYLLA_OPERATOR_NAMESPACE, timeout=300)

        log.info("Rollout webhook-server pods to verify that it's HA really works")
        db_cluster.k8s_cluster.kubectl(
            "rollout restart deployment webhook-server",
            namespace=SCYLLA_OPERATOR_NAMESPACE)
        db_cluster.k8s_cluster.kubectl(
            "rollout status deployment webhook-server --watch=true",
            namespace=SCYLLA_OPERATOR_NAMESPACE)

        log.info("Stop update of the Scylla cluster sysctl specification")
        terminate_change_spec_thread.set()
        change_cluster_spec_thread.join()
        change_cluster_spec_thread_stopped = True

        current_aio_max_nr_value = get_scylla_sysctl_value(db_cluster, sysctl_name)
    finally:
        if not change_cluster_spec_thread_stopped:
            # NOTE: needed to stop update of the spec when something happens wrong before
            #       such steps are reached above.
            terminate_change_spec_thread.set()
            change_cluster_spec_thread.join()

        set_scylla_sysctl_value(db_cluster, sysctl_name, original_aio_max_nr_value)
        # NOTE: scylla-operator spawns very fast so we need to wait for some time to avoid races
        time.sleep(10)

        log.info("Bring back scylla-operator pods to life")
        db_cluster.k8s_cluster.kubectl(
            patch_operator_replicas_cmd % 2, namespace=SCYLLA_OPERATOR_NAMESPACE)
        db_cluster.k8s_cluster.kubectl_wait(
            "--for=condition=Ready pod -l app.kubernetes.io/name=scylla-operator",
            namespace=SCYLLA_OPERATOR_NAMESPACE, timeout=300)

    try:
        assert expected_aio_max_nr_value == current_aio_max_nr_value, (
            "Cluster specification has not been updated correctly. "
            f"Expected value for '{sysctl_name}' sysctl is {expected_aio_max_nr_value}, "
            f"actual is {current_aio_max_nr_value}")
    finally:
        assert not crd_update_errors, (
            "Found following errors during webhook-server pods rollout restart: {}".format(
                "\n".join(crd_update_errors)))


@pytest.mark.readonly
def test_scylla_operator_pods(db_cluster: ScyllaPodCluster):
    scylla_operator_pods = get_pods_and_statuses(db_cluster=db_cluster, namespace=SCYLLA_OPERATOR_NAMESPACE,
                                                 label='app.kubernetes.io/instance=scylla-operator')

    assert len(scylla_operator_pods) == 2, f'Expected 2 scylla-operator pods, but exists {len(scylla_operator_pods)}'

    not_running_pods = ','.join(
        [pods_info['name'] for pods_info in scylla_operator_pods if pods_info['status'] != 'Running'])
    assert not not_running_pods, f'There are pods in state other than running: {not_running_pods}'

    # Cover https://github.com/scylladb/scylla-operator/issues/408:
    # Operator shouldn't run as StatefulSet, should be Deployment
    pods = db_cluster.k8s_cluster.kubectl(f"get pods {scylla_operator_pods[0]['name']} -o yaml",
                                          namespace=SCYLLA_OPERATOR_NAMESPACE)
    for owner_reference in yaml.safe_load(pods.stdout)['metadata'].get('ownerReferences', []):
        assert owner_reference["kind"] == "ReplicaSet", (
            f"Expected 'ReplicaSet' kind as owner, but got '{owner_reference['Kind']}'"
        )


@pytest.mark.readonly
def test_startup_probe_exists_in_scylla_pods(db_cluster: ScyllaPodCluster):
    pods = get_pods_without_probe(
        db_cluster=db_cluster,
        probe_type="startupProbe",
        selector="app.kubernetes.io/name=scylla",
        container_name="scylla")
    assert not pods, f"startupProbe is not found in the following pods: {pods}"


@pytest.mark.readonly
@pytest.mark.requires_mgmt
def test_readiness_probe_exists_in_mgmt_pods(db_cluster: ScyllaPodCluster):
    """
    PR: https://github.com/scylladb/scylla-operator/pull/725
    Issue: https://github.com/scylladb/scylla-operator/issues/718
    """
    pods = get_pods_without_probe(
        db_cluster=db_cluster,
        probe_type="readinessProbe",
        selector="app.kubernetes.io/name=scylla-manager",
        container_name="scylla-manager")
    assert not pods, f"readinessProbe is not found in the following pods: {pods}"


def test_deploy_helm_with_default_values(db_cluster: ScyllaPodCluster):
    """
    https://github.com/scylladb/scylla-operator/issues/501
    https://github.com/scylladb/scylla-operator/pull/502

    Deploy Scylla using helm chart with only default values.
    Storage capacity expected to be 10Gi
    """

    target_chart_name, namespace = ("t-default-values",) * 2
    expected_capacity = '10Gi'
    need_to_collect_logs, k8s_cluster = True, db_cluster.k8s_cluster
    logdir = f"{os.path.join(k8s_cluster.logdir, 'test_deploy_helm_with_default_values')}"

    k8s_cluster.create_namespace(namespace=namespace)
    k8s_cluster.create_scylla_manager_agent_config(namespace=namespace)

    log.debug('Deploy cluster with default storage capacity (expected "%s")', expected_capacity)
    log.debug(k8s_cluster.helm_install(
        target_chart_name=target_chart_name,
        source_chart_name="scylla-operator/scylla",
        version=k8s_cluster.scylla_operator_chart_version,
        use_devel=True,
        namespace=namespace,
    ))

    try:
        k8s_cluster.kubectl_wait(
            "--all --for=condition=Ready pod",
            namespace=namespace,
            timeout=1200,
        )

        pods_name_and_status = get_pods_and_statuses(
            db_cluster, namespace=namespace, label=db_cluster.pod_selector)

        assert len(pods_name_and_status) == 3, (
            f"Expected 3 pods to be created in {namespace} namespace "
            f"but actually {len(pods_name_and_status)}: {pods_name_and_status}")

        for pod_name_and_status in pods_name_and_status:
            assert pod_name_and_status['status'] == PodStatuses.RUNNING.value, (
                f"Expected '{PodStatuses.RUNNING.value}' status of pod '{pod_name_and_status['name']}' "
                f"but actually it's {pod_name_and_status['status']}")

            storage_capacity = get_pod_storage_capacity(db_cluster, namespace=namespace,
                                                        pod_name=pod_name_and_status['name'],
                                                        label="app.kubernetes.io/name=scylla")
            assert storage_capacity[0]['capacity'] == expected_capacity, (
                f"Expected capacity is {expected_capacity}, actual capacity of pod "
                f"'{pod_name_and_status['name']}' is {storage_capacity[0]['capacity']}")

            scylla_version = k8s_cluster.kubectl(
                f"exec {pod_name_and_status['name']} -c scylla -- scylla --version",
                namespace=namespace)
            assert not scylla_version.stderr, (
                f"Failed to get scylla version from {pod_name_and_status['name']}. Error: {scylla_version.stderr}")
            assert scylla_version.stdout, (
                f"Failed to get scylla version from {pod_name_and_status['name']}. "
                f"Output of command 'scylla --version' is empty")
        need_to_collect_logs = False
        log.info("Scylla clsuter with default info has successfully passed validation")
        return None
    finally:
        if need_to_collect_logs:
            KubernetesOps.gather_k8s_logs(
                logdir_path=logdir, kubectl=k8s_cluster.kubectl, namespaces=[namespace])
        k8s_cluster.helm(f"uninstall {target_chart_name} --timeout 120s", namespace=namespace)
        try:
            k8s_cluster.kubectl(f"delete namespace {namespace}")
        except invoke.exceptions.CommandTimedOut as exc:
            log.warning("Deletion of the '%s' namespace timed out: %s", namespace, exc)


@pytest.mark.restart_is_used
def test_rolling_config_change_internode_compression(db_cluster, scylla_yaml):
    """
    Cover logic of disrupt_rolling_config_change_internode_compression nemesis
    """
    internode_compression_option_name = "internode_compression"

    with db_cluster.nodes[0].actual_scylla_yaml() as props:
        original_compression = dict(props).get(internode_compression_option_name)

    log.debug("Current compression is %s", original_compression)
    values = ['dc', 'all', 'none']
    values_to_toggle = list(filter(lambda value: value != original_compression, values))
    new_compression = random.choice(values_to_toggle)

    with scylla_yaml() as props:
        props[internode_compression_option_name] = new_compression

    db_cluster.restart_scylla()


@pytest.mark.restart_is_used
def test_scylla_yaml_override(db_cluster, scylla_yaml):
    """
    Test of applying scylla.yaml via configmap
    - update parameter that exists in scylla.yaml
    - add parameter
    """
    hh_enabled_option_name = "hinted_handoff_enabled"
    hh_throttle_option_name = "hinted_handoff_throttle_in_kb"

    with scylla_yaml() as props:
        configmap_scylla_yaml_content = props

    original_hinted_handoff_throttle_in_kb, new_hinted_handoff_throttle_in_kb = None, None

    with db_cluster.nodes[0].actual_scylla_yaml() as props:
        original_hinted_handoff = dict(props).get(hh_enabled_option_name)
        if not configmap_scylla_yaml_content.get(hh_throttle_option_name):
            original_hinted_handoff_throttle_in_kb = dict(props).get(hh_throttle_option_name) or 1024

    log.info("configMap's scylla.yaml = %s", configmap_scylla_yaml_content)

    assert isinstance(original_hinted_handoff, bool), (
        f"configMap scylla.yaml have unexpected '{hh_enabled_option_name}' type: {type(original_hinted_handoff)}. "
        "Expected 'bool'")
    new_hinted_handoff = not original_hinted_handoff

    if original_hinted_handoff_throttle_in_kb:
        assert isinstance(original_hinted_handoff_throttle_in_kb, int), (
            f"Node scylla.yaml have unexpected '{hh_throttle_option_name}' type: "
            f"{type(original_hinted_handoff_throttle_in_kb)}. Expected 'int'")
        new_hinted_handoff_throttle_in_kb = original_hinted_handoff_throttle_in_kb * 2

    with scylla_yaml() as props:
        props[hh_enabled_option_name] = new_hinted_handoff
        if new_hinted_handoff_throttle_in_kb:
            props[hh_throttle_option_name] = new_hinted_handoff_throttle_in_kb
        else:
            dict(props).pop(hh_throttle_option_name)

    # NOTE: sleep for some time to avoid race between following restart and configmap object
    #       update which gets made in the above 'with scylla_yaml() as props' context manager.
    time.sleep(5)
    db_cluster.restart_scylla()
    for node in db_cluster.nodes:
        with node.actual_scylla_yaml() as props:
            assert dict(props).get(hh_enabled_option_name) == new_hinted_handoff
            if new_hinted_handoff_throttle_in_kb:
                assert dict(props).get(hh_throttle_option_name) == new_hinted_handoff_throttle_in_kb
            else:
                assert not dict(props).get(hh_throttle_option_name)

    # NOTE: check nodes states from all the nodes, because it is possible to have it be inconsistent
    for node in db_cluster.nodes:
        db_cluster.wait_for_nodes_up_and_normal(nodes=db_cluster.nodes, verification_node=node)

    with scylla_yaml() as props:
        assert dict(props).get(hh_enabled_option_name) == new_hinted_handoff
        if hh_enabled_option_name not in configmap_scylla_yaml_content:
            props.pop(hh_enabled_option_name, None)
        else:
            props[hh_enabled_option_name] = configmap_scylla_yaml_content[hh_enabled_option_name]

        if new_hinted_handoff_throttle_in_kb:
            props.pop(hh_throttle_option_name, None)
        else:
            props[hh_throttle_option_name] = configmap_scylla_yaml_content[hh_throttle_option_name]

    time.sleep(5)
    db_cluster.restart_scylla()
    for node in db_cluster.nodes:
        with node.actual_scylla_yaml() as props:
            assert dict(props).get(hh_enabled_option_name) == original_hinted_handoff
            assert (dict(props).get(hh_throttle_option_name) or 1024) == original_hinted_handoff_throttle_in_kb

    for node in db_cluster.nodes:
        db_cluster.wait_for_nodes_up_and_normal(nodes=db_cluster.nodes, verification_node=node)

    with scylla_yaml() as props:
        assert configmap_scylla_yaml_content == props


@pytest.mark.readonly
def test_default_dns_policy(db_cluster: ScyllaPodCluster):
    expected_policy = "ClusterFirstWithHostNet"
    pods = db_cluster.k8s_cluster.kubectl("get pods -l scylla/cluster="
                                          f"{db_cluster.params.get('k8s_scylla_cluster_name')} -o yaml",
                                          namespace=SCYLLA_NAMESPACE)

    pods_with_wrong_dns_policy = []
    for pod in yaml.safe_load(pods.stdout)["items"]:
        dns_policy = pod["spec"]["dnsPolicy"]
        if dns_policy != expected_policy:
            pods_with_wrong_dns_policy.append({"pod_name": pod["metadata"]["name"],
                                               "dnsPolicy": dns_policy,
                                               })

    assert not pods_with_wrong_dns_policy, (
        f"Found pods that have unexpected dnsPolicy.\n"
        f"Expected is '{expected_policy}'.\n"
        f"Pods: {yaml.safe_dump(pods_with_wrong_dns_policy, indent=2)}")


@pytest.mark.required_operator("v1.8.0")
@pytest.mark.requires_tls_and_sni
def test_operator_managed_tls(db_cluster: ScyllaPodCluster, tmp_path: path.Path):

    cluster_name = db_cluster.k8s_cluster.k8s_scylla_cluster_name

    crt_filename = tmp_path / 'tls.crt'
    key_filename = tmp_path / 'tls.key'
    ca_filename = tmp_path / 'ca.crt'

    crt_data = db_cluster.k8s_cluster.kubectl(fr"get secret/{cluster_name}-local-client-ca -o jsonpath='{{.data.tls\.crt}}'",
                                              namespace=db_cluster.namespace)
    key_data = db_cluster.k8s_cluster.kubectl(fr"get secret/{cluster_name}-local-client-ca -o jsonpath='{{.data.tls\.key}}'",
                                              namespace=db_cluster.namespace)
    ca_data = db_cluster.k8s_cluster.kubectl(fr"get secret/{cluster_name}-local-serving-ca -o jsonpath='{{.data.tls\.crt}}'",
                                             namespace=db_cluster.namespace)

    crt_filename.write_bytes(base64.decodebytes(bytes(crt_data.stdout.strip(), encoding='utf-8')))
    key_filename.write_bytes(base64.decodebytes(bytes(key_data.stdout.strip(), encoding='utf-8')))
    ca_filename.write_bytes(base64.decodebytes(bytes(ca_data.stdout.strip(), encoding='utf-8')))

    for file in (crt_filename, key_filename, ca_filename):
        log.debug(file)
        log.debug(file.read_text())

    # since ip address of this node can change in previous tests,
    # and we don't have the ip tracker thread in local-kind setup
    db_cluster.nodes[0].refresh_ip_address()

    execution_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([
                                         db_cluster.nodes[0].cql_address]))
    cluster = Cluster(contact_points=[db_cluster.nodes[0].cql_address], port=db_cluster.nodes[0].CQL_SSL_PORT,
                      execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile})
    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_SSLv23)
    ssl_context.verify_mode = ssl.VerifyMode.CERT_REQUIRED

    ssl_context.load_verify_locations(cadata=ca_filename.read_text())
    ssl_context.load_cert_chain(keyfile=key_filename, certfile=crt_filename)

    cluster.ssl_context = ssl_context

    with cluster.connect(wait_for_all_pools=True) as session:
        for host in cluster.metadata.all_hosts():
            log.debug(host)
        res = session.execute("SELECT * FROM system.local")
        output = res.all()
        assert len(output) == 1
        log.debug(output)


def test_can_recover_from_fatal_pod_termination(db_cluster):
    target_node = db_cluster.nodes[-1]
    experiment = PodFailureExperiment(pod=target_node, duration="60s")
    experiment.start()
    db_cluster.k8s_cluster.kubectl(
        f"wait --timeout=1m --for=condition=Ready=false pod {target_node.name} -n {db_cluster.namespace}")
    experiment.wait_until_finished()
    db_cluster.wait_for_nodes_up_and_normal(nodes=[target_node], verification_node=target_node)


# NOTE: non-fast K8S backends such as 'k8s-gke' and 'k8s-local-kind' are affected by following bug:
#       https://github.com/scylladb/scylla-operator/issues/1077
@pytest.mark.requires_backend("k8s-eks")
def test_nodetool_flush_and_reshard(db_cluster: ScyllaPodCluster):
    target_node = db_cluster.nodes[0]

    # Calculate new value for the CPU cores dedicated for Scylla pods
    current_cpus = convert_cpu_value_from_k8s_to_units(
        db_cluster.k8s_cluster.scylla_cpu_limit)
    new_cpus = current_cpus + 1 if current_cpus <= 1 else current_cpus - 1
    new_cpus = convert_cpu_units_to_k8s_value(new_cpus)

    # Run 'nodetool flush' command
    target_node.run_nodetool("flush -- keyspace1")

    try:
        # Change number of CPUs dedicated for Scylla pods
        # and make sure that the resharding process begins and finishes
        verify_resharding_on_k8s(db_cluster, new_cpus)
    finally:
        # Return the cpu count back and wait for the resharding begin and finish
        verify_resharding_on_k8s(db_cluster, db_cluster.k8s_cluster.scylla_cpu_limit)
