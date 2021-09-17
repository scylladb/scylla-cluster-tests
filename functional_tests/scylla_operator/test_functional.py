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
import random
import threading
import time
import pytest

from sdcm.cluster_k8s import (
    ScyllaPodCluster,
    SCYLLA_OPERATOR_NAMESPACE,
)
from sdcm.mgmt import TaskStatus
from sdcm.utils.k8s import HelmValues
from functional_tests.scylla_operator.libs.helpers import (
    get_scylla_sysctl_value,
    get_orphaned_services,
    get_pods_without_probe,
    scylla_operator_pods_and_statuses,
    set_scylla_sysctl_value,
    wait_for_resource_absence,
)

log = logging.getLogger()


@pytest.mark.skip("Disabled due to the https://github.com/scylladb/scylla-cluster-tests/issues/3786")
def test_cassandra_rackdc(cassandra_rackdc_properties):
    """
    Test of applying cassandra-rackdc.properties via configmap
    """

    with cassandra_rackdc_properties() as props:
        original = props['prefer_local']

    log.info("cassandra-rackdc.properties.prefer_local = %s", original)

    if original == 'false':
        changed = 'true'
    elif original == 'true':
        changed = 'false'
    else:
        assert False, f'cassandra-rackdc.properties have unexpected prefer_local value {original}'

    with cassandra_rackdc_properties() as props:
        assert original == props['prefer_local']
        props['prefer_local'] = changed

    with cassandra_rackdc_properties() as props:
        assert changed == props['prefer_local']
        props['prefer_local'] = original

    with cassandra_rackdc_properties() as props:
        assert original == props['prefer_local']


def test_rolling_restart_cluster(db_cluster):
    old_force_redeployment_reason = db_cluster.get_scylla_cluster_value("/spec/forceRedeploymentReason")
    db_cluster.restart_scylla()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))
    new_force_redeployment_reason = db_cluster.get_scylla_cluster_value("/spec/forceRedeploymentReason")

    assert old_force_redeployment_reason != new_force_redeployment_reason, (
        f"'{old_force_redeployment_reason}' must be different than '{new_force_redeployment_reason}'")


@pytest.mark.require_node_terminate('drain_k8s_node')
def test_drain_and_replace_node_kubernetes(db_cluster):
    target_node = random.choice(db_cluster.non_seed_nodes)
    old_uid = target_node.k8s_pod_uid
    log.info('TerminateNode %s (uid=%s)', target_node, old_uid)
    target_node.drain_k8s_node()
    target_node.mark_to_be_replaced()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    target_node.wait_for_pod_readiness()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


@pytest.mark.require_node_terminate('drain_k8s_node')
def test_drain_wait_and_replace_node_kubernetes(db_cluster):
    target_node = random.choice(db_cluster.non_seed_nodes)
    old_uid = target_node.k8s_pod_uid
    log.info('TerminateNode %s (uid=%s)', target_node, old_uid)
    target_node.drain_k8s_node()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    old_uid = target_node.k8s_pod_uid
    log.info('Mark %s (uid=%s) to be replaced', target_node, old_uid)
    target_node.mark_to_be_replaced()
    target_node.wait_till_k8s_pod_get_uid(ignore_uid=old_uid)
    target_node.wait_for_pod_readiness()
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


@pytest.mark.require_node_terminate('drain_k8s_node')
def test_drain_terminate_decommission_add_node_kubernetes(db_cluster):
    target_rack = random.choice([*db_cluster.racks])
    target_node = db_cluster.get_rack_nodes(target_rack)[-1]
    target_node.drain_k8s_node()
    db_cluster.decommission(target_node)
    db_cluster.add_nodes(count=1, dc_idx=0, enable_auto_bootstrap=True, rack=0)
    db_cluster.wait_for_pods_readiness(pods_to_wait=1, total_pods=len(db_cluster.nodes))


@pytest.mark.require_mgmt()
def test_mgmt_repair(db_cluster):
    mgr_cluster = db_cluster.get_cluster_manager()
    mgr_task = mgr_cluster.create_repair_task()
    assert mgr_task, "Failed to create repair task"
    task_final_status = mgr_task.wait_and_get_final_status(timeout=86400)  # timeout is 24 hours
    assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(
        mgr_task.id, str(mgr_task.status))


@pytest.mark.require_mgmt()
def test_mgmt_backup(db_cluster):
    mgr_cluster = db_cluster.get_cluster_manager()
    backup_bucket_location = db_cluster.params.get('backup_bucket_location')
    bucket_name = f"s3:{backup_bucket_location.split()[0]}"
    mgr_task = mgr_cluster.create_backup_task(location_list=[bucket_name, ])
    assert mgr_task, "Failed to create backup task"
    status = mgr_task.wait_and_get_final_status(timeout=54000, step=5, only_final=True)
    assert TaskStatus.DONE == status


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
        'datacenter': db_cluster.k8s_cluster.params.get('k8s_scylla_datacenter'),
        'racks': [{
            'name': db_cluster.k8s_cluster.params.get('k8s_scylla_rack'),
            'members': 1,
            'storage': {},
            'resources': {
                'limits': {'cpu': 1, 'memory': "200Mi"},
                'requests': {'cpu': 1, 'memory': "200Mi"},
            },
        }]
    })
    db_cluster.k8s_cluster.kubectl(f"create namespace {namespace}", ignore_status=True)
    db_cluster.k8s_cluster.helm_install(
        target_chart_name=target_chart_name,
        source_chart_name="scylla-operator/scylla",
        version=db_cluster.k8s_cluster._scylla_operator_chart_version,  # pylint: disable=protected-access
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
            except Exception as error:  # pylint: disable=broad-except
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
        db_cluster.k8s_cluster.kubectl(
            "wait -l app.kubernetes.io/name=scylla-operator --for=condition=Ready pod",
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


def test_scylla_operator_pods(db_cluster: ScyllaPodCluster):
    scylla_operator_pods = scylla_operator_pods_and_statuses(db_cluster)

    assert len(scylla_operator_pods) == 2, f'Expected 2 scylla-operator pods, but exists {len(scylla_operator_pods)}'

    not_running_pods = ','.join([pods_info[0] for pods_info in scylla_operator_pods if pods_info[1] != 'Running'])
    assert not not_running_pods, f'There are pods in state other than running: {not_running_pods}'


def test_startup_probe_exists_in_scylla_pods(db_cluster: ScyllaPodCluster):
    pods = get_pods_without_probe(
        db_cluster=db_cluster,
        probe_type="startupProbe",
        selector="app.kubernetes.io/name=scylla",
        container_name="scylla")
    assert not pods, f"startupProbe is not found in the following pods: {pods}"


@pytest.mark.require_mgmt()
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
