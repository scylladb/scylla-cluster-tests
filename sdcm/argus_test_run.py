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
import time
import os
import logging
import unittest.mock
from uuid import UUID

from argus.db.db_types import TestStatus
from argus.db.testrun import TestRunWithHeartbeat, TestRunInfo, TestDetails, TestResources, TestLogs, TestResults, \
    TestResourcesSetup
from argus.db.cloud_types import CloudInstanceDetails, AWSSetupDetails, GCESetupDetails, BaseCloudSetupDetails, \
    CloudNodesInfo
from argus.db.config import Config as ArgusConfig

from sdcm.keystore import KeyStore
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.net import get_my_ip, get_sct_runner_ip
from sdcm.utils.get_username import get_username
from sdcm.utils.git import get_git_current_branch, get_git_commit_id
from sdcm.utils.ci_tools import get_job_url, get_job_name, get_test_name

LOGGER = logging.getLogger(__name__)


class ArgusTestRunError(Exception):
    pass


def _prepare_aws_resource_setup(sct_config: SCTConfiguration):
    db_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_db_scylla"),
                                   instance_type=sct_config.get("instance_type_db"),
                                   node_amount=sct_config.get("n_db_nodes"),
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_loader"),
                                       instance_type=sct_config.get("instance_type_loader"),
                                       node_amount=sct_config.get("n_loaders"),
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_monitor"),
                                        instance_type=sct_config.get("instance_type_monitor"),
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = AWSSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                  monitor_node=monitor_node_setup)

    return cloud_setup


def _prepare_gce_resource_setup(sct_config: SCTConfiguration):
    db_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_db"),
                                   instance_type=sct_config.get("gce_instance_type_db"),
                                   node_amount=sct_config.get("n_db_nodes"),
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_loader"),
                                       instance_type=sct_config.get("gce_instance_type_loader"),
                                       node_amount=sct_config.get("n_loaders"),
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_monitor"),
                                        instance_type=sct_config.get("gce_instance_type_monitor"),
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = GCESetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                  monitor_node=monitor_node_setup)

    return cloud_setup


def _prepare_unknown_resource_setup(sct_config: SCTConfiguration):
    LOGGER.error("Unknown backend encountered: %s", sct_config.get("cluster_backend"))
    db_node_setup = CloudNodesInfo(image_id="UNKNOWN",
                                   instance_type="UNKNOWN",
                                   node_amount=-1,
                                   post_behaviour="UNKNOWN")
    loader_node_setup = CloudNodesInfo(image_id="UNKNOWN",
                                       instance_type="UNKNOWN",
                                       node_amount=-1,
                                       post_behaviour="UNKNOWN")
    monitor_node_setup = CloudNodesInfo(image_id="UNKNOWN",
                                        instance_type="UNKNOWN",
                                        node_amount=-1,
                                        post_behaviour="UNKNOWN")
    cloud_setup = BaseCloudSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                        monitor_node=monitor_node_setup, backend=sct_config.get("cluster_backend"))

    return cloud_setup


def _prepare_bare_metal_resource_setup(sct_config: SCTConfiguration):
    db_node_setup = CloudNodesInfo(image_id="bare_metal",
                                   instance_type="bare_metal",
                                   node_amount=sct_config.get("n_db_nodes"),
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id="bare_metal",
                                       instance_type="bare_metal",
                                       node_amount=sct_config.get("n_loaders"),
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id="bare_metal",
                                        instance_type="bare_metal",
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = BaseCloudSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                        monitor_node=monitor_node_setup, backend=sct_config.get("cluster_backend"))

    return cloud_setup


def _prepare_k8s_gce_minikube_resource_setup(sct_config: SCTConfiguration):
    cloud_setup = _prepare_gce_resource_setup(sct_config)

    image_id = sct_config.get("scylla_version")
    cloud_setup.db_node.image_id = f"scylladb/scylladb:{image_id}"
    cloud_setup.db_node.instance_type = sct_config.get("gce_instance_type_minikube")

    return cloud_setup


def _prepare_k8s_gke_resource_setup(sct_config: SCTConfiguration):
    cloud_setup = _prepare_gce_resource_setup(sct_config)
    image_id = sct_config.get("scylla_version")
    cloud_setup.db_node.image_id = f"scylladb/scylladb:{image_id}"
    cloud_setup.monitor_node.image_id = sct_config.get("mgmt_docker_image")
    cloud_setup.loader_node.image_id = f"scylladb/scylladb:{image_id}"

    return cloud_setup


def _prepare_k8s_eks_resource_setup(sct_config: SCTConfiguration):
    cloud_setup = _prepare_aws_resource_setup(sct_config)

    return cloud_setup


def _prepare_docker_resource_setup(sct_config: SCTConfiguration):
    db_node_setup = CloudNodesInfo(image_id=sct_config.get('docker_image'),
                                   instance_type="docker",
                                   node_amount=sct_config.get("n_db_nodes"),
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get('docker_image'),
                                       instance_type="docker",
                                       node_amount=sct_config.get("n_loaders"),
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get('docker_image'),
                                        instance_type="docker",
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = BaseCloudSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                        monitor_node=monitor_node_setup, backend=sct_config.get("cluster_backend"))

    return cloud_setup


class ArgusTestRun:
    WARNINGS_SENT = set()
    TESTRUN_INSTANCE: TestRunWithHeartbeat | None = None
    BACKEND_MAP = {
        "aws": _prepare_aws_resource_setup,
        "aws-siren": _prepare_aws_resource_setup,
        "gce": _prepare_gce_resource_setup,
        "gce-siren": _prepare_gce_resource_setup,
        "k8s-eks": _prepare_k8s_eks_resource_setup,
        "k8s-gke": _prepare_k8s_gke_resource_setup,
        "k8s-gce-minikube": _prepare_k8s_gce_minikube_resource_setup,
        "baremetal": _prepare_bare_metal_resource_setup,
        "docker": _prepare_docker_resource_setup,
        "unknown": _prepare_unknown_resource_setup,
    }
    CONFIG = ArgusConfig(**KeyStore().get_argusdb_credentials(), keyspace_name="argus")
    AVAILABLE_RELEASES = [
        "argus-integration",
        "master",
        "branch-2022.1",
        "branch-4.6",
        "manager-2.7",
        "operator-1.6",
    ]

    def __init__(self):
        pass

    @classmethod
    def warn_once(cls, message: str, *args: list):
        if message in cls.WARNINGS_SENT:
            return
        cls.WARNINGS_SENT.add(message)
        LOGGER.warning(message, *args)

    @classmethod
    def from_sct_config(cls, test_id: UUID, test_module_path: str,
                        sct_config: SCTConfiguration) -> TestRunWithHeartbeat:
        # pylint: disable=too-many-locals
        if cls.TESTRUN_INSTANCE:
            raise ArgusTestRunError("Instance already initialized")

        release_name = os.getenv("GIT_BRANCH", get_git_current_branch()).split("/")[-1]
        if release_name not in cls.AVAILABLE_RELEASES:
            raise ArgusTestRunError("Refusing to track a non-whitelisted branch", release_name, cls.AVAILABLE_RELEASES)
        LOGGER.info("Preparing Test Details...")
        test_group, *_ = test_module_path.split(".")
        config_files = sct_config.get("config_files")
        job_name = get_job_name()
        job_url = get_job_url()
        started_by = get_username()

        details = TestDetails(name=get_test_name(), scm_revision_id=get_git_commit_id(), started_by=started_by,
                              build_job_name=job_name, build_job_url=job_url,
                              yaml_test_duration=sct_config.get("test_duration"), start_time=int(time.time()),
                              config_files=config_files, packages=[])
        LOGGER.info("Preparing Resource Setup...")
        backend = sct_config.get("cluster_backend")
        regions = sct_config.region_names

        sct_runner_info = CloudInstanceDetails(public_ip=get_sct_runner_ip(), provider=backend,
                                               region=regions[0], private_ip=get_my_ip())

        cloud_setup = cls.BACKEND_MAP.get(backend, _prepare_unknown_resource_setup)(sct_config)

        setup_details = TestResourcesSetup(sct_runner_host=sct_runner_info, region_name=regions,
                                           cloud_setup=cloud_setup)

        logs = TestLogs()
        resources = TestResources()
        results = TestResults(status=TestStatus.CREATED)

        run_info = TestRunInfo(details=details, setup=setup_details, resources=resources, logs=logs, results=results)
        LOGGER.info("Initializing TestRun...")
        cls.TESTRUN_INSTANCE = TestRunWithHeartbeat(test_id=test_id, group=test_group, release_name=release_name,
                                                    assignee="",
                                                    run_info=run_info,
                                                    config=cls.CONFIG)

        return cls.TESTRUN_INSTANCE

    @classmethod
    def get(cls, test_id: UUID = None) -> TestRunWithHeartbeat:
        if test_id and not cls.TESTRUN_INSTANCE:
            cls.TESTRUN_INSTANCE = TestRunWithHeartbeat.from_id(test_id, config=cls.CONFIG)

        if not cls.TESTRUN_INSTANCE:
            cls.warn_once("Returning MagicMock from ArgusTestRun.get() as we are unable to acquire Argus connection")
            return unittest.mock.MagicMock()

        return cls.TESTRUN_INSTANCE

    @classmethod
    def destroy(cls):
        if not cls.TESTRUN_INSTANCE:
            return False
        cls.TESTRUN_INSTANCE.shutdown()
        cls.TESTRUN_INSTANCE = None
        return True
