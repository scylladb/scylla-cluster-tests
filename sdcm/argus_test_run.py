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
import unittest.mock
from uuid import UUID
from datetime import datetime, timedelta
from random import randint

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
from sdcm.utils.git import get_git_commit_id
from sdcm.utils.ci_tools import get_job_url, get_job_name

LOGGER = logging.getLogger(__name__)


class ArgusTestRunError(Exception):
    pass


def _get_node_amounts(config: SCTConfiguration) -> tuple[int, int]:
    num_db_node = config.get("n_db_nodes")
    num_db_node = sum([int(i) for i in num_db_node.split()]) if isinstance(num_db_node, str) else num_db_node
    num_loaders = config.get("n_loaders")
    num_loaders = sum([int(i) for i in num_loaders.split()]) if isinstance(num_loaders, str) else num_loaders

    return num_db_node, num_loaders


def _prepare_aws_resource_setup(sct_config: SCTConfiguration) -> AWSSetupDetails:
    num_db_nodes, n_loaders = _get_node_amounts(sct_config)
    db_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_db_scylla"),
                                   instance_type=sct_config.get("instance_type_db"),
                                   node_amount=num_db_nodes,
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_loader"),
                                       instance_type=sct_config.get("instance_type_loader"),
                                       node_amount=n_loaders,
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get("ami_id_monitor"),
                                        instance_type=sct_config.get("instance_type_monitor"),
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = AWSSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                  monitor_node=monitor_node_setup)

    return cloud_setup


def _prepare_gce_resource_setup(sct_config: SCTConfiguration) -> GCESetupDetails:
    num_db_nodes, n_loaders = _get_node_amounts(sct_config)
    db_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_db") or "N/A",
                                   instance_type=sct_config.get("gce_instance_type_db") or "N/A",
                                   node_amount=num_db_nodes,
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_loader") or "N/A",
                                       instance_type=sct_config.get("gce_instance_type_loader") or "N/A",
                                       node_amount=n_loaders,
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get("gce_image_monitor") or "N/A",
                                        instance_type=sct_config.get("gce_instance_type_monitor") or "N/A",
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = GCESetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                  monitor_node=monitor_node_setup)

    return cloud_setup


def _prepare_azure_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
    num_db_nodes, n_loaders = _get_node_amounts(sct_config)
    db_node_setup = CloudNodesInfo(image_id=sct_config.get("azure_image_db"),
                                   instance_type=sct_config.get("azure_instance_type_db"),
                                   node_amount=num_db_nodes,
                                   post_behaviour=sct_config.get("post_behavior_db_nodes"))
    loader_node_setup = CloudNodesInfo(image_id=sct_config.get("azure_image_loader"),
                                       instance_type=sct_config.get("azure_instance_type_loader"),
                                       node_amount=n_loaders,
                                       post_behaviour=sct_config.get("post_behavior_loader_nodes"))
    monitor_node_setup = CloudNodesInfo(image_id=sct_config.get("azure_image_monitor"),
                                        instance_type=sct_config.get("azure_instance_type_monitor"),
                                        node_amount=sct_config.get("n_monitor_nodes"),
                                        post_behaviour=sct_config.get("post_behavior_monitor_nodes"))
    cloud_setup = BaseCloudSetupDetails(db_node=db_node_setup, loader_node=loader_node_setup,
                                        monitor_node=monitor_node_setup, backend=sct_config.get("cluster_backend"))

    return cloud_setup


def _prepare_unknown_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
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


def _prepare_bare_metal_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
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


def _prepare_k8s_gke_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
    cloud_setup = _prepare_gce_resource_setup(sct_config)

    return cloud_setup


def _prepare_k8s_eks_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
    cloud_setup = _prepare_aws_resource_setup(sct_config)

    return cloud_setup


def _prepare_docker_resource_setup(sct_config: SCTConfiguration) -> BaseCloudSetupDetails:
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
        "azure": _prepare_azure_resource_setup,
        "gce": _prepare_gce_resource_setup,
        "gce-siren": _prepare_gce_resource_setup,
        "k8s-eks": _prepare_k8s_eks_resource_setup,
        "k8s-gke": _prepare_k8s_gke_resource_setup,
        "baremetal": _prepare_bare_metal_resource_setup,
        "docker": _prepare_docker_resource_setup,
        "unknown": _prepare_unknown_resource_setup,
    }

    REGION_PROPERTY_MAP = {
        "aws": "region_name",
        "aws-siren": "region_name",
        "k8s-eks": "region_name",
        "gce": "gce_datacenter",
        "gce-siren": "gce_datacenter",
        "k8s-gke": "gce_datacenter",
        "azure": "azure_region_name",
        "default": "region_name",
    }
    _config: ArgusConfig = None

    def __init__(self):
        pass

    @classmethod
    def config(cls):
        if cls._config is None:
            cls._config = ArgusConfig(**KeyStore().get_argusdb_credentials(), keyspace_name="argus")
        return cls._config

    @classmethod
    def warn_once(cls, message: str, *args: list):
        if message in cls.WARNINGS_SENT:
            return
        cls.WARNINGS_SENT.add(message)
        LOGGER.warning(message, *args)

    @classmethod
    def from_sct_config(cls, test_id: UUID, sct_config: SCTConfiguration) -> TestRunWithHeartbeat:
        # pylint: disable=too-many-locals
        if cls.TESTRUN_INSTANCE:
            raise ArgusTestRunError("Instance already initialized")

        LOGGER.info("Preparing Test Details...")
        job_name = get_job_name()
        job_url = get_job_url()
        if not job_name or job_name == "local_run":
            raise ArgusTestRunError("Will not track a locally run job")

        config_files = sct_config.get("config_files")
        started_by = get_username()

        # start time is a part of primary key and getting same start time will cause an overwrite
        safe_start_time = datetime.utcnow().replace(microsecond=0) + timedelta(seconds=(randint(10, 60)))

        details = TestDetails(scm_revision_id=get_git_commit_id(), started_by=started_by, build_job_url=job_url,   # pylint: disable=no-value-for-parameter
                              yaml_test_duration=sct_config.get("test_duration"),
                              start_time=safe_start_time,
                              config_files=config_files, packages=[])

        LOGGER.info("Preparing Resource Setup...")
        backend = sct_config.get("cluster_backend")
        region_key = cls.REGION_PROPERTY_MAP.get(backend, cls.REGION_PROPERTY_MAP["default"])
        raw_regions = sct_config.get(region_key) or "undefined_region"
        regions = raw_regions.split() if isinstance(raw_regions, str) else raw_regions
        primary_region = regions[0]

        sct_runner_info = CloudInstanceDetails(public_ip=get_sct_runner_ip(), provider=backend,
                                               region=primary_region, private_ip=get_my_ip())

        cloud_setup = cls.BACKEND_MAP.get(backend, _prepare_unknown_resource_setup)(sct_config)

        setup_details = TestResourcesSetup(sct_runner_host=sct_runner_info, region_name=regions,
                                           cloud_setup=cloud_setup)

        logs = TestLogs()
        resources = TestResources()
        results = TestResults(status=TestStatus.CREATED)

        run_info = TestRunInfo(details=details, setup=setup_details, resources=resources, logs=logs, results=results)
        LOGGER.info("Initializing TestRun...")
        cls.TESTRUN_INSTANCE = TestRunWithHeartbeat(test_id=test_id, build_id=job_name,  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
                                                    assignee=None,
                                                    run_info=run_info,
                                                    config=cls.config())

        return cls.TESTRUN_INSTANCE

    @classmethod
    def get(cls, test_id: UUID = None) -> TestRunWithHeartbeat:
        if test_id and not cls.TESTRUN_INSTANCE:
            cls.TESTRUN_INSTANCE = TestRunWithHeartbeat.from_id(test_id, config=cls.config())

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
