import time
import logging
import unittest.mock
from uuid import UUID

from cassandra import ConsistencyLevel
from argus.db.db_types import TestStatus
from argus.db.testrun import TestRunWithHeartbeat, TestRunInfo, TestDetails, TestResources, TestLogs, TestResults, \
    TestResourcesSetup
from argus.db.cloud_types import CloudInstanceDetails, AWSSetupDetails, GCESetupDetails, BaseCloudSetupDetails, \
    CloudNodesInfo
from argus.db.interface import ArgusDatabase
from argus.db.config import Config

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
    db_init = False

    def __init__(self):
        pass

    @classmethod
    def init_db(cls):
        if cls.db_init:
            LOGGER.warning("ArgusDB already initialized.")
            return
        LOGGER.info("Initializing ScyllaDB connection session...")
        ks = KeyStore()
        database = ArgusDatabase.from_config(Config(**ks.get_argusdb_credentials(), keyspace_name="argus"))
        database.session.default_consistency_level = ConsistencyLevel.QUORUM
        cls.db_init = True

    @classmethod
    def from_sct_config(cls, test_id: UUID, test_module_path: str, sct_config: SCTConfiguration) -> TestRunWithHeartbeat:  # pylint: disable=too-many-locals
        if cls.TESTRUN_INSTANCE:
            raise ArgusTestRunError("Instance already initialized")

        if not cls.db_init:
            cls.init_db()

        LOGGER.info("Preparing Test Details...")
        test_group, *_ = test_module_path.split(".")
        config_files = sct_config.get("config_files")
        job_name = get_job_name()
        job_url = get_job_url()
        started_by = get_username()
        release_name = sct_config.environment.get("release_name", get_git_current_branch())

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
        cls.TESTRUN_INSTANCE = TestRunWithHeartbeat(test_id=test_id, group=test_group, release_name=release_name, assignee="",
                                                    run_info=run_info)

        return cls.TESTRUN_INSTANCE

    @classmethod
    def get(cls, test_id: UUID = None) -> TestRunWithHeartbeat:
        if not cls.db_init:
            cls.init_db()

        if test_id and not cls.TESTRUN_INSTANCE:
            cls.TESTRUN_INSTANCE = TestRunWithHeartbeat.from_id(test_id)

        if not cls.TESTRUN_INSTANCE:
            return unittest.mock.MagicMock()

        return cls.TESTRUN_INSTANCE

    @classmethod
    def destroy(cls):
        ArgusDatabase.destroy()
        cls.db_init = False
        if not cls.TESTRUN_INSTANCE:
            return False
        cls.TESTRUN_INSTANCE.shutdown()
        cls.TESTRUN_INSTANCE = None
        return True
