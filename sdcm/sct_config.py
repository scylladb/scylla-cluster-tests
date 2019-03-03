"""
Handling Scylla-cluster-test configuration loading
"""

from __future__ import print_function

import os
import ast
import logging
from distutils.util import strtobool

import anyconfig

from sdcm.utils import get_s3_scylla_repos_mapping, get_scylla_ami_versions

LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods
class UnsetMarker(object):
    """
    Marker object for function empty default, used SCTConfiguration.get to mimic dict.get behavior
    """
    pass


def str_or_list(value):
    """
    Convert an environment variable into a python list

    :param value: raw string variable
    :return: list of strings
    """
    if isinstance(value, basestring):
        try:
            return ast.literal_eval(value)
        except Exception:  # pylint: disable=broad-except
            pass
        return [str(value)]

    elif isinstance(value, list):
        return value

    raise ValueError("{} isn't string or list".format(value))


def int_or_list(value):
    if isinstance(value, basestring):
        try:
            values = value.split()
            [int(v) for v in values]
            return value
        except Exception:  # pylint: disable=broad-except
            pass
    elif isinstance(value, int):
        return value

    raise ValueError("{} isn't int or list".format(value))


def boolean(value):
    if isinstance(value, bool):
        return value
    elif isinstance(value, basestring):
        return bool(strtobool(value))
    else:
        raise ValueError("{} isn't a boolean".format(type(value)))


class SCTConfiguration(dict):
    """
    Class the hold the SCT configuration
    """

    available_backends = ['aws', 'gce', 'docker', 'libvirt', 'baremetal', 'openstack']

    config_options = [
        dict(name="config_files", env="SCT_CONFIG_FILES", default=None, type=str_or_list, required=True,
             help="a list of config files that would be used"),

        dict(name="cluster_backend", env="SCT_CLUSTER_BACKEND", default="docker", type=str, required=True,
             help="backend that will be used, aws/gce/docker/libvirt/openstack"),

        dict(name="test_duration", env="SCT_TEST_DURATION", default=600, type=int, required=True,
             help="""
                  Test duration (min). Parameter used to keep instances produced by tests that are
                  supposed to run longer than 24 hours from being killed
             """),

        dict(name="n_db_nodes", env="SCT_N_DB_NODES", default=3, type=int_or_list, required=True,
             help="Number list of database nodes in multiple data centers."),

        dict(name="n_loaders", env="SCT_N_LOADERS", default=1, type=int_or_list, required=True,
             help="Number list of loader nodes in multiple data centers"),

        dict(name="n_monitor_nodes", env="SCT_N_MONITORS_NODES", default=1, type=int_or_list, required=True,
             help="Number list of monitor nodes in multiple data centers"),

        dict(name="failure_post_behavior", env="SCT_FAILURE_POST_BEHAVIOR", default='destroy', type=str, required=True,
             help="""
                Failure/post test behavior. i.e. what to do with the cloud instances at the end of the test.
                
                'destroy' - Destroy instances and credentials (default)
                'keep' - Keep instances running and leave credentials alone
                'stop' - Stop instances and leave credentials alone
             """),

        dict(name="endpoint_snitch", env="SCT_ENDPOINT_SNITCH", default=None, type=str, required=False,
             help="""
                The snitch class scylla would use
                
                'GossipingPropertyFileSnitch' - default 
                'Ec2MultiRegionSnitch' - default on aws backend
                'GoogleCloudSnitch'
             """),

        dict(name="user_credentials_path", env="SCT_USER_CREDENTIALS_PATH", default='~/.ssh/scylla-test', type=str, required=True,
             help="""Path to your user credentials. qa key are downloaded automatically from S3 bucket"""),

        dict(name="ip_ssh_connections", env="SCT_IP_SSH_CONNECTIONS", default='public', type=str, required=False,
             help="""
                Type of IP used to connect to machine instances.
                This depends on whether you are running your tests from a machine inside
                your cloud provider, where it makes sense to use 'private', or outside (use 'public')
                
                Default: Use public IPs to connect to instances (public)
                Use private IPs to connect to instances (private)
             """),

        dict(name="scylla_repo", env="SCT_SCYLLA_REPO",
             default=None, type=str,
             required=False,
             help="Url to the repo of scylla version to install scylla"),

        dict(name="scylla_version", env="SCT_SCYLLA_VERSION",
             default=None, type=str, required=False,
             help="""Version of scylla to install, ex. '2.3.1' 
                     Automatically lookup AMIs and repo links for formal versions. 
                     WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'"""),

        dict(name="scylla_repo_m", env="SCT_SCYLLA_REPO_M",
             default=None, type=str,
             required=False,
             help="Url to the repo of scylla version to install scylla from for managment tests"),

        dict(name="scylla_mgmt_repo", env="SCT_SCYLLA_MGMT_REPO",
             default=None, type=str, required=False,
             help="Url to the repo of scylla manager version to install for management tests"),

        dict(name="use_mgmt", env="SCT_USE_MGMT", default=None, type=boolean, required=False,
             help="When define true, will install scylla management"),

        dict(name="mgmt_port", env="SCT_MGMT_PORT", default=None, type=int, required=False,
             help="The port of scylla management"),

        dict(name="update_db_packages", env="SCT_UPDATE_DB_PACKAGES", default=None, type=str, required=False,
             help="""A local directory of rpms to install a custom version on top of
                     the scylla installed (or from repo or from ami)"""),

        dict(name="monitor_branch", env="SCT_MONITOR_BRANCH", default='branch-2.1', type=str, required=False,
             help="The port of scylla management"),

        dict(name="db_type", env="SCT_DB_TYPE", default='scylla', type=str, required=False,
             help="Db type to install into db nodes, scylla/cassandra"),

        dict(name="user_prefix", env="SCT_USER_PREFIX", default=False, type=str,
             required=True,
             help="the prefix of the name of the cloud instances, defaults to username"),

        dict(name="ami_id_db_scylla_desc", env="SCT_AMI_ID_DB_SCYLLA_DESC", default=None, type=str, required=False,
             help="version name to report stats to Elasticsearch"),

        dict(name="version_tag", env="SCT_VERSION_TAG", default=None, type=str, required=False,
             help="version name to be tagged on cloud instances"),

        dict(name="store_results_in_elasticsearch", env="SCT_STORE_RESULTS_IN_ELASTICSEARCH", default=True, type=boolean,
             required=True,
             help="save the results in elasticsearch"),

        dict(name="sct_public_ip", env="SCT_SCT_PUBLIC_IP", default=None, type=str, required=False,
             help="""
                Override the default hostname address of the sct test runner,
                for the monitoring of the Nemesis.
                can only work out of the box in AWS
             """),

        dict(name="reuse_cluster", env="SCT_REUSE_CLUSTER", default=False, type=str, required=False,
             help="""
            If true `test_id` would be used to run a test with existing cluster.
            You have to define all the nodes ip addresses both public and private:

            `reuse_cluster: True`
            `test_id: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`
            `db_nodes_public_ip: []`
            `db_nodes_private_ip: []`
            `loaders_public_ip: []`
            `loaders_private_ip: []`
            `monitor_nodes_public_ip: []`
            `monitor_nodes_private_ip: []` 
         """),

        dict(name="test_id", env="SCT_TEST_ID", default=None, type=str, required=False,
             help="""see [`reuse_cluster`](#reuse_cluster) for more info on usage."""),

        dict(name="seeds_first", env="SCT_SEEDS_FIRST", default=None, type=boolean, required=False,
             help="""If true would start and wait for the seed nodes to finish booting"""),

        dict(name="seeds_num", env="SCT_SEEDS_NUM", default=None, type=int, required=False,
             help="""Number of seeds to select, would be the first `seeds_num` of the cluster"""),

        dict(name="send_email", env="SCT_SEND_EMAIL", default=None, type=boolean, required=False,
             help="""If true would send email out of the performance regression test"""),

        dict(name="email_recipients", env="SCT_EMAIL_RECIPIENTS", default=None, type=str_or_list, required=False,
             help="""list of email of send the performance regression test to"""),

        # should be removed once stress commands would be refactored
        dict(name="bench_run", env="SCT_BENCH_RUN", default=None, type=boolean, required=False,
             help="""If true would kill the scylla-bench thread in the test teardown"""),

        # should be removed once stress commands would be refactored
        dict(name="fullscan", env="SCT_FULLSCAN", default=None, type=boolean, required=False,
             help="""If true would kill the fullscan thread in the test teardown"""),

        # Scylla command line arguments options

        dict(name="experimental", env="SCT_EXPERIMENTAL", default=None, type=boolean, required=False,
             help="when enabled scylla will use it's experimental features"),

        dict(name="enable_tc", env="SCT_ENABLE_TC", default=None, type=boolean, required=False,
             help="when enable scylla will use traffic control"),

        dict(name="server_encrypt", env="SCT_SERVER_ENCRYPT", default=None, type=boolean, required=False,
             help="when enable scylla will use encryption on the server side"),

        dict(name="client_encrypt", env="SCT_CLIENT_ENCRYPT", default=None, type=boolean, required=False,
             help="when enable scylla will use encryption on the client side"),

        dict(name="hinted_handoff_disabled", env="SCT_HINTED_HANDOFF_DISABLED", default=None, type=boolean, required=False,
             help="when enable scylla will disable hinted handoffs"),

        dict(name="authenticator", env="SCT_AUTHENTICATOR", default=None, type=str, required=False,
             help="which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator"),

        dict(name="append_scylla_args", env="SCT_APPEND_SCYLLA_ARGS", default=None, type=str, required=False,
             help="More arguments to append to scylla command line"),

        # Nemesis config options

        dict(name="nemesis_class_name", env="SCT_NEMESIS_CLASS_NAME", default='NoOpMonkey', type=str, required=False,
             help="""Nemesis class to use (possible types in sdcm.nemesis)."""),

        dict(name="nemesis_interval", env="SCT_NEMESIS_CLASS_NAME", default=None, type=int, required=False,
             help="""Nemesis sleep interval to use if None provided specifically in the test"""),

        dict(name="nemesis_during_prepare", env="SCT_NEMESIS_CLASS_NAME", default=False, type=boolean, required=False,
             help="""Run nemesis during prepare stage of the test"""),

        dict(name="space_node_threshold", env="SCT_SPACE_NODE_THRESHOLD", default=6442450944, type=int, required=False,
             help="""
                 Space node threshold before starting nemesis (bytes)
                 The default value is 6GB (6x1024^3 bytes)
                 This value is supposed to reproduce
                 https://github.com/scylladb/scylla/issues/1140
             """),

        # Stress Commands

        dict(name="stress_cmd", env="SCT_STRESS_CMD", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure. 
                    multiple commands can passed as a list"""),

        dict(name="gemini_url", env="SCT_GEMINI_URL", default=None, type=str, required=False,
             help="""Url of download of the binaries of gemini tool"""),

        dict(name="gemini_static_url", env="SCT_GEMINI_STATIC_URL", default=None, type=str, required=False,
             help="""Url of the schema/configuration the gemini tool would use """),

        dict(name="gemini_cmd", env="SCT_GEMINI_CMD", default=None, type=str, required=False,
             help="""gemini command to run (for now used only in GeminiTest)"""),

        # AWS config options

        dict(name="instance_type_loader", env="SCT_INSTANCE_TYPE_LOADER", default=None, type=str, required=False,
             help="AWS image type of the loader node"),

        dict(name="instance_type_monitor", env="SCT_INSTANCE_TYPE_MONITOR", default=None, type=str, required=False,
             help="AWS image type of the monitor node"),

        dict(name="instance_type_db", env="SCT_INSTANCE_TYPE_DB", default=None, type=str, required=False,
             help="AWS image type of the db node"),

        dict(name="region_name", env="SCT_REGION_NAME", default=None, type=str_or_list, required=False,
             help="AWS regions to use"),

        dict(name="security_group_ids", env="SCT_SECURITY_GROUP_IDS", default=None, type=str_or_list, required=False,
             help="AWS security groups ids to use"),

        dict(name="subnet_id", env="SCT_SUBNET_ID", default=None, type=str_or_list, required=False,
             help="AWS subnet ids to use"),

        dict(name="ami_id_db_scylla", env="SCT_AMI_ID_DB_SCYLLA", default=None, type=str, required=False,
             help="AMS AMI id to use for scylla db node"),

        dict(name="ami_id_loader", env="SCT_AMI_ID_LOADER", default=None, type=str, required=False,
             help="AMS AMI id to use for loader node"),

        dict(name="ami_id_monitor", env="SCT_AMI_ID_MONITOR", default=None, type=str, required=False,
             help="AMS AMI id to use for monitor node"),

        dict(name="ami_id_db_cassandra", env="SCT_AMI_ID_DB_CASSANDRA", default=None, type=str, required=False,
             help="AMS AMI id to use for cassandra node"),

        dict(name="aws_root_disk_size_monitor", env="SCT_AWS_ROOT_DISK_SIZE_MONITOR", default=None, type=str, required=False,
             help=""),

        dict(name="aws_root_disk_name_monitor", env="SCT_AWS_ROOT_DISK_NAME_MONITOR", default=None, type=str, required=False,
             help=""),

        dict(name="ami_db_scylla_user", env="SCT_AMI_DB_SCYLLA_USER", default=None, type=str, required=False,
             help=""),

        dict(name="ami_monitor_user", env="SCT_AMI_MONITOR_USER", default=None, type=str, required=False,
             help=""),

        dict(name="ami_loader_user", env="SCT_AMI_LOADER_USER", default=None, type=str, required=False,
             help=""),

        dict(name="ami_db_cassandra_user", env="SCT_AMI_DB_CASSANDRA_USER", default=None, type=str, required=False,
             help=""),

        dict(name="instance_provision", env="SCT_AMI_DB_CASSANDRA_USER", default='spot_low_price', type=str, required=False,
             help=" aws instance_provision: on_demand|spot_fleet|spot_low_price|spot_duration"),

        # GCE config options

        dict(name="gce_datacenter", env="SCT_GCE_DATACENTER", default=None, type=str, required=False,
             help=""),

        dict(name="gce_network", env="SCT_GCE_DATACENTER", default=None, type=str, required=False,
             help=""),

        dict(name="gce_image", env="SCT_GCE_IMAGE", default=None, type=str, required=False,
             help=""),

        dict(name="gce_image_username", env="SCT_GCE_IMAGE_USERNAME", default=None, type=str, required=False,
             help=""),

        dict(name="gce_instance_type_loader", env="SCT_GCE_INSTANCE_TYPE_LOADER", default=None, type=str, required=False,
             help=""),

        dict(name="gce_root_disk_type_loader", env="SCT_GCE_ROOT_DISK_TYPE_LOADER", default=None, type=str, required=False,
             help=""),

        dict(name="gce_n_local_ssd_disk_loader", env="SCT_GCE_N_LOCAL_SSD_DISK_LOADER", default=None, type=int, required=False,
             help=""),

        dict(name="gce_instance_type_monitor", env="SCT_GCE_INSTANCE_TYPE_MONITOR", default=None, type=str, required=False,
             help=""),

        dict(name="gce_root_disk_type_monitor", env="SCT_GCE_ROOT_DISK_TYPE_MONITOR", default=None, type=str, required=False,
             help=""),

        dict(name="gce_root_disk_size_monitor", env="SCT_GCE_ROOT_DISK_SIZE_MONITOR", default=None, type=int, required=False,
             help=""),

        dict(name="gce_n_local_ssd_disk_monitor", env="SCT_GCE_N_LOCAL_SSD_DISK_MONITOR", default=None, type=int, required=False,
             help=""),

        dict(name="gce_instance_type_db", env="SCT_GCE_INSTANCE_TYPE_DB", default=None, type=str, required=False,
             help=""),

        dict(name="gce_root_disk_type_db", env="SCT_GCE_ROOT_DISK_TYPE_DB", default=None, type=str, required=False,
             help=""),

        dict(name="gce_root_disk_size_db", env="SCT_GCE_ROOT_DISK_SIZE_DB", default=None, type=int, required=False,
             help=""),

        dict(name="gce_n_local_ssd_disk_db", env="SCT_GCE_N_LOCAL_SSD_DISK_DB", default=None, type=int, required=False,
             help=""),


        # docker config options

        dict(name="docker_image", env="SCT_DOCKER_IMAGE", default=None, type=str, required=False, help=""),

        # libvirt config options

        dict(name="libvirt_uri", env="SCT_LIBVIRT_URI", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_bridge", env="SCT_LIBVIRT_BRIDGE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_image", env="SCT_LIBVIRT_LOADER_IMAGE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_image_user", env="SCT_LIBVIRT_LOADER_IMAGE_USER", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_image_password", env="SCT_LIBVIRT_LOADER_IMAGE_PASSWORD", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_os_type", env="SCT_LIBVIRT_LOADER_OS_TYPE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_os_variant", env="SCT_LIBVIRT_LOADER_OS_VARIANT", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_loader_memory", env="SCT_LIBVIRT_LOADER_MEMORY", default=None, type=int, required=False,
             help=""),

        dict(name="libvirt_db_image", env="SCT_LIBVIRT_DB_IMAGE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_db_image_user", env="SCT_LIBVIRT_DB_IMAGE_USER", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_db_image_password", env="SCT_LIBVIRT_DB_IMAGE_PASSWORD", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_db_os_type", env="SCT_LIBVIRT_DB_OS_TYPE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_db_os_variant", env="SCT_LIBVIRT_DB_OS_VARIANT", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_db_memory", env="SCT_LIBVIRT_DB_MEMORY", default=None, type=int, required=False,
             help=""),

        dict(name="libvirt_monitor_image", env="SCT_LIBVIRT_MONITOR_IMAGE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_monitor_image_user", env="SCT_LIBVIRT_MONITOR_IMAGE_USER", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_monitor_image_password", env="SCT_LIBVIRT_MONITOR_IMAGE_PASSWORD", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_monitor_os_type", env="SCT_LIBVIRT_MONITOR_OS_TYPE", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_monitor_os_variant", env="SCT_LIBVIRT_MONITOR_OS_VARIANT", default=None, type=str, required=False,
             help=""),

        dict(name="libvirt_monitor_memory", env="SCT_LIBVIRT_MONITOR_MEMORY", default=None, type=int, required=False,
             help=""),

        # baremetal config options

        dict(name="db_nodes_private_ip", env="SCT_DB_NODES_PRIVATE_IP", default=None, type=str_or_list, required=False,
             help=""),

        dict(name="db_nodes_public_ip", env="SCT_DB_NODES_PUBLIC_IP", default=None, type=str_or_list, required=False,
             help=""),

        dict(name="loader_nodes_private_ip", env="SCT_LOADER_NODES_PRIVATE_IP", default=None, type=str_or_list, required=False,
             help=""),

        dict(name="loader_nodes_public_ip", env="SCT_LOADER_NODES_PUBLIC_IP", default=None, type=str_or_list, required=False,
             help=""),

        dict(name="monitor_nodes_private_ip", env="SCT_MONITOR_NODES_PRIVATE_IP", default=None, type=str_or_list,
             required=False,
             help=""),

        dict(name="monitor_nodes_public_ip", env="SCT_MONITOR_NODES_PUBLIC_IP", default=None, type=str_or_list,
             required=False,
             help=""),

        # openstack config options

        dict(name="openstack_user", env="SCT_OPENSTACK_USER", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_password", env="SCT_OPENSTACK_PASSWORD", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_tenant", env="SCT_OPENSTACK_TENANT", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_auth_version", env="SCT_OPENSTACK_AUTH_VERSION", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_auth_url", env="SCT_OPENSTACK_AUTH_URL", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_service_type", env="SCT_OPENSTACK_SERVICE_TYPE", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_service_name", env="SCT_OPENSTACK_SERVICE_NAME", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_service_region", env="SCT_OPENSTACK_SERVICE_REGION", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_instance_type_loader", env="SCT_OPENSTACK_INSTANCE_TYPE_LOADER", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_instance_type_db", env="SCT_OPENSTACK_INSTANCE_TYPE_DB", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_instance_type_monitor", env="SCT_OPENSTACK_INSTANCE_TYPE_MONITOR", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_image", env="SCT_OPENSTACK_IMAGE", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_image_username", env="SCT_OPENSTACK_IMAGE_USERNAME", default=None, type=str, required=False,
             help=""),

        dict(name="openstack_network", env="SCT_OPENSTACK_NETWORK", default=None, type=str, required=False,
             help=""),


        # test specific config parameters

        # GrowClusterTest
        dict(name="cassandra_stress_population_size", env="SCT_CASSANDRA_STRESS_POPULATION_SIZE", default=None, type=int, required=False,
             help=""),
        dict(name="cassandra_stress_threads", env="SCT_CASSANDRA_STRESS_THREADS", default=None, type=int, required=False,
             help=""),
        dict(name="add_node_cnt", env="SCT_ADD_NODE_CNT", default=None, type=int, required=False,
             help=""),

        # LongevityTest
        dict(name="stress_multiplier", env="SCT_STRESS_MULTIPLIER", default=None, type=int, required=False,
             help=""),
        dict(name="run_fullscan", env="SCT_RUN_FULLSCAN", default=None, type=boolean, required=False,
             help=""),
        dict(name="keyspace_num", env="SCT_KEYSPACE_NUM", default=None, type=int, required=False,
             help=""),
        dict(name="round_robin", env="SCT_ROUND_ROBIN", default=None, type=str, required=False,
             help=""),
        dict(name="batch_size", env="SCT_BATCH_SIZE", default=None, type=int, required=False,
             help=""),
        dict(name="pre_create_schema", env="SCT_PRE_CREATE_SCHEMA", default=None, type=boolean, required=False,
             help=""),

        dict(name="stress_read_cmd", env="SCT_STRESS_READ_CMD", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="prepare_verify_cmd", env="SCT_PREPARE_VERIFY_CMD", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure. 
            multiple commands can passed as a list"""),

        # MgmtCliTest
        dict(name="scylla_mgmt_upgrade_to_repo", env="SCT_SCYLLA_MGMT_UPGRADE_TO_REPO", default=None, type=str, required=False,
             help="Url to the repo of scylla manager version to upgrade to for management tests"),

        # PerformanceRegressionTest
        dict(name="stress_cmd_w", env="SCT_STRESS_CMD_W", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure. 
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_r", env="SCT_STRESS_CMD_R", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure. 
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_m", env="SCT_STRESS_CMD_M", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure. 
                    multiple commands can passed as a list"""),

        dict(name="prepare_write_cmd", env="SCT_PREPARE_WRITE_CMD", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                    You can specify everything but the -node parameter, which is going to
                    be provided by the test suite infrastructure. 
                    multiple commands can passed as a list"""),

        dict(name="stress_cmd_no_mv", env="SCT_STRESS_CMD_NO_MV", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_no_mv_profile", env="SCT_STRESS_CMD_NO_MV_PROFILE", default=None, type=str, required=False,
             help=""),

        # PerformanceRegressionUserProfilesTest
        dict(name="cs_user_profiles", env="SCT_CS_USER_PROFILES", default=None, type=str_or_list, required=False,
             help=""),
        dict(name="cs_duration", env="SCT_CS_DURATION", default=None, type=str, required=False,
             help=""),

        # RefreshTest
        dict(name="skip_download", env="SCT_SKIP_DOWNLOAD", default=None, type=str, required=False,
             help=""),
        dict(name="sstable_file", env="SCT_SSTABLE_FILE", default=None, type=str, required=False,
             help=""),
        dict(name="sstable_url", env="SCT_SSTABLE_URL", default=None, type=str, required=False,
             help=""),
        dict(name="sstable_md5", env="SCT_SSTABLE_MD5", default=None, type=str, required=False,
             help=""),
        dict(name="flush_times", env="SCT_FLUSH_TIMES", default=None, type=int, required=False,
             help=""),
        dict(name="flush_period", env="SCT_FLUSH_PERIOD", default=None, type=int, required=False,
             help=""),

        # UpgradeTest
        dict(name="new_scylla_repo", env="SCT_NEW_SCYLLA_REPO", default=None, type=str, required=False,
             help=""),
        dict(name="new_version", env="new_version", default=None, type=str, required=False,
             help=""),
        dict(name="upgrade_node_packages", env="SCT_UPGRADE_NODE_PACKAGES", default=None, type=int, required=False,
             help=""),
        dict(name="test_sst3", env="SCT_TEST_SST3", default=None, type=boolean, required=False,
             help=""),
        dict(name="new_introduced_pkgs", env="SCT_NEW_INTRODUCED_PKGS", default=None, type=str, required=False,
             help=""),
        dict(name="recover_system_tables", env="SCT_RECOVER_SYSTEM_TABLES", default=None, type=boolean, required=False,
             help=""),

        dict(name="stress_cmd_1", env="SCT_STRESS_CMD_1", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_sst3_prepare", env="SCT_STRESS_CMD_SST3_PREPARE", default=None, type=str_or_list, required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="prepare_write_stress", env="SCT_PREPARE_WRITE_STRESS", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_read_10m", env="SCT_STRESS_CMD_READ_10M", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_read_clall", env="SCT_STRESS_CMD_READ_CLALL", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_read_20m", env="SCT_STRESS_CMD_READ_20M", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_sst3_verify_read", env="SCT_STRESS_CMD_SST3_VERIFY_READ", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="stress_cmd_sst3_verify_more", env="SCT_STRESS_CMD_SST3_VERIFY_MORE", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),

        dict(name="disable_read_repair_chance", env="SCT_DISABLE_READ_REPAIR_CHANCE", default=None, type=str_or_list,
             required=False,
             help="""cassandra-stress commands. 
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure. 
                multiple commands can passed as a list"""),
    ]

    required_params = ['cluster_backend', 'test_duration', 'n_db_nodes', 'n_loaders', 'failure_post_behavior',
                       'user_credentials_path']

    # those can be added to a json scheme to validate / or write the validation code for it to be a bit clearer output
    backend_required_params = {
        'aws':  ['user_prefix', "instance_type_loader", "instance_type_monitor", "instance_type_db",
                 "region_name", "security_group_ids", "subnet_id", "ami_id_db_scylla", "ami_id_loader",
                 "ami_id_monitor", "aws_root_disk_size_monitor", "aws_root_disk_name_monitor", "ami_db_scylla_user",
                 "ami_monitor_user"],

        'gce': ['user_prefix', 'gce_network', 'gce_image', 'gce_image_username', 'gce_instance_type_db', 'gce_root_disk_type_db',
                'gce_root_disk_size_db', 'gce_n_local_ssd_disk_db', 'gce_instance_type_loader',
                'gce_root_disk_type_loader', 'gce_n_local_ssd_disk_loader', 'gce_instance_type_monitor',
                'gce_root_disk_type_monitor', 'gce_root_disk_size_monitor', 'gce_n_local_ssd_disk_monitor',
                'gce_datacenter', 'scylla_repo'],

        'docker': ['docker_image', 'user_credentials_path', 'scylla_repo'],

        'libvirt':  ['libvirt_uri', 'libvirt_bridge', 'scylla_repo'],

        'baremetal': ['db_nodes_private_ip', 'db_nodes_public_ip', 'user_credentials_path'],

        'openstack': ['openstack_user', 'openstack_password', 'openstack_tenant', 'openstack_auth_version',
                      'openstack_auth_url', 'openstack_service_type', 'openstack_service_name', 'openstack_service_region',
                      'openstack_instance_type_loader', 'openstack_instance_type_db', 'openstack_instance_type_monitor',
                      'openstack_image', 'openstack_image_username', 'openstack_network']
    }

    defaults_config_files = {
        "aws": ['defaults/aws_config.yaml'],
        "gce": ['defaults/gce_config.yaml'],
        "docker": ['defaults/docker_config.yaml'],
        "libvirt": ['defaults/libvirt_config.yaml'],
        "baremetal": [],
        "openstack": ['defaults/openstack_config.yaml']
    }

    multi_region_params = [
        'region_name'
    ]

    def __init__(self):
        super(SCTConfiguration, self).__init__()

        env = self._load_environment_variables()
        config_files = env.get('config_files', [])

        # prepend to the config list the defaults the config files
        backend = env.get('cluster_backend')
        backend_config_files = []
        if backend:
            backend_config_files = self.defaults_config_files[str(backend)]

        # 0) get defaults
        defaults = self._get_defaults()
        anyconfig.merge(self, defaults)

        # 1) load the default backend config files
        files = anyconfig.load(list(backend_config_files))
        anyconfig.merge(self, files)

        regions_data = self.get('regions_data', {})
        if regions_data:
            del self['regions_data']

        # 2) load the config files
        files = anyconfig.load(list(config_files))
        anyconfig.merge(self, files)

        # 2.2) load the region data
        region_names = self.get('region_name', '').split()
        for region in region_names:
            for k, v in regions_data[region].items():
                if k not in self.keys():
                    self[k] = v
                else:
                    self[k] += " {}".format(v)

        # 3) overwrite with environment variables
        anyconfig.merge(self, env)

        # 4) assume multi dc by n_db_nodes set size
        num_of_regions = len(self.get('region_name', '').split())
        num_of_db_nodes_sets = len(str(self.get('n_db_nodes', '')).split(' '))
        if num_of_db_nodes_sets > num_of_regions:
            for region in regions_data.keys()[:num_of_db_nodes_sets]:
                for k, v in regions_data[region].items():
                    if k not in self.keys():
                        self[k] = v
                    else:
                        self[k] += " {}".format(v)

        # 5) handle scylla_version if exists
        scylla_version = self.get('scylla_version', None)
        if scylla_version:
            # Look for the version, and return it's info ami + repo
            # According to backend, populate 'scylla_repo' or 'ami_id_db_scylla'
            if 'ami_id_db_scylla' not in self and self.get('cluster_backend') == 'aws':
                amis = get_scylla_ami_versions(self.get('region_name'))
                for ami in amis:
                    if scylla_version in ami['Name']:
                        self['ami_id_db_scylla'] = ami['ImageId']
                        break
                else:
                    raise ValueError("AMI for scylla version {} wasn't found".format(scylla_version))

            elif 'scylla_repo' not in self:
                repo_map = get_s3_scylla_repos_mapping()

                for key in repo_map.keys():
                    if scylla_version.startswith(key):
                        self['scylla_repo'] = repo_map[key]
                        break
                else:
                    raise ValueError("repo for scylla version {} wasn't found".format(scylla_version))

            else:
                raise ValueError("'scylla_version' can't used together with  'ami_id_db_scylla' or with 'scylla_repo'")
        LOGGER.info(self.dump_config())

    @classmethod
    def get_config_option(cls, name):
        return [o for o in cls.config_options if o['name'] == name][0]

    def _get_defaults(self):
        config_defaults = dict()
        for opt in self.config_options:
            if 'default' not in opt: raise Exception(str(opt))
            if opt['default'] is not None:
                config_defaults[opt['name']] = opt['default']
        return config_defaults

    def _load_environment_variables(self):
        environment_vars = {}
        for opt in self.config_options:
            if opt['env'] in os.environ:
                try:
                    environment_vars[opt['name']] = opt['type'](os.environ[opt['env']])
                except Exception as ex:
                    raise ValueError("failed to parse {} from environment variable:\n\t\t{}".format(opt['env'], ex))
        return environment_vars

    def get(self, key, default=UnsetMarker):
        """
        get a specific configuration by key
        :param key: the key to get
        :param default: default value if the key doesn't exist
        :return: int / str / list
        """

        if default is not UnsetMarker:
            ret_val = super(SCTConfiguration, self).get(key, default)
        else:
            ret_val = super(SCTConfiguration, self).get(key)

        if key in self.multi_region_params and isinstance(ret_val, list):
            ret_val = ' '.join(ret_val)

        return ret_val

    def _validate_value(self, opt):
        try:
            opt['type'](self.get(opt['name']))
        except Exception as ex:
            raise ValueError("failed to validate {}:\n\t\t{}".format(opt['name'], ex))

    def verify_configuration(self):
        """
        Check that all required values are set, and validated each value to be of correct type or value
        also check required options per backend

        :return: None
        :raises ValueError: on failures in validations
        :raise Exception: on unsupported backends
        """

        # validate passed configuration
        for opt in self.config_options:
            if opt['required']:
                assert opt['name'] in self, "{} missing from config".format(opt['name'])
                self._validate_value(opt)
            else:
                if opt['name'] in self:
                    self._validate_value(opt)

        # check for unsupported configuration
        config_names = set([o['name'] for o in self.config_options])
        unsupported_option = set(self.keys()).difference(config_names)

        if unsupported_option:
            res = "Unsupported config option/s found:\n"
            for option in unsupported_option:
                res += "\t * '{}: {}'\n".format(option, self[option])
            raise ValueError(res)

        # validated per backend
        def _check_backend_defaults(backend, required_params):
            opts = [o for o in self.config_options if o['name'] in required_params]
            for _opt in opts:
                assert _opt['name'] in self, "{} missing from config for {}".format(_opt['name'], backend)

        backend = self.get('cluster_backend')
        if backend in self.available_backends:
            _check_backend_defaults(backend, self.backend_required_params[backend])
        else:
            raise ValueError("Unsupported backend [{}]".format(backend))

    def dump_config(self):
        """
        Dump current configuration to string

        :return: str
        """
        return anyconfig.dumps(self, ac_parser="yaml")

    def dump_help_config_markdown(self):
        """
        Dump all configuration options with their defaults and help to string in markdown format

        :return: str
        """
        header = """
            # scylla-cluster-tests configuration options
            | Parameter | Description  | Default | Override environment<br>variable 
            | :-------  | :----------  | :------ | :-------------------------------   
        """

        def strip_help_text(text):
            """
            strip all lines, and also remove empty lines from start or end
            """
            output = [l.strip() for l in text.splitlines()]
            return '\n'.join(output[1 if not output[0] else 0:-1 if not output[-1] else None])

        ret = strip_help_text(header) + '\n'

        for opt in self.config_options:
            if opt['help']:
                help_text = '<br>'.join(strip_help_text(opt['help']).splitlines())
            else:
                help_text = ''
            default_text = opt['default'] if opt['default'] is not None else 'N/A'
            ret += """| **<a name="{name}">{name}</a>**  | {help_text} | {default_text} | {env}\n""".format(help_text=help_text, default_text=default_text, **opt)

        return ret

    def dump_help_config_yaml(self):
        """
        Dump all configuration options with their defaults and help to string in yaml format

        :return: str
        """
        ret = ""
        for opt in self.config_options:
            if opt['help']:
                help_text = '\n'.join(["# {}".format(l.strip()) for l in opt['help'].splitlines() if l.strip()]) + '\n'
            else:
                help_text = ''
            ret += "{help_text}{name}: {default}\n\n".format(help_text=help_text, **opt)

        return ret


if __name__ == "__main__":
    import unittest

    # pylint: disable=missing-docstring
    class ConfigurationTests(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            logging.basicConfig(level=logging.ERROR)
            logging.getLogger('botocore').setLevel(logging.CRITICAL)
            logging.getLogger('boto3').setLevel(logging.CRITICAL)
            logging.getLogger('anyconfig').setLevel(logging.ERROR)

            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

            cls.conf = SCTConfiguration()

        def tearDown(self):
            for k, _ in os.environ.items():
                if k.startswith('SCT_'):
                    del os.environ[k]
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        def test_01_dump_config(self):
            logging.debug(self.conf.dump_config())

        def test_02_verify_config(self):
            self.conf.verify_configuration()

        def test_03_dump_help_config_yaml(self):
            logging.debug(self.conf.dump_help_config_yaml())

        def test_03_dump_help_config_markdown(self):
            logging.debug(self.conf.dump_help_config_markdown())

        @staticmethod
        def test_04_check_env_parse():
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_REGION_NAME'] = '["eu-west1", "us-east2"]'
            os.environ['SCT_N_DB_NODES'] = '2 2 2'
            os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
            os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

            conf = SCTConfiguration()
            conf.verify_configuration()
            conf.dump_config()

        def test_05_docker(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

            conf = SCTConfiguration()
            conf.verify_configuration()
            self.assertIn('docker_image', conf.dump_config())
            self.assertEqual(conf.get('docker_image'), 'scylladb/scylla')

        def test_06_libvirt(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'libvirt'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
            conf = SCTConfiguration()
            conf.verify_configuration()
            self.assertIn('libvirt_uri', conf.dump_config())
            self.assertEqual(conf.get('libvirt_uri'), 'qemu:///system')

        def test_07_baremetal_exception(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
            conf = SCTConfiguration()
            self.assertRaises(AssertionError, conf.verify_configuration)

        def test_08_baremetal(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
            os.environ['SCT_DB_NODES_PRIVATE_IP'] = '["1.2.3.4", "1.2.3.5"]'
            os.environ['SCT_DB_NODES_PUBLIC_IP'] = '["1.2.3.4", "1.2.3.5"]'
            conf = SCTConfiguration()
            conf.verify_configuration()

            self.assertIn('db_nodes_private_ip', conf.dump_config())
            self.assertEqual(conf.get('db_nodes_private_ip'), ["1.2.3.4", "1.2.3.5"])

        def test_09_unknown_configure(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
            conf = SCTConfiguration()
            self.assertRaises(ValueError, conf.verify_configuration)

        def test_10_longevity(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/complex_test_case_with_version.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_10_mananger_regression(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_11_openstack(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'openstack'
            conf = SCTConfiguration()
            conf.verify_configuration()
            self.assertIn('openstack_auth_url', conf.dump_config())
            self.assertEqual(conf.get('openstack_auth_url'), 'http://1.2.3.4:5000')

        def test_12_scylla_version_ami(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_12_scylla_version_ami_case1(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
            os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_12_scylla_version_ami_case2(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            self.assertRaisesRegexp(ValueError, r"AMI for scylla version 99.0.3 wasn't found", SCTConfiguration)

        def test_12_scylla_version_repo(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_12_scylla_version_repo_case1(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
            os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()
            conf.verify_configuration()

        def test_12_scylla_version_repo_case2(self):
            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            self.assertRaisesRegexp(ValueError, r"repo for scylla version 99.0.3 wasn't found", SCTConfiguration)

        def test_config_dupes(self):
            import itertools

            def get_dupes(c):
                '''sort/tee/izip'''
                a, b = itertools.tee(sorted(c))
                next(b, None)
                r = None
                for k, g in itertools.izip(a, b):
                    if k != g: continue
                    if k != r:
                        yield k
                        r = k

            opts = [o['name'] for o in SCTConfiguration.config_options]

            self.assertListEqual(list(get_dupes(opts)), [])

        def test_13_bool(self):

            os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
            os.environ['SCT_STORE_RESULTS_IN_ELASTICSEARCH'] = 'False'
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
            conf = SCTConfiguration()

            self.assertEqual(conf['store_results_in_elasticsearch'], False)

    unittest.main()
