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
# Copyright (c) 2016 ScyllaDB

# pylint: disable=too-many-lines

import queue
import getpass
import logging
import os
import random
import re
import tempfile
import threading
import time
import uuid
import itertools
from typing import List
from textwrap import dedent
from datetime import datetime
import subprocess

from invoke.exceptions import UnexpectedExit, Failure, CommandTimedOut
import yaml
import requests
from paramiko import SSHException
from paramiko.ssh_exception import NoValidConnectionsError


from sdcm.collectd import ScyllaCollectdSetup
from sdcm.mgmt import ScyllaManagerError, get_scylla_manager_tool
from sdcm.prometheus import start_metrics_server, PrometheusAlertManagerListener
from sdcm.rsyslog_daemon import start_rsyslog
from sdcm.log import SDCMAdapter
from sdcm.remote import RemoteCmdRunner, LocalCmdRunner, NETWORK_EXCEPTIONS
from sdcm import wait
from sdcm.utils.common import log_run_info, retrying, get_data_dir_path, verify_scylla_repo_file, S3Storage, \
    get_latest_gemini_version, get_my_ip, makedirs, normalize_ipv6_url, deprecation, download_dir_from_cloud
from sdcm.utils.distro import Distro
from sdcm.utils.thread import raise_event_on_failure
from sdcm.utils.remotewebbrowser import RemoteWebDriverContainer
from sdcm.utils.version_utils import get_gemini_version
from sdcm.sct_events import Severity, CoreDumpEvent, DatabaseLogEvent, \
    ClusterHealthValidatorEvent, set_grafana_url, ScyllaBenchEvent
from sdcm.auto_ssh import start_auto_ssh, RSYSLOG_SSH_TUNNEL_LOCAL_PORT
from sdcm.logcollector import GrafanaSnapshot, GrafanaScreenShot, PrometheusSnapshots, MonitoringStack

SCYLLA_CLUSTER_DEVICE_MAPPINGS = [{"DeviceName": "/dev/xvdb",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}},
                                  {"DeviceName": "/dev/xvdc",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}}]

CREDENTIALS = []
DEFAULT_USER_PREFIX = getpass.getuser()
# Test duration (min). Parameter used to keep instances produced by tests that
# are supposed to run longer than 24 hours from being killed
TEST_DURATION = 60
IP_SSH_CONNECTIONS = 'private'
TASK_QUEUE = 'task_queue'
RES_QUEUE = 'res_queue'
WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SCYLLA_YAML_PATH = "/etc/scylla/scylla.yaml"
SCYLLA_MANAGER_YAML_PATH = "/etc/scylla-manager/scylla-manager.yaml"
SCYLLA_DIR = "/var/lib/scylla"

INSTANCE_PROVISION_ON_DEMAND = 'on_demand'
SPOT_TERMINATION_CHECK_DELAY = 5

LOGGER = logging.getLogger(__name__)
LOCALRUNNER = LocalCmdRunner()


def set_ip_ssh_connections(ip_type):
    # pylint: disable=global-statement
    global IP_SSH_CONNECTIONS
    IP_SSH_CONNECTIONS = ip_type


def set_duration(duration):
    # pylint: disable=global-statement
    global TEST_DURATION
    TEST_DURATION = duration


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


class Setup:
    KEEP_ALIVE_DB_NODES = False
    KEEP_ALIVE_LOADER_NODES = False
    KEEP_ALIVE_MONITOR_NODES = False

    REUSE_CLUSTER = False
    MIXED_CLUSTER = False
    MULTI_REGION = False
    BACKTRACE_DECODING = False
    INTRA_NODE_COMM_PUBLIC = False
    RSYSLOG_ADDRESS = None
    DECODING_QUEUE = None

    _test_id = None
    _test_name = None
    TAGS = dict()
    _logdir = None

    @classmethod
    def test_id(cls):
        return cls._test_id

    @classmethod
    def set_test_id(cls, test_id):
        if not cls._test_id:
            cls._test_id = str(test_id)
            test_id_file_path = os.path.join(cls.logdir(), "test_id")
            with open(test_id_file_path, "w") as test_id_file:
                test_id_file.write(str(test_id))
        else:
            LOGGER.warning("TestID already set!")

    @classmethod
    def logdir(cls):
        if not cls._logdir:
            sct_base = os.path.expanduser(os.environ.get('_SCT_LOGDIR', '~/sct-results'))
            date_time_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            cls._logdir = os.path.join(sct_base, str(date_time_formatted))
            makedirs(cls._logdir)

            latest_symlink = os.path.join(sct_base, 'latest')
            if os.path.islink(latest_symlink):
                os.remove(latest_symlink)
            os.symlink(os.path.relpath(cls._logdir, sct_base), latest_symlink)
            os.environ['_SCT_TEST_LOGDIR'] = cls._logdir
        return cls._logdir

    @classmethod
    def test_name(cls):
        return cls._test_name

    @classmethod
    def set_test_name(cls, test_name):
        cls._test_name = test_name

    @classmethod
    def set_multi_region(cls, multi_region):
        cls.MULTI_REGION = multi_region

    @classmethod
    def set_decoding_queue(cls):
        cls.DECODING_QUEUE = queue.Queue()

    @classmethod
    def set_intra_node_comm_public(cls, intra_node_comm_public):
        cls.INTRA_NODE_COMM_PUBLIC = intra_node_comm_public

    @classmethod
    def reuse_cluster(cls, val=False):
        cls.REUSE_CLUSTER = val

    @classmethod
    def keep_cluster(cls, node_type, val='destroy'):
        if "db_nodes" in node_type:
            cls.KEEP_ALIVE_DB_NODES = bool(val == 'keep')
        if "loader_nodes" in node_type:
            cls.KEEP_ALIVE_LOADER_NODES = bool(val == 'keep')
        if "monitor_nodes" in node_type:
            cls.KEEP_ALIVE_MONITOR_NODES = bool(val == 'keep')

    @classmethod
    def mixed_cluster(cls, val=False):
        cls.MIXED_CLUSTER = val

    @classmethod
    def tags(cls, key, value):
        cls.TAGS[key] = value

    @classmethod
    def configure_rsyslog(cls, enable_ngrok=False):

        port = start_rsyslog(cls.test_id(), cls.logdir())

        if enable_ngrok:
            requests.delete('http://localhost:4040/api/tunnels/rsyslogd')

            tunnel = {
                "addr": port,
                "proto": "tcp",
                "name": "rsyslogd",
                "bind_tls": False
            }
            res = requests.post('http://localhost:4040/api/tunnels', json=tunnel)
            assert res.ok, "failed to add a ngrok tunnel [{}, {}]".format(res, res.text)
            ngrok_address = res.json()['public_url'].replace('tcp://', '')

            address, port = ngrok_address.split(':')
        else:
            address = get_my_ip()

        cls.RSYSLOG_ADDRESS = (address, port)

    @classmethod
    def get_startup_script(cls):
        post_boot_script = '#!/bin/bash'
        post_boot_script += dedent(r'''
               sudo sed -i 's/#MaxSessions \(.*\)$/MaxSessions 1000/' /etc/ssh/sshd_config
               sudo sed -i 's/#MaxStartups \(.*\)$/MaxStartups 60/' /etc/ssh/sshd_config
               sudo sed -i 's/#LoginGraceTime \(.*\)$/LoginGraceTime 15s/' /etc/ssh/sshd_config
               sudo systemctl restart sshd
               ''')
        if cls.RSYSLOG_ADDRESS:

            if IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION:
                post_boot_script += dedent('''
                       sudo echo 'action(type="omfwd" Target="{0}" Port="{1}" Protocol="tcp")'>> /etc/rsyslog.conf
                       sudo systemctl restart rsyslog
                       '''.format('127.0.0.1', RSYSLOG_SSH_TUNNEL_LOCAL_PORT))
            else:
                post_boot_script += dedent('''
                       sudo echo 'action(type="omfwd" Target="{0}" Port="{1}" Protocol="tcp")'>> /etc/rsyslog.conf
                       sudo systemctl restart rsyslog
                       '''.format(*cls.RSYSLOG_ADDRESS))  # pylint: disable=not-an-iterable

        post_boot_script += dedent(r'''
               sed -i -e 's/^\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*4096/*\t\tsoft\tnproc\t\tunlimited/' \
               /etc/security/limits.d/20-nproc.conf
               echo -e '*\t\thard\tnproc\t\tunlimited' >> /etc/security/limits.d/20-nproc.conf
               ''')
        return post_boot_script


def get_username():

    def is_email_in_scylladb_domain(email_addr):
        return bool(email_addr and "@scylladb.com" in email_addr)

    def get_email_user(email_addr):
        return email_addr.strip().split("@")[0]

    # First check that we running on Jenkins try to get user email
    email = os.environ.get('BUILD_USER_EMAIL')
    if is_email_in_scylladb_domain(email):
        return get_email_user(email)
    user_id = os.environ.get('BUILD_USER_ID')
    if user_id:
        return user_id
    current_linux_user = getpass.getuser()
    if current_linux_user == "jenkins":
        return current_linux_user
    # We are not on Jenkins and running in Hydra, try to get email from Git
    # when running in Hydra there are env issues so we pass it using SCT_GIT_USER_EMAIL variable before docker run
    git_user_email = os.environ.get('GIT_USER_EMAIL')
    if is_email_in_scylladb_domain(git_user_email):
        return get_email_user(git_user_email)
    # we are outside of Hydra
    res = LOCALRUNNER.run(cmd="git config --get user.email", ignore_status=True)
    if is_email_in_scylladb_domain(res.stdout):
        return get_email_user(res.stdout)
    # we didn't find email, fallback to current user with unknown email user identifier
    return "linux_user={}".format(current_linux_user)


def create_common_tags():
    build_tag = os.environ.get('BUILD_TAG', None)
    job_name = os.environ.get('JOB_NAME', None)
    tags = dict(RunByUser=get_username(),
                TestName=str(Setup.test_name()),
                TestId=str(Setup.test_id()))

    if build_tag:
        tags["JenkinsJobTag"] = build_tag

    # In order to calculate costs by version in AWS, we will use the root dir of the job in jenkins as the version.
    if job_name:
        tags["version"] = job_name.split('/')[0]
    else:
        tags["version"] = "unknown"

    tags.update(Setup.TAGS)
    return tags


class NodeError(Exception):

    def __init__(self, msg=None):
        super(NodeError, self).__init__()
        self.msg = msg

    def __str__(self):
        if self.msg is not None:
            return self.msg
        else:
            return ""


class PrometheusSnapshotErrorException(Exception):
    pass


def prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class UserRemoteCredentials():

    def __init__(self, key_file):
        self.type = 'user'
        self.key_file = key_file
        self.name = os.path.basename(self.key_file)
        self.key_pair_name = self.name

    def __str__(self):
        return "Key Pair %s -> %s" % (self.name, self.key_file)

    def write_key_file(self):
        pass

    def destroy(self):
        pass


class BaseNode():  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    CQL_PORT = 9042

    def __init__(self, name, parent_cluster, ssh_login_info=None, base_logdir=None, node_prefix=None, dc_idx=0):  # pylint: disable=too-many-arguments,unused-argument
        self.name = name
        self.is_seed = False
        self.dc_idx = dc_idx
        self.parent_cluster = parent_cluster  # reference to the Cluster object that the node belongs to
        self.logdir = os.path.join(base_logdir, self.name)
        makedirs(self.logdir)
        self.log = SDCMAdapter(LOGGER, extra={'prefix': str(self)})

        ssh_login_info['hostname'] = self.external_address

        self.remoter = RemoteCmdRunner(**ssh_login_info)
        self.ssh_login_info = ssh_login_info

        self.log.debug(self.remoter.ssh_debug_cmd())

        self._spot_monitoring_thread = None
        self._journal_thread = None
        self._docker_log_process = None
        self.n_coredumps = 0
        self._discovered_backtraces = []
        self._maximum_number_of_cores_to_publish = 10

        self.last_line_no = 1
        self.last_log_position = 0
        self._backtrace_thread = None
        self._db_log_reader_thread = None
        self._scylla_manager_journal_thread = None
        self._decoding_backtraces_thread = None
        self._init_system = None
        self.db_init_finished = False

        self._short_hostname = None
        self._alert_manager = None

        self._database_log_errors_index = []
        self._database_error_events = [DatabaseLogEvent(type='NO_SPACE_ERROR', regex='No space left on device'),
                                       DatabaseLogEvent(type='UNKNOWN_VERB',
                                                        regex='unknown verb exception',
                                                        severity=Severity.WARNING),
                                       DatabaseLogEvent(
                                           type='BROKEN_PIPE', severity=Severity.WARNING,
                                           regex='cql_server - exception while processing connection:.*Broken pipe'),
                                       DatabaseLogEvent(type='DATABASE_ERROR', regex='Exception '),
                                       DatabaseLogEvent(type='BAD_ALLOC', regex='std::bad_alloc'),
                                       DatabaseLogEvent(type='SCHEMA_FAILURE', regex='Failed to load schema version'),
                                       DatabaseLogEvent(type='RUNTIME_ERROR', regex='std::runtime_error'),
                                       DatabaseLogEvent(type='FILESYSTEM_ERROR', regex='filesystem_error'),
                                       DatabaseLogEvent(type='STACKTRACE', regex='stacktrace'),
                                       DatabaseLogEvent(type='BACKTRACE', regex='backtrace', severity=Severity.ERROR),
                                       DatabaseLogEvent(type='SEGMENTATION', regex='segmentation'),
                                       DatabaseLogEvent(type='INTEGRITY_CHECK', regex='integrity check failed'),
                                       DatabaseLogEvent(type='REACTOR_STALLED', regex='Reactor stalled',
                                                        severity=Severity.WARNING),
                                       DatabaseLogEvent(type='SEMAPHORE_TIME_OUT', regex='semaphore_timed_out'),
                                       DatabaseLogEvent(type='BOOT', regex='Starting Scylla Server',
                                                        severity=Severity.NORMAL),
                                       DatabaseLogEvent(type='SUPPRESSED_MESSAGES', regex='journal: Suppressed',
                                                        severity=Severity.WARNING)]

        self.termination_event = threading.Event()
        self._running_nemesis = None
        if not Setup.REUSE_CLUSTER:
            self.set_hostname()
        self.start_task_threads()
        # We should disable bootstrap when we create nodes to establish the cluster,
        # if we want to add more nodes when the cluster already exists, then we should
        # enable bootstrap.
        self.enable_auto_bootstrap = False
        self.scylla_version = ''
        self._is_enterprise = None
        self.replacement_node_ip = None  # if node is a replacement for a dead node, store dead node private ip here
        self._distro = None
        self._kernel_version = None
        self._cassandra_stress_version = None
        self.lock = threading.Lock()

        if (IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION) and Setup.RSYSLOG_ADDRESS:
            start_auto_ssh(Setup.test_id(), self, Setup.RSYSLOG_ADDRESS[1], RSYSLOG_SSH_TUNNEL_LOCAL_PORT)

    @property
    def short_hostname(self):
        if not self._short_hostname:
            try:
                self._short_hostname = self.remoter.run('hostname -s').stdout.strip()
            except Exception:  # pylint: disable=broad-except
                return "no_booted_yet"
        return self._short_hostname

    @property
    def database_log(self):
        orig_log_path = os.path.join(self.logdir, 'database.log')

        if Setup.RSYSLOG_ADDRESS:
            rsys_log_path = os.path.join(Setup.logdir(), 'hosts', self.short_hostname, 'messages.log')
            if os.path.exists(rsys_log_path) and (not os.path.islink(orig_log_path)):
                os.symlink(os.path.relpath(rsys_log_path, self.logdir), orig_log_path)
            return rsys_log_path
        else:
            return orig_log_path

    @property
    def cassandra_stress_version(self):
        if not self._cassandra_stress_version:
            result = self.remoter.run(cmd="cassandra-stress version", ignore_status=True, verbose=True)
            match = re.match("Version: (.*)", result.stdout)
            if match:
                self._cassandra_stress_version = match.group(1)
            else:
                self.log.error("C-S version not found!")
                self._cassandra_stress_version = "unknown"
        return self._cassandra_stress_version

    @property
    def running_nemesis(self):
        return self._running_nemesis

    @running_nemesis.setter
    def running_nemesis(self, nemesis):
        """Set name of nemesis which is started on node

        Decorators:
            running_nemesis.setter

        Arguments:
            nemesis {str} -- class name of Nemesis
        """

        # Only one Nemesis could run on node. Limitation
        # of first version for X parallel Nemesis

        with self.lock:
            self._running_nemesis = nemesis

    @property
    def distro(self):
        if not self._distro:
            self.log.info("Trying to detect Linux distribution...")
            self._distro = Distro.from_os_release(self.remoter.run("cat /etc/os-release", ignore_status=True).stdout)
        return self._distro

    @property
    def is_client_encrypt(self):
        result = self.remoter.run(
            "grep ^client_encryption_options: /etc/scylla/scylla.yaml -A 3 | grep enabled | awk '{print $2}'", ignore_status=True)
        return 'true' in result.stdout.lower()

    @property
    def is_server_encrypt(self):
        result = self.remoter.run("grep '^server_encryption_options:' /etc/scylla/scylla.yaml", ignore_status=True)
        return 'server_encryption_options' in result.stdout.lower()

    def extract_seeds_from_scylla_yaml(self):
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct'), 'scylla.yaml')
        wait.wait_for(func=self.remoter.receive_files, step=10, text='Waiting for copying scylla.yaml', timeout=300,
                      throw_exc=True, src=SCYLLA_YAML_PATH, dst=yaml_dst_path)
        with open(yaml_dst_path, 'r') as yaml_stream:
            try:
                conf_dict = yaml.safe_load(yaml_stream)
            except Exception:
                yaml_stream.seek(0)
                self.log.error('Parsing failed. Scylla YAML config contents:')
                self.log.exception(yaml_stream.read())
                raise

            try:
                node_seeds = conf_dict['seed_provider'][0]['parameters'][0].get('seeds')
            except Exception as details:
                self.log.debug('Loaded YAML data structure: %s', conf_dict)
                raise ValueError('Exception determining seed node ips: %s' % details)

            if node_seeds:
                return node_seeds.split(',')
            else:
                raise Exception('Seeds not found in the scylla.yaml')

    def is_centos7(self):
        deprecation("consider to use node.distro.is_centos7 property instead")
        return self.distro.is_centos7

    def is_rhel7(self):
        deprecation("consider to use node.distro.is_rhel7 property instead")
        return self.distro.is_rhel7

    def is_rhel_like(self):
        deprecation("consider to use node.distro.is_rhel_like property instead")
        return self.distro.is_rhel_like

    def is_ubuntu14(self):
        deprecation("consider to use node.distro.is_ubuntu14 property instead")
        return self.distro.is_ubuntu14

    def is_ubuntu16(self):
        deprecation("consider to use node.distro.is_ubuntu16 property instead")
        return self.distro.is_ubuntu16

    def is_ubuntu18(self):
        deprecation("consider to use node.distro.is_ubuntu18 property instead")
        return self.distro.is_ubuntu18

    def is_ubuntu(self):
        deprecation("consider to use node.distro.is_ubuntu property instead")
        return self.distro.is_ubuntu

    def is_debian8(self):
        deprecation("consider to use node.distro.is_debian8 property instead")
        return self.distro.is_debian8

    def is_debian9(self):
        deprecation("consider to use node.distro.is_debian9 property instead")
        return self.distro.is_debian9

    def is_debian(self):
        deprecation("consider to use node.distro.is_debian property instead")
        return self.distro.is_debian

    # pylint: disable=too-many-arguments
    def pkg_install(self, pkgs, apt_pkgs=None, ubuntu14_pkgs=None, ubuntu16_pkgs=None,
                    debian8_pkgs=None, debian9_pkgs=None, ubuntu18_pkgs=None):
        """
        Support to install packages to multiple distros

        :param pkgs: default package name string
        :param apt_pkgs: special package name string for apt-get
        """
        # pylint: disable=too-many-return-statements
        # install packages for special debian like system
        if self.is_ubuntu14() and ubuntu14_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % ubuntu14_pkgs)
            return
        if self.is_ubuntu16() and ubuntu16_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % ubuntu16_pkgs)
            return
        if self.is_ubuntu18() and ubuntu18_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % ubuntu18_pkgs)
            return
        if self.is_ubuntu14() and ubuntu14_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % ubuntu14_pkgs)
            return
        if self.is_debian8() and debian8_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % debian8_pkgs)
            return
        if self.is_debian9() and debian9_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % debian9_pkgs)
            return

        # install general packages for debian like system
        if apt_pkgs:
            self.remoter.run('sudo apt-get install -y %s' % apt_pkgs)
            return

        if not self.is_rhel_like():
            self.log.error('Install packages for unknown distro by yum')
        self.remoter.run('sudo yum install -y %s' % pkgs)

    def is_docker(self):
        return self.__class__.__name__ == 'DockerNode'

    def is_gce(self):
        return self.__class__.__name__ == "GCENode"

    def scylla_pkg(self):
        return 'scylla-enterprise' if self.is_enterprise else 'scylla'

    def file_exists(self, file_path):
        try:
            result = self.remoter.run('sudo test -e %s' % file_path,
                                      ignore_status=True)
            return result.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error checking if file %s exists: %s',
                           file_path, details)

    @property
    def is_enterprise(self):
        if self._is_enterprise is None:
            if self.is_rhel_like():
                result = self.remoter.run("sudo yum search scylla-enterprise 2>&1", ignore_status=True)
                if 'One of the configured repositories failed (Extra Packages for Enterprise Linux 7 - x86_64)' in result.stdout:
                    result = self.remoter.run("sudo cat /etc/yum.repos.d/scylla.repo")
                    self._is_enterprise = 'enterprise' in result.stdout
                else:
                    self._is_enterprise = bool('scylla-enterprise.x86_64' in result.stdout or
                                               'No matches found' not in result.stdout)
            else:
                result = self.remoter.run("sudo apt-cache search scylla-enterprise", ignore_status=True)
                self._is_enterprise = 'scylla-enterprise' in result.stdout

        return self._is_enterprise

    @property
    def public_ip_address(self):
        public_ips, _ = self._refresh_instance_state()
        if public_ips:
            return public_ips[0]
        else:
            return None

    @property
    def private_ip_address(self):
        _, private_ips = self._refresh_instance_state()
        if private_ips:
            return private_ips[0]
        else:
            return None

    @property
    def ipv6_ip_address(self):
        raise NotImplementedError()

    def _wait_public_ip(self):
        public_ips, _ = self._refresh_instance_state()
        while not public_ips:
            time.sleep(1)
            public_ips, _ = self._refresh_instance_state()

    def _wait_private_ip(self):
        _, private_ips = self._refresh_instance_state()
        while not private_ips:
            time.sleep(1)
            _, private_ips = self._refresh_instance_state()

    def _refresh_instance_state(self):
        raise NotImplementedError()

    @property
    def ip_address(self):
        if IP_SSH_CONNECTIONS == "ipv6":
            return self.ipv6_ip_address
        elif Setup.INTRA_NODE_COMM_PUBLIC:
            return self.public_ip_address
        else:
            return self.private_ip_address

    @property
    def external_address(self):
        """
        the communication address for usage between the test and the nodes
        :return:
        """
        if IP_SSH_CONNECTIONS == "ipv6":
            return self.ipv6_ip_address
        elif IP_SSH_CONNECTIONS == 'public' or Setup.INTRA_NODE_COMM_PUBLIC:
            return self.public_ip_address
        else:
            return self.private_ip_address

    @property
    def is_spot(self):
        return False

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        :return: a delay to a next check if the instance termination was started, otherwise 0
        """
        raise NotImplementedError('Derived classes must implement check_spot_termination')

    def spot_monitoring_thread(self):
        while True:
            if self.termination_event.isSet():
                break
            try:
                self.wait_ssh_up(verbose=False)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.warning("Unable to connect to '%s'. Probably the node was terminated or is still booting. "
                                 "Error details: '%s'", self.name, ex)
                continue
            next_check_delay = self.check_spot_termination() or SPOT_TERMINATION_CHECK_DELAY
            time.sleep(next_check_delay)

    def start_spot_monitoring_thread(self):
        self._spot_monitoring_thread = threading.Thread(target=self.spot_monitoring_thread)
        self._spot_monitoring_thread.daemon = True
        self._spot_monitoring_thread.start()

    @property
    def init_system(self):
        if self._init_system is None:
            result = self.remoter.run('journalctl --version',
                                      ignore_status=True)
            if result.exit_status == 0:
                self._init_system = 'systemd'
            else:
                self._init_system = 'sysvinit'

        return self._init_system

    def retrieve_journal(self, since):
        try:
            since = '--since "{}" '.format(since) if since else ""
            self.log.debug("Reading Scylla logs from {}".format(since[8:] if since else "the beginning"))
            if self.init_system == 'systemd':
                # Here we're assuming that journalctl systems are Scylla images
                db_services_log_cmd = (f'sudo journalctl -f --no-tail --no-pager --utc {since}'
                                       '-u scylla-ami-setup.service '
                                       '-u scylla-image-setup.service '
                                       '-u scylla-io-setup.service '
                                       '-u scylla-server.service '
                                       '-u scylla-jmx.service')
            elif self.file_exists('/usr/bin/scylla'):
                db_services_log_cmd = ('sudo tail -f /var/log/syslog | grep scylla')
            else:
                # Here we are assuming we're using a cassandra image, based
                # on older Ubuntu
                cassandra_log = '/var/log/cassandra/system.log'
                wait.wait_for(self.file_exists, step=10, timeout=600, throw_exc=True, file_path=cassandra_log)
                db_services_log_cmd = ('sudo tail -f %s' % cassandra_log)
            self.remoter.run(db_services_log_cmd,
                             verbose=True, ignore_status=True,
                             log_file=self.database_log)
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error retrieving remote node DB service log: %s',
                           details)

    @raise_event_on_failure
    def journal_thread(self):
        read_from_timestamp = None
        while True:
            if self.termination_event.isSet():
                break
            self.wait_ssh_up(verbose=False)
            self.retrieve_journal(since=read_from_timestamp)
            # when rebooting a node we would like to start reading from the timestamp that we stopped receiving logs
            read_from_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def start_journal_thread(self):
        logs_transport = self.parent_cluster.params.get("logs_transport")
        cluster_backend = self.parent_cluster.params.get("cluster_backend")

        if cluster_backend == "docker":
            self._docker_log_process = subprocess.Popen(
                ['/bin/sh', '-c', f"docker logs -f  {self.name}  > {self.database_log} 2>&1"])
            return

        if logs_transport == "rsyslog":
            self.log.info("Using rsyslog as log transport")
        elif logs_transport == "ssh":
            self._journal_thread = threading.Thread(target=self.journal_thread)
            self._journal_thread.daemon = True
            self._journal_thread.start()
        else:
            raise Exception("Unknown logs transport: %s" % logs_transport)

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on getting coredump backtrace")
    def _get_coredump_backtraces(self, pid):
        """
        Get coredump backtraces.

        :param pid: PID of the core.
        :return: fabric.Result output
        """
        try:
            backtrace_cmd = f'sudo coredumpctl info --no-pager --no-legend {pid}'
            return self.remoter.run(backtrace_cmd, verbose=False, ignore_status=True, new_session=True)
        except NETWORK_EXCEPTIONS:  # pylint: disable=try-except-raise
            raise
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error retrieving core dump backtraces : %s',
                           details)

    def _pack_coredump(self, coredump: str):
        extensions = ['.lz4', '.zip', '.gz', '.gzip']
        for extension in extensions:
            if coredump.endswith(extension):
                return coredump
        try:  # pylint: disable=unreachable
            if self.is_debian() or self.is_ubuntu():
                self.remoter.run('sudo apt-get install -y pigz')
            else:
                self.remoter.run('sudo yum install -y pigz')
            self.remoter.run('sudo pigz --fast --keep {}'.format(coredump))
            coredump += '.gz'
        except NETWORK_EXCEPTIONS:  # pylint: disable=try-except-raise
            raise
        except Exception as ex:  # pylint: disable=broad-except
            self.log.warning("Failed to compress coredump '%s': %s", coredump, ex)
        return coredump

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on uploading coredump")
    def _upload_coredump(self, coredump):
        coredump = self._pack_coredump(coredump)
        base_upload_url = 'upload.scylladb.com/%s/%s'
        coredump_id = os.path.basename(coredump)[:-3]
        upload_url = base_upload_url % (coredump_id, os.path.basename(coredump))
        self.log.info('Uploading coredump %s to %s' % (coredump, upload_url))
        self.remoter.run("sudo curl --request PUT --upload-file "
                         "'%s' '%s'" % (coredump, upload_url))
        download_url = 'https://storage.cloud.google.com/%s' % upload_url
        self.log.info("You can download it by %s (available for ScyllaDB employee)", download_url)
        download_instructions = 'gsutil cp gs://%s .\ngunzip %s' % (upload_url, coredump)
        return download_url, download_instructions

    def _notify_backtrace(self, pid):
        """
        Notify coredump backtraces to test log and coredump.log file.

        :param last: Whether to show only the last backtrace.
        """
        result = self._get_coredump_backtraces(pid)
        if result.exit_status == 127:  # coredumpctl command not found
            return False
        log_file = os.path.join(self.logdir, 'coredump.log')
        output = result.stdout + result.stderr
        coredump = None
        timestamp = None
        # Extracting Coredump and Timestamp from coredumpctl output:
        #            PID: 37349 (scylla)
        #            UID: 996 (scylla)
        #            GID: 1001 (scylla)
        #         Signal: 3 (QUIT)
        #      Timestamp: Tue 2020-01-14 10:40:25 UTC (6min ago)
        #   Command Line: /usr/bin/scylla --blocked-reactor-notify-ms 500 --abort-on-lsa-bad-alloc 1
        #       --abort-on-seastar-bad-alloc --abort-on-internal-error 1 --log-to-syslog 1 --log-to-stdout 0
        #       --default-log-level info --network-stack posix --io-properties-file=/etc/scylla.d/io_properties.yaml
        #       --cpuset 1-7,9-15
        #     Executable: /opt/scylladb/libexec/scylla
        #  Control Group: /scylla.slice/scylla-server.slice/scylla-server.service
        #           Unit: scylla-server.service
        #          Slice: scylla-server.slice
        #        Boot ID: 0dc7f4137d5f47a3bda07dd046937fc2
        #     Machine ID: df877a200226bc47d06f26dae0736ec9
        #       Hostname: longevity-10gb-3h-dkropachev-db-node-b9ebdfb0-1
        #       Coredump: /var/lib/systemd/coredump/core.scylla.996.0dc7f4137d5f47a3bda07dd046937fc2.37349.1578998425000000
        #        Message: Process 37349 (scylla) of user 996 dumped core.
        #
        #                 Stack trace of thread 37349:
        #                 #0  0x00007ffc2e724704 n/a (linux-vdso.so.1)
        #                 #1  0x00007ffc2e724992 __vdso_clock_gettime (linux-vdso.so.1)
        #                 #2  0x00007f251c3082c3 __clock_gettime (libc.so.6)
        #                 #3  0x00007f251c5f3b85 _ZNSt6chrono3_V212steady_clock3nowEv (libstdc++.so.6)
        #                 #4  0x0000000002ab94e5 _ZN7seastar7reactor3runEv (scylla)
        #                 #5  0x00000000029fc1ed _ZN7seastar12app_template14run_deprecatedEiPPcOSt8functionIFvvEE (scylla)
        #                 #6  0x00000000029fcedf _ZN7seastar12app_template3runEiPPcOSt8functionIFNS_6futureIJiEEEvEE (scylla)
        #                 #7  0x0000000000794222 main (scylla)
        #
        # Coredump could be absent when file was removed
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Coredump:') or line.startswith('Storage:'):
                if "(inaccessible)" in line:
                    # Ignore inaccessible cores
                    #       Storage: /var/lib/systemd/coredump/core.vi.1000.6c4de4c206a0476e88444e5ebaaac482.18554.1578994298000000.lz4 (inaccessible)
                    continue
                coredump = line.split()[-1]
            elif line.startswith('Timestamp:'):
                try:
                    # Converting time string "Tue 2020-01-14 10:40:25 UTC (6min ago)" to timestamp
                    tmp = re.match(r'Timestamp: ([^(]+)(\([^)]+\)|)', line).group(1)
                    timestamp = time.mktime(datetime.strptime(tmp.strip(' '), "%a %Y-%m-%d %H:%M:%S UTC").timetuple())
                except Exception:  # pylint: disable=broad-except
                    self.log.error(f"Failed to convert date '{timestamp}'")
        result = False
        if coredump:
            url = ""
            download_instructions = "failed to upload"
            try:
                self.log.debug('Found coredump file: {}'.format(coredump))
                url, download_instructions = self._upload_coredump(coredump)
                result = True
            except Exception as exc:  # pylint: disable=broad-except
                self.log.error(f"Following error occured during uploading coredump {coredump}: {str(exc)}")
            finally:
                CoreDumpEvent(corefile_url=url, download_instructions=download_instructions,
                              backtrace=output, node=self, timestamp=timestamp)

        with open(log_file, 'a') as log_file_obj:
            log_file_obj.write(output)
        for line in output.splitlines():
            self.log.error(line)
        return result

    def get_backtraces(self):
        """
        Get core files from node and report them
        """
        try:
            self.wait_ssh_up(verbose=False)
            if self.is_ubuntu14():
                # fixme: ubuntu14 doesn't has coredumpctl, skip it.
                return
            core_pids = self._get_core_pids()
            if not core_pids:
                return
            for core_pid in core_pids:
                if core_pid in self._discovered_backtraces:
                    continue
                if len(self._discovered_backtraces) > self._maximum_number_of_cores_to_publish:
                    break
                self._notify_backtrace(core_pid)
                self._discovered_backtraces.append(core_pid)
        except NETWORK_EXCEPTIONS as exc:
            self.log.error(f"Following network error occurred during getting back traces: {str(exc)}")

    @retrying(n=10, sleep_time=20, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying on getting pid of cores")
    def _get_core_pids(self):
        result = self.remoter.run('sudo coredumpctl --no-pager --no-legend 2>&1',
                                  verbose=False, ignore_status=True, new_session=True)
        if "No coredumps found" in result.stdout or result.exit_status == 127:  # exit_status 127: coredumpctl command not found
            return None
        output = []
        result = result.stdout + result.stderr
        # Extracting PIDs from coredumpctl output as such:
        #     TIME                            PID   UID   GID SIG COREFILE  EXE
        #     Tue 2020-01-14 16:16:39 +07    3530  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:18:56 +07    6321  1000  1000   3 present   /usr/bin/scylla
        #     Tue 2020-01-14 16:31:39 +07   18554  1000  1000   3 present   /usr/bin/scylla
        for line in result.splitlines():
            columns = re.split(r'[ ]{2,}', line)
            if len(columns) < 2:
                continue
            pid = columns[1]
            if re.findall(r'[^0-9]', pid):
                self.log.error("PID pattern matched non-numerical value. Looks like coredumpctl changed it's output")
                continue
            output.append(pid)
        return output

    @raise_event_on_failure
    def backtrace_thread(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        while not self.termination_event.isSet():
            self.termination_event.wait(15)
            try:
                self.get_backtraces()
            except NETWORK_EXCEPTIONS:
                pass

    @raise_event_on_failure
    def db_log_reader_thread(self):
        """
        Keep reporting new events from db log, every 30 seconds.
        """
        while not self.termination_event.isSet():
            self.termination_event.wait(15)
            try:
                self.search_database_log(start_from_beginning=False, publish_events=True)
            except Exception:  # pylint: disable=broad-except
                self.log.exception("failed to read db log")
            except (SystemExit, KeyboardInterrupt) as ex:
                self.log.debug("db_log_reader_thread() stopped by %s", ex.__class__.__name__)

    def start_backtrace_thread(self):
        self._backtrace_thread = threading.Thread(target=self.backtrace_thread)
        self._backtrace_thread.daemon = True
        self._backtrace_thread.start()

    def start_db_log_reader_thread(self):
        self._db_log_reader_thread = threading.Thread(target=self.db_log_reader_thread)
        self._db_log_reader_thread.daemon = True
        self._db_log_reader_thread.start()

    def start_alert_manager_thread(self):
        self._alert_manager = PrometheusAlertManagerListener(self.external_address, stop_flag=self.termination_event)
        self._alert_manager.start()

    def __str__(self):
        return 'Node %s [%s | %s%s] (seed: %s)' % (self.name,
                                                   self.public_ip_address,
                                                   self.private_ip_address,
                                                   " | %s" % self.ipv6_ip_address if IP_SSH_CONNECTIONS == "ipv6" else "",
                                                   self.is_seed)

    def restart(self):
        raise NotImplementedError('Derived classes must implement restart')

    def hard_reboot(self):  # pylint: disable=no-self-use
        # Need to re-implement this method if the backend supports hard reboot.
        raise Exception("The backend doesn't support hard_reboot")

    def reboot(self, hard=True, verify_ssh=True):
        result = self.remoter.run('uptime -s')
        pre_uptime = result.stdout

        def uptime_changed():
            try:
                result = self.remoter.run('uptime -s', ignore_status=True)
                return pre_uptime != result.stdout
            except SSHException as ex:
                self.log.debug("Network isn't available, reboot might already start, %s" % ex)
                return False
            except Exception as ex:  # pylint: disable=broad-except
                self.log.debug('Failed to get uptime during reboot, %s' % ex)
                return False

        if hard:
            self.log.debug('Hardly rebooting node')
            self.hard_reboot()
        else:
            self.log.debug('Softly rebooting node')
            self.remoter.run('sudo reboot', ignore_status=True)

        # wait until the reboot is executed
        wait.wait_for(func=uptime_changed, step=10, timeout=500, throw_exc=True)

        if verify_ssh:
            self.wait_ssh_up()

    @log_run_info
    def start_task_threads(self):
        if self.is_spot:
            self.start_spot_monitoring_thread()
        if 'db-node' in self.name:  # this should be replaced when DbNode class will be created
            self.start_journal_thread()
            self.start_backtrace_thread()
            self.start_db_log_reader_thread()
        elif 'monitor' in self.name:
            # TODO: start alert manager thread here when start_task_threads will be run after node setup
            # self.start_alert_manager_thread()
            if Setup.BACKTRACE_DECODING:
                self.start_decode_on_monitor_node_thread()

    @log_run_info
    def stop_task_threads(self, timeout=10):
        if self.termination_event.isSet():
            return
        self.log.info('Set termination_event')
        self.termination_event.set()
        if self._spot_monitoring_thread:
            self._spot_monitoring_thread.join(timeout)
        if self._backtrace_thread:
            self._backtrace_thread.join(timeout)
        if self._db_log_reader_thread:
            self._db_log_reader_thread.join(timeout)
        if self._alert_manager:
            self._alert_manager.stop(timeout)
        if self._journal_thread:
            self.remoter.run(cmd='sudo pkill -f "journalctl.*scylla"', ignore_status=True)
            self._journal_thread.join(timeout)
        if self._scylla_manager_journal_thread:
            self.stop_scylla_manager_log_capture(timeout)
        if self._decoding_backtraces_thread:
            self._decoding_backtraces_thread.join(timeout)
            self._decoding_backtraces_thread = None
        if self._docker_log_process:
            self._docker_log_process.kill()

    def get_cpumodel(self):
        """Get cpu model from /proc/cpuinfo

        Get cpu model from /proc/cpuinfo of node
        """
        cpuinfo_cmd = 'cat /proc/cpuinfo'

        try:
            result = self.remoter.run(cmd=cpuinfo_cmd, verbose=False)
            model = re.findall(r'^model\sname\s:(.+)$', result.stdout.strip(), re.MULTILINE)
            return model[0].strip()

        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error retrieving CPU model info : %s',
                           details)
            return None

    def get_installed_packages(self):
        """Get installed packages on node

        Execute package manager command and get
        list of all installed packages
        """
        if self.is_ubuntu() or self.is_debian():
            cmd = 'dpkg-query --show'
        else:
            cmd = 'rpm -qa'

        try:
            result = self.remoter.run(cmd, verbose=False)
            return result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error retrieving installed packages: %s',
                           details)
            return None

    def get_system_info(self):
        """Get system info by uname -a

        Get system info as a result of uname -a
        """

        cmd = 'uname -a'
        try:
            result = self.remoter.run(cmd, verbose=False)
            return result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error retrieving command \'%s\' result : %s',
                           (cmd, details))
            return None

    def destroy(self):
        raise NotImplementedError('Derived classes must implement destroy')

    def wait_ssh_up(self, verbose=True, timeout=500):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self.remoter.is_up, step=10, text=text, timeout=timeout, throw_exc=True)

    def is_port_used(self, port, service_name):
        try:
            # check that port is taken
            result_netstat = self.remoter.run('netstat -ln | grep :%s' % port,
                                              # -n don't translate port numbers to names
                                              verbose=False, ignore_status=True)
            return result_netstat.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            self.log.error("Error checking for '%s' on port %s: %s", service_name, port, details)
            return False

    def db_up(self):
        return self.is_port_used(port=self.CQL_PORT, service_name="scylla-server")

    def jmx_up(self):
        return self.is_port_used(port=7199, service_name="scylla-jmx")

    def cs_installed(self, cassandra_stress_bin=None):
        if cassandra_stress_bin is None:
            cassandra_stress_bin = '/usr/bin/cassandra-stress'
        return self.file_exists(cassandra_stress_bin)

    @staticmethod
    def _parse_cfstats(cfstats_output):
        stat_dict = {}
        for line in cfstats_output.splitlines()[1:]:
            # Example of line of cfstats output:
            #       Space used (total): 123456
            stat_line = [element for element in line.strip().split(':') if
                         element]
            if stat_line:
                try:
                    try:
                        # Fix for space_node_threshold: if there are a few tables in the keyspace and space is used by the
                        # table, that arrives last in the cfstats output, will be less then space_node_threshold,
                        # the nemesis never will be run. Because of this, we sum space of all tables in the keyspace
                        # This function is used just for wait_total_space_used_per_node, so I fix "Space used.." statistics only
                        current_value = stat_dict[stat_line[0]] if 'Space used' in stat_line[0] \
                                                                   and stat_line[0] in stat_dict else 0
                        if '.' in stat_line[1].split()[0]:
                            stat_dict[stat_line[0]] = float(stat_line[1].split()[0]) + current_value
                        else:
                            stat_dict[stat_line[0]] = int(stat_line[1].split()[0]) + current_value
                    except IndexError:
                        continue
                except ValueError:
                    stat_dict[stat_line[0]] = stat_line[1].split()[0]
        return stat_dict

    def _get_tcpdump_logs(self, tcpdump_id):
        try:
            pcap_name = 'tcpdump-%s.pcap' % tcpdump_id
            pcap_tmp_file = os.path.join('/tmp', pcap_name)
            pcap_file = os.path.join(self.logdir, pcap_name)
            self.remoter.run('sudo tcpdump -vv -i lo port 10000 -w %s > /dev/null 2>&1' %
                             pcap_tmp_file, ignore_status=True)
            self.remoter.receive_files(src=pcap_tmp_file, dst=pcap_file)  # pylint: disable=not-callable
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Error running tcpdump on lo, tcp port 10000: %s',
                           str(details))

    def get_cfstats(self, keyspace, tcpdump=False):
        def keyspace_available():
            self.run_nodetool("flush", ignore_status=True)
            res = self.run_nodetool(sub_cmd='cfstats', args=keyspace, ignore_status=True)
            return res.exit_status == 0
        tcpdump_id = uuid.uuid4()
        if tcpdump:
            self.log.info('START tcpdump thread uuid: %s', tcpdump_id)
            tcpdump_thread = threading.Thread(target=self._get_tcpdump_logs,
                                              kwargs={'tcpdump_id': tcpdump_id})
            tcpdump_thread.daemon = True
            tcpdump_thread.start()
        wait.wait_for(keyspace_available, step=60, text='Waiting until keyspace {} is available'.format(keyspace))
        try:
            result = self.run_nodetool(sub_cmd='cfstats', args=keyspace)
        except (Failure, UnexpectedExit):
            self.log.error('nodetool error - see tcpdump thread uuid %s for '
                           'debugging info', tcpdump_id)
            raise
        finally:
            if tcpdump:
                self.remoter.run('sudo killall tcpdump', ignore_status=True)
        self.log.info('END tcpdump thread uuid: %s', tcpdump_id)
        return self._parse_cfstats(result.stdout)

    def wait_jmx_up(self, verbose=True, timeout=None):
        text = None
        if verbose:
            text = '%s: Waiting for JMX service to be up' % self
        wait.wait_for(func=self.jmx_up, step=60, text=text, timeout=timeout, throw_exc=True)

    def wait_jmx_down(self, verbose=True, timeout=None):
        text = None
        if verbose:
            text = '%s: Waiting for JMX service to be down' % self
        wait.wait_for(func=lambda: not self.jmx_up(), step=60, text=text, timeout=timeout, throw_exc=True)

    def _report_housekeeping_uuid(self, verbose=False):
        """
        report uuid of test db nodes to ScyllaDB
        """
        uuid_path = '/var/lib/scylla-housekeeping/housekeeping.uuid'
        mark_path = '/var/lib/scylla-housekeeping/housekeeping.uuid.marked'
        cmd = 'curl "https://i6a5h9l1kl.execute-api.us-east-1.amazonaws.com/prod/check_version?uu=%s&mark=scylla"'

        uuid_result = self.remoter.run('test -e %s' % uuid_path,
                                       ignore_status=True, verbose=verbose)
        mark_result = self.remoter.run('test -e %s' % mark_path,
                                       ignore_status=True, verbose=verbose)
        if uuid_result.exit_status == 0 and mark_result.exit_status != 0:
            result = self.remoter.run('cat %s' % uuid_path, verbose=verbose)
            self.remoter.run(cmd % result.stdout.strip(), ignore_status=True)

            if self.is_docker():  # in docker we don't have scylla user and run as root
                self.remoter.run('sudo touch %s' % mark_path,
                                 verbose=verbose)
            else:
                self.remoter.run('sudo -u scylla touch %s' % mark_path,
                                 verbose=verbose)

    def wait_db_up(self, verbose=True, timeout=3600):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60, text=text, timeout=timeout, throw_exc=True)
        self.db_init_finished = True
        try:
            self._report_housekeeping_uuid()
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Failed to report housekeeping uuid. Error details: %s', details)

    # Configuration node-exporter.service when use IPv6
    def set_web_listen_address(self):
        node_exporter_file = '/usr/lib/systemd/system/node-exporter.service'
        find_web_param = self.remoter.run('grep "web.listen-address" %s' % node_exporter_file,
                                          ignore_status=True)
        if find_web_param.exit_status == 1:
            cmd = """sudo sh -c "sed -i 's|ExecStart=/usr/bin/node_exporter  --collector.interrupts|""" \
                  """ExecStart=/usr/bin/node_exporter  --collector.interrupts """ \
                  """--web.listen-address="[%s]:9100"|g' %s" """ % (self.ip_address, node_exporter_file)
            self.remoter.run(cmd)
            self.remoter.run('sudo systemctl restart node-exporter.service')

    def apt_running(self):
        try:
            result = self.remoter.run('sudo lsof /var/lib/dpkg/lock', ignore_status=True)
            return result.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            self.log.error('Failed to check if APT is running in the background. Error details: %s', details)
            return False

    def wait_apt_not_running(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for apt to finish running in the background' % self
        wait.wait_for(func=lambda: not self.apt_running(), step=60,
                      text=text)

    def wait_db_down(self, verbose=True, timeout=3600):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be down' % self
        wait.wait_for(func=lambda: not self.db_up(), step=60, text=text, timeout=timeout, throw_exc=True)

    def wait_cs_installed(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for cassandra-stress' % self
        wait.wait_for(func=self.cs_installed, step=60,
                      text=text)

    def mark_log(self):
        """
        Returns "a mark" to the current position of this node Cassandra log.
        This is for use with the from_mark parameter of watch_log_for_* methods,
        allowing to watch the log from the position when this method was called.
        """
        if not os.path.exists(self.database_log):
            return 0
        with open(self.database_log) as log_file:
            log_file.seek(0, os.SEEK_END)
            return log_file.tell()

    def search_database_log(self, search_pattern=None, start_from_beginning=False, publish_events=True):
        """
        Search for all known patterns listed in  `_database_error_events`

        :param start_from_beginning: if True will search log from first line
        :param publish_events: if True will publish events
        :return: list of (line, error) tuples
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements

        matches = []
        patterns = []
        backtraces = []

        index = 0
        # prepare/compile all regexes
        if search_pattern:
            expression = DatabaseLogEvent(type="user-query", regex=search_pattern, severity=Severity.CRITICAL)
            patterns += [(re.compile(expression.regex, re.IGNORECASE), expression)]
        else:
            for expression in self._database_error_events:
                patterns += [(re.compile(expression.regex, re.IGNORECASE), expression)]

        backtrace_regex = re.compile(r'(?P<other_bt>/lib.*?\+0x[0-f]*\n)|(?P<scylla_bt>0x[0-f]*\n)', re.IGNORECASE)

        if not os.path.exists(self.database_log):
            return matches

        if start_from_beginning:
            start_search_from_byte = 0
            last_line_no = 0
        else:
            start_search_from_byte = self.last_log_position
            last_line_no = self.last_line_no

        with open(self.database_log, 'r') as db_file:
            if start_search_from_byte:
                db_file.seek(start_search_from_byte)
            for index, line in enumerate(db_file, start=last_line_no):
                if not start_from_beginning and Setup.RSYSLOG_ADDRESS:
                    LOGGER.debug(line.strip())

                match = backtrace_regex.search(line)
                if match and backtraces:
                    data = match.groupdict()
                    if data['other_bt']:
                        backtraces[-1]['backtrace'] += [data['other_bt'].strip()]
                    if data['scylla_bt']:
                        backtraces[-1]['backtrace'] += [data['scylla_bt'].strip()]

                if index not in self._database_log_errors_index or start_from_beginning:
                    # for each line use all regexes to match, and if found send an event
                    for pattern, event in patterns:
                        match = pattern.search(line)
                        if match:
                            self._database_log_errors_index.append(index)
                            cloned_event = event.clone_with_info(node=self, line_number=index, line=line)
                            backtraces.append(dict(event=cloned_event, backtrace=[]))
                            matches.append((index, line))
                            break  # Stop iterating patterns to avoid creating two events for one line of the log

            if not start_from_beginning:
                self.last_line_no = index if index else last_line_no
                self.last_log_position = db_file.tell() + 1

        if publish_events:
            traces_count = 0
            for backtrace in backtraces:
                backtrace['event'].add_backtrace_info(raw_backtrace="\n".join(backtrace['backtrace']))
                if backtrace['event'].type == 'BACKTRACE':
                    traces_count += 1

            # filter function to attach the backtrace to the correct error and not to the back traces
            # if the error is within 10 lines and the last isn't backtrace type, the backtrace would be appended to the previous error
            def filter_backtraces(backtrace):
                last_error = filter_backtraces.last_error
                try:
                    if (last_error and
                            backtrace['event'].line_number <= filter_backtraces.last_error.line_number + 20
                            and not filter_backtraces.last_error.type == 'BACKTRACE' and backtrace['event'].type == 'BACKTRACE'):
                        last_error.add_backtrace_info(raw_backtrace="\n".join(backtrace['backtrace']))
                        return False
                    return True
                finally:
                    filter_backtraces.last_error = backtrace['event']

            # support interlaced reactor stalled
            for _ in range(traces_count):
                filter_backtraces.last_error = None
                backtraces = list(filter(filter_backtraces, backtraces))

            for backtrace in backtraces:
                if Setup.BACKTRACE_DECODING:
                    if backtrace['event'].raw_backtrace:
                        scylla_debug_info = self.get_scylla_debuginfo_file()
                        self.log.debug('Debug info file %s', scylla_debug_info)
                        Setup.DECODING_QUEUE.put({"node": self, "debug_file": scylla_debug_info,
                                                  "event": backtrace['event']})
                else:
                    backtrace['event'].publish()

        return matches

    def start_decode_on_monitor_node_thread(self):
        self._decoding_backtraces_thread = threading.Thread(target=self.decode_backtrace)
        self._decoding_backtraces_thread.daemon = True
        self._decoding_backtraces_thread.start()

    def decode_backtrace(self):
        scylla_debug_file = None
        while True:
            event = None
            obj = None
            try:
                obj = Setup.DECODING_QUEUE.get(timeout=5)
                if obj is None:
                    Setup.DECODING_QUEUE.task_done()
                    break
                event = obj["event"]
                if not scylla_debug_file:
                    scylla_debug_file = self.copy_scylla_debug_info(obj["node"], obj["debug_file"])
                output = self.decode_raw_backtrace(scylla_debug_file, " ".join(event.raw_backtrace.split('\n')))
                event.add_backtrace_info(backtrace=output.stdout)
                Setup.DECODING_QUEUE.task_done()
            except queue.Empty:
                pass
            except Exception as details:  # pylint: disable=broad-except
                self.log.error("failed to decode backtrace %s", details)
            finally:
                if event:
                    event.publish()

            if self.termination_event.isSet() and Setup.DECODING_QUEUE.empty():
                break

    def copy_scylla_debug_info(self, node, debug_file):
        """Copy scylla debug file from db-node to monitor-node

        Copy via builder
        :param node: db node
        :type node: BaseNode
        :param scylla_debug_file: path to scylla_debug_file on db-node
        :type scylla_debug_file: str
        :returns: path on monitor node
        :rtype: {str}
        """
        base_scylla_debug_file = os.path.basename(debug_file)
        transit_scylla_debug_file = os.path.join(node.parent_cluster.logdir,
                                                 base_scylla_debug_file)
        final_scylla_debug_file = os.path.join("/tmp", base_scylla_debug_file)

        if not os.path.exists(transit_scylla_debug_file):
            node.remoter.receive_files(debug_file, transit_scylla_debug_file)
        res = self.remoter.run(
            "test -f {}".format(final_scylla_debug_file), ignore_status=True, verbose=False)
        if res.exited != 0:
            self.remoter.send_files(transit_scylla_debug_file,  # pylint: disable=not-callable
                                    final_scylla_debug_file)
        self.log.info("File on monitor node %s: %s", self, final_scylla_debug_file)
        self.log.info("Remove transit file: %s", transit_scylla_debug_file)
        os.remove(transit_scylla_debug_file)
        return final_scylla_debug_file

    def decode_raw_backtrace(self, scylla_debug_file, raw_backtrace):
        """run decode backtrace on monitor node

        Decode backtrace on monitor node
        :param scylla_debug_file: file path on db-node
        :type scylla_debug_file: str
        :param raw_backtrace: string with backtrace data
        :type raw_backtrace: str
        :returns: result of bactrace
        :rtype: {str}
        """
        return self.remoter.run('addr2line -Cpife {0} {1}'.format(scylla_debug_file, raw_backtrace), verbose=True)

    def get_scylla_debuginfo_file(self):
        """
        Lookup the scylla debug information, in various places it can be.

        :return the path to the scylla debug information
        :rtype str
        """
        build_id = None

        # first try default location
        scylla_debug_info = '/usr/lib/debug/bin/scylla.debug'
        results = self.remoter.run('[[ -f {} ]]'.format(scylla_debug_info), ignore_status=True)
        if results.exit_status == 0:
            return scylla_debug_info

        # then try the relocatable location
        results = self.remoter.run('ls /usr/lib/debug/opt/scylladb/libexec/scylla*.debug', ignore_status=True)
        if results.stdout.strip():
            return results.stdout.strip()

        # then look it up base on the build id
        for scylla_executable in ['/usr/bin/scylla', '/opt/scylladb/libexec/scylla']:
            try:
                results = self.remoter.run(f'file {scylla_executable}')
                build_id_regex = re.compile(r'BuildID\[.*\]=(.*),')
                build_id = build_id_regex.search(results.stdout).group(1)
            except Exception:  # pylint: disable=broad-except
                LOGGER.warning(f'{scylla_executable} did had BuildID in it')

        if build_id:
            scylla_debug_info = "/usr/lib/debug/.build-id/{0}/{1}.debug".format(build_id[:2], build_id[2:])
            results = self.remoter.run('[[ -f {} ]]'.format(scylla_debug_info), ignore_status=True)
            if results.exit_status == 0:
                return scylla_debug_info

        raise Exception("Couldn't find scylla debug information")

    def datacenter_setup(self, datacenters):
        cmd = "sudo sh -c 'echo \"\ndc={}\nrack=RACK1\nprefer_local=true\ndc_suffix={}\n\" >> /etc/scylla/cassandra-rackdc.properties'"
        region_name = datacenters[self.dc_idx]
        ret = re.findall('-([a-z]+).*-', region_name)
        if ret:
            dc_suffix = 'scylla_node_{}'.format(ret[0])
        else:
            dc_suffix = region_name.replace('-', '_')

        cmd = cmd.format(datacenters[self.dc_idx], dc_suffix)
        self.remoter.run(cmd)

    # pylint: disable=invalid-name,too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    def config_setup(self, seed_address=None, cluster_name=None, enable_exp=True, endpoint_snitch=None,
                     yaml_file=SCYLLA_YAML_PATH, broadcast=None, authenticator=None, server_encrypt=None,
                     client_encrypt=None, append_scylla_yaml=None, append_scylla_args=None, debug_install=False,
                     hinted_handoff='enabled', murmur3_partitioner_ignore_msb_bits=None, authorizer=None,
                     alternator_port=None, listen_on_all_interfaces=False, ip_ssh_connections=None):
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'), 'scylla.yaml')
        wait.wait_for(self.remoter.receive_files, step=10, text='Waiting for copying scylla.yaml',
                      timeout=300, throw_exc=True, src=yaml_file, dst=yaml_dst_path)

        with open(yaml_dst_path, 'r') as scylla_yaml_file:
            scylla_yaml_contents = scylla_yaml_file.read()

        if seed_address:
            # Set seeds
            pattern = re.compile('seeds:.*')
            scylla_yaml_contents = pattern.sub('seeds: "{0}"'.format(seed_address),
                                               scylla_yaml_contents)

            # NOTICE: the following configuration always have to use private_ip_address for multi-region to work
            # Set listen_address
            pattern = re.compile('listen_address:.*')
            scylla_yaml_contents = pattern.sub('listen_address: {0}'.format(self.private_ip_address),
                                               scylla_yaml_contents)
            # Set rpc_address
            pattern = re.compile('\n[# ]*rpc_address:.*')
            scylla_yaml_contents = pattern.sub('\nrpc_address: {0}'.format(self.private_ip_address),
                                               scylla_yaml_contents)

        if listen_on_all_interfaces:
            # Set listen_address
            pattern = re.compile('listen_address:.*')
            scylla_yaml_contents = pattern.sub('listen_address: {0}'.format("0.0.0.0"), scylla_yaml_contents)
            # Set rpc_address
            pattern = re.compile('\n[# ]*rpc_address:.*')
            scylla_yaml_contents = pattern.sub('\nrpc_address: {0}'.format("0.0.0.0"), scylla_yaml_contents)

        if broadcast:
            # Set broadcast_address
            pattern = re.compile('[# ]*broadcast_address:.*')
            scylla_yaml_contents = pattern.sub('broadcast_address: {0}'.format(broadcast),
                                               scylla_yaml_contents)

            # Set broadcast_rpc_address
            pattern = re.compile('[# ]*broadcast_rpc_address:.*')
            scylla_yaml_contents = pattern.sub('broadcast_rpc_address: {0}'.format(broadcast),
                                               scylla_yaml_contents)

        if cluster_name:
            pattern = re.compile('[# ]*cluster_name:.*')
            scylla_yaml_contents = pattern.sub('cluster_name: {0}'.format(cluster_name),
                                               scylla_yaml_contents)

        # disable hinted handoff (it is enabled by default in Scylla). Expected values: "enabled"/"disabled"
        if hinted_handoff == 'disabled':
            pattern = re.compile('[# ]*hinted_handoff_enabled:.*')
            scylla_yaml_contents = pattern.sub('hinted_handoff_enabled: false', scylla_yaml_contents, count=1)

        if ip_ssh_connections == 'ipv6':
            self.log.debug('Enable IPv6 DNS lookup')
            pattern = re.compile('enable_ipv6_dns_lookup:.*')
            if pattern.findall(scylla_yaml_contents):
                scylla_yaml_contents = pattern.sub('enable_ipv6_dns_lookup: true', scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nenable_ipv6_dns_lookup: true\n"

            # Set prometheus_address
            pattern = re.compile('\n[# ]*prometheus_address:.*')
            scylla_yaml_contents = pattern.sub('\nprometheus_address: {0}'.format(self.ip_address),
                                               scylla_yaml_contents)

            # Set broadcast_rpc_address
            pattern = re.compile('\n[# ]*broadcast_rpc_address:.*')
            scylla_yaml_contents = pattern.sub('\nbroadcast_rpc_address: {0}'.format(self.ip_address),
                                               scylla_yaml_contents)

            # Set listen_address
            pattern = re.compile('listen_address:.*')
            scylla_yaml_contents = pattern.sub('listen_address: {0}'.format(self.ip_address),
                                               scylla_yaml_contents)
            # Set rpc_address
            pattern = re.compile('\n[# ]*rpc_address:.*')
            scylla_yaml_contents = pattern.sub('\nrpc_address: {0}'.format(self.ip_address),
                                               scylla_yaml_contents)

        if murmur3_partitioner_ignore_msb_bits:
            self.log.debug('Change murmur3_partitioner_ignore_msb_bits to {}'.format(
                murmur3_partitioner_ignore_msb_bits))
            pattern = re.compile('murmur3_partitioner_ignore_msb_bits:.*')
            if pattern.findall(scylla_yaml_contents):
                scylla_yaml_contents = pattern.sub('murmur3_partitioner_ignore_msb_bits: {0}'.format(murmur3_partitioner_ignore_msb_bits),
                                                   scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nmurmur3_partitioner_ignore_msb_bits: {0}\n".format(
                    murmur3_partitioner_ignore_msb_bits)

        if enable_exp:
            scylla_yaml_contents += "\nexperimental: true\n"

        if endpoint_snitch:
            pattern = re.compile('endpoint_snitch:.*')
            scylla_yaml_contents = pattern.sub('endpoint_snitch: "{0}"'.format(endpoint_snitch),
                                               scylla_yaml_contents)
        if not client_encrypt:
            pattern = re.compile('.*enabled: true.*# <client_encrypt>.*')
            scylla_yaml_contents = pattern.sub(
                '   enabled: false                    # <client_encrypt>', scylla_yaml_contents)

        if self.enable_auto_bootstrap:
            if 'auto_bootstrap' in scylla_yaml_contents:
                if re.findall("auto_bootstrap: False", scylla_yaml_contents):
                    self.log.debug('auto_bootstrap is not set as expected, update it to `True`.')
                pattern = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = pattern.sub('auto_bootstrap: True',
                                                   scylla_yaml_contents)
            else:
                self.log.debug('auto_bootstrap is missing, set it `True`.')
                scylla_yaml_contents += "\nauto_bootstrap: True\n"
        else:
            if 'auto_bootstrap' in scylla_yaml_contents:
                if re.findall("auto_bootstrap: True", scylla_yaml_contents):
                    self.log.debug('auto_bootstrap is not set as expected, update it to `False`.')
                pattern = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = pattern.sub('auto_bootstrap: False',
                                                   scylla_yaml_contents)
            else:
                self.log.debug('auto_bootstrap is missing, set it `False`.')
                scylla_yaml_contents += "\nauto_bootstrap: False\n"

        if authenticator in ['AllowAllAuthenticator', 'PasswordAuthenticator']:
            pattern = re.compile('[# ]*authenticator:.*')
            scylla_yaml_contents = pattern.sub('authenticator: {0}'.format(authenticator),
                                               scylla_yaml_contents)
        if authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
            pattern = re.compile('[# ]*authorizer:.*')
            scylla_yaml_contents = pattern.sub('authorizer: {0}'.format(authorizer),
                                               scylla_yaml_contents)

        if server_encrypt or client_encrypt:
            self.config_client_encrypt()
        if server_encrypt:
            scylla_yaml_contents += """
server_encryption_options:
   internode_encryption: all
   certificate: /etc/scylla/ssl_conf/db.crt
   keyfile: /etc/scylla/ssl_conf/db.key
   truststore: /etc/scylla/ssl_conf/cadb.pem
"""

        if client_encrypt:
            client_encrypt_conf = dedent("""
                            client_encryption_options:                               # <client_encrypt>
                               enabled: true                                         # <client_encrypt>
                               certificate: /etc/scylla/ssl_conf/client/test.crt   # <client_encrypt>
                               keyfile: /etc/scylla/ssl_conf/client/test.key       # <client_encrypt>
                               truststore: /etc/scylla/ssl_conf/client/catest.pem  # <client_encrypt>
            """)
            scylla_yaml_contents += client_encrypt_conf

        if self.replacement_node_ip:
            self.log.debug("%s is a replacement node for '%s'.", self.name, self.replacement_node_ip)
            scylla_yaml_contents += "\nreplace_address_first_boot: %s\n" % self.replacement_node_ip
        else:
            pattern = re.compile('^replace_address_first_boot:')
            scylla_yaml_contents = pattern.sub('# replace_address_first_boot:',
                                               scylla_yaml_contents)

        if alternator_port:
            if 'alternator_port' in scylla_yaml_contents:
                p = re.compile('[# ]*alternator_port:.*')
                scylla_yaml_contents = p.sub('alternator_port: {0}'.format(alternator_port),
                                             scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nalternator_port: %s\n" % alternator_port

        # system_key must be pre-created, kmip keys will be used for kmip server auth
        if append_scylla_yaml and ('system_key_directory' in append_scylla_yaml or 'system_info_encryption' in append_scylla_yaml or 'kmip_hosts:' in append_scylla_yaml):
            self.remoter.send_files(src='./data_dir/encrypt_conf',  # pylint: disable=not-callable
                                    dst='/tmp/')
            self.remoter.run('sudo rm -rf /etc/encrypt_conf')
            self.remoter.run('sudo mv -f /tmp/encrypt_conf /etc/')
            self.remoter.run('sudo mkdir /etc/encrypt_conf/system_key_dir/')
            self.remoter.run('sudo chown -R scylla:scylla /etc/encrypt_conf/')
            self.remoter.run('sudo md5sum /etc/encrypt_conf/*.pem', ignore_status=True)

        if append_scylla_yaml:
            scylla_yaml_contents += append_scylla_yaml

        with open(yaml_dst_path, 'w') as scylla_yaml_file:
            scylla_yaml_file.write(scylla_yaml_contents)

        self.log.debug("Scylla YAML configuration:\n%s", scylla_yaml_contents)
        wait.wait_for(self.remoter.send_files, step=10, text='Waiting for copying scylla.yaml to node',
                      timeout=300, throw_exc=True, src=yaml_dst_path, dst='/tmp/scylla.yaml')
        self.remoter.run('sudo mv /tmp/scylla.yaml {}'.format(yaml_file))

        if append_scylla_args:
            if self.is_rhel_like():
                result = self.remoter.run("sudo grep '\\%s' /etc/sysconfig/scylla-server" %
                                          append_scylla_args, ignore_status=True)
                if result.exit_status == 1:
                    self.remoter.run(
                        "sudo sed -i -e 's/SCYLLA_ARGS=\"/SCYLLA_ARGS=\"%s /' /etc/sysconfig/scylla-server" % append_scylla_args)
            elif self.is_debian() or self.is_ubuntu():
                result = self.remoter.run("sudo grep '\\%s' /etc/default/scylla-server" %
                                          append_scylla_args, ignore_status=True)
                if result.exit_status == 1:
                    self.remoter.run(
                        "sudo sed -i -e 's/SCYLLA_ARGS=\"/SCYLLA_ARGS=\"%s /'  /etc/default/scylla-server" % append_scylla_args)

        if debug_install and self.is_rhel_like():
            self.remoter.run('sudo yum install -y scylla-gdb', verbose=True, ignore_status=True)

    def config_client_encrypt(self):
        self.remoter.send_files(src='./data_dir/ssl_conf', dst='/tmp/')  # pylint: disable=not-callable
        setup_script = dedent("""
            mkdir -p ~/.cassandra/
            cp /tmp/ssl_conf/client/cqlshrc ~/.cassandra/
            sudo mkdir -p /etc/scylla/
            sudo rm -rf /etc/scylla/ssl_conf/
            sudo mv -f /tmp/ssl_conf/ /etc/scylla/
        """)
        self.remoter.run('bash -cxe "%s"' % setup_script)

    def download_scylla_repo(self, scylla_repo):
        if not scylla_repo:
            self.log.error("Scylla YUM repo file url is not provided, it should be defined in configuration YAML!!!")
            return
        if self.is_rhel_like():
            repo_path = '/etc/yum.repos.d/scylla.repo'
            self.remoter.run('sudo curl -o %s -L %s' % (repo_path, scylla_repo))
            self.remoter.run('sudo chown root:root %s' % repo_path)
            self.remoter.run('sudo chmod 644 %s' % repo_path)
            result = self.remoter.run('cat %s' % repo_path, verbose=True)
            verify_scylla_repo_file(result.stdout, is_rhel_like=True)
        else:
            repo_path = '/etc/apt/sources.list.d/scylla.list'
            self.remoter.run('sudo curl -o %s -L %s' % (repo_path, scylla_repo))
            result = self.remoter.run('cat %s' % repo_path, verbose=True)
            verify_scylla_repo_file(result.stdout, is_rhel_like=False)
        self.update_repo_cache()

    def download_scylla_manager_repo(self, scylla_repo):
        if self.is_rhel_like():
            repo_path = '/etc/yum.repos.d/scylla-manager.repo'
        else:
            repo_path = '/etc/apt/sources.list.d/scylla-manager.list'
        self.remoter.run('sudo curl -o %s -L %s' % (repo_path, scylla_repo))
        if not self.is_rhel_like():
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)

    def install_manager_agent(self, package_path=None):
        auth_token = Setup.test_id()
        if package_path:
            package_name = '{}scylla-manager-agent*'.format(package_path)
        else:
            self.download_scylla_manager_repo(
                self.parent_cluster.params.get("scylla_mgmt_agent_repo",
                                               self.parent_cluster.params.get("scylla_mgmt_repo", None)))
            package_name = 'scylla-manager-agent'
        install_and_config_agent_command = dedent(r"""
            yum install -y {}
            sed -i 's/# auth_token:.*$/auth_token: {}/' /etc/scylla-manager-agent/scylla-manager-agent.yaml
            scyllamgr_ssl_cert_gen
            sed -i 's/#tls_cert_file/tls_cert_file/' /etc/scylla-manager-agent/scylla-manager-agent.yaml
            sed -i 's/#tls_key_file/tls_key_file/' /etc/scylla-manager-agent/scylla-manager-agent.yaml
            sed -i 's/#https/https/' /etc/scylla-manager-agent/scylla-manager-agent.yaml
            systemctl restart scylla-manager-agent
            systemctl enable scylla-manager-agent
        """.format(package_name, auth_token))
        self.remoter.run('sudo bash -cxe "%s"' % install_and_config_agent_command)
        version = self.remoter.run('scylla-manager-agent --version').stdout
        self.log.info(f'node {self.name} has scylla-manager-agent version {version}')

    def clean_scylla(self):
        """
        Uninstall scylla
        """
        self.stop_scylla_server(verify_down=False, ignore_status=True)
        if self.is_rhel_like():
            self.remoter.run('sudo yum remove -y scylla\\*')
        else:
            self.remoter.run('sudo rm -f /etc/apt/sources.list.d/scylla.list')
            self.remoter.run('sudo apt-get remove -y scylla\\*', ignore_status=True)
        self.update_repo_cache()
        self.remoter.run('sudo rm -rf /var/lib/scylla/commitlog/*')
        self.remoter.run('sudo rm -rf /var/lib/scylla/data/*')

    def update_repo_cache(self):
        try:
            if self.is_rhel_like():
                # try to avoid ERROR 404 of yum, reference https://wiki.centos.org/yum-errors
                self.remoter.run('sudo yum clean all')
                self.remoter.run('sudo rm -rf /var/cache/yum/')
                self.remoter.run('sudo yum makecache', retry=3)
            else:
                self.remoter.run('sudo apt-get clean all')
                self.remoter.run('sudo rm -rf /var/cache/apt/')
                self.remoter.run('sudo apt-get update', retry=3)
                self.remoter.run("echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections")
        except Exception as ex:  # pylint: disable=broad-except
            self.log.error('Failed to update repo cache: %s', ex)

    def upgrade_system(self):
        if self.is_rhel_like():
            # update system to latest
            result = self.remoter.run('ls /etc/yum.repos.d/epel.repo', ignore_status=True)
            if result.exit_status == 0:
                self.remoter.run('sudo yum update -y --skip-broken --disablerepo=epel', retry=3)
            else:
                self.remoter.run('sudo yum update -y --skip-broken', retry=3)
        else:
            self.remoter.run(
                'sudo DEBIAN_FRONTEND=noninteractive apt-get --force-yes -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" upgrade -y', retry=3)
        # update repo cache after upgrade
        self.update_repo_cache()

    def install_scylla(self, scylla_repo):
        """
        Download and install scylla on node
        :param scylla_repo: scylla repo file URL
        """
        self.log.info("Installing Scylla...")
        if self.is_rhel_like():
            # `screen' package is missed in CentOS/RHEL 8. Should be installed from EPEL repository.
            if self.distro.is_centos8:
                self.remoter.run("sudo yum install -y epel-release")
            elif self.distro.is_rhel8:
                self.remoter.run(
                    "sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm")
            self.remoter.run('sudo yum install -y rsync tcpdump screen wget net-tools')
            self.download_scylla_repo(scylla_repo)
            # hack cause of broken caused by EPEL
            self.remoter.run('sudo yum install -y python36-PyYAML', ignore_status=True)
            self.remoter.run('sudo yum install -y {}'.format(self.scylla_pkg()))
            self.remoter.run('sudo yum install -y scylla-gdb', ignore_status=True)
        else:
            if self.is_ubuntu14():
                self.remoter.run('sudo apt-get install software-properties-common -y')
                self.remoter.run('sudo add-apt-repository -y ppa:openjdk-r/ppa')
                self.remoter.run('sudo add-apt-repository -y ppa:scylladb/ppa')
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install -y openjdk-8-jre-headless')
                self.remoter.run('sudo update-java-alternatives -s java-1.8.0-openjdk-amd64')
            elif self.is_ubuntu18() or self.is_ubuntu16():
                install_prereqs = dedent("""
                    apt-get install software-properties-common -y
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6B2BFD3660EF3F5B
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5e08fbd8b5d6ec9c
                    add-apt-repository -y ppa:scylladb/ppa
                    apt-get update
                    apt-get install -y openjdk-8-jre-headless
                    update-java-alternatives -s java-1.8.0-openjdk-amd64
                """)
                self.remoter.run('sudo bash -cxe "%s"' % install_prereqs)
            elif self.is_debian8():
                self.remoter.run("sudo sed -i -e 's/jessie-updates/stable-updates/g' /etc/apt/sources.list")
                self.remoter.run(
                    'echo "deb http://archive.debian.org/debian jessie-backports main" |sudo tee /etc/apt/sources.list.d/backports.list')
                self.remoter.run(
                    r"sudo sed -i -e 's/:\/\/.*\/debian jessie-backports /:\/\/archive.debian.org\/debian jessie-backports /g' /etc/apt/sources.list.d/*.list")
                self.remoter.run(
                    "echo 'Acquire::Check-Valid-Until \"false\";' |sudo tee /etc/apt/apt.conf.d/99jessie-backports")
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install gnupg-curl -y')
                self.remoter.run(
                    'sudo apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key')
                self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19')
                self.remoter.run(
                    'echo "deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /" |sudo tee /etc/apt/sources.list.d/scylla-3rdparty.list')
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install -y openjdk-8-jre-headless -t jessie-backports')
                self.remoter.run('sudo update-java-alternatives -s java-1.8.0-openjdk-amd64')
            elif self.is_debian9():
                install_debian_9_prereqs = dedent("""
                    apt-get update
                    apt-get install apt-transport-https -y
                    apt-get install gnupg1-curl dirmngr -y
                    apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-stretch/Debian_9.0/Release.key
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5E08FBD8B5D6EC9C
                    echo 'deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-stretch/Debian_9.0/ /' > /etc/apt/sources.list.d/scylla-3rdparty.list
                """)
                self.remoter.run('sudo bash -cxe "%s"' % install_debian_9_prereqs)
            elif self.distro.is_debian10:
                install_debian_10_prereqs = dedent("""
                    export DEBIAN_FRONTEND=noninteractive
                    apt-get update
                    apt-get install apt-transport-https -y
                    apt-get install gnupg1-curl dirmngr -y
                    apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-buster/Debian_10.0/Release.key
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19
                    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5E08FBD8B5D6EC9C
                    echo 'deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-buster/Debian_10.0/ /' > /etc/apt/sources.list.d/scylla-3rdparty.list
                """)
                self.remoter.run('sudo bash -cxe "%s"' % install_debian_10_prereqs)

            self.remoter.run(
                'sudo DEBIAN_FRONTEND=noninteractive apt-get --force-yes -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" upgrade -y')
            self.remoter.run('sudo apt-get install -y rsync tcpdump screen wget net-tools')
            self.download_scylla_repo(scylla_repo)
            self.remoter.run('sudo apt-get update')
            self.remoter.run(
                'sudo apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" --force-yes --allow-unauthenticated {0}'.format(self.scylla_pkg()))

    def install_scylla_debuginfo(self):
        self.log.info("Installing Scylla debug info...")
        if not self.scylla_version:
            self.get_scylla_version()
        if self.is_rhel_like():
            self.remoter.run(
                r'sudo yum install -y {0}-debuginfo-{1}\*'.format(self.scylla_pkg(), self.scylla_version), ignore_status=True)
        else:
            self.remoter.run(r'sudo apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" --force-yes --allow-unauthenticated {0}-server-dbg={1}\*'
                             .format(self.scylla_pkg(), self.scylla_version), ignore_status=True)

    def get_scylla_version(self):
        version_commands = ["scylla --version", "rpm -q {}".format(self.scylla_pkg())]
        for version_cmd in version_commands:
            try:
                result = self.remoter.run(version_cmd)
                self.log.info("'scylla --version' output: %s", result.stdout)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.error('Failed getting scylla version: %s', ex)
            else:
                match = re.match(r"((\d+)\.(\d+)\.([\d\w]+)\.?([\d\w]+)?).*", result.stdout)
                if match:
                    scylla_version = match.group(1)
                    self.log.info("Found ScyllaDB version: %s", scylla_version)
                    self.scylla_version = scylla_version
                    return scylla_version
                else:
                    self.log.error("Unmatched ScyllaDB version, not caching it")
        return None

    @log_run_info("Detecting disks")
    def detect_disks(self, nvme=True):
        """
        Detect local disks
        :param nvme: NVMe(True) or SCSI(False) disk
        :return: list of disk names
        """
        patt = (r'nvme0n*', r'nvme0n\w+') if nvme else (r'sd[b-z]', r'sd\w+')
        result = self.remoter.run('ls /dev/{}'.format(patt[0]))
        disks = re.findall(r'/dev/{}'.format(patt[1]), result.stdout)
        assert disks, 'Failed to find disks!'
        self.log.debug("Found disks: %s", disks)
        return disks

    @property
    def kernel_version(self):
        if not self._kernel_version:
            res = self.remoter.run("uname -r", ignore_status=True)
            if res.exit_status:
                self._kernel_version = "unknown"
            else:
                self._kernel_version = res.stdout.strip()
            self.log.info("Found kernel version: {}".format(self._kernel_version))
        return self._kernel_version

    @log_run_info
    def scylla_setup(self, disks):
        """
        Setup scylla
        :param disks: list of disk names
        """
        result = self.remoter.run('/sbin/ip -o link show |grep ether |awk -F": " \'{print $2}\'', verbose=True)
        devname = result.stdout.strip()
        if self.parent_cluster.params.get('workaround_kernel_bug_for_iotune'):
            self.log.warning(dedent("""
                Kernel version is {}. Due to known kernel bug in this version using predefined iotune.
                related issue: https://github.com/scylladb/scylla/issues/5181
                known kernel bug will cause scylla_io_setup fails in executing iotune.
                The kernel bug doesn't occur all the time, so we can get some succeed gce instance.
                the config files are copied from a succeed GCE instance (same instance type, same test
            """.format(self.kernel_version)))
            self.remoter.run(
                'sudo /usr/lib/scylla/scylla_setup --nic {} --disks {} --no-io-setup'.format(devname, ','.join(disks)))
            for conf in ['io.conf', 'io_properties.yaml']:
                self.remoter.send_files(src=os.path.join('./configurations/', conf),  # pylint: disable=not-callable
                                        dst='/tmp/')
                self.remoter.run('sudo mv /tmp/{0} /etc/scylla.d/{0}'.format(conf))
        else:
            self.remoter.run('sudo /usr/lib/scylla/scylla_setup --nic {} --disks {}'.format(devname, ','.join(disks)))

        result = self.remoter.run('cat /proc/mounts')
        assert ' /var/lib/scylla ' in result.stdout, "RAID setup failed, scylla directory isn't mounted correctly"
        self.remoter.run('sudo sync')
        self.log.info('io.conf right after setup')
        self.remoter.run('sudo cat /etc/scylla.d/io.conf')
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl enable scylla-server.service')
            self.remoter.run('sudo systemctl enable scylla-jmx.service')

    def upgrade_mgmt(self, scylla_mgmt_repo):
        self.download_scylla_manager_repo(scylla_mgmt_repo)
        self.log.debug('Upgrade scylla-manager via repo: {}'.format(scylla_mgmt_repo))
        if self.is_rhel_like():
            self.remoter.run('sudo yum update scylla-manager -y')
        else:
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            # Upgrade should update packages of:
            # 1) scylla-manager
            # 2) scylla-manager-client
            # 3) scylla-manager-server
            self.remoter.run('sudo apt-get --only-upgrade install -y scylla-manager*')
        time.sleep(3)
        if self.is_docker():
            self.remoter.run('sudo supervisorctl start scylla-manager')
        else:
            self.remoter.run('sudo systemctl restart scylla-manager.service')
        time.sleep(5)

    # pylint: disable=too-many-branches,too-many-statements
    def install_mgmt(self, scylla_mgmt_repo, auth_token, segments_per_repair, package_url=None):
        self.log.debug('Install scylla-manager')
        rsa_id_dst = '/tmp/scylla-test'
        rsa_id_dst_pub = '/tmp/scylla-test-pub'
        mgmt_user = 'scylla-manager'
        if not (self.is_rhel_like() or self.is_debian() or self.is_ubuntu()):
            raise ValueError('Unsupported Distribution type: {}'.format(str(self.distro)))
        if self.is_debian8():
            self.remoter.run(cmd="sudo sed -i -e 's/jessie-updates/stable-updates/g' /etc/apt/sources.list")
            self.remoter.run(
                cmd="sudo echo 'deb http://archive.debian.org/debian jessie-backports main' | sudo tee -a /etc/apt/sources.list.d/backports.list")
            self.remoter.run(cmd="sudo touch /etc/apt/apt.conf.d/99jessie-backports")
            self.remoter.run(
                cmd="sudo echo 'Acquire::Check-Valid-Until \"false\";' | sudo tee /etc/apt/apt.conf.d/99jessie-backports")
            self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F76221572C52609D', retry=3)
            install_transport_https = dedent(r"""
                            if [ ! -f /etc/apt/sources.list.d/backports.list ]; then sudo echo 'deb http://archive.debian.org/debian jessie-backports main' | sudo tee /etc/apt/sources.list.d/backports.list > /dev/null; fi
                            sed -e 's/:\/\/.*\/debian jessie-backports /:\/\/archive.debian.org\/debian jessie-backports /g' /etc/apt/sources.list.d/*.list
                            echo 'Acquire::Check-Valid-Until false;' > /etc/apt/apt.conf.d/99jessie-backports
                            sed -i -e 's/jessie-updates/stable-updates/g' /etc/apt/sources.list
                            apt-get install apt-transport-https
                            apt-get install gnupg-curl
                            apt-get update
                            apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
                            apt-get install -t jessie-backports ca-certificates-java -y
            """)
            self.remoter.run('sudo bash -cxe "%s"' % install_transport_https)
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)

            install_transport_backports = dedent("""
                apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
                # apt-get install -t jessie-backports ca-certificates-java -y
                apt-get install ca-certificates-java -y
            """)
            self.remoter.run('sudo bash -cxe "%s"' % install_transport_backports)

        if self.is_debian9():
            install_debian_9_prereqs = dedent("""
                if [ ! -f /etc/apt/sources.list.d/backports.list ]; then echo 'deb http://http.debian.net/debian jessie-backports main' | tee /etc/apt/sources.list.d/backports.list > /dev/null; fi
                apt-get install apt-transport-https
                apt-get update
                apt-get install dirmngr
                apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
            """)
            self.remoter.run('sudo bash -cxe "%s"' % install_debian_9_prereqs)

        if self.is_debian():
            install_open_jdk = dedent("""
                apt-get install -y openjdk-8-jre-headless -t jessie-backports
                update-java-alternatives --jre-headless -s java-1.8.0-openjdk-amd64
            """)
            self.remoter.run('sudo bash -cxe "%s"' % install_open_jdk)

        if self.is_rhel_like():
            self.remoter.run('sudo yum install -y epel-release', retry=3)
            self.remoter.run('sudo yum install python36-PyYAML -y', retry=3)
        else:
            self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 6B2BFD3660EF3F5B', retry=3)
            self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19', retry=3)
            self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 5E08FBD8B5D6EC9C', retry=3)

        self.log.debug("Copying TLS files from data_dir to node")
        self.remoter.send_files(src='./data_dir/ssl_conf', dst='/tmp/')  # pylint: disable=not-callable

        if package_url:
            package_names = '{0}scylla-manager-server* {0}scylla-manager-client*'.format(package_url)
        else:
            self.download_scylla_manager_repo(scylla_mgmt_repo)
            package_names = 'scylla-manager'
        if self.is_docker():
            self.remoter.run('sudo yum remove -y scylla scylla-jmx scylla-tools scylla-tools-core'
                             ' scylla-server scylla-conf')

        if self.is_rhel_like():
            self.remoter.run('sudo yum install -y {}'.format(package_names))
        else:
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            self.remoter.run(
                'sudo apt-get install -y {}{}'.format(package_names, ' --force-yes' if not self.is_debian9() else ''))

        if self.is_docker():
            try:
                self.remoter.run('echo no| sudo scyllamgr_setup')
            except Exception as ex:  # pylint: disable=broad-except
                self.log.warning(ex)
        else:
            self.remoter.run('echo yes| sudo scyllamgr_setup')
        self.remoter.send_files(src=self.ssh_login_info['key_file'], dst=rsa_id_dst)  # pylint: disable=not-callable
        ssh_config_script = dedent("""
                chmod 0400 {rsa_id_dst}
                chown {mgmt_user}:{mgmt_user} {rsa_id_dst}
                ssh-keygen -y -f {rsa_id_dst} > {rsa_id_dst_pub}
        """.format(mgmt_user=mgmt_user, rsa_id_dst=rsa_id_dst, rsa_id_dst_pub=rsa_id_dst_pub))  # generate ssh public key from private key.
        self.remoter.run('sudo bash -cxe "%s"' % ssh_config_script)

        self.config_scylla_manager_yaml(segments_per_repair=segments_per_repair)

        if self.is_docker():
            self.remoter.run('sudo supervisorctl restart scylla-manager')
            res = self.remoter.run('sudo supervisorctl status scylla-manager')
        else:
            self.remoter.run('sudo systemctl restart scylla-manager.service')
            res = self.remoter.run('sudo systemctl status scylla-manager.service')

        if self.is_rhel_like():  # TODO: Add debian and ubuntu support
            configuring_manager_command = dedent("""
            scyllamgr_ssl_cert_gen
            sed -i 's/#tls_cert_file/tls_cert_file/' /etc/scylla-manager/scylla-manager.yaml
            sed -i 's/#tls_key_file/tls_key_file/' /etc/scylla-manager/scylla-manager.yaml
            systemctl restart scylla-manager
            """.format(auth_token))  # pylint: disable=too-many-format-args
            self.remoter.run('sudo bash -cxe "%s"' % configuring_manager_command)

        if not res or "Active: failed" in res.stdout:
            raise ScyllaManagerError("Scylla-Manager is not properly installed or not running: {}".format(res))

        self.start_scylla_manager_log_capture()

    def retrieve_scylla_manager_log(self):
        mgmt_log_name = os.path.join(self.logdir, 'scylla_manager.log')
        cmd = "sudo journalctl -u scylla-manager -f"
        self.remoter.run(cmd, ignore_status=True, verbose=True, log_file=mgmt_log_name)

    def scylla_manager_log_thread(self):
        while not self.termination_event.isSet():
            self.retrieve_scylla_manager_log()

    def start_scylla_manager_log_capture(self):
        self._scylla_manager_journal_thread = threading.Thread(target=self.scylla_manager_log_thread)
        self._scylla_manager_journal_thread.start()

    def stop_scylla_manager_log_capture(self, timeout=10):
        cmd = "sudo pkill -f \"sudo journalctl -u scylla-manager -f\""
        self.remoter.run(cmd, ignore_status=True, verbose=True)
        self._scylla_manager_journal_thread.join(timeout)
        self._scylla_manager_journal_thread = None

    def config_scylla_manager_yaml(self, segments_per_repair):
        yaml_attr = "repair:\n  segments_per_repair: %d\n" % segments_per_repair
        self.remoter.run("""sudo sh -c 'echo "{}" >> {}'""".format(yaml_attr, SCYLLA_MANAGER_YAML_PATH))

    def config_scylla_manager(self, mgmt_port, db_hosts):
        """
        this code was took out from  install_mgmt() method.
        it may be usefull for manager testing future enhancements.

        Usage may be:
            if self.params.get('mgmt_db_local', default=True):
                mgmt_db_hosts = ['127.0.0.1']
            else:
                mgmt_db_hosts = [str(trg) for trg in self.targets['db_nodes']]
            node.config_scylla_manager(mgmt_port=self.params.get('mgmt_port', default=10090),
                              db_hosts=mgmt_db_hosts)

        :param mgmt_port:
        :param db_hosts:
        :return:
        """
        # only support for centos
        self.log.debug('Install scylla-manager')
        rsa_id_dst = '/tmp/scylla-test'
        mgmt_conf_tmp = '/tmp/scylla-manager.yaml'
        mgmt_conf_dst = '/etc/scylla-manager/scylla-manager.yaml'

        mgmt_conf = {'http': '0.0.0.0:{}'.format(mgmt_port),
                     'database':
                         {'hosts': db_hosts,
                          'timeout': '5s'},
                     'ssh':
                         {'user': self.ssh_login_info['user'],
                          'identity_file': rsa_id_dst}
                     }
        (_, conf_file) = tempfile.mkstemp(dir='/tmp')
        with open(conf_file, 'w') as fd:
            yaml.dump(mgmt_conf, fd, default_flow_style=False)
        self.remoter.send_files(src=conf_file, dst=mgmt_conf_tmp)  # pylint: disable=not-callable
        self.remoter.run('sudo cp {} {}'.format(mgmt_conf_tmp, mgmt_conf_dst))
        if self.is_docker():
            self.remoter.run('sudo supervisorctl start scylla-manager')
        else:
            self.remoter.run('sudo systemctl restart scylla-manager.service')

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl start scylla-server.service', timeout=timeout)
        else:
            self.remoter.run('sudo service scylla-server start', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=verify_up_timeout)

    def start_scylla_jmx(self, verify_up=True, verify_down=False, timeout=300, verify_up_timeout=300):
        if verify_down:
            self.wait_jmx_down(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl start scylla-jmx.service', timeout=timeout)
        else:
            self.remoter.run('sudo service scylla-jmx start', timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=verify_up_timeout)

    @log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=timeout)

    @retrying(n=3, sleep_time=5, allowed_exceptions=(CommandTimedOut, NoValidConnectionsError,),
              message="Failed to stop scylla.server, retrying...")
    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300, ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl stop scylla-server.service', timeout=timeout, ignore_status=ignore_status)
        else:
            self.remoter.run('sudo service scylla-server stop', timeout=timeout, ignore_status=ignore_status)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    def stop_scylla_jmx(self, verify_up=False, verify_down=True, timeout=300):
        if verify_up:
            self.wait_jmx_up(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl stop scylla-jmx.service', timeout=timeout)
        else:
            self.remoter.run('sudo service scylla-jmx stop', timeout=timeout)
        if verify_down:
            self.wait_jmx_down(timeout=timeout)

    @log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        if verify_down:
            self.wait_jmx_down(timeout=timeout)

    def restart_scylla_server(self, verify_up_before=False, verify_up_after=True, timeout=300, ignore_status=False):
        if verify_up_before:
            self.wait_db_up(timeout=timeout)
        if not self.distro.is_ubuntu14:
            self.remoter.run("sudo systemctl restart scylla-server.service",
                             timeout=timeout, ignore_status=ignore_status)
        else:
            self.remoter.run("sudo service scylla-server restart",
                             timeout=timeout, ignore_status=ignore_status)
        if verify_up_after:
            self.wait_db_up(timeout=timeout)

    def restart_scylla_jmx(self, verify_up_before=False, verify_up_after=True, timeout=300):
        if verify_up_before:
            self.wait_jmx_up(timeout=timeout)
        if not self.distro.is_ubuntu14:
            self.remoter.run("sudo systemctl restart scylla-jmx.service", timeout=timeout)
        else:
            self.remoter.run("sudo service scylla-jmx restart", timeout=timeout)
        if verify_up_after:
            self.wait_jmx_up(timeout=timeout)

    @log_run_info
    def restart_scylla(self, verify_up_before=False, verify_up_after=True, timeout=300):
        self.restart_scylla_server(verify_up_before=verify_up_before, verify_up_after=verify_up_after, timeout=timeout)
        if verify_up_after:
            self.wait_jmx_up(timeout=timeout)

    def enable_client_encrypt(self):
        SCYLLA_YAML_PATH_TMP = "/tmp/scylla.yaml"
        self.remoter.run("sudo cat {} | grep -v '<client_encrypt>' > {}".format(SCYLLA_YAML_PATH, SCYLLA_YAML_PATH_TMP))
        self.remoter.run("sudo mv -f {} {}".format(SCYLLA_YAML_PATH_TMP, SCYLLA_YAML_PATH))
        self.parent_cluster.node_config_setup(node=self, client_encrypt=True)
        self.stop_scylla()
        self.start_scylla()

    def disable_client_encrypt(self):
        self.parent_cluster.node_config_setup(node=self, client_encrypt=False)
        self.stop_scylla()
        self.start_scylla()

    def prepare_files_for_archive(self, fileslist):
        """Prepare files for creating archives on node

        Create directories structure and copy files
        from absolute path of files in fileslist
        in node /tmp/node_name and return path to created
        structure
        Ex.:
            filelists = ['/proc/cpuinfo', /var/log/system.log]

            result directory:
            /tmp/<node_name>/proc/cpuinfo
                            /var/log/system.log

        Arguments:
            fileslist {list} -- list of file with fullpaths on node

        Returns:
            [type] -- path to created directory structure
        """
        root_dir = '/tmp/%s' % self.name
        self.remoter.run('mkdir -p %s' % root_dir, ignore_status=True)
        for f in fileslist:
            if self.file_exists(f):
                old_full_path = os.path.dirname(f)[1:]
                new_full_path = os.path.join(root_dir, old_full_path)
                self.remoter.run('mkdir -p %s' % new_full_path, ignore_status=True)
                self.remoter.run('cp -r %s %s' % (f, new_full_path), ignore_status=True)
        return root_dir

    def generate_coredump_file(self, restart_scylla=True):
        self.log.info('Generate scylla core')
        self.remoter.run("sudo pkill -f --signal 3 /usr/bin/scylla")
        self.wait_db_down(timeout=600)
        if restart_scylla:
            self.log.debug('Restart scylla server')
            self.stop_scylla(timeout=600)
            self.start_scylla(timeout=600)

    def get_console_output(self):
        # TODO add to each type of node
        # comment raising exception. replace with log warning
        # raise NotImplementedError('Derived classes must implement get_console_output')
        self.log.warning('Method is not implemented for %s' % self.__class__.__name__)
        return ''

    def get_console_screenshot(self):
        # TODO add to each type of node
        # comment raising exception. replace with log warning
        # raise NotImplementedError('Derived classes must implement get_console_output')
        self.log.warning('Method is not implemented for %s' % self.__class__.__name__)
        return b''

    def _resharding_status(self, status):
        """
        Check is there's Reshard listed in the log
        status : expected values: "start" or "finish"
        """
        patt = re.compile('RESHARD')
        result = self.run_nodetool("compactionstats")
        found = patt.search(result.stdout)
        # wait_for_status=='finish': If 'RESHARD' is not found in the compactionstats output, return True -
        # means resharding was finished
        # wait_for_status=='start: If 'RESHARD' is found in the compactionstats output, return True -
        # means resharding was started

        return bool(found) if status == 'start' else not bool(found)

    # Default value of murmur3_partitioner_ignore_msb_bits parameter is 12
    def restart_node_with_resharding(self, murmur3_partitioner_ignore_msb_bits=12):
        self.stop_scylla()
        # Change murmur3_partitioner_ignore_msb_bits parameter to cause resharding.
        self.parent_cluster.node_config_setup(
            node=self, murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)
        self.start_scylla()

        resharding_started = wait.wait_for(func=self._resharding_status, step=5, timeout=3600,
                                           text="Wait for re-sharding to be started", status='start')
        if not resharding_started:
            self.log.error('Resharding has not been started (murmur3_partitioner_ignore_msb_bits={}) '
                           'Check the log for the details'.format(murmur3_partitioner_ignore_msb_bits))

            return

        resharding_finished = wait.wait_for(func=self._resharding_status, step=5,
                                            text="Wait for re-sharding to be finished", status='finish')

        if not resharding_finished:
            self.log.error('Resharding was not finished! (murmur3_partitioner_ignore_msb_bits={}) '
                           'Check the log for the details'.format(murmur3_partitioner_ignore_msb_bits))
        else:
            self.log.debug('Resharding has been finished successfully (murmur3_partitioner_ignore_msb_bits={})'.
                           format(murmur3_partitioner_ignore_msb_bits))

    def _gen_nodetool_cmd(self, sub_cmd, args, options):
        credentials = self.parent_cluster.get_db_auth()
        if credentials:
            options += '-u {} -pw {} '.format(*credentials)
        return "nodetool {options} {sub_cmd} {args}".format(options=options, sub_cmd=sub_cmd, args=args)

    def run_nodetool(self, sub_cmd, args="", options="", timeout=None,
                     ignore_status=False, verbose=True, coredump_on_timeout=False):
        """
            Wrapper for nodetool command.
            Command format: nodetool [options] command [args]

        :param sub_cmd: subcommand like status
        :param args: arguments for the subcommand
        :param options: nodetool options:
            -h  --host  Hostname or IP address.
            -p  --port  Port number.
            -pwf    --password-file Password file path.
            -pw --password  Password.
            -u  --username  Username.
        :param timeout: time for command execution
        :param ignore_status: don't throw exception if the command fails
        :param coredump_on_timeout: Send signal SIGQUIT to scylla process
        :return: Remoter result object
        """
        cmd = self._gen_nodetool_cmd(sub_cmd, args, options)
        try:
            result = self.remoter.run(cmd, timeout=timeout, ignore_status=ignore_status, verbose=verbose)
            self.log.debug("Command '%s' duration -> %s s" % (result.command, result.duration))
            return result
        except Exception as details:  # pylint: disable=broad-except
            self.log.critical(f"Command '{cmd}' error: {details}")
            if coredump_on_timeout and isinstance(details, CommandTimedOut):
                self.generate_coredump_file()
            raise

    def check_node_health(self):
        # Task 1443: ClusterHealthCheck is bottle neck in scale test and create a lot of noise in 5000 tables test.
        # Disable it
        if not self.parent_cluster.params.get('cluster_health_check'):
            return

        self.check_nodes_status()
        self.check_schema_version()

    def check_nodes_status(self):
        # Validate nodes health
        node_type = 'target' if self.running_nemesis else 'regular'
        # TODO: improve this part (using get_nodetool_status function from ScyllaCluster)
        try:
            self.log.info('Status for %s node %s: %s' % (node_type, self.name, self.run_nodetool('status')))

        except Exception as ex:  # pylint: disable=broad-except
            ClusterHealthValidatorEvent(type='error', name='NodesStatus', status=Severity.ERROR,
                                        node=self.name,
                                        error="Unable to get nodetool status from '{node}': {ex}".format(ex=ex,
                                                                                                         node=self.name))

    def validate_gossip_nodes_info(self, gossip_info):
        gossip_node_schemas = {}
        schema, ip, status = '', '', ''
        for line in gossip_info.stdout.split():
            if line.startswith('SCHEMA:'):
                schema = line.replace('SCHEMA:', '')
            elif line.startswith('RPC_ADDRESS:'):
                ip = line.replace('RPC_ADDRESS:', '')
            elif line.startswith('STATUS:'):
                status = line.replace('STATUS:', '').split(',')[0]
            # STATUS is "LEFT": in case the node was decommissioned
            # STATUS is "removed": in case the node was removed by nodetool removenode
            # STATUS is "BOOT": node during boot and not exists in the cluster yet
            # and they will remain in the gossipinfo 3 days.
            # It's expected behaviour and we won't send the error in this case
            if schema and ip and status:
                if status not in ['LEFT', 'removed', 'BOOT']:
                    gossip_node_schemas[ip] = {'schema': schema, 'status': status}
                schema, ip, status = '', '', ''

        self.log.info('Gossipinfo schema version and status of all nodes: {}'.format(gossip_node_schemas))

        # Validate that ALL initiated nodes in the gossip
        cluster_nodes = [node.ip_address for node in self.parent_cluster.nodes if node.db_init_finished]

        not_in_gossip = list(set(cluster_nodes) - set(gossip_node_schemas.keys()))
        if not_in_gossip:
            ClusterHealthValidatorEvent(type='error', name='GossipNodeSchemaVersion', status=Severity.ERROR,
                                        node=self.name,
                                        error='Node(s) %s exists in the cluster, but doesn\'t exist in the gossip'
                                              % ', '.join(ip for ip in not_in_gossip))

        # Validate that JUST existent nodes in the gossip
        not_in_cluster = list(set(gossip_node_schemas.keys()) - set(cluster_nodes))
        if not_in_cluster:
            ClusterHealthValidatorEvent(type='error', name='GossipNodeSchemaVersion', status=Severity.ERROR,
                                        node=self.name,
                                        error='Node(s) %s exists in the gossip, but doesn\'t exist in the cluster'
                                              % ', '.join(ip for ip in not_in_cluster))
            for ip in not_in_cluster:
                gossip_node_schemas.pop(ip)

        # Validate that same schema on all nodes
        if len(set(node_info['schema'] for node_info in gossip_node_schemas.values())) > 1:
            ClusterHealthValidatorEvent(type='error', name='GossipNodeSchemaVersion', status=Severity.ERROR,
                                        node=self.name,
                                        error='Schema version is not same on all nodes in gossip info: {}'
                                        .format('\n'.join('{}: {}'.format(ip, schema_version['schema'])
                                                          for ip, schema_version in gossip_node_schemas.items())))

        return gossip_node_schemas

    def check_schema_version(self):
        # Get schema version
        errors = []
        peers_details = ''
        try:
            gossip_info = self.run_nodetool('gossipinfo', verbose=False)
            peers_details = self.run_cqlsh('select peer, data_center, host_id, rack, release_version, '
                                           'rpc_address, schema_version, supported_features from system.peers',
                                           split=True, verbose=False)
            gossip_node_schemas = self.validate_gossip_nodes_info(gossip_info)
            status = gossip_node_schemas[self.ip_address]['status']
            if status != 'NORMAL':
                self.log.debug('Node status is {status}. Schema version can\'t be validated'. format(status=status))
                return

            # Search for nulls in system.peers

            for line in peers_details[3:-2]:
                line_splitted = line.split('|')
                current_node_ip = line_splitted[0].strip()
                nulls = [column for column in line_splitted[1:] if column.strip() == 'null']

                if nulls:
                    errors.append('Found nulls in system.peers on the node %s: %s' % (current_node_ip, peers_details))

                peer_schema_version = line_splitted[6].strip()
                gossip_node_schema_version = gossip_node_schemas[self.ip_address]['schema']
                if gossip_node_schema_version and peer_schema_version != gossip_node_schema_version:
                    errors.append('Expected schema version: %s. Wrong schema version found on the '
                                  'node %s: %s' % (gossip_node_schema_version, current_node_ip, peer_schema_version))
        except Exception as ex:  # pylint: disable=broad-except
            errors.append('Validate schema version failed.{} Error: {}'
                          .format(' SYSTEM.PEERS content: {}\n'.format(peers_details) if peers_details else '', str(ex)))

        if errors:
            ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.ERROR,
                                        node=self.name, error='\n'.join(errors))

    def _gen_cqlsh_cmd(self, command, keyspace, timeout, host, port, connect_timeout):
        """cqlsh [options] [host [port]]"""
        credentials = self.parent_cluster.get_db_auth()
        auth_params = '-u {} -p {}'.format(*credentials) if credentials else ''
        use_keyspace = "--keyspace {}".format(keyspace) if keyspace else ""
        ssl_params = '--ssl' if self.parent_cluster.params.get("client_encrypt") else ''
        options = "--no-color {auth_params} {use_keyspace} --request-timeout={timeout} " \
                  "--connect-timeout={connect_timeout} {ssl_params}".format(
                      auth_params=auth_params, use_keyspace=use_keyspace, timeout=timeout,
                      connect_timeout=connect_timeout, ssl_params=ssl_params)
        return 'cqlsh {options} -e "{command}" {host} {port}'.format(options=options, command=command, host=host,
                                                                     port=port)

    def run_cqlsh(self, cmd, keyspace=None, port=None, timeout=120, verbose=True, split=False, target_db_node=None,
                  connect_timeout=60):
        """Runs CQL command using cqlsh utility"""
        cmd = self._gen_cqlsh_cmd(command=cmd, keyspace=keyspace, timeout=timeout,
                                  host=self.ip_address if not target_db_node else target_db_node.ip_address,
                                  port=port if port else self.CQL_PORT,
                                  connect_timeout=connect_timeout)
        cqlsh_out = self.remoter.run(cmd, timeout=timeout + 30,  # we give 30 seconds to cqlsh timeout mechanism to work
                                     verbose=verbose)
        # stdout of cqlsh example:
        #      pk
        #      ----
        #       2
        #       3
        #
        #      (10 rows)
        return cqlsh_out if not split else list(map(str.strip, cqlsh_out.stdout.splitlines()))

    def run_startup_script(self):
        startup_script_remote_path = '/tmp/sct-startup.sh'

        with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(Setup.get_startup_script())
            tmp_file.flush()
            self.remoter.send_files(src=tmp_file.name, dst=startup_script_remote_path)  # pylint: disable=not-callable

        cmds = dedent("""
                chmod +x {0}
                {0}
            """.format(startup_script_remote_path))

        result = self.remoter.run("sudo bash -ce '%s'" % cmds)
        LOGGER.debug(result.stdout)

    def create_swap_file(self, size=1024):
        """Create swap file on instance

        Create swap file on instance with size 1MB * 1024 = 1GB
        :param size: size of swap file in MB, defaults to 1024MB
        :type size: number, optional
        """
        commands = dedent("""sudo /bin/dd if=/dev/zero of=/var/sct_configured_swapfile bs=1M count={}
                          sudo /sbin/mkswap /var/sct_configured_swapfile
                          sudo chmod 600 /var/sct_configured_swapfile
                          sudo /sbin/swapon /var/sct_configured_swapfile""".format(size))
        self.log.info("Add swap file to loader %s", self)
        result = self.remoter.run(commands, ignore_status=True)
        if not result.ok:
            self.log.warning("Swap file was not created on loader node %s.\nError details: %s", self, result.stderr)
        result = self.remoter.run("grep /sct_configured_swapfile /proc/swaps", ignore_status=True)
        if "sct_configured_swapfile" not in result.stdout:
            self.log.warning("Swap file is not used on loader node %s.\nError details: %s", self, result.stderr)

    def set_hostname(self):
        self.log.warning('Method is not implemented for %s' % self.__class__.__name__)

    @property
    def scylla_packages_installed(self) -> List[str]:
        if self.distro.is_rhel_like:
            cmd = "rpm -qa 'scylla*'"
        else:
            cmd = "dpkg-query --show 'scylla*'"
        return self.remoter.run(cmd).stdout.splitlines()


class BaseCluster:  # pylint: disable=too-many-instance-attributes
    """
    Cluster of Node objects.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, cluster_uuid=None, cluster_prefix='cluster', node_prefix='node', n_nodes=3, params=None,
                 region_names=None, node_type=None, extra_network_interface=False):
        self.extra_network_interface = extra_network_interface
        if cluster_uuid is None:
            self.uuid = Setup.test_id()
        else:
            self.uuid = cluster_uuid
        self.node_type = node_type
        self.shortid = str(self.uuid)[:8]
        self.name = '%s-%s' % (cluster_prefix, self.shortid)
        self.node_prefix = '%s-%s' % (node_prefix, self.shortid)
        self._node_index = 0
        # I wanted to avoid some parameter passing
        # from the tester class to the cluster test.
        assert '_SCT_TEST_LOGDIR' in os.environ

        self.logdir = os.path.join(os.environ['_SCT_TEST_LOGDIR'],
                                   self.name)
        makedirs(self.logdir)

        self.log = SDCMAdapter(LOGGER, extra={'prefix': str(self)})
        self.log.info('Init nodes')
        self.nodes = []
        self.instance_provision = params.get('instance_provision')
        self.params = params
        self.datacenter = region_names or []
        if Setup.REUSE_CLUSTER:
            # get_node_ips_param should be defined in child
            self._node_public_ips = self.params.get(self.get_node_ips_param(public_ip=True), None) or []
            self._node_private_ips = self.params.get(self.get_node_ips_param(public_ip=False), None) or []
            self.log.debug('Node public IPs: {}, private IPs: {}'.format(self._node_public_ips, self._node_private_ips))

        if isinstance(n_nodes, list):
            for dc_idx, num in enumerate(n_nodes):
                self.add_nodes(num, dc_idx=dc_idx)
        elif isinstance(n_nodes, int):  # legacy type
            self.add_nodes(n_nodes)
        else:
            raise ValueError('Unsupported type: {}'.format(type(n_nodes)))
        self.coredumps = dict()
        super(BaseCluster, self).__init__()

    def send_file(self, src, dst, verbose=False):
        for loader in self.nodes:
            loader.remoter.send_files(src=src, dst=dst, verbose=verbose)

    def run(self, cmd, verbose=False):
        for loader in self.nodes:
            loader.remoter.run(cmd=cmd, verbose=verbose)

    def run_func_parallel(self, func, node_list=None):
        if node_list is None:
            node_list = self.nodes

        _queue = queue.Queue()
        for node in node_list:
            setup_thread = threading.Thread(target=func, args=(node, _queue))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(_queue.get(block=True, timeout=5))
            except queue.Empty:
                pass
        return results

    def get_backtraces(self):
        for node in self.nodes:
            try:
                node.get_backtraces()
                if node.n_coredumps > 0:
                    self.coredumps[node.name] = node.n_coredumps
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unable to get coredump status from node {node}: {ex}".format(node=node, ex=ex))

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(self, public_ip=True):
        raise NotImplementedError("Derived class must implement 'get_node_ips_param' method!")

    def wait_for_init(self):
        raise NotImplementedError("Derived class must implement 'wait_for_init' method!")

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        """
        :param count: number of nodes to add
        :param ec2_user_data:
        :param dc_idx: datacenter index, used as an index for self.datacenter list
        :return: list of Nodes
        """
        raise NotImplementedError("Derived class must implement 'add_nodes' method!")

    def get_node_private_ips(self):
        return [node.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.public_ip_address for node in self.nodes]

    def get_node_external_ips(self):
        return [node.external_address for node in self.nodes]

    def get_node_database_errors(self):
        errors = {}
        for node in self.nodes:
            node_errors = node.search_database_log(start_from_beginning=True, publish_events=False)
            if node_errors:
                errors.update({node.name: node_errors})
        return errors

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            if IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION:
                from sdcm.auto_ssh import stop_auto_ssh
                stop_auto_ssh(Setup.test_id(), node)

            node.destroy()

    def terminate_node(self, node):
        self.nodes.remove(node)
        if IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION:
            from sdcm.auto_ssh import stop_auto_ssh
            stop_auto_ssh(Setup.test_id(), node)

        node.destroy()

    def get_db_auth(self):
        user = self.params.get('authenticator_user', default=None)
        password = self.params.get('authenticator_password', default=None)
        return (user, password) if user and password else None

    def write_node_public_ip_file(self):
        public_ip_file_path = os.path.join(self.logdir, 'public_ips')
        with open(public_ip_file_path, 'w') as public_ip_file:
            public_ip_file.write("%s" % "\n".join(self.get_node_public_ips()))
            public_ip_file.write("\n")

    def write_node_private_ip_file(self):
        private_ip_file_path = os.path.join(self.logdir, 'private_ips')
        with open(private_ip_file_path, 'w') as private_ip_file:
            private_ip_file.write("%s" % "\n".join(self.get_node_private_ips()))
            private_ip_file.write("\n")

    def set_keep_tag_on_failure(self):
        for node in self.nodes:
            if hasattr(node, "set_keep_tag"):
                node.set_keep_tag()


class NodeSetupFailed(Exception):
    pass


class NodeSetupTimeout(Exception):
    pass


def wait_for_init_wrap(method):
    """
    Wraps wait_for_init class method.
    Run setup of nodes simultaneously and wait for all the setups finished.
    Raise exception if setup failed or timeout expired.
    """
    def wrapper(*args, **kwargs):
        cl_inst = args[0]
        LOGGER.debug('Class instance: %s', cl_inst)
        LOGGER.debug('Method kwargs: %s', kwargs)
        node_list = kwargs.get('node_list', None) or cl_inst.nodes
        timeout = kwargs.get('timeout', None)
        setup_kwargs = {k: kwargs[k] for k in kwargs if k != 'node_list'}

        _queue = queue.Queue()

        @raise_event_on_failure
        def node_setup(node):
            status = True
            try:
                cl_inst.node_setup(node, **setup_kwargs)
            except Exception:  # pylint: disable=broad-except
                cl_inst.log.exception('Node setup failed: %s', str(node))
                status = False

            _queue.put((node, status))
            _queue.task_done()

        start_time = time.time()

        for node in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(node_list):
            time_elapsed = time.time() - start_time
            try:
                node, node_status = _queue.get(block=True, timeout=5)
                if not node_status:
                    raise NodeSetupFailed(f"{node}:{node_status}")
                results.append(node)
                cl_inst.log.info("(%d/%d) nodes ready, node %s. Time elapsed: %d s",
                                 len(results), len(node_list), str(node), int(time_elapsed))
            except queue.Empty:
                pass
            if timeout and time_elapsed / 60 > timeout:
                msg = 'TIMEOUT [%d min]: Waiting for node(-s) setup(%d/%d) expired!' % (
                    timeout, len(results), len(node_list))
                cl_inst.log.error(msg)
                raise NodeSetupTimeout(msg)

        time_elapsed = time.time() - start_time
        cl_inst.log.debug('Setup duration -> %s s', int(time_elapsed))

        method(*args, **kwargs)
    return wrapper


class ClusterNodesNotReady(Exception):
    pass


class BaseScyllaCluster:  # pylint: disable=too-many-public-methods

    def __init__(self, *args, **kwargs):
        self.termination_event = threading.Event()
        self.nemesis = []
        self.nemesis_threads = []
        self._seed_nodes_ips = []
        self._seed_nodes = []
        self._non_seed_nodes = []
        super(BaseScyllaCluster, self).__init__(*args, **kwargs)

    @staticmethod
    def get_node_ips_param(public_ip=True):
        if Setup.MIXED_CLUSTER:
            return 'oracle_db_nodes_public_ip' if public_ip else 'oracle_db_nodes_private_ip'
        return 'db_nodes_public_ip' if public_ip else 'db_nodes_private_ip'

    def get_scylla_args(self):
        # pylint: disable=no-member
        return self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else \
            self.params.get('append_scylla_args')

    def set_seeds(self, wait_for_timeout=300):
        seeds_selector = self.params.get('seeds_selector')
        seeds_num = self.params.get('seeds_num')
        cluster_backend = self.params.get('cluster_backend')

        seed_nodes_ips = None
        if seeds_selector == 'reflector' or Setup.REUSE_CLUSTER or cluster_backend == 'aws-siren':
            node = self.nodes[0]
            node.wait_ssh_up()
            # When cluster just started, seed IP in the scylla.yaml may be like '127.0.0.1'
            # In this case we want to ignore it and wait, when reflector will select real node and update scylla.yaml
            seed_nodes_ips = wait.wait_for(self.get_seed_selected_by_reflector,
                                           step=10, text='Waiting for seed is selected by reflector',
                                           timeout=wait_for_timeout, throw_exc=True)
        else:
            if seeds_selector == 'random':
                selected_nodes = random.sample(self.nodes, seeds_num)
            # seeds_selector == 'first'
            else:
                selected_nodes = self.nodes[:seeds_num]

            seed_nodes_ips = [node.ip_address for node in selected_nodes]

        for node in self.nodes:
            if node.ip_address in seed_nodes_ips:
                node.is_seed = True
        assert seed_nodes_ips, "We should have at least one selected seed by now"

    @property
    def seed_nodes_ips(self):
        if not self._seed_nodes_ips:
            self._seed_nodes_ips = [node.ip_address for node in self.nodes if node.is_seed]
            assert self._seed_nodes_ips, "We should have at least one selected seed by now"
        return self._seed_nodes_ips

    @property
    def seed_nodes(self):
        if not self._seed_nodes:
            self._seed_nodes = [node for node in self.nodes if node.is_seed]
            assert self._seed_nodes, "We should have at least one selected seed by now"
        return self._seed_nodes

    @property
    def non_seed_nodes(self):
        if not self._non_seed_nodes:
            self._non_seed_nodes = [node for node in self.nodes if not node.is_seed]
        return self._non_seed_nodes

    def validate_seeds_on_all_nodes(self):
        for node in self.nodes:
            yaml_seeds_ips = node.extract_seeds_from_scylla_yaml()
            for ip in yaml_seeds_ips:
                assert ip in self.seed_nodes_ips, \
                    'Wrong seed IP {act_ip} in the scylla.yaml on the {node_name} node. ' \
                    'Expected {exp_ips}'.format(node_name=node.name,
                                                exp_ips=self.seed_nodes_ips,
                                                act_ip=ip)

    def enable_client_encrypt(self):
        for node in self.nodes:
            self.log.debug("Enabling client encryption on node")
            node.enable_client_encrypt()

    def disable_client_encrypt(self):
        for node in self.nodes:
            self.log.debug("Disabling client encryption on node")
            node.disable_client_encrypt()

    def _update_db_binary(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB binary...')

        def update_scylla_bin(node, _queue):
            node.log.info('Updating DB binary')
            node.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)

            # scylla binary is moved to different directory after relocation
            # check if the installed binary is the relocated one or not
            relocated_binary = '/opt/scylladb/libexec/scylla.bin'
            if node.file_exists(relocated_binary):
                binary_path = relocated_binary
            else:
                binary_path = '/usr/bin/scylla'
            # replace the binary
            prereqs_script = dedent("""
                cp -f {binary_path} {binary_path}.origin
                cp -f /tmp/scylla {binary_path}
                chown root.root {binary_path}
                chmod +x {binary_path}
            """.format(binary_path=binary_path))
            node.remoter.run("sudo bash -ce '%s'" % prereqs_script)
            _queue.put(node)
            _queue.task_done()

        def stop_scylla(node, _queue):
            node.stop_scylla(verify_down=True, verify_up=True)
            _queue.put(node)
            _queue.task_done()

        def start_scylla(node, _queue):
            node.start_scylla(verify_down=True, verify_up=True)
            _queue.put(node)
            _queue.task_done()

        start_time = time.time()

        # First, stop *all* non seed nodes
        self.run_func_parallel(func=stop_scylla, node_list=self.non_seed_nodes)  # pylint: disable=no-member
        # First, stop *all* seed nodes
        self.run_func_parallel(func=stop_scylla, node_list=self.seed_nodes)  # pylint: disable=no-member
        # Then, update bin only on requested nodes
        self.run_func_parallel(func=update_scylla_bin, node_list=node_list)  # pylint: disable=no-member
        # Start all seed nodes
        self.run_func_parallel(func=start_scylla, node_list=self.seed_nodes)  # pylint: disable=no-member
        # Start all non seed nodes
        self.run_func_parallel(func=start_scylla, node_list=self.non_seed_nodes)  # pylint: disable=no-member

        time_elapsed = time.time() - start_time
        self.log.debug('Update DB binary duration -> %s s', int(time_elapsed))

    def _update_db_packages(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB packages...')

        def update_scylla_packages(node, _queue):
            node.log.info('Updating DB packages')
            node.remoter.run('mkdir /tmp/scylla')
            node.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)
            # replace the packages
            node.remoter.run('yum list installed | grep scylla')
            node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/*.rpm', ignore_status=False, verbose=True)
            node.remoter.run('yum list installed | grep scylla')
            _queue.put(node)
            _queue.task_done()

        def stop_scylla(node, _queue):
            node.stop_scylla(verify_down=True, verify_up=True)
            _queue.put(node)
            _queue.task_done()

        def start_scylla(node, _queue):
            node.start_scylla(verify_down=True, verify_up=True)
            _queue.put(node)
            _queue.task_done()

        start_time = time.time()

        if len(node_list) == 1:
            # Stop only new nodes
            self.run_func_parallel(func=stop_scylla, node_list=node_list)  # pylint: disable=no-member
            # Then, update packages only on requested node
            self.run_func_parallel(func=update_scylla_packages, node_list=node_list)  # pylint: disable=no-member
            # Start new nodes
            self.run_func_parallel(func=start_scylla, node_list=node_list)  # pylint: disable=no-member
        else:
            # First, stop *all* non seed nodes
            self.run_func_parallel(func=stop_scylla, node_list=self.non_seed_nodes)  # pylint: disable=no-member
            # First, stop *all* seed nodes
            self.run_func_parallel(func=stop_scylla, node_list=self.seed_nodes)  # pylint: disable=no-member
            # Then, update packages only on requested nodes
            self.run_func_parallel(func=update_scylla_packages, node_list=node_list)  # pylint: disable=no-member
            # Start all seed nodes
            self.run_func_parallel(func=start_scylla, node_list=self.seed_nodes)  # pylint: disable=no-member
            # Start all non seed nodes
            self.run_func_parallel(func=start_scylla, node_list=self.non_seed_nodes)  # pylint: disable=no-member

        time_elapsed = time.time() - start_time
        self.log.debug('Update DB packages duration -> %s s', int(time_elapsed))

    def update_db_binary(self, node_list=None):
        if node_list is None:
            node_list = self.nodes

        new_scylla_bin = self.params.get('update_db_binary')
        if new_scylla_bin:
            self._update_db_binary(new_scylla_bin, node_list)

    def update_db_packages(self, node_list=None):
        new_scylla_bin = self.params.get('update_db_packages')
        if new_scylla_bin:
            if node_list is None:
                node_list = self.nodes
            self._update_db_packages(new_scylla_bin, node_list)

    def get_node_info_list(self, verification_node):
        """
            !!! Deprecated !!!!
            use self.get_nodetool_status instead
        """
        assert verification_node in self.nodes
        cmd_result = verification_node.run_nodetool('status')
        node_info_list = []
        for line in cmd_result.stdout.splitlines():
            line = line.strip()
            if line.startswith('UN'):
                try:
                    status, ip, load, _, tokens, owns, host_id, rack = line.split()
                    node_info = {'status': status,
                                 'ip': ip,
                                 'load': load,
                                 'tokens': tokens,
                                 'owns': owns,
                                 'host_id': host_id,
                                 'rack': rack}
                    # Cassandra banners have nodetool status output as well.
                    # Need to guarantee unique set of results.
                    node_ips = [node_info['ip']
                                for node_info in node_info_list]
                    if node_info['ip'] not in node_ips:
                        node_info_list.append(node_info)
                except ValueError:
                    pass
        return node_info_list

    def get_nodetool_status(self, verification_node=None):  # pylint: disable=too-many-locals
        """
            Runs nodetool status and generates status structure.
            Status format:
            status = {
                "datacenter1": {
                    "ip1": {
                        'state': state,
                        'load': load,
                        'tokens': tokens,
                        'owns': owns,
                        'host_id': host_id,
                        'rack': rack
                    }
                }
            }
        :param verification_node: node to run the nodetool on
        :return: dict
        """
        if not verification_node:
            verification_node = random.choice(self.nodes)
        status = {}
        res = verification_node.run_nodetool('status')
        data_centers = res.stdout.strip().split("Datacenter: ")
        for dc in data_centers:
            if dc:
                lines = dc.splitlines()
                dc_name = lines[0]
                status[dc_name] = {}
                for line in lines[1:]:
                    if line.startswith('--'):  # ignore the title line in result
                        continue
                    try:
                        state, ip, load, load_unit, tokens, owns, host_id, rack = line.split()
                        node_info = {'state': state,
                                     'load': '%s%s' % (load, load_unit),
                                     'tokens': tokens,
                                     'owns': owns,
                                     'host_id': host_id,
                                     'rack': rack,
                                     }
                        status[dc_name][ip] = node_info
                    except ValueError:
                        pass
        return status

    @staticmethod
    def get_nodetool_info(node):
        """
            Runs nodetool info and generates status structure.
            Info format:

            :param node: node to run the nodetool on
            :return: dict
        """
        res = node.run_nodetool('info')
        info_res = yaml.load(res.stdout)
        return info_res

    def check_cluster_health(self):
        # Task 1443: ClusterHealthCheck is bottle neck in scale test and create a lot of noise in 5000 tables test.
        # Disable it
        if not self.params.get('cluster_health_check'):
            self.log.debug('Cluster health check disabled')
            return

        for node in self.nodes:
            node.check_node_health()

        ClusterHealthValidatorEvent(type='done', name='ClusterHealthCheck', status=Severity.NORMAL,
                                    message='Cluster health check finished')

    def check_nodes_up_and_normal(self, nodes=None, verification_node=None):
        """Checks via nodetool that node joined the cluster and reached 'UN' state"""
        if not nodes:
            nodes = self.nodes
        status = self.get_nodetool_status(verification_node=verification_node)
        up_statuses = []
        for node in nodes:
            for dc_status in status.values():
                ip_status = dc_status.get(node.ip_address)
                if ip_status and ip_status["state"] == "UN":
                    up_statuses.append(True)
                else:
                    up_statuses.append(False)
        if not all(up_statuses):
            raise ClusterNodesNotReady("Not all nodes joined the cluster")

    @retrying(n=30, sleep_time=3, allowed_exceptions=(ClusterNodesNotReady, UnexpectedExit),
              message="Waiting for nodes to join the cluster")
    def wait_for_nodes_up_and_normal(self, nodes, verification_node=None):
        self.check_nodes_up_and_normal(nodes=nodes, verification_node=verification_node)

    def get_scylla_version(self):
        if not self.nodes[0].scylla_version:
            scylla_version = self.nodes[0].get_scylla_version()

            if scylla_version:
                for node in self.nodes:
                    node.scylla_version = scylla_version

    def get_test_keyspaces(self):
        out = self.nodes[0].run_cqlsh('select keyspace_name from system_schema.keyspaces',
                                      split=True)
        return [ks.strip() for ks in out[3:-3] if 'system' not in ks]

    def cfstat_reached_threshold(self, key, threshold, keyspaces=None):
        """
        Find whether a certain cfstat key in all nodes reached a certain value.

        :param key: cfstat key, example, 'Space used (total)'.
        :param threshold: threshold value for cfstats key. Example, 2432043080.
        :param keyspace: keyspace name or table full name(example: 'keyspace1.standard1'). If keyspaces is None,
                         receive all
        :return: Whether all nodes reached that threshold or not.
        """
        if not keyspaces:
            keyspaces = self.get_test_keyspaces()

        self.log.debug("Waiting for threshold: %s" % (threshold))
        node = self.nodes[0]
        node_space = 0
        # Calculate space on the disk of all test keyspaces on the one node.
        # It's decided to check the threshold on one node only
        for keyspace_name in keyspaces:
            self.log.debug("Get cfstats on the node %s for %s keyspace" %
                           (node.name, keyspace_name))
            node_space += node.get_cfstats(keyspace_name)[key]
        self.log.debug("Current cfstats on the node %s for %s keyspaces: %s" %
                       (node.name, keyspaces, node_space))
        reached_threshold = True
        if node_space < threshold:
            reached_threshold = False
        if reached_threshold:
            self.log.debug("Done waiting on cfstats: %s" % node_space)
        return reached_threshold

    def wait_total_space_used_per_node(self, size=None, keyspace='keyspace1'):
        if size is None:
            size = int(self.params.get('space_node_threshold'))
        if size:
            if keyspace and not isinstance(keyspace, list):
                keyspace = [keyspace]
            key = 'Space used (total)'
            wait.wait_for(func=self.cfstat_reached_threshold, step=10, timeout=300,
                          text="Waiting until cfstat '%s' reaches value '%s'" % (key, size),
                          key=key, threshold=size, keyspaces=keyspace)

    def add_nemesis(self, nemesis, tester_obj):
        for nem in nemesis:
            for _ in range(nem['num_threads']):
                self.nemesis.append(nem['nemesis'](tester_obj=tester_obj,
                                                   termination_event=self.termination_event))

    def clean_nemesis(self):
        self.nemesis = []

    @log_run_info("Start nemesis threads on cluster")
    def start_nemesis(self, interval=30):
        for nemesis in self.nemesis:
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval, ))
            nemesis_thread.daemon = True
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)

    @log_run_info("Stop nemesis threads on cluster")
    def stop_nemesis(self, timeout=10):
        if self.termination_event.isSet():
            return
        self.log.info('Set termination_event')
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(timeout)
        self.nemesis_threads = []

    def node_config_setup(self, node, seed_address=None, endpoint_snitch=None, murmur3_partitioner_ignore_msb_bits=None, client_encrypt=None):  # pylint: disable=too-many-arguments,invalid-name
        node.config_setup(seed_address=seed_address,
                          cluster_name=self.name,  # pylint: disable=no-member
                          enable_exp=self.params.get('experimental'),
                          endpoint_snitch=endpoint_snitch,
                          authenticator=self.params.get('authenticator'),
                          server_encrypt=self.params.get('server_encrypt'),
                          client_encrypt=client_encrypt if client_encrypt is not None else self.params.get(
                              'client_encrypt'),
                          append_scylla_yaml=self.params.get('append_scylla_yaml'),
                          append_scylla_args=self.get_scylla_args(),
                          hinted_handoff=self.params.get('hinted_handoff'),
                          authorizer=self.params.get('authorizer'),
                          alternator_port=self.params.get('alternator_port'),
                          murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)

    def node_setup(self, node, verbose=False, timeout=3600):
        node.wait_ssh_up(verbose=verbose, timeout=timeout)

        if not Setup.REUSE_CLUSTER:
            self._scylla_pre_install(node)
            self._scylla_install(node)

            node.install_scylla_debuginfo()

            if Setup.MULTI_REGION:
                node.datacenter_setup(self.datacenter)  # pylint: disable=no-member
            self.node_config_setup(node, ','.join(self.seed_nodes_ips), self.get_endpoint_snitch())

            self._scylla_post_install(node)

            node.stop_scylla_server(verify_down=False)
            node.start_scylla_server(verify_up=False)

            self.log.info('io.conf right after reboot')
            node.remoter.run('sudo cat /etc/scylla.d/io.conf')

            if self.params.get('use_mgmt', None):
                pkgs_url = self.params.get('scylla_mgmt_pkg', None)
                pkg_path = None
                if pkgs_url:
                    pkg_path = download_dir_from_cloud(pkgs_url)
                    node.remoter.run('mkdir -p {}'.format(pkg_path))
                    node.remoter.send_files(src='{}*.rpm'.format(pkg_path), dst=pkg_path)
                node.install_manager_agent(package_path=pkg_path)
        else:
            self._reuse_cluster_setup(node)

        node.wait_db_up(verbose=verbose, timeout=timeout)
        node.check_nodes_status()

        self.clean_replacement_node_ip(node)

    @staticmethod
    def _scylla_pre_install(node):
        node.update_repo_cache()
        if node.init_system == 'systemd' and (node.is_ubuntu() or node.is_debian()):
            node.remoter.run('sudo systemctl disable apt-daily.timer')
            node.remoter.run('sudo systemctl disable apt-daily-upgrade.timer')
            node.remoter.run('sudo systemctl stop apt-daily.timer', ignore_status=True)
            node.remoter.run('sudo systemctl stop apt-daily-upgrade.timer', ignore_status=True)
        node.clean_scylla()

    def _scylla_install(self, node):
        node.install_scylla(scylla_repo=self.params.get('scylla_repo'))

    @staticmethod
    def _scylla_post_install(node):
        try:
            disks = node.detect_disks(nvme=True)
        except AssertionError:
            disks = node.detect_disks(nvme=False)
        node.scylla_setup(disks)

    def _reuse_cluster_setup(self, node):
        pass

    def get_endpoint_snitch(self, default_multi_region="GossipingPropertyFileSnitch"):
        endpoint_snitch = self.params.get('endpoint_snitch')
        if Setup.MULTI_REGION:
            if not endpoint_snitch:
                endpoint_snitch = default_multi_region
        return endpoint_snitch

    @staticmethod
    def clean_replacement_node_ip(node):
        if node.replacement_node_ip:
            # If this is a replacement node, we need to set back configuration in case
            # when scylla-server process will be restarted
            node.replacement_node_ip = None
            node.remoter.run(
                'sudo sed -i -e "s/^replace_address_first_boot:/# replace_address_first_boot:/g" /etc/scylla/scylla.yaml')

    @staticmethod
    def verify_logging_from_nodes(nodes_list):
        for node in nodes_list:
            if not os.path.exists(node.database_log):
                error_msg = "No db log from node [%s] " % node
                raise Exception(error_msg)
        return True

    @wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):  # pylint: disable=unused-argument
        """
        Scylla cluster setup.
        :param node_list: List of nodes to watch for init.
        :param verbose: Whether to print extra info while watching for init.
        :param timeout: timeout in minutes to wait for init to be finished
        :return:
        """
        node_list = node_list or self.nodes
        self.update_db_binary(node_list)
        self.update_db_packages(node_list)
        self.get_scylla_version()

        wait.wait_for(self.verify_logging_from_nodes, nodes_list=node_list,
                      text="wait for db logs", step=20, timeout=300, throw_exc=True)

        self.log.info("{} nodes configured and stated.".format(node_list))
        node_list[0].check_node_health()

    def restart_scylla(self, nodes=None):
        if nodes:
            nodes_to_restart = nodes
        else:
            nodes_to_restart = self.nodes
        self.log.info("Going to restart Scylla on %s" % [n.name for n in nodes_to_restart])
        for node in nodes_to_restart:
            node.stop_scylla(verify_down=True)
            node.start_scylla(verify_up=True)
            self.log.debug("'{0.name}' restarted.".format(node))

    def get_seed_selected_by_reflector(self, node=None):
        """
        Check if reflector updated the scylla.yaml with selected seed IP
        """
        if not node:
            node = self.nodes[0]

        seed_nodes_ips = node.extract_seeds_from_scylla_yaml()
        # When cluster just started, seed IP in the scylla.yaml may be like '127.0.0.1'
        # In this case we want to ignore it and wait, when reflector will select real node and update scylla.yaml
        return [n.ip_address for n in self.nodes if n.ip_address in seed_nodes_ips]


class BaseLoaderSet():

    def __init__(self, params):
        self._loader_cycle = None
        self.params = params
        self._gemini_version = None
        self._gemini_base_path = None

    @property
    def gemini_version(self):
        if not self._gemini_version:
            try:
                result = self.nodes[0].remoter.run('cd $HOME; ./gemini --version', ignore_status=True)
                if result.ok:
                    self._gemini_version = get_gemini_version(result.stdout)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error("Error get gemini version: %s", details)
        return self._gemini_version

    @property
    def gemini_base_path(self):
        if not self._gemini_base_path:
            result = self.nodes[0].remoter.run("echo $HOME", ignore_status=True)
            self._gemini_base_path = result.stdout.strip()
        return self._gemini_base_path

    def install_gemini(self, node):
        gemini_version = self.params.get('gemini_version', default='0.9.2')
        if gemini_version.lower() == 'latest':
            gemini_version = get_latest_gemini_version()

        gemini_url = 'http://downloads.scylladb.com/gemini/{0}/gemini_{0}_Linux_x86_64.tar.gz'.format(gemini_version)
        # TODO: currently schema is not used by gemini tool need to store the schema
        #       in data_dir for each test
        gemini_schema_url = self.params.get('gemini_schema_url')
        if not gemini_url or not gemini_schema_url:
            self.log.warning('Gemini URLs should be defined to run the gemini tool')
        else:
            gemini_tar = os.path.basename(gemini_url)  # pylint: disable=unused-variable
            install_gemini_script = dedent(f"""
                cd {self.gemini_base_path}
                rm -rf gemini*
                curl -LO {gemini_url}
                tar -xvf {gemini_tar}
                chmod a+x gemini
                curl -LO  {gemini_schema_url}
            """)
            node.remoter.run("bash -cxe '%s'" % install_gemini_script)
            self.log.debug('Gemini version {}'.format(self.gemini_version))

    def node_setup(self, node, verbose=False, db_node_address=None, **kwargs):  # pylint: disable=unused-argument
        self.log.info('Setup in BaseLoaderSet')
        node.wait_ssh_up(verbose=verbose)
        # add swap file
        if not Setup.REUSE_CLUSTER:
            swap_size = self.params.get("loader_swap_size")
            if not swap_size:
                self.log.info("Swap file for the loader is not configured")
            else:
                node.create_swap_file(swap_size)
        # update repo cache and system after system is up
        node.update_repo_cache()

        if Setup.REUSE_CLUSTER:
            self.kill_stress_thread()
            return

        collectd_setup = ScyllaCollectdSetup()
        collectd_setup.install(node)
        self.install_gemini(node=node)
        if self.params.get('client_encrypt'):
            node.config_client_encrypt()

        result = node.remoter.run('test -e ~/PREPARED-LOADER', ignore_status=True)
        if result.exit_status == 0:
            self.log.debug('Skip loader setup for using a prepared AMI')
            return

        if node.is_ubuntu14():
            install_java_script = dedent("""
                apt-get install software-properties-common -y
                add-apt-repository -y ppa:openjdk-r/ppa
                add-apt-repository -y ppa:scylladb/ppa
                apt-get update
                apt-get install -y openjdk-8-jre-headless
                update-java-alternatives -s java-1.8.0-openjdk-amd64
            """)
            node.remoter.run('sudo bash -cxe "%s"' % install_java_script)

        elif node.is_debian8():
            install_java_script = dedent(r"""
                sed -i -e 's/jessie-updates/stable-updates/g' /etc/apt/sources.list
                echo 'deb http://archive.debian.org/debian jessie-backports main' |sudo tee /etc/apt/sources.list.d/backports.list
                sed -e 's/:\/\/.*\/debian jessie-backports /:\/\/archive.debian.org\/debian jessie-backports /g' /etc/apt/sources.list.d/*.list
                echo 'Acquire::Check-Valid-Until false;' |sudo tee /etc/apt/apt.conf.d/99jessie-backports
                apt-get update
                apt-get install gnupg-curl -y
                apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
                apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19
                echo 'deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /' |sudo tee /etc/apt/sources.list.d/scylla-3rdparty.list
                apt-get update
                apt-get install -y openjdk-8-jre-headless -t jessie-backports
                update-java-alternatives -s java-1.8.0-openjdk-amd64
            """)
            node.remoter.run('sudo bash -cxe "%s"' % install_java_script)

        scylla_repo_loader = self.params.get('scylla_repo_loader')
        if not scylla_repo_loader:
            scylla_repo_loader = self.params.get('scylla_repo')
        node.download_scylla_repo(scylla_repo_loader)
        if node.is_rhel_like():
            node.remoter.run('sudo yum install -y {}-tools'.format(node.scylla_pkg()))
            node.remoter.run('sudo yum install -y screen')
        else:
            node.remoter.run('sudo apt-get update')
            node.remoter.run('sudo apt-get install -y -o Dpkg::Options::="--force-confdef"'
                             ' -o Dpkg::Options::="--force-confold" --force-yes'
                             ' --allow-unauthenticated {}-tools'.format(node.scylla_pkg()))
            node.remoter.run('sudo apt-get install -y screen')

        if db_node_address is not None:
            node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" % db_node_address)

        node.wait_cs_installed(verbose=verbose)

        # scylla-bench
        node.remoter.run('sudo yum install git -y')
        node.remoter.run('curl -LO https://storage.googleapis.com/golang/go1.13.linux-amd64.tar.gz')
        node.remoter.run('sudo tar -C /usr/local -xvzf go1.13.linux-amd64.tar.gz')
        node.remoter.run("echo 'export GOPATH=$HOME/go' >> $HOME/.bashrc")
        node.remoter.run("echo 'export PATH=$PATH:/usr/local/go/bin' >> $HOME/.bashrc")
        node.remoter.run("source $HOME/.bashrc")
        node.remoter.run("go get github.com/scylladb/scylla-bench")

        # install ycsb
        ycsb_install = dedent("""
            cd ~/
            curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.15.0/ycsb-0.15.0.tar.gz
            tar xfvz ycsb-0.15.0.tar.gz
        """)
        node.remoter.run('bash -cxe "%s"' % ycsb_install)

    @wait_for_init_wrap
    def wait_for_init(self, verbose=False, db_node_address=None):
        pass

    @staticmethod
    def get_node_ips_param(public_ip=True):
        return 'loaders_public_ip' if public_ip else 'loaders_private_ip'

    def get_loader(self):
        if not self._loader_cycle:
            self._loader_cycle = itertools.cycle(self.nodes)
        return next(self._loader_cycle)

    def kill_stress_thread(self):
        for loader in self.nodes:
            try:
                loader.remoter.run(cmd='pgrep -f cassandra-stress | xargs -I{}  kill -TERM -{}', ignore_status=True)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.warning("failed to kill stress-command on [%s]: [%s]",
                                 str(loader), str(ex))

    def kill_ycsb_thread(self):
        for loader in self.nodes:
            try:
                loader.remoter.run(cmd='pgrep -f ycsb | xargs -I{}  kill -TERM -{}', ignore_status=True)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.warning("failed to kill ycsb stress command on [%s]: [%s]",
                                 str(loader), str(ex))

    @staticmethod
    def _parse_cs_summary(lines):
        """
        Parsing c-stress results, only parse the summary results.
        Collect results of all nodes and return a dictionaries' list,
        the new structure data will be easy to parse, compare, display or save.
        """
        results = {}
        enable_parse = False

        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Parse loader & cpu info
            if line.startswith('TAG:'):
                ret = re.findall(r"TAG: loader_idx:(\d+)-cpu_idx:(\d+)-keyspace_idx:(\d+)", line)
                results['loader_idx'] = ret[0][0]
                results['cpu_idx'] = ret[0][1]
                results['keyspace_idx'] = ret[0][2]
            if line.startswith('Username:'):
                results['username'] = line.split('Username:')[1].strip()
            if line.startswith('Results:'):
                enable_parse = True
                continue
            if line == '':
                continue
            if line == 'END':
                break
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip().lower()
            value = line[split_idx + 1:].split()[0].replace(",", "")
            results[key] = value
            match = re.findall(r'.*READ:(\d+), WRITE:(\d+)]', line)
            if match:  # parse results for mixed workload
                results['%s read' % key] = match[0][0]
                results['%s write' % key] = match[0][1]

        if not enable_parse:
            LOGGER.warning('Cannot find summary in c-stress results: %s', lines[-10:])
            return {}
        return results

    @staticmethod
    def _parse_bench_summary(lines):  # pylint: disable=too-many-branches
        """
        Parsing bench results, only parse the summary results.
        Collect results of all nodes and return a dictionaries' list,
        the new structure data will be easy to parse, compare, display or save.
        """
        results = {'keyspace_idx': None, 'stdev gc time(ms)': None, 'Total errors': None,
                   'total gc count': None, 'loader_idx': None, 'total gc time (s)': None,
                   'total gc mb': 0, 'cpu_idx': None, 'avg gc time(ms)': None, 'latency mean': None}
        enable_parse = False

        for line in lines:
            line.strip()
            # Parse load params
            # pylint: disable=too-many-boolean-expressions
            if line.startswith('Mode:') or line.startswith('Workload:') or line.startswith('Timeout:') or \
                    line.startswith('Consistency level:') or line.startswith('Partition count') or \
                    line.startswith('Clustering rows:') or line.startswith('Clustering row size:') or \
                    line.startswith('Rows per request:') or line.startswith('Page size:') or \
                    line.startswith('Concurrency:') or line.startswith('Connections:') or \
                    line.startswith('Maximum rate:') or line.startswith('Client compression:'):
                split_idx = line.index(':')
                key = line[:split_idx].strip()
                value = line[split_idx + 1:].split()[0]
                results[key] = value

            if line.startswith('Results'):
                enable_parse = True
                continue
            if line.startswith('Latency:') or ':' not in line:
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            # the value may be in milliseconds(ms) or microseconds(string containing non-ascii character)
            try:
                value = float(str(value).rstrip('ms'))
            except UnicodeDecodeError:
                value = float(str(value, errors='ignore').rstrip('s')) / 1000  # convert to milliseconds
            except ValueError:
                pass  # save as is

            # we try to use the same stats as we have in cassandra
            if key == 'max':
                key = 'latency max'
            elif key == '99.9th':
                key = 'latency 99.9th percentile'
            elif key == '99th':
                key = 'latency 99th percentile'
            elif key == '95th':
                key = 'latency 95th percentile'
            elif key == 'median':
                key = 'latency median'
            elif key == 'mean':
                key = 'latency mean'
            elif key == 'Operations/s':
                key = 'op rate'
            elif key == 'Rows/s':
                key = 'row rate'
                results['partition rate'] = value
            elif key == 'Total ops':  # ==Total rows?
                key = 'Total partitions'
            elif key == 'Time (avg)':
                key = 'Total operation time'
            results[key] = value
        return results

    @staticmethod
    def _parse_cs_results(lines):
        results = dict()
        results['time'] = []
        results['ops'] = []
        results['totalops'] = []
        results['latmax'] = []
        results['lat999'] = []
        results['lat99'] = []
        results['lat95'] = []
        for line in lines:
            line.strip()
            if line.startswith('total,'):
                items = line.split(',')
                totalops = items[1]
                ops = items[2]
                lat95 = items[7]
                lat99 = items[8]
                lat999 = items[9]
                latmax = items[10]
                time_point = items[11]
                results['time'].append(time_point)
                results['totalops'].append(totalops)
                results['ops'].append(ops)
                results['lat95'].append(lat95)
                results['lat99'].append(lat99)
                results['lat999'].append(lat999)
                results['latmax'].append(latmax)
        return results

    def get_stress_results_bench(self, _queue):
        results = []
        ret = []
        self.log.debug('Wait for %s bench stress threads results',
                       _queue[TASK_QUEUE].qsize())
        _queue[TASK_QUEUE].join()
        while not _queue[RES_QUEUE].empty():
            results.append(_queue[RES_QUEUE].get())

        for _, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            ret.append(self._parse_bench_summary(lines))

        return ret

    def run_stress_thread_bench(self, stress_cmd, timeout, node_list=None, round_robin=False, use_single_loader=False):  # pylint: disable=too-many-arguments
        _queue = {TASK_QUEUE: queue.Queue(), RES_QUEUE: queue.Queue()}

        def node_run_stress_bench(node, loader_idx, stress_cmd, node_list):
            _queue[TASK_QUEUE].put(node)

            ScyllaBenchEvent(type='start', node=str(node), stress_cmd=stress_cmd)

            makedirs(node.logdir)

            log_file_name = os.path.join(node.logdir,
                                         'scylla-bench-l%s-%s.log' %
                                         (loader_idx, uuid.uuid4()))
            # Select first seed node to send the scylla-bench cmds
            ips = node_list[0].private_ip_address
            bench_log = tempfile.NamedTemporaryFile(prefix='scylla-bench-', suffix='.log').name

            result = node.remoter.run(cmd="/$HOME/go/bin/{name} -nodes {ips} |tee {log}".format(name=stress_cmd.strip(),
                                                                                                ips=ips,
                                                                                                log=bench_log),
                                      timeout=timeout,
                                      ignore_status=True,
                                      log_file=log_file_name)

            ScyllaBenchEvent(type='finish', node=str(node), stress_cmd=stress_cmd, log_file_name=log_file_name)

            _queue[RES_QUEUE].put((node, result))
            _queue[TASK_QUEUE].task_done()

        if round_robin:
            loaders = [self.get_loader()]
        else:
            loaders = self.nodes if not use_single_loader else [self.nodes[0]]
        LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))

        for loader_idx, loader in enumerate(loaders):
            setup_thread = threading.Thread(target=node_run_stress_bench,
                                            args=(loader, loader_idx, stress_cmd, node_list))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        return _queue

    def kill_stress_thread_bench(self):
        for loader in self.nodes:
            sb_active = loader.remoter.run(cmd='pgrep -f scylla-bench', ignore_status=True)
            if sb_active.exit_status == 0:
                kill_result = loader.remoter.run('pkill -f -TERM scylla-bench', ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate scylla-bench on node %s:\n%s',
                                   loader, kill_result)

    def kill_gemini_thread(self):
        for loader in self.nodes:
            sb_active = loader.remoter.run(cmd='pgrep -f gemini', ignore_status=True)
            if sb_active.exit_status == 0:
                kill_result = loader.remoter.run('pkill -f -TERM gemini', ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate gemini on node %s:\n%s', loader, kill_result)


class BaseMonitorSet():  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    # This is a Mixin for monitoring cluster and should not be inherited
    def __init__(self, targets, params):
        self.targets = targets
        self.params = params
        self.local_metrics_addr = start_metrics_server()  # start prometheus metrics server locally and return local ip
        self.sct_ip_port = self.set_local_sct_ip()
        self.grafana_port = 3000
        self.remote_webdriver_port = 4444
        self.monitor_branch = self.params.get('monitor_branch')
        self._monitor_install_path_base = None
        self.phantomjs_installed = False
        self.grafana_start_time = 0
        self._sct_dashboard_json_file = None

    @staticmethod
    def get_monitor_install_path_base(node):
        return node.remoter.run("echo $HOME").stdout.strip()

    @property
    def monitor_install_path_base(self):
        if not self._monitor_install_path_base:
            self._monitor_install_path_base = self.get_monitor_install_path_base(self.nodes[0])
        return self._monitor_install_path_base

    @property
    def monitor_install_path(self):
        return os.path.join(self.monitor_install_path_base, "scylla-monitoring-{}".format(self.monitor_branch))

    @property
    def monitoring_conf_dir(self):
        return os.path.join(self.monitor_install_path, "config")

    @property
    def monitoring_data_dir(self):
        return os.path.join(self.monitor_install_path_base, "scylla-monitoring-data")

    @staticmethod
    def get_node_ips_param(public_ip=True):
        param_name = 'monitor_nodes_public_ip' if public_ip else 'monitor_nodes_private_ip'
        return param_name

    @property
    def scylla_version(self):
        return self.targets["db_cluster"].nodes[0].scylla_version

    @property
    def monitoring_version(self):
        self.log.debug("Using %s ScyllaDB version to derive monitoring version" %
                       self.scylla_version)
        version = re.match(r"(\d+\.\d+)", self.scylla_version)
        if not version:
            return 'master'
        else:
            return version.group(1)

    @property
    def is_enterprise(self):
        return self.targets["db_cluster"].nodes[0].is_enterprise

    @property
    def sct_dashboard_json_file(self):
        if not self._sct_dashboard_json_file:
            sct_dashboard_json_filename = f"scylla-dash-per-server-nemesis.{self.monitoring_version}.json"
            sct_dashboard_json_path = get_data_dir_path(sct_dashboard_json_filename)
            if not os.path.exists(sct_dashboard_json_path):
                sct_dashboard_json_filename = "scylla-dash-per-server-nemesis.master.json"
                sct_dashboard_json_path = get_data_dir_path(sct_dashboard_json_filename)
            self._sct_dashboard_json_file = sct_dashboard_json_path
        return self._sct_dashboard_json_file

    def node_setup(self, node, **kwargs):  # pylint: disable=unused-argument
        self.log.info('Setup in BaseMonitorSet')
        node.wait_ssh_up()
        # add swap file
        if not Setup.REUSE_CLUSTER:
            monitor_swap_size = self.params.get("monitor_swap_size")
            if not monitor_swap_size:
                self.log.info("Swap file for the monitor is not configured")
            else:
                node.create_swap_file(monitor_swap_size)
        # update repo cache and system after system is up
        node.update_repo_cache()
        self.mgmt_auth_token = Setup.test_id()  # pylint: disable=attribute-defined-outside-init

        if Setup.REUSE_CLUSTER:
            self.configure_scylla_monitoring(node)
            self.restart_scylla_monitoring(sct_metrics=True)
            set_grafana_url("http://{}:{}".format(normalize_ipv6_url(node.external_address), self.grafana_port))
            self.stop_selenium_remote_webdriver(node)
            self.start_selenium_remote_webdriver(node)
            return

        self.install_scylla_monitoring(node)
        self.configure_scylla_monitoring(node)
        try:
            self.start_scylla_monitoring(node)
        except (Failure, UnexpectedExit):
            self.restart_scylla_monitoring()
        # The time will be used in url of Grafana monitor,
        # the data from this point to the end of test will
        # be captured.
        self.grafana_start_time = time.time()
        set_grafana_url("http://{}:{}".format(normalize_ipv6_url(node.external_address), self.grafana_port))
        # since monitoring node is started last (after db nodes and loader) we can't actually set the timeout
        # for starting the alert manager thread (since it depends on DB cluster size and num of loaders)
        node.start_alert_manager_thread()  # remove when start task threads will be started after node setup
        if node.is_rhel_like():
            node.remoter.run('sudo yum install screen -y')
        else:
            node.remoter.run('sudo apt-get install screen -y')
        if self.params.get("use_mgmt", default=None):
            self.install_scylla_manager(node, auth_token=self.mgmt_auth_token)

        self.start_selenium_remote_webdriver(node)

    def install_scylla_manager(self, node, auth_token):
        if self.params.get('use_mgmt', default=None):
            node.install_scylla(scylla_repo=self.params.get('scylla_repo_m'))
            package_path = self.params.get('scylla_mgmt_pkg', None)
            if package_path:
                node.remoter.run('mkdir -p {}'.format(package_path))
                node.remoter.send_files(src='{}*.rpm'.format(package_path), dst=package_path)
            node.install_mgmt(scylla_mgmt_repo=self.params.get('scylla_mgmt_repo'), auth_token=auth_token,
                              segments_per_repair=self.params.get('mgmt_segments_per_repair'),
                              package_url=package_path)
            wait.wait_for(func=self.is_manager_up, step=20, text='Waiting until the manager client is up',
                          timeout=300, throw_exc=True)

    def is_manager_up(self):
        manager_tool = get_scylla_manager_tool(manager_node=self.nodes[0])  # pylint: disable=no-member
        try:
            LOGGER.debug(manager_tool.version)
            return True
        except ScyllaManagerError:
            return False

    def configure_ngrok(self):
        port = self.local_metrics_addr.split(':')[1]

        requests.delete('http://localhost:4040/api/tunnels/sct')

        tunnel = {
            "addr": port,
            "proto": "http",
            "name": "sct",
            "bind_tls": False
        }
        res = requests.post('http://localhost:4040/api/tunnels', json=tunnel)
        assert res.ok, "failed to add a ngrok tunnel [{}, {}]".format(res, res.text)
        ngrok_address = res.json()['public_url'].replace('http://', '')

        return "{}:80".format(ngrok_address)

    def set_local_sct_ip(self):

        ngrok_name = self.params.get('sct_ngrok_name', default=None)
        if ngrok_name:
            return self.configure_ngrok()

        sct_public_ip = self.params.get('sct_public_ip')
        if sct_public_ip:
            return sct_public_ip + ':' + self.local_metrics_addr.split(':')[1]
        else:
            return self.local_metrics_addr

    @wait_for_init_wrap
    def wait_for_init(self, *args, **kwargs):
        pass

    @staticmethod
    def install_scylla_monitoring_prereqs(node):  # pylint: disable=invalid-name
        if node.is_rhel_like():
            node.remoter.run("sudo yum install -y epel-release")
            node.update_repo_cache()
            prereqs_script = dedent("""
                yum install -y unzip wget
                yum install -y python36
                yum install -y python36-pip
                python3 -m pip install --upgrade pip
                python3 -m pip install pyyaml
                curl -fsSL get.docker.com -o get-docker.sh
                sh get-docker.sh
                systemctl start docker
            """)
        elif node.is_ubuntu():
            prereqs_script = dedent("""
                curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
                sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
                sudo apt-get update
                sudo apt-get install -y docker docker.io
                apt-get install -y software-properties-common
                add-apt-repository -y ppa:deadsnakes/ppa
                apt-get update
                apt-get install -y python3.6 python3.6-dev
                apt-get install -y python-setuptools unzip wget
                update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
                apt install -y python3-pip
                python3 -m pip install --upgrade pip
                python3 -m pip install pyyaml
                pip3 install -I -U psutil
                systemctl start docker
            """)
        elif node.is_debian8():
            node.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F76221572C52609D', retry=3)
            node.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            node.remoter.run(cmd="sudo apt-get dist-upgrade -y")
            node.remoter.run(
                cmd="sudo echo 'deb https://apt.dockerproject.org/repo debian-jessie main' | sudo tee /etc/apt/sources.list.d/docker.list")
            node.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            node.remoter.run(cmd="sudo apt-get install docker-engine -y --force-yes")
            node.remoter.run(cmd="sudo apt-get install -y curl")
            node.remoter.run(cmd="sudo apt-get install software-properties-common -y")
            node.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            node.remoter.run(cmd="apt-get install python36")
            node.remoter.run(cmd="sudo apt-get install -y python-setuptools wget")
            node.remoter.run(cmd="sudo apt-get install -y unzip")
            prereqs_script = dedent("""
                python3 -m pip install --upgrade pip
                python3 -m pip install pyyaml
                systemctl start docker
            """)
        else:
            raise ValueError('Unsupported Distribution type: {}'.format(str(node.distro)))

        node.remoter.run(cmd="sudo bash -ce '%s'" % prereqs_script)
        node.remoter.run("sudo usermod -aG docker $USER")
        node.remoter.reconnect()

    def download_scylla_monitoring(self, node):
        install_script = dedent("""
            mkdir -p {0.monitor_install_path_base}
            cd {0.monitor_install_path_base}
            wget https://github.com/scylladb/scylla-monitoring/archive/{0.monitor_branch}.zip
            unzip {0.monitor_branch}.zip
        """.format(self))
        node.remoter.run("bash -ce '%s'" % install_script)

    def configure_scylla_monitoring(self, node, sct_metrics=True, alert_manager=True):  # pylint: disable=too-many-locals
        cloud_prom_bearer_token = self.params.get('cloud_prom_bearer_token')

        if sct_metrics:
            temp_dir = tempfile.mkdtemp()
            template_fn = "prometheus.yml.template"
            prometheus_yaml_template = os.path.join(self.monitor_install_path, "prometheus", template_fn)
            local_template_tmp = os.path.join(temp_dir, template_fn + ".orig")
            local_template = os.path.join(temp_dir, template_fn)
            node.remoter.receive_files(src=prometheus_yaml_template,
                                       dst=local_template_tmp)
            with open(local_template_tmp) as output_file:
                templ_yaml = yaml.load(output_file, Loader=yaml.SafeLoader)  # to override avocado
                self.log.debug("Configs %s" % templ_yaml)
            loader_targets_list = ["[%s]:9103" % n.ip_address for n in self.targets["loaders"].nodes]

            # remove those jobs if exists, for support of 'reuse_cluster: true'
            def remove_sct_metrics(metric):
                return metric['job_name'] not in ['stress_metrics', 'sct_metrics']
            templ_yaml["scrape_configs"] = list(filter(remove_sct_metrics, templ_yaml["scrape_configs"]))

            scrape_configs = templ_yaml["scrape_configs"]
            scrape_configs.append(dict(job_name="stress_metrics", honor_labels=True,
                                       static_configs=[dict(targets=loader_targets_list)]))

            if cloud_prom_bearer_token:
                cloud_prom_path = self.params.get('cloud_prom_path')
                cloud_prom_host = self.params.get('cloud_prom_host')
                scrape_configs.append(dict(job_name="scylla_cloud_cluster", honor_labels=True, scrape_interval='15s',
                                           metrics_path=cloud_prom_path, scheme='https',
                                           params={'match[]': ['{job=~".+"}']},
                                           bearer_token=cloud_prom_bearer_token,
                                           static_configs=[dict(targets=[cloud_prom_host])]))

            if self.params.get('gemini_cmd'):
                gemini_loader_targets_list = ["%s:2112" % n.ip_address for n in self.targets["loaders"].nodes]
                scrape_configs.append(dict(job_name="gemini_metrics", honor_labels=True,
                                           static_configs=[dict(targets=gemini_loader_targets_list)]))

            if self.sct_ip_port:
                scrape_configs.append(dict(job_name="sct_metrics", honor_labels=True,
                                           static_configs=[dict(targets=[self.sct_ip_port])]))
            with open(local_template, "w") as output_file:
                yaml.safe_dump(templ_yaml, output_file, default_flow_style=False)  # to remove tag !!python/unicode
            node.remoter.send_files(src=local_template, dst=prometheus_yaml_template, delete_dst=True)

            LOCALRUNNER.run(f"rm -rf {temp_dir}", ignore_status=True)

        self.reconfigure_scylla_monitoring()
        if alert_manager:
            self.configure_alert_manager(node)

    def configure_alert_manager(self, node):
        alertmanager_conf_file = os.path.join(self.monitor_install_path, "prometheus", "prometheus.rules")
        conf = dedent("""
        # Alert for any instance that it's root disk free disk space bellow 25%.
        ALERT RootDiskFull
          IF node_filesystem_avail{mountpoint="/"}/node_filesystem_size{mountpoint="/"}*100 < 25
          FOR 30s
          LABELS { severity = "1" }
          ANNOTATIONS {
            summary = "Instance {{ $labels.instance }} root disk low space",
            description = "{{ $labels.instance }} root disk has less than 25% free disk space.",
          }
        # Alert for 99% cassandra stress write spikes
        ALERT CassandraStressWriteTooSlow
          IF collectd_cassandra_stress_write_gauge{type="lat_perc_99"} > 1000
          FOR 1s
          LABELS { severity = "1" }
          ANNOTATIONS {
            summary = "Cassandra Stress write latency more than 1000ms",
            description = "Cassandra Stress write latency is more than 1000ms during 1 sec period of time",
          }
        """)
        with tempfile.NamedTemporaryFile("w") as alert_cont_tmp_file:
            alert_cont_tmp_file.write(conf)
            node.remoter.send_files(src=alert_cont_tmp_file.name, dst=alert_cont_tmp_file.name)
            node.remoter.run("bash -ce 'cat %s >> %s'" % (alert_cont_tmp_file.name, alertmanager_conf_file))

    @retrying(n=5, sleep_time=10, allowed_exceptions=(Failure, UnexpectedExit),
              message="Waiting for restarting scylla monitoring")
    def restart_scylla_monitoring(self, sct_metrics=False):
        for node in self.nodes:
            self.stop_scylla_monitoring(node)
            # We use sct_metrics=False, alert_manager=False since they should be configured once
            self.configure_scylla_monitoring(node, sct_metrics=sct_metrics, alert_manager=False)
            self.start_scylla_monitoring(node)

    @retrying(n=5, sleep_time=10, allowed_exceptions=(Failure, UnexpectedExit),
              message="Waiting for reconfiguring scylla monitoring")
    def reconfigure_scylla_monitoring(self):
        for node in self.nodes:
            db_targets_list = ["[%s]:9180" % n.ip_address for n in self.targets["db_cluster"].nodes]
            self._monitoring_targets = " ".join(db_targets_list)  # pylint: disable=attribute-defined-outside-init
            configure_script = dedent("""
                        cd {0.monitor_install_path}
                        mkdir -p {0.monitoring_conf_dir}
                        python3 genconfig.py -s -n -d {0.monitoring_conf_dir} {0._monitoring_targets}
                    """.format(self))
            node.remoter.run("sudo bash -ce '%s'" % configure_script, verbose=True)

            if self.params.get('cloud_prom_bearer_token', None):
                cloud_prom_script = dedent("""
                                        echo "targets: [] " > {0.monitoring_conf_dir}/scylla_servers.yml
                                        echo "targets: [] " > {0.monitoring_conf_dir}/node_exporter_servers.yml
                                    """.format(self))

                node.remoter.run("sudo bash -ce '%s'" % cloud_prom_script, verbose=True)

    def start_scylla_monitoring(self, node):
        node.remoter.run("cp {0.monitor_install_path}/prometheus/scylla_manager_servers.example.yml"
                         " {0.monitor_install_path}/prometheus/scylla_manager_servers.yml".format(self))
        run_script = dedent("""
            cd {0.monitor_install_path}
            mkdir -p {0.monitoring_data_dir}
            ./start-all.sh \
            -s {0.monitoring_conf_dir}/scylla_servers.yml \
            -n {0.monitoring_conf_dir}/node_exporter_servers.yml \
            -d {0.monitoring_data_dir} -l -v master,{0.monitoring_version} -b "-web.enable-admin-api"
        """.format(self))
        node.remoter.run("bash -ce '%s'" % run_script, verbose=True)
        self.add_sct_dashboards_to_grafana(node)
        self.save_sct_dashboards_config(node)
        self.save_monitoring_version(node)

    @retrying(n=3, sleep_time=10, allowed_exceptions=(Failure, UnexpectedExit),
              message="Waiting for restarting selenium remote webdriver")
    def start_selenium_remote_webdriver(self, node):
        self.log.debug("Start docker container with selenium chrome driver")
        RemoteWebDriverContainer(node).run()

    def stop_selenium_remote_webdriver(self, node):
        self.log.debug("Delete docker container with selenium chrome driver")
        RemoteWebDriverContainer(node).kill()

    def save_monitoring_version(self, node):
        node.remoter.run(
            'echo "{0.monitor_branch}:{0.monitoring_version}" > \
            {0.monitor_install_path}/monitor_version'.format(self), ignore_status=True)

    def add_sct_dashboards_to_grafana(self, node):

        def _register_grafana_json(json_filename):
            # added "[]" / "\\" for IPv6 support
            url = "'http://\\[{0.external_address}\\]:{1.grafana_port}/api/dashboards/db'".format(node, self)
            result = LOCALRUNNER.run('curl -XPOST -i %s --data-binary @%s -H "Content-Type: application/json"' %
                                     (url, json_filename))
            return result.exited == 0

        wait.wait_for(_register_grafana_json, step=10,
                      text="Waiting to register '%s'..." % self.sct_dashboard_json_file,
                      json_filename=self.sct_dashboard_json_file)

    def save_sct_dashboards_config(self, node):
        sct_monitoring_addons_dir = os.path.join(self.monitor_install_path, 'sct_monitoring_addons')

        node.remoter.run('mkdir -p {}'.format(sct_monitoring_addons_dir), ignore_status=True)
        node.remoter.send_files(src=self.sct_dashboard_json_file, dst=sct_monitoring_addons_dir)

    @log_run_info
    def install_scylla_monitoring(self, node):
        self.install_scylla_monitoring_prereqs(node)
        self.download_scylla_monitoring(node)

    def get_grafana_annotations(self, node):
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        try:
            res = requests.get(url=annotations_url.format(node_ip=normalize_ipv6_url(node.external_address),
                                                          grafana_port=self.grafana_port))
            if res.ok:
                return res.content
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("unable to get grafana annotations [%s]", str(ex))
        return ""

    def set_grafana_annotations(self, node, annotations_data):
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        res = requests.post(url=annotations_url.format(node_ip=normalize_ipv6_url(node.external_address),
                                                       grafana_port=self.grafana_port),
                            data=annotations_data, headers={'Content-Type': 'application/json'})
        self.log.info("posting annotations result: %s", res)

    def stop_scylla_monitoring(self, node):
        kill_script = dedent("""
            cd {0.monitor_install_path}
            ./kill-all.sh
        """.format(self))
        node.remoter.run("bash -ce '%s'" % kill_script)

    def get_grafana_screenshot_and_snapshot(self, test_start_time=None):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        if not test_start_time:
            self.log.error("No start time for test")
            return {}
        date_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        for node in self.nodes:
            screenshot_links = []
            screenshot_collector = GrafanaScreenShot(name="grafana-screenshot",
                                                     test_start_time=test_start_time)
            screenshot_files = screenshot_collector.collect(node, self.logdir)
            for screenshot in screenshot_files:
                s3_path = "{test_id}/{date}".format(test_id=Setup.test_id(), date=date_time)
                screenshot_links.append(S3Storage().upload_file(screenshot, s3_path))

            snapshots_collector = GrafanaSnapshot(name="grafana-snapshot",
                                                  test_start_time=test_start_time)
            snapshots = snapshots_collector.collect(node, self.logdir)
        return {'screenshots': screenshot_links, 'snapshots': snapshots['links']}

    def upload_annotations_to_s3(self):
        annotations_url = ''
        if not self.nodes:
            return annotations_url
        try:
            annotations = self.get_grafana_annotations(self.nodes[0])
            if annotations:
                annotations_url = S3Storage().generate_url('annotations.json', Setup.test_id())
                self.log.info("Uploading 'annotations.json' to {s3_url}".format(
                    s3_url=annotations_url))
                response = requests.put(annotations_url, data=annotations, headers={
                                        'Content-type': 'application/json; charset=utf-8'})
                response.raise_for_status()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("failed to upload annotations to S3")

        return annotations_url

    @log_run_info
    def download_monitor_data(self):
        for node in self.nodes:
            try:
                collector = PrometheusSnapshots(name='prometheus_snapshot')

                snapshot_archive = collector.collect(node, self.logdir)
                self.log.debug('Snapshot local path: {}'.format(snapshot_archive))

                return S3Storage().upload_file(snapshot_archive, dest_dir=Setup.test_id())
            except Exception as details:  # pylint: disable=broad-except
                self.log.error('Error downloading prometheus data dir: %s', str(details))
                return ""

    def get_prometheus_snapshot(self, node):
        collector = PrometheusSnapshots(name="prometheus_data")

        return collector.collect(node, self.logdir)

    def download_monitoring_data_stack(self):

        for node in self.nodes:
            collector = MonitoringStack(name="monitoring-stack")
            local_path_to_monitor_stack = collector.collect(node, self.logdir)
            self.log.info('Path to monitoring stack {}'.format(local_path_to_monitor_stack))

            return S3Storage().upload_file(local_path_to_monitor_stack, dest_dir=Setup.test_id())


class NoMonitorSet():

    def __init__(self):

        self.log = SDCMAdapter(LOGGER, extra={'prefix': str(self)})
        self.nodes = []

    def __str__(self):
        return 'NoMonitorSet'

    def wait_for_init(self):
        self.log.info('Monitor nodes disabled for this run')

    def get_backtraces(self):
        pass

    def get_monitor_snapshot(self):
        pass

    def reconfigure_scylla_monitoring(self):
        pass

    def download_monitor_data(self):
        pass

    def destroy(self):
        pass

    def collect_logs(self, storage_dir):
        pass

    def get_grafana_screenshot_and_snapshot(self, test_start_time=None):  # pylint: disable=unused-argument,no-self-use,invalid-name
        return {}
