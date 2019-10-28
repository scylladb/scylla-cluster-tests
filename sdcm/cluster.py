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

import Queue
import atexit
import getpass
import logging
import os
import random
import re
import tempfile
import threading
import time
import uuid
import yaml
import shutil
import itertools
import json
import io
import concurrent.futures

from base64 import decodestring

import requests

from invoke.exceptions import UnexpectedExit, Failure

from collections import defaultdict

from sdcm.mgmt import ScyllaManagerError
from sdcm.prometheus import start_metrics_server
from textwrap import dedent
from datetime import datetime
from paramiko import SSHException

from sdcm.rsyslog_daemon import start_rsyslog
from .log import SDCMAdapter
from .remote import RemoteCmdRunner, LocalCmdRunner
from . import wait
from sdcm.utils.common import log_run_info, retrying, get_data_dir_path, Distro, verify_scylla_repo_file, S3Storage, \
    get_latest_gemini_version, get_my_ip
from .collectd import ScyllaCollectdSetup
from .db_stats import PrometheusDBStats

from sdcm.sct_events import Severity, CoreDumpEvent, CassandraStressEvent, DatabaseLogEvent, FullScanEvent, \
    ClusterHealthValidatorEvent
from sdcm.sct_events import EVENTS_PROCESSES
from sdcm.auto_ssh import start_auto_ssh, RSYSLOG_SSH_TUNNEL_LOCAL_PORT

SCYLLA_CLUSTER_DEVICE_MAPPINGS = [{"DeviceName": "/dev/xvdb",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}},
                                  {"DeviceName": "/dev/xvdc",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}}]

CREDENTIALS = []
OPENSTACK_INSTANCES = []
OPENSTACK_SERVICE = None
LIBVIRT_DOMAINS = []
LIBVIRT_IMAGES = []
LIBVIRT_URI = 'qemu:///system'
DEFAULT_USER_PREFIX = getpass.getuser()
# Test duration (min). Parameter used to keep instances produced by tests that
# are supposed to run longer than 24 hours from being killed
TEST_DURATION = 60
IP_SSH_CONNECTIONS = 'private'
TASK_QUEUE = 'task_queue'
RES_QUEUE = 'res_queue'
WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SCYLLA_YAML_PATH = "/etc/scylla/scylla.yaml"
SCYLLA_DIR = "/var/lib/scylla"


logger = logging.getLogger(__name__)
localrunner = LocalCmdRunner()


def set_ip_ssh_connections(ip_type):
    global IP_SSH_CONNECTIONS
    IP_SSH_CONNECTIONS = ip_type


def set_duration(duration):
    global TEST_DURATION
    TEST_DURATION = duration


def set_libvirt_uri(libvirt_uri):
    global LIBVIRT_URI
    LIBVIRT_URI = libvirt_uri


def clean_domain(domain_name):
    global LIBVIRT_URI
    localrunner.run('virsh -c %s destroy %s' % (LIBVIRT_URI, domain_name),
                    ignore_status=True)

    localrunner.run('virsh -c %s undefine %s' % (LIBVIRT_URI, domain_name),
                    ignore_status=True)


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def cleanup_instances(behavior='destroy'):
    global OPENSTACK_INSTANCES
    global OPENSTACK_SERVICE
    global CREDENTIALS
    global LIBVIRT_DOMAINS
    global LIBVIRT_IMAGES

    for openstack_instance in OPENSTACK_INSTANCES:
        if behavior == 'destroy':
            openstack_instance.destroy()

    for cred in CREDENTIALS:
        if behavior == 'destroy':
            if hasattr(cred, 'destroy'):
                cred.destroy()
            else:
                if OPENSTACK_SERVICE is not None:
                    OPENSTACK_SERVICE.delete_key_pair(cred.key_pair_name)

    for domain_name in LIBVIRT_DOMAINS:
        clean_domain(domain_name)

    for libvirt_image in LIBVIRT_IMAGES:
        shutil.rmtree(libvirt_image, ignore_errors=True)


def destroy_instances():
    cleanup_instances(behavior='destroy')


def stop_instances():
    cleanup_instances(behavior='stop')


def remove_cred_from_cleanup(cred):
    global CREDENTIALS
    if cred in CREDENTIALS:
        CREDENTIALS.remove(cred)


def register_cleanup(cleanup='destroy'):
    if cleanup == 'destroy':
        atexit.register(destroy_instances)
    elif cleanup == 'stop':
        atexit.register(stop_instances)


class Setup(object):

    KEEP_ALIVE = False

    REUSE_CLUSTER = False
    MIXED_CLUSTER = False
    MULTI_REGION = False
    BACKTRACE_DECODING = False
    INTRA_NODE_COMM_PUBLIC = False
    RSYSLOG_ADDRESS = None

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
            cls._test_id = test_id
            test_id_file_path = os.path.join(cls.logdir(), "test_id")
            with open(test_id_file_path, "w") as test_id_file:
                test_id_file.write(str(test_id))
        else:
            logger.warning("TestID already set!")

    @classmethod
    def logdir(cls):
        if not cls._logdir:
            sct_base = os.path.expanduser(os.environ.get('_SCT_LOGDIR', '~/sct-results'))
            date_time_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            cls._logdir = os.path.join(sct_base, str(date_time_formatted))
            os.makedirs(cls._logdir)

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
    def set_intra_node_comm_public(cls, intra_node_comm_public):
        cls.INTRA_NODE_COMM_PUBLIC = intra_node_comm_public

    @classmethod
    def reuse_cluster(cls, val=False):
        cls.REUSE_CLUSTER = val

    @classmethod
    def keep_cluster(cls, val='destroy'):
        if val in 'keep':
            cls.KEEP_ALIVE = True
        else:
            cls.KEEP_ALIVE = False

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
                       '''.format(*cls.RSYSLOG_ADDRESS))

        post_boot_script += dedent('''
               sudo sed -i 's/#MaxSessions \(.*\)$/MaxSessions 1000/' /etc/ssh/sshd_config
               sudo systemctl restart sshd
               sed -i -e 's/^\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*4096/*\t\tsoft\tnproc\t\tunlimited/' \
               /etc/security/limits.d/20-nproc.conf
               echo -e '*\t\thard\tnproc\t\tunlimited' >> /etc/security/limits.d/20-nproc.conf
               ''')
        return post_boot_script


def get_username():

    def is_email_in_scylladb_domain(email_addr):
        return True if email_addr and "@scylladb.com" in email_addr else False

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
    res = localrunner.run(cmd="git config --get user.email", ignore_status=True)
    if is_email_in_scylladb_domain(res.stdout):
        return get_email_user(res.stdout)
    # we didn't find email, fallback to current user with unknown email user identifier
    return "linux_user={}".format(current_linux_user)


def create_common_tags():
    build_tag = os.environ.get('BUILD_TAG', None)
    tags = dict(RunByUser=get_username(),
                TestName=str(Setup.test_name()),
                TestId=str(Setup.test_id()))

    if build_tag:
        tags["JenkinsJobTag"] = build_tag

    tags.update(Setup.TAGS)
    return tags


class NodeError(Exception):

    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        if self.msg is not None:
            return self.msg


class PrometheusSnapshotErrorException(Exception):
    pass


def prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class UserRemoteCredentials(object):

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


class BaseNode(object):
    CQL_PORT = 9042

    def __init__(self, name, parent_cluster, ssh_login_info=None, base_logdir=None, node_prefix=None, dc_idx=0):
        self.name = name
        self.is_seed = False
        self.dc_idx = dc_idx
        self.parent_cluster = parent_cluster  # reference to the Cluster object that the node belongs to
        self.logdir = os.path.join(base_logdir, self.name)
        os.makedirs(self.logdir)

        self._public_ip_address = None
        self._private_ip_address = None
        ssh_login_info['hostname'] = self.external_address

        self.remoter = RemoteCmdRunner(**ssh_login_info)
        self._ssh_login_info = ssh_login_info

        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug(self.remoter.ssh_debug_cmd())

        self._journal_thread = None
        self.n_coredumps = 0
        self.last_line_no = 1
        self.last_log_position = 0
        self._backtrace_thread = None
        self._db_log_reader_thread = None
        self._scylla_manager_journal_thread = None
        self._init_system = None
        self.db_init_finished = False

        self._short_hostname = None

        self._database_log_errors_index = []
        self._database_error_events = [DatabaseLogEvent(type='NO_SPACE_ERROR', regex='No space left on device'),
                                       DatabaseLogEvent(type='DATABASE_ERROR', regex='Exception '),
                                       DatabaseLogEvent(type='BAD_ALLOC', regex='std::bad_alloc'),
                                       DatabaseLogEvent(type='SCHEMA_FAILURE', regex='Failed to load schema version'),
                                       DatabaseLogEvent(type='RUNTIME_ERROR', regex='std::runtime_error'),
                                       DatabaseLogEvent(type='FILESYSTEM_ERROR', regex='filesystem_error'),
                                       DatabaseLogEvent(type='STACKTRACE', regex='stacktrace'),
                                       DatabaseLogEvent(type='BACKTRACE', regex='backtrace', severity=Severity.ERROR),
                                       DatabaseLogEvent(type='SEGMENTATION', regex='segmentation'),
                                       DatabaseLogEvent(type='INTEGRITY_CHECK', regex='integrity check failed'),
                                       DatabaseLogEvent(type='REACTOR_STALLED', regex='Reactor stalled'),
                                       DatabaseLogEvent(type='SEMAPHORE_TIME_OUT', regex='semaphore_timed_out'),
                                       DatabaseLogEvent(type='BOOT', regex='Starting Scylla Server', severity=Severity.NORMAL)]

        self.termination_event = threading.Event()
        self._running_nemesis = None
        self.start_task_threads()
        # We should disable bootstrap when we create nodes to establish the cluster,
        # if we want to add more nodes when the cluster already exists, then we should
        # enable bootstrap.
        self.enable_auto_bootstrap = False
        self.scylla_version = ''
        self._is_enterprise = None
        self.replacement_node_ip = None  # if node is a replacement for a dead node, store dead node private ip here
        self._distro = None
        self._cassandra_stress_version = None
        self.lock = threading.Lock()

        if (IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION) and Setup.RSYSLOG_ADDRESS:
            start_auto_ssh(Setup.test_id(), self, Setup.RSYSLOG_ADDRESS[1], RSYSLOG_SSH_TUNNEL_LOCAL_PORT)

    @property
    def short_hostname(self):
        if not self._short_hostname:
            try:
                self._short_hostname = self.remoter.run('hostname -s').stdout.strip()
            except Exception:
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
        # Distro attribute won't be changed, only need to detect once.
        return self._distro

    @distro.setter
    def distro(self, new_distro):
        self._distro = new_distro

    def probe_distro(self):
        # Probe the distro type
        distro = None

        result = self.remoter.run('cat /etc/redhat-release', ignore_status=True)
        if 'CentOS' in result.stdout and 'release 7.' in result.stdout:
            distro = Distro.CENTOS7
        if 'Red Hat Enterprise Linux' in result.stdout and 'release 7.' in result.stdout:
            distro = Distro.RHEL7

        if not distro:
            result = self.remoter.run('cat /etc/issue', ignore_status=True)
            if 'Ubuntu 14.04' in result.stdout:
                distro = Distro.UBUNTU14
            elif 'Ubuntu 16.04' in result.stdout:
                distro = Distro.UBUNTU16
            elif 'Ubuntu 18.04' in result.stdout:
                distro = Distro.UBUNTU18
            elif 'Debian GNU/Linux 8' in result.stdout:
                distro = Distro.DEBIAN8
            elif 'Debian GNU/Linux 9' in result.stdout:
                distro = Distro.DEBIAN9

        if not distro:
            self.log.debug("Failed to detect the distro name, %s" % result.stdout)

        return distro

    @property
    def is_client_encrypt(self):
        result = self.remoter.run("grep ^client_encryption_options: /etc/scylla/scylla.yaml -A 3 | grep enabled | awk '{print $2}'", ignore_status=True)
        return 'true' in result.stdout.lower()

    @property
    def is_server_encrypt(self):
        result = self.remoter.run("grep '^server_encryption_options:' /etc/scylla/scylla.yaml", ignore_status=True)
        return 'server_encryption_options' in result.stdout.lower()

    def is_centos7(self):
        return self.distro == Distro.CENTOS7

    def is_rhel7(self):
        return self.distro == Distro.RHEL7

    def is_rhel_like(self):
        return self.distro == Distro.CENTOS7 or self.distro == Distro.RHEL7

    def is_ubuntu14(self):
        return self.distro == Distro.UBUNTU14

    def is_ubuntu16(self):
        return self.distro == Distro.UBUNTU16

    def is_ubuntu18(self):
        return self.distro == Distro.UBUNTU18

    def is_ubuntu(self):
        return self.distro == Distro.UBUNTU16 or self.distro == Distro.UBUNTU14 or self.distro == Distro.UBUNTU18

    def is_debian8(self):
        return self.distro == Distro.DEBIAN8

    def is_debian9(self):
        return self.distro == Distro.DEBIAN9

    def is_debian(self):
        return self.distro == Distro.DEBIAN8 or self.distro == Distro.DEBIAN9

    def pkg_install(self, pkgs, apt_pkgs=None, ubuntu14_pkgs=None, ubuntu16_pkgs=None,
                    debian8_pkgs=None, debian9_pkgs=None):
        """
        Support to install packages to multiple distros

        :param pkgs: default package name string
        :param apt_pkgs: special package name string for apt-get
        """
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
        except Exception as details:
            self.log.error('Error checking if file %s exists: %s',
                           file_path, details)

    @property
    def is_enterprise(self):
        if self._is_enterprise is None:
            if self.is_rhel_like():
                result = self.remoter.run("sudo yum search scylla-enterprise", ignore_status=True)
                if 'One of the configured repositories failed (Extra Packages for Enterprise Linux 7 - x86_64)' in result.stdout:
                    result = self.remoter.run("sudo cat /etc/yum.repos.d/scylla.repo")
                    self._is_enterprise = 'enterprise' in result.stdout
                else:
                    self._is_enterprise = True if ('scylla-enterprise.x86_64' in result.stdout or
                                                   'No matches found' not in result.stdout) else False
            else:
                result = self.remoter.run("sudo apt-cache search scylla-enterprise", ignore_status=True)
                self._is_enterprise = True if 'scylla-enterprise' in result.stdout else False

        return self._is_enterprise

    @property
    def public_ip_address(self):
        return self._public_ip_address

    @property
    def private_ip_address(self):
        return self._private_ip_address

    @property
    def ip_address(self):
        if Setup.INTRA_NODE_COMM_PUBLIC:
            return self.public_ip_address
        else:
            return self.private_ip_address

    @property
    def external_address(self):
        """
        the communication address for usage between the test and the nodes
        :return:
        """

        if IP_SSH_CONNECTIONS == 'public' or Setup.INTRA_NODE_COMM_PUBLIC:
            return self.public_ip_address
        else:
            return self.private_ip_address

    @property
    def is_spot(self):
        return False

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
                db_services_log_cmd = ('sudo journalctl -f --no-tail --no-pager --utc {since}'
                                       '-u scylla-ami-setup.service '
                                       '-u scylla-io-setup.service '
                                       '-u scylla-server.service '
                                       '-u scylla-jmx.service'.format(**locals()))
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
        except Exception as details:
            self.log.error('Error retrieving remote node DB service log: %s',
                           details)

    def run(self, cmd, timeout=None, ignore_status=False,
            connect_timeout=300, options='', verbose=True,
            args=None, log_file=None, watch_stdout_pattern=None):
        """
        Run a shell command on a Node. Shorthand to remoter.run.

        See remote.Remoter.run() docstring for parameter documentation.
        """
        return self.remoter.run(cmd=cmd, timeout=timeout,
                                ignore_status=ignore_status,
                                connect_timeout=connect_timeout,
                                options=options, verbose=verbose,
                                args=args, log_file=log_file,
                                watch_stdout_pattern=watch_stdout_pattern)

    def send_files(self, src, dst, delete_dst=False,
                   preserve_symlinks=False, verbose=False):
        """
        Copy files from a local path to a Node. Shorthand to remoter.send_files.

        See remote.Remoter.send_files() docstring for parameter documentation.
        """
        self.remoter.send_files(src=src, dst=dst, delete_dst=delete_dst,
                                preserve_symlinks=preserve_symlinks,
                                verbose=verbose)

    def receive_files(self, src, dst, delete_dst=False,
                      preserve_perm=True, preserve_symlinks=False,
                      verbose=False):
        """
        Copy files from this Node to a local path. Shorthand to remoter.receive_files.

        See remote.Remoter.receive_files() docstring for parameter
        documentation.
        """
        self.remoter.receive_files(src=src, dst=dst, delete_dst=delete_dst,
                                   preserve_perm=preserve_perm,
                                   preserve_symlinks=preserve_symlinks,
                                   verbose=verbose)

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
        if logs_transport == "rsyslog":
            self.log.info("Using rsyslog as log transport")
        elif logs_transport == "ssh":
            self._journal_thread = threading.Thread(target=self.journal_thread)
            self._journal_thread.daemon = True
            self._journal_thread.start()
        else:
            raise Exception("Unknown logs transport: %s" % logs_transport)

    def _get_coredump_backtraces(self, last=True):
        """
        Get coredump backtraces.

        :param last: Whether to only show the last backtrace.
        :return: fabric.Result output
        """
        try:
            backtrace_cmd = 'sudo coredumpctl info'
            if last:
                backtrace_cmd += ' -1'
            return self.remoter.run(backtrace_cmd,
                                    verbose=False, ignore_status=True)
        except Exception as details:
            self.log.error('Error retrieving core dump backtraces : %s',
                           details)

    def _upload_coredump(self, coredump):
        try:
            if self.is_debian() or self.is_ubuntu():
                self.remoter.run('sudo apt-get install -y pigz')
            else:
                self.remoter.run('sudo yum install -y pigz')
            self.remoter.run('sudo pigz --fast --keep {}'.format(coredump))
            coredump += '.gz'
        except Exception as ex:  # pylint: disable=broad-except
            self.log.warning("Failed to compress coredump '%s': %s", coredump, ex)

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

    def _notify_backtrace(self, last):
        """
        Notify coredump backtraces to test log and coredump.log file.

        :param last: Whether to show only the last backtrace.
        """
        result = self._get_coredump_backtraces(last=last)
        if result.exit_status == 127:  # coredumpctl command not found
            return
        log_file = os.path.join(self.logdir, 'coredump.log')
        output = result.stdout + result.stderr
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Coredump:'):
                url = ""
                download_instructions = "failed to upload"
                try:
                    coredump = line.split()[-1]
                    self.log.debug('Found coredump file: {}'.format(coredump))
                    url, download_instructions = self._upload_coredump(coredump)
                finally:
                    CoreDumpEvent(corefile_url=url, download_instructions=download_instructions, backtrace=output, node=self)

        with open(log_file, 'a') as log_file_obj:
            log_file_obj.write(output)
        for line in output.splitlines():
            self.log.error(line)

    def _get_n_coredumps(self):
        """
        Get the number of coredumps stored on this Node.

        :return: Number of coredumps
        :rtype: int
        """
        n_backtraces_cmd = 'sudo coredumpctl --no-pager --no-legend 2>&1'
        result = self.remoter.run(n_backtraces_cmd, verbose=False, ignore_status=True)
        if "No coredumps found" in result.stdout or result.exit_status == 127:  # exit_status 127: coredumpctl command not found
            return 0
        return len(result.stdout.splitlines())

    def get_backtraces(self):
        """
        Verify the number of backtraces stored, report if new ones were found.
        """
        self.wait_ssh_up(verbose=False)
        if self.is_ubuntu14():
            # fixme: ubuntu14 doesn't has coredumpctl, skip it.
            return
        new_n_coredumps = self._get_n_coredumps()
        if new_n_coredumps is not None:
            if (new_n_coredumps - self.n_coredumps) == 1:
                self._notify_backtrace(last=True)
            elif (new_n_coredumps - self.n_coredumps) > 1:
                self._notify_backtrace(last=False)
            self.n_coredumps = new_n_coredumps

    def backtrace_thread(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        while not self.termination_event.isSet():
            self.termination_event.wait(15)
            self.get_backtraces()

    def db_log_reader_thread(self):
        """
        Keep reporting new events from db log, every 30 seconds.
        """
        while not self.termination_event.isSet():
            self.termination_event.wait(15)
            try:
                self.search_database_log(start_from_beginning=False, publish_events=True)
            except Exception:
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

    def __str__(self):
        return 'Node %s [%s | %s] (seed: %s)' % (self.name,
                                                 self.public_ip_address,
                                                 self.private_ip_address,
                                                 self.is_seed)

    def restart(self):
        raise NotImplementedError('Derived classes must implement restart')

    def hard_reboot(self):
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
            except Exception as ex:
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
        if 'db-node' in self.name:  # this should be replaced when DbNode class will be created
            self.start_journal_thread()
            self.start_backtrace_thread()
            self.start_db_log_reader_thread()

    @log_run_info
    def stop_task_threads(self, timeout=10):
        if self.termination_event.isSet():
            return
        self.termination_event.set()
        if self._backtrace_thread:
            self._backtrace_thread.join(timeout)
        if self._db_log_reader_thread:
            self._db_log_reader_thread.join(timeout)
        if self._journal_thread:
            self.remoter.run(cmd='sudo pkill -f "journalctl.*scylla"', ignore_status=True)
            self._journal_thread.join(timeout)
        if self._scylla_manager_journal_thread:
            self.stop_scylla_manager_log_capture(timeout)

    def get_cpumodel(self):
        """Get cpu model from /proc/cpuinfo

        Get cpu model from /proc/cpuinfo of node
        """
        cpuinfo_cmd = 'cat /proc/cpuinfo'

        try:
            result = self.remoter.run(cmd=cpuinfo_cmd, verbose=False)
            model = re.findall('^model\sname\s:(.+)$', result.stdout.strip(), re.MULTILINE)
            return model[0].strip()

        except Exception as details:
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
        except Exception as details:
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
        except Exception as details:
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
        if not self.distro:
            self.distro = self.probe_distro()

    def is_port_used(self, port, service_name):
        try:
            # check that port is taken
            result_netstat = self.remoter.run('netstat -ln | grep :%s' % port,
                                              # -n don't translate port numbers to names
                                              verbose=False, ignore_status=True)
            return result_netstat.exit_status == 0
        except Exception as details:
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
            self.remoter.receive_files(src=pcap_tmp_file, dst=pcap_file)
        except Exception as details:
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
            self.remoter.run('sudo -u scylla touch %s' % mark_path,
                             verbose=verbose)

    def wait_db_up(self, verbose=True, timeout=3600):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60, text=text, timeout=timeout, throw_exc=True)
        self.db_init_finished = True
        self._report_housekeeping_uuid()

    def apt_running(self):
        try:
            result = self.remoter.run('sudo lsof /var/lib/dpkg/lock', ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
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
        with open(self.database_log) as f:
            f.seek(0, os.SEEK_END)
            return f.tell()

    def search_database_log(self, search_pattern=None, start_from_beginning=False, publish_events=True):
        """
        Search for all known patterns listed in  `_database_error_events`

        :param start_from_beginning: if True will search log from first line
        :param publish_events: if True will publish events
        :return: list of (line, error) tuples
        """

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

        with open(self.database_log, 'r') as f:
            if start_search_from_byte:
                f.seek(start_search_from_byte)
            for index, line in enumerate(f, start=last_line_no):
                if not start_from_beginning and Setup.RSYSLOG_ADDRESS:
                    logger.debug(line)

                m = backtrace_regex.search(line)
                if m and backtraces:
                    data = m.groupdict()
                    if data['other_bt']:
                        backtraces[-1]['backtrace'] += [data['other_bt'].strip()]
                    if data['scylla_bt']:
                        backtraces[-1]['backtrace'] += [data['scylla_bt'].strip()]

                if index not in self._database_log_errors_index or start_from_beginning:
                    # for each line use all regexes to match, and if found send an event
                    for pattern, event in patterns:
                        m = pattern.search(line)
                        if m:
                            self._database_log_errors_index.append(index)
                            e = event.clone_with_info(node=self, line_number=index, line=line)
                            backtraces.append(dict(event=e, backtrace=[]))
                            matches.append((index, line))

            if not start_from_beginning:
                self.last_line_no = index if index else last_line_no
                self.last_log_position = f.tell() + 1

        if publish_events:
            traces_count = 0
            for b in backtraces:
                b['event'].add_backtrace_info(raw_backtrace="\n".join(b['backtrace']))
                if b['event'].type == 'BACKTRACE':
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
                backtraces = filter(filter_backtraces, backtraces)

            for backtrace in backtraces:
                if Setup.BACKTRACE_DECODING:
                    if backtrace['event'].raw_backtrace:
                        try:
                            scylla_debug_info = self.get_scylla_debuginfo_file()
                            output = self.remoter.run('addr2line -Cpife {0} {1}'.format(scylla_debug_info, " ".join(backtrace['event'].raw_backtrace.split('\n'))), verbose=False)
                            backtrace['event'].add_backtrace_info(backtrace=output.stdout)
                        except Exception:
                            self.log.exception("failed to decode backtrace")
                backtrace['event'].publish()

        return matches

    def get_scylla_debuginfo_file(self):
        """
        Lookup the scylla debug information, in various places it can be.

        :return the path to the scylla debug information
        :rtype str
        """
        # first try default location
        scylla_debug_info = '/usr/lib/debug/bin/scylla.debug'
        results = self.remoter.run('[[ -f {} ]]'.format(scylla_debug_info), ignore_status=True)
        if results.exit_status == 0:
            return scylla_debug_info

        # then try the relocatable location
        results = self.remoter.run('ls /usr/lib/debug/opt/scylladb/libexec/scylla.bin*.debug')
        if results.stdout.strip():
            return results.stdout.strip()

        # then look it up base on the build id
        results = self.remoter.run('file /usr/bin/scylla')
        build_id_regex = re.compile(r'BuildID\[.*\]=(.*),')
        build_id = build_id_regex.search(results.stdout).group(1)

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

    def config_setup(self, seed_address=None, cluster_name=None, enable_exp=True, endpoint_snitch=None,
                     yaml_file=SCYLLA_YAML_PATH, broadcast=None, authenticator=None, server_encrypt=None,
                     client_encrypt=None, append_conf=None, append_scylla_args=None, debug_install=False,
                     hinted_handoff='enabled', murmur3_partitioner_ignore_msb_bits=None, authorizer=None):
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'), 'scylla.yaml')
        self.remoter.receive_files(src=yaml_file, dst=yaml_dst_path)

        with open(yaml_dst_path, 'r') as f:
            scylla_yaml_contents = f.read()

        if seed_address:
            # Set seeds
            p = re.compile('seeds:.*')
            scylla_yaml_contents = p.sub('seeds: "{0}"'.format(seed_address),
                                         scylla_yaml_contents)

            # Set listen_address
            p = re.compile('listen_address:.*')
            scylla_yaml_contents = p.sub('listen_address: {0}'.format(self.private_ip_address),
                                         scylla_yaml_contents)
            # Set rpc_address
            p = re.compile('\n[# ]*rpc_address:.*')
            scylla_yaml_contents = p.sub('\nrpc_address: {0}'.format(self.private_ip_address),
                                         scylla_yaml_contents)
        if broadcast:
            # Set broadcast_address
            p = re.compile('[# ]*broadcast_address:.*')
            scylla_yaml_contents = p.sub('broadcast_address: {0}'.format(broadcast),
                                         scylla_yaml_contents)

            # Set broadcast_rpc_address
            p = re.compile('[# ]*broadcast_rpc_address:.*')
            scylla_yaml_contents = p.sub('broadcast_rpc_address: {0}'.format(broadcast),
                                         scylla_yaml_contents)

        if cluster_name:
            p = re.compile('[# ]*cluster_name:.*')
            scylla_yaml_contents = p.sub('cluster_name: {0}'.format(cluster_name),
                                         scylla_yaml_contents)

        # disable hinted handoff (it is enabled by default in Scylla). Expected values: "enabled"/"disabled"
        if hinted_handoff == 'disabled':
            p = re.compile('[# ]*hinted_handoff_enabled:.*')
            scylla_yaml_contents = p.sub('hinted_handoff_enabled: false', scylla_yaml_contents, count=1)

        if murmur3_partitioner_ignore_msb_bits:
            self.log.debug('Change murmur3_partitioner_ignore_msb_bits to {}'.format(murmur3_partitioner_ignore_msb_bits))
            p = re.compile('murmur3_partitioner_ignore_msb_bits:.*')
            if p.findall(scylla_yaml_contents):
                scylla_yaml_contents = p.sub('murmur3_partitioner_ignore_msb_bits: {0}'.format(murmur3_partitioner_ignore_msb_bits),
                                             scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nmurmur3_partitioner_ignore_msb_bits: {0}\n".format(murmur3_partitioner_ignore_msb_bits)

        if enable_exp:
            scylla_yaml_contents += "\nexperimental: true\n"

        if endpoint_snitch:
            p = re.compile('endpoint_snitch:.*')
            scylla_yaml_contents = p.sub('endpoint_snitch: "{0}"'.format(endpoint_snitch),
                                         scylla_yaml_contents)
        if not client_encrypt:
            p = re.compile('.*enabled: true.*# <client_encrypt>.*')
            scylla_yaml_contents = p.sub('   enabled: false                    # <client_encrypt>', scylla_yaml_contents)

        if self.enable_auto_bootstrap:
            if 'auto_bootstrap' in scylla_yaml_contents:
                if re.findall("auto_bootstrap: False", scylla_yaml_contents):
                    self.log.debug('auto_bootstrap is not set as expected, update it to `True`.')
                p = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = p.sub('auto_bootstrap: True',
                                             scylla_yaml_contents)
            else:
                self.log.debug('auto_bootstrap is missing, set it `True`.')
                scylla_yaml_contents += "\nauto_bootstrap: True\n"
        else:
            if 'auto_bootstrap' in scylla_yaml_contents:
                if re.findall("auto_bootstrap: True", scylla_yaml_contents):
                    self.log.debug('auto_bootstrap is not set as expected, update it to `False`.')
                p = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = p.sub('auto_bootstrap: False',
                                             scylla_yaml_contents)
            else:
                self.log.debug('auto_bootstrap is missing, set it `False`.')
                scylla_yaml_contents += "\nauto_bootstrap: False\n"

        if authenticator in ['AllowAllAuthenticator', 'PasswordAuthenticator']:
            p = re.compile('[# ]*authenticator:.*')
            scylla_yaml_contents = p.sub('authenticator: {0}'.format(authenticator),
                                         scylla_yaml_contents)
        if authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
            p = re.compile('[# ]*authorizer:.*')
            scylla_yaml_contents = p.sub('authorizer: {0}'.format(authorizer),
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
            logger.debug("%s is a replacement node for '%s'." % (self.name, self.replacement_node_ip))
            scylla_yaml_contents += "\nreplace_address_first_boot: %s\n" % self.replacement_node_ip

        if append_conf:
            scylla_yaml_contents += append_conf

        with open(yaml_dst_path, 'w') as f:
            f.write(scylla_yaml_contents)

        self.log.debug("Scylla YAML configuration:\n%s", scylla_yaml_contents)
        self.remoter.send_files(src=yaml_dst_path,
                                dst='/tmp/scylla.yaml')
        self.remoter.run('sudo mv /tmp/scylla.yaml {}'.format(yaml_file))

        if append_scylla_args:
            if self.is_rhel_like():
                self.remoter.run("sudo sed -i -e 's/SCYLLA_ARGS=\"/SCYLLA_ARGS=\"%s /' /etc/sysconfig/scylla-server" % append_scylla_args)
            elif self.is_debian() or self.is_ubuntu():
                self.remoter.run("sudo sed -i -e 's/SCYLLA_ARGS=\"/SCYLLA_ARGS=\"%s /'  /etc/default/scylla-server" % append_scylla_args)

        if debug_install and self.is_rhel_like():
            self.remoter.run('sudo yum install -y scylla-gdb', verbose=True, ignore_status=True)

    def config_client_encrypt(self):
        self.remoter.send_files(src='./data_dir/ssl_conf', dst='/tmp/')
        setup_script = dedent("""
            mkdir -p ~/.cassandra/
            cp /tmp/ssl_conf/client/cqlshrc ~/.cassandra/
            sudo mkdir -p /etc/scylla/
            sudo mv /tmp/ssl_conf/ /etc/scylla/
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

    def clean_scylla(self):
        """
        Uninstall scylla
        """
        self.stop_scylla_server(verify_down=False, ignore_status=True)
        if self.is_rhel_like():
            self.remoter.run('sudo yum remove -y scylla\*')
        else:
            self.remoter.run('sudo rm -f /etc/apt/sources.list.d/scylla.list')
            self.remoter.run('sudo apt-get remove -y scylla\*', ignore_status=True)
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
        except Exception as ex:
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
            self.remoter.run('sudo DEBIAN_FRONTEND=noninteractive apt-get --force-yes -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" upgrade -y', retry=3)
        # update repo cache after upgrade
        self.update_repo_cache()

    def install_scylla(self, scylla_repo):
        """
        Download and install scylla on node
        :param scylla_repo: scylla repo file URL
        """
        if self.is_rhel_like():
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
                    add-apt-repository -y ppa:scylladb/ppa
                    apt-get update
                    apt-get install -y openjdk-8-jre-headless
                    update-java-alternatives -s java-1.8.0-openjdk-amd64
                """)
                self.remoter.run('sudo bash -cxe "%s"' % install_prereqs)
            elif self.is_debian8():
                self.remoter.run("sudo sed -i -e 's/jessie-updates/stable-updates/g' /etc/apt/sources.list")
                self.remoter.run('echo "deb http://archive.debian.org/debian jessie-backports main" |sudo tee /etc/apt/sources.list.d/backports.list')
                self.remoter.run("sudo sed -i -e 's/:\/\/.*\/debian jessie-backports /:\/\/archive.debian.org\/debian jessie-backports /g' /etc/apt/sources.list.d/*.list")
                self.remoter.run("echo 'Acquire::Check-Valid-Until \"false\";' |sudo tee /etc/apt/apt.conf.d/99jessie-backports")
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install gnupg-curl -y')
                self.remoter.run('sudo apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key')
                self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19')
                self.remoter.run('echo "deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /" |sudo tee /etc/apt/sources.list.d/scylla-3rdparty.list')
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
                    echo 'deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-stretch/Debian_9.0/ /' > /etc/apt/sources.list.d/scylla-3rdparty.list
                """)
                self.remoter.run('sudo bash -cxe "%s"' % install_debian_9_prereqs)

            self.remoter.run('sudo DEBIAN_FRONTEND=noninteractive apt-get --force-yes -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" upgrade -y')
            self.remoter.run('sudo apt-get install -y rsync tcpdump screen wget net-tools')
            self.download_scylla_repo(scylla_repo)
            self.remoter.run('sudo apt-get update')
            self.remoter.run('sudo apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" --force-yes --allow-unauthenticated {0}'.format(self.scylla_pkg()))

    def install_scylla_debuginfo(self):
        if not self.scylla_version:
            self.get_scylla_version()
        if self.is_rhel_like():
            self.remoter.run(r'sudo yum install -y {0}-debuginfo-{1}\*'.format(self.scylla_pkg(), self.scylla_version), ignore_status=True)
        else:
            self.remoter.run(r'sudo apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" --force-yes --allow-unauthenticated {0}-server-dbg={1}\*'.format(self.scylla_pkg(), self.scylla_version), ignore_status=True)

    def get_scylla_version(self):
        version_commands = ["scylla --version", "rpm -q {}".format(self.scylla_pkg())]
        for version_cmd in version_commands:
            try:
                result = self.remoter.run(version_cmd)
            except Exception as ex:
                self.log.error('Failed getting scylla version: %s', ex)
            else:
                match = re.match(r"((\d+)\.(\d+)\.([\d\w]+)\.?([\d\w]+)?).*", result.stdout)
                if match:
                    scylla_version = match.group(1)
                    self.log.info("Found ScyllaDB version: %s" % scylla_version)
                    self.scylla_version = scylla_version
                    return scylla_version
                else:
                    self.log.error("Unknown ScyllaDB version")

    @log_run_info("Detecting disks")
    def detect_disks(self, nvme=True):
        """
        Detect local disks
        :param nvme: NVMe(True) or SCSI(False) disk
        :return: list of disk names
        """
        patt = ('nvme0n*', 'nvme0n\w+') if nvme else ('sd[b-z]', 'sd\w+')
        result = self.remoter.run('ls /dev/{}'.format(patt[0]))
        disks = re.findall('/dev/{}'.format(patt[1]), result.stdout)
        assert disks, 'Failed to find disks!'
        self.log.debug("Found disks: %s", disks)
        return disks

    @log_run_info
    def scylla_setup(self, disks):
        """
        Setup scylla
        :param disks: list of disk names
        """
        result = self.remoter.run('/sbin/ip -o link show |grep ether |awk -F": " \'{print $2}\'', verbose=True)
        devname = result.stdout.strip()
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

    def install_mgmt(self, scylla_repo, scylla_mgmt_repo):
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
            install_transport_https = dedent("""
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

        self.log.debug("Copying TLS files from data_dir to node")
        self.remoter.send_files(src='./data_dir/ssl_conf', dst='/tmp/')

        self.download_scylla_repo(scylla_repo)
        self.download_scylla_manager_repo(scylla_mgmt_repo)
        if self.is_docker():
            self.remoter.run('sudo yum remove -y scylla scylla-jmx scylla-tools scylla-tools-core'
                             ' scylla-server scylla-conf')

        if self.is_rhel_like():
            self.remoter.run('sudo yum install -y scylla-manager')
        else:
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)
            self.remoter.run('sudo apt-get install -y scylla-manager --force-yes')

        if self.is_docker():
            try:
                self.remoter.run('echo no| sudo scyllamgr_setup')
            except Exception as ex:
                self.log.warning(ex)
        else:
            self.remoter.run('echo yes| sudo scyllamgr_setup')
        self.remoter.send_files(src=self._ssh_login_info['key_file'], dst=rsa_id_dst)
        ssh_config_script = dedent("""
                chmod 0400 {rsa_id_dst}
                chown {mgmt_user}:{mgmt_user} {rsa_id_dst}
                ssh-keygen -y -f {rsa_id_dst} > {rsa_id_dst_pub}
        """.format(**locals()))  # generate ssh public key from private key.
        self.remoter.run('sudo bash -cxe "%s"' % ssh_config_script)

        if self.is_docker():
            self.remoter.run('sudo supervisorctl restart scylla-manager')
            res = self.remoter.run('sudo supervisorctl status scylla-manager')
        else:
            self.remoter.run('sudo systemctl restart scylla-manager.service')
            res = self.remoter.run('sudo systemctl status scylla-manager.service')

        if not res or "Active: failed" in res.stdout:
            raise ScyllaManagerError("Scylla-Manager is not properly installed or not running: {}".format(res))

        self.start_scylla_manager_log_capture()

    def retrieve_scylla_manager_log(self):
        mgmt_log_name = os.path.join(self.logdir, 'scylla_manager.log')
        cmd = "sudo journalctl -u scylla-manager -f".format(mgmt_log_name)
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

    def collect_mgmt_log(self):
        self.log.debug("Collect scylla manager log ...")
        mgmt_log_name = "/tmp/{}_scylla_manager.log".format(self.name)
        self.remoter.run('sudo journalctl -u scylla-manager.service --no-tail > {}'.format(mgmt_log_name), ignore_status=True, verbose=False)
        self.log.debug("Collected log : {}".format(mgmt_log_name))
        return mgmt_log_name

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
                         {'user': self._ssh_login_info['user'],
                          'identity_file': rsa_id_dst}
                     }
        (_, conf_file) = tempfile.mkstemp(dir='/tmp')
        with open(conf_file, 'w') as fd:
            yaml.dump(mgmt_conf, fd, default_flow_style=False)
        self.remoter.send_files(src=conf_file, dst=mgmt_conf_tmp)
        self.remoter.run('sudo cp {} {}'.format(mgmt_conf_tmp, mgmt_conf_dst))
        if self.is_docker():
            self.remoter.run('sudo supervisorctl start scylla-manager')
        else:
            self.remoter.run('sudo systemctl restart scylla-manager.service')

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl start scylla-server.service', timeout=timeout)
        else:
            self.remoter.run('sudo service scylla-server start', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=timeout)

    def start_scylla_jmx(self, verify_up=True, verify_down=False, timeout=300):
        if verify_down:
            self.wait_jmx_down(timeout=timeout)
        if not self.is_ubuntu14():
            self.remoter.run('sudo systemctl start scylla-jmx.service', timeout=timeout)
        else:
            self.remoter.run('sudo service scylla-jmx start', timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=timeout)

    @log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=timeout)

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

    def enable_client_encrypt(self):
        SCYLLA_YAML_PATH_TMP = "/tmp/scylla.yaml"
        self.remoter.run("sudo cat {} | grep -v '<client_encrypt>' > {}".format(SCYLLA_YAML_PATH, SCYLLA_YAML_PATH_TMP))
        self.remoter.run("sudo mv -f {} {}".format(SCYLLA_YAML_PATH_TMP, SCYLLA_YAML_PATH))
        self.config_setup(client_encrypt=True)
        self.stop_scylla()
        self.start_scylla()

    def disable_client_encrypt(self):
        self.config_setup(client_encrypt=False)
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
        return ''

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
        self.config_setup(murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)
        self.start_scylla()

        resharding_started = wait.wait_for(func=self._resharding_status, step=5, timeout=3600,
                                           text="Wait for re-sharding to be started", status='start')
        if not resharding_started:
            logger.error('Resharding has not been started (murmur3_partitioner_ignore_msb_bits={}) '
                         'Check the log for the detailes'.format(murmur3_partitioner_ignore_msb_bits))
            return

        resharding_finished = wait.wait_for(func=self._resharding_status, step=5,
                                            text="Wait for re-sharding to be finished", status='finish')

        if not resharding_finished:
            logger.error('Resharding was not finished! (murmur3_partitioner_ignore_msb_bits={}) '
                         'Check the log for the detailes'.format(murmur3_partitioner_ignore_msb_bits))
        else:
            logger.debug('Resharding has been finished successfully (murmur3_partitioner_ignore_msb_bits={})'.
                         format(murmur3_partitioner_ignore_msb_bits))

    def _gen_nodetool_cmd(self, sub_cmd, args, options):
        credentials = self.parent_cluster.get_db_auth()
        if credentials:
            options += '-u {} -pw {} '.format(*credentials)
        return "nodetool {options} {sub_cmd} {args}".format(**locals())

    def run_nodetool(self, sub_cmd, args="", options="", ignore_status=False, verbose=True):
        """
            Wrapper for nodetool command.
            Command format: nodetool [options] command [args]

        :param sub_cmd: subcommand like status
        :param args: arguments for the subcommand
        :param options: nodetool options:
            -h	--host	Hostname or IP address.
            -p	--port	Port number.
            -pwf	--password-file	Password file path.
            -pw	--password	Password.
            -u	--username	Username.
        :param ignore_status: don't throw exception if the command fails
        :return: Remoter result object
        """
        cmd = self._gen_nodetool_cmd(sub_cmd, args, options)
        result = self.remoter.run(cmd, ignore_status=ignore_status, verbose=verbose)
        self.log.debug("Command '%s' duration -> %s s" % (result.command, result.duration))

        return result

    def check_node_health(self):
        self.check_nodes_status()
        self.check_schema_version()

    def check_nodes_status(self):
        # Validate nodes health
        node_type = 'target' if self.running_nemesis else 'regular'
        # TODO: improve this part (using get_nodetool_status function from ScyllaCluster)
        try:
            self.log.info('Status for %s node %s: %s' % (node_type, self.name, self.run_nodetool('status')))

        except Exception as ex:
            ClusterHealthValidatorEvent(type='error', name='NodesStatus', status=Severity.CRITICAL,
                                        node=self.name,
                                        error="Unable to get nodetool status from '{node}': {ex}".format(ex=ex,
                                                                                                         node=self.name))

    def validate_gossip_nodes_info(self):
        result = self.run_nodetool('gossipinfo', verbose=False)
        gossip_node_schemas = {}
        schema, ip, status = '', '', ''
        for line in result.stdout.split():
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
        cluster_nodes = [node.private_ip_address for node in self.parent_cluster.nodes if node.db_init_finished]

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
            ClusterHealthValidatorEvent(type='error', name='GossipNodeSchemaVersion', status=Severity.CRITICAL,
                                        node=self.name,
                                        error='Schema version is not same on all nodes in gossip info: {}'
                                        .format('\n'.join('{}: {}'.format(ip, schema_version['schema'])
                                                          for ip, schema_version in gossip_node_schemas.iteritems())))

        return gossip_node_schemas

    def check_schema_version(self):
        # Get schema version
        errors = []
        peers_details = ''
        try:
            gossip_node_schemas = self.validate_gossip_nodes_info()
            status = gossip_node_schemas[self.private_ip_address]['status']
            if status != 'NORMAL':
                self.log.debug('Node status is {status}. Schema version can\'t be validated'. format(status=status))
                return

            # Search for nulls in system.peers
            peers_details = self.run_cqlsh('select peer, data_center, host_id, rack, release_version, '
                                           'rpc_address, schema_version, supported_features from system.peers',
                                           split=True, verbose=False)

            for line in peers_details[3:-2]:
                line_splitted = line.split('|')
                current_node_ip = line_splitted[0].strip()
                nulls = [column for column in line_splitted[1:] if column.strip() == 'null']

                if nulls:
                    errors.append('Found nulls in system.peers on the node %s: %s' % (current_node_ip, peers_details))

                peer_schema_version = line_splitted[6].strip()
                gossip_node_schema_version = gossip_node_schemas[self.private_ip_address]['schema']
                if gossip_node_schema_version and peer_schema_version != gossip_node_schema_version:
                    errors.append('Expected schema version: %s. Wrong schema version found on the '
                                  'node %s: %s' % (gossip_node_schema_version, current_node_ip, peer_schema_version))
        except Exception as e:
            errors.append('Validate schema version failed.{} Error: {}'
                          .format(' SYSTEM.PEERS content: {}\n'.format(peers_details) if peers_details else '', str(e)))

        if errors:
            ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.CRITICAL,
                                        node=self.name, error='\n'.join(errors))

    def _gen_cqlsh_cmd(self, command, keyspace, timeout, host, port, connect_timeout):
        """cqlsh [options] [host [port]]"""
        credentials = self.parent_cluster.get_db_auth()
        auth_params = '-u {} -p {}'.format(*credentials) if credentials else ''
        use_keyspace = "--keyspace {}".format(keyspace) if keyspace else ""
        ssl_params = '--ssl' if self.parent_cluster.params.get("client_encrypt") else ''
        options = "--no-color {auth_params} {use_keyspace} --request-timeout={timeout} --connect-timeout={connect_timeout} {ssl_params}".format(
            auth_params=auth_params, use_keyspace=use_keyspace, timeout=timeout, connect_timeout=connect_timeout, ssl_params=ssl_params)
        return 'cqlsh {options} -e "{command}" {host} {port}'.format(options=options, command=command, host=host, port=port)

    def run_cqlsh(self, cmd, keyspace=None, port=None, timeout=60, verbose=True, split=False, target_db_node=None, connect_timeout=5):
        """Runs CQL command using cqlsh utility"""
        cmd = self._gen_cqlsh_cmd(command=cmd, keyspace=keyspace, timeout=timeout,
                                  host=self.ip_address if not target_db_node else target_db_node.ip_address,
                                  port=port if port else self.CQL_PORT,
                                  connect_timeout=connect_timeout)
        cqlsh_out = self.remoter.run(cmd, timeout=timeout + 5,  # we give 5 seconds to cqlsh timeout mechanism to work
                                     verbose=verbose)
        # stdout of cqlsh example:
        #      pk
        #      ----
        #       2
        #       3
        #
        #      (10 rows)
        return cqlsh_out if not split else map(str.strip, cqlsh_out.stdout.splitlines())

    def run_startup_script(self):
        startup_script_remote_path = '/tmp/sct-startup.sh'

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(Setup.get_startup_script())
            tmp_file.flush()
            self.remoter.send_files(src=tmp_file.name, dst=startup_script_remote_path)

        cmds = dedent("""
                chmod +x {0}
                {0}
            """.format(startup_script_remote_path))

        result = self.remoter.run("sudo bash -ce '%s'" % cmds)
        logger.debug(result.stdout)


class BaseCluster(object):

    """
    Cluster of Node objects.
    """

    def __init__(self, cluster_uuid=None, cluster_prefix='cluster',
                 node_prefix='node', n_nodes=[10], params=None, region_names=None):
        if cluster_uuid is None:
            self.uuid = Setup.test_id()
        else:
            self.uuid = cluster_uuid
        self.shortid = str(self.uuid)[:8]
        self.name = '%s-%s' % (cluster_prefix, self.shortid)
        self.node_prefix = '%s-%s' % (node_prefix, self.shortid)
        self._node_index = 0
        # I wanted to avoid some parameter passing
        # from the tester class to the cluster test.
        assert '_SCT_TEST_LOGDIR' in os.environ

        self.logdir = os.path.join(os.environ['_SCT_TEST_LOGDIR'],
                                   self.name)
        os.makedirs(self.logdir)

        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Init nodes')
        self.nodes = []
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

        queue = Queue.Queue()
        for node in node_list:
            setup_thread = threading.Thread(target=func, args=(node, queue))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass
        return results

    def get_backtraces(self):
        for node in self.nodes:
            try:
                node.get_backtraces()
                if node.n_coredumps > 0:
                    self.coredumps[node.name] = node.n_coredumps
            except Exception as ex:
                self.log.warning("Unable to get coredump status from node {node}: {ex}".format(**locals()))

    def node_setup(self, node, verbose=False, timeout=3600):
        raise NotImplementedError("Derived class must implement 'node_setup' method!")

    def get_node_ips_param(public_ip=True):
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

    def get_node_database_errors(self):
        errors = {}
        for node in self.nodes:
            node_errors = node.search_database_log(start_from_beginning=True, publish_events=False)
            if node_errors:
                errors.update({node.name: node_errors})
        return errors

    def _param_enabled(self, param):
        param = self.params.get(param)
        if isinstance(param, str):
            return True if param and param.lower() == 'true' else False
        elif isinstance(param, bool):
            return param
        elif param is None:
            return False
        else:
            raise ValueError('Unsupported type: {}'.format(type(param)))

    def set_tc(self, node, dst_nodes):
        node.remoter.run("sudo modprobe sch_netem")
        node.remoter.run("sudo tc qdisc del dev eth0 root", ignore_status=True)
        node.remoter.run("sudo tc qdisc add dev eth0 handle 1: root prio")
        for dst in dst_nodes:
            node.remoter.run("sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {} flowid 2:1".format(dst.private_ip_address))
        node.remoter.run("sudo tc qdisc add dev eth0 parent 1:1 handle 2:1 netem delay 100ms 20ms 25% reorder 5% 25% loss random 5% 25%")
        node.remoter.run("sudo tc qdisc show dev eth0", verbose=True)
        node.remoter.run("sudo tc filter show dev eth0", verbose=True)

    def set_traffic_control(self):
        if self._param_enabled('enable_tc'):
            for node in self.nodes:
                dst_nodes = [n for n in self.nodes if n.dc_idx != node.dc_idx]
                local_nodes = [n for n in self.nodes if n.dc_idx == node.dc_idx and n != node]
                self.set_tc(node, dst_nodes, local_nodes)

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
        logger.debug('Class instance: %s', cl_inst)
        logger.debug('Method kwargs: %s', kwargs)
        node_list = kwargs.get('node_list', None) or cl_inst.nodes
        timeout = kwargs.get('timeout', None)
        setup_kwargs = {k: kwargs[k] for k in kwargs if k != 'node_list'}

        queue = Queue.Queue()

        def node_setup(node):
            status = True
            try:
                cl_inst.node_setup(node, **setup_kwargs)
            except Exception:
                cl_inst.log.exception('Node setup failed: %s', str(node))
                status = False

            queue.put((node, status))
            queue.task_done()

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
                node, node_status = queue.get(block=True, timeout=5)
                if not node_status:
                    raise NodeSetupFailed("{node}:{node_status}".format(**locals()))
                results.append(node)
                cl_inst.log.info("(%d/%d) nodes ready, node %s. Time elapsed: %d s",
                                 len(results), len(node_list), str(node), int(time_elapsed))
            except Queue.Empty:
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


class BaseScyllaCluster(object):

    def __init__(self, *args, **kwargs):
        self.termination_event = threading.Event()
        self.nemesis = []
        self.nemesis_threads = []
        self._seed_node_rebooted = False
        self.seed_nodes_ips = None
        super(BaseScyllaCluster, self).__init__(*args, **kwargs)

    @staticmethod
    def get_node_ips_param(public_ip=True):
        if Setup.MIXED_CLUSTER:
            return 'oracle_db_nodes_public_ip' if public_ip else 'oracle_db_nodes_private_ip'
        return 'db_nodes_public_ip' if public_ip else 'db_nodes_private_ip'

    def get_scylla_args(self):
        return self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else\
            self.params.get('append_scylla_args')

    def get_seed_nodes_from_node(self):
        if self.seed_nodes_ips is None:
            node = self.nodes[0]
            yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct'),
                                         'scylla.yaml')
            node.remoter.receive_files(src=SCYLLA_YAML_PATH,
                                       dst=yaml_dst_path)
            with open(yaml_dst_path, 'r') as yaml_stream:
                conf_dict = yaml.safe_load(yaml_stream)
                try:
                    self.seed_nodes_ips = conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
                except Exception as details:
                    self.log.debug('Loaded YAML data structure: %s', conf_dict)
                    self.log.error('Scylla YAML config contents:')
                    with open(yaml_dst_path, 'r') as yaml_stream:
                        self.log.error(yaml_stream.read())
                    raise ValueError('Exception determining seed node ips: %s' %
                                     details)
        return self.seed_nodes_ips

    def enable_client_encrypt(self):
        for node in self.nodes:
            logger.debug("Enabling client encryption on node")
            node.enable_client_encrypt()

    def disable_client_encrypt(self):
        for node in self.nodes:
            logger.debug("Disabling client encryption on node")
            node.disable_client_encrypt()

    def get_seed_nodes(self):
        seed_nodes_ips = self.get_seed_nodes_from_node()
        seed_nodes = []
        for node in self.nodes:
            if node.ip_address in seed_nodes_ips:
                node.is_seed = True  # TODO: check if we need to change the is_seed, later on
                seed_nodes.append(node)
        assert seed_nodes, "We should have at least one selected seed by now"
        return seed_nodes

    def get_seed_nodes_by_flag(self):
        """
        We set is_seed at two point, before and after node_setup.
        However, we can only call this function when the flag is set.
        """

        node_ips = [node.ip_address for node
                    in self.nodes if node.is_seed]
        seeds = ",".join(node_ips)

        assert seeds, "We should have at least one selected seed by now"
        return seeds

    def _update_db_binary(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB binary...')

        seed_nodes = self.get_seed_nodes()
        non_seed_nodes = [n for n in self.nodes if not n in seed_nodes]

        def update_scylla_bin(node, queue):
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
            """.format(**locals()))
            node.remoter.run("sudo bash -ce '%s'" % prereqs_script)
            queue.put(node)
            queue.task_done()

        def stop_scylla(node, queue):
            node.stop_scylla(verify_down=True, verify_up=True)
            queue.put(node)
            queue.task_done()

        def start_scylla(node, queue):
            node.start_scylla(verify_down=True, verify_up=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        # First, stop *all* non seed nodes
        self.run_func_parallel(func=stop_scylla, node_list=non_seed_nodes)
        # First, stop *all* seed nodes
        self.run_func_parallel(func=stop_scylla, node_list=seed_nodes)
        # Then, update bin only on requested nodes
        self.run_func_parallel(func=update_scylla_bin, node_list=node_list)
        # Start all seed nodes
        self.run_func_parallel(func=start_scylla, node_list=seed_nodes)
        # Start all non seed nodes
        self.run_func_parallel(func=start_scylla, node_list=non_seed_nodes)

        time_elapsed = time.time() - start_time
        self.log.debug('Update DB binary duration -> %s s', int(time_elapsed))

    def _update_db_packages(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB packages...')

        seed_nodes = self.get_seed_nodes()
        non_seed_nodes = [n for n in self.nodes if not n in seed_nodes]

        def update_scylla_packages(node, queue):
            node.log.info('Updating DB packages')
            node.remoter.run('mkdir /tmp/scylla')
            node.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)
            node.remoter.run(r'sudo yum update -y --skip-broken -x scylla\*')
            # replace the packages
            node.remoter.run('yum list installed | grep scylla')
            node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/*', ignore_status=False, verbose=True)
            node.remoter.run('yum list installed | grep scylla')
            queue.put(node)
            queue.task_done()

        def stop_scylla(node, queue):
            node.stop_scylla(verify_down=True, verify_up=True)
            queue.put(node)
            queue.task_done()

        def start_scylla(node, queue):
            node.start_scylla(verify_down=True, verify_up=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        if len(node_list) == 1:
            # Stop only new nodes
            self.run_func_parallel(func=stop_scylla, node_list=node_list)
            # Then, update packages only on requested node
            self.run_func_parallel(func=update_scylla_packages, node_list=node_list)
            # Start new nodes
            self.run_func_parallel(func=start_scylla, node_list=node_list)
        else:
            # First, stop *all* non seed nodes
            self.run_func_parallel(func=stop_scylla, node_list=non_seed_nodes)
            # First, stop *all* seed nodes
            self.run_func_parallel(func=stop_scylla, node_list=seed_nodes)
            # Then, update packages only on requested nodes
            self.run_func_parallel(func=update_scylla_packages, node_list=node_list)
            # Start all seed nodes
            self.run_func_parallel(func=start_scylla, node_list=seed_nodes)
            # Start all non seed nodes
            self.run_func_parallel(func=start_scylla, node_list=non_seed_nodes)

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

    def get_nodetool_status(self, verification_node=None):
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
        data_centers = res.stdout.strip().split("Data center: ")
        for dc in data_centers:
            if dc:
                lines = dc.splitlines()
                dc_name = lines[0]
                status[dc_name] = {}
                for line in lines[1:]:
                    try:
                        state, ip, load, _, tokens, owns, host_id, rack = line.split()
                        node_info = {'state': state,
                                     'load': load,
                                     'tokens': tokens,
                                     'owns': owns,
                                     'host_id': host_id,
                                     'rack': rack,
                                     }
                        status[dc_name][ip] = node_info
                    except ValueError as e:
                        pass
        return status

    def check_cluster_health(self):
        for node in self.nodes:
            node.check_node_health()

        ClusterHealthValidatorEvent(type='done', name='ClusterHealthCheck', status=Severity.NORMAL,
                                    message='Cluster health check finished')

    def check_nodes_up_and_normal(self, nodes=[]):
        """Checks via nodetool that node joined the cluster and reached 'UN' state"""
        if not nodes:
            nodes = self.nodes
        status = self.get_nodetool_status()
        up_statuses = []
        for node in nodes:
            for dc, dc_status in status.iteritems():
                ip_status = dc_status.get(node.ip_address)
                if ip_status and ip_status["state"] == "UN":
                    up_statuses.append(True)
                else:
                    up_statuses.append(False)
        if not all(up_statuses):
            raise ClusterNodesNotReady("Not all nodes joined the cluster")

    @retrying(n=30, sleep_time=3, allowed_exceptions=(ClusterNodesNotReady,),
              message="Waiting for nodes to join the cluster")
    def wait_for_nodes_up_and_normal(self, nodes):
        self.check_nodes_up_and_normal(nodes=nodes)

    def get_scylla_version(self):
        if not self.nodes[0].scylla_version:
            scylla_version = self.nodes[0].get_scylla_version()

            if scylla_version:
                for node in self.nodes:
                    node.scylla_version = scylla_version

    def get_test_keyspaces(self):
        out = self.nodes[0].run_cqlsh('select keyspace_name from system_schema.keyspaces', split=True)
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
            self.log.debug("Get cfstats on the node %s for %s keyspace" % (node.name, keyspace_name))
            node_space += node.get_cfstats(keyspace_name)[key]
        self.log.debug("Current cfstats on the node %s for %s keyspaces: %s" % (node.name, keyspaces, node_space))
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
            for i in range(nem['num_threads']):
                self.nemesis.append(nem['nemesis'](tester_obj=tester_obj,
                                                   termination_event=self.termination_event))

    def clean_nemesis(self):
        self.nemesis = []

    @log_run_info("Start nemesis threads on cluster")
    def start_nemesis(self, interval=30):
        for nemesis in self.nemesis:
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval, ), verbose=True)
            nemesis_thread.daemon = True
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)

    @log_run_info("Stop nemesis threads on cluster")
    def stop_nemesis(self, timeout=10):
        if self.termination_event.isSet():
            return
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(timeout)
        self.nemesis_threads = []

    def node_config_setup(self, node, seed_address, endpoint_snitch):
        node.config_setup(seed_address=seed_address, cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'), endpoint_snitch=endpoint_snitch,
                          authenticator=self.params.get('authenticator'),
                          server_encrypt=self._param_enabled('server_encrypt'),
                          client_encrypt=self._param_enabled('client_encrypt'),
                          append_conf=self.params.get('append_conf'), append_scylla_args=self.get_scylla_args(),
                          hinted_handoff=self.params.get('hinted_handoff'),
                          authorizer=self.params.get('authorizer'))

    def node_setup(self, node, verbose=False, timeout=3600):
        """
        Install, configure and run scylla on node
        :param node: scylla node object
        :param verbose:
        """
        node.wait_ssh_up(verbose=verbose)
        # update repo cache and system after system is up
        node.update_repo_cache()
        if node.init_system == 'systemd' and (node.is_ubuntu() or node.is_debian()):
            node.remoter.run('sudo systemctl disable apt-daily.timer')
            node.remoter.run('sudo systemctl disable apt-daily-upgrade.timer')
            node.remoter.run('sudo systemctl stop apt-daily.timer', ignore_status=True)
            node.remoter.run('sudo systemctl stop apt-daily-upgrade.timer', ignore_status=True)
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = self.get_seed_nodes_by_flag()

        if not Setup.REUSE_CLUSTER:
            node.clean_scylla()
            node.install_scylla(scylla_repo=self.params.get('scylla_repo'))
            node.install_scylla_debuginfo()

            if Setup.MULTI_REGION:
                if not endpoint_snitch:
                    endpoint_snitch = 'GossipingPropertyFileSnitch'
                node.datacenter_setup(self.datacenter)

            self.node_config_setup(node, seed_address, endpoint_snitch)
            try:
                disks = node.detect_disks(nvme=True)
            except AssertionError:
                disks = node.detect_disks(nvme=False)
            node.scylla_setup(disks)

            seed_address_list = seed_address.split(',')
            if node.ip_address not in seed_address_list:
                wait.wait_for(func=lambda: self._seed_node_rebooted is True,
                              step=30,
                              text='Wait for seed node to be up after reboot')
            node.restart()
            node.wait_ssh_up()

            if node.ip_address in seed_address_list:
                self.log.info('Seed node is up after reboot')
                self._seed_node_rebooted = True

            self.log.info('io.conf right after reboot')
            node.remoter.run('sudo cat /etc/scylla.d/io.conf')

        node.wait_db_up(timeout=timeout)
        node.wait_jmx_up()

    def clean_replacement_node_ip(self, node, seed_address, endpoint_snitch):
        if node.replacement_node_ip:
            # If this is a replacement node, we need to set back configuration in case
            # when scylla-server process will be restarted
            node.replacement_node_ip = None
            self.node_config_setup(node, seed_address, endpoint_snitch)

    @staticmethod
    def verify_logging_from_nodes(nodes_list):
        for node in nodes_list:
            if not os.path.exists(node.database_log):
                error_msg = "No db log from node [%s] " % node
                logger.warning(error_msg)
                raise Exception(error_msg)
        return True

    @wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):
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
        self.get_seed_nodes()
        self.get_scylla_version()
        self.set_traffic_control()
        wait.wait_for(self.verify_logging_from_nodes, nodes_list=node_list,
                      text="wait for db logs", step=20, timeout=300, throw_exc=True)

        self.log.info("All DB nodes configured and stated. ScyllaDB status:\n%s" % self.nodes[0].check_node_health())

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

    def collect_logs(self, storing_dir):
        storing_dir = os.path.join(storing_dir, os.path.basename(self.logdir))
        os.mkdir(storing_dir)

        files_to_archive = ['/proc/meminfo', '/proc/cpuinfo', '/proc/interrupts', '/proc/vmstat', '/etc/scylla/scylla.yaml']
        for node in self.nodes:
            storing_dir_for_node = os.path.join(storing_dir, node.name)
            src_dir = node.prepare_files_for_archive(files_to_archive)
            node.receive_files(src=src_dir, dst=storing_dir)
            with open(os.path.join(storing_dir_for_node, 'installed_pkgs'), 'w') as pkg_list_file:
                pkg_list_file.write(node.get_installed_packages())
            with open(os.path.join(storing_dir_for_node, 'console_output'), 'w') as co:
                co.write(node.get_console_output())
            with open(os.path.join(storing_dir_for_node, 'console_screenshot.jpg'), 'wb') as cscrn:
                imagedata = node.get_console_screenshot()
                cscrn.write(decodestring(imagedata))
            if os.path.exists(node.database_log):
                shutil.copy(node.database_log, storing_dir_for_node)
        return storing_dir


class BaseLoaderSet(object):

    def __init__(self, params):
        self._loader_cycle = None
        self.params = params
        self._gemini_version = None

    @property
    def gemini_version(self):
        result = self.nodes[0].remoter.run('cd $HOME; ./gemini --version')  # pylint: disable=no-member
        # result : gemini version 1.0.1, commit ef7c6f422c78ef6b84a6f3bccf52ea9ec846bba0, date 2019-05-16T09:56:16Z
        # take only version number - 1.0.1
        self._gemini_version = result.stdout.split(',')[0].split(' ')[2]
        return self._gemini_version

    def install_gemini(self, node):
        gemini_version = self.params.get('gemini_version', default='0.9.2')
        if gemini_version.lower() == 'latest':
            gemini_version = get_latest_gemini_version()

        gemini_url = 'http://downloads.scylladb.com/gemini/{0}/gemini_{0}_Linux_x86_64.tar.gz'.format(gemini_version)
        # TODO: currently schema is not used by gemini tool need to store the schema
        #       in data_dir for each test
        gemini_schema_url = self.params.get('gemini_schema_url')
        if not gemini_url or not gemini_schema_url:
            logger.warning('Gemini URLs should be defined to run the gemini tool')
        else:
            gemini_tar = os.path.basename(gemini_url)
            install_gemini_script = dedent("""
                cd $HOME
                rm -rf gemini*
                curl -LO {gemini_url}
                tar -xvf {gemini_tar}
                chmod a+x gemini
                curl -LO  {gemini_schema_url}
            """.format(**locals()))
            node.remoter.run("bash -cxe '%s'" % install_gemini_script)
            logger.debug('Gemini version {}'.format(self.gemini_version))

    def node_setup(self, node, verbose=False, db_node_address=None, **kwargs):
        self.log.info('Setup in BaseLoaderSet')
        node.wait_ssh_up(verbose=verbose)
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
            install_java_script = dedent("""
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

    @wait_for_init_wrap
    def wait_for_init(self, verbose=False, db_node_address=None):
        pass

    @staticmethod
    def get_node_ips_param(public_ip=True):
        return 'loaders_public_ip' if public_ip else 'loaders_private_ip'

    def get_loader(self):

        if not self._loader_cycle:
            self._loader_cycle = itertools.cycle([node for node in self.nodes])
        return next(self._loader_cycle)

    def kill_stress_thread(self):
        for loader in self.nodes:
            try:
                loader.remoter.run(cmd='pgrep -f cassandra-stress | xargs -I{}  kill -TERM -{}', ignore_status=True)
            except Exception as ex:
                self.log.warning("failed to kill stress-command on [%s]: [%s]", str(loader), str(ex))

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
            line.strip()
            if not line:
                continue
            # Parse loader & cpu info
            if line.startswith('TAG:'):
                ret = re.findall("TAG: loader_idx:(\d+)-cpu_idx:(\d+)-keyspace_idx:(\d+)", line)
                results['loader_idx'] = ret[0][0]
                results['cpu_idx'] = ret[0][1]
                results['keyspace_idx'] = ret[0][2]

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
            m = re.findall('.*READ:(\d+), WRITE:(\d+)]', line)
            if m:  # parse results for mixed workload
                results['%s read' % key] = m[0][0]
                results['%s write' % key] = m[0][1]

        if not enable_parse:
            logger.warning('Cannot find summary in c-stress results: %s', lines[-10:])
            return {}
        return results

    @staticmethod
    def _parse_bench_summary(lines):
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
                value = float(unicode(value).rstrip('ms'))
            except UnicodeDecodeError:
                value = float(unicode(value, errors='ignore').rstrip('s')) / 1000  # convert to milliseconds
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

    def get_stress_results_bench(self, queue):
        results = []
        ret = []
        self.log.debug('Wait for %s bench stress threads results', queue[TASK_QUEUE].qsize())
        queue[TASK_QUEUE].join()
        while not queue[RES_QUEUE].empty():
            results.append(queue[RES_QUEUE].get())

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            ret.append(self._parse_bench_summary(lines))

        return ret

    def run_stress_thread_bench(self, stress_cmd, timeout, output_dir, node_list=[]):
        queue = {TASK_QUEUE: Queue.Queue(), RES_QUEUE: Queue.Queue()}

        def node_run_stress_bench(node, loader_idx, stress_cmd, node_list):
            queue[TASK_QUEUE].put(node)

            CassandraStressEvent(type='start', node=str(node), stress_cmd=stress_cmd)

            logdir = os.path.join(output_dir, self.name)
            try:
                os.makedirs(logdir)
            except OSError:
                pass

            log_file_name = os.path.join(logdir,
                                         'scylla-bench-l%s-%s.log' %
                                         (loader_idx, uuid.uuid4()))
            ips = ",".join([n.private_ip_address for n in node_list])
            bench_log = tempfile.NamedTemporaryFile(prefix='scylla-bench-', suffix='.log').name
            result = node.remoter.run(cmd="/$HOME/go/bin/{} -nodes {} | tee {}".format(
                                      stress_cmd.strip(), ips, bench_log),
                                      timeout=timeout,
                                      ignore_status=True,
                                      log_file=log_file_name)

            CassandraStressEvent(type='finish', node=str(node), stress_cmd=stress_cmd, log_file_name=log_file_name)

            queue[RES_QUEUE].put((node, result))
            queue[TASK_QUEUE].task_done()

        for loader_idx, loader in enumerate(self.nodes):
            setup_thread = threading.Thread(target=node_run_stress_bench,
                                            args=(loader, loader_idx,
                                                  stress_cmd, node_list))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        return queue

    def kill_stress_thread_bench(self):
        for loader in self.nodes:
            sb_active = loader.remoter.run(cmd='pgrep -f scylla-bench', ignore_status=True)
            if sb_active.exit_status == 0:
                kill_result = loader.remoter.run('pkill -f -TERM scylla-bench', ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate scylla-bench on node %s:\n%s', loader, kill_result)

    def kill_gemini_thread(self):
        for loader in self.nodes:
            sb_active = loader.remoter.run(cmd='pgrep -f gemini', ignore_status=True)
            if sb_active.exit_status == 0:
                kill_result = loader.remoter.run('pkill -f -TERM gemini', ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate gemini on node %s:\n%s', loader, kill_result)

    def collect_logs(self, storing_dir):
        storing_dir = os.path.join(storing_dir, os.path.basename(self.logdir))
        os.mkdir(storing_dir)

        for node in self.nodes:
            storing_dir_for_node = os.path.join(storing_dir, node.name)
            if os.path.exists(node.logdir):
                shutil.copytree(node.logdir, storing_dir_for_node)
        return storing_dir


class BaseMonitorSet(object):
    # This is a Mixin for monitoring cluster and should not be inherited
    def __init__(self, targets, params):
        self.targets = targets
        self.params = params
        self.local_metrics_addr = start_metrics_server()  # start prometheus metrics server locally and return local ip
        self.sct_ip_port = self.set_local_sct_ip()
        self.grafana_port = 3000
        self.monitor_branch = self.params.get('monitor_branch', default='branch-2.4')
        self._monitor_install_path_base = None
        self.phantomjs_installed = False

    @property
    def monitor_install_path_base(self):
        if not self._monitor_install_path_base:
            self._monitor_install_path_base = self.nodes[0].remoter.run("echo $HOME").stdout.strip()
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
        self.log.debug("Using %s ScyllaDB version to derive monitoring version" % self.scylla_version)
        version = re.match(r"(\d+\.\d+)", self.scylla_version)
        if not version:
            return 'master'
        else:
            return version.group(1)

    @property
    def is_enterprise(self):
        return self.targets["db_cluster"].nodes[0].is_enterprise

    def node_setup(self, node, **kwargs):
        self.log.info('Setup in BaseMonitorSet')
        node.wait_ssh_up()
        # update repo cache and system after system is up
        node.update_repo_cache()

        if Setup.REUSE_CLUSTER:
            self.configure_scylla_monitoring(node)
            self.restart_scylla_monitoring(sct_metrics=True)
            EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'].set_grafana_url(
                "http://{0.external_address}:{1.grafana_port}".format(node, self))
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
        EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'].set_grafana_url("http://{0.external_address}:{1.grafana_port}".format(node, self))
        if node.is_rhel_like():
            node.remoter.run('sudo yum install screen -y')
        else:
            node.remoter.run('sudo apt-get install screen -y')
        self.install_scylla_manager(node)

    def install_scylla_manager(self, node):
        if self.params.get('use_mgmt', default=None):
            node.install_mgmt(scylla_repo=self.params.get('scylla_repo_m'), scylla_mgmt_repo=self.params.get('scylla_mgmt_repo'))

    def configure_ngrok(self, ngrok_name):
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
            return self.configure_ngrok(ngrok_name)

        sct_public_ip = self.params.get('sct_public_ip')
        if sct_public_ip:
            return sct_public_ip + ':' + self.local_metrics_addr.split(':')[1]
        else:
            return self.local_metrics_addr

    @wait_for_init_wrap
    def wait_for_init(self):
        pass

    def install_scylla_monitoring_prereqs(self, node):
        if node.is_rhel_like():
            node.remoter.run("sudo yum install -y epel-release")
            node.update_repo_cache()
            prereqs_script = dedent("""
                yum install -y unzip wget
                easy_install pip
                pip install --upgrade pip
                pip install pyyaml
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
                apt-get install -y python-setuptools unzip wget
                easy_install pip
                pip install --upgrade pip
                pip install pyyaml
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
            node.remoter.run(cmd="sudo apt-get install -y python-setuptools wget")
            node.remoter.run(cmd="sudo apt-get install -y unzip")
            prereqs_script = dedent("""
                easy_install pip
                pip install --upgrade pip
                pip install pyyaml
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

    def configure_scylla_monitoring(self, node, sct_metrics=True, alert_manager=True):
        db_targets_list = [n.ip_address for n in self.targets["db_cluster"].nodes]  # node exporter + scylladb
        if sct_metrics:
            temp_dir = tempfile.mkdtemp()
            template_fn = "prometheus.yml.template"
            prometheus_yaml_template = os.path.join(self.monitor_install_path, "prometheus", template_fn)
            local_template_tmp = os.path.join(temp_dir, template_fn + ".orig")
            local_template = os.path.join(temp_dir, template_fn)
            node.remoter.receive_files(src=prometheus_yaml_template,
                                       dst=local_template_tmp)
            with open(local_template_tmp) as f:
                templ_yaml = yaml.load(f, Loader=yaml.SafeLoader)  # to override avocado
                self.log.debug("Configs %s" % templ_yaml)
            loader_targets_list = ["%s:9103" % n.ip_address for n in self.targets["loaders"].nodes]

            # remove those jobs if exists, for support of 'reuse_cluster: true'
            def remove_sct_metrics(x):
                return x['job_name'] not in ['stress_metrics', 'sct_metrics']
            templ_yaml["scrape_configs"] = filter(remove_sct_metrics, templ_yaml["scrape_configs"])

            scrape_configs = templ_yaml["scrape_configs"]
            scrape_configs.append(dict(job_name="stress_metrics", honor_labels=True,
                                       static_configs=[dict(targets=loader_targets_list)]))

            if self.params.get('gemini_cmd', None):
                gemini_loader_targets_list = ["%s:2112" % n.ip_address for n in self.targets["loaders"].nodes]
                scrape_configs.append(dict(job_name="gemini_metrics", honor_labels=True,
                                           static_configs=[dict(targets=gemini_loader_targets_list)]))

            if self.sct_ip_port:
                scrape_configs.append(dict(job_name="sct_metrics", honor_labels=True,
                                           static_configs=[dict(targets=[self.sct_ip_port])]))
            with open(local_template, "w") as fo:
                yaml.safe_dump(templ_yaml, fo, default_flow_style=False)  # to remove tag !!python/unicode
            node.send_files(src=local_template, dst=prometheus_yaml_template, delete_dst=True, verbose=True)
            localrunner.run("rm -rf {temp_dir}".format(**locals()))
        self._monitoring_targets = " ".join(db_targets_list)
        configure_script = dedent("""
            cd {0.monitor_install_path}
            mkdir -p {0.monitoring_conf_dir}
            ./genconfig.py -s -n -d {0.monitoring_conf_dir} {0._monitoring_targets}
        """.format(self))
        node.remoter.run("bash -ce '%s'" % configure_script, verbose=True)
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
        with tempfile.NamedTemporaryFile("w", bufsize=0) as f:
            f.write(conf)
            node.send_files(src=f.name, dst=f.name)
            node.remoter.run("bash -ce 'cat %s >> %s'" % (f.name, alertmanager_conf_file))

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
            db_targets_list = [n.ip_address for n in self.targets["db_cluster"].nodes]  # node exporter + scylladb
            self._monitoring_targets = " ".join(db_targets_list)
            configure_script = dedent("""
                        cd {0.monitor_install_path}
                        mkdir -p {0.monitoring_conf_dir}
                        ./genconfig.py -s -n -d {0.monitoring_conf_dir} {0._monitoring_targets}
                    """.format(self))
            node.remoter.run("sudo bash -ce '%s'" % configure_script, verbose=True)

    def start_scylla_monitoring(self, node):
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

    def add_sct_dashboards_to_grafana(self, node):
        sct_dashboard_json = "scylla-dash-per-server-nemesis.{0.monitoring_version}.json".format(self)

        def _register_grafana_json(json_filename):
            url = "http://{0.external_address}:{1.grafana_port}/api/dashboards/db".format(node, self)
            json_path = get_data_dir_path(json_filename)
            result = localrunner.run('curl -XPOST -i %s --data-binary @%s -H "Content-Type: application/json"' %
                                     (url, json_path))
            return result.exited == 0

        wait.wait_for(_register_grafana_json, step=10,
                      text="Waiting to register 'data_dir/%s'..." % sct_dashboard_json,
                      json_filename=sct_dashboard_json)

    def get_sct_dashboards_config(self, node):
        sct_dashboard_json_filename = "scylla-dash-per-server-nemesis.{0.monitoring_version}.json".format(self)

        return get_data_dir_path(sct_dashboard_json_filename)

    def download_monitoring_data_dir(self, node):
        try:
            local_monitoring_data_dir = os.path.join(node.logdir, "monitoring_data")
            node.remoter.receive_files(src=self.monitoring_data_dir, dst=local_monitoring_data_dir)
            return local_monitoring_data_dir
        except Exception as e:
            self.log.error("Unable to downlaod Prometheus data: %s" % e)
            return ""

    @log_run_info
    def install_scylla_monitoring(self, node):
        self.install_scylla_monitoring_prereqs(node)
        self.download_scylla_monitoring(node)

    def get_grafana_annotations(self, node):
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        try:
            res = requests.get(annotations_url.format(node_ip=node.external_address, grafana_port=self.grafana_port))
            if res.ok:
                return res.text
        except Exception as ex:
            logger.warning("unable to get grafana annotations [%s]", str(ex))

    def set_grafana_annotations(self, node, annotations_data):
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        res = requests.post(annotations_url.format(node_ip=node.external_address, grafana_port=self.grafana_port),
                            data=annotations_data, headers={'Content-Type': 'application/json'})
        logger.info("posting annotations result: %s", res)

    def stop_scylla_monitoring(self, node):
        kill_script = dedent("""
            cd {0.monitor_install_path}
            ./kill-all.sh
        """.format(self))
        node.remoter.run("bash -ce '%s'" % kill_script)

    def install_phantom_js(self):
        if not self.phantomjs_installed:
            phantomjs_base = "phantomjs-2.1.1-linux-x86_64"
            phantomjs_tar = "{phantomjs_base}.tar.bz2".format(**locals())
            phantomjs_url = "https://bitbucket.org/ariya/phantomjs/downloads/{phantomjs_tar}".format(**locals())
            install_phantom_js_script = dedent("""
                rm -rf {phantomjs_base}*
                curl {phantomjs_url} -o {phantomjs_tar} -L
                tar xvfj {phantomjs_tar}
            """.format(**locals()))
            localrunner.run("bash -ce '%s'" % install_phantom_js_script)
            localrunner.run("cd phantomjs-2.1.1-linux-x86_64 && sed -e 's/200);/10000);/' examples/rasterize.js |grep -v 'use strict' > r.js")
            self.phantomjs_installed = True
        else:
            self.log.debug("PhantomJS is already installed!")

    def get_grafana_screenshot_and_snapshot(self, test_start_time=None):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        if not test_start_time:
            self.log.error("No start time for test")
            return
        start_time = str(test_start_time).split('.')[0] + '000'

        screenshot_names = [
            {
                'name': 'per-server-metrics-nemesis',
                'path': 'dashboard/db/scylla-{scr_name}-{version}'},
            {
                'name': 'overview-metrics',
                'path': 'd/overview-{version}/scylla-{scr_name}'
            }
        ]

        screenshot_url_tmpl = "http://{node_ip}:{grafana_port}/{path}?from={st}&to=now"

        try:
            self.install_phantom_js()
            for n, node in enumerate(self.nodes):
                screenshots = []
                snapshots = []
                for i, screenshot in enumerate(screenshot_names):
                    version = self.monitoring_version.replace('.', '-')
                    path = screenshot['path'].format(
                        version=version,
                        scr_name=screenshot['name'])
                    grafana_url = screenshot_url_tmpl.format(
                        node_ip=node.external_address,
                        grafana_port=self.grafana_port,
                        path=path,
                        st=start_time)
                    datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
                    snapshot_path = os.path.join(node.logdir,
                                                 "grafana-screenshot-%s-%s-%s.png" % (screenshot['name'], datetime_now, n))

                    screenshots.append(self._get_screenshot_link(grafana_url, snapshot_path))
                    snapshots.append(self._get_shared_snapshot_link(grafana_url))

                return {'screenshots': screenshots, 'snapshots': snapshots}

        except Exception as details:
            self.log.error('Error taking monitor snapshot: %s',
                           str(details))
        return {}

    def upload_annotations_to_s3(self):
        annotations_url = ''
        if not self.nodes:
            return annotations_url
        try:
            annotations = self.get_grafana_annotations(self.nodes[0])
            if annotations:
                annotations_url = S3Storage().generate_url('annotations.json', Setup.test_id())
                logger.info("Uploading 'annotations.json' to {s3_url}".format(s3_url=annotations_url))
                response = requests.put(annotations_url, data=annotations)
                response.raise_for_status()
        except Exception:
            logger.exception("failed to upload annotations to S3")

        return annotations_url

    def _get_screenshot_link(self, grafana_url, snapshot_path):
        localrunner.run("cd phantomjs-2.1.1-linux-x86_64 && bin/phantomjs r.js \"%s\" \"%s\" 1920px" % (
                        grafana_url, snapshot_path))
        return S3Storage().upload_file(snapshot_path, dest_dir=Setup.test_id())

    def _get_shared_snapshot_link(self, grafana_url):
        result = localrunner.run("cd phantomjs-2.1.1-linux-x86_64 && bin/phantomjs ../data_dir/share_snapshot.js \"%s\"" % (grafana_url))
        # since there is only one monitoring node returning here
        output = result.stdout.strip()
        if "Error" in output:
            self.log.info(output)
            return ""
        else:
            self.log.info("Shared grafana snapshot: {}".format(output))
            return output

    @log_run_info
    def download_monitor_data(self):
        for node in self.nodes:
            try:
                snapshot_archive = self.get_prometheus_snapshot(node)
                self.log.debug('Snapshot local path: {}'.format(snapshot_archive))

                return S3Storage().upload_file(snapshot_archive, dest_dir=Setup.test_id())
            except Exception, details:
                self.log.error('Error downloading prometheus data dir: %s',
                               str(details))
                return ""

    def download_scylla_manager_log(self):
        for node in self.nodes:
            try:
                scylla_mgmt_log_path = node.collect_mgmt_log()
                scylla_manager_local_log_path = os.path.join(node.logdir, 'scylla_manager_log')
                node.receive_files(src=scylla_mgmt_log_path, dst=scylla_manager_local_log_path)
                shutil.make_archive(scylla_manager_local_log_path, 'zip', scylla_mgmt_log_path)
                link = S3Storage().upload_file("{}.zip".format(scylla_manager_local_log_path), dest_dir=Setup.test_id())
                self.log.info("Scylla manager log uploaded {}".format(link))
            except Exception, details:
                self.log.error('Error downloading scylla manager log : %s',
                               str(details))

    def collect_scylla_monitoring_logs(self, node):
        log_paths = []
        monitoring_dockers = self._get_running_monitoring_dockers(node)
        for m_docker in monitoring_dockers:
            m_docker_log_path = self._collect_monitoring_docker_log(node, m_docker)
            if m_docker_log_path:
                log_paths.append(m_docker_log_path)

        return log_paths

    def _get_running_monitoring_dockers(self, node):
        monitoring_dockers = []
        result = node.remoter.run("docker ps -a --format {{.Names}}", ignore_status=True)
        if result.exit_status == 0:
            monitoring_dockers = [docker.strip() for docker in result.stdout.split('\n') if docker.strip()]
        else:
            self.log.warning("Getting running docker containers failed: {}".format(result))

        self.log.debug("Next monitoring dockers are running: {}".format(monitoring_dockers))
        return monitoring_dockers

    def _collect_monitoring_docker_log(self, node, docker):
        log_path = "/tmp/{}.log".format(docker)
        cmd = "docker logs --details -t {} > {}".format(docker, log_path)
        result = node.remoter.run(cmd, ignore_status=True)
        if result.exit_status == 0:
            return log_path
        else:
            self.log.warning("Collecting {} log failed {}".format(docker, result))
            return ""

    def collect_logs(self, storing_dir):
        storing_dir = os.path.join(storing_dir, os.path.basename(self.logdir))
        os.mkdir(storing_dir)

        for node in self.nodes:
            scylla_mgmt_log = node.collect_mgmt_log()
            node.receive_files(src=scylla_mgmt_log, dst=storing_dir)
            monitoring_dockers_logs = self.collect_scylla_monitoring_logs(node)
            node.receive_files(src=monitoring_dockers_logs, dst=storing_dir)

        return storing_dir

    @retrying(n=3, sleep_time=10, allowed_exceptions=(PrometheusSnapshotErrorException, ), message='Create prometheus snapshot')
    def create_prometheus_snapshot(self, node):
        ps = PrometheusDBStats(host=node.external_address)
        result = ps.create_snapshot()
        if result and "success" in result['status']:
            snapshot_dir = os.path.join(self.monitoring_data_dir,
                                        "snapshots",
                                        result['data']['name'])
            return snapshot_dir
        else:
            raise PrometheusSnapshotErrorException(result)

    def get_prometheus_snapshot(self, node):
        try:
            snapshot_dir = self.create_prometheus_snapshot(node)
        except PrometheusSnapshotErrorException as details:
            self.log.warning('Create prometheus snapshot failed {}.\nUse prometheus data directory'.format(details))
            snapshot_dir = self.monitoring_data_dir

        archive_snapshot_name = '/tmp/prometheus_snapshot-{}.tar.gz'.format(self.shortid)
        result = node.remoter.run('cd {}; tar -czvf {} .'.format(snapshot_dir,
                                                                 archive_snapshot_name),
                                  ignore_status=True)
        if result.exit_status > 0:
            self.log.warning('Unable to create archive {}'.format(result))
            return ""

        try:
            local_snapshot_data_dir = os.path.join(node.logdir, "prometheus_data")

            if not os.path.exists(local_snapshot_data_dir):
                os.mkdir(local_snapshot_data_dir)

            local_archive_path = os.path.join(local_snapshot_data_dir,
                                              os.path.basename(archive_snapshot_name))
            if os.path.exists(local_archive_path):
                self.log.info('Remove previous file {}'.format(local_archive_path))
                os.remove(local_archive_path)

            node.remoter.receive_files(src=archive_snapshot_name, dst=local_snapshot_data_dir)
            return local_archive_path

        except Exception as e:
            self.log.error("Unable to downlaod Prometheus data: %s" % e)
            return ""

    def get_monitoring_data_stack(self, node):
        archive_name = "monitoring_data_stack_{0.monitor_branch}_{0.monitoring_version}.tar.gz".format(self)

        sct_monitoring_addons_dir = os.path.join(self.monitor_install_path, 'sct_monitoring_addons')

        node.remoter.run('mkdir -p {}'.format(sct_monitoring_addons_dir))
        sct_dashboard_file = self.get_sct_dashboards_config(node)
        node.remoter.send_files(src=sct_dashboard_file, dst=sct_monitoring_addons_dir)

        annotations_json = self.get_grafana_annotations(node)
        if annotations_json:
            tmp_dir = tempfile.mkdtemp()
            with io.open(os.path.join(tmp_dir, 'annotations.json'), 'w', encoding='utf-8') as f:
                f.write(annotations_json)
            node.remoter.send_files(src=os.path.join(tmp_dir, 'annotations.json'), dst=sct_monitoring_addons_dir)

        node.remoter.run("cd {}; tar -czvf {} {}/".format(self.monitor_install_path_base,
                                                          archive_name,
                                                          os.path.basename(self.monitor_install_path)),
                         ignore_status=True)
        node.receive_files(src=os.path.join(self.monitor_install_path_base, archive_name),
                           dst=self.logdir)
        return os.path.join(self.logdir, archive_name)

    def download_monitoring_data_stack(self):
        for node in self.nodes:
            local_path_to_monitor_stack = self.get_monitoring_data_stack(node)
            self.log.info('Path to monitoring stack {}'.format(local_path_to_monitor_stack))

            return S3Storage().upload_file(local_path_to_monitor_stack, dest_dir=Setup.test_id())


class NoMonitorSet(object):

    def __init__(self):

        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
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

    def collect_logs(self, storing_dir):
        pass
