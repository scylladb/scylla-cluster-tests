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

from base64 import decodestring

from sdcm.mgmt import ScyllaManagerError
from sdcm.prometheus import start_metrics_server
from textwrap import dedent
from avocado.utils import path
from avocado.utils import process
from avocado.utils import script
from datetime import datetime
from .log import SDCMAdapter
from .remote import Remote, RemoteFabric
from .remote import disable_master_ssh
from . import wait
from .utils import log_run_info, retrying, get_data_dir_path, Distro, verify_scylla_repo_file, S3Storage
from .loader import CassandraStressExporterSetup

from avocado.utils import runtime as avocado_runtime

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
# max limit of coredump file can be uploaded(5 GB)
COREDUMP_MAX_SIZE = 1024 * 1024 * 1024 * 5
IP_SSH_CONNECTIONS = 'private'
TASK_QUEUE = 'task_queue'
RES_QUEUE = 'res_queue'
WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SCYLLA_YAML_PATH = "/etc/scylla/scylla.yaml"
SCYLLA_DIR = "/var/lib/scylla"


logger = logging.getLogger(__name__)


def set_ip_ssh_connections(ip_type):
    global IP_SSH_CONNECTIONS
    IP_SSH_CONNECTIONS = ip_type


def set_duration(duration):
    global TEST_DURATION
    TEST_DURATION = duration
    if TEST_DURATION >= 3 * 60:
        disable_master_ssh()


def set_libvirt_uri(libvirt_uri):
    global LIBVIRT_URI
    LIBVIRT_URI = libvirt_uri


def clean_domain(domain_name):
    global LIBVIRT_URI
    process.run('virsh -c %s destroy %s' % (LIBVIRT_URI, domain_name),
                ignore_status=True)

    process.run('virsh -c %s undefine %s' % (LIBVIRT_URI, domain_name),
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
    REMOTE = Remote

    _test_id = None

    TAGS = dict()

    @classmethod
    def test_id(cls):
        if not cls._test_id:
            cls._test_id = uuid.uuid4()
        return cls._test_id

    @classmethod
    def set_test_id(cls, new_test_id):
        if new_test_id:
            cls._test_id = new_test_id

    @classmethod
    def set_multi_region(cls, multi_region):
        cls.MULTI_REGION = multi_region

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
    def set_remote_runner(cls, val='Remote'):
        if 'fabric' in val.lower():
            cls.REMOTE = RemoteFabric


def create_common_tags():
    username = os.environ.get('BUILD_USER', getpass.getuser())
    build_tag = os.environ.get('BUILD_TAG', None)
    tags = dict(RunByUser=username,
                TestName=str(avocado_runtime.CURRENT_TEST.name),
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


def prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, key_prefix='keypair', user_prefix=None):
        self.type = 'generated'
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        self.service = service
        key_prefix = prepend_user_prefix(user_prefix, key_prefix)
        self.name = '%s-%s' % (key_prefix, self.shortid)
        try:
            self.key_pair = self.service.create_key_pair(KeyName=self.name)
        except TypeError:
            self.key_pair = self.service.create_key_pair(name=self.name)
        self.key_pair_name = self.key_pair.name
        self.key_file = os.path.join(tempfile.gettempdir(),
                                     '%s.pem' % self.name)
        self.write_key_file()
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Created')

    def __str__(self):
        return "Key Pair %s -> %s" % (self.name, self.key_file)

    def write_key_file(self):
        # key_material is the attribute for boto3
        attr = 'key_material'
        if hasattr(self.key_pair, 'private_key'):
            # private_key is the attribute for libcloud
            attr = 'private_key'
        with open(self.key_file, 'w') as key_file_obj:
            key_file_obj.write(getattr(self.key_pair, attr))
        os.chmod(self.key_file, 0o400)

    def destroy(self):
        if hasattr(self.key_pair, 'delete'):
            self.key_pair.delete()
        else:
            self.service.delete_key_pair(self.key_pair)
        try:
            remove_if_exists(self.key_file)
        except OSError:
            pass
        self.log.info('Destroyed')


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

    def __init__(self, name, ssh_login_info=None, base_logdir=None, node_prefix=None, dc_idx=0):
        self.name = name
        self.is_seed = None
        self.dc_idx = dc_idx
        try:
            self.logdir = path.init_dir(base_logdir, self.name)
        except OSError:
            self.logdir = os.path.join(base_logdir, self.name)

        self._public_ip_address = None
        self._private_ip_address = None
        ssh_login_info['hostname'] = self.ip_address()

        self.remoter = Setup.REMOTE(**ssh_login_info)
        self._ssh_login_info = ssh_login_info
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug(self.remoter.ssh_debug_cmd())

        self._journal_thread = None
        self.n_coredumps = 0
        self._backtrace_thread = None
        self._sct_log_formatter_installed = False
        self._init_system = None

        self.database_log = os.path.join(self.logdir, 'database.log')
        self._database_log_errors_index = []
        self._database_error_patterns = ['std::bad_alloc', 'integrity check failed']
        self.termination_event = threading.Event()
        self.start_task_threads()
        # We should disable bootstrap when we create nodes to establish the cluster,
        # if we want to add more nodes when the cluster already exists, then we should
        # enable bootstrap.
        self.enable_auto_bootstrap = False
        self.scylla_version = ''
        self.is_enterprise = None
        self.replacement_node_ip = None  # if node is a replacement for a dead node, store dead node private ip here
        self._distro = None

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

    def is_ubuntu(self):
        return self.distro == Distro.UBUNTU16 or self.distro == Distro.UBUNTU14

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
        if self.is_enterprise is None:
            if self.is_rhel_like():
                result = self.remoter.run("sudo yum search scylla-enterprise", ignore_status=True)
                self.is_enterprise = True if ('scylla-enterprise.x86_64' in result.stdout or
                                              'No matches found' not in result.stdout) else False
            else:
                result = self.remoter.run("sudo apt-cache search scylla-enterprise", ignore_status=True)
                self.is_enterprise = True if 'scylla-enterprise' in result.stdout else False

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
    def public_ip_address(self):
        return self._public_ip_address

    @property
    def private_ip_address(self):
        return self._private_ip_address

    def ip_address(self):
        if IP_SSH_CONNECTIONS == 'public' or Setup.MULTI_REGION:
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

    def retrieve_journal(self):
        try:
            if self.init_system == 'systemd':
                # Here we're assuming that journalctl systems are Scylla images
                db_services_log_cmd = ('journalctl -f --no-tail --no-pager '
                                       '-u scylla-ami-setup.service '
                                       '-u scylla-io-setup.service '
                                       '-u scylla-server.service '
                                       '-u scylla-jmx.service '
                                       '-o json | /var/tmp/sct_log_formatter')
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

    def install_sct_log_formatter(self):
        result = self.remoter.run('test -e /var/tmp/sct_log_formatter', verbose=False, ignore_status=True)
        if result.exit_status != 0:
            sct_log_formatter = get_data_dir_path('sct_log_formatter')
            self.remoter.send_files(src=sct_log_formatter, dst='/var/tmp')
            self.remoter.run('chmod +x /var/tmp/sct_log_formatter')
            self._sct_log_formatter_installed = True

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

    def run_nodetool(self, subcmd="status"):
        result = self.remoter.run(cmd="nodetool %s" % subcmd, timeout=60, verbose=True)
        return result.stdout

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
        while True:
            if self.termination_event.isSet():
                break
            self.wait_ssh_up(verbose=False)
            self.retrieve_journal()

    def start_journal_thread(self):
        self._journal_thread = threading.Thread(target=self.journal_thread)
        self._journal_thread.start()

    def _get_coredump_backtraces(self, last=True):
        """
        Get coredump backtraces.

        :param last: Whether to only show the last backtrace.
        :return: process.CmdResult output
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
        base_upload_url = 'upload.scylladb.com/%s/%s'
        coredump_id = os.path.basename(coredump)[:-3]
        upload_url = base_upload_url % (coredump_id, os.path.basename(coredump))
        self.log.info('Uploading coredump %s to %s' % (coredump, upload_url))
        self.remoter.run("sudo curl --request PUT --upload-file "
                         "'%s' '%s'" % (coredump, upload_url))
        self.log.info("You can download it by https://storage.cloud.google.com/%s (available for ScyllaDB employee)" % upload_url)

    def _get_coredump_size(self, coredump):
        try:
            res = self.remoter.run('stat -c %s {}'.format(coredump))
            return int(res.stdout.strip())
        except Exception as ex:
            self.log.error('Failed getting coredump file size: %s', ex)
        return None

    def _try_split_coredump(self, coredump):
        """
        Compress coredump file, try to split the compressed file if it's too big

        :param coredump: coredump file path
        :return: coredump files list
        """
        core_files = []
        try:
            self.remoter.run('sudo yum install -y pigz')
            self.remoter.run('sudo pigz --fast {}'.format(coredump))
            file_name = '{}.gz'.format(coredump)
            core_files.append(file_name)
            file_size = self._get_coredump_size(file_name)
            if file_size and file_size > COREDUMP_MAX_SIZE:
                cnt = file_size / COREDUMP_MAX_SIZE
                cnt += 1 if file_size % COREDUMP_MAX_SIZE > 0 else cnt
                self.log.debug('Splitting coredump to {} files'.format(cnt))
                res = self.remoter.run('sudo split -n {} {} {}.;ls {}.*'.format(
                    cnt, file_name, file_name, file_name))
                core_files = [f.strip() for f in res.stdout.split()]
        except Exception as ex:
            self.log.error('Failed splitting coredump file: %s', ex)
        return core_files or [coredump]

    def _notify_backtrace(self, last):
        """
        Notify coredump backtraces to test log and coredump.log file.

        :param last: Whether to show only the last backtrace.
        """
        result = self._get_coredump_backtraces(last=last)
        log_file = os.path.join(self.logdir, 'coredump.log')
        output = result.stdout + result.stderr
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Coredump:'):
                coredump = line.split()[-1]
                self.log.debug('Found coredump file: {}'.format(coredump))
                coredump_files = self._try_split_coredump(coredump)
                for f in coredump_files:
                    self._upload_coredump(f)
                if len(coredump_files) > 1 or coredump_files[0].endswith('.gz'):
                    base_name = os.path.basename(coredump)
                    if len(coredump_files) > 1:
                        self.log.info("To join coredump pieces, you may use: "
                                      "'cat {}.gz.* > {}.gz'".format(base_name, base_name))
                    self.log.info("To decompress you may use: 'unpigz --fast {}.gz'".format(base_name))

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
        try:
            n_backtraces_cmd = 'sudo coredumpctl --no-pager --no-legend 2>/dev/null'
            result = self.remoter.run(n_backtraces_cmd,
                                      verbose=False, ignore_status=True)
            return len(result.stdout.splitlines())
        except Exception as details:
            self.log.error('Error retrieving number of core dumps : %s',
                           details)
            return None

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
        while True:
            if self.termination_event.isSet():
                break
            self.get_backtraces()
            time.sleep(30)

    def start_backtrace_thread(self):
        self._backtrace_thread = threading.Thread(target=self.backtrace_thread)
        self._backtrace_thread.start()

    def __str__(self):
        return 'Node %s [%s | %s] (seed: %s)' % (self.name,
                                                 self.public_ip_address,
                                                 self.private_ip_address,
                                                 self.is_seed)

    def restart(self):
        raise NotImplementedError('Derived classes must implement restart')

    def reboot(self):
        raise NotImplementedError('Derived classes must implement reboot')

    @log_run_info
    def start_task_threads(self):
        if 'db-node' in self.name:  # this should be replaced when DbNode class will be created
            self.start_journal_thread()
        self.start_backtrace_thread()

    @log_run_info
    def stop_task_threads(self, timeout=10):
        if self.termination_event.isSet():
            return
        self.termination_event.set()
        if self._backtrace_thread:
            self._backtrace_thread.join(timeout)
        if self._journal_thread:
            # current implementation of journal thread uses blocking journalctl cmd with pipe to sct_log_formatter
            # hence to stop the thread we need to kill the process first
            self.remoter.run(cmd="sudo pkill -f sct_log_formatter", ignore_status=True)
            self._journal_thread.join(timeout)
        del self.remoter

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
        if not self._sct_log_formatter_installed:
            self.install_sct_log_formatter()
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
        return self.is_port_used(port=9042, service_name="scylla-server")

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
            self.remoter.run('nodetool flush', ignore_status=True)
            res = self.remoter.run('nodetool cfstats {}'.format(keyspace), ignore_status=True)
            return res.exit_status == 0
        tcpdump_id = uuid.uuid4()
        if tcpdump:
            self.log.info('START tcpdump thread uuid: %s', tcpdump_id)
            tcpdump_thread = threading.Thread(target=self._get_tcpdump_logs,
                                              kwargs={'tcpdump_id': tcpdump_id})
            tcpdump_thread.start()
        wait.wait_for(keyspace_available, step=60, text='Waiting until keyspace {} is available'.format(keyspace))
        try:
            result = self.remoter.run('nodetool cfstats {}'.format(keyspace))
        except process.CmdError:
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
            self.remoter.run(cmd % result.stdout.strip())
            self.remoter.run('sudo -u scylla touch %s' % mark_path,
                             verbose=verbose)

    def wait_db_up(self, verbose=True, timeout=3600):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60, text=text, timeout=timeout, throw_exc=True)
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

    def search_database_log(self, expression, start_search_from_byte=0):
        matches = []
        pattern = re.compile(expression, re.IGNORECASE)

        if not os.path.exists(self.database_log):
            return matches
        with open(self.database_log, 'r') as f:
            if start_search_from_byte:
                f.seek(start_search_from_byte)
            for index, line in enumerate(f):
                if index not in self._database_log_errors_index:
                    m = pattern.search(line)
                    if m:
                        self._database_log_errors_index.append(index)
                        matches.append((index, line))
        return matches

    def search_database_log_errors(self):
        errors = []
        for expression in self._database_error_patterns:
            errors += self.search_database_log(expression)
        return errors

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
                     yaml_file=SCYLLA_YAML_PATH, broadcast=None, authenticator=None,
                     server_encrypt=None, client_encrypt=None, append_conf=None, append_scylla_args=None,
                     debug_install=False, hinted_handoff_disabled=False, murmur3_partitioner_ignore_msb_bits=None):
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
        if hinted_handoff_disabled:  # disable hinted handoff (it is enabled by default in Scylla)
            p = re.compile('[# ]*hinted_handoff_enabled:.*')
            scylla_yaml_contents = p.sub('hinted_handoff_enabled: false', scylla_yaml_contents)

        if murmur3_partitioner_ignore_msb_bits:
            self.log.debug('Change murmur3_partitioner_ignore_msb_bits to {}'.format(murmur3_partitioner_ignore_msb_bits))
            p = re.compile('murmur3_partitioner_ignore_msb_bits:.*')
            if p.findall(scylla_yaml_contents):
                scylla_yaml_contents = p.sub('murmur3_partitioner_ignore_msb_bits: {0}'.format(murmur3_partitioner_ignore_msb_bits),
                                             scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nmurmur3_partitioner_ignore_msb_bits: {0}\n".format(murmur3_partitioner_ignore_msb_bits)

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

        if server_encrypt or client_encrypt:
            self.remoter.send_files(src='./data_dir/ssl_conf',
                                    dst='/tmp/')
            self.remoter.run('sudo mv /tmp/ssl_conf/*.* /etc/scylla/')

        if server_encrypt:
            scylla_yaml_contents += """
server_encryption_options:
   internode_encryption: all
   certificate: /etc/scylla/db.crt
   keyfile: /etc/scylla/db.key
   truststore: /etc/scylla/cadb.pem
"""

        if client_encrypt:
            client_encrypt_conf = dedent("""
                            client_encryption_options:          # <client_encrypt>
                               enabled: true                    # <client_encrypt>
                               certificate: /etc/scylla/db.crt  # <client_encrypt>
                               keyfile: /etc/scylla/db.key      # <client_encrypt>
            """)
            scylla_yaml_contents += client_encrypt_conf

        if self.replacement_node_ip:
            logger.debug("%s is a replacement node for '%s'." % (self.name, self.replacement_node_ip))
            scylla_yaml_contents += "\nreplace_address_first_boot: %s\n" % self.replacement_node_ip

        if append_conf:
            scylla_yaml_contents += append_conf

        with open(yaml_dst_path, 'w') as f:
            f.write(scylla_yaml_contents)

        self.remoter.send_files(src=yaml_dst_path,
                                dst='/tmp/scylla.yaml')
        self.remoter.run('sudo mv /tmp/scylla.yaml {}'.format(yaml_file))

        if append_scylla_args:
            self.remoter.run("sudo sed -i -e 's/SCYLLA_ARGS=\"/SCYLLA_ARGS=\"%s /' /etc/sysconfig/scylla-server" % append_scylla_args)

        if debug_install and self.is_rhel_like():
            self.remoter.run('sudo yum install -y scylla-gdb', verbose=True, ignore_status=True)

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
            self.remoter.run('sudo yum clean all')
        else:
            repo_path = '/etc/apt/sources.list.d/scylla.list'
            self.remoter.run('sudo curl -o %s -L %s' % (repo_path, scylla_repo))
            result = self.remoter.run('cat %s' % repo_path, verbose=True)
            verify_scylla_repo_file(result.stdout, is_rhel_like=False)
            self.remoter.run(cmd="sudo apt-get update", ignore_status=True)

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
            self.remoter.run('sudo yum clean all')
        else:
            self.remoter.run('sudo rm -f /etc/apt/sources.list.d/scylla.list')
            self.remoter.run('sudo apt-get remove -y scylla\*', ignore_status=True)
            self.remoter.run('sudo apt-get clean all')
        self.remoter.run('sudo rm -rf /var/lib/scylla/commitlog/*')
        self.remoter.run('sudo rm -rf /var/lib/scylla/data/*')

    def install_scylla(self, scylla_repo):
        """
        Download and install scylla on node
        :param scylla_repo: scylla repo file URL
        """
        if self.is_rhel_like():
            result = self.remoter.run('ls /etc/yum.repos.d/epel.repo', ignore_status=True)
            if result.exit_status == 0:
                self.remoter.run('sudo yum update -y --skip-broken --disablerepo=epel', retry=3)
            else:
                self.remoter.run('sudo yum update -y --skip-broken', retry=3)
            self.remoter.run('sudo yum install -y rsync tcpdump screen wget net-tools')
            self.download_scylla_repo(scylla_repo)
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
            elif self.is_debian8():
                self.remoter.run('echo "deb http://http.debian.net/debian jessie-backports main" |sudo tee /etc/apt/sources.list.d/jessie-backports.list')
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install gnupg-curl -y')
                self.remoter.run('sudo apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key')
                self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19')
                self.remoter.run('echo "deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /" |sudo tee /etc/apt/sources.list.d/scylla-3rdparty.list')
                self.remoter.run('sudo apt-get update')
                self.remoter.run('sudo apt-get install -y openjdk-8-jre-headless -t jessie-backports')
                self.remoter.run('sudo update-java-alternatives -s java-1.8.0-openjdk-amd64')
            self.remoter.run('sudo apt-get upgrade -y')
            self.remoter.run('sudo apt-get install -y rsync tcpdump screen wget net-tools')
            self.download_scylla_repo(scylla_repo)
            self.remoter.run('sudo apt-get update')
            self.remoter.run('sudo apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" --force-yes --allow-unauthenticated {}'.format(self.scylla_pkg()))

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
                cmd="sudo echo 'deb http://archive.debian.org/debian jessie-backports main' | sudo tee -a /etc/apt/sources.list.d/jessie-backports.list")
            self.remoter.run(cmd="sudo touch /etc/apt/apt.conf.d/99jessie-backports")
            self.remoter.run(
                cmd="sudo echo 'Acquire::Check-Valid-Until \"false\";' | sudo tee /etc/apt/apt.conf.d/99jessie-backports")
            self.remoter.run('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F76221572C52609D', retry=3)
            install_transport_https = dedent("""
                            if [ ! -f /etc/apt/sources.list.d/backports.list ]; then sudo echo 'deb http://http.debian.net/debian jessie-backports main' | sudo tee /etc/apt/sources.list.d/backports.list > /dev/null; fi
                            apt-get install apt-transport-https
                            apt-get install gnupg-curl
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

    def collect_mgmt_log(self):
        self.log.debug("Collect scylla manager log ...")
        mgmt_log_name = "/tmp/{}_scylla_manager.log".format(self.name)
        self.remoter.run('journalctl -u scylla-manager.service --no-tail > {}'.format(mgmt_log_name), ignore_status=True, verbose=False)
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

    def _resharding_finished(self):
        """
        Check is there's Reshard listed in the log
        """
        patt = re.compile('RESHARD')
        out = self.run_nodetool("compactionstats")
        m = patt.search(out)
        # If 'RESHARD' is not found in the compactionstats output, return True - means resharding was finished
        # (or was not started)
        return True if not m else False

    # Default value of murmur3_partitioner_ignore_msb_bits parameter is 12
    def restart_node_with_resharding(self, murmur3_partitioner_ignore_msb_bits=12):
        self.stop_scylla()
        # Change murmur3_partitioner_ignore_msb_bits parameter to cause resharding.
        self.config_setup(murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits)
        self.start_scylla()

        resharding_finished = wait.wait_for(func=self._resharding_finished, step=5, timeout=3600,
                                            text="Wait for re-sharding to be finished")
        if not resharding_finished:
            logger.error('Resharding has not been started or was not finished! (murmur3_partitioner_ignore_msb_bits={}) '
                         'Check the log for the detailes'.format(murmur3_partitioner_ignore_msb_bits))
        else:
            logger.debug('Resharding has been finished successfully (murmur3_partitioner_ignore_msb_bits={})'.
                         format(murmur3_partitioner_ignore_msb_bits))

    def get_cql_auth(self):
        user = self.params.get('authenticator_user', default=None)
        password = self.params.get('authenticator_password', default=None)
        return (user, password) if user and password else None


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
        assert 'AVOCADO_TEST_LOGDIR' in os.environ
        try:
            self.logdir = path.init_dir(os.environ['AVOCADO_TEST_LOGDIR'],
                                        self.name)
        except OSError:
            self.logdir = os.path.join(os.environ['AVOCADO_TEST_LOGDIR'],
                                       self.name)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Init nodes')
        self.nodes = []
        self.nemesis = []
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

    def get_backtraces(self):
        for node in self.nodes:
            node.get_backtraces()
            if node.n_coredumps > 0:
                self.coredumps[node.name] = node.n_coredumps

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
            node_errors = node.search_database_log_errors()
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
            node.destroy()

    def terminate_node(self, node):
        self.nodes.remove(node)
        node.destroy()

    def get_cql_auth(self):
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
        self.seed_nodes_private_ips = None
        super(BaseScyllaCluster, self).__init__(*args, **kwargs)

    @staticmethod
    def get_node_ips_param(public_ip=True):
        if Setup.MIXED_CLUSTER:
            return 'oracle_db_nodes_public_ip' if public_ip else 'oracle_db_nodes_private_ip'
        return 'db_nodes_public_ip' if public_ip else 'db_nodes_private_ip'

    def get_scylla_args(self):
        return self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else\
            self.params.get('append_scylla_args')

    def get_seed_nodes_private_ips(self):
        if self.seed_nodes_private_ips is None:
            node = self.nodes[0]
            yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct'),
                                         'scylla.yaml')
            node.remoter.receive_files(src=SCYLLA_YAML_PATH,
                                       dst=yaml_dst_path)
            with open(yaml_dst_path, 'r') as yaml_stream:
                conf_dict = yaml.safe_load(yaml_stream)
                try:
                    self.seed_nodes_private_ips = conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
                except Exception, details:
                    self.log.debug('Loaded YAML data structure: %s', conf_dict)
                    self.log.error('Scylla YAML config contents:')
                    with open(yaml_dst_path, 'r') as yaml_stream:
                        self.log.error(yaml_stream.read())
                    raise ValueError('Exception determining seed node ips: %s' %
                                     details)
        return self.seed_nodes_private_ips

    def enable_client_encrypt(self):
        for node in self.nodes:
            logger.debug("Enabling client encryption on node")
            node.enable_client_encrypt()

    def disable_client_encrypt(self):
        for node in self.nodes:
            logger.debug("Disabling client encryption on node")
            node.disable_client_encrypt()

    def get_seed_nodes(self):
        seed_nodes_private_ips = self.get_seed_nodes_private_ips()
        seed_nodes = []
        for node in self.nodes:
            if node.private_ip_address in seed_nodes_private_ips:
                node.is_seed = True
                seed_nodes.append(node)
            else:
                node.is_seed = False
        return seed_nodes

    def get_seed_nodes_by_flag(self, private_ip=True):
        """
        We set is_seed at two point, before and after node_setup.
        However, we can only call this function when the flag is set.
        """
        if private_ip:
            node_private_ips = [node.private_ip_address for node
                                in self.nodes if node.is_seed]
            seeds = ",".join(node_private_ips)
        else:
            node_public_ips = [node.public_ip_address for node
                               in self.nodes if node.is_seed]
            seeds = ",".join(node_public_ips)
        if not seeds:
            # use first node as seed by default
            seeds = self.nodes[0].private_ip_address if private_ip else self.nodes[0].public_ip_address
            self.nodes[0].is_seed = True
        return seeds

    def _update_db_binary(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB binary...')

        seed_nodes = self.get_seed_nodes()
        non_seed_nodes = [n for n in self.nodes if not n.is_seed]

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
        non_seed_nodes = [n for n in self.nodes if not n.is_seed]

        def update_scylla_packages(node, queue):
            node.log.info('Updating DB packages')
            node.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)
            node.remoter.run('sudo yum update -y --skip-broken')
            node.remoter.run('sudo yum install python34-PyYAML -y')
            # replace the packages
            node.remoter.run('yum list installed | grep scylla')
            # update *developmen* packages
            node.remoter.run('sudo rpm -UvhR --oldpackage /tmp/scylla/*development* | true')
            # and all the rest
            node.remoter.run('sudo rpm -URvh --replacefiles /tmp/scylla/* | true')
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
        cmd_result = verification_node.remoter.run('nodetool status')
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
        data_centers = res.strip().split("Data center: ")
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

    def check_nodes_up_and_normal(self, nodes=[]):
        """Checks via nodetool that node joined the cluster and reached 'UN' state"""
        if not nodes:
            nodes = self.nodes
        status = self.get_nodetool_status()
        up_statuses = []
        for node in nodes:
            for dc, dc_status in status.iteritems():
                ip_status = dc_status.get(node.private_ip_address)
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
            version_commands = ["scylla --version", "rpm -q {}".format(self.nodes[0].scylla_pkg())]
            for version_cmd in version_commands:
                try:
                    result = self.nodes[0].remoter.run(version_cmd)
                except Exception as ex:
                    self.log.error('Failed getting scylla version: %s', ex)
                else:
                    match = re.match("(\d+[.]\d+[.]\w+).*", result.stdout)
                    if match:
                        scylla_version = match.group(1)
                        self.log.info("Found ScyllaDB version: %s" % scylla_version)
                        for node in self.nodes:
                            node.scylla_version = scylla_version
                        break
                    else:
                        self.log.error("Unknown ScyllaDB version")

    def run_cqlsh(self, node, cql_cmd, timeout=60, verbose=True, split=False):
        cql_auth = self.get_cql_auth()
        cql_auth = '-u {} -p {}'.format(*cql_auth) if cql_auth else ''
        cmd = 'cqlsh --no-color {} -e "{}" {} --request-timeout={}'.format(cql_auth, cql_cmd, node.private_ip_address, timeout)
        cqlsh_out = node.remoter.run(cmd, timeout=timeout, verbose=verbose)
        # stdout of cqlsh example:
        #      pk
        #      ----
        #       2
        #       3
        #
        #      (10 rows)
        return cqlsh_out.stdout if not split else [line.strip() for line in cqlsh_out.stdout.split('\n')]

    def get_test_keyspaces(self):
        out = self.run_cqlsh(node=self.nodes[0], cql_cmd='select keyspace_name from system_schema.keyspaces')
        return [ks.strip() for ks in out.split('\n')[3:-3] if 'system' not in ks]

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
        self.nemesis.append(nemesis(tester_obj=tester_obj,
                                    termination_event=self.termination_event))

    def clean_nemesis(self):
        self.nemesis = []

    @log_run_info("Start nemesis threads on cluster")
    def start_nemesis(self, interval=30):
        for nemesis in self.nemesis:
            nemesis.set_termination_event(self.termination_event)
            nemesis.set_target_node()
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval,), verbose=True)
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
        node.config_setup(seed_address=seed_address,
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          endpoint_snitch=endpoint_snitch,
                          authenticator=self.params.get('authenticator'),
                          server_encrypt=self._param_enabled('server_encrypt'),
                          client_encrypt=self._param_enabled('client_encrypt'),
                          append_conf=self.params.get('append_conf'),
                          hinted_handoff_disabled=self._param_enabled('hinted_handoff_disabled'),
                          append_scylla_args=self.get_scylla_args())

    def node_setup(self, node, verbose=False, timeout=3600):
        """
        Install, configure and run scylla on node
        :param node: scylla node object
        :param verbose:
        """
        node.wait_ssh_up(verbose=verbose)
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = self.get_seed_nodes_by_flag()

        if not Setup.REUSE_CLUSTER:
            node.clean_scylla()
            node.install_scylla(scylla_repo=self.params.get('scylla_repo'))

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
            if node.private_ip_address not in seed_address_list:
                wait.wait_for(func=lambda: self._seed_node_rebooted is True,
                              step=30,
                              text='Wait for seed node to be up after reboot')
            node.restart()
            node.wait_ssh_up()

            if node.private_ip_address in seed_address_list:
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
            src_dir = node.prepare_files_for_archive(files_to_archive)
            node.receive_files(src=src_dir, dst=storing_dir)
            with open(os.path.join(storing_dir, node.name, 'installed_pkgs'), 'w') as pkg_list_file:
                pkg_list_file.write(node.get_installed_packages())
            with open(os.path.join(storing_dir, node.name, 'console_output'), 'w') as co:
                co.write(node.get_console_output())
            with open(os.path.join(storing_dir, node.name, 'console_screenshot.jpg'), 'wb') as cscrn:
                imagedata = node.get_console_screenshot()
                cscrn.write(decodestring(imagedata))


class BaseLoaderSet(object):

    def __init__(self, params):
        self._loader_queue = []
        self.params = params
        self.cassandra_stress_version = ""

    def get_cassandra_stress_version(self, node):
        if not self.cassandra_stress_version:
            result = node.remoter.run(cmd="cassandra-stress version", ignore_status=True, verbose=True)
            match = re.match("Version: (.*)", result.stdout)
            if match:
                self.cassandra_stress_version = match.group(1)
            else:
                self.log.error("C-S version not found!")
                self.cassandra_stress_version = "unknown"

    def node_setup(self, node, verbose=False, db_node_address=None, **kwargs):
        self.log.info('Setup in BaseLoaderSet')
        node.wait_ssh_up(verbose=verbose)

        if Setup.REUSE_CLUSTER:
            self.kill_stress_thread()
            return

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
                echo "deb http://http.debian.net/debian jessie-backports main" |sudo tee /etc/apt/sources.list.d/jessie-backports.list
                apt-get update
                apt-get install gnupg-curl -y
                apt-key adv --fetch-keys https://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/Release.key
                apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 17723034C56D4B19
                echo "deb http://download.opensuse.org/repositories/home:/scylladb:/scylla-3rdparty-jessie/Debian_8.0/ /" |sudo tee /etc/apt/sources.list.d/scylla-3rdparty.list
                apt-get update
                apt-get install -y openjdk-8-jre-headless -t jessie-backports
                update-java-alternatives -s java-1.8.0-openjdk-amd64
            """)
            node.remoter.run('sudo bash -cxe "%s"' % install_java_script)

        node.download_scylla_repo(self.params.get('scylla_repo'))
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
        cs_exporter_setup = CassandraStressExporterSetup()
        cs_exporter_setup.install(node)
        self.get_cassandra_stress_version(node)

        # scylla-bench
        node.remoter.run('sudo yum install git -y')
        node.remoter.run('curl -LO https://storage.googleapis.com/golang/go1.9.2.linux-amd64.tar.gz')
        node.remoter.run('sudo tar -C /usr/local -xvzf go1.9.2.linux-amd64.tar.gz')
        node.remoter.run("echo 'export GOPATH=$HOME/go' >> $HOME/.bashrc")
        node.remoter.run("echo 'export PATH=$PATH:/usr/local/go/bin' >> $HOME/.bashrc")
        node.remoter.run("source $HOME/.bashrc")
        node.remoter.run("go get github.com/scylladb/scylla-bench")

        # gemini tool
        gemini_url = self.params.get('gemini_url')
        gemini_static_url = self.params.get('gemini_static_url')
        if not gemini_url or not gemini_static_url:
            logger.warning('Gemini URLs should be defined to run the gemini tool')
        else:
            install_gemini_script = dedent("""
                cd $HOME
                curl -L -o gemini {gemini_url}
                chmod a+x gemini
                curl -LO  {gemini_static_url}
            """.format(**locals()))
            node.remoter.run("bash -cxe '%s'" % install_gemini_script)

    @wait_for_init_wrap
    def wait_for_init(self, verbose=False, db_node_address=None):
        pass

    @staticmethod
    def get_node_ips_param(public_ip=True):
        return 'loaders_public_ip' if public_ip else 'loaders_private_ip'

    def get_loader(self):
        if not self._loader_queue:
            self._loader_queue = [node for node in self.nodes]
        return self._loader_queue.pop(0)

    def run_stress_thread(self, stress_cmd, timeout, output_dir, stress_num=1, keyspace_num=1, keyspace_name='',
                          profile=None, node_list=[], round_robin=False):
        if self.cassandra_stress_version == "unknown":  # Prior to 3.11, cassandra-stress didn't have version argument
            stress_cmd = stress_cmd.replace("throttle", "limit")  # after 3.11 limit was renamed to throttle

        if keyspace_name:
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace={} ".format(keyspace_name))
        elif 'keyspace=' not in stress_cmd:  # if keyspace is defined in the command respect that
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace=keyspace$2 ")

        if profile:
            with open(profile) as fp:
                profile_content = fp.read()
                cs_profile = script.TemporaryScript(name=os.path.basename(profile), content=profile_content)
                self.log.info('Profile content:\n%s' % profile_content)
                cs_profile.save()
        queue = {TASK_QUEUE: Queue.Queue(), RES_QUEUE: Queue.Queue()}

        def node_run_stress(node, loader_idx, cpu_idx, keyspace_idx, profile, stress_cmd):
            queue[TASK_QUEUE].put(node)
            if node_list and '-node' not in stress_cmd:
                first_node = [n for n in node_list if n.dc_idx == loader_idx % 3]
                first_node = first_node[0] if first_node else node_list[0]
                stress_cmd += " -node {}".format(first_node.ip_address())

            cql_auth = self.get_cql_auth()
            if cql_auth and 'user=' not in stress_cmd:
                # put the credentials into the right place into -mode section
                stress_cmd = re.sub(r'(-mode.*?)-', r'\1 user={} password={} -'.format(*cql_auth), stress_cmd)

            stress_cmd_opt = stress_cmd.split()[1]
            cs_pipe_name = '/tmp/cs_{}_pipe_$1_$2'.format(stress_cmd_opt)
            stress_cmd = "mkfifo {}; cat {}|python /usr/bin/cassandra_stress_exporter {} & ".format(cs_pipe_name,
                                                                                                    cs_pipe_name,
                                                                                                    stress_cmd_opt) +\
                         stress_cmd +\
                         "|tee {}; pkill -P $$ -f cassandra_stress_exporter; rm -f {}".format(cs_pipe_name,
                                                                                              cs_pipe_name)
            # We'll save a script with the last c-s command executed on loaders
            stress_script = script.TemporaryScript(name='run_cassandra_stress.sh',
                                                   content='%s\n' % stress_cmd)
            self.log.info('Stress script content:\n%s' % stress_cmd)
            stress_script.save()

            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            log_file_name = os.path.join(logdir,
                                         'cassandra-stress-l%s-c%s-%s.log' %
                                         (loader_idx, cpu_idx, uuid.uuid4()))
            # This tag will be output in the header of c-stress result,
            # we parse it to know the loader & cpu info in _parse_cs_summary().
            tag = 'TAG: loader_idx:%s-cpu_idx:%s-keyspace_idx:%s' % (loader_idx, cpu_idx, keyspace_idx)

            self.log.debug('cassandra-stress log: %s', log_file_name)
            dst_stress_script_dir = os.path.join('/home', node.remoter.user)
            dst_stress_script = os.path.join(dst_stress_script_dir,
                                             os.path.basename(stress_script.path))
            node.remoter.send_files(stress_script.path, dst_stress_script_dir)
            if profile:
                node.remoter.send_files(cs_profile.path, os.path.join('/tmp', os.path.basename(profile)))
            node.remoter.run(cmd='chmod +x %s' % dst_stress_script)

            if stress_num > 1:
                node_cmd = 'taskset -c %s %s' % (cpu_idx, dst_stress_script)
            else:
                node_cmd = dst_stress_script
            node_cmd = 'echo %s; %s %s %s' % (tag, node_cmd, cpu_idx, keyspace_idx)

            result = node.remoter.run(cmd=node_cmd,
                                      timeout=timeout,
                                      ignore_status=True,
                                      log_file=log_file_name)
            queue[RES_QUEUE].put((node, result))
            queue[TASK_QUEUE].task_done()

        if round_robin:
            # cancel stress_num
            stress_num = 1
            loaders = [self.get_loader()]
            self.log.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.nodes

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(stress_num):
                for ks_idx in range(1, keyspace_num + 1):
                    setup_thread = threading.Thread(target=node_run_stress,
                                                    args=(loader, loader_idx,
                                                          cpu_idx, ks_idx, profile, stress_cmd))
                    setup_thread.daemon = True
                    setup_thread.start()
                    if loader_idx == 0 and cpu_idx == 0:
                        time.sleep(30)

        return queue

    def kill_stress_thread(self):
        kill_script_contents = 'PIDS=$(pgrep -f cassandra-stress) && pkill -TERM -P $PIDS'
        kill_script = script.TemporaryScript(name='kill_cassandra_stress.sh',
                                             content=kill_script_contents)
        kill_script.save()
        kill_script_dir = os.path.dirname(kill_script.path)
        for loader in self.nodes:
            loader.remoter.run(cmd='mkdir -p %s' % kill_script_dir)
            loader.remoter.send_files(kill_script.path, kill_script_dir)
            loader.remoter.run(cmd='chmod +x %s' % kill_script.path)
            cs_active = loader.remoter.run(cmd='pgrep -f cassandra-stress',
                                           ignore_status=True)
            if cs_active.exit_status == 0:
                kill_result = loader.remoter.run(kill_script.path,
                                                 ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate c-s on node %s:\n%s',
                                   loader, kill_result)
            loader.remoter.run(cmd='rm -rf %s' % kill_script_dir)
        kill_script.remove()

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

    @staticmethod
    def _parse_gemini_summary(lines):
        results = {}
        enable_parse = False

        for line in lines:
            line.strip()
            if line.startswith('Results:'):
                enable_parse = True
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            results[key] = int(value)
        return results

    def verify_stress_thread(self, queue, db_cluster):
        results = []
        cs_summary = []
        self.log.debug('Wait for %s stress threads to verify', queue[TASK_QUEUE].qsize())
        queue[TASK_QUEUE].join()
        while not queue[RES_QUEUE].empty():
            results.append(queue[RES_QUEUE].get())

        errors = []
        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            node_cs_res = self._parse_cs_summary(lines)
            if node_cs_res:
                cs_summary.append(node_cs_res)
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]

        return cs_summary, errors

    def get_stress_results(self, queue):
        results = []
        ret = []
        self.log.debug('Wait for %s stress threads results', queue[TASK_QUEUE].qsize())
        queue[TASK_QUEUE].join()
        while not queue[RES_QUEUE].empty():
            results.append(queue[RES_QUEUE].get())

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            node_cs_res = self._parse_cs_summary(lines)
            if node_cs_res:
                ret.append(node_cs_res)

        return ret

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
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
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

    def run_gemini_thread(self, cmd, timeout, output_dir, test_node, oracle_node):
        queue = {TASK_QUEUE: Queue.Queue(), RES_QUEUE: Queue.Queue()}

        def _run_gemini(node, loader_idx, cmd, test_node, oracle_node):
            queue[TASK_QUEUE].put(node)
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            log_file_name = os.path.join(logdir,
                                         'gemini-l%s-%s.log' %
                                         (loader_idx, uuid.uuid4()))
            gemini_log = tempfile.NamedTemporaryFile(prefix='gemini-', suffix='.log').name
            result = node.remoter.run(cmd="/$HOME/{} --test-cluster={} --oracle-cluster={} | tee {}".format(
                                      cmd.strip(), test_node, oracle_node, gemini_log),
                                      timeout=timeout,
                                      ignore_status=True,
                                      log_file=log_file_name)
            queue[RES_QUEUE].put((node, result))
            queue[TASK_QUEUE].task_done()

        for loader_idx, loader in enumerate(self.nodes):
            setup_thread = threading.Thread(target=_run_gemini,
                                            args=(loader, loader_idx,
                                                  cmd, test_node, oracle_node))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        return queue

    def get_gemini_results(self, queue):
        results = []
        ret = []
        self.log.debug('Wait for %s gemini threads results', queue[TASK_QUEUE].qsize())
        queue[TASK_QUEUE].join()
        while not queue[RES_QUEUE].empty():
            results.append(queue[RES_QUEUE].get())

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            res = self._parse_gemini_summary(lines)
            if res:
                ret.append(res)

        return ret

    def kill_gemini_thread(self):
        for loader in self.nodes:
            sb_active = loader.remoter.run(cmd='pgrep -f gemini', ignore_status=True)
            if sb_active.exit_status == 0:
                kill_result = loader.remoter.run('pkill -f -TERM gemini', ignore_status=True)
                if kill_result.exit_status != 0:
                    self.log.error('Terminate gemini on node %s:\n%s', loader, kill_result)

    def get_non_system_ks_cf_list(self, loader_node, db_node_ip):
        """Get all not system keyspace.tables pairs

        Arguments:
            loader_node {BaseNode} -- LoaderNoder to send request
            db_node_ip {str} -- ip of db_node
        """
        cmd = 'SELECT keyspace_name, table_name from system_schema.tables'
        result = loader_node.remoter.run('cqlsh --no-color --execute "{}" {}'.format(cmd, db_node_ip), verbose=False)

        avaialable_ks_cf = []
        for row in result.stdout.split('\n'):
            if '|' not in row:
                continue
            avaialable_ks_cf.append('.'.join([name.strip() for name in row.split('|')]))
        return [ks_cf for ks_cf in avaialable_ks_cf[1:] if 'system' not in ks_cf]

    def run_fullscan(self, ks_cf, loader_node, db_node_ip):
        """Run cql select count(*) request

        if ks_cf is not random, use value from config
        if ks_cf is random, choose random from not system

        Arguments:
            loader_node {BaseNode} -- loader cluster node
            db_node {BaseNode} -- db cluster node

        Returns:
            object -- object with result of remoter.run command
        """

        ks_cf_list = self.get_non_system_ks_cf_list(loader_node, db_node_ip)
        if ks_cf not in ks_cf_list:
            self.log.warning('Keyspace of column family {} does not exists. Using random.'.format(ks_cf))
            ks_cf = 'random'

        if 'random' in ks_cf.lower():
            ks_cf = random.choice(ks_cf_list)

        self.log.info('Fullscan for ks.cf: {}'.format(ks_cf))
        cmd = 'select count(*) from {}'.format(ks_cf)
        result = loader_node.remoter.run('cqlsh --no-color --execute "{}" {}'.format(cmd, db_node_ip), verbose=False)

        return result

    def run_fullscan_thread(self, ks_cf, db_nodes, interval=60, duration=60):
        """Run in thread request from loader node

        Run on randomly choosen loader node the cql request to
        randomly choosen db node during time duration in seconds and with
        interval timeout in seconds

        Arguments:
            db_nodes {list} -- list of db nodes db_cluster.nodes

        Keyword Arguments:
            timeout {int} -- time interval (default: {60})
            duration {int} -- duration of running (default: {60})
        """

        @log_run_info('Fullscan thread')
        def run_in_thread():
            start = current = time.time()
            while current - start < duration:
                loader_node = random.choice(self.nodes)
                if loader_node.termination_event.isSet():
                    break
                db_node = random.choice(db_nodes)
                if db_node.remoter and not db_node.remoter.is_up():
                    self.log.warning("Chosen node is down. Choose new one")
                    continue
                self.log.info('Fullscan start on node {} from loader {}'.format(db_node.name, loader_node.name))

                result = self.run_fullscan(ks_cf, loader_node, db_node.ip_address())
                self.log.debug(result)
                self.log.info('Fullscan done on node {} from loader {}'.format(db_node.name, loader_node.name))
                time.sleep(interval)
                current = time.time()

        th = threading.Thread(target=run_in_thread)
        th.start()

    def kill_fullscan_thread(self):
        for node in self.nodes:
            node.termination_event.set()


class BaseMonitorSet(object):
    # This is a Mixin for monitoring cluster and should not be inherited
    def __init__(self, targets, params):
        self.targets = targets
        self.params = params
        self.local_metrics_addr = start_metrics_server()  # start prometheus metrics server locally and return local ip
        self.sct_ip_port = self.set_local_sct_ip()
        self.grafana_port = 3000
        self.monitor_branch = self.params.get('monitor_branch', default='branch-2.2')
        self.monitor_install_path_base = "/var/lib/scylla"
        self.monitor_install_path = os.path.join(self.monitor_install_path_base,
                                                 "scylla-monitoring-{}".format(self.monitor_branch))
        self.monitoring_conf_dir = os.path.join(self.monitor_install_path, "config")
        self.monitoring_data_dir = os.path.join(self.monitor_install_path_base, "scylla-monitoring-data")
        self.phantomjs_installed = False

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

        if Setup.REUSE_CLUSTER:
            return
        self.install_scylla_monitoring(node)
        self.configure_scylla_monitoring(node)
        try:
            self.start_scylla_monitoring(node)
        except process.CmdError:
            self.reconfigure_scylla_monitoring()
        # The time will be used in url of Grafana monitor,
        # the data from this point to the end of test will
        # be captured.
        self.grafana_start_time = time.time()
        if node.is_rhel_like():
            node.remoter.run('sudo yum install screen -y')
        else:
            node.remoter.run('sudo apt-get install screen -y')
        self.install_scylla_manager(node)

    def install_scylla_manager(self, node):
        if self.params.get('use_mgmt', default=None):
            node.install_mgmt(scylla_repo=self.params.get('scylla_repo_m'), scylla_mgmt_repo=self.params.get('scylla_mgmt_repo'))

    def set_local_sct_ip(self):
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
            prereqs_script = dedent("""
                yum install -y epel-release
                yum install -y python-pip unzip wget docker
                pip install --upgrade pip
                pip install pyyaml
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
            """)
        else:
            raise ValueError('Unsupported Distribution type: {}'.format(str(node.distro)))
        node.remoter.run(cmd="sudo bash -ce '%s'" % prereqs_script)

    def download_scylla_monitoring(self, node):
        install_script = dedent("""
            mkdir -p {0.monitor_install_path_base}
            cd {0.monitor_install_path_base}
            wget https://github.com/scylladb/scylla-monitoring/archive/{0.monitor_branch}.zip
            unzip {0.monitor_branch}.zip
        """.format(self))
        node.remoter.run("sudo bash -ce '%s'" % install_script)

    def configure_scylla_monitoring(self, node, sct_metrics=True, alert_manager=True):
        db_targets_list = [n.ip_address() for n in self.targets["db_cluster"].nodes]  # node exporter + scylladb
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
            loader_targets_list = ["%s:9103" % n.ip_address() for n in self.targets["loaders"].nodes]
            scrape_configs = templ_yaml["scrape_configs"]
            scrape_configs.append(dict(job_name="stress_metrics", honor_labels=True,
                                       static_configs=[dict(targets=loader_targets_list)]))
            if self.sct_ip_port:
                scrape_configs.append(dict(job_name="sct_metrics", honor_labels=True,
                                           static_configs=[dict(targets=[self.sct_ip_port])]))
            with open(local_template, "w") as fo:
                yaml.safe_dump(templ_yaml, fo, default_flow_style=False)  # to remove tag !!python/unicode
            node.remoter.run("sudo chmod 777 %s" % prometheus_yaml_template)
            node.send_files(src=local_template, dst=prometheus_yaml_template, delete_dst=True, verbose=True)
            process.run("rm -rf {temp_dir}".format(**locals()))
        self._monitoring_targets = " ".join(db_targets_list)
        configure_script = dedent("""
            cd {0.monitor_install_path}
            mkdir -p {0.monitoring_conf_dir}
            ./genconfig.py -ns -d {0.monitoring_conf_dir} {0._monitoring_targets}
        """.format(self))
        node.remoter.run("sudo bash -ce '%s'" % configure_script, verbose=True)
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
            node.remoter.run("chmod 777 %s" % f.name)
            node.remoter.run("sudo bash -ce 'cat %s >> %s'" % (f.name, alertmanager_conf_file))

    @retrying(n=5, sleep_time=10, allowed_exceptions=(process.CmdError,),
              message="Waiting for reconfiguring scylla monitoring")
    def reconfigure_scylla_monitoring(self):
        for node in self.nodes:
            self.stop_scylla_monitoring(node)
            # We use sct_metrics=False, alert_manager=False since they should be configured once
            self.configure_scylla_monitoring(node, sct_metrics=False, alert_manager=False)
            self.start_scylla_monitoring(node)

    def start_scylla_monitoring(self, node):
        if node.is_rhel_like() or node.is_ubuntu16():
            run_script = dedent("""
                cd {0.monitor_install_path}
                sudo systemctl start docker
                mkdir -p {0.monitoring_data_dir}
                chmod 777 {0.monitoring_data_dir}
                ./start-all.sh -s {0.monitoring_conf_dir}/scylla_servers.yml -n {0.monitoring_conf_dir}/node_exporter_servers.yml -d {0.monitoring_data_dir} -l -v {0.monitoring_version} -b "-web.enable-admin-api"
            """.format(self))
        else:
            run_script = dedent("""
                cd {0.monitor_install_path}
                sudo service docker start || true
                mkdir -p {0.monitoring_data_dir}
                chmod 777 {0.monitoring_data_dir}
                ./start-all.sh -s {0.monitoring_conf_dir}/scylla_servers.yml -n {0.monitoring_conf_dir}/node_exporter_servers.yml -d {0.monitoring_data_dir} -l -v {0.monitoring_version} -b "-web.enable-admin-api"
            """.format(self))
        node.remoter.run("sudo bash -ce '%s'" % run_script, verbose=True)
        self.add_sct_dashboards_to_grafana(node)

    def add_sct_dashboards_to_grafana(self, node):
        sct_dashboard_json = "scylla-dash-per-server-nemesis.{0.monitoring_version}.json".format(self)

        def _register_grafana_json(json_filename):
            url = "http://{0.public_ip_address}:{1.grafana_port}/api/dashboards/db".format(node, self)
            json_path = get_data_dir_path(json_filename)
            result = process.run('curl -XPOST -i %s --data-binary @%s -H "Content-Type: application/json"' %
                                 (url, json_path), ignore_status=True)
            return result.exit_status == 0

        wait.wait_for(_register_grafana_json, step=10,
                      text="Waiting to register 'data_dir/%s'..." % sct_dashboard_json,
                      json_filename=sct_dashboard_json)

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

    def stop_scylla_monitoring(self, node):
        kill_script = dedent("""
            cd {0.monitor_install_path}
            ./kill-all.sh
        """.format(self))
        node.remoter.run("sudo bash -ce '%s'" % kill_script)

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
            process.run("bash -ce '%s'" % install_phantom_js_script)
            process.run("cd phantomjs-2.1.1-linux-x86_64 && "
                        "sed -e 's/200);/10000);/' examples/rasterize.js |grep -v 'use strict' > r.js",
                        shell=True)
            self.phantomjs_installed = True
        else:
            self.log.debug("PhantomJS is already installed!")

    def get_grafana_screenshot(self, test_start_time=None):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        if not test_start_time:
            self.log.error("No start time for test")
            return
        start_time = str(test_start_time).split('.')[0] + '000'

        screenshot_names = [
            'per-server-metrics-nemesis',
            'overview-metrics'
        ]

        screenshot_url_tmpl = "http://{node_ip}:{grafana_port}/dashboard/db/{scylla_pkg}-{scr_name}-{version}?from={st}&to=now"
        try:
            self.install_phantom_js()
            scylla_pkg = 'scylla-enterprise' if self.is_enterprise else 'scylla'
            for n, node in enumerate(self.nodes):
                screenshots = []
                for i, name in enumerate(screenshot_names):
                    version = self.monitoring_version.replace('.', '-')
                    grafana_url = screenshot_url_tmpl.format(
                        node_ip=node.public_ip_address,
                        grafana_port=self.grafana_port,
                        scylla_pkg=scylla_pkg,
                        scr_name=name,
                        version=version,
                        st=start_time)
                    datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
                    snapshot_path = os.path.join(node.logdir,
                                                 "grafana-screenshot-%s-%s-%s.png" % (name, datetime_now, n))
                    process.run("cd phantomjs-2.1.1-linux-x86_64 && "
                                "bin/phantomjs r.js \"%s\" \"%s\" 1920px" % (
                                    grafana_url, snapshot_path), shell=True)
                # since there is only one monitoring node returning here
                    screenshots.append(S3Storage.upload_file(snapshot_path))
                return screenshots

        except Exception, details:
            self.log.error('Error taking monitor snapshot: %s',
                           str(details))
        return ""

    @log_run_info
    def download_monitor_data(self):
        for node in self.nodes:
            try:
                snapshot_archive = self.get_prometheus_snapshot(node)
                self.log.debug('Snapshot local path: {}'.format(snapshot_archive))

                return S3Storage.upload_file(snapshot_archive)
            except Exception, details:
                self.log.error('Error downloading prometheus data dir: %s',
                               str(details))
        return ""

    def download_scylla_manager_log(self):
        for node in self.nodes:
            try:
                sclylla_mgmt_log_path = node.collect_mgmt_log()
                scylla_manager_local_log_path = os.path.join(node.logdir, 'scylla_manager_log')
                shutil.make_archive(scylla_manager_local_log_path, 'zip', sclylla_mgmt_log_path)
                link = S3Storage.upload_file("{}.zip".format(scylla_manager_local_log_path))
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
        result = node.remoter.run("sudo docker ps -a --format {{.Names}}", ignore_status=True)
        if result.exit_status == 0:
            monitoring_dockers = [docker.strip() for docker in result.stdout.split('\n') if docker.strip()]
        else:
            self.log.warning("Getting running docker containers failed: {}".format(result))

        self.log.debug("Next monitoring dockers are running: {}".format(monitoring_dockers))
        return monitoring_dockers

    def _collect_monitoring_docker_log(self, node, docker):
        log_path = "/tmp/{}.log".format(docker)
        cmd = "sudo docker logs --details -t {} > {}".format(docker, log_path)
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

    def get_prometheus_snapshot(self, node):
        # avoid cyclic-decencies between cluster and db_stats
        from sdcm.db_stats import PrometheusDBStats

        ps = PrometheusDBStats(host=node.public_ip_address)
        result = ps.create_snapshot()
        self.log.debug(result)
        if "success" in result['status']:
            snapshot_dir = os.path.join(self.monitoring_data_dir,
                                        "snapshots",
                                        result['data']['name'])
        else:
            return ""

        archive_snapshot_name = '/tmp/{}.tar.gz'.format(Setup.test_id())
        result = node.remoter.run('cd {}; tar -czvf {} .'.format(snapshot_dir,
                                                                 archive_snapshot_name),
                                  ignore_status=True)
        if result.exit_status > 0:
            self.log.warning('Unable to create archive {}'.format(result))
            return ""

        try:
            local_snapshot_data_dir = os.path.join(node.logdir, "prometheus_data")
            os.mkdir(local_snapshot_data_dir)
            node.remoter.receive_files(src=archive_snapshot_name, dst=local_snapshot_data_dir)
            return os.path.join(local_snapshot_data_dir,
                                os.path.basename(archive_snapshot_name))
        except Exception as e:
            self.log.error("Unable to downlaod Prometheus data: %s" % e)
            return ""


class NoMonitorSet(object):

    def __init__(self):
        logger = logging.getLogger('avocado.test')
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
