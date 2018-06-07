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

import ConfigParser
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
import matplotlib
import platform
import shutil
import glob

# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from avocado.utils import path
from avocado.utils import process
from avocado.utils import script

from .log import SDCMAdapter
from .remote import Remote
from .remote import disable_master_ssh
from . import data_path
from . import wait
from .utils import get_monitor_version, log_run_info

from .loader import CassandraStressExporterSetup
from .prometheus import start_metrics_server

SCYLLA_CLUSTER_DEVICE_MAPPINGS = [{"DeviceName": "/dev/xvdb",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}},
                                  {"DeviceName": "/dev/xvdc",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}}]

CREDENTIALS = []
EC2_INSTANCES = []
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
IP_SSH_CONNECTIONS = 'public'
TASK_QUEUE = 'task_queue'
RES_QUEUE = 'res_queue'
WORKSPACE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SCYLLA_YAML_PATH = "/etc/scylla/scylla.yaml"

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
    global EC2_INSTANCES
    global OPENSTACK_INSTANCES
    global OPENSTACK_SERVICE
    global CREDENTIALS
    global LIBVIRT_DOMAINS
    global LIBVIRT_IMAGES

    for ec2_instance in EC2_INSTANCES:
        if behavior == 'destroy':
            ec2_instance.terminate()
        elif behavior == 'stop':
            ec2_instance.stop()

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
    REUSE_CLUSTER = False

    @classmethod
    def reuse_cluster(cls, val=False):
        cls.REUSE_CLUSTER = val


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
        self.name = os.path.basename(self.key_file)[:-4]
        self.key_pair_name = self.name

    def __str__(self):
        return "Key Pair %s -> %s" % (self.name, self.key_file)

    def write_key_file(self):
        pass

    def destroy(self):
        pass


class GCECredentials(object):

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
        self._ssh_ip_mapping = {'public': self.public_ip_address,
                                'private': self.private_ip_address}
        ssh_login_info['hostname'] = self._ssh_ip_mapping[IP_SSH_CONNECTIONS]

        self.remoter = Remote(**ssh_login_info)
        self._ssh_login_info = ssh_login_info
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug(self.remoter.ssh_debug_cmd())

        self._journal_thread = None
        self.n_coredumps = 0
        self._backtrace_thread = None
        self._prometheus_thread = None
        self._collectd_exporter_thread = None
        self._sct_log_formatter_installed = False
        self._init_system = None
        self._metrics_target = None
        self.prometheus_data_dir = None

        self.cs_start_time = None
        self.database_log = os.path.join(self.logdir, 'database.log')
        self._database_log_errors_index = []
        self._database_error_patterns = ['std::bad_alloc', 'integrity check failed']
        self.termination_event = threading.Event()
        if node_prefix is not None and 'db-node' in node_prefix:
            self.start_journal_thread()
        self.start_backtrace_thread()
        # We should disable bootstrap when we create nodes to establish the cluster,
        # if we want to add more nodes when the cluster already exists, then we should
        # enable bootstrap. So addition means not the first set of node.
        self.is_addition = False
        self.scylla_version = ''
        self.is_enterprise = None
        self.replacement_node_ip = None  # if node is a replacement for a dead node, store dead node private ip here
        self._set_prometheus_paths()

    def is_docker(self):
        return self.__class__.__name__ == 'DockerNode'

    def scylla_pkg(self):
        if self.is_enterprise is None:
            result = self.remoter.run("sudo yum search scylla-enterprise", ignore_status=True)
            self.is_enterprise = True if ('scylla-enterprise.x86_64' in result.stdout or
                                          'No matches found' not in result.stdout) else False

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
            sct_log_formatter = data_path.get_data_path('sct_log_formatter')
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

    def install_grafana(self):
        self.remoter.run('sudo yum install rsync -y')
        self.remoter.run(
            'sudo yum install https://grafanarel.s3.amazonaws.com/builds/grafana-3.1.1-1470047149.x86_64.rpm -y',
            ignore_status=True)
        self.remoter.run('sudo grafana-cli plugins install grafana-piechart-panel')

    def setup_grafana(self, scylla_version=''):
        self.remoter.run('sudo cp /etc/grafana/grafana.ini /tmp/grafana-noedit.ini')
        self.remoter.run('sudo chown %s /tmp/grafana-noedit.ini' % self.remoter.user)
        grafana_ini_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct'),
                                            'grafana.ini')
        self.remoter.receive_files(src='/tmp/grafana-noedit.ini',
                                   dst=grafana_ini_dst_path)

        grafana_cfg = ConfigParser.SafeConfigParser()
        grafana_cfg.read(grafana_ini_dst_path)
        grafana_cfg.set(section='auth.basic', option='enabled', value='false')
        grafana_cfg.set(section='auth.anonymous', option='enabled', value='true')
        grafana_cfg.set(section='auth.anonymous', option='org_role', value='Admin')
        with open(grafana_ini_dst_path, 'wb') as grafana_ini_dst:
            grafana_cfg.write(grafana_ini_dst)
        self.remoter.send_files(src=grafana_ini_dst_path,
                                dst='/tmp/grafana.ini')
        self.remoter.run('sudo mv /tmp/grafana.ini /etc/grafana/grafana.ini')
        self.remoter.run('sudo chmod 666 /etc/grafana/grafana.ini')

        if self.is_docker():
            self.remoter.run('sudo service grafana-server start')
        else:
            self.remoter.run('sudo systemctl daemon-reload')
            self.remoter.run('sudo systemctl start grafana-server')
        self.remoter.run('sudo systemctl enable grafana-server.service')

        def _register_grafana_json(json_filename, url):
            json_path = data_path.get_data_path(json_filename)
            result = process.run('curl -XPOST -i %s --data-binary @%s -H "Content-Type: application/json"' %
                                 (url, json_path), ignore_status=True)
            return result.exit_status == 0

        json_mapping = {'scylla-data-source.json': 'datasources',
                        'scylla-dash-timeout-metrics.json': 'dashboards/db'}

        if platform.linux_distribution()[0].lower() == 'ubuntu':
            process.run('sudo apt-get --assume-yes install git')
        else:
            process.run('sudo yum install git -y')

        scylla_version = get_monitor_version(scylla_version, clone=True)
        for i in glob.glob('data_dir/grafana/*.%s.json' % scylla_version):
            json_mapping[i.replace('data_dir/', '')] = 'dashboards/db'

        for i in glob.glob('data_dir/scylla-dash-per-server-nemesis.%s.json' % scylla_version):
            json_mapping[i.replace('data_dir/', '')] = 'dashboards/db'

        for grafana_json in json_mapping:
            url = 'http://%s:3000/api/%s' % (self.public_ip_address, json_mapping[grafana_json])
            wait.wait_for(_register_grafana_json, step=10,
                          text="Waiting to register 'data_dir/%s' on '%s'..." % (grafana_json, url),
                          json_filename=grafana_json, url=url)

        self.log.info('Prometheus Web UI: http://%s:3000', self.public_ip_address)
        self.log.info('Grafana Web UI: http://%s:3000', self.public_ip_address)

    def _set_prometheus_paths(self):
        self.prometheus_system_base_dir = '/var/tmp'
        self.prometheus_base_dir = 'prometheus-1.0.2.linux-amd64'
        self.prometheus_tarball = '%s.tar.gz' % self.prometheus_base_dir
        self.prometheus_base_url = 'https://github.com/prometheus/prometheus/releases/download/v1.0.2'
        self.prometheus_system_dir = os.path.join(self.prometheus_system_base_dir,
                                                  self.prometheus_base_dir)
        self.prometheus_path = os.path.join(self.prometheus_system_dir, 'prometheus')
        self.prometheus_custom_cfg_basename = 'prometheus-scylla.yml'
        self.prometheus_custom_cfg_path = os.path.join(self.prometheus_system_dir,
                                                       self.prometheus_custom_cfg_basename)
        self.prometheus_data_dir = os.path.join(self.prometheus_system_dir, 'data')
        self.prometheus_service_path_tmp = '/tmp/prometheus.service'
        self.prometheus_service_path = '/etc/systemd/system/prometheus.service'

    def install_prometheus(self):
        self.remoter.run('curl %s/%s -o %s/%s -L' %
                         (self.prometheus_base_url, self.prometheus_tarball,
                          self.prometheus_system_base_dir, self.prometheus_tarball))
        self.remoter.run('tar -xzvf %s/%s -C %s' %
                         (self.prometheus_system_base_dir,
                          self.prometheus_tarball,
                          self.prometheus_system_base_dir), verbose=False)

    def download_prometheus_data_dir(self):
        self.remoter.run('sudo chown -R %s:%s %s' %
                         (self.remoter.user, self.remoter.user, self.prometheus_data_dir))
        dst = os.path.join(self.logdir, 'prometheus')
        self.remoter.receive_files(src=self.prometheus_data_dir, dst=dst)

    def _write_prometheus_cfg(self, targets):
        """
        9180 -> prometheus API server
        9100 -> node exporter
        9103 -> collectd exporter
        """
        scylla_targets_list = ['%s:9180' % str(ip) for ip in targets['db_nodes']]
        node_exporter_targets_list = ['%s:9100' % str(ip) for ip in targets['db_nodes']]
        loader_targets_list = ['%s:9103' % str(ip) for ip in targets['loaders']]
        prometheus_cfg = """
global:
  scrape_interval: 15s

  external_labels:
    monitor: 'scylla-monitor'

scrape_configs:
- job_name: scylla
  honor_labels: true
  static_configs:
  - targets: %s

- job_name: node_exporter
  honor_labels: true
  static_configs:
  - targets: %s

- job_name: stress_metrics
  honor_labels: true
  static_configs:
  - targets: %s

""" % (scylla_targets_list, node_exporter_targets_list, loader_targets_list)

        if self._metrics_target:
            prometheus_cfg += """

- job_name: sct_metrics
  honor_labels: true
  static_configs:
  - targets: %s

""" % [self._metrics_target]

        tmp_dir_prom = tempfile.mkdtemp(prefix='scm-prometheus')
        tmp_path_prom = os.path.join(tmp_dir_prom,
                                     self.prometheus_custom_cfg_basename)
        with open(tmp_path_prom, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(prometheus_cfg)
        try:
            self.remoter.send_files(src=tmp_path_prom,
                                    dst=self.prometheus_custom_cfg_path,
                                    verbose=True,
                                    ssh_timeout=60)
        finally:
            shutil.rmtree(tmp_dir_prom)

    def reconfigure_prometheus(self, targets):
        self._write_prometheus_cfg(targets)
        self.remoter.run('sudo systemctl restart prometheus.service')

    def setup_prometheus(self, targets, metrics_cfg=True, sct_public_ip=None):
        if metrics_cfg:
            default_addr = start_metrics_server()
            if sct_public_ip:
                self._metrics_target = sct_public_ip + ':' + default_addr.split(':')[1]
            else:
                self._metrics_target = default_addr
        self._write_prometheus_cfg(targets)

        systemd_unit = """[Unit]
Description=Prometheus

[Service]
Type=simple
User=root
Group=root
ExecStart=%s -config.file %s -storage.local.path %s

[Install]
WantedBy=multi-user.target
""" % (self.prometheus_path, self.prometheus_custom_cfg_path, self.prometheus_data_dir)
        tmp_dir_prom = tempfile.mkdtemp(prefix='scm-prometheus-systemd')
        tmp_path_prom = os.path.join(tmp_dir_prom, 'prometheus.service')
        with open(tmp_path_prom, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(systemd_unit)
        try:
            self.remoter.send_files(src=tmp_path_prom,
                                    dst=self.prometheus_service_path_tmp)
            self.remoter.run('sudo mv %s %s' %
                             (self.prometheus_service_path_tmp,
                              self.prometheus_service_path))
            if self.is_docker():
                self.remoter.run('sudo %s -config.file %s -storage.local.path %s &' %
                                 (self.prometheus_path, self.prometheus_custom_cfg_path, self.prometheus_data_dir))
            else:
                self.remoter.run('sudo systemctl start prometheus.service')
        finally:
            shutil.rmtree(tmp_dir_prom)

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
                    self.log.info("To decompress you may use: 'pigz --fast {}.gz'".format(base_name))

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

    @log_run_info
    def stop_task_threads(self, timeout=10):
        self.termination_event.set()
        if self._backtrace_thread:
            self._backtrace_thread.join(timeout)
        if self._journal_thread:
            self._journal_thread.join(timeout)
        del(self.remoter)

    def destroy(self):
        raise NotImplementedError('Derived classes must implement destroy')

    def wait_ssh_up(self, verbose=True, timeout=300):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self.remoter.is_up, step=10, text=text, timeout=timeout, throw_exc=True)
        if not self._sct_log_formatter_installed:
            self.install_sct_log_formatter()

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
            stat_line = [element for element in line.strip().split(':') if
                         element]
            if stat_line:
                try:
                    try:
                        if '.' in stat_line[1].split()[0]:
                            stat_dict[stat_line[0]] = float(stat_line[1].split()[0])
                        else:
                            stat_dict[stat_line[0]] = int(stat_line[1].split()[0])
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

    def get_cfstats(self, tcpdump=False):
        def keyspace1_available():
            self.remoter.run('nodetool flush', ignore_status=True)
            res = self.remoter.run('nodetool cfstats keyspace1',
                                   ignore_status=True)
            return res.exit_status == 0
        tcpdump_id = uuid.uuid4()
        if tcpdump:
            self.log.info('START tcpdump thread uuid: %s', tcpdump_id)
            tcpdump_thread = threading.Thread(target=self._get_tcpdump_logs,
                                              kwargs={'tcpdump_id': tcpdump_id})
            tcpdump_thread.start()
        wait.wait_for(keyspace1_available, step=60,
                      text='Waiting until keyspace1 is available')
        try:
            result = self.remoter.run('nodetool cfstats keyspace1')
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

    def search_database_log(self, expression):
        matches = []
        pattern = re.compile(expression, re.IGNORECASE)

        if not os.path.exists(self.database_log):
            return matches
        with open(self.database_log, 'r') as f:
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
        cmd = "sudo sh -c 'echo \"\ndc={}\nrack=RACK1\nprefer_local=true\n\" >> /etc/scylla/cassandra-rackdc.properties'"
        cmd = cmd.format(datacenters[self.dc_idx])
        self.remoter.run(cmd)

    def config_setup(self, seed_address=None, cluster_name=None, enable_exp=True, endpoint_snitch=None,
                     yaml_file=SCYLLA_YAML_PATH, broadcast=None, authenticator=None,
                     server_encrypt=None, client_encrypt=None, append_conf=None):
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

        if enable_exp:
            scylla_yaml_contents += "\nexperimental: true\n"

        if endpoint_snitch:
            p = re.compile('endpoint_snitch:.*')
            scylla_yaml_contents = p.sub('endpoint_snitch: "{0}"'.format(endpoint_snitch),
                                         scylla_yaml_contents)

        if self.is_addition:
            if 'auto_bootstrap' in scylla_yaml_contents:
                p = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = p.sub('auto_bootstrap: True',
                                             scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nauto_bootstrap: True\n"
        else:
            if 'auto_bootstrap' in scylla_yaml_contents:
                p = re.compile('auto_bootstrap:.*')
                scylla_yaml_contents = p.sub('auto_bootstrap: False',
                                             scylla_yaml_contents)
            else:
                scylla_yaml_contents += "\nauto_bootstrap: False\n"

        if authenticator in ['AllowAllAuthenticator', 'PasswordAuthenticator']:
            p = re.compile('[# ]*authenticator:.*')
            scylla_yaml_contents = p.sub('authenticator: {0}'.format(authenticator),
                                         scylla_yaml_contents)

        if server_encrypt or client_encrypt:
            self.remoter.send_files(src='./data_dir/ssl_conf',
                                    dst='/tmp/')
            self.remoter.run('sudo mv /tmp/ssl_conf/* /etc/scylla/')

        if server_encrypt:
            scylla_yaml_contents += """
server_encryption_options:
   internode_encryption: all
   certificate: /etc/scylla/db.crt
   keyfile: /etc/scylla/db.key
   truststore: /etc/scylla/cadb.pem
"""

        if client_encrypt:
            scylla_yaml_contents += """
client_encryption_options:
   enabled: true
   certificate: /etc/scylla/db.crt
   keyfile: /etc/scylla/db.key
"""

        if self.replacement_node_ip:
            logger.debug("%s is a replacement node for '%s'." % (self.name, self.replacement_node_ip))
            scylla_yaml_contents += "\nreplace_address: %s\n" % self.replacement_node_ip
        else:
            scylla_yaml_contents = scylla_yaml_contents.replace("replace_address", "#replace_address")

        if append_conf:
            scylla_yaml_contents += append_conf

        with open(yaml_dst_path, 'w') as f:
            f.write(scylla_yaml_contents)

        self.remoter.send_files(src=yaml_dst_path,
                                dst='/tmp/scylla.yaml')
        self.remoter.run('sudo mv /tmp/scylla.yaml {}'.format(yaml_file))

    def download_scylla_repo(self, scylla_repo, repo_path='/etc/yum.repos.d/scylla.repo'):
        self.remoter.run('sudo curl -o %s -L %s' % (repo_path, scylla_repo))

    def clean_scylla(self):
        """
        Uninstall scylla
        """
        self.remoter.run('sudo systemctl stop scylla-server.service', ignore_status=True)
        self.remoter.run('sudo yum remove -y "scylla*"')
        self.remoter.run('sudo yum clean all')
        self.remoter.run('sudo rm -rf /var/lib/scylla/commitlog/*')
        self.remoter.run('sudo rm -rf /var/lib/scylla/data/*')

    def install_scylla(self, scylla_repo):
        """
        Download and install scylla on node
        :param scylla_repo: scylla repo file URL
        """
        result = self.remoter.run('ls /etc/yum.repos.d/epel.repo', ignore_status=True)
        if result.exit_status == 0:
            self.remoter.run('sudo yum update -y --skip-broken --disablerepo=epel', retry=3)
        else:
            self.remoter.run('sudo yum update -y --skip-broken', retry=3)
        self.remoter.run('sudo yum install -y rsync tcpdump screen wget net-tools')
        self.download_scylla_repo(scylla_repo)
        self.remoter.run('sudo yum install -y {}'.format(self.scylla_pkg()))
        self.remoter.run('sudo yum install -y {}-gdb'.format(self.scylla_pkg()), ignore_status=True)

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
        self.remoter.run('sudo /usr/lib/scylla/scylla_setup --nic eth0 --disks {}'.format(','.join(disks)))
        self.remoter.run('sudo sync')
        self.log.info('io.conf right after setup')
        self.remoter.run('sudo cat /etc/scylla.d/io.conf')
        self.remoter.run('sudo systemctl enable scylla-server.service')
        self.remoter.run('sudo systemctl enable scylla-jmx.service')

    def install_mgmt(self, scylla_repo, scylla_mgmt_repo, mgmt_port, db_hosts):
        self.log.debug('Install scylla-manager')
        rsa_id_dst = '/tmp/scylla-test'
        mgmt_user = 'scylla-manager'
        mgmt_conf_tmp = '/tmp/scylla-manager.yaml'
        mgmt_conf_dst = '/etc/scylla-manager/scylla-manager.yaml'

        self.remoter.run('sudo yum install -y epel-release', retry=3)
        self.download_scylla_repo(scylla_repo)
        self.download_scylla_repo(scylla_mgmt_repo, repo_path='/etc/yum.repos.d/scylla-manager.repo')
        if self.is_docker():
            self.remoter.run('sudo yum remove -y scylla scylla-jmx scylla-tools scylla-tools-core'
                             ' scylla-server scylla-conf')
        self.remoter.run('sudo yum install -y scylla-manager')
        if self.is_docker():
            try:
                self.remoter.run('echo no| sudo scyllamgr_setup')
            except Exception as ex:
                pass
        else:
            self.remoter.run('echo yes| sudo scyllamgr_setup')
        self.remoter.send_files(src=self._ssh_login_info['key_file'], dst=rsa_id_dst)
        self.remoter.run('sudo chmod 0400 {}'.format(rsa_id_dst))
        self.remoter.run('sudo chown {}:{} {}'.format(mgmt_user, mgmt_user, rsa_id_dst))

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
        self.remoter.run('sudo systemctl start scylla-server.service', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=timeout)

    def start_scylla_jmx(self, verify_up=True, verify_down=False, timeout=300):
        if verify_down:
            self.wait_jmx_down(timeout=timeout)
        self.remoter.run('sudo systemctl start scylla-jmx.service', timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=timeout)

    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        self.start_scylla_jmx(verify_up=verify_up, verify_down=verify_down, timeout=timeout)

    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('sudo systemctl stop scylla-server.service', timeout=timeout)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    def stop_scylla_jmx(self, verify_up=False, verify_down=True, timeout=300):
        if verify_up:
            self.wait_jmx_up(timeout=timeout)
        self.remoter.run('sudo systemctl stop scylla-jmx.service', timeout=timeout)
        if verify_down:
            self.wait_jmx_down(timeout=timeout)

    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        self.stop_scylla_jmx(verify_up=verify_up, verify_down=verify_down, timeout=timeout)


class BaseCluster(object):

    """
    Cluster of Node objects.
    """

    def __init__(self, cluster_uuid=None, cluster_prefix='cluster',
                 node_prefix='node', n_nodes=[10], params=None, region_names=None):
        super(BaseCluster, self).__init__()
        if cluster_uuid is None:
            self.uuid = uuid.uuid4()
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
            self._node_public_ips = self.params.get(self._get_node_ips_param(), None) or []
            self._node_private_ips = self.params.get(self._get_node_ips_param('private'), None) or []
            self.log.debug('Node public IPs: {}, private IPs: {}'.format(self._node_public_ips, self._node_private_ips))

        if isinstance(n_nodes, list):
            for dc_idx, num in enumerate(n_nodes):
                self.add_nodes(num, dc_idx=dc_idx)
        elif isinstance(n_nodes, int):  # legacy type
            self.add_nodes(n_nodes)
        else:
            raise ValueError('Unsupported type: {}'.format(type(n_nodes)))
        self.coredumps = dict()

    @classmethod
    def _get_node_ips_param(cls, ip_type='public'):
        return 'db_nodes_public_ip' if ip_type == 'public' else 'db_nodes_private_ip'

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

    def add_nodes(self, count, ec2_user_data='', dc_idx=0):
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

    def set_tc(self, node, dst_nodes, local_nodes):
        # FIXME: local_nodes isn't used
        node.remoter.run("sudo modprobe sch_netem")
        node.remoter.run("sudo tc qdisc del dev eth0 root", ignore_status=True)
        node.remoter.run("sudo tc qdisc add dev eth0 handle 1: root prio")
        for dst in dst_nodes:
            node.remoter.run("sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {} flowid 2:1".format(dst.private_ip_address))
        node.remoter.run("sudo tc qdisc add dev eth0 parent 1:1 handle 2:1 netem delay 100ms 20ms 25% reorder 5% 25% loss random 5% 25%")
        node.remoter.run("sudo tc qdisc show dev eth0", verbose=True)
        node.remoter.run("sudo tc filter show dev eth0", verbose=True)

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()

    def terminate_node(self, node):
        self.nodes.remove(node)
        node.destroy()


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
        setup_kwargs = {k: kwargs[k] for k in kwargs if k not in ('node_list', 'timeout')}

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
                assert node_status is True, 'Setup has failed!'
                results.append(node)
                cl_inst.log.info("(%d/%d) nodes ready, node %s. Time elapsed: %d s",
                                 len(results), len(node_list), str(node), int(time_elapsed))
            except Queue.Empty:
                pass
            if timeout and time_elapsed / 60 > timeout:
                msg = 'TIMEOUT [%d min]: Waiting for node(-s) setup(%d/%d) expired!' % (
                    timeout, len(results), len(node_list))
                cl_inst.log.error(msg)
                raise Exception(msg)

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

    def get_seed_nodes_by_flag(self):
        """
        We set is_seed at two point, before and after node_setup.
        However, we can only call this function when the flag is set.
        """
        node_private_ips = [node.private_ip_address for node
                            in self.nodes if node.is_seed]
        seeds = ",".join(node_private_ips)
        if not seeds:
            # use first node as seed by default
            seeds = self.nodes[0].private_ip_address
            self.nodes[0].is_seed = True
        return seeds

    def _update_db_binary(self, new_scylla_bin, node_list):
        self.log.debug('User requested to update DB binary...')

        seed_nodes = self.get_seed_nodes()
        non_seed_nodes = [n for n in self.nodes if not n.is_seed]

        def update_scylla_bin(node, queue):
            node.log.info('Updating DB binary')
            node.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)
            # replace the binary
            node.remoter.run('sudo cp -f /usr/bin/scylla /usr/bin/scylla.origin')
            node.remoter.run('sudo cp -f /tmp/scylla /usr/bin/scylla')
            node.remoter.run('sudo chown root.root /usr/bin/scylla')
            node.remoter.run('sudo chmod +x  /usr/bin/scylla')
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

    def get_scylla_version(self):
        if not self.nodes[0].scylla_version:
            try:
                result = self.nodes[0].remoter.run("rpm -q {}".format(self.nodes[0].scylla_pkg()))
            except Exception as ex:
                self.log.error('Failed getting scylla version: %s', ex)
            else:
                match = re.findall("scylla-(.*).el7.centos", result.stdout)
                if match:
                    for node in self.nodes:
                        node.scylla_version = match[0]

    def cfstat_reached_threshold(self, key, threshold):
        """
        Find whether a certain cfstat key in all nodes reached a certain value.

        :param key: cfstat key, example, 'Space used (total)'.
        :param threshold: threshold value for cfstats key. Example, 2432043080.
        :return: Whether all nodes reached that threshold or not.
        """
        tcpdump = self.params.get('tcpdump')
        cfstats = [node.get_cfstats(tcpdump)[key] for node in self.nodes]
        self.log.debug("Current cfstats: %s" % cfstats)
        reached_threshold = True
        if max(cfstats) < threshold:
            reached_threshold = False
        if reached_threshold:
            self.log.debug("Done waiting on cfstats: %s" % cfstats)
        return reached_threshold

    def wait_cfstat_reached_threshold(self, key, threshold):
        text = "Waiting until cfstat '%s' reaches value '%s'" % (
            key, threshold)
        wait.wait_for(func=self.cfstat_reached_threshold, step=10,
                      text=text, key=key, threshold=threshold)

    def wait_total_space_used_per_node(self, size=None):
        if size is None:
            size = int(self.params.get('space_node_threshold'))
        self.wait_cfstat_reached_threshold('Space used (total)', size)

    def add_nemesis(self, nemesis, loaders, monitoring_set, **kwargs):
        self.nemesis.append(nemesis(cluster=self,
                                    loaders=loaders,
                                    monitoring_set=monitoring_set,
                                    termination_event=self.termination_event, **kwargs))

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
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(timeout)
        self.nemesis_threads = []

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

    def _node_setup(self, node, verbose=False):
        """
        Install, configure and run scylla on node
        :param node: scylla node object
        :param verbose:
        """
        node.wait_ssh_up(verbose=verbose)

        if not Setup.REUSE_CLUSTER:
            node.clean_scylla()
            node.install_scylla(scylla_repo=self.params.get('scylla_repo'))

            endpoint_snitch = ''
            if len(self.datacenter) > 1:
                endpoint_snitch = 'GossipingPropertyFileSnitch'
                node.datacenter_setup(self.datacenter)
            authenticator = self.params.get('authenticator')
            seed_address = self.get_seed_nodes_by_flag()

            def node_config_setup():
                node.config_setup(seed_address=seed_address,
                                  cluster_name=self.name,
                                  enable_exp=self._param_enabled('experimental'),
                                  endpoint_snitch=endpoint_snitch,
                                  authenticator=authenticator,
                                  server_encrypt=self._param_enabled('server_encrypt'),
                                  client_encrypt=self._param_enabled('client_encrypt'),
                                  append_conf=self.params.get('append_conf'))

            node_config_setup()
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

        node.wait_db_up()
        node.wait_jmx_up()
        if node.replacement_node_ip:
            # If this is a replacement node, we need to set back configuration in case
            # when scylla-server process will be restarted
            node.replacement_node_ip = None
            node_config_setup()



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


class BaseLoaderSet(object):

    def __init__(self, *args, **kwargs):
        self._loader_queue = []
        self._install_cs = False
        self.params = kwargs.get('params', {})

    def node_setup(self, node, verbose=False, db_node_address=None):
        self.log.info('Setup in BaseLoaderSet')
        node.wait_ssh_up(verbose=verbose)

        if Setup.REUSE_CLUSTER:
            self.kill_stress_thread()
            return

        if self._install_cs:
            node.download_scylla_repo(self.params.get('scylla_repo'))
            node.remoter.run('sudo yum install -y {}-tools'.format(node.scylla_pkg()))

        node.wait_cs_installed(verbose=verbose)
        node.remoter.run('sudo yum install -y screen')
        if db_node_address is not None:
            node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" %
                             db_node_address)

        cs_exporter_setup = CassandraStressExporterSetup()
        cs_exporter_setup.install(node)

        if self.params.get('bench_run', default=False):
            # go1.7 still not in repo
            node.remoter.run('sudo yum install git -y')
            node.remoter.run('curl -LO https://storage.googleapis.com/golang/go1.7.linux-amd64.tar.gz')
            node.remoter.run('sudo tar -C /usr/local -xvzf go1.7.linux-amd64.tar.gz')
            node.remoter.run("echo 'export GOPATH=$HOME/go' >> $HOME/.bashrc")
            node.remoter.run("echo 'export PATH=$PATH:/usr/local/go/bin' >> $HOME/.bashrc")
            node.remoter.run("source $HOME/.bashrc")
            node.remoter.run("go get github.com/scylladb/scylla-bench")

    @wait_for_init_wrap
    def wait_for_init(self, verbose=False, db_node_address=None):
        pass

    @classmethod
    def get_node_ips_param(cls, ip_type='public'):
        return 'loaders_public_ip' if ip_type == 'public' else 'loaders_private_ip'

    def get_loader(self):
        if not self._loader_queue:
            self._loader_queue = [node for node in self.nodes]
        return self._loader_queue.pop(0)

    def run_stress_thread(self, stress_cmd, timeout, output_dir, stress_num=1, keyspace_num=1, keyspace_name='',
                          profile=None, node_list=[], round_robin=False):
        if keyspace_name:
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace={} ".format(keyspace_name))
        else:
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
                stress_cmd += " -node {}".format(first_node.private_ip_address)
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
                                      watch_stdout_pattern='total,',
                                      log_file=log_file_name)
            node.cs_start_time = result.stdout_pattern_found_at
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
            # Parse loader & cpu info
            if line.startswith('TAG:'):
                ret = re.findall("TAG: loader_idx:(\d+)-cpu_idx:(\d+)-keyspace_idx:(\d+)", line)
                results['loader_idx'] = ret[0][0]
                results['cpu_idx'] = ret[0][1]
                results['keyspace_idx'] = ret[0][2]

            if line.startswith('Results:'):
                enable_parse = True
                continue
            if line == 'END':
                break
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
            results[key] = value
            m = re.findall('.*READ:(\d+), WRITE:(\d+)]', line)
            if m:  # parse results for mixed workload
                results['%s read' % key] = m[0][0]
                results['%s write' % key] = m[0][1]

        if not enable_parse:
            logger.warning('Cannot find summary in c-stress results: %s', lines)
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
    def _plot_nemesis_events(nemesis, node):
        nemesis_event_start_times = [
            operation['start'] - node.cs_start_time for operation in nemesis.operation_log]
        for start_time in nemesis_event_start_times:
            plt.axvline(start_time, color='blue', linestyle='dashdot')
        nemesis_event_end_times = [
            operation['end'] - node.cs_start_time for operation in nemesis.operation_log]
        for end_time in nemesis_event_end_times:
            plt.axvline(end_time, color='red', linestyle='dashdot')

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

    def _plot_metric_data(self, cs_results, x_title, y_title, color, title,
                          db_cluster, plotfile, node):
        for nemesis in db_cluster.nemesis:
            self._plot_nemesis_events(nemesis, node)
        plt.plot(cs_results[x_title], cs_results[y_title], label=y_title,
                 color=color)
        plt.title(title)
        plt.xlabel(x_title)
        plt.ylabel(y_title)
        plt.legend()
        plt.savefig(plotfile + '-%s.svg' % y_title)
        plt.savefig(plotfile + '-%s.png' % y_title)
        plt.close()

    def _cassandra_stress_plot(self, lines, plotfile='plot', node=None,
                               db_cluster=None):
        cs_results = self._parse_cs_results(lines)

        self._plot_metric_data(cs_results=cs_results, x_title='time',
                               y_title='ops', color='green',
                               title='Operations vs Time',
                               db_cluster=db_cluster,
                               plotfile=plotfile,
                               node=node)

        self._plot_metric_data(cs_results=cs_results, x_title='time',
                               y_title='lat95', color='blue',
                               title='Latency 95% vs Time',
                               db_cluster=db_cluster,
                               plotfile=plotfile,
                               node=node)

        self._plot_metric_data(cs_results=cs_results, x_title='time',
                               y_title='lat99', color='green',
                               title='Latency 99% vs Time',
                               db_cluster=db_cluster,
                               plotfile=plotfile,
                               node=node)

        self._plot_metric_data(cs_results=cs_results, x_title='time',
                               y_title='lat999', color='black',
                               title='Latency 99.9% vs Time',
                               db_cluster=db_cluster,
                               plotfile=plotfile,
                               node=node)

        self._plot_metric_data(cs_results=cs_results, x_title='time',
                               y_title='latmax', color='red',
                               title='Maximum Latency vs Time',
                               db_cluster=db_cluster,
                               plotfile=plotfile,
                               node=node)

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
            plotfile = os.path.join(self.logdir, str(node))
            self._cassandra_stress_plot(lines, plotfile, node, db_cluster)

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
            result = node.remoter.run(cmd="/$HOME/go/bin/{0} -nodes {1}".format(stress_cmd.strip(), ips),
                                      timeout=timeout,
                                      ignore_status=True,
                                      watch_stdout_pattern='Latency:',
                                      log_file=log_file_name)
            node.cs_start_time = result.stdout_pattern_found_at
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


class BaseMonitorSet(object):

    grafana_start_time = None
    scylla_version = ''
    is_enterprise = None

    @classmethod
    def get_node_ips_param(cls, ip_type='public'):
        return 'monitor_nodes_public_ip' if ip_type == 'public' else 'monitor_nodes_private_ip'

    def node_setup(self, node, targets=None, verbose=False, scylla_version='', is_enterprise=None, **kwargs):
        self.log.info('Setup in BaseMonitorSet')
        node.wait_ssh_up(verbose=verbose)

        if Setup.REUSE_CLUSTER:
            # We have to start http server to expose the metrics via prometheus client
            node.setup_prometheus(targets=targets, sct_public_ip=self.params.get('sct_public_ip'))
            return

        # The init scripts should install/update c-s, so
        # let's try to guarantee it will be there before
        # proceeding
        node.install_prometheus()
        node.setup_prometheus(targets=targets, sct_public_ip=self.params.get('sct_public_ip'))
        node.install_grafana()
        self.scylla_version = scylla_version
        self.is_enterprise = is_enterprise
        node.setup_grafana(scylla_version)
        # The time will be used in url of Grafana monitor,
        # the data from this point to the end of test will
        # be captured.
        self.grafana_start_time = time.time()
        node.remoter.run('sudo yum install screen -y')
        if 'scylla_repo' in kwargs and 'scylla_mgmt_repo' in kwargs:
            if kwargs.get('mgmt_db_local') is True:
                mgmt_db_hosts = ['127.0.0.1']
            else:
                mgmt_db_hosts = [str(trg) for trg in targets['db_nodes']]
            node.install_mgmt(scylla_repo=kwargs.get('scylla_repo'),
                              scylla_mgmt_repo=kwargs.get('scylla_mgmt_repo'),
                              mgmt_port=kwargs.get('mgmt_port'),
                              db_hosts=mgmt_db_hosts)

    @wait_for_init_wrap
    def wait_for_init(self, targets=None, verbose=False, scylla_version='', is_enterprise=None, **kwargs):
        pass

    def get_monitor_snapshot(self):
        """
        Take snapshot for grafana monitor in the end of test
        """
        phantomjs_url = "https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2"
        phantomjs_tar = os.path.basename(phantomjs_url)
        if not self.grafana_start_time:
            self.log.error("grafana isn't setup, skip to get snapshot")
            return
        start_time = str(self.grafana_start_time).split('.')[0] + '000'

        try:
            process.run('curl "{}" -o {} -L'.format(phantomjs_url, phantomjs_tar))
            process.run('tar xvfj {}'.format(phantomjs_tar),
                        verbose=False)
            process.run("cd phantomjs-2.1.1-linux-x86_64 && "
                        "sed -e 's/200);/10000);/' examples/rasterize.js |grep -v 'use strict' > r.js",
                        shell=True)
            scylla_version = get_monitor_version(self.scylla_version)
            version = scylla_version.replace('.', '-')

            scylla_pkg = 'scylla-enterprise' if self.is_enterprise else 'scylla'
            for n, node in enumerate(self.nodes):
                grafana_url = "http://%s:3000/dashboard/db/%s-per-server-metrics-%s?from=%s&to=now" % (
                              node.public_ip_address, scylla_pkg, version, start_time)
                snapshot_path = os.path.join(self.logdir,
                                             "grafana-snapshot-%s.png" % n)
                process.run("sudo yum install fontconfig bitmap-fonts -y")
                process.run("cd phantomjs-2.1.1-linux-x86_64 && "
                            "bin/phantomjs r.js \"%s\" \"%s\" 1920px" % (
                             grafana_url, snapshot_path), shell=True)
        except Exception, details:
            self.log.error('Error taking monitor snapshot: %s',
                           str(details))

    def download_monitor_data(self):
        self.log.debug('Download monitor data')
        for node in self.nodes:
            try:
                node.remoter.run('sudo systemctl stop prometheus.service', ignore_status=True)
                if node.prometheus_data_dir:
                    node.download_prometheus_data_dir()
            except Exception, details:
                self.log.error('Error downloading prometheus data dir: %s',
                               str(details))


class NoMonitorSet(object):

    def __init__(self):
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.nodes = []

    def __str__(self):
        return 'NoMonitorSet'

    def wait_for_init(self, targets, verbose=False, scylla_version='', is_enterprise=None):
        del targets
        del verbose
        self.log.info('Monitor nodes disabled for this run')

    def get_backtraces(self):
        pass

    def get_monitor_snapshot(self):
        pass

    def download_monitor_data(self):
        pass

    def destroy(self):
        pass
