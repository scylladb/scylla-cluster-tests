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
import re
import tempfile
import threading
import time
import uuid
import yaml
import matplotlib
import platform
import subprocess
import shutil
import glob
import xml.etree.cElementTree as etree

# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from avocado.utils import path
from avocado.utils import process
from avocado.utils import script
from avocado.utils import runtime as avocado_runtime

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from .log import SDCMAdapter
from .remote import Remote
from .remote import disable_master_ssh
from . import data_path
from . import wait
from .utils import get_monitor_version

from .collectd import ScyllaCollectdSetup

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
GCE_INSTANCES = []
GCE_SERVICE = None
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


def get_openstack_service(user, password, auth_version, auth_url, service_type, service_name, service_region, tenant):
    service_cls = get_driver(Provider.OPENSTACK)
    service = service_cls(user, password,
                          ex_force_auth_version=auth_version,
                          ex_force_auth_url=auth_url,
                          ex_force_service_type=service_type,
                          ex_force_service_name=service_name,
                          ex_force_service_region=service_region,
                          ex_tenant_name=tenant)
    return service


def clean_openstack_instance(user, password, auth_version, auth_url, service_type, service_name, service_region,
                             tenant, instance_name):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        instance = [n for n in service.list_nodes() if n.name == instance_name][0]
        service.destroy_node(instance)
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def clean_openstack_credential(user, password, auth_version, auth_url, service_type, service_name, service_region,
                               tenant, credential_key_name, credential_key_file):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        key_pair = service.get_key_pair(credential_key_name)
        service.delete_key_pair(key_pair)
        remove_if_exists(credential_key_file)
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))


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


class NodeError(Exception):

    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        if self.msg is not None:
            return self.msg


class LoaderSetInitError(Exception):
    pass


def _prepend_user_prefix(user_prefix, base_name):
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
        key_prefix = _prepend_user_prefix(user_prefix, key_prefix)
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

        self._ssh_ip_mapping = {'public': self.public_ip_address,
                                'private': self.private_ip_address}
        ssh_login_info['hostname'] = self._ssh_ip_mapping[IP_SSH_CONNECTIONS]

        self.remoter = Remote(**ssh_login_info)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug(self.remoter.ssh_debug_cmd())

        self._journal_thread = None
        self.n_coredumps = 0
        self._backtrace_thread = None
        self._public_ip_address = None
        self._private_ip_address = None
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
        result = self.remoter.run('test -e /var/tmp/sct_log_formatter',
                                  ignore_status=True)
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
        self.remoter.run('sudo yum install https://grafanarel.s3.amazonaws.com/builds/grafana-3.1.1-1470047149.x86_64.rpm -y')
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
        if scylla_version == 'master':
            json_mapping['scylla-dash-per-server-nemesis.master.json'] = 'dashboards/db'

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
        self._set_prometheus_paths()
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
        scylla_targets_list = ['%s:9180' % str(ip) for ip in targets['db_nodes'] + targets['loaders']]
        node_exporter_targets_list = ['%s:9100' % str(ip) for ip in targets['db_nodes'] + targets['loaders']]
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

    def setup_prometheus(self, targets, metrics_cfg=True):
        if metrics_cfg:
            self._metrics_target = start_metrics_server()
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
        base_upload_url = 'scylladb-users-upload.s3.amazonaws.com/%s/%s'
        coredump_id = os.path.basename(coredump)[:-3]
        upload_url = base_upload_url % (coredump_id, os.path.basename(coredump))
        self.log.info('Uploading coredump %s to %s' % (coredump, upload_url))
        self.remoter.run("sudo curl --request PUT --upload-file "
                         "'%s' '%s'" % (coredump, upload_url))
        self.log.info("To download it, you may use "
                      "'curl --user-agent [user-agent] %s > %s'",
                      upload_url, os.path.basename(coredump))

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

    def stop_task_threads(self, timeout=10):
        del(self.remoter)
        self.termination_event.set()
        if self._backtrace_thread:
            self._backtrace_thread.join(timeout)
        if self._journal_thread:
            self._journal_thread.join(timeout)

    def destroy(self):
        raise NotImplementedError('Derived classes must implement destroy')

    def wait_ssh_up(self, verbose=True, timeout=None):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self.remoter.is_up, step=10, text=text, timeout=timeout, throw_exc=True)
        if not self._sct_log_formatter_installed:
            self.install_sct_log_formatter()

    def db_up(self):
        try:
            result = self.remoter.run('netstat -l | grep :9042',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
            self.log.error('Error checking for DB status: %s', details)
            return False

    def jmx_up(self):
        try:
            result = self.remoter.run('netstat -l | grep :7199',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
            self.log.error('Error checking for JMX status: %s', details)
            return False

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

    def _report_housekeeping_uuid(self, verbose=True):
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

    def wait_db_up(self, verbose=True, timeout=None):
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

    def wait_db_down(self, verbose=True, timeout=None):
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

    def config_setup(self, seed_address=None, cluster_name=None, enable_exp=True, endpoint_snitch=None, yaml_file='/etc/scylla/scylla.yaml', broadcast=None, authenticator=None, server_encrypt=None, client_encrypt=None, append_conf=None):
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
            p = re.compile('rpc_address:.*')
            scylla_yaml_contents = p.sub('rpc_address: {0}'.format(self.private_ip_address),
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
            scylla_yaml_contents = scylla_yaml_contents.replace("cluster_name: 'Test Cluster'",
                                                                "cluster_name: '{0}'".format(cluster_name))

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

        if append_conf:
            scylla_yaml_contents += append_conf

        with open(yaml_dst_path, 'w') as f:
            f.write(scylla_yaml_contents)

        self.remoter.send_files(src=yaml_dst_path,
                                dst='/tmp/scylla.yaml')
        self.remoter.run('sudo mv /tmp/scylla.yaml {}'.format(yaml_file))


class OpenStackNode(BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, openstack_instance, openstack_service, credentials,
                 node_prefix='node', node_index=1, openstack_image_username='root',
                 base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = openstack_instance
        self._openstack_service = openstack_service
        self._wait_private_ip()
        ssh_login_info = {'hostname': None,
                          'user': openstack_image_username,
                          'key_file': credentials.key_file,
                          'wait_key_installed': 30,
                          'extra_ssh_options': '-tt'}
        super(OpenStackNode, self).__init__(name=name,
                                            ssh_login_info=ssh_login_info,
                                            base_logdir=base_logdir,
                                            node_prefix=node_prefix)

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_private_ip_address()

    def _get_public_ip_address(self):
        public_ips, _ = self._refresh_instance_state()
        if public_ips:
            return public_ips[0]
        else:
            return None

    def _get_private_ip_address(self):
        _, private_ips = self._refresh_instance_state()
        if private_ips:
            return private_ips[0]
        else:
            return None

    def _wait_private_ip(self):
        _, private_ips = self._refresh_instance_state()
        while not private_ips:
            time.sleep(1)
            _, private_ips = self._refresh_instance_state()

    def _refresh_instance_state(self):
        node_name = self._instance.name
        instance = [n for n in self._openstack_service.list_nodes() if n.name == node_name][0]
        self._instance = instance
        ip_tuple = (instance.public_ips, instance.private_ips)
        return ip_tuple

    def restart(self):
        self._instance.reboot()

    def destroy(self):
        self._instance.destroy()
        self.stop_task_threads()
        self.log.info('Destroyed')


class LibvirtNode(BaseNode):

    """
    Wraps a domain object, so that we can also control the it through SSH.
    """

    def __init__(self, domain, hypervisor, node_prefix='node', node_index=1,
                 domain_username='root', domain_password='', base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._backing_image = None
        self._domain = domain
        self._hypervisor = hypervisor
        wait.wait_for(self._domain.isActive)
        self._wait_public_ip()
        ssh_login_info = {'hostname': None,
                          'user': domain_username,
                          'password': domain_password}
        super(LibvirtNode, self).__init__(name=name,
                                          ssh_login_info=ssh_login_info,
                                          base_logdir=base_logdir,
                                          node_prefix=node_prefix)

    def _get_public_ip_address(self):
        desc = etree.fromstring(self._domain.XMLDesc(0))
        mac_path = "devices/interface[@type='bridge']/mac"
        node = desc.find(mac_path)
        if node is None:
            return None

        mac = node.get("address")
        if mac is None:
            return None

        mac = mac.lower().strip()
        output = subprocess.Popen(["arp", "-n"],
                                  stdout=subprocess.PIPE).communicate()[0]
        lines = [line.split() for line in output.split("\n")[1:]]
        addresses = [line[0] for line in lines if (line and (line[2] == mac))]
        if addresses:
            # Just return the first address, this is a best effort attempt
            return addresses[0]

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_public_ip_address()

    def _wait_public_ip(self):
        while self._get_public_ip_address() is None:
            time.sleep(1)

    # Remove after node setup is finished
    def db_up(self):
        try:
            result = self.remoter.run('netstat -l | grep :9042',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
            self.log.error('Error checking for DB status: %s', details)
            return False

    # Remove after node setup is finished
    def cs_installed(self, cassandra_stress_bin=None):
        if cassandra_stress_bin is None:
            cassandra_stress_bin = '/usr/bin/cassandra-stress'
        return self.file_exists(cassandra_stress_bin)

    def restart(self):
        self._domain.reboot()
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self.public_ip_address)
        self.remoter.hostname = self.public_ip_address
        self.wait_db_up()

    def destroy(self):
        self._domain.destroy()
        self._domain.undefine()
        remove_if_exists(self._backing_image)
        self.stop_task_threads()
        self.log.info('Destroyed')


class BaseCluster(object):

    """
    Cluster of Node objects.
    """

    def __init__(self, cluster_uuid=None, cluster_prefix='cluster',
                 node_prefix='node', n_nodes=[10], params=None, region_names=None):
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
        self.datacenter = region_names
        if isinstance(n_nodes, list):
            for dc_idx, num in enumerate(n_nodes):
                self.add_nodes(num, dc_idx=dc_idx)
        elif isinstance(n_nodes, int):  # legacy type
            self.add_nodes(n_nodes)
        else:
            raise ValueError('Unsupported type: {}'.format(type(n_nodes)))
        self.coredumps = dict()

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
        pass

    def get_node_private_ips(self):
        return [node.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.public_ip_address for node in self.nodes]

    def get_node_database_errors(self):
        errors = []
        for node in self.nodes:
            node_errors = node.search_database_log_errors()
            if node_errors:
                errors.append({node.name: node_errors})
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


class BaseScyllaCluster(object):

    def get_seed_nodes_private_ips(self):
        if self.seed_nodes_private_ips is None:
            node = self.nodes[0]
            yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct'),
                                         'scylla.yaml')
            node.remoter.receive_files(src='/etc/scylla/scylla.yaml',
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
            seeds = self.nodes[0].is_seed = True
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
            node.wait_db_up()
            node.remoter.run('sudo systemctl stop scylla-server.service')
            node.remoter.run('sudo systemctl stop scylla-jmx.service')
            node.wait_db_down()
            queue.put(node)
            queue.task_done()

        def start_scylla(node, queue):
            node.wait_db_down()
            node.remoter.run('sudo systemctl start scylla-server.service')
            node.remoter.run('sudo systemctl start scylla-jmx.service')
            node.wait_db_up()
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
            node.wait_db_up()
            node.remoter.run('sudo systemctl stop scylla-server.service')
            node.remoter.run('sudo systemctl stop scylla-jmx.service')
            node.wait_db_down()
            queue.put(node)
            queue.task_done()

        def start_scylla(node, queue):
            node.wait_db_down()
            try:
                node.remoter.run('sudo systemctl start scylla-server.service')
                node.remoter.run('sudo systemctl start scylla-jmx.service')
            except Exception as e:
                node.remoter.run('sudo systemctl status scylla-server.service | true')
                node.remoter.run('sudo systemctl status scylla-jmx.service | true')
                print e
            node.wait_db_up()
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

    def cfstat_reached_threshold(self, key, threshold):
        """
        Find whether a certain cfstat key in all nodes reached a certain value.

        :param key: cfstat key, example, 'Space used (total)'.
        :param threshold: threshold value for cfstats key. Example, 2432043080.
        :return: Whether all nodes reached that threshold or not.
        """
        tcpdump = self.params.get('tcpdump')
        cfstats = [node.get_cfstats(tcpdump)[key] for node in self.nodes]
        reached_threshold = True
        for value in cfstats:
            if value < threshold:
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

    def start_nemesis(self, interval=30):
        self.log.debug('Start nemesis begin')
        self.termination_event = threading.Event()
        for nemesis in self.nemesis:
            nemesis.set_termination_event(self.termination_event)
            nemesis.set_target_node()
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval,), verbose=True)
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)
        self.log.debug('Start nemesis end')

    def stop_nemesis(self, timeout=10):
        self.log.debug('Stop nemesis begin')
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(timeout)
        self.nemesis_threads = []
        self.log.debug('Stop nemesis end')

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

    def _wait_for_node_setup(self, node_list, verbose=False, timeout=None):
        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            self._node_setup(node=node)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y {}-gdb'.format(node.scylla_pkg()),
                             verbose=verbose, ignore_status=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        # avoid using node.remoter in thread
        for node in node_list:
            node.wait_ssh_up(verbose=verbose)
            self.collectd_setup.install(node)

        for node in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node, ))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            time_elapsed = time.time() - start_time
            try:
                results.append(queue.get(block=True, timeout=5))
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list), int(time_elapsed))
            except Queue.Empty:
                pass
            if timeout and time_elapsed / 60 > timeout:
                self.log.error('Stop waiting for node(-s) setup(%d/%d): timeout - %d min - has expired',
                               len(results), len(node_list), timeout)
                return False

        self.update_db_binary(node_list)
        self.update_db_packages(node_list)
        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))
        return True


class BaseLoaderSet(object):

    def wait_for_init(self, verbose=False, db_node_address=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in BaseLoaderSet')
            node.wait_ssh_up(verbose=verbose)

            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.wait_cs_installed(verbose=verbose)
            node.remoter.run('sudo yum install -y screen')
            if db_node_address is not None:
                node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" %
                                 db_node_address)

            cs_exporter_setup = CassandraStressExporterSetup()
            cs_exporter_setup.install(node)

            if self.params.get('bench_run', default='').lower() == 'true':
                # go1.7 still not in repo
                node.remoter.run('sudo yum install git -y')
                node.remoter.run('curl -LO https://storage.googleapis.com/golang/go1.7.linux-amd64.tar.gz')
                node.remoter.run('sudo tar -C /usr/local -xvzf go1.7.linux-amd64.tar.gz')
                node.remoter.run("echo 'export GOPATH=$HOME/go' >> $HOME/.bashrc")
                node.remoter.run("echo 'export PATH=$PATH:/usr/local/go/bin' >> $HOME/.bashrc")
                node.remoter.run("source $HOME/.bashrc")
                node.remoter.run("go get github.com/pdziepak/scylla-bench")

            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))

    def run_stress_thread(self, stress_cmd, timeout, output_dir, stress_num=1, keyspace_num=1, profile=None,
                          node_list=[]):
        stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace=keyspace$2 ")
        if profile:
            with open(profile) as fp:
                profile_content = fp.read()
                cs_profile = script.TemporaryScript(name=os.path.basename(profile), content=profile_content)
                self.log.info('Profile content:\n%s' % profile_content)
                cs_profile.save()
        queue = Queue.Queue()

        def node_run_stress(node, loader_idx, cpu_idx, keyspace_idx, profile, stress_cmd):
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
            queue.put((node, result))
            queue.task_done()

        for loader_idx, loader in enumerate(self.nodes):
            for cpu_idx in range(stress_num):
                for ks_idx in range(1, keyspace_num + 1):
                    setup_thread = threading.Thread(target=node_run_stress,
                                                    args=(loader, loader_idx,
                                                          cpu_idx, ks_idx, profile, stress_cmd))
                    setup_thread.daemon = True
                    setup_thread.start()

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
            if line.startswith('Latency:'):
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]
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

    def verify_stress_thread(self, queue, db_cluster, stress_num=1, keyspace_num=1):
        results = []
        cs_summary = []
        while len(results) != len(self.nodes) * stress_num * keyspace_num:
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        errors = []
        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            cs_summary.append(self._parse_cs_summary(lines))
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]
            plotfile = os.path.join(self.logdir, str(node))
            self._cassandra_stress_plot(lines, plotfile, node, db_cluster)

        return cs_summary, errors

    def get_stress_results(self, queue, stress_num=1, keyspace_num=1):
        results = []
        ret = []
        while len(results) != len(self.nodes) * stress_num * keyspace_num:
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            ret.append(self._parse_cs_summary(lines))

        return ret

    def get_stress_results_bench(self, queue):
        results = []
        ret = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            ret.append(self._parse_bench_summary(lines))

        return ret

    def run_stress_thread_bench(self, stress_cmd, timeout, output_dir, node_list=[]):
        queue = Queue.Queue()

        def node_run_stress_bench(node, loader_idx, stress_cmd, node_list):
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            log_file_name = os.path.join(logdir,
                                         'scylla-bench-l%s-%s.log' %
                                         (loader_idx, uuid.uuid4()))
            ips = ",".join([n.private_ip_address for n in node_list])
            result = node.remoter.run(cmd="/home/centos/go/bin/{0} -nodes {1}".format(stress_cmd.strip(), ips),
                                      timeout=timeout,
                                      ignore_status=True,
                                      # watch_stdout_pattern='total,',
                                      log_file=log_file_name)
            node.cs_start_time = result.stdout_pattern_found_at
            queue.put((node, result))
            queue.task_done()

        for loader_idx, loader in enumerate(self.nodes):
            setup_thread = threading.Thread(target=node_run_stress_bench,
                                            args=(loader, loader_idx,
                                                  stress_cmd, node_list))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        return queue

    def kill_stress_thread_bench(self):
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


class BaseMonitorSet(object):

    grafana_start_time = None
    scylla_version = ''
    is_enterprise = None

    def wait_for_init(self, targets, verbose=False, scylla_version='', is_enterprise=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in BaseMonitorSet')
            node.wait_ssh_up(verbose=verbose)
            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.install_prometheus()
            node.setup_prometheus(targets=targets)
            node.install_grafana()
            self.scylla_version = scylla_version
            self.is_enterprise = is_enterprise
            node.setup_grafana(scylla_version)
            # The time will be used in url of Grafana monitor,
            # the data from this point to the end of test will
            # be captured.
            self.grafana_start_time = time.time()
            node.remoter.run('sudo yum install screen -y')
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for node in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))

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


class LibvirtCluster(BaseCluster):

    """
    Cluster of Node objects, started on Libvirt.
    """

    def __init__(self, domain_info, hypervisor, cluster_uuid=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        self._domain_info = domain_info
        self._hypervisor = hypervisor
        super(LibvirtCluster, self).__init__(cluster_uuid=cluster_uuid,
                                             cluster_prefix=cluster_prefix,
                                             node_prefix=node_prefix,
                                             n_nodes=n_nodes,
                                             params=params)

    def __str__(self):
        return 'LibvirtCluster %s (Image: %s)' % (self.name,
                                                  os.path.basename(self._domain_info['image']))

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

    def add_nodes(self, count, user_data=None):
        del user_data
        global LIBVIRT_DOMAINS
        global LIBVIRT_IMAGES
        global LIBVIRT_URI
        nodes = []
        os_type = self._domain_info['os_type']
        os_variant = self._domain_info['os_variant']
        memory = self._domain_info['memory']
        bridge = self._domain_info['bridge']
        uri = self._domain_info['uri']
        LIBVIRT_URI = uri
        image_parent_dir = os.path.dirname(self._domain_info['image'])
        for index in range(self._node_index, self._node_index + count):
            index += 1
            name = '%s-%s' % (self.node_prefix, index)
            dst_image_basename = '%s.qcow2' % name
            dst_image_path = os.path.join(image_parent_dir, dst_image_basename)
            if self.params.get('failure_post_behavior') == 'destroy':
                avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': remove_if_exists,
                                                               'args': (dst_image_path,),
                                                               'once': True})
            self.log.info('Copying %s -> %s',
                          self._domain_info['image'], dst_image_path)
            LIBVIRT_IMAGES.append(dst_image_path)
            shutil.copyfile(self._domain_info['image'], dst_image_path)
            if self.params.get('failure_post_behavior') == 'destroy':
                avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_domain,
                                                               'args': (name,),
                                                               'once': True})
            virt_install_cmd = ('virt-install --connect %s --name %s '
                                '--memory %s --os-type=%s '
                                '--os-variant=%s '
                                '--disk %s,device=disk,bus=virtio '
                                '--network bridge=%s,model=virtio '
                                '--vnc --noautoconsole --import' %
                                (uri, name, memory, os_type, os_variant,
                                 dst_image_path, bridge))
            process.run(virt_install_cmd)
            LIBVIRT_DOMAINS.append(name)
            for domain in self._hypervisor.listAllDomains():
                if domain.name() == name:
                    node = LibvirtNode(hypervisor=self._hypervisor,
                                       domain=domain,
                                       node_prefix=self.node_prefix,
                                       node_index=index,
                                       domain_username=self._domain_info[
                                           'user'],
                                       domain_password=self._domain_info[
                                           'password'],
                                       base_logdir=self.logdir)
                    node._backing_image = dst_image_path
                    nodes.append(node)
        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes
        self.write_node_public_ip_file()
        self.write_node_private_ip_file()
        return nodes


class ScyllaLibvirtCluster(LibvirtCluster, BaseScyllaCluster):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'db-node')

        super(ScyllaLibvirtCluster, self).__init__(domain_info=domain_info,
                                                   hypervisor=hypervisor,
                                                   cluster_uuid=cluster_uuid,
                                                   cluster_prefix=cluster_prefix,
                                                   node_prefix=node_prefix,
                                                   n_nodes=n_nodes,
                                                   params=params)
        self.collectd_setup = ScyllaCollectdSetup()
        self.seed_nodes_private_ips = None
        self.termination_event = threading.Event()
        self.nemesis_threads = []

    def _node_setup(self, node):
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y "scylla*"')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update -y --skip-broken')
        node.remoter.run('sudo yum install -y rsync tcpdump screen')
        yum_config_path = '/etc/yum.repos.d/scylla.repo'
        node.remoter.run('sudo curl %s -o %s' %
                         (self.params.get('scylla_repo'), yum_config_path))
        node.remoter.run('sudo yum install -y {}'.format(node.scylla_pkg()))
        node.config_setup(seed_address=self.get_seed_nodes_by_flag(),
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          append_conf=self.params.get('append_conf'))

        node.remoter.run(
            'sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.remoter.run('sudo systemctl start scylla-jmx.service')
        node.remoter.run('sudo iptables -F')

    def wait_for_init(self, node_list=None, verbose=False):
        """
        Configure scylla.yaml on all cluster nodes.

        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.

        :param node_list: List of nodes to watch for init.
        :param verbose: Whether to print extra info while watching for init.
        :return:
        """
        if node_list is None:
            node_list = self.nodes

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            self._node_setup(node=node)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y {}-gdb'.format(node.scylla_pkg()),
                             verbose=verbose, ignore_status=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        # avoid using node.remoter in thread
        for node in node_list:
            node.wait_ssh_up(verbose=verbose)
            self.collectd_setup.install(node)

        # If we setup all nodes in paralel, we might have troubles
        # with nodes not able to contact the seed node.
        # Let's setup the seed node first, then set up the others
        seed_address = self.get_seed_nodes_by_flag()
        seed_address_list = seed_address.split(',')
        for i in seed_address_list:
            node_setup(i)
        for node in node_list:
            if node in seed_address_list:
                continue
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
                time_elapsed = time.time() - start_time
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list),
                              int(time_elapsed))
            except Queue.Empty:
                pass

        self.update_db_binary(node_list)
        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))
        if not node_list[0].scylla_version:
            result = node_list[0].remoter.run("scylla --version")
            for node in node_list:
                node.scylla_version = result.stdout

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaLibvirtCluster, self).destroy()


class LoaderSetLibvirt(LibvirtCluster, BaseLoaderSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(
            user_prefix, 'loader-node')
        node_prefix = _prepend_user_prefix(user_prefix, 'loader-set')

        super(LoaderSetLibvirt, self).__init__(domain_info=domain_info,
                                               hypervisor=hypervisor,
                                               cluster_uuid=cluster_uuid,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)

    def wait_for_init(self, verbose=False, db_node_address=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in LoaderSetLibvirt')
            node.wait_ssh_up(verbose=verbose)
            yum_config_path = '/etc/yum.repos.d/scylla.repo'
            node.remoter.run('sudo curl %s -o %s' %
                             (self.params.get('scylla_repo'), yum_config_path))
            node.remoter.run('sudo yum install -y {}-tools'.format(node.scylla_pkg()))
            node.wait_cs_installed(verbose=verbose)
            node.remoter.run('sudo yum install -y screen')
            if db_node_address is not None:
                node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" %
                                 db_node_address)

            cs_exporter_setup = CassandraStressExporterSetup()
            cs_exporter_setup.install(node)

            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))


class MonitorSetLibvirt(LibvirtCluster, BaseMonitorSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=1,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'monitor-node')
        node_prefix = _prepend_user_prefix(user_prefix, 'monitor-set')

        super(MonitorSetLibvirt, self).__init__(domain_info=domain_info,
                                                hypervisor=hypervisor,
                                                cluster_uuid=cluster_uuid,
                                                cluster_prefix=cluster_prefix,
                                                node_prefix=node_prefix,
                                                n_nodes=n_nodes,
                                                params=params)

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()


class OpenStackCluster(BaseCluster):

    """
    Cluster of Node objects, started on OpenStack.
    """

    def __init__(self, openstack_image, openstack_network, service, credentials, cluster_uuid=None,
                 openstack_instance_type='m1.small', openstack_image_username='root',
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        if credentials.type == 'generated':
            credential_key_name = credentials.key_pair_name
            credential_key_file = credentials.key_file
            user = params.get('openstack_user', None)
            password = params.get('openstack_password', None)
            tenant = params.get('openstack_tenant', None)
            auth_version = params.get('openstack_auth_version', None)
            auth_url = params.get('openstack_auth_url', None)
            service_type = params.get('openstack_service_type', None)
            service_name = params.get('openstack_service_name', None)
            service_region = params.get('openstack_service_region', None)
            global OPENSTACK_SERVICE
            if OPENSTACK_SERVICE is None:
                OPENSTACK_SERVICE = service
            if params.get('failure_post_behavior') == 'destroy':
                avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_openstack_credential,
                                                               'args': (user,
                                                                        password,
                                                                        tenant,
                                                                        auth_version,
                                                                        auth_url,
                                                                        service_type,
                                                                        service_name,
                                                                        service_region,
                                                                        credential_key_name,
                                                                        credential_key_file),
                                                               'once': True})
        global CREDENTIALS
        CREDENTIALS.append(credentials)

        self._openstack_image = openstack_image
        self._openstack_network = openstack_network
        self._openstack_service = service
        self._credentials = credentials
        self._openstack_instance_type = openstack_instance_type
        self._openstack_image_username = openstack_image_username
        super(OpenStackCluster, self).__init__(cluster_uuid=cluster_uuid,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)

    def __str__(self):
        return 'Cluster %s (Image: %s Type: %s)' % (self.name,
                                                    self._openstack_image,
                                                    self._openstack_instance_type)

    def add_nodes(self, count, ec2_user_data=''):
        nodes = []
        size = [d for d in self._openstack_service.list_sizes() if d.name == self._openstack_instance_type][0]
        image = self._openstack_service.get_image(self._openstack_image)
        networks = [n for n in self._openstack_service.ex_list_networks() if n.name == self._openstack_network]
        for node_index in range(self._node_index + 1, count + 1):
            name = '%s-%s' % (self.node_prefix, node_index)
            instance = self._openstack_service.create_node(name=name, image=image, size=size, networks=networks,
                                                           ex_keyname=self._credentials.name)
            OPENSTACK_INSTANCES.append(instance)
            nodes.append(OpenStackNode(openstack_instance=instance, openstack_service=self._openstack_service,
                                       credentials=self._credentials,
                                       openstack_image_username=self._openstack_image_username,
                                       node_prefix=self.node_prefix, node_index=node_index,
                                       base_logdir=self.logdir))

        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes

        return nodes


class ScyllaOpenStackCluster(OpenStackCluster, BaseScyllaCluster):
    def __init__(self, openstack_image, openstack_network, service, credentials,
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = _prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'db-node')
        super(ScyllaOpenStackCluster, self).__init__(openstack_image=openstack_image,
                                                     openstack_network=openstack_network,
                                                     openstack_instance_type=openstack_instance_type,
                                                     openstack_image_username=openstack_image_username,
                                                     services=service,
                                                     credentials=credentials,
                                                     cluster_prefix=cluster_prefix,
                                                     node_prefix=node_prefix,
                                                     n_nodes=n_nodes,
                                                     params=params)
        self.collectd_setup = ScyllaCollectdSetup()
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()
        self.seed_nodes_private_ips = None
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data=''):
        added_nodes = super(ScyllaOpenStackCluster, self).add_nodes(count=count,
                                                                    ec2_user_data=ec2_user_data)
        return added_nodes

    def _node_setup(self, node):
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y "scylla*"')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update -y --skip-broken')
        node.remoter.run('sudo yum install -y rsync tcpdump screen wget')
        yum_config_path = '/etc/yum.repos.d/scylla.repo'
        node.remoter.run('sudo curl %s -o %s' %
                         (self.params.get('scylla_repo'), yum_config_path))
        node.remoter.run('sudo yum install -y {}'.format(node.scylla_pkg()))
        node.config_setup(seed_address=self.get_seed_nodes_by_flag(),
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          append_conf=self.params.get('append_conf'))

        node.remoter.run('sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup')
        # Work around a systemd bug in RHEL 7.3 -> https://github.com/scylladb/scylla/issues/1846
        node.remoter.run('sudo sh -c "sed -i s/OnBootSec=0/OnBootSec=3/g /usr/lib/systemd/system/scylla-housekeeping.timer"')
        node.remoter.run('sudo cat /usr/lib/systemd/system/scylla-housekeeping.timer')
        node.remoter.run('sudo systemctl daemon-reload')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.restart()
        node.wait_ssh_up()
        node.wait_db_up()
        node.wait_jmx_up()

    def wait_for_init(self, node_list=None, verbose=False):
        """
        Configure scylla.yaml on all cluster nodes.

        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.

        :param node_list: List of nodes to watch for init.
        :param verbose: Whether to print extra info while watching for init.
        :return:
        """
        if node_list is None:
            node_list = self.nodes

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            self._node_setup(node=node)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y {}-gdb'.format(node.scylla_pkg()),
                             verbose=verbose, ignore_status=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        # avoid using node.remoter in thread
        for node in node_list:
            self.collectd_setup.install(node)

        # If we setup all nodes in paralel, we might have troubles
        # with nodes not able to contact the seed node.
        # Let's setup the seed node first, then set up the others
        seed_address = self.get_seed_nodes_by_flag()
        seed_address_list = seed_address.split(',')
        for i in seed_address_list:
            node_setup(i)
        for node in node_list:
            if node in seed_address_list:
                continue
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
                time_elapsed = time.time() - start_time
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list),
                              int(time_elapsed))
            except Queue.Empty:
                pass

        self.update_db_binary(node_list)
        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))
        if not node_list[0].scylla_version:
            result = node_list[0].remoter.run("scylla --version")
            for node in node_list:
                node.scylla_version = result.stdout

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaOpenStackCluster, self).destroy()


class LoaderSetOpenStack(OpenStackCluster, BaseLoaderSet):

    def __init__(self, openstack_image, openstack_network, service, credentials,
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'loader-set')
        super(LoaderSetOpenStack, self).__init__(openstack_image=openstack_image,
                                                 openstack_network=openstack_network,
                                                 openstack_instance_type=openstack_instance_type,
                                                 openstack_image_username=openstack_image_username,
                                                 services=service,
                                                 credentials=credentials,
                                                 cluster_prefix=cluster_prefix,
                                                 node_prefix=node_prefix,
                                                 n_nodes=n_nodes,
                                                 params=params)
        self.scylla_repo = scylla_repo

    def wait_for_init(self, verbose=False, db_node_address=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in LoaderSetOpenStack')
            node.wait_ssh_up(verbose=verbose)
            yum_config_path = '/etc/yum.repos.d/scylla.repo'
            node.remoter.run('sudo curl %s -o %s' %
                             (self.params.get('scylla_repo'), yum_config_path))
            node.remoter.run('sudo yum install -y {}-tools'.format(node.scylla_pkg()))
            node.wait_cs_installed(verbose=verbose)
            node.remoter.run('sudo yum install -y screen')
            if db_node_address is not None:
                node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" %
                                 db_node_address)

            cs_exporter_setup = CassandraStressExporterSetup()
            cs_exporter_setup.install(node)

            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))


class MonitorSetOpenStack(OpenStackCluster, BaseMonitorSet):

    def __init__(self, openstack_image, openstack_network, service, credentials,
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'monitor-set')
        super(MonitorSetOpenStack, self).__init__(openstack_image=openstack_image,
                                                  openstack_network=openstack_network,
                                                  openstack_instance_type=openstack_instance_type,
                                                  openstack_image_username=openstack_image_username,
                                                  services=service,
                                                  credentials=credentials,
                                                  cluster_prefix=cluster_prefix,
                                                  node_prefix=node_prefix,
                                                  n_nodes=n_nodes,
                                                  params=params)
        self.scylla_repo = scylla_repo

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()
