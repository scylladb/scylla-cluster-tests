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
import subprocess
import shutil
import xml.etree.cElementTree as etree

# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from avocado.utils import path
from avocado.utils import process
from avocado.utils import script
from avocado.utils import runtime as avocado_runtime

from botocore.exceptions import WaiterError
import boto3.session

from .log import SDCMAdapter
from .remote import Remote
from . import data_path
from . import wait

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
LIBVIRT_DOMAINS = []
LIBVIRT_IMAGES = []
DEFAULT_USER_PREFIX = getpass.getuser()
# Test duration (min). Parameter used to keep instances produced by tests that
# are supposed to run longer than 24 hours from being killed
TEST_DURATION = 60


def set_duration(duration):
    global TEST_DURATION
    TEST_DURATION = duration


def clean_domain(domain_name):
    process.run('virsh -c qemu:///system destroy %s' % domain_name,
                ignore_status=True)

    process.run('virsh -c qemu:///system undefine %s' % domain_name,
                ignore_status=True)


def clean_aws_instances(region_name, instance_ids):
    try:
        session = boto3.session.Session(region_name=region_name)
        service = session.resource('ec2')
        service.instances.filter(InstanceIds=instance_ids).terminate()
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))


def clean_aws_credential(region_name, credential_key_name, credential_key_file):
    try:
        session = boto3.session.Session(region_name=region_name)
        service = session.resource('ec2')
        key_pair_info = service.KeyPair(credential_key_name)
        key_pair_info.delete()
        os.unlink(credential_key_file)
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))


def cleanup_instances(behavior='destroy'):
    global EC2_INSTANCES
    global CREDENTIALS
    global LIBVIRT_DOMAINS
    global LIBVIRT_IMAGES

    for instance in EC2_INSTANCES:
        if behavior == 'destroy':
            instance.terminate()
        elif behavior == 'stop':
            instance.stop()

    for cred in CREDENTIALS:
        if behavior == 'destroy':
            cred.destroy()

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
    if user_prefix is None:
        user_prefix = DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, key_prefix='keypair', user_prefix=None):
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        key_prefix = _prepend_user_prefix(user_prefix, key_prefix)
        self.name = '%s-%s' % (key_prefix, self.shortid)
        self.key_pair = service.create_key_pair(KeyName=self.name)
        self.key_file = os.path.join(tempfile.gettempdir(),
                                     '%s.pem' % self.name)
        self.write_key_file()
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Created')

    def __str__(self):
        return "Key Pair %s -> %s" % (self.name, self.key_file)

    def write_key_file(self):
        with open(self.key_file, 'w') as key_file_obj:
            key_file_obj.write(self.key_pair.key_material)
        os.chmod(self.key_file, 0o400)

    def destroy(self):
        self.key_pair.delete()
        try:
            os.remove(self.key_file)
        except OSError:
            pass
        self.log.info('Destroyed')


class BaseNode(object):

    def __init__(self, name, ssh_login_info=None, base_logdir=None):
        self.name = name
        self.is_seed = None
        try:
            self.logdir = path.init_dir(base_logdir, self.name)
        except OSError:
            self.logdir = os.path.join(base_logdir, self.name)

        self.remoter = Remote(**ssh_login_info)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug(self.remoter.ssh_debug_cmd())

        self._journal_thread = None
        self._n_coredumps = 0
        self._backtrace_thread = None
        self._public_ip_address = None
        self._private_ip_address = None
        self._prometheus_thread = None
        self._collectd_exporter_thread = None

        self.cs_start_time = None
        self.database_log = os.path.join(self.logdir, 'database.log')
        self.start_journal_thread()
        self.start_backtrace_thread()

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

    def retrieve_journal(self):
        try:
            result = self.remoter.run('journalctl --version',
                                      ignore_status=True)
            if result.exit_status == 0:
                # Here we're assuming that journalctl systems are Scylla images
                db_services_log_cmd = ('sudo journalctl -f '
                                       '-u scylla-ami-setup.service '
                                       '-u scylla-io-setup.service '
                                       '-u scylla-server.service '
                                       '-u scylla-jmx.service')
            else:
                # Here we are assuming we're using a cassandra image, based
                # on older Ubuntu
                cassandra_log = '/var/log/cassandra/system.log'
                wait.wait_for(self.file_exists, step=10,
                              file_path=cassandra_log)
                db_services_log_cmd = ('sudo tail -f %s' % cassandra_log)
            self.remoter.run(db_services_log_cmd,
                             verbose=True, ignore_status=True,
                             log_file=self.database_log)
        except Exception as details:
            self.log.error('Error retrieving remote node DB service log: %s',
                           details)

    def install_grafana(self):
        self.remoter.run('sudo yum install rsync -y')
        self.remoter.run('sudo yum install https://grafanarel.s3.amazonaws.com/builds/grafana-3.1.0-1468321182.x86_64.rpm -y')
        self.remoter.run('sudo grafana-cli plugins install grafana-piechart-panel')

    def setup_grafana(self):
        self.remoter.run('sudo cp /etc/grafana/grafana.ini /tmp/grafana-noedit.ini')
        self.remoter.run('sudo chown centos /tmp/grafana-noedit.ini')
        grafana_ini_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'),
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

        def _register_data_source():
            scylla_data_source_json = data_path.get_data_path('scylla-data-source.json')
            result = process.run('curl -XPOST -i http://%s:3000/api/datasources --data-binary @%s -H "Content-Type: application/json"' %
                                 (self.public_ip_address, scylla_data_source_json), ignore_status=True)
            return result.exit_status == 0

        def _register_dash():
            scylla_dash_json = data_path.get_data_path('scylla-dash.json')
            result = process.run('curl -XPOST -i http://%s:3000/api/dashboards/db --data-binary @%s -H "Content-Type: application/json"' %
                                 (self.public_ip_address, scylla_dash_json), ignore_status=True)
            return result.exit_status == 0

        def _register_dash_per_server():
            scylla_dash_per_server_json = data_path.get_data_path('scylla-dash-per-server.json')
            result = process.run('curl -XPOST -i http://%s:3000/api/dashboards/db --data-binary @%s -H "Content-Type: application/json"' %
                                 (self.public_ip_address, scylla_dash_per_server_json), ignore_status=True)
            return result.exit_status == 0

        wait.wait_for(_register_data_source, step=10,
                      text='Waiting to register data source...')
        wait.wait_for(_register_dash, step=10,
                      text='Waiting to register dash...')
        wait.wait_for(_register_dash_per_server, step=10,
                      text='Waiting to register dash per server...')

        self.log.info('Grafana Web UI: http://%s:3000', self.public_ip_address)

    def _set_prometheus_paths(self):
        self.prometheus_system_base_dir = '/var/tmp'
        self.prometheus_base_dir = 'prometheus-1.0.0.linux-amd64'
        self.prometheus_tarball = '%s.tar.gz' % self.prometheus_base_dir
        self.prometheus_base_url = 'https://github.com/prometheus/prometheus/releases/download/v1.0.0'
        self.prometheus_system_dir = os.path.join(self.prometheus_system_base_dir,
                                                  self.prometheus_base_dir)
        self.prometheus_path = os.path.join(self.prometheus_system_dir, 'prometheus')
        self.prometheus_custom_cfg_basename = 'prometheus-scylla.yml'
        self.prometheus_custom_cfg_path = os.path.join(self.prometheus_system_dir,
                                                       self.prometheus_custom_cfg_basename)
        self.prometheus_data_dir = os.path.join(self.prometheus_system_dir, 'data')

    def install_prometheus(self):
        self._set_prometheus_paths()
        self.remoter.run('curl %s/%s -o %s/%s -L' %
                         (self.prometheus_base_url, self.prometheus_tarball,
                          self.prometheus_system_base_dir, self.prometheus_tarball))
        self.remoter.run('tar -xzvf %s/%s -C %s' %
                         (self.prometheus_system_base_dir,
                          self.prometheus_tarball,
                          self.prometheus_system_base_dir))

    def download_prometheus_data_dir(self):
        dst = os.path.join(self.logdir, 'prometheus')
        self.remoter.receive_files(src=self.prometheus_data_dir, dst=dst)

    def run_prometheus(self, targets):
        targets_list = ['%s:9103' % ip for ip in targets]
        prometheus_cfg = """
global:
  scrape_interval: 15s

  external_labels:
    monitor: 'scylla-monitor'

scrape_configs:
- job_name: scylla
  static_configs:
  - targets: %s
""" % targets_list
        tmp_dir_prom = tempfile.mkdtemp(prefix='scm-prometheus')
        tmp_path_prom = os.path.join(tmp_dir_prom,
                                     self.prometheus_custom_cfg_basename)
        with open(tmp_path_prom, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(prometheus_cfg)
        try:
            self.remoter.send_files(src=tmp_path_prom,
                                    dst=self.prometheus_custom_cfg_path)
        finally:
            shutil.rmtree(tmp_dir_prom)
        self.remoter.run('%s -config.file %s -storage.local.path %s' %
                         (self.prometheus_path,
                          self.prometheus_custom_cfg_path,
                          self.prometheus_data_dir))

    def prometheus_thread(self, targets):
        while True:
            self.wait_ssh_up(verbose=False)
            self.run_prometheus(targets=targets)
            time.sleep(60)

    def start_prometheus_thread(self, targets):
        self._prometheus_thread = threading.Thread(target=self.prometheus_thread,
                                                   args=(targets,))
        self._prometheus_thread.start()

    def _set_collectd_exporter_paths(self):
        self.collectd_exporter_system_base_dir = '/var/tmp'
        self.collectd_exporter_base_dir = 'collectd_exporter-0.3.1.linux-amd64'
        self.collectd_exporter_tarball = '%s.tar.gz' % self.collectd_exporter_base_dir
        self.collectd_exporter_base_url = 'https://github.com/prometheus/collectd_exporter/releases/download/0.3.1'
        self.collectd_exporter_system_dir = os.path.join(self.collectd_exporter_system_base_dir,
                                                         self.collectd_exporter_base_dir)
        self.collectd_exporter_path = os.path.join(self.collectd_exporter_system_dir, 'collectd_exporter')

    def _setup_collectd(self):
        collectd_cfg = """
LoadPlugin network
LoadPlugin disk
LoadPlugin interface
LoadPlugin unixsock
LoadPlugin df
LoadPlugin processes
<Plugin network>
        Listen "127.0.0.1" "25826"
        Server "127.0.0.1" "65534"
        Forward true
</Plugin>
<Plugin disk>
</Plugin>
<Plugin interface>
</Plugin>
<Plugin "df">
  FSType "xfs"
  IgnoreSelected false
</Plugin>
<Plugin unixsock>
    SocketFile "/var/run/collectd-unixsock"
    SocketPerms "0666"
</Plugin>
<Plugin processes>
    Process "scylla"
</Plugin>
"""
        tmp_dir_exporter = tempfile.mkdtemp(prefix='scm-collectd-exporter')
        tmp_path_exporter = os.path.join(tmp_dir_exporter, 'scylla.conf')
        tmp_path_remote = '/tmp/scylla-collectd.conf'
        system_path_remote = '/etc/collectd.d/scylla.conf'
        with open(tmp_path_exporter, 'w') as tmp_cfg_prom:
            tmp_cfg_prom.write(collectd_cfg)
        try:
            self.remoter.send_files(src=tmp_path_exporter, dst=tmp_path_remote)
            self.remoter.run('sudo mv %s %s' %
                             (tmp_path_remote, system_path_remote))
            self.remoter.run('sudo service collectd restart')
        finally:
            shutil.rmtree(tmp_dir_exporter)

    def install_collectd_exporter(self):
        self._setup_collectd()
        self._set_collectd_exporter_paths()
        self.remoter.run('curl %s/%s -o %s/%s -L' %
                         (self.collectd_exporter_base_url, self.collectd_exporter_tarball,
                          self.collectd_exporter_system_base_dir, self.collectd_exporter_tarball))
        self.remoter.run('tar -xzvf %s/%s -C %s' %
                         (self.collectd_exporter_system_base_dir,
                          self.collectd_exporter_tarball,
                          self.collectd_exporter_system_base_dir))

    def run_collectd_exporter(self):
        self.remoter.run('%s -collectd.listen-address="0.0.0.0:65534"' % self.collectd_exporter_path, ignore_status=True)

    def collectd_exporter_thread(self):
        while True:
            self.wait_ssh_up(verbose=False)
            self.run_collectd_exporter()
            time.sleep(60)

    def start_collectd_exporter_thread(self):
        self._collectd_exporter_thread = threading.Thread(target=self.collectd_exporter_thread)
        self._collectd_exporter_thread.start()

    def journal_thread(self):
        while True:
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

    def _notify_backtrace(self, last):
        """
        Notify coredump backtraces to test log and coredump.log file.

        :param last: Whether to show only the last backtrace.
        """
        result = self._get_coredump_backtraces(last=last)
        log_file = os.path.join(self.logdir, 'coredump.log')
        output = result.stdout + result.stderr
        base_upload_url = 'scylladb-users-upload.s3.amazonaws.com/%s/%s'
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Coredump:'):
                coredump = line.split()[-1]
                coredump_id = os.path.basename(coredump)[:-3]
                upload_url = base_upload_url % (coredump_id,
                                                os.path.basename(coredump))
                self.log.info('Uploading coredump %s to %s' %
                              (coredump, upload_url))
                self.remoter.run("sudo curl --request PUT --upload-file "
                                 "'%s' '%s'" % (coredump, upload_url))
                self.log.info("To download it, you may use "
                              "'curl --user-agent [user-agent] %s > %s'",
                              upload_url, os.path.basename(coredump))
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
            n_backtraces_cmd = 'sudo coredumpctl --no-legend 2>/dev/null'
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
            if (new_n_coredumps - self._n_coredumps) == 1:
                self._notify_backtrace(last=True)
            elif (new_n_coredumps - self._n_coredumps) > 1:
                self._notify_backtrace(last=False)
            self._n_coredumps = new_n_coredumps

    def backtrace_thread(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        while True:
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

    def destroy(self):
        raise NotImplementedError('Derived classes must implement destroy')

    def wait_ssh_up(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self.remoter.is_up, step=10,
                      text=text)

    def db_up(self):
        try:
            result = self.remoter.run('netstat -a | grep :9042',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
            self.log.error('Error checking for DB status: %s', details)
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
            log_name = 'tcpdump-%s.log' % tcpdump_id
            pcap_name = 'tcpdump-%s.pcap' % tcpdump_id
            log_file = os.path.join(self.logdir, log_name)
            pcap_tmp_file = os.path.join('/tmp', pcap_name)
            pcap_file = os.path.join(self.logdir, pcap_name)
            self.remoter.run('sudo tcpdump -vv -i lo port 10000 -w %s' %
                             pcap_tmp_file, ignore_status=True,
                             log_file=log_file)
            self.remoter.receive_files(src=pcap_tmp_file, dst=pcap_file)
        except Exception as details:
            self.log.error('Error running tcpdump on lo, tcp port 10000: %s',
                           str(details))

    def get_cfstats(self):
        def keyspace1_available():
            res = self.remoter.run('nodetool cfstats keyspace1',
                                   ignore_status=True)
            return res.exit_status == 0
        tcpdump_id = uuid.uuid4()
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
            self.remoter.run('sudo killall tcpdump', ignore_status=True)
        self.log.info('END tcpdump thread uuid: %s', tcpdump_id)
        return self._parse_cfstats(result.stdout)

    def wait_db_up(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60,
                      text=text)

    def wait_db_down(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be down' % self
        wait.wait_for(func=lambda: not self.db_up(), step=60,
                      text=text)

    def wait_cs_installed(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for cassandra-stress' % self
        wait.wait_for(func=self.cs_installed, step=60,
                      text=text)


class AWSNode(BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials,
                 node_prefix='node', node_index=1, ami_username='root',
                 base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = ec2_instance
        self._ec2 = ec2_service
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        self._ec2.create_tags(Resources=[self._instance.id],
                              Tags=[{'Key': 'Name', 'Value': name}])
        ssh_login_info = {'hostname': self._instance.public_ip_address,
                          'user': ami_username,
                          'key_file': credentials.key_file}
        super(AWSNode, self).__init__(name=name,
                                      ssh_login_info=ssh_login_info,
                                      base_logdir=base_logdir)
        if TEST_DURATION >= 24 * 60:
            self.log.info('Test duration set to %s. '
                          'Tagging node with {"keep": "alive"}',
                          TEST_DURATION)
            self._ec2.create_tags(Resources=[self._instance.id],
                                  Tags=[{'Key': 'keep', 'Value': 'alive'}])

    @property
    def public_ip_address(self):
        return self._instance.public_ip_address

    @property
    def private_ip_address(self):
        return self._instance.private_ip_address

    def _instance_wait_safe(self, instance_method):
        """
        Wrapper around AWS instance waiters that is safer to use.

        Since AWS adopts an eventual consistency model, sometimes the method
        wait_until_running will raise a botocore.exceptions.WaiterError saying
        the instance does not exist. AWS API guide [1] recommends that the
        procedure is retried using an exponencial backoff algorithm [2].

        :see: [1] http://docs.aws.amazon.com/AWSEC2/latest/APIReference/query-api-troubleshooting.html#eventual-consistency
        :see: [2] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        threshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                instance_method()
                ok = True
            except WaiterError:
                time.sleep(max((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise NodeError('AWS instance %s waiter error after '
                            'exponencial backoff wait' % self._instance.id)

    def _wait_public_ip(self):
        while self._instance.public_ip_address is None:
            time.sleep(1)
            self._instance.reload()

    def restart(self):
        self._instance.stop()
        self._instance_wait_safe(self._instance.wait_until_stopped)
        self._instance.start()
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self._instance.public_ip_address)
        self.remoter.hostname = self._instance.public_ip_address
        self.wait_db_up()

    def destroy(self):
        self._instance.terminate()
        global EC2_INSTANCES
        EC2_INSTANCES.remove(self._instance)
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
        ssh_login_info = {'hostname': self.public_ip_address,
                          'user': domain_username,
                          'password': domain_password}
        super(LibvirtNode, self).__init__(name=name,
                                          ssh_login_info=ssh_login_info,
                                          base_logdir=base_logdir)

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
            result = self.remoter.run('netstat -a | grep :9042',
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
        os.remove(self._backing_image)
        self.log.info('Destroyed')


class BaseCluster(object):

    """
    Cluster of Node objects.
    """

    def __init__(self, cluster_uuid=None, cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        if cluster_uuid is None:
            self.uuid = uuid.uuid4()
        else:
            self.uuid = cluster_uuid
        self.shortid = str(self.uuid)[:8]
        self.name = '%s-%s' % (cluster_prefix, self.shortid)
        self.node_prefix = '%s-%s' % (node_prefix, self.shortid)
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
        self.add_nodes(n_nodes)

    def send_file(self, src, dst, verbose=False):
        for loader in self.nodes:
            loader.remoter.send_files(src=src, dst=dst, verbose=verbose)

    def run(self, cmd, verbose=False):
        for loader in self.nodes:
            loader.remoter.run(cmd=cmd, verbose=verbose)

    def get_backtraces(self):
        for node in self.nodes:
            node.get_backtraces()

    def add_nodes(self, count, ec2_user_data=''):
        pass

    def get_node_private_ips(self):
        return [node.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.public_ip_address for node in self.nodes]

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()


class BaseScyllaCluster(object):

    def get_seed_nodes_private_ips(self):
        if self.seed_nodes_private_ips is None:
            node = self.nodes[0]
            yaml_dst_path = os.path.join(tempfile.mkdtemp(
                prefix='scylla-longevity'), 'scylla.yaml')
            node.remoter.receive_files(src='/etc/scylla/scylla.yaml',
                                       dst=yaml_dst_path)
            with open(yaml_dst_path, 'r') as yaml_stream:
                conf_dict = yaml.load(yaml_stream)
                try:
                    self.seed_nodes_private_ips = conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
                except Exception, details:
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

    def update_db_binary(self, node_list=None):
        if node_list is None:
            node_list = self.nodes

        new_scylla_bin = self.params.get('update_db_binary')
        if new_scylla_bin:
            for node in node_list:
                self.log.info('Upgrading a DB binary for Node')

                node.remoter.send_files(
                    new_scylla_bin, '/tmp/scylla', verbose=True)

                # stop scylla-server before replace the binary
                node.remoter.run('sudo systemctl stop scylla-server.service')

                # replace the binary
                node.remoter.run(
                    'sudo cp -f /usr/bin/scylla /usr/bin/scylla.origin')
                node.remoter.run('sudo cp -f /tmp/scylla /usr/bin/scylla')
                node.remoter.run('sudo chown root.root /usr/bin/scylla')
                node.remoter.run('sudo chmod +x  /usr/bin/scylla')

                node.remoter.run(
                    'sudo systemctl restart scylla-server.service')
                node.remoter.run('sudo systemctl restart scylla-jmx.service')
            # Wait for DB is up
            for node in node_list:
                node.wait_db_up()

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
        cfstats = [node.get_cfstats()[key] for node in self.nodes]
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

    def add_nemesis(self, nemesis):
        self.nemesis.append(nemesis(cluster=self,
                                    termination_event=self.termination_event))

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


class BaseLoaderSet(object):

    def wait_for_init(self, verbose=False):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup')
            node.wait_ssh_up(verbose=verbose)
            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.wait_cs_installed(verbose=verbose)
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

    def run_stress_thread(self, stress_cmd, timeout, output_dir):
        queue = Queue.Queue()

        def node_run_stress(node):
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            result = node.remoter.run(cmd=stress_cmd, timeout=timeout,
                                      ignore_status=True,
                                      watch_stdout_pattern='total,')
            node.cs_start_time = result.stdout_pattern_found_at
            log_file_name = os.path.join(
                logdir, 'cassandra-stress-%s.log' % uuid.uuid4())
            self.log.debug('Writing cassandra-stress log %s', log_file_name)
            with open(log_file_name, 'w') as log_file:
                log_file.write(str(result))
            queue.put((node, result))
            queue.task_done()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_run_stress,
                                            args=(loader,))
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
            result = loader.remoter.run(kill_script.path)
            self.log.debug(
                'Terminate cassandra-stress process on loader %s: %s', str(loader), str(result))
            loader.remoter.run(cmd='rm -rf %s' % kill_script_dir)
        kill_script.remove()

    @staticmethod
    def _plot_nemesis_events(nemesis, node, plot):
        nemesis_event_start_times = [
            operation['start'] - node.cs_start_time for operation in nemesis.operation_log]
        for start_time in nemesis_event_start_times:
            plot.axvline(start_time, color='blue', linestyle='dashdot')
        nemesis_event_end_times = [
            operation['end'] - node.cs_start_time for operation in nemesis.operation_log]
        for end_time in nemesis_event_end_times:
            plot.axvline(end_time, color='red', linestyle='dashdot')

    def _cassandra_stress_plot(self, lines, plotfile='plot', node=None, db_cluster=None):
        time_plot = []
        ops_plot = []
        latmax_plot = []
        lat999_plot = []
        lat99_plot = []
        lat95_plot = []
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
                time_plot.append(time_point)
                ops_plot.append(ops)
                lat95_plot.append(lat95)
                lat99_plot.append(lat99)
                lat999_plot.append(lat999)
                latmax_plot.append(latmax)
        # ops
        for nemesis in db_cluster.nemesis:
            self._plot_nemesis_events(nemesis, node, plt)

        plt.plot(time_plot, ops_plot, label='ops', color='green')
        plt.title('Operations vs. Time')
        plt.xlabel('time')
        plt.ylabel('ops')
        plt.legend()
        plt.savefig(plotfile + '-ops.svg')
        plt.savefig(plotfile + '-ops.png')
        plt.close()

        # lat
        for nemesis in db_cluster.nemesis:
            self._plot_nemesis_events(nemesis, node, plt)

        plt.plot(time_plot, lat95_plot, label='lat95', color='blue')
        plt.plot(time_plot, lat99_plot, label='lat99', color='green')
        plt.plot(time_plot, lat999_plot, label='lat999', color='black')
        plt.plot(time_plot, latmax_plot, label='latmax', color='red')

        plt.title('Latency vs. Time')
        plt.xlabel('time')
        plt.ylabel('latency')
        plt.legend()
        plt.grid()
        plt.savefig(plotfile + '-lat.svg')
        plt.savefig(plotfile + '-lat.png')
        plt.close()

    def verify_stress_thread(self, queue, db_cluster):
        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        errors = []
        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]
            plotfile = os.path.join(self.logdir, str(node))
            self._cassandra_stress_plot(lines, plotfile, node, db_cluster)

        return errors


class BaseMonitorSet(object):

    def wait_for_init(self, targets, verbose=False):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup')
            node.wait_ssh_up(verbose=verbose)
            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.install_prometheus()
            node.start_prometheus_thread(targets=targets)
            node.install_grafana()
            node.setup_grafana()
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

    def download_monitor_data(self):
        kill_script_contents = 'PIDS=$(pgrep -f prometheus) && kill $PIDS'
        kill_script = script.TemporaryScript(name='kill_prometheus.sh',
                                             content=kill_script_contents)
        kill_script.save()
        kill_script_dir = os.path.dirname(kill_script.path)
        for node in self.nodes:
            node.remoter.run(cmd='mkdir -p %s' % kill_script_dir)
            node.remoter.send_files(kill_script.path, kill_script_dir)
            node.remoter.run(cmd='chmod +x %s' % kill_script.path)
            node.remoter.run(kill_script.path, ignore_status=True)
            time.sleep(5)
            node.download_prometheus_data_dir()
            node.remoter.run(cmd='rm -rf %s' % kill_script_dir)
        kill_script.remove()


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

    def add_nodes(self, count, user_data=None):
        del user_data
        global LIBVIRT_DOMAINS
        global LIBVIRT_IMAGES
        nodes = []
        os_type = self._domain_info['os_type']
        os_variant = self._domain_info['os_variant']
        memory = self._domain_info['memory']
        bridge = self._domain_info['bridge']
        uri = self._domain_info['uri']
        image_parent_dir = os.path.dirname(self._domain_info['image'])
        for index in range(len(self.nodes), len(self.nodes) + count):
            index += 1
            name = '%s-%s' % (self.node_prefix, index)
            dst_image_basename = '%s.qcow2' % name
            dst_image_path = os.path.join(image_parent_dir, dst_image_basename)
            avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': os.unlink,
                                                           'args': (dst_image_path,),
                                                           'once': True})
            self.log.info('Copying %s -> %s',
                          self._domain_info['image'], dst_image_path)
            LIBVIRT_IMAGES.append(dst_image_path)
            shutil.copyfile(self._domain_info['image'], dst_image_path)
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
        self.nodes += nodes
        return nodes


class ScyllaLibvirtCluster(LibvirtCluster, BaseScyllaCluster):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-node')

        super(ScyllaLibvirtCluster, self).__init__(domain_info=domain_info,
                                                   hypervisor=hypervisor,
                                                   cluster_uuid=cluster_uuid,
                                                   cluster_prefix=cluster_prefix,
                                                   node_prefix=node_prefix,
                                                   n_nodes=n_nodes,
                                                   params=params)
        self.seed_nodes_private_ips = None
        self.termination_event = threading.Event()
        self.nemesis_threads = []

    def _node_setup(self, node, seed_address):
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'),
                                     'scylla.yaml')
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y scylla*')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update -y')
        node.remoter.run('sudo yum install -y rsync')
        node.remoter.run('sudo yum install -y tcpdump')
        yum_config_path = '/etc/yum.repos.d/scylla.repo'
        node.remoter.run('sudo curl %s -o %s' %
                         (self.params.get('scylla_repo'), yum_config_path))
        node.remoter.run('sudo yum install -y scylla')
        node.remoter.receive_files(src='/etc/scylla/scylla.yaml',
                                   dst=yaml_dst_path)

        with open(yaml_dst_path, 'r') as f:
            scylla_yaml_contents = f.read()

        # Set seeds
        p = re.compile('seeds:.*')
        scylla_yaml_contents = p.sub('seeds: "{0}"'.format(seed_address),
                                     scylla_yaml_contents)

        # Set listen_address
        p = re.compile('listen_address:.*')
        scylla_yaml_contents = p.sub('listen_address: {0}'.format(node.public_ip_address),
                                     scylla_yaml_contents)
        # Set rpc_address
        p = re.compile('rpc_address:.*')
        scylla_yaml_contents = p.sub('rpc_address: {0}'.format(node.public_ip_address),
                                     scylla_yaml_contents)
        scylla_yaml_contents = scylla_yaml_contents.replace("cluster_name: 'Test Cluster'",
                                                            "cluster_name: '{0}'".format(self.name))

        with open(yaml_dst_path, 'w') as f:
            f.write(scylla_yaml_contents)

        node.remoter.send_files(src=yaml_dst_path,
                                dst='/tmp/scylla.yaml')
        node.remoter.run('sudo mv /tmp/scylla.yaml /etc/scylla/scylla.yaml')
        node.remoter.run(
            'sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.remoter.run('sudo systemctl enable collectd.service')
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.remoter.run('sudo systemctl start scylla-jmx.service')
        node.remoter.run('sudo systemctl start collectd.service')
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

        def node_setup(node, seed_address):
            node.wait_ssh_up(verbose=verbose)
            self._node_setup(node=node, seed_address=seed_address)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y scylla-gdb',
                             verbose=verbose, ignore_status=True)
            node.install_collectd_exporter()
            node.start_collectd_exporter_thread()
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        seed = node_list[0].public_ip_address
        for loader in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader, seed))
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

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaLibvirtCluster, self).destroy()


class LoaderSetLibvirt(LibvirtCluster, BaseLoaderSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(
            user_prefix, 'scylla-loader-node')
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-loader-set')

        super(LoaderSetLibvirt, self).__init__(domain_info=domain_info,
                                               hypervisor=hypervisor,
                                               cluster_uuid=cluster_uuid,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)


class MonitorSetLibvirt(LibvirtCluster, BaseMonitorSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=1,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-monitor-node')
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-monitor-set')

        super(MonitorSetLibvirt, self).__init__(domain_info=domain_info,
                                                hypervisor=hypervisor,
                                                cluster_uuid=cluster_uuid,
                                                cluster_prefix=cluster_prefix,
                                                node_prefix=node_prefix,
                                                n_nodes=n_nodes,
                                                params=params)

    def destroy(self):
        self.log.info('Destroy nodes')
        self.download_monitor_data()
        for node in self.nodes:
            node.destroy()


class AWSCluster(BaseCluster):

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        credential_key_name = credentials.key_pair.name
        credential_key_file = credentials.key_file
        region_name = params.get('region_name')
        avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_aws_credential,
                                                       'args': (region_name,
                                                                credential_key_name,
                                                                credential_key_file),
                                                       'once': True})
        global CREDENTIALS
        CREDENTIALS.append(credentials)

        self._ec2_ami_id = ec2_ami_id
        self._ec2_subnet_id = ec2_subnet_id
        self._ec2_security_group_ids = ec2_security_group_ids
        self._ec2 = service
        self._credentials = credentials
        self._ec2_instance_type = ec2_instance_type
        self._ec2_ami_username = ec2_ami_username
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        self._ec2_block_device_mappings = ec2_block_device_mappings
        self._ec2_user_data = ec2_user_data
        self._ec2_ami_id = ec2_ami_id
        super(AWSCluster, self).__init__(cluster_uuid=cluster_uuid,
                                         cluster_prefix=cluster_prefix,
                                         node_prefix=node_prefix,
                                         n_nodes=n_nodes,
                                         params=params)

    def __str__(self):
        return 'Cluster %s (AMI: %s Type: %s)' % (self.name,
                                                  self._ec2_ami_id,
                                                  self._ec2_instance_type)

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            ec2_user_data = self._ec2_user_data
        global EC2_INSTANCES
        self.log.debug("Passing user_data '%s' to create_instances",
                       ec2_user_data)
        instances = self._ec2.create_instances(ImageId=self._ec2_ami_id,
                                               UserData=ec2_user_data,
                                               MinCount=count,
                                               MaxCount=count,
                                               KeyName=self._credentials.key_pair.name,
                                               SecurityGroupIds=self._ec2_security_group_ids,
                                               BlockDeviceMappings=self._ec2_block_device_mappings,
                                               SubnetId=self._ec2_subnet_id,
                                               InstanceType=self._ec2_instance_type)
        instance_ids = [i.id for i in instances]
        region_name = self.params.get('region_name')
        avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_aws_instances,
                                                       'args': (region_name,
                                                                instance_ids),
                                                       'once': True})
        EC2_INSTANCES += instances
        added_nodes = [self._create_node(instance, self._ec2_ami_username,
                                         self.node_prefix, node_index,
                                         self.logdir)
                       for node_index, instance in
                       enumerate(instances, start=len(self.nodes) + 1)]
        self.nodes += added_nodes
        return added_nodes

    def _create_node(self, instance, ami_username, node_prefix, node_index,
                     base_logdir):
        return AWSNode(ec2_instance=instance, ec2_service=self._ec2,
                       credentials=self._credentials, ami_username=ami_username,
                       node_prefix=node_prefix, node_index=node_index,
                       base_logdir=base_logdir)

    def cfstat_reached_threshold(self, key, threshold):
        """
        Find whether a certain cfstat key in all nodes reached a certain value.

        :param key: cfstat key, example, 'Space used (total)'.
        :param threshold: threshold value for cfstats key. Example, 2432043080.
        :return: Whether all nodes reached that threshold or not.
        """
        cfstats = [node.get_cfstats()[key] for node in self.nodes]
        reached_threshold = True
        for value in cfstats:
            if value < threshold:
                reached_threshold = False
        if reached_threshold:
            self.log.debug("Done waiting on cfstats: %s" % cfstats)
        return reached_threshold

    def wait_cfstat_reached_threshold(self, key, threshold):
        text = "Waiting until cfstat '%s' reaches value '%s'" % (key, threshold)
        wait.wait_for(func=self.cfstat_reached_threshold, step=10,
                      text=text, key=key, threshold=threshold)

    def wait_total_space_used_per_node(self, size=None):
        if size is None:
            size = int(self.params.get('space_node_threshold'))
        self.wait_cfstat_reached_threshold('Space used (total)', size)


class ScyllaAWSCluster(AWSCluster, BaseScyllaCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=10,
                 params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-node')
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s' % (name, n_nodes))
        super(ScyllaAWSCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                               ec2_subnet_id=ec2_subnet_id,
                                               ec2_security_group_ids=ec2_security_group_ids,
                                               ec2_instance_type=ec2_instance_type,
                                               ec2_ami_username=ec2_ami_username,
                                               ec2_user_data=user_data,
                                               ec2_block_device_mappings=ec2_block_device_mappings,
                                               cluster_uuid=cluster_uuid,
                                               service=service,
                                               credentials=credentials,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()
        self.seed_nodes_private_ips = None
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            if self.nodes:
                node_private_ips = [node.private_ip_address for node
                                    in self.nodes if node.is_seed]
                seeds = ",".join(node_private_ips)
                ec2_user_data = ('--clustername %s --bootstrap true '
                                 '--totalnodes %s --seeds %s' % (self.name,
                                                                 count,
                                                                 seeds))
        added_nodes = super(ScyllaAWSCluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data)
        return added_nodes

    def wait_for_init(self, node_list=None, verbose=False):
        if node_list is None:
            node_list = self.nodes

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y scylla-gdb',
                             verbose=verbose, ignore_status=True)
            node.install_collectd_exporter()
            node.start_collectd_exporter_thread()
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
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

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaAWSCluster, self).destroy()


class CassandraAWSCluster(ScyllaAWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='ubuntu',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=10,
                 params=None):
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix,
                                              'cassandra-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'cassandra-db-node')
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s --version community '
                     '--release 2.1.15' % (name, n_nodes))

        super(ScyllaAWSCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                               ec2_subnet_id=ec2_subnet_id,
                                               ec2_security_group_ids=ec2_security_group_ids,
                                               ec2_instance_type=ec2_instance_type,
                                               ec2_ami_username=ec2_ami_username,
                                               ec2_user_data=user_data,
                                               ec2_block_device_mappings=ec2_block_device_mappings,
                                               cluster_uuid=cluster_uuid,
                                               service=service,
                                               credentials=credentials,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()

    def get_seed_nodes(self):
        node = self.nodes[0]
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='cassandra-longevity'), 'cassandra.yaml')
        node.remoter.receive_files(src='/etc/cassandra/cassandra.yaml',
                                   dst=yaml_dst_path)
        with open(yaml_dst_path, 'r') as yaml_stream:
            conf_dict = yaml.load(yaml_stream)
            try:
                return conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
            except:
                raise ValueError('Unexpected cassandra.yaml '
                                 'contents:\n%s' % yaml_stream.read())

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            if self.nodes:
                seeds = ",".join(self.get_seed_nodes())
                ec2_user_data = ('--clustername %s --bootstrap true '
                                 '--totalnodes %s --seeds %s '
                                 '--version community '
                                 '--release 2.1.15' % (self.name,
                                                       count,
                                                       seeds))
        added_nodes = super(ScyllaAWSCluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data)
        return added_nodes

    def wait_for_init(self, node_list=None, verbose=False):
        if node_list is None:
            node_list = self.nodes

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            node.wait_db_up(verbose=verbose)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
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

        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))


class LoaderSetAWS(AWSCluster, BaseLoaderSet):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-loader-set')
        user_data = ('--clustername %s --totalnodes %s --bootstrap false --stop-services' %
                     (cluster_prefix, n_nodes))
        super(LoaderSetAWS, self).__init__(ec2_ami_id=ec2_ami_id,
                                           ec2_subnet_id=ec2_subnet_id,
                                           ec2_security_group_ids=ec2_security_group_ids,
                                           ec2_instance_type=ec2_instance_type,
                                           ec2_ami_username=ec2_ami_username,
                                           ec2_user_data=user_data,
                                           service=service,
                                           ec2_block_device_mappings=ec2_block_device_mappings,
                                           credentials=credentials,
                                           cluster_prefix=cluster_prefix,
                                           node_prefix=node_prefix,
                                           n_nodes=n_nodes,
                                           params=params)
        self.scylla_repo = scylla_repo

    def wait_for_init(self, verbose=False):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup')
            node.wait_ssh_up(verbose=verbose)
            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.wait_cs_installed(verbose=verbose)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))

    def run_stress_thread(self, stress_cmd, timeout, output_dir):
        queue = Queue.Queue()

        def node_run_stress(node):
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            result = node.remoter.run(cmd=stress_cmd, timeout=timeout,
                                      ignore_status=True,
                                      watch_stdout_pattern='total,')
            node.cs_start_time = result.stdout_pattern_found_at
            log_file_name = os.path.join(logdir, 'cassandra-stress-%s.log' % uuid.uuid4())
            self.log.debug('Writing cassandra-stress log %s', log_file_name)
            with open(log_file_name, 'w') as log_file:
                log_file.write(str(result))
            queue.put((node, result))
            queue.task_done()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_run_stress,
                                            args=(loader,))
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
            result = loader.remoter.run(kill_script.path)
            self.log.debug('Terminate cassandra-stress process on loader %s: %s', str(loader), str(result))
            loader.remoter.run(cmd='rm -rf %s' % kill_script_dir)
        kill_script.remove()

    @staticmethod
    def _plot_nemesis_events(nemesis, node, plot):
        nemesis_event_start_times = [operation['start'] - node.cs_start_time for operation in nemesis.operation_log]
        for start_time in nemesis_event_start_times:
            plot.axvline(start_time, color='blue', linestyle='dashdot')
        nemesis_event_end_times = [operation['end'] - node.cs_start_time for operation in nemesis.operation_log]
        for end_time in nemesis_event_end_times:
            plot.axvline(end_time, color='red', linestyle='dashdot')

    def _cassandra_stress_plot(self, lines, plotfile='plot', node=None, db_cluster=None):
        time_plot = []
        ops_plot = []
        latmax_plot = []
        lat999_plot = []
        lat99_plot = []
        lat95_plot = []
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
                time_plot.append(time_point)
                ops_plot.append(ops)
                lat95_plot.append(lat95)
                lat99_plot.append(lat99)
                lat999_plot.append(lat999)
                latmax_plot.append(latmax)
        # ops
        for nemesis in db_cluster.nemesis:
            self._plot_nemesis_events(nemesis, node, plt)

        plt.plot(time_plot, ops_plot, label='ops', color='green')
        plt.title('Operations vs. Time')
        plt.xlabel('time')
        plt.ylabel('ops')
        plt.legend()
        plt.savefig(plotfile + '-ops.svg')
        plt.savefig(plotfile + '-ops.png')
        plt.close()

        # lat
        for nemesis in db_cluster.nemesis:
            self._plot_nemesis_events(nemesis, node, plt)

        plt.plot(time_plot, lat95_plot, label='lat95', color='blue')
        plt.plot(time_plot, lat99_plot, label='lat99', color='green')
        plt.plot(time_plot, lat999_plot, label='lat999', color='black')
        plt.plot(time_plot, latmax_plot, label='latmax', color='red')

        plt.title('Latency vs. Time')
        plt.xlabel('time')
        plt.ylabel('latency')
        plt.legend()
        plt.grid()
        plt.savefig(plotfile + '-lat.svg')
        plt.savefig(plotfile + '-lat.png')
        plt.close()

    def _parse_results(self, lines):
        """
        Parsing c-stress results, only parse the summary results.
        Collect results of all nodes and return a dictionaries' list,
        the new structure data will be easy to parse, compare, display or save.
        """
        results = {}
        enable_parse = False

        for line in lines:
            line.strip()
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

    def verify_stress_thread(self, queue, db_cluster):
        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        errors = []
        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]
            plotfile = os.path.join(self.logdir, str(node))
            self._cassandra_stress_plot(lines, plotfile, node, db_cluster)

        return errors

    def get_stress_results(self, queue):
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
            ret.append(self._parse_results(lines))

        return ret


class MonitorSetAWS(AWSCluster, BaseMonitorSet):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-monitor-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-monitor-set')
        user_data = ('--clustername %s --totalnodes %s --bootstrap false --stop-services' %
                     (cluster_prefix, n_nodes))
        super(MonitorSetAWS, self).__init__(ec2_ami_id=ec2_ami_id,
                                            ec2_subnet_id=ec2_subnet_id,
                                            ec2_security_group_ids=ec2_security_group_ids,
                                            ec2_instance_type=ec2_instance_type,
                                            ec2_ami_username=ec2_ami_username,
                                            ec2_user_data=user_data,
                                            service=service,
                                            ec2_block_device_mappings=ec2_block_device_mappings,
                                            credentials=credentials,
                                            cluster_prefix=cluster_prefix,
                                            node_prefix=node_prefix,
                                            n_nodes=n_nodes,
                                            params=params)
        self.scylla_repo = scylla_repo

    def destroy(self):
        self.log.info('Destroy nodes')
        self.download_monitor_data()
        for node in self.nodes:
            node.destroy()
