import datetime
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile
import fnmatch

from textwrap import dedent

import requests

from sdcm.utils.common import (S3Storage, list_instances_aws, list_instances_gce,
                               retrying, ParallelObject, remove_files, get_builder_by_test_id,
                               get_testrun_dir, search_test_id_in_latest, filter_aws_instances_by_type,
                               makedirs, filter_gce_instances_by_type)
from sdcm.db_stats import PrometheusDBStats
from sdcm.remote import LocalCmdRunner, RemoteCmdRunner

LOGGER = logging.getLogger(__name__)


class CollectingNode(object):  # pylint: disable=too-few-public-methods

    def __init__(self, name, ssh_login_info, instance, global_ip):
        self.remoter = RemoteCmdRunner(**ssh_login_info)
        self.name = name
        self._instance = instance
        self.external_address = global_ip


class PrometheusSnapshotErrorException(Exception):
    pass


class BaseLogEntity(object):  # pylint: disable=too-few-public-methods
    """Base class for log entity

    LogEntity any file, command, operation, complex actions
    which require to be logged, stored locally, uploaded to
    S3 storage
    """

    def __init__(self, name, command="", search_locally=False):
        self.name = name
        self.cmd = command
        self.search_locally = search_locally

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):  # pylint: disable=unused-argument,no-self-use
        raise Exception('Should be implemented in child class')


class CommandLog(BaseLogEntity):  # pylint: disable=too-few-public-methods
    """LogEntity to save output result to file on remote host

    LogEntity which allow to save the output (usually log output)
    to file. The log output should be produced by the command

    Extends:
        BaseLogEntity
    """

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        remote_logfile = LogCollector.collect_log_remotely(node=node,
                                                           cmd=self.cmd,
                                                           log_filename=os.path.join(remote_dst, self.name))
        archive_logfile = LogCollector.archive_log_remotely(node=node,
                                                            log_filename=remote_logfile)
        local_logfile = LogCollector.receive_log(node=node,
                                                 remote_log_path=archive_logfile,
                                                 local_dir=local_dst)
        return local_logfile


class FileLog(CommandLog):
    """Log File Entinty

    Allow to find log file locally first, if it have not
    found
    Run command to collect log data remotely if command provided

    Extends:
        CommandLogEntity
    """
    @staticmethod
    def find_local_files(search_in_dir, search_pattern, except_patterns="collected_logs"):
        local_files = []
        for root, _, files in os.walk(search_in_dir):
            for f in files:  # pylint: disable=invalid-name
                full_path = os.path.join(root, f)
                if except_patterns in full_path:
                    continue
                if full_path.endswith(search_pattern) or fnmatch.fnmatch(full_path, "*{}".format(search_pattern)):
                    if os.path.islink(full_path):
                        full_path = os.path.realpath(full_path)
                    local_files.append(full_path)
        return local_files

    @staticmethod
    def find_on_builder(builder, file_name, search_in_dir="/"):  # pylint: disable=unused-argument
        result = builder.remoter.run('find {search_in_dir} -name {file_name}'.format(**locals()),
                                     ignore_status=True)
        if not result.exited and not result.stderr:
            path = result.stdout.strip()
        else:
            path = None

        return path

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        if self.search_locally and local_search_path:
            search_pattern = self.name if not node else "/".join([node.name, self.name])
            local_logfiles = self.find_local_files(local_search_path, search_pattern)
            for logfile in local_logfiles:
                shutil.copy(src=logfile, dst=local_dst)

        if not os.listdir(local_dst) and self.cmd:
            super(FileLog, self).collect(node, local_dst, remote_dst)

        return local_dst

    def collect_from_builder(self, builder, local_dst, search_in_dir):

        file_path = self.find_on_builder(builder, self.name, search_in_dir)
        if not file_path:
            return None
        archive = LogCollector.archive_log_remotely(builder, file_path)
        builder.remoter.receive_files(archive, local_dst)
        return os.path.join(local_dst, os.path.basename(file_path))


class PrometheusSnapshots(BaseLogEntity):
    """Get Prometheus snapshot entity

    Specific for Prometheus Log entity which allow to
    collect Prometheus snapshot or data dir

    Extends:
        BaseLogEntity
    """
    monitoring_data_dir_name = "scylla-monitoring-data"

    def __init__(self, *args, **kwargs):
        self.monitoring_data_dir = kwargs.pop('monitoring_data_dir', None)
        super(PrometheusSnapshots, self).__init__(*args, **kwargs)

        self.current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    @retrying(n=3, sleep_time=10, allowed_exceptions=(PrometheusSnapshotErrorException,),
              message='Create prometheus snapshot')
    def create_prometheus_snapshot(self, node):
        prometheus_client = PrometheusDBStats(host=node.external_address)
        result = prometheus_client.create_snapshot()
        if result and "success" in result['status']:
            snapshot_dir = os.path.join(self.monitoring_data_dir,
                                        "snapshots",
                                        result['data']['name'])
            return snapshot_dir
        else:
            raise PrometheusSnapshotErrorException(result)

    def get_prometheus_snapshot_remote(self, node):
        try:
            snapshot_dir = self.create_prometheus_snapshot(node)
        except PrometheusSnapshotErrorException as details:
            LOGGER.warning(
                'Create prometheus snapshot failed %s.\nUse prometheus data directory', details)
            node.remoter.run('docker stop aprom', ignore_status=True)
            snapshot_dir = self.monitoring_data_dir
        LOGGER.info(snapshot_dir)

        archive_path = LogCollector.archive_log_remotely(
            node, snapshot_dir, "{}_{}".format(self.name, self.current_datetime))
        node.remoter.run('docker start aprom', ignore_status=True)
        LOGGER.info(archive_path)
        return archive_path

    def setup_monitor_data_dir(self, node):
        if self.monitoring_data_dir:
            return
        result = node.remoter.run('echo $HOME', ignore_status=True)
        if result.exited == 0 and result.stderr:
            base_dir = result.stdout.strip()
        else:
            base_dir = "/home/centos"

        self.monitoring_data_dir = os.path.join(base_dir, self.monitoring_data_dir_name)

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        """

        :type node: CollectingNode
        """
        self.setup_monitor_data_dir(node)
        remote_snapshot_archive = self.get_prometheus_snapshot_remote(node)
        local_archive_path = LogCollector.receive_log(node,
                                                      remote_log_path=remote_snapshot_archive,
                                                      local_dir=local_dst)
        return local_archive_path


class MonitoringStack(BaseLogEntity):
    """LogEntinty to collect MonitoringStack

    Collect monitoring stack

    Extends:
        BaseLogEntity

    Variables:
        grafana_port {number} -- Grafana server port
    """
    grafana_port = 3000

    @staticmethod
    def get_monitoring_base_dir(node):
        result = node.remoter.run('echo $HOME', ignore_status=True, verbose=False)
        if result.exited == 0 and not result.stderr:
            base_dir = result.stdout.strip()
        else:
            base_dir = "/home/centos"

        return base_dir

    def get_monitoring_data_stack(self, node, local_dist):
        monitor_base_dir = self.get_monitoring_base_dir(node)
        monitor_install_dir_name, monitor_branch, monitor_version = self.get_monitoring_version(node)
        LOGGER.info("%s, %s, %s", monitor_install_dir_name, monitor_branch, monitor_version)
        if not monitor_branch or not monitor_version:
            return ""
        archive_name = "monitoring_data_stack_{monitor_branch}_{monitor_version}.tar.gz".format(**locals())

        annotations_json = self.get_grafana_annotations(node)
        tmp_dir = tempfile.mkdtemp()
        with io.open(os.path.join(tmp_dir, 'annotations.json'), 'w', encoding='utf-8') as f:  # pylint: disable=invalid-name
            f.write(annotations_json)
        node.remoter.send_files(src=os.path.join(tmp_dir, 'annotations.json'),
                                dst=os.path.join(monitor_base_dir, monitor_install_dir_name, "sct_monitoring_addons"))

        node.remoter.run("cd {}; tar -czvf {} {}/".format(monitor_base_dir,
                                                          archive_name,
                                                          monitor_install_dir_name),
                         ignore_status=True)
        node.remoter.receive_files(src=os.path.join(monitor_base_dir, archive_name),
                                   dst=local_dist)
        local_archive_path = os.path.join(local_dist, archive_name)
        return local_archive_path

    @staticmethod
    def get_monitoring_version(node):
        basedir = MonitoringStack.get_monitoring_base_dir(node)
        result = node.remoter.run(
            'ls {} | grep scylla-monitoring-branch'.format(basedir), ignore_status=True, verbose=False)

        name = result.stdout.strip()
        if not name:
            LOGGER.error("Dir with scylla monitoring stack was not found")
            return None, None, None
        result = node.remoter.run("cat {}/{}/monitor_version".format(basedir, name), ignore_status=True, verbose=False)
        try:
            monitor_version, scylla_version = result.stdout.strip().split(':')
        except ValueError:
            monitor_version = "None"
            scylla_version = "None"
        return name, monitor_version, scylla_version

    def get_grafana_annotations(self, node):  # pylint: disable=inconsistent-return-statements
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        try:
            res = requests.get(annotations_url.format(
                node_ip=node.external_address, grafana_port=self.grafana_port))
            if res.ok:
                return res.text
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("unable to get grafana annotations [%s]", str(ex))
            return ""

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        local_archive = self.get_monitoring_data_stack(node, local_dst)
        if not local_archive:
            LOGGER.error('Monitoring stack were not collected')
        return local_archive


class GrafanaEntity(BaseLogEntity):
    """Base Gragana log entity

    Base class to support various Grafana log entity

    Extends:
        BaseLogEntity

    Variables:
        grafana_entity_names {list} -- list of links to collect
        grafana_port {number} -- Grafana server port
        grafana_entity_url_tmpl {str} -- template of grafana url to collect
        phantomjs_base {str} -- name and version of phantomjs package
    """
    grafana_entity_names = [
        {
            'name': 'per-server-metrics-nemesis',
            'path': 'dashboard/db/scylla-{scr_name}-{version}'},
        {
            'name': 'overview-metrics',
            'path': 'd/overview-{version}/scylla-{scr_name}'
        }
    ]
    grafana_port = 3000
    grafana_entity_url_tmpl = "http://{node_ip}:{grafana_port}/{path}?from={st}&to=now"
    phantomjs_base = "phantomjs-2.1.1-linux-x86_64"

    def __init__(self, *args, **kwargs):
        test_start_time = kwargs.pop("test_start_time", None)
        if not test_start_time:
            # set test start time previous 6 hours
            test_start_time = time.time() - (6 * 3600)
        self.start_time = str(test_start_time).split('.')[0] + '000'
        super(GrafanaEntity, self).__init__(*args, **kwargs)
        self.install_phantom_js()

    @property
    def phantomjs_installed(self):
        result = LocalCmdRunner().run("test -d {}".format(self.phantomjs_base),
                                      ignore_status=True, verbose=False)
        return result.exited == 0

    def install_phantom_js(self):
        localrunner = LocalCmdRunner()
        if not self.phantomjs_installed:
            # pylint: disable=unused-variable
            phantomjs_base = self.phantomjs_base
            phantomjs_tar = "{phantomjs_base}.tar.bz2".format(**locals())
            phantomjs_url = "https://bitbucket.org/ariya/phantomjs/downloads/{phantomjs_tar}".format(
                **locals())
            install_phantom_js_script = dedent("""
                rm -rf {phantomjs_base}*
                curl {phantomjs_url} -o {phantomjs_tar} -L
                tar xvfj {phantomjs_tar}
            """.format(**locals()))
            localrunner.run("bash -ce '%s'" % install_phantom_js_script)
        else:
            LOGGER.debug("PhantomJS is already installed!")
        localrunner.run(
            "cd {0.phantomjs_base} && sed -e 's/200);/10000);/' examples/rasterize.js |grep -v 'use strict' > r.js".format(self))


class GrafanaScreenShot(GrafanaEntity):
    """Collect Grafana screenshot

    Collect grafana screenshot

    Extends:
        GrafanaEntity
    """
    resolution = '1920px*4000px'

    def _get_screenshot_link(self, grafana_url, screenshot_path):
        LocalCmdRunner().run("cd {0.phantomjs_base} && bin/phantomjs r.js \"{1}\" \"{2}\" {0.resolution}".format(
            self, grafana_url, screenshot_path), ignore_status=True)

    def get_grafana_screenshot(self, node, local_dst):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        _, _, monitoring_version = MonitoringStack.get_monitoring_version(node)
        try:
            screenshots = []

            for screenshot in self.grafana_entity_names:
                version = monitoring_version.replace('.', '-')
                path = screenshot['path'].format(
                    version=version,
                    scr_name=screenshot['name'])
                grafana_url = self.grafana_entity_url_tmpl.format(
                    node_ip=node.external_address,
                    grafana_port=self.grafana_port,
                    path=path,
                    st=self.start_time)
                datetime_now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = os.path.join(local_dst,
                                               "%s-%s-%s-%s.png" % (self.name, screenshot['name'],
                                                                    datetime_now, node.name))
                self._get_screenshot_link(grafana_url, screenshot_path)
                screenshots.append(screenshot_path)

            return screenshots

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error('Error taking monitor snapshot: %s',
                         str(details))
            return []

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        return self.get_grafana_screenshot(node, local_dst)


class GrafanaSnapshot(GrafanaEntity):
    """Grafana snapshot

    Collect Grafana snapshot

    Extends:
        GrafanaEntity
    """

    def _get_shared_snapshot_link(self, grafana_url):
        result = LocalCmdRunner().run(
            "cd {0.phantomjs_base} && bin/phantomjs ../data_dir/share_snapshot.js \"{1}\"".format(self, grafana_url))
        # since there is only one monitoring node returning here
        output = result.stdout.strip()
        if "Error" in output:
            LOGGER.error(output)
            return ""
        else:
            import re
            matched = re.search(r"https://snapshot.raintank.io/dashboard/snapshot/\w+", output)
            LOGGER.info("Shared grafana snapshot: {}".format(matched.group()))

            return matched.group()

    def get_grafana_snapshot(self, node):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        _, _, monitoring_version = MonitoringStack.get_monitoring_version(node)
        try:

            snapshots = []
            for snapshot in self.grafana_entity_names:
                version = monitoring_version.replace('.', '-')
                path = snapshot['path'].format(
                    version=version,
                    scr_name=snapshot['name'])
                grafana_url = self.grafana_entity_url_tmpl.format(
                    node_ip=node.external_address,
                    grafana_port=self.grafana_port,
                    path=path,
                    st=self.start_time)
                snapshots.append(self._get_shared_snapshot_link(grafana_url))

            return snapshots

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error('Error taking monitor snapshot: %s', str(details))
        return []

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        snapshots = self.get_grafana_snapshot(node)
        snapshots_file = os.path.join(local_dst, "grafana_snapshots")
        with open(snapshots_file, "w") as f:  # pylint: disable=invalid-name
            for snapshot in snapshots:
                f.write(snapshot + '\n')

        return {'links': snapshots, 'file': snapshots_file}


class LogCollector(object):
    """Base class for LogCollector types

    Base class implements interface for collecting
    various LogEntities on different types of Clusters/RemoteHosts

    Variables:
        node_remote_dir {str} -- name of remote dir on remote host
        _current_run {str} -- DateTime of current collecting log run
        cluster_log_type {str} -- Type of cluster
        log_entities {list} -- List of log entities, which should be collected on remote hosts
        USER {str} -- name of user, for which search local file log versions
    """
    _current_run = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    cluster_log_type = 'base'
    log_entities = []
    node_remote_dir = '/tmp'

    @property
    def current_run(self):
        return LogCollector._current_run

    def __init__(self, nodes, test_id, storage_dir):
        self.test_id = test_id
        self.nodes = nodes
        self.local_dir = self.create_local_storage_dir(storage_dir)

    def create_local_storage_dir(self, base_local_dir):
        local_dir = os.path.join(base_local_dir, self.current_run,
                                 "{}-{}".format(self.cluster_log_type, self.test_id[:8]))
        try:
            os.makedirs(local_dir)
        except OSError as details:
            if not os.path.exists(local_dir):
                LOGGER.error("Folder is not created. {}".format(details))
                raise
        return local_dir

    def create_remote_storage_dir(self, node, path=''):
        if not path:
            path = node.name
        remote_dir = os.path.join(self.node_remote_dir, path)
        result = node.remoter.run('mkdir -p {}'.format(remote_dir), ignore_status=True)

        if result.exited > 0:
            LOGGER.error(
                'Remote storing folder not created.\n{}'.format(result))

        return remote_dir

    @staticmethod
    def collect_log_remotely(node, cmd, log_filename):  # pylint: disable=unused-argument
        collect_log_command = "{cmd} >& {log_filename}".format(**locals())
        result = node.remoter.run(
            collect_log_command, ignore_status=True, verbose=True)
        result = node.remoter.run(
            'test -f {}'.format(log_filename), ignore_status=True)
        return log_filename if result.exited == 0 else False

    @staticmethod
    def archive_log_remotely(node, log_filename, archive_name=None):
        archive_dir = os.path.dirname(log_filename)
        if archive_name:
            archive_name = os.path.join(archive_dir, archive_name)
        else:
            archive_name = os.path.join(archive_dir, os.path.basename(log_filename))
        log_filename = os.path.basename(log_filename)
        node.remoter.run(
            "cd {archive_dir}; tar -czf {archive_name}.tar.gz {log_filename}".format(**locals()), ignore_status=True)
        return "{}.tar.gz".format(archive_name)

    @staticmethod
    def receive_log(node, remote_log_path, local_dir):
        makedirs(local_dir)
        node.remoter.receive_files(src=remote_log_path, dst=local_dir)
        return local_dir

    @staticmethod
    def upload_logs(archive_path, storing_path):
        s3_link = S3Storage().upload_file(file_path=archive_path, dest_dir=storing_path)
        return s3_link

    def collect_logs(self, local_search_path=None):
        if not self.nodes:
            LOGGER.warning('No any running instances for %s', self.cluster_log_type)
            return None

        def collect_logs_per_node(node):
            LOGGER.info('Collecting logs on host: %s', node.name)
            remote_node_dir = self.create_remote_storage_dir(node)
            local_node_dir = os.path.join(self.local_dir, node.name)
            os.makedirs(local_node_dir)
            for log_entity in self.log_entities:
                try:
                    log_entity.collect(node, local_node_dir, remote_node_dir, local_search_path=local_search_path)
                except Exception as details:  # pylint: disable=unused-variable, broad-except
                    LOGGER.error("Error occured during collecting on host: %s\n%s", node.name, details)
        try:
            workers_number = int(len(self.nodes) / 2)
            workers_number = len(self.nodes) if workers_number < 2 else workers_number
            ParallelObject(self.nodes, num_workers=workers_number, timeout=300).run(collect_logs_per_node)
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error('Error occured during collecting logs %s', details)
        final_archive = self.archive_dir_with_zip64(self.local_dir)
        s3_link = self.upload_logs(final_archive, "{0.test_id}/{0.current_run}".format(self))
        return s3_link

    def update_db_info(self):
        pass

    @staticmethod
    def archive_dir_with_zip(logdir):
        return shutil.make_archive("{}".format(logdir), "zip", root_dir=logdir)

    @staticmethod
    def archive_dir_with_zip64(logdir):
        archive_base_name = os.path.basename(logdir)
        archive_storage_dir = os.path.dirname(logdir)
        archive_full_name = os.path.join(archive_storage_dir, archive_base_name + ".zip")
        cur_dir = os.getcwd()
        try:
            with zipfile.ZipFile(archive_full_name, "w", allowZip64=True) as arch:
                os.chdir(logdir)
                for root, _, files in os.walk(logdir):
                    for log_file in files:
                        full_path = os.path.join(root, log_file)
                        arch.write(full_path, full_path.replace(logdir, ""))
                os.chdir(cur_dir)
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error("Error durint creating archive. Error details: \n%s", details)
            archive_full_name = None
        return archive_full_name


class ScyllaLogCollector(LogCollector):
    """ScyllaDB cluster log collecting

    Collect on each node the logs for Scylla DB cluster

    Extends:
        LogCollector

    Variables:
        log_entities {list} -- LogEntities to collect
        cluster_log_type {str} -- cluster type name
    """
    log_entities = [FileLog(name='database.log',
                            command="sudo journalctl --no-tail --no-pager -u scylla-ami-setup.service -u scylla-io-setup.service -u scylla-server.service -u scylla-jmx.service",
                            search_locally=True),
                    CommandLog(name='cpu_info',
                               command='cat /proc/cpuinfo'),
                    CommandLog(name='mem_info',
                               command='cat /proc/meminfo'),
                    CommandLog(name='interrupts',
                               command='cat /proc/interrupts'),
                    CommandLog(name='vmstat',
                               command='cat /proc/vmstat'),
                    CommandLog(name='scylla.yaml',
                               command='cat /etc/scylla/scylla.yaml'),
                    CommandLog(name='coredumps.info',
                               command='sudo coredumpctl info')
                    ]
    cluster_log_type = "db-cluster"


class LoaderLogCollector(LogCollector):
    cluster_log_type = "loader-set"
    log_entities = [
        FileLog(name='*cassandra-stress*.log',
                search_locally=True)
    ]


class MonitorLogCollector(LogCollector):
    """SCT monitor set log collector

    Collect required log entites for Monitor set nodes

    Extends:
        LogCollector

    """
    log_entities = [
        CommandLog(name='aprom.log',
                   command='docker logs --details -t aprom'),
        CommandLog(name='agraf.log',
                   command='docker logs --details -t agraf'),
        CommandLog(name='aalert.log',
                   command='docker logs --details -t aalert'),
        FileLog(name='scylla_manager.log',
                command='sudo journalctl -u scylla-manager.service --no-tail',
                search_locally=True),
        PrometheusSnapshots(name='prometheus_data'),
        MonitoringStack(name='monitoring-stack'),
        GrafanaScreenShot(name='grafana-screenshot'),
        GrafanaSnapshot(name='grafana-snapshot')
    ]
    cluster_log_type = "monitor-set"


class SCTLogCollector(LogCollector):
    """logs for hydra test run

    Find and collect local version of files
    for sct

    Extends:
        LogCollector

    """
    log_entities = [
        FileLog(name='sct.log',
                search_locally=True),
        FileLog(name='raw_events.log',
                search_locally=True),
        FileLog(name='events.log',
                search_locally=True),
        FileLog(name='output.log',
                search_locally=True),
        FileLog(name='critical.log',
                search_locally=True)
    ]
    cluster_log_type = 'sct-runner'

    def collect_logs(self, local_search_path=None):
        for ent in self.log_entities:
            ent.collect(None, self.local_dir, None, local_search_path=local_search_path)
        if not os.listdir(self.local_dir):
            LOGGER.warning('No any local files')
            LOGGER.info('Searching on builders')
            builders = get_builder_by_test_id(self.test_id)

            for obj in builders:
                builder = CollectingNode(name=obj['builder']['name'],
                                         ssh_login_info={
                                             "hostname": obj['builder']['public_ip'],
                                             "user": obj['builder']['user'],
                                             "key_file": obj["builder"]['key_file']},
                                         instance=None,
                                         global_ip=obj['builder']['public_ip'])
                for ent in self.log_entities:
                    ent.collect_from_builder(builder, self.local_dir, obj["path"])

            if not os.listdir(self.local_dir):
                LOGGER.warning('Nothing found')
                return None

        final_archive = self.archive_dir_with_zip64(self.local_dir)

        s3_link = self.upload_logs(final_archive, "{0.test_id}/{0.current_run}".format(self))
        remove_files(self.local_dir)
        remove_files(final_archive)
        return s3_link


class Collector(object):  # pylint: disable=too-many-instance-attributes,
    """Collector instance

    Collector instance which should be run to collect logs and additional info
    as separate stage in pipeline on as subcommand in hydra

    """

    def __init__(self, test_id=None, test_dir=None, params=None):
        """Constructor of Collector object

        Build Collector instance to run collecting log processes for running instances

        Keyword Arguments:
            test_id {str} -- Test Id. Name of hydra docker or from log file. if
                             not defined, then test id will be search localy (default: {None})
            test_dir {str} -- where to search and store log files (default: {None})
            params  {SCTConfiguration}  -- SCTConfiguration object (sdcm/sct_config.py)
        """

        self.base_dir = os.environ.get('HOME')
        self._test_id = test_id
        self.backend = params['cluster_backend']
        self.params = params
        self.storage_dir = None
        self._test_dir = test_dir
        self.db_cluster = []
        self.monitor_set = []
        self.loader_set = []
        self.sct_set = []
        self.cluster_log_collectors = {
            ScyllaLogCollector: self.db_cluster,
            MonitorLogCollector: self.monitor_set,
            LoaderLogCollector: self.loader_set,
            SCTLogCollector: self.sct_set
        }

    @property
    def test_id(self):
        return self._test_id

    @property
    def sct_result_dir(self):
        return self._test_dir if self._test_dir else os.path.join(self.base_dir, "sct-results")

    def define_test_id(self):
        if not self._test_id:
            self._test_id = search_test_id_in_latest(self.sct_result_dir)

    def get_aws_instances_by_testid(self):
        instances = list_instances_aws({"TestId": self.test_id}, running=True)
        filtered_instances = filter_aws_instances_by_type(instances)
        for instance in filtered_instances['db_nodes']:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            self.db_cluster.append(CollectingNode(name=name[0],
                                                  ssh_login_info={
                                                      "hostname": instance['PublicIpAddress'],
                                                      "user": self.params['ami_db_scylla_user'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance['PublicIpAddress']))
        for instance in filtered_instances['monitor_nodes']:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            self.monitor_set.append(CollectingNode(name=name[0],
                                                   ssh_login_info={
                                                       "hostname": instance['PublicIpAddress'],
                                                       "user": self.params['ami_monitor_user'],
                                                       "key_file": self.params['user_credentials_path']},
                                                   instance=instance,
                                                   global_ip=instance['PublicIpAddress']))
        for instance in filtered_instances['loader_nodes']:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            self.loader_set.append(CollectingNode(name=name[0],
                                                  ssh_login_info={
                                                      "hostname": instance['PublicIpAddress'],
                                                      "user": self.params['ami_loader_user'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance['PublicIpAddress']))

    def get_gce_instances_by_testid(self):
        instances = list_instances_gce({"TestId": self.test_id}, running=True)
        filtered_instances = filter_gce_instances_by_type(instances)
        for instance in filtered_instances['db_nodes']:
            self.db_cluster.append(CollectingNode(name=instance.name,
                                                  ssh_login_info={
                                                      "hostname": instance.public_ips[0],
                                                      "user": self.params['gce_image_username'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance.public_ips[0]))
        for instance in filtered_instances['monitor_nodes']:
            self.monitor_set.append(CollectingNode(name=instance.name,
                                                   ssh_login_info={
                                                       "hostname": instance.public_ips[0],
                                                       "user": self.params['gce_image_username'],
                                                       "key_file": self.params['user_credentials_path']},
                                                   instance=instance,
                                                   global_ip=instance.public_ips[0]))
        for instance in filtered_instances['loader_nodes']:
            self.loader_set.append(CollectingNode(name=instance.name,
                                                  ssh_login_info={
                                                      "hostname": instance.public_ips[0],
                                                      "user": self.params['gce_image_username'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance.public_ips[0]))

    def get_running_cluster_sets(self):
        if self.backend == 'aws':
            self.get_aws_instances_by_testid()
        if self.backend == 'gce':
            self.get_gce_instances_by_testid()

    def run(self):
        """Run collect logs process as standalone operation

        Run collect log operations as standalon process or run
        as single stage in pipeline for defined cluster sets
        """
        results = {}
        self.define_test_id()
        if not self.test_id:
            LOGGER.warning("No test_id provided or found")
            return results
        self.get_running_cluster_sets()

        local_dir_with_logs = get_testrun_dir(self.sct_result_dir, self.test_id)
        LOGGER.info("Found sct result directory with logs: %s", local_dir_with_logs)

        self.create_base_storage_dir(local_dir_with_logs)
        LOGGER.info("Created directory to storing collected logs: %s", self.storage_dir)
        for cluster_log_collector, nodes in self.cluster_log_collectors.items():
            log_collector = cluster_log_collector(nodes,
                                                  test_id=self.test_id,
                                                  storage_dir=self.storage_dir)
            LOGGER.info("Start collect logs for cluster %s", log_collector.cluster_log_type)
            result = log_collector.collect_logs(local_search_path=local_dir_with_logs)
            results[log_collector.cluster_log_type] = result
            LOGGER.info("collected data for %s\n%s\n", log_collector.cluster_log_type, result)
        return results

    def create_base_storage_dir(self, test_dir=None):
        date_time_formatted = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        log_dir = os.path.basename(test_dir) if test_dir else date_time_formatted
        self.storage_dir = os.path.join(self.sct_result_dir, log_dir, 'collected_logs')
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        if not os.path.exists(os.path.join(os.path.dirname(self.storage_dir), "test_id")):
            with open(os.path.join(os.path.dirname(self.storage_dir), "test_id"), "w") as f:  # pylint: disable=invalid-name
                f.write(self.test_id)
