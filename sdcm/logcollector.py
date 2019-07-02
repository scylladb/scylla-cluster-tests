import datetime
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile

from textwrap import dedent

import requests

from utils.common import (S3Storage, list_instances_aws, list_instances_gce,
                          retrying, ParallelObject, remove_files)
from .db_stats import PrometheusDBStats
from .remote import LocalCmdRunner, RemoteCmdRunner

LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.INFO)


class CollectingNode(object):

    def __init__(self, name, ssh_login_info, instance, global_ip):
        self.remoter = RemoteCmdRunner(**ssh_login_info)
        self.name = name
        self._instance = instance
        self.external_address = global_ip


class PrometheusSnapshotErrorException(Exception):
    pass


class BaseLogEntity(object):
    """Base class for log entity

    LogEntity any file, command, operation, complex actions
    which require to be logged, stored locally, uploaded to
    S3 storage
    """

    def __init__(self, name, command="", search_locally=False):
        self.name = name
        self.cmd = command
        self.search_locally = search_locally

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        raise Exception('Should be implemented in child class')


class CommandLogEntity(BaseLogEntity):
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


class FileLogEntity(CommandLogEntity):
    """Log File Entinty

    Allow to find log file locally first, if it have not
    found, run command to collect log data remotely if command provided

    Extends:
        CommandLogEntity
    """
    # TODO check simlink

    def get_local_file_version(self, search_dir, search_pattern):
        local_file_version = None
        for root, _, files in os.walk(search_dir):
            if root.endswith(search_pattern) and self.name in files:
                local_file_version = os.path.join(root, self.name)
                break
            elif os.path.exists(os.path.join(root, self.name)):
                local_file_version = os.path.join(root, self.name)
                break
        if local_file_version and os.path.islink(local_file_version):
            local_file_version = os.path.realpath(local_file_version)
        return local_file_version

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        if self.search_locally and local_search_path:
            logfile = None
            search_for = node.name if node else self.name
            local_logfile = self.get_local_file_version(local_search_path, search_for)
            if local_logfile:
                shutil.copy(src=local_logfile, dst=local_dst)
                logfile = os.path.join(local_dst, os.path.basename(local_logfile))
            return logfile
        if self.cmd:
            logfile = super(FileLogEntity, self).collect(node, local_dst, remote_dst)

            return logfile


class PrometheusSnapshotsEntity(BaseLogEntity):
    """Get Prometheus snapshot entity

    Specific for Prometheus Log entity which allow to
    collect Prometheus snapshot or data dir

    Extends:
        BaseLogEntity
    """
    monitoring_data_dir_name = "scylla-monitoring-data"

    def __init__(self, *args, **kwargs):
        self.monitoring_data_dir = kwargs.pop('monitoring_data_dir', None)
        super(PrometheusSnapshotsEntity, self).__init__(*args, **kwargs)

        self.current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    @retrying(n=3, sleep_time=10, allowed_exceptions=(PrometheusSnapshotErrorException,),
              message='Create prometheus snapshot')
    def create_prometheus_snapshot(self, node):
        ps = PrometheusDBStats(host=node.external_address)
        result = ps.create_snapshot()
        if "success" in result['status']:
            snapshot_dir = os.path.join(self.monitoring_data_dir,
                                        "snapshots",
                                        result['data']['name'])
            return snapshot_dir
        else:
            raise PrometheusSnapshotErrorException(result)

    def get_prometheus_snapshot_remote(self, node):
        try:
            snapshot_dir = self.create_prometheus_snapshot(node)
        except Exception as details:
            LOGGER.warning(
                'Create prometheus snapshot failed {}.\nUse prometheus data directory'.format(details))
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


class MonitoringStackEntity(BaseLogEntity):
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
        with io.open(os.path.join(tmp_dir, 'annotations.json'), 'w', encoding='utf-8') as f:
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
        basedir = MonitoringStackEntity.get_monitoring_base_dir(node)
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
            monitor_version = None
            scylla_version = None
        return name, monitor_version, scylla_version

    def get_grafana_annotations(self, node):
        annotations_url = "http://{node_ip}:{grafana_port}/api/annotations"
        try:
            res = requests.get(annotations_url.format(
                node_ip=node.external_address, grafana_port=self.grafana_port))
            if res.ok:
                return res.text
        except Exception as ex:
            LOGGER.warning("unable to get grafana annotations [%s]", str(ex))

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
        return True if result.exited == 0 else False

    def install_phantom_js(self):
        if not self.phantomjs_installed:
            localrunner = LocalCmdRunner()
            phantomjs_base = self.phantomjs_base
            phantomjs_tar = "{phantomjs_base}.tar.bz2".format(**locals())
            phantomjs_url = "https://bitbucket.org/ariya/phantomjs/downloads/{phantomjs_tar}".format(**locals())
            install_phantom_js_script = dedent("""
                rm -rf {phantomjs_base}*
                curl {phantomjs_url} -o {phantomjs_tar} -L
                tar xvfj {phantomjs_tar}
            """.format(**locals()))
            localrunner.run("bash -ce '%s'" % install_phantom_js_script)
            localrunner.run(
                "cd {0.phantomjs_base} && sed -e 's/200);/10000);/' examples/rasterize.js |grep -v 'use strict' > r.js".format(self))
        else:
            LOGGER.debug("PhantomJS is already installed!")


class GrafanaScreenShotEntity(GrafanaEntity):
    """Collect Grafana screenshot

    Collect grafana screenshot

    Extends:
        GrafanaEntity
    """

    def _get_screenshot_link(self, grafana_url, screenshot_path):
        LocalCmdRunner().run("cd {0.phantomjs_base} && bin/phantomjs r.js \"{1}\" \"{2}\" 1920px".format(
            self, grafana_url, screenshot_path))

    def get_grafana_screenshot(self, node, local_dst):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        _, _, monitoring_version = MonitoringStackEntity.get_monitoring_version(node)
        try:
            screenshots = []

            for i, screenshot in enumerate(self.grafana_entity_names):
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

        except Exception as details:
            LOGGER.error('Error taking monitor snapshot: %s',
                         str(details))
            return []

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        return self.get_grafana_screenshot(node, local_dst)


class GrafanaSnapshotEntity(GrafanaEntity):
    """Grafana snapshot

    Collect Grafana snapshot

    Extends:
        GrafanaEntity
    """

    def __init__(self, *args, **kwargs):
        super(GrafanaSnapshotEntity, self).__init__(*args, **kwargs)

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
        _, _, monitoring_version = MonitoringStackEntity.get_monitoring_version(node)
        try:

            snapshots = []
            for i, snapshot in enumerate(self.grafana_entity_names):
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

        except Exception as details:
            LOGGER.error('Error taking monitor snapshot: %s', str(details))
        return []

    def collect(self, node, local_dst, remote_dist=None, local_search_path=None):
        snapshots = self.get_grafana_snapshot(node)
        snapshots_file = os.path.join(local_dst, "grafana_snapshots")
        with open(snapshots_file, "w") as f:
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

    def __init__(self, nodes, test_id, storing_dir):
        self.test_id = test_id
        self.nodes = nodes
        self.local_dir = self.create_local_storing_dir(storing_dir)

    def create_local_storing_dir(self, base_local_dir):
        local_dir = os.path.join(base_local_dir, self.current_run,
                                 "{}-{}".format(self.cluster_log_type, self.test_id[:8]))
        try:
            os.makedirs(local_dir)
        except OSError as details:
            if not os.path.exists(local_dir):
                LOGGER.error("Folder is not created. {}".format(details))
                raise
        return local_dir

    def create_remote_storing_dir(self, node, path=''):
        if not path:
            path = node.name
        remote_dir = os.path.join(self.node_remote_dir, path)
        result = node.remoter.run('mkdir -p {}'.format(remote_dir), ignore_status=True)

        if result.exited > 0:
            LOGGER.error(
                'Remote storing folder not created.\n{}'.format(result))

        return remote_dir

    @staticmethod
    def collect_log_remotely(node, cmd, log_filename):
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
        result = node.remoter.run(
            "cd {archive_dir}; tar -czf {archive_name}.tar.gz {log_filename}".format(**locals()), ignore_status=True)
        return "{}.tar.gz".format(archive_name)

    @staticmethod
    def receive_log(node, remote_log_path, local_dir):
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        node.remoter.receive_files(src=remote_log_path, dst=local_dir)
        return local_dir

    @staticmethod
    def upload_logs(archive_path, storing_path):
        s3_link = S3Storage().upload_file(file_path=archive_path, dest_dir=storing_path)
        return s3_link

    def collect_logs(self, local_search_path=None):
        if not self.nodes:
            LOGGER.warning('No any running instances for {}'.format(self.cluster_log_type))
            return None

        def collect_logs_per_node(node):
            LOGGER.info('Collecting logs on host: {node.name}'.format(**locals()))
            try:
                remote_node_dir = self.create_remote_storing_dir(node)
                local_node_dir = os.path.join(self.local_dir, node.name)
                os.makedirs(local_node_dir)
                for log_entity in self.log_entities:
                    log_entity.collect(node, local_node_dir, remote_node_dir, local_search_path=local_search_path)
            except Exception as details:
                LOGGER.error("Error occured during collecting on host: {node.name}\n{details}".format(**locals()))
        try:
            ParallelObject(self.nodes, num_workers=3, timeout=300).run(collect_logs_per_node)
        except Exception:
            LOGGER.error('Error occured during collecting logs')
        final_archive = shutil.make_archive("{}".format(self.local_dir),
                                            "zip",
                                            root_dir=self.local_dir)
        s3_link = self.upload_logs(final_archive, "{0.test_id}/{0.current_run}".format(self))
        return s3_link

    def update_db_info(self):
        pass


class ScyllaLogCollector(LogCollector):
    """ScyllaDB cluster log collecting

    Collect on each node the logs for Scylla DB cluster

    Extends:
        LogCollector

    Variables:
        log_entities {list} -- LogEntities to collect
        cluster_log_type {str} -- cluster type name
    """
    log_entities = [FileLogEntity(name='database.log',
                                  command="sudo journalctl --no-tail --no-pager -u scylla-ami-setup.service -u scylla-io-setup.service -u scylla-server.service -u scylla-jmx.service",
                                  search_locally=True),
                    CommandLogEntity(name='cpu_info',
                                     command='cat /proc/cpuinfo'),
                    CommandLogEntity(name='mem_info',
                                     command='cat /proc/meminfo'),
                    CommandLogEntity(name='interrupts',
                                     command='cat /proc/interrupts'),
                    CommandLogEntity(name='vmstat',
                                     command='cat /proc/vmstat'),
                    CommandLogEntity(name='scylla.yaml',
                                     command='cat /etc/scylla/scylla.yaml'),
                    CommandLogEntity(name='coredumps.info',
                                     command='sudo coredumpctl info')
                    ]
    cluster_log_type = "db-cluster"


class LoaderLogCollector(LogCollector):
    cluster_log_type = "loader-set"


class MonitorLogCollector(LogCollector):
    """SCT monitor set log collector

    Collect required log entites for Monitor set nodes

    Extends:
        LogCollector

    """
    log_entities = [
        CommandLogEntity(name='aprom.log',
                         command='docker logs --details -t aprom'),
        CommandLogEntity(name='agraf.log',
                         command='docker logs --details -t agraf'),
        CommandLogEntity(name='aalert.log',
                         command='docker logs --details -t aalert'),
        FileLogEntity(name='scylla_manager.log',
                      command='sudo journalctl -u scylla-manager.service --no-tail',
                      search_locally=True),
        PrometheusSnapshotsEntity(name='prometheus_data'),
        MonitoringStackEntity(name='monitoring-stack'),
        GrafanaScreenShotEntity(name='grafana-screenshot'),
        GrafanaSnapshotEntity(name='grafana-snapshot')
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
        FileLogEntity(name='sct.log',
                      search_locally=True),
        FileLogEntity(name='raw_events.log',
                      search_locally=True),
        FileLogEntity(name='events.log',
                      search_locally=True),
        FileLogEntity(name='output.log',
                      search_locally=True),
        FileLogEntity(name='critical.log',
                      search_locally=True)
    ]
    cluster_log_type = 'sct-runner'

    def collect_logs(self, local_search_path=None):
        for ent in self.log_entities:
            ent.collect(None, self.local_dir, None, local_search_path=local_search_path)
        if not os.listdir(self.local_dir):
            LOGGER.warning('No any local files')
            return self.local_dir, None, None

        final_archive = self.archive_dir_with_zip64(self.local_dir)

        s3_link = self.upload_logs(final_archive, "{0.test_id}/{0.current_run}".format(self))
        remove_files(self.local_dir)
        remove_files(final_archive)
        return s3_link

    def archive_dir_with_zip(self, logdir):
        return shutil.make_archive("{}".format(logdir), "zip", root_dir=logdir)

    def archive_dir_with_zip64(self, logdir):

        archive_base_name = os.path.basename(logdir)
        archive_storing_dir = os.path.dirname(logdir)
        archive_full_name = os.path.join(archive_storing_dir, archive_base_name + ".zip")
        cur_dir = os.getcwd()
        try:
            with zipfile.ZipFile(archive_full_name, "w", allowZip64=True) as arch:
                os.chdir(logdir)
                for root, _, files in os.walk(logdir):
                    for log_file in files:
                        arch.write(log_file)
                os.chdir(cur_dir)
        except Exception as details:
            LOGGER.error("{}".format(details))
            archive_full_name = None
        return archive_full_name


class Collector:
    """Collector instance

    Collector instance which should be run to collect logs and additional info
    as separate stage in pipeline on as subcommand in hydra

    """
    LOCAL_SEARCH_FOR_USER = "jenkins"

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
        self.storing_dir = None
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

    @property
    def current_user(self):
        return os.path.basename(self.base_dir)

    def define_test_id(self):
        if not self.test_id:
            result = LocalCmdRunner().run('cat {0.sct_result_dir}/latest/test_id'.format(self))
            if result.exited == 0 and result.stdout:
                self._test_id = result.stdout.strip()
                LOGGER.info("Found latest test_id: {0.test_id}".format(self))
        LOGGER.info("Collect logs for test-run with test-id: {}".format(self.test_id))
        return self.test_id

    def search_testdir_locally(self):
        if not self._test_id:
            self._test_id = self.define_test_id()
        LOGGER.info('Search dir with logs locally for test id: {}'.format(self.test_id))
        search_cmd = "grep -rl {0.test_id} {0.sct_result_dir}/*/test_id".format(self)
        result = LocalCmdRunner().run(cmd=search_cmd, ignore_status=True)
        LOGGER.info("Search result {}".format(result))
        if result.exited == 0 and result.stdout:
            found_dirs = result.stdout.strip().split('\n')
            LOGGER.info(found_dirs)
            return os.path.dirname(found_dirs[0])
        LOGGER.info("No any dirs found locally for current test id")
        return None

    def get_local_dir_with_logs(self):
        if self.current_user == self.LOCAL_SEARCH_FOR_USER:
            return self.search_testdir_locally()
        return None

    def get_aws_instances_by_testid(self):
        instances = list_instances_aws({"TestId": self.test_id}, running=True)
        for instance in instances:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            if 'db-node' in name[0]:
                self.db_cluster.append(CollectingNode(name=name[0],
                                                      ssh_login_info={
                                                          "hostname": instance['PublicIpAddress'],
                                                          "user": self.params['ami_db_scylla_user'],
                                                          "key_file": self.params['user_credentials_path']},
                                                      instance=instance,
                                                      global_ip=instance['PublicIpAddress']))
            if 'monitor-node' in name[0]:
                self.monitor_set.append(CollectingNode(name=name[0],
                                                       ssh_login_info={
                                                           "hostname": instance['PublicIpAddress'],
                                                           "user": self.params['ami_monitor_user'],
                                                           "key_file": self.params['user_credentials_path']},
                                                       instance=instance,
                                                       global_ip=instance['PublicIpAddress']))
            if 'loader-node' in name[0]:
                self.loader_set.append(CollectingNode(name=name[0],
                                                      ssh_login_info={
                                                          "hostname": instance['PublicIpAddress'],
                                                          "user": self.params['ami_loader_user'],
                                                          "key_file": self.params['user_credentials_path']},
                                                      instance=instance,
                                                      global_ip=instance['PublicIpAddress']))

    def get_gce_instances_by_testid(self):
        instances = list_instances_gce({"TestId": self.test_id}, running=True)
        for instance in instances:
            if 'db-node' in instance.name:
                self.db_cluster.append(CollectingNode(name=instance.name,
                                                      ssh_login_info={
                                                          "hostname": instance.public_ips[0],
                                                          "user": self.params['gce_image_username'],
                                                          "key_file": self.params['user_credentials_path']},
                                                      instance=instance,
                                                      global_ip=instance.public_ips[0]))
            if 'monitor-node' in instance.name:
                self.monitor_set.append(CollectingNode(name=instance.name,
                                                       ssh_login_info={
                                                           "hostname": instance.public_ips[0],
                                                           "user": self.params['gce_image_username'],
                                                           "key_file": self.params['user_credentials_path']},
                                                       instance=instance,
                                                       global_ip=instance.public_ips[0]))
            if 'loader-node' in instance.name:
                self.loader_set.append(CollectingNode(name=instance.name,
                                                      ssh_login_info={
                                                          "hostname": instance.public_ips[0],
                                                          "user": self.params['gce_image_username'],
                                                          "key_file": self.params['user_credentials_path']},
                                                      instance=instance,
                                                      global_ip=instance.public_ips[0]))

    def get_running_cluster_sets_by_testid(self):
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
        self.get_running_cluster_sets_by_testid()

        local_dir_with_logs = self.get_local_dir_with_logs()
        LOGGER.info("Found sct result directory with logs: {}".format(local_dir_with_logs))

        self.create_base_storing_dir(local_dir_with_logs)
        LOGGER.info("Created directory to storing collected logs: {}".format(self.storing_dir))
        for cluster_log_collector, nodes in self.cluster_log_collectors.items():
            if (cluster_log_collector is SCTLogCollector and self.current_user != self.LOCAL_SEARCH_FOR_USER):
                continue
            log_collector = cluster_log_collector(nodes,
                                                  test_id=self.test_id,
                                                  storing_dir=self.storing_dir)
            LOGGER.info("Start collect logs for cluster {}".format(log_collector.cluster_log_type))
            result = log_collector.collect_logs(local_search_path=local_dir_with_logs)
            results[log_collector.cluster_log_type] = result
            LOGGER.info("collected data for {}\n{}\n".format(log_collector.cluster_log_type, result))
        return results

    def create_base_storing_dir(self, test_dir=None):
        date_time_formatted = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        log_dir = os.path.basename(test_dir) if test_dir else date_time_formatted
        self.storing_dir = os.path.join(self.sct_result_dir, log_dir, 'collected_logs')
        if not os.path.exists(self.storing_dir):
            os.makedirs(self.storing_dir)
        if not os.path.exists(os.path.join(os.path.dirname(self.storing_dir), "test_id")):
            with open(os.path.join(os.path.dirname(self.storing_dir), "test_id"), "w") as f:
                f.write(self.test_id)
