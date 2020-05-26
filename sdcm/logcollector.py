#  pylint: disable=too-many-lines
import datetime
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile
import fnmatch
import traceback
import requests

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from sdcm.utils.common import (S3Storage, list_instances_aws, list_instances_gce,
                               ParallelObject, remove_files, get_builder_by_test_id,
                               get_testrun_dir, search_test_id_in_latest, filter_aws_instances_by_type,
                               filter_gce_instances_by_type, get_sct_root_path)
from sdcm.utils.decorators import retrying, cached_property
from sdcm.utils.get_username import get_username
from sdcm.db_stats import PrometheusDBStats
from sdcm.remote import RemoteCmdRunner, LocalCmdRunner
from sdcm.utils.auto_ssh import AutoSshContainerMixin
from sdcm.utils.remotewebbrowser import RemoteBrowser, WebDriverContainerMixin
from sdcm.utils.docker_utils import get_docker_bridge_gateway


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class CollectingNode(AutoSshContainerMixin, WebDriverContainerMixin):
    # pylint: disable=too-few-public-methods,too-many-instance-attributes

    def __init__(self, name, ssh_login_info=None, instance=None, global_ip=None, grafana_ip=None, tags=None):  # pylint: disable=too-many-arguments
        self._containers = {}
        self.name = name
        if ssh_login_info is None:
            self.remoter = LocalCmdRunner()
        else:
            self.remoter = RemoteCmdRunner(**ssh_login_info)
        self.ssh_login_info = ssh_login_info
        self._instance = instance
        self.external_address = global_ip
        if grafana_ip is None:
            self.grafana_address = global_ip
        else:
            self.grafana_address = grafana_ip
        self.tags = {**(tags or {}), "Name": self.name, }


class PrometheusSnapshotErrorException(Exception):
    pass


class BaseLogEntity():  # pylint: disable=too-few-public-methods
    """Base class for log entity

    LogEntity any file, command, operation, complex actions
    which require to be logged, stored locally, uploaded to
    S3 storage
    """
    collect_timeout = 300

    def __init__(self, name, command="", search_locally=False):
        self.name = name
        self.cmd = command
        self.search_locally = search_locally

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):  # pylint: disable=unused-argument,no-self-use
        raise Exception('Should be implemented in child class')


class CommandLog(BaseLogEntity):  # pylint: disable=too-few-public-methods
    """Command to get log and save output result to file on remote host

    CommandLog which allow to save the output (usually log output)
    to file. The log output should be produced by the command

    Extends:
        BaseLogEntity
    """

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        if not node.remoter or remote_dst is None:
            return None

        remote_logfile = LogCollector.collect_log_remotely(node=node,
                                                           cmd=self.cmd,
                                                           log_filename=os.path.join(remote_dst, self.name))
        archive_logfile = LogCollector.archive_log_remotely(node=node, log_filename=remote_logfile)
        LogCollector.receive_log(node=node,
                                 remote_log_path=archive_logfile,
                                 local_dir=local_dst,
                                 timeout=self.collect_timeout)
        return os.path.join(local_dst, os.path.basename(archive_logfile))


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

    def _is_file_collected(self, local_dst):
        for collected_file in os.listdir(local_dst):
            if self.name in collected_file or fnmatch.fnmatch(collected_file, "*{}".format(self.name)):
                return True
        return False

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        os.makedirs(local_dst, exist_ok=True)
        if self.search_locally and local_search_path:
            search_pattern = self.name if not node else "/".join([node.name, self.name])
            local_logfiles = self.find_local_files(local_search_path, search_pattern)
            for logfile in local_logfiles:
                shutil.copy(src=logfile, dst=local_dst)

        if self.cmd and not self._is_file_collected(local_dst):
            super(FileLog, self).collect(node, local_dst, remote_dst)

        return local_dst

    def collect_from_builder(self, builder, local_dst, search_in_dir):
        file_path = self.find_on_builder(builder, self.name, search_in_dir)
        if not file_path:
            return None
        archive = LogCollector.archive_log_remotely(builder, file_path)
        builder.remoter.receive_files(archive, local_dst, timeout=self.collect_timeout)
        return os.path.join(local_dst, os.path.basename(file_path))


class PrometheusSnapshots(BaseLogEntity):
    """Get Prometheus snapshot entity

    Specific for Prometheus Log entity which allow to
    collect Prometheus snapshot or data dir

    Extends:
        BaseLogEntity
    """
    monitoring_data_dir_name = "scylla-monitoring-data"
    collect_timeout = 3000

    def __init__(self, *args, **kwargs):
        self.monitoring_data_dir = kwargs.pop('monitoring_data_dir', None)
        super(PrometheusSnapshots, self).__init__(*args, **kwargs)

        self.current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    @retrying(n=3, sleep_time=10, allowed_exceptions=(PrometheusSnapshotErrorException, Exception),
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
        except (PrometheusSnapshotErrorException, Exception) as details:  # pylint: disable=broad-except
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
        base_dir = MonitoringStack.get_monitoring_base_dir(node)

        self.monitoring_data_dir = os.path.join(base_dir, self.monitoring_data_dir_name)

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        self.setup_monitor_data_dir(node)
        remote_snapshot_archive = self.get_prometheus_snapshot_remote(node)
        LogCollector.receive_log(
            node,
            remote_log_path=remote_snapshot_archive,
            local_dir=local_dst,
            timeout=self.collect_timeout)
        return os.path.join(local_dst, os.path.basename(remote_snapshot_archive))


class MonitoringStack(BaseLogEntity):
    """LogEntinty to collect MonitoringStack

    Collect monitoring stack

    Extends:
        BaseLogEntity

    Variables:
        grafana_port {number} -- Grafana server port
    """
    grafana_port = 3000
    collect_timeout = 1800

    @staticmethod
    def get_monitoring_base_dir(node):
        # Avoid cyclic dependencies
        from sdcm.cluster import BaseMonitorSet
        if hasattr(node, "parent_cluster") and node.parent_cluster:
            return node.parent_cluster.monitor_install_path_base
        else:
            return BaseMonitorSet.get_monitor_install_path_base(node)

    def get_monitoring_data_stack(self, node, local_dist):
        monitor_base_dir = self.get_monitoring_base_dir(node)
        monitor_install_dir_name, monitor_branch, monitor_version = self.get_monitoring_version(node)
        LOGGER.info("%s, %s, %s", monitor_install_dir_name, monitor_branch, monitor_version)
        if not monitor_branch or not monitor_version:
            return ""
        archive_name = "monitoring_data_stack_{monitor_branch}_{monitor_version}.tar.gz".format(**locals())

        annotations_json = self.get_grafana_annotations(node.grafana_address)
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
                                   dst=local_dist, timeout=self.collect_timeout)
        local_archive_path = os.path.join(local_dist, archive_name)
        return local_archive_path

    @staticmethod
    def get_monitoring_version(node):
        basedir = MonitoringStack.get_monitoring_base_dir(node)
        result = node.remoter.run(
            f'ls {basedir} | grep scylla-monitoring-src', ignore_status=True, verbose=False)
        name = result.stdout.strip()
        if not name:
            LOGGER.error("Dir with scylla monitoring stack was not found")
            return None, None, None
        result = node.remoter.run(
            f"cat {basedir}/scylla-monitoring-src/monitor_version", ignore_status=True, verbose=False)
        try:
            monitor_version, scylla_version = result.stdout.strip().split(':')
        except ValueError:
            monitor_version = None
            scylla_version = None
        return name, monitor_version, scylla_version

    def get_grafana_annotations(self, grafana_ip):  # pylint: disable=inconsistent-return-statements
        annotations_url = "http://{grafana_ip}:{grafana_port}/api/annotations"
        try:
            res = requests.get(annotations_url.format(
                grafana_ip=grafana_ip, grafana_port=self.grafana_port))
            if res.ok:
                return res.text
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("unable to get grafana annotations [%s]", str(ex))
            return ""

    @staticmethod
    def dashboard_exists(grafana_ip, uid):
        """Check on Grafana server, that dashboard exists

        Send request to Grafana and validate that dashboard with uid
        provided as "uid" parameter is available and could be requested

        Arguments:
            node {CollectingNode} -- Remote host with grafana server
            uid {str} -- uid of grafana dashboard

        Returns:
            bool -- return True if exists, false otherwise
        """
        checked_dashboard_url = "http://{grafana_ip}:{grafana_port}/api/dashboards/db/{uid}"
        try:
            res = requests.get(checked_dashboard_url.format(grafana_ip=grafana_ip,
                                                            grafana_port=MonitoringStack.grafana_port,
                                                            uid=uid))
            return bool(res.ok and res.json())

        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.warning("Error during checking if dashboard is exists. %s", ex)
            return False

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        local_archive = self.get_monitoring_data_stack(node, local_dst)
        if not local_archive:
            LOGGER.error('Monitoring stack were not collected')
        return local_archive


class GrafanaEntity(BaseLogEntity):  # pylint: disable=too-few-public-methods
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
    base_grafana_entity_names = [
        {
            'name': 'scylla-per-server-metrics-nemesis',
            'path': 'dashboard/db/{dashboard_name}-{version}',
            'resolution': '1920px*7000px',
        },
        {
            'name': 'overview',
            'path': 'd/overview-{version}/scylla-{dashboard_name}',
            'resolution': '1920px*4000px'
        }
    ]
    grafana_port = 3000
    grafana_entity_url_tmpl = "http://{node_ip}:{grafana_port}/{path}?from={st}&to=now"
    sct_base_path = get_sct_root_path()

    def __init__(self, *args, **kwargs):
        test_start_time = kwargs.pop("test_start_time", None)
        if not test_start_time:
            # set test start time previous 6 hours
            test_start_time = time.time() - (6 * 3600)
        self.start_time = str(test_start_time).split('.')[0] + '000'
        self.grafana_entity_names = self.base_grafana_entity_names + kwargs.pop("extra_entities", [])
        super(GrafanaEntity, self).__init__(*args, **kwargs)


class GrafanaScreenShot(GrafanaEntity):
    """Collect Grafana screenshot

    Collect grafana screenshot

    Extends:
        GrafanaEntity
    """
    collect_timeout = 150

    @retrying(n=5)
    def get_grafana_screenshot(self, node, local_dst):
        """
            Take screenshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        _, _, monitoring_version = MonitoringStack.get_monitoring_version(node)
        if not monitoring_version:
            LOGGER.warning("Monitoring version was not found")
            return []
        version = monitoring_version.replace('.', '-')

        try:
            screenshots = []
            remote_browser = RemoteBrowser(node)

            for screenshot in self.grafana_entity_names:
                dashboard_exists = MonitoringStack.dashboard_exists(grafana_ip=node.grafana_address,
                                                                    uid="-".join([screenshot['name'],
                                                                                  version])
                                                                    )
                if not dashboard_exists:
                    version = "master"

                path = screenshot['path'].format(
                    version=version,
                    dashboard_name=screenshot['name'])
                grafana_url = self.grafana_entity_url_tmpl.format(
                    node_ip=node.grafana_address,
                    grafana_port=self.grafana_port,
                    path=path,
                    st=self.start_time)
                screenshot_path = os.path.join(local_dst,
                                               "%s-%s-%s-%s.png" % (self.name,
                                                                    screenshot['name'],
                                                                    datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
                                                                    node.name))
                LOGGER.debug("Get screenshot for url %s, save to %s", grafana_url, screenshot_path)
                remote_browser.get_screenshot(grafana_url, screenshot_path,
                                              screenshot['resolution'],
                                              load_page_screenshot_delay=self.collect_timeout)
                screenshots.append(screenshot_path)

            return screenshots

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error(f'Error taking monitor screenshot: {str(details)}, traceback: {traceback.format_exc()}')
            return []

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        node.logdir = local_dst
        os.makedirs(local_dst, exist_ok=True)
        return self.get_grafana_screenshot(node, local_dst)


class GrafanaSnapshot(GrafanaEntity):
    """Grafana snapshot

    Collect Grafana snapshot

    Extends:
        GrafanaEntity
    """

    snapshot_locators_sequence = [
        (By.XPATH, """//button[contains(@class, "navbar-button--share")]"""),
        (By.LINK_TEXT, """Snapshot"""),
        (By.XPATH, """//a[contains(text(), "Snapshot") and contains(@class, "gf-tabs-link")]"""),
        (By.XPATH, """//button[contains(text(), "Publish to snapshot.raintank.io") and contains(@class, "gf-form-btn")]"""),
        (By.XPATH, """//a[contains(@href, "https://snapshot.raintank.io")]""")
    ]

    def _get_shared_snapshot_link(self, remote_browser, grafana_url):
        """Get link from page to remote snapshot on https://snapshot.raintank.io

        using selenium remote web driver remote_browser find sequentially web_element by locators
        in self.snapshot_locators_sequence run actiom WebElement.click() and
        get value from result link found by latest element in snapshot_locators_sequence

        :param remote_browser: remote webdirver instance
        :type remote_browser: selenium.webdriver.Remote
        :param grafana_url: url to load and get snapshot
        :type grafana_url: str
        :returns: return value of link to remote snapshot on https://snapshot.raintank.io
        :rtype: {str}
        """
        remote_browser.get(grafana_url)

        for element in self.snapshot_locators_sequence[:-1]:
            WebDriverWait(remote_browser, 60).until(EC.visibility_of_element_located(element))
            found_element = remote_browser.find_element(*element)
            found_element.click()
        snapshot_link_locator = self.snapshot_locators_sequence[-1]
        WebDriverWait(remote_browser, 60).until(EC.visibility_of_element_located(snapshot_link_locator))
        snapshot_link_element = remote_browser.find_element(*snapshot_link_locator)

        LOGGER.debug(snapshot_link_element.text)
        return snapshot_link_element.text

    @retrying(n=5)
    def get_grafana_snapshot(self, node):
        """
            Take snapshot of the Grafana per-server-metrics dashboard and upload to S3
        """
        _, _, monitoring_version = MonitoringStack.get_monitoring_version(node)
        if not monitoring_version:
            LOGGER.warning("Monitoring version was not found")
            return []
        try:
            remote_browser = RemoteBrowser(node)
            snapshots = []
            for snapshot in self.grafana_entity_names:
                version = monitoring_version.replace('.', '-')
                dashboard_exists = MonitoringStack.dashboard_exists(grafana_ip=node.grafana_address,
                                                                    uid="-".join([snapshot['name'],
                                                                                  version])
                                                                    )
                if not dashboard_exists:
                    version = "master"

                path = snapshot['path'].format(
                    version=version,
                    dashboard_name=snapshot['name'])
                grafana_url = self.grafana_entity_url_tmpl.format(
                    node_ip=node.grafana_address,
                    grafana_port=self.grafana_port,
                    path=path,
                    st=self.start_time)
                LOGGER.info("Get snapshot link for url %s", grafana_url)
                snapshots.append(self._get_shared_snapshot_link(remote_browser.browser, grafana_url))

            return snapshots

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error(f'Error taking monitor snapshot: {str(details)}, traceback: {traceback.format_exc()}')
        return []

    def collect(self, node, local_dst, remote_dst=None, local_search_path=None):
        node.logdir = local_dst
        os.makedirs(local_dst, exist_ok=True)
        snapshots = self.get_grafana_snapshot(node)
        snapshots_file = os.path.join(local_dst, "grafana_snapshots")
        with open(snapshots_file, "w") as f:  # pylint: disable=invalid-name
            for snapshot in snapshots:
                f.write(snapshot + '\n')

        return {'links': snapshots, 'file': snapshots_file}


class LogCollector:
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
    collect_timeout = 300

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
        try:
            remote_dir = os.path.join(self.node_remote_dir, path)
            result = node.remoter.run('mkdir -p {}'.format(remote_dir), ignore_status=True)

            if result.exited > 0:
                LOGGER.error(
                    'Remote storing folder not created.\n{}'.format(result))
                remote_dir = self.node_remote_dir

        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error("Error during creating remote directory %s", details)
            remote_dir = self.node_remote_dir

        return remote_dir

    @staticmethod
    def collect_log_remotely(node, cmd, log_filename):  # pylint: disable=unused-argument
        if not node.remoter:
            return False
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
        if node.remoter:
            node.remoter.run(
                "cd {archive_dir}; tar -czf {archive_name}.tar.gz {log_filename}".format(**locals()), ignore_status=True)
        return "{}.tar.gz".format(archive_name)

    @staticmethod
    def receive_log(node, remote_log_path, local_dir, timeout=300):
        os.makedirs(local_dir, exist_ok=True)
        if node.remoter:
            node.remoter.receive_files(src=remote_log_path,
                                       dst=local_dir,
                                       timeout=timeout)
        return local_dir

    @staticmethod
    def upload_logs(archive_path, storing_path):
        s3_link = S3Storage().upload_file(file_path=archive_path, dest_dir=storing_path)
        return s3_link

    def collect_logs(self, local_search_path=None):
        def collect_logs_per_node(node):
            LOGGER.info('Collecting logs on host: %s', node.name)
            remote_node_dir = self.create_remote_storage_dir(node)
            local_node_dir = os.path.join(self.local_dir, node.name)
            for log_entity in self.log_entities:
                try:
                    log_entity.collect(node, local_node_dir, remote_node_dir, local_search_path=local_search_path)
                except Exception as details:  # pylint: disable=unused-variable, broad-except
                    LOGGER.error("Error occured during collecting on host: %s\n%s", node.name, details)

        LOGGER.debug("Nodes list %s", [node.name for node in self.nodes])

        if not self.nodes:
            LOGGER.warning(f'No nodes found for {self.cluster_log_type} cluster. Logs will not be collected')
            return None
        try:
            workers_number = int(len(self.nodes) / 2)
            workers_number = len(self.nodes) if workers_number < 2 else workers_number
            ParallelObject(self.nodes, num_workers=workers_number, timeout=self.collect_timeout).run(
                collect_logs_per_node, ignore_exceptions=True)
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error('Error occured during collecting logs %s', details)

        if not os.listdir(self.local_dir):
            LOGGER.warning('Directory %s is empty', self.local_dir)
            return None

        final_archive = self.archive_dir_with_zip64(self.local_dir)
        s3_link = self.upload_logs(final_archive, "{0.test_id}/{0.current_run}".format(self))
        remove_files(self.local_dir)
        remove_files(final_archive)
        return s3_link

    def collect_logs_for_inactive_nodes(self, local_search_path=None):
        node_names = {node.name for node in self.nodes}
        if not local_search_path:
            return
        for root, _, _ in os.walk(local_search_path):
            if self.cluster_log_type in root:
                node_dirs = {dir_name for dir_name in os.listdir(
                    root) if os.path.isdir(os.path.join(root, dir_name))}
                if len(node_names) != len(node_dirs):
                    inactive_nodes = node_dirs.difference(node_names)
                    for dir_name in inactive_nodes:
                        for entity in self.log_entities:
                            entity.collect(CollectingNode(name=dir_name),
                                           os.path.join(self.local_dir, dir_name),
                                           local_search_path=os.path.join(root, dir_name))

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
                            command="sudo journalctl --no-tail --no-pager -u scylla-ami-setup.service -u scylla-image-setup.service -u scylla-io-setup.service -u scylla-server.service -u scylla-jmx.service",
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
    collect_timeout = 600

    def collect_logs(self, local_search_path=None):
        self.collect_logs_for_inactive_nodes(local_search_path)
        return super(ScyllaLogCollector, self).collect_logs(local_search_path)


class LoaderLogCollector(LogCollector):
    cluster_log_type = "loader-set"
    log_entities = [
        FileLog(name='*cassandra-stress*.log',
                search_locally=True),
        FileLog(name='*gemini-l*.log',
                search_locally=True),
        FileLog(name='gemini_result*.log',
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
    collect_timeout = 3600


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
                search_locally=True),
        FileLog(name='warning.log',
                search_locally=True),
        FileLog(name='error.log',
                search_locally=True),
        FileLog(name='normal.log',
                search_locally=True),
        FileLog(name='summary.log',
                search_locally=True),
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


class Collector():  # pylint: disable=too-many-instance-attributes,
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

    @cached_property
    def tags(self):
        return {"RunByUser": get_username(),
                "TestId": self.test_id,
                "keep_action": "terminate", }

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
                                                  global_ip=instance['PublicIpAddress'],
                                                  tags={**self.tags, "NodeType": "scylla-db", }))
        for instance in filtered_instances['monitor_nodes']:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            self.monitor_set.append(CollectingNode(name=name[0],
                                                   ssh_login_info={
                                                       "hostname": instance['PublicIpAddress'],
                                                       "user": self.params['ami_monitor_user'],
                                                       "key_file": self.params['user_credentials_path']},
                                                   instance=instance,
                                                   global_ip=instance['PublicIpAddress'],
                                                   tags={**self.tags, "NodeType": "monitor", }))
        for instance in filtered_instances['loader_nodes']:
            name = [tag['Value']
                    for tag in instance['Tags'] if tag['Key'] == 'Name']
            self.loader_set.append(CollectingNode(name=name[0],
                                                  ssh_login_info={
                                                      "hostname": instance['PublicIpAddress'],
                                                      "user": self.params['ami_loader_user'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance['PublicIpAddress'],
                                                  tags={**self.tags, "NodeType": "loader", }))

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
                                                  global_ip=instance.public_ips[0],
                                                  tags={**self.tags, "NodeType": "scylla-db", }))
        for instance in filtered_instances['monitor_nodes']:
            self.monitor_set.append(CollectingNode(name=instance.name,
                                                   ssh_login_info={
                                                       "hostname": instance.public_ips[0],
                                                       "user": self.params['gce_image_username'],
                                                       "key_file": self.params['user_credentials_path']},
                                                   instance=instance,
                                                   global_ip=instance.public_ips[0],
                                                   tags={**self.tags, "NodeType": "monitor", }))
        for instance in filtered_instances['loader_nodes']:
            self.loader_set.append(CollectingNode(name=instance.name,
                                                  ssh_login_info={
                                                      "hostname": instance.public_ips[0],
                                                      "user": self.params['gce_image_username'],
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance.public_ips[0],
                                                  tags={**self.tags, "NodeType": "loader", }))

    def get_docker_instances_by_testid(self):
        instances = list_instances_gce({"TestId": self.test_id}, running=True)
        filtered_instances = filter_gce_instances_by_type(instances)
        for instance in filtered_instances['db_nodes']:
            self.db_cluster.append(CollectingNode(name=instance.name,
                                                  ssh_login_info={
                                                      "hostname": instance.public_ips[0],
                                                      "user": 'scylla-test',
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance.public_ips[0],
                                                  tags={**self.tags, "NodeType": "scylla-db", }))
        self.monitor_set.append(CollectingNode(name=f"monitor-node-{self.test_id}-0",
                                               global_ip='127.0.0.1',
                                               grafana_ip=get_docker_bridge_gateway(LocalCmdRunner()),
                                               tags={**self.tags, "NodeType": "monitor", }))
        for instance in filtered_instances['loader_nodes']:
            self.loader_set.append(CollectingNode(name=instance.name,
                                                  ssh_login_info={
                                                      "hostname": instance.public_ips[0],
                                                      "user": 'scylla-test',
                                                      "key_file": self.params['user_credentials_path']},
                                                  instance=instance,
                                                  global_ip=instance.public_ips[0],
                                                  tags={**self.tags, "NodeType": "loader", }))

    def get_running_cluster_sets(self):
        if self.backend == 'aws':
            self.get_aws_instances_by_testid()
        elif self.backend == 'gce':
            self.get_gce_instances_by_testid()
        elif self.backend == 'docker':
            self.get_docker_instances_by_testid()

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
        os.makedirs(self.storage_dir, exist_ok=True)
        if not os.path.exists(os.path.join(os.path.dirname(self.storage_dir), "test_id")):
            with open(os.path.join(os.path.dirname(self.storage_dir), "test_id"), "w") as f:  # pylint: disable=invalid-name
                f.write(self.test_id)
