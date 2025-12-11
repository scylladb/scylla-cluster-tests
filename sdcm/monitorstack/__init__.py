import logging
import os
import tarfile
import zipfile
import tempfile
import json
import datetime
import time
from textwrap import dedent
from pathlib import Path

import requests
import yaml

from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import list_logs_by_test_id, S3Storage, remove_files, get_free_port
from sdcm.utils.decorators import retrying

from sdcm.logcollector import GrafanaEntity, MonitoringStack  # noqa: PLC0415
from sdcm.db_stats import PrometheusDBStats  # noqa: PLC0415

LOGGER = logging.getLogger(name='monitoringstack')

GRAFANA_DOCKER_NAME = "agraf"
PROMETHEUS_DOCKER_NAME = "aprom"
ALERT_DOCKER_NAME = "aalert"

GRAFANA_DOCKER_PORT = get_free_port(ports_to_try=(3000, 0, ))
ALERT_DOCKER_PORT = get_free_port(ports_to_try=(6000, 0, ))
PROMETHEUS_DOCKER_PORT = get_free_port(ports_to_try=(9090, 0, ))
COMMAND_TIMEOUT = 1800


class ErrorUploadSCTDashboard(Exception):
    pass


class ErrorUploadAnnotations(Exception):
    pass


def restore_monitoring_stack(test_id, date_time=None):  # noqa: PLR0911
    if not is_docker_available():
        return False

    arch = get_monitoring_stack_archive(test_id, date_time)
    if not arch:
        return False
    # Arch element structure:
    #     {
    #         "file_path": log_file,
    #         "type": log_type,
    #         "link": link to archive,
    #         "date": date of create
    #     }

    LOGGER.info('Restoring monitoring stack from archive %s', arch['file_path'])
    monitoring_stack_base_dir = tempfile.mkdtemp()
    LOGGER.info('Download file {} to directory {}'.format(arch['link'], monitoring_stack_base_dir))
    downloaded_monitoring_archive = S3Storage().download_file(arch['link'],
                                                              dst_dir=monitoring_stack_base_dir)
    monitoring_data_arch = extract_monitoring_data_archive(downloaded_monitoring_archive,
                                                           monitoring_stack_base_dir)
    monitoring_stack_arch = extract_monitoring_stack_archive(downloaded_monitoring_archive,
                                                             monitoring_stack_base_dir)

    if not monitoring_data_arch:
        LOGGER.error("No prometheus snapshot were found in arch %s", arch['file_path'])
        return False
    if not monitoring_stack_arch:
        LOGGER.error("No monitoring stack archive were found in arch %s", arch['file_path'])
        return False

    if sorted(monitoring_data_arch) != sorted(monitoring_stack_arch):
        LOGGER.error("Prometheus data is not fitting to to monitoring stack. Clusters are not same."
                     "\n Prometheus data for clusters: %s"
                     "\nMonitoring stack for clusters: %s",
                     monitoring_data_arch, monitoring_stack_arch)
        return False

    monitors_containers = {}
    for monitoring_cluster, monitoring_arch in monitoring_stack_arch.items():
        LOGGER.info("Creating monitoring stack directories for %s cluster", monitoring_cluster)
        monitoring_data_dir = create_monitoring_data_dir(monitoring_stack_base_dir,
                                                         monitoring_data_arch[monitoring_cluster],
                                                         tenant_dir=monitoring_cluster)
        monitoring_stack_dir = create_monitoring_stack_dir(monitoring_stack_base_dir, monitoring_arch)

        if not monitoring_stack_dir or not monitoring_data_dir:
            LOGGER.error('Creating monitoring stack directories failed:\ndata_dir: %s; stack_dir: %s',
                         monitoring_data_dir, monitoring_stack_dir)
        _, scylla_version = get_monitoring_stack_scylla_version(monitoring_stack_dir)

        containers_ports = run_monitoring_stack_containers(monitoring_stack_dir, monitoring_data_dir, scylla_version,
                                                           tenants_number=len(monitoring_stack_arch))
        if not containers_ports:
            return False

        # To support multi-tenant cluster
        dashboard_file = get_nemesis_dashboard_file_for_cluster(base_dir=monitoring_stack_base_dir,
                                                                archive=monitoring_arch,
                                                                file_name_for_search="scylla-dash-per-server-nemesis")
        LOGGER.info("Nemesis dashboard file for the cluster %s is '%s'", monitoring_cluster, dashboard_file)
        status = restore_grafana_dashboards_and_annotations(monitoring_stack_dir,
                                                            grafana_docker_port=containers_ports["grafana_docker_port"],
                                                            sct_dashboard_file=dashboard_file)
        if not status:
            return False

        status = verify_monitoring_stack(containers_ports=containers_ports)
        if not status:
            remove_files(monitoring_stack_base_dir)
            return False

        LOGGER.info("Monitoring stack for cluster %s is running", monitoring_cluster)
        monitors_containers[monitoring_cluster] = containers_ports
    return monitors_containers


def get_monitoring_stack_archive(test_id, date_time):
    """Return monitor_set archive file info

    choose from list of archives files stored on S3 for test_run with test_id
    only monitor-set archives sorted by date_time of archive creation
    if date_time profided return archive file info for found files,
    otherwise return latest archive file info
    archive file info:
        {
         "file_path": log_file,
         "type": log_type,
         "link": link to archive,
         "date": date of create
        }

    :param test_id: test-run test-id
    :type test_id: str
    :param date_time: datetime from column eturned by hydra investigate show-logs <TEST_ID>
    :type date_time: str
    :returns: file info data
    :rtype: {dict}
    """
    arch = None

    stored_files_by_test_id = list_logs_by_test_id(test_id)
    monitoring_stack_archives = [arch for arch in stored_files_by_test_id if 'monitor-set' in arch['type']]
    if not monitoring_stack_archives:
        LOGGER.warning('No any archive for monitoring stack')
        return False

    if date_time:
        found_archives = [archive for archive in monitoring_stack_archives
                          if archive['date'] == datetime.datetime.strptime(date_time, "%Y%m%d_%H%M%S")]
        if not found_archives:
            LOGGER.warning('Monitoring stack archive for date %s was not found', date_time)
        else:
            arch = found_archives[-1]
    else:
        arch = monitoring_stack_archives[-1]

    return arch


def create_monitoring_data_dir(base_dir, archive, tenant_dir=""):
    monitoring_data_base_dir = os.path.join(base_dir, 'monitoring_data_dir')
    if tenant_dir:
        monitoring_data_base_dir = os.path.join(monitoring_data_base_dir, tenant_dir)

    cmd = dedent("""
        mkdir -p {data_dir}
        cd {data_dir}
        cp {archive} ./
        tar -xvf {archive_name}
        chmod -R 777 {data_dir}
        """.format(data_dir=monitoring_data_base_dir,
                   archive=archive,
                   archive_name=os.path.basename(archive)))
    result = LocalCmdRunner().run(cmd, timeout=COMMAND_TIMEOUT, ignore_status=True)
    if result.exited > 0:
        LOGGER.error("Error during extracting prometheus snapshot. Switch to next archive")
        return False
    return get_monitoring_data_dir(monitoring_data_base_dir)


def get_nemesis_dashboard_file_for_cluster(base_dir, archive, file_name_for_search):
    # To support multi-tenant cluster
    # We have dashboard file for every tenant. After untar the archive all files will be in the same folder and we need
    # to recognize correct dashboard file for every tenant.
    # Here we get the correct file name
    cmd = f'tar -tf {archive} | grep "{file_name_for_search}"'

    result = LocalCmdRunner().run(cmd, timeout=COMMAND_TIMEOUT, ignore_status=True)
    if result.exited > 0:
        LOGGER.error("Error during extracting monitoring stack")
        return None

    dashboard_file = result.stdout.strip()
    if not dashboard_file:
        return None

    dashboard_file = Path(base_dir) / dashboard_file
    if not dashboard_file.exists():
        return None

    return dashboard_file


def create_monitoring_stack_dir(base_dir, archive):
    cmd = dedent("""
        cd {data_dir}
        cp {archive} ./
        tar -xvf {archive_name}
        chmod -R 777 {data_dir}
        """.format(data_dir=base_dir,
                   archive_name=os.path.basename(archive),
                   archive=archive))

    result = LocalCmdRunner().run(cmd, timeout=COMMAND_TIMEOUT, ignore_status=True)
    if result.exited > 0:
        LOGGER.error("Error during extracting monitoring stack")
        return False

    return get_monitoring_stack_dir(base_dir)


def extract_monitoring_stack_archive(downloaded_monitor_set_archive, monitoring_stack_base_dir):

    return extract_file_from_archive('monitoring_data_stack',
                                     downloaded_monitor_set_archive,
                                     monitoring_stack_base_dir)


def extract_monitoring_data_archive(downloaded_monitor_set_archive, monitoring_stack_base_dir):

    return extract_file_from_archive('prometheus_data',
                                     downloaded_monitor_set_archive,
                                     monitoring_stack_base_dir)


def extract_file_from_archive(pattern, archive, extract_dir) -> dict[str, str | Path]:
    if archive.endswith('.zip'):
        return extract_file_from_zip_archive(pattern, archive, extract_dir)
    if archive.endswith('.tar.gz'):
        return extract_file_from_tar_archive(pattern, archive, extract_dir)
    if archive.endswith('.tar.zst'):
        return extract_file_from_tar_zstd(pattern, archive, extract_dir)
    raise ValueError(f"Not supported archive type - {archive.split('.')[-1]}")


def extract_file_from_zip_archive(pattern, archive, extract_dir):
    found_file = None
    with zipfile.ZipFile(archive) as zfile:
        for name in zfile.namelist():
            if pattern in name:
                target_dir = os.path.join(extract_dir, name)
                if is_path_outside_of_dir(target_dir, extract_dir):
                    LOGGER.warning('Skipping %s file it leads to outside of the target dir', name)
                    continue
                zfile.extract(name, extract_dir)
                found_file = target_dir
                break
    return found_file


def is_path_outside_of_dir(path, base) -> bool:
    real_base = os.path.realpath(base)
    return os.path.commonpath((os.path.realpath(path), real_base)) != real_base


def extract_file_from_tar_archive(pattern, archive, extract_dir):
    found_file = {}
    with tarfile.open(archive) as tar_file:
        for name in tar_file.getnames():
            if pattern in name:
                target_dir = os.path.join(extract_dir, name)
                if is_path_outside_of_dir(target_dir, extract_dir):
                    LOGGER.warning('Skipping %s file it leads to outside of the target dir', name)
                    continue
                tar_file.extract(name, extract_dir)
                found_file[os.path.dirname(target_dir).split("/")[-1]] = target_dir
    return found_file


def extract_file_from_tar_zstd(pattern, archive, extract_dir) -> dict[str, str | Path]:
    found_file = {}
    result = LocalCmdRunner().run(f'tar --zstd -tf {archive}')
    if not result.ok:
        LOGGER.error("Error listing archive contents: %s", result.stderr)
    else:
        for name in result.stdout.splitlines():
            if pattern in name:
                target_path = Path(extract_dir) / name
                if is_path_outside_of_dir(target_path, extract_dir):
                    LOGGER.warning('Skipping %s file it leads to outside of the target dir', name)
                    continue
                extract_result = LocalCmdRunner().run(f"tar --zstd -xvf '{archive}' -C '{extract_dir}' '{name}'")
                if not extract_result.ok:
                    LOGGER.error("Error extracting file %s: %s", name, extract_result.stderr)
                else:
                    found_file[target_path.parent.name] = target_path
    return found_file


def get_monitoring_data_dir(base_dir):
    monitoring_data_dirs = [d for d in os.listdir(base_dir)
                            if os.path.isdir(os.path.join(base_dir, d))]
    if not monitoring_data_dirs:
        LOGGER.error("%s is empty. No data found", base_dir)
        return False
    return os.path.join(base_dir, monitoring_data_dirs[0])


def get_monitoring_stack_dir(base_dir):
    monitoring_stack_dir = [d for d in os.listdir(base_dir) if 'scylla-monitoring' in d]
    if not monitoring_stack_dir:
        LOGGER.error("%s is empty. No data found", base_dir)
        return False
    return os.path.join(base_dir, monitoring_stack_dir[0])


def get_monitoring_stack_scylla_version(monitoring_stack_dir):
    try:
        with open(os.path.join(monitoring_stack_dir, 'monitor_version'), encoding="utf-8") as f:
            versions = f.read().strip()
        monitoring_version, scylla_version = versions.split(':')
        if not requests.head(f'https://github.com/scylladb/scylla-monitoring/tree/{monitoring_version}'
                             f'/grafana/build/ver_{scylla_version}').ok:
            scylla_version = 'master'

        return monitoring_version, scylla_version
    except Exception:  # noqa: BLE001
        return 'branch-3.0', 'master'


def restore_grafana_dashboards_and_annotations(monitoring_dockers_dir, grafana_docker_port, sct_dashboard_file):
    status = []
    try:
        status.append(restore_sct_dashboards(grafana_docker_port=grafana_docker_port,
                                             sct_dashboard_file=sct_dashboard_file))
        status.append(restore_annotations_data(monitoring_dockers_dir, grafana_docker_port=grafana_docker_port))
    except Exception as details:  # noqa: BLE001
        LOGGER.error("Error during uploading sct monitoring data %s", details)
        status.append(False)

    return all(status)


def run_monitoring_stack_containers(monitoring_stack_dir, monitoring_data_dir, scylla_version, tenants_number=1):
    try:
        return start_dockers(monitoring_stack_dir, monitoring_data_dir, scylla_version, tenants_number)
    except Exception as details:  # noqa: BLE001
        LOGGER.error("Dockers are not started. Error: %s", details)
        return {}


@retrying(n=3, sleep_time=20, message='Uploading sct dashboard')
def restore_sct_dashboards(grafana_docker_port, sct_dashboard_file):
    if not sct_dashboard_file:
        LOGGER.warning('There is no dashboard %s. defaults to master dashboard', sct_dashboard_file)
        sct_dashboard_file_name = "scylla-dash-per-server-nemesis.master.json"
        sct_dashboard_file = [Path(__file__).parent.parent.parent / 'data_dir' / sct_dashboard_file_name]

    dashboard_url = f'http://localhost:{grafana_docker_port}/api/dashboards/db'
    with open(sct_dashboard_file, encoding="utf-8") as f:
        dashboard_config = json.load(f)
        # NOTE: remove value from the 'dashboard.id' field to avoid following error:
        #
        #       Error message {"message":"Dashboard not found","status":"not-found"}
        #
        #       Creating dashboard from scratch we should not have 'id', because Grafana will try
        #       to 'update' existing one which is absent in such a case.
        if dashboard_config.get('dashboard', {}).get('id'):
            dashboard_config['dashboard']['id'] = None

    try:
        res = requests.post(dashboard_url,
                            data=json.dumps(dashboard_config),
                            headers={'Content-Type': 'application/json'})

        if res.status_code != 200:
            LOGGER.info('Error uploading dashboard %s. Error message %s', sct_dashboard_file, res.text)
            raise ErrorUploadSCTDashboard('Error uploading dashboard {}. Error message {}'.format(
                sct_dashboard_file,
                res.text))
        LOGGER.info('Dashboard %s loaded successfully', sct_dashboard_file)
        return True
    except Exception as details:
        LOGGER.error('Error uploading dashboard %s. Error message %s', sct_dashboard_file, details)
        raise


@retrying(n=3, sleep_time=20, message='Uploading annotations data')
def restore_annotations_data(monitoring_stack_dir, grafana_docker_port):
    annotations_file = os.path.join(monitoring_stack_dir,
                                    'sct_monitoring_addons',
                                    'annotations.json')

    if not os.path.exists(annotations_file):
        LOGGER.info('There is no annotations file.Skip loading annotations')
        return False
    try:
        with open(annotations_file, encoding="utf-8") as f:
            annotations = json.load(f)

        annotations_url = f"http://localhost:{grafana_docker_port}/api/annotations"
        for an in annotations:
            res = requests.post(annotations_url, data=json.dumps(an), headers={'Content-Type': 'application/json'})
            if res.status_code != 200:
                LOGGER.info('Error during uploading annotation %s. Error message %s', an, res.text)
                raise ErrorUploadAnnotations('Error during uploading annotation {}. Error message {}'.format(an,
                                                                                                             res.text))
        LOGGER.info('Annotations loaded successfully')
        return True
    except Exception as details:
        LOGGER.error("Error during annotations data upload %s", details)
        raise


@retrying(n=3, sleep_time=5, message='Start docker containers')
def start_dockers(monitoring_dockers_dir, monitoring_stack_data_dir, scylla_version, tenants_number):
    graf_port = get_free_port(ports_to_try=[GRAFANA_DOCKER_PORT + i for i in range(tenants_number)] + [0])
    alert_port = get_free_port(ports_to_try=[ALERT_DOCKER_PORT + i for i in range(tenants_number)] + [0])
    prom_port = get_free_port(ports_to_try=[PROMETHEUS_DOCKER_PORT + i for i in range(tenants_number)] + [0])

    lr = LocalCmdRunner()
    lr.run('cd {monitoring_dockers_dir}; ./kill-all.sh -g {graf_port} -m {alert_port} -p {prom_port}'.format(**locals()),
           ignore_status=True, verbose=False)

    # clear scylla nodes from configuration
    servers_yaml = Path(monitoring_dockers_dir) / 'config' / 'scylla_servers.yml'
    servers_yaml.write_text("- targets: []")

    # clear SCT scrape configurations
    prom_tmpl_file = Path(monitoring_dockers_dir) / 'prometheus' / 'prometheus.yml.template'
    templ_yaml = yaml.safe_load(prom_tmpl_file.read_text())

    def remove_sct_metrics(metric):
        return '_metrics' not in metric['job_name']

    templ_yaml["scrape_configs"] = list(filter(remove_sct_metrics, templ_yaml["scrape_configs"]))
    prom_tmpl_file.write_text(yaml.safe_dump(templ_yaml))

    # clear alert manager configuration to the minimal need config
    alert_manager_config = Path(monitoring_dockers_dir) / 'prometheus' / 'rule_config.yml'
    alert_manager_config.write_text(dedent("""
        global:
          resolve_timeout: 5m
        route:
          receiver: "null"
          group_by:
          - job
          group_wait: 30s
          group_interval: 5m
          repeat_interval: 12h
        receivers:
        - name: "null"
    """).strip())

    cmd = dedent("""cd {monitoring_dockers_dir};
            # patch to make podman work for result that don't have https://github.com/scylladb/scylla-monitoring/pull/2149
            sed -i 's/DOCKER_HOST/HOST_ADDRESS/' *.sh

            echo "" > UA.sh
            bash -x ./start-all.sh \
            $(grep -q -- --no-renderer ./start-all.sh && echo "--no-renderer")  \
            $(grep -q -- --no-loki ./start-all.sh && echo "--no-loki")  \
            -g {graf_port} -m {alert_port} -p {prom_port} \
            -s {monitoring_dockers_dir}/config/scylla_servers.yml \
            -d {monitoring_stack_data_dir} -v {scylla_version} \
            -b '-storage.tsdb.retention.time=100y' \
            -c 'GF_USERS_DEFAULT_THEME=dark'""".format(**locals()))
    res = lr.run(cmd, ignore_status=True)
    if res.ok:
        LOGGER.info("Docker containers for monitoring stack are started")
    else:
        LOGGER.error("Failure to start monitoring stack stderr: %s", res.stderr)
        LOGGER.error("Failure to start monitoring stack stdout: %s", res.stdout)

        raise Exception("fail to start monitoring stack")
    return {"grafana_docker_port": graf_port,
            "alert_docker_port": alert_port,
            "prometheus_docker_port": prom_port}


def is_docker_available():
    LOGGER.info("Checking that docker is available...")
    result = LocalCmdRunner().run('docker ps', ignore_status=True, verbose=False)
    if result.ok:
        LOGGER.info('Docker is available')
        return True
    else:
        LOGGER.warning('Docker is not available on your computer. Please install docker software before continue')
        return False


def verify_monitoring_stack(containers_ports):
    checked_statuses = []
    checked_statuses.append(verify_dockers_are_running(containers_ports=containers_ports))
    checked_statuses.append(verify_grafana_is_available(grafana_docker_port=containers_ports["grafana_docker_port"]))
    checked_statuses.append(verify_prometheus_is_available(
        prometheus_docker_port=containers_ports["prometheus_docker_port"]))
    return all(checked_statuses)


def verify_dockers_are_running(containers_ports):
    result = LocalCmdRunner().run("docker ps --format '{{.Names}}'", ignore_status=True)
    docker_names = result.stdout.strip().split()
    result = LocalCmdRunner().run("docker ps --format '{{.Names}}'", ignore_status=True)
    grafana_docker_port = containers_ports["grafana_docker_port"]
    prometheus_docker_port = containers_ports["prometheus_docker_port"]
    if result.ok and docker_names:
        if f"{GRAFANA_DOCKER_NAME}-{grafana_docker_port}" in docker_names \
                and f"{PROMETHEUS_DOCKER_NAME}-{prometheus_docker_port}" in docker_names:
            LOGGER.info("Monitoring stack docker containers are running.\n%s", result.stdout)
            return True
    LOGGER.error("Monitoring stack containers are not running\nStdout:\n%s\nstderr:%s", result.stdout, result.stderr)
    return False


def verify_grafana_is_available(grafana_docker_port=GRAFANA_DOCKER_PORT):
    grafana_statuses = []
    for dashboard in GrafanaEntity.base_grafana_dashboards:
        try:
            LOGGER.info("Check dashboard %s", dashboard.title)
            result = MonitoringStack.get_dashboard_by_title(grafana_ip="localhost",
                                                            port=grafana_docker_port,
                                                            title=dashboard.title)
            grafana_statuses.append(result)
            LOGGER.info("Dashboard {} is available".format(dashboard.title))
        except Exception as details:  # noqa: BLE001
            LOGGER.error("Dashboard %s is not available. Error: %s", dashboard.title, details)
            grafana_statuses.append(False)

    result = any(grafana_statuses)

    if not result:
        LOGGER.error("None of the expected dashboards are available.")

    return result


def verify_prometheus_is_available(prometheus_docker_port=PROMETHEUS_DOCKER_PORT):
    """Get result from prometheus for latest 10 minutes

    Validate that request to Prometheus container is not failed
    :returns: True if request is successful, False otherwise
    :rtype: {bool}
    """

    time_end = time.time()
    time_start = time_end - 600
    try:
        LOGGER.info("Send request to Prometheus")
        prom_client = PrometheusDBStats("localhost", port=prometheus_docker_port)
        prom_client.get_throughput(time_start, time_end)
        LOGGER.info("Prometheus is up")
        return True
    except Exception as details:  # noqa: BLE001
        LOGGER.error("Error requesting prometheus %s", details)
        return False


def get_monitoring_stack_services(ports):
    return [{"service": "Grafana service", "name": GRAFANA_DOCKER_NAME, "port": ports["grafana_docker_port"]},
            {"service": "Prometheus service", "name": PROMETHEUS_DOCKER_NAME, "port": ports["prometheus_docker_port"]},
            {"service": "Alert service", "name": ALERT_DOCKER_NAME, "port": ports["alert_docker_port"]}]


def kill_running_monitoring_stack_services(ports=None):
    dockers_ports = ports or {"grafana_docker_port": GRAFANA_DOCKER_PORT,
                              "prometheus_docker_port": PROMETHEUS_DOCKER_PORT,
                              "alert_docker_port": ALERT_DOCKER_PORT}

    lr = LocalCmdRunner()
    for docker in get_monitoring_stack_services(ports=dockers_ports):
        LOGGER.info("Killing %s", docker['service'])
        lr.run('docker rm -f {name}-{port}'.format(name=docker['name'], port=docker['port']),
               ignore_status=True)
