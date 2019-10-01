import logging
import os
import zipfile
import tempfile
import json
import datetime

from textwrap import dedent

import requests

from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import list_logs_by_test_id, S3Storage, retrying, remove_files

LOGGER = logging.getLogger(name='monitorstack')


def restore_monitor_stack(test_id, date_time=None):
    if not is_docker_available():
        return False

    monitor_stack_archives = get_monitor_set_archives(test_id)
    arch = get_monitor_stack_archive(monitor_stack_archives, date_time)
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
    monitor_stack_base_dir = tempfile.mkdtemp()
    LOGGER.info('Download file {} to directory {}'.format(arch['link'], monitor_stack_base_dir))
    downloaded_monitor_archive = S3Storage().download_file(arch['link'],
                                                           dst_dir=monitor_stack_base_dir)
    monitor_data_arch = extract_monitor_data_archive(downloaded_monitor_archive,
                                                     monitor_stack_base_dir)
    monitor_stack_arch = extract_monitor_stack_archive(downloaded_monitor_archive,
                                                       monitor_stack_base_dir)

    if not monitor_data_arch:
        LOGGER.error("No prometheus snapshot were found in arch %s", arch['file_path'])
        return False
    if not monitor_stack_arch:
        LOGGER.error("No monitor stack archive were found in arch %s", arch['file_path'])
        return False

    monitor_data_dir = create_monitoring_data_folder(monitor_stack_base_dir, monitor_data_arch)
    monitor_stack_dir = create_monitoring_stack_folder(monitor_stack_base_dir, monitor_stack_arch)

    if not monitor_stack_dir or not monitor_data_dir:
        LOGGER.error('Creating monitor stack directories failed:\ndata_dir: %s; stack_dir: %s',
                     monitor_data_dir, monitor_stack_dir)
    _, scylla_version = get_monitorstack_scylla_version(monitor_stack_dir)

    status = start_dockers(monitor_stack_dir,
                           monitor_data_dir,
                           scylla_version)
    if status:
        upload_sct_dashboards(monitor_stack_dir, scylla_version)
        upload_annotations(monitor_stack_dir)
        return status
    else:
        LOGGER.error('Error during dockers starting. Trying next arhive')
        remove_files(monitor_stack_base_dir)
        return False


def get_monitor_stack_archive(archives, date_time):
    arch = None
    if not archives:
        LOGGER.warning('No any archive for monitoring stack')
    if date_time:
        found_archives = [archive for archive in archives
                          if archive['date'] == datetime.datetime.strptime(date_time, "%Y%m%d_%H%M%S")]
        if not found_archives:
            LOGGER.warning('Monitor stack archive for date %s was not found', date_time)
        else:
            arch = found_archives[0]
    else:
        arch = archives[0]

    return arch


def create_monitoring_data_folder(base_dir, archive):
    monitor_data_base_dir = os.path.join(base_dir, 'monitor_data_dir')
    cmd = dedent("""
        mkdir -p {data_dir}
        cd {data_dir}
        cp {archive} ./
        tar -xvf {archive_name}
        chmod -R 777 {data_dir}
        """.format(data_dir=monitor_data_base_dir,
                   archive=archive,
                   archive_name=os.path.basename(archive)))
    result = LocalCmdRunner().run(cmd, ignore_status=True)
    if result.exited > 0:
        LOGGER.error("Error during extracting prometheus snapshot. Switch to next archive")
        return False
    return get_monitor_data_dir(monitor_data_base_dir)


def create_monitoring_stack_folder(base_dir, archive):
    cmd = dedent("""
        cd {data_dir}
        cp {archive} ./
        tar -xvf {archive_name}
        chmod -R 777 {data_dir}
        """.format(data_dir=base_dir,
                   archive_name=os.path.basename(archive),
                   archive=archive))

    result = LocalCmdRunner().run(cmd, ignore_status=True)
    if result.exited > 0:
        LOGGER.error("Error during extracting monitor stack")
        return False

    return get_monitor_stack_dir(base_dir)


def get_monitor_set_archives(test_id):
    stored_files_by_test_id = list_logs_by_test_id(test_id)
    monitor_stack_archives = [arch for arch in stored_files_by_test_id if 'monitor-set' in arch['type']]
    monitor_stack_archives.reverse()
    return monitor_stack_archives


def extract_monitor_stack_archive(downloaded_monitor_set_archive, monitor_stack_base_dir):

    return extract_file_from_zip_archive('monitoring_data_stack',
                                         downloaded_monitor_set_archive,
                                         monitor_stack_base_dir)


def extract_monitor_data_archive(downloaded_monitor_set_archive, monitor_stack_base_dir):

    return extract_file_from_zip_archive('prometheus_data',
                                         downloaded_monitor_set_archive,
                                         monitor_stack_base_dir)


def extract_file_from_zip_archive(pattern, archive, extract_dir):
    found_file = None
    with zipfile.ZipFile(archive) as zfile:
        for name in zfile.namelist():
            if pattern in name:
                zfile.extract(name, extract_dir)
                found_file = os.path.join(extract_dir, name)
                break
    return found_file


def get_monitor_data_dir(base_dir):
    monitor_data_dirs = [d for d in os.listdir(base_dir)
                         if os.path.isdir(os.path.join(base_dir, d))]
    return os.path.join(base_dir, monitor_data_dirs[0])


def get_monitor_stack_dir(base_dir):
    monitoring_stack_dir = [d for d in os.listdir(base_dir) if 'scylla-monitoring' in d][0]
    return os.path.join(base_dir, monitoring_stack_dir)


def get_monitorstack_scylla_version(monitor_stack_dir):
    try:
        with open(os.path.join(monitor_stack_dir, 'monitor_version'), 'r') as f:  # pylint: disable=invalid-name
            versions = f.read().strip()
        monitor_version, scylla_version = versions.split(':')
        return monitor_version, scylla_version
    except Exception:  # pylint: disable=broad-except
        return 'branch-3.0', 'master'


def upload_sct_dashboards(monitoring_dockers_dir, scylla_version):
    sct_dashboard_file_name = "scylla-dash-per-server-nemesis.{}.json".format(scylla_version)
    sct_dashboard_file = os.path.join(monitoring_dockers_dir,
                                      'sct_monitoring_addons',
                                      sct_dashboard_file_name)

    if not os.path.exists(sct_dashboard_file):
        LOGGER.info('There is no dashboard %s. Skip load dashboard', sct_dashboard_file_name)
        return False

    dashboard_url = 'http://localhost:3000/api/dashboards/db'
    with open(sct_dashboard_file, "r") as f:  # pylint: disable=invalid-name
        dashboard_config = json.load(f)

    res = requests.post(dashboard_url,
                        data=json.dumps(dashboard_config),
                        headers={'Content-Type': 'application/json'})

    if res.status_code != 200:
        LOGGER.info('Error uploading dashboard %s. Error message %s', sct_dashboard_file, res.text)
        return False
    LOGGER.info('Dashboard %s loaded successfully', sct_dashboard_file)
    return True


def upload_annotations(monitor_stack_dir):
    annotations_file = os.path.join(monitor_stack_dir,
                                    'sct_monitoring_addons',
                                    'annotations.json')

    if not os.path.exists(annotations_file):
        LOGGER.info('There is no annotations file.Skip loading annotations')
        return False

    with open(annotations_file, "r") as f:  # pylint: disable=invalid-name
        annotations = json.load(f)

    annotations_url = "http://localhost:3000/api/annotations"
    for an in annotations:  # pylint: disable=invalid-name
        res = requests.post(annotations_url, data=json.dumps(an), headers={'Content-Type': 'application/json'})
        if res.status_code != 200:
            LOGGER.info('Error during uploading annotation %s. Error message %s', an, res.text)
            return False
    LOGGER.info('Annotations loaded successfully')
    return True


@retrying(n=3, sleep_time=1, message='Start docker containers')
def start_dockers(monitoring_dockers_dir, monitoring_stack_data_dir, scylla_version):  # pylint: disable=unused-argument
    lr = LocalCmdRunner()  # pylint: disable=invalid-name
    lr.run('cd {}; ./kill-all.sh'.format(monitoring_dockers_dir))
    cmd = dedent("""cd {monitoring_dockers_dir};
            ./start-all.sh \
            -s {monitoring_dockers_dir}/config/scylla_servers.yml \
            -n {monitoring_dockers_dir}/config/node_exporter_servers.yml \
            -d {monitoring_stack_data_dir} -v {scylla_version}""".format(**locals()))
    res = lr.run(cmd, ignore_status=True)
    if res.ok:
        r = lr.run('docker ps')  # pylint: disable=invalid-name
        LOGGER.info(r.stdout.encode('utf-8'))
        return True
    else:
        raise Exception('dockers start failed. {}'.format(res))


def is_docker_available():
    LOGGER.info("Checking that docker is available...")
    result = LocalCmdRunner().run('docker ps', ignore_status=True, verbose=False)
    if result.ok:
        LOGGER.info('Docker is available')
        return True
    else:
        LOGGER.warning('Docker is not available on your computer. Please install docker software before continue')
        return False
