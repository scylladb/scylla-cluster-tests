import logging
import os
import zipfile
import tempfile
import json
import requests

from textwrap import dedent

from sdcm.remote import LocalCmdRunner
from .common import list_logs_by_test_id, S3Storage, retrying, remove_files

logger = logging.getLogger(name='monitorstack')


def restore_monitor_stack(test_id):
    if not is_docker_available():
        return False

    monitor_stack_archives = get_monitor_set_archives(test_id)

    for arch in monitor_stack_archives:
        logger.info('Restoring monitoring stack from archive %s', arch['file_path'])
        monitor_stack_base_dir = tempfile.mkdtemp()
        logger.info('Download file {} to directory {}'.format(arch['link'], monitor_stack_base_dir))
        downloaded_monitor_archive = S3Storage().download_file(arch['link'],
                                                               dst_dir=monitor_stack_base_dir)
        monitor_data_arch = extract_monitor_data_archive(downloaded_monitor_archive,
                                                         monitor_stack_base_dir)
        monitor_stack_arch = extract_monitor_stack_archive(downloaded_monitor_archive,
                                                           monitor_stack_base_dir)

        if not monitor_data_arch:
            logger.error("No prometheus snapshot were found. Switch to next archive")
            continue
        if not monitor_stack_arch:
            logger.error("No monitor stack archive were found. Switch to next archive")
            continue

        monitor_data_base_dir = os.path.join(monitor_stack_base_dir, 'monitor_data_dir')
        cmd = dedent("""
            mkdir -p {data_dir}
            cd {data_dir}
            cp {archive} ./
            tar -xvf {archive_name}
            chmod -R 777 {data_dir}
            """.format(data_dir=monitor_data_base_dir,
                       archive=monitor_data_arch,
                       archive_name=os.path.basename(monitor_data_arch)))
        result = LocalCmdRunner().run(cmd, ignore_status=True)
        if result.exited > 0:
            logger.error("Error during extracting prometheus snapshot. Swithc to next archive")
            continue

        monitor_data_dir = get_monitor_data_dir(monitor_data_base_dir)

        cmd = dedent("""
            cd {data_dir}
            cp {archive} ./
            tar -xvf {archive_name}
            chmod -R 777 {data_dir}
            """.format(data_dir=monitor_stack_base_dir,
                       archive_name=os.path.basename(monitor_stack_arch),
                       archive=monitor_stack_arch))

        result = LocalCmdRunner().run(cmd, ignore_status=True)
        if result.exited > 0:
            logger.error("Error during extracting monitor stack")
            continue

        monitor_stack_dir = get_monitor_stack_dir(monitor_stack_base_dir)
        monitor_version, scylla_version = get_monitor_stack_scylla_version(monitor_stack_dir)

        status = start_dockers(monitor_stack_dir,
                               monitor_data_dir,
                               scylla_version)
        if status:
            upload_sct_dashboards(monitor_stack_dir, scylla_version)
            upload_annotations(monitor_stack_dir)
            return status
        else:
            logger.error('Error during dockers starting. Trying next arhive')
            remove_files(monitor_stack_base_dir)
            continue
    else:
        return False


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
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():
            if pattern in name:
                z.extract(name, extract_dir)
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


def get_monitor_stack_scylla_version(monitor_stack_dir):
    try:
        versions = open(os.path.join(monitor_stack_dir, 'monitor_version'), 'r').read().strip()
        monitor_version, scylla_version = versions.split(':')
        return monitor_version, scylla_version
    except Exception:
        return 'branch-3.0', 'master'


def upload_sct_dashboards(monitoring_dockers_dir, scylla_version):
    sct_dashboard_file_name = "scylla-dash-per-server-nemesis.{}.json".format(scylla_version)
    sct_dashboard_file = os.path.join(monitoring_dockers_dir,
                                      'sct_monitoring_addons',
                                      sct_dashboard_file_name)

    if not os.path.exists(sct_dashboard_file):
        logger.info('There is no dashboard {}. Skip load dashboard'.format(sct_dashboard_file_name))
        return False

    dashboard_url = 'http://localhost:3000/api/dashboards/db'
    with open(sct_dashboard_file, "r") as f:
        dashboard_config = json.load(f)

    res = requests.post(dashboard_url,
                        data=json.dumps(dashboard_config),
                        headers={'Content-Type': 'application/json'})

    if res.status_code != 200:
        logger.info('Error uploading dashboard {}. Error message {}'.format(sct_dashboard_file, res.text))
        return False
    logger.info('Dashboard {} loaded successfully'.format(sct_dashboard_file))


def upload_annotations(monitor_stack_dir):
    annotations_file = os.path.join(monitor_stack_dir,
                                    'sct_monitoring_addons',
                                    'annotations.json')

    if not os.path.exists(annotations_file):
        logger.info('There is no annotations file.Skip loading annotations')
        return False

    with open(annotations_file, "r") as f:
        annotations = json.load(f)

    annotations_url = "http://localhost:3000/api/annotations"
    for an in annotations:
        res = requests.post(annotations_url, data=json.dumps(an), headers={'Content-Type': 'application/json'})
        if res.status_code != 200:
            logger.info('Error during uploading annotation {}. Error message {}'.format(an, res.text))
            return False
    logger.info('Annotations loaded successfully')


@retrying(n=3, sleep_time=1, message='Start docker containers')
def start_dockers(monitoring_dockers_dir, monitoring_stack_data_dir, scylla_version):
    lr = LocalCmdRunner()
    lr.run('cd {}; ./kill-all.sh'.format(monitoring_dockers_dir))
    cmd = dedent("""cd {monitoring_dockers_dir};
            ./start-all.sh \
            -s {monitoring_dockers_dir}/config/scylla_servers.yml \
            -n {monitoring_dockers_dir}/config/node_exporter_servers.yml \
            -d {monitoring_stack_data_dir} -v {scylla_version}""".format(**locals()))
    res = lr.run(cmd, ignore_status=True)
    if res.ok:
        r = lr.run('docker ps')
        logger.info(r.stdout.encode('utf-8'))
        return True
    else:
        raise Exception('dockers start failed. {}'.format(res))


def is_docker_available():
    logger.info("Checking that docker is available...")
    result = LocalCmdRunner().run('docker ps', ignore_status=True, verbose=False)
    if result.ok:
        logger.info('Docker is available')
        return True
    else:
        logger.warning('Docker is not available on your computer. Please install docker software before continue')
        return False
