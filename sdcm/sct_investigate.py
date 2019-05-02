import os.path
import tempfile
import re
import logging

from utils import S3Storage, retrying
from remote import LocalCmdRunner
from textwrap import dedent

logger = logging.getLogger('sct.investigate')


def list_logs_by_test_id(test_id):
    log_types = ['db-cluster', 'monitor-set', 'prometheus', 'grafana', 'job', 'annotations', 'monitoring_data_stack']
    results = []

    if not test_id:
        return results

    log_files = S3Storage().search_by_path(path=test_id)
    for log_file in log_files:
        for log_type in log_types:
            if log_type in log_file:
                results.append({"file_path": log_file,
                                "type": log_type,
                                "link": "https://{}.s3.amazonaws.com/{}".format(S3Storage.bucket_name, log_file)})
                break

    return results


def restore_monitoring_stack(test_id):
    lr = LocalCmdRunner()
    logger.info("Checking that docker is available...")
    result = lr.run('docker ps', ignore_status=True, verbose=False)
    if result.ok:
        logger.info('Docker is available')
    else:
        logger.warning('Docker is not available on your computer. Please install docker software before continue')
        return False

    monitor_stack_base_dir = tempfile.mkdtemp()
    stored_files_by_test_id = list_logs_by_test_id(test_id)
    monitor_stack_archives = []
    for f in stored_files_by_test_id:
        if f['type'] in ['monitoring_data_stack', 'prometheus']:
            monitor_stack_archives.append(f)
    if not monitor_stack_archives or len(monitor_stack_archives) < 2:
        logger.warning('There is no available archive files for monitoring data stack restoring for test id : {}'.format(test_id))
        return False

    for arch in monitor_stack_archives:
        logger.info('Download file {} to directory {}'.format(arch['link'], monitor_stack_base_dir))
        local_path_monitor_stack = S3Storage().download_file(arch['link'], dst_dir=monitor_stack_base_dir)
        monitor_stack_workdir = os.path.dirname(local_path_monitor_stack)
        monitoring_stack_archive_file = os.path.basename(local_path_monitor_stack)
        logger.info('Extracting data from archive {}'.format(arch['file_path']))
        if arch['type'] == 'prometheus':
            monitoring_stack_data_dir = os.path.join(monitor_stack_workdir, 'monitor_data_dir')
            cmd = dedent("""
                mkdir -p {data_dir}
                cd {data_dir}
                cp ../{archive} ./
                tar -xvf {archive}
                chmod -R 777 {data_dir}
                """.format(data_dir=monitoring_stack_data_dir,
                           archive=monitoring_stack_archive_file))
            result = lr.run(cmd, ignore_status=True)
        else:
            branches = re.search('(?P<monitoring_branch>branch-[0-9]+\.[0-9]+?)_(?P<scylla_version>[\d]\.[\d]+?)',
                                 monitoring_stack_archive_file)
            monitoring_branch = branches.group('monitoring_branch')
            scylla_version = branches.group('scylla_version')
            cmd = dedent("""
                cd {workdir}
                tar -xvf {archive}
                """.format(workdir=monitor_stack_workdir, archive=os.path.basename(local_path_monitor_stack)))
            result = lr.run(cmd, ignore_status=True)
        if not result.ok:
                logger.warning("During restoring file {} next errors occured:\n {}".format(arch['link'], result))
                return False
        logger.info("Extracting data finished")

    logger.info('Monitoring stack files available {}'.format(monitor_stack_workdir))

    monitoring_dockers_dir = os.path.join(monitor_stack_workdir, 'scylla-grafana-monitoring-{}'.format(monitoring_branch))

    @retrying(n=3, sleep_time=1, message='Start docker containers')
    def start_dockers(monitoring_dockers_dir, monitoring_stack_data_dir, scylla_version):
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

    status = False
    status = start_dockers(monitoring_dockers_dir, monitoring_stack_data_dir, scylla_version)
    return status
