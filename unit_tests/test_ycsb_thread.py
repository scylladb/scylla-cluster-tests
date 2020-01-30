import os

import pytest
import requests

from sdcm.ycsb_thread import YcsbStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy
from sdcm.utils.alternator import create_table
from sdcm.utils.common import timeout

pytestmark = pytest.mark.usefixtures('events')

# TODO: move this into some utils.common


def running_in_docker():
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )


def test_01_dynamodb_api(docker_scylla, prom_address):
    loader_set = LocalLoaderSetDummy()
    if running_in_docker():
        create_table(f'http://{docker_scylla.internal_ip_address}:8000', 'HASH_AND_RANGE')
    else:
        address = docker_scylla.get_port('8000')
        create_table(f'http://{address}', 'HASH_AND_RANGE')

    cmd = 'bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=100 -p fieldcount=10 -p fieldlength=1024 -s'
    ycsb_thread = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=60, params=dict(
        alternator_port='8000', dynamodb_primarykey_type='HASH_AND_RANGE'))
    ycsb_thread.run()

    @timeout(timeout=30)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        assert 'collectd_ycsb_read_gauge' in output
        assert 'collectd_ycsb_update_gauge' in output

    check_metrics()
    ycsb_thread.get_results()
