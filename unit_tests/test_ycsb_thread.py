import re

import pytest
import requests

from sdcm.ycsb_thread import YcsbStressThread
from sdcm.utils.alternator import create_table
from sdcm.utils.common import timeout
from sdcm.utils.docker import running_in_docker

from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = pytest.mark.usefixtures('events')


def test_01_dynamodb_api(docker_scylla, prom_address):
    loader_set = LocalLoaderSetDummy()
    alternator_port = 8000

    if running_in_docker():
        create_table(f'http://{docker_scylla.internal_ip_address}:{alternator_port}', 'HASH_AND_RANGE')
    else:
        address = docker_scylla.get_port(f'{alternator_port}')
        create_table(f'http://{address}', 'HASH_AND_RANGE')

    cmd = 'bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=1000000 -p fieldcount=10 -p fieldlength=1024 -p operationcount=200200300 -s'
    ycsb_thread = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=10, params=dict(
        alternator_port=f'{alternator_port}', dynamodb_primarykey_type='HASH_AND_RANGE'))
    ycsb_thread.run()

    @timeout(timeout=30)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r'^collectd_ycsb_read_gauge.*?([0-9\.]*?)$', re.MULTILINE)
        assert 'collectd_ycsb_read_gauge' in output
        assert 'collectd_ycsb_update_gauge' in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()
    ycsb_thread.get_results()
