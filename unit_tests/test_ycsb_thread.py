import re

import pytest
import requests

from sdcm.ycsb_thread import YcsbStressThread
from sdcm.utils.alternator import create_table as alternator_create_table
from sdcm.utils.common import timeout
from sdcm.utils.docker import running_in_docker

from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = pytest.mark.usefixtures('events', 'create_table')

ALTERNATOR_PORT = 8000
TEST_PARAMS = dict(dynamodb_primarykey_type='HASH_AND_RANGE',
                   alternator_use_dns_routing=True, alternator_port=ALTERNATOR_PORT)


@pytest.fixture(scope='session')
def create_table(docker_scylla):
    if running_in_docker():
        alternator_create_table(f'http://{docker_scylla.internal_ip_address}:{ALTERNATOR_PORT}', TEST_PARAMS)
    else:
        address = docker_scylla.get_port(f'{ALTERNATOR_PORT}')
        alternator_create_table(f'http://{address}', TEST_PARAMS)


def test_01_dynamodb_api(request, docker_scylla, prom_address):
    loader_set = LocalLoaderSetDummy()

    cmd = 'bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=1000000 -p fieldcount=10 -p fieldlength=1024 -p operationcount=200200300 -s'
    ycsb_thread = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=TEST_PARAMS)

    def cleanup_thread():
        ycsb_thread.kill()
    request.addfinalizer(cleanup_thread)

    ycsb_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r'^collectd_ycsb_read_gauge.*?([0-9\.]*?)$', re.MULTILINE)
        assert 'collectd_ycsb_read_gauge' in output
        assert 'collectd_ycsb_update_gauge' in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()
    ycsb_thread.get_results()


def test_02_dynamodb_api_dataintegrity(request, docker_scylla, prom_address, events):
    loader_set = LocalLoaderSetDummy()

    critical_log_content_before = events.get_event_log_file('critical.log')

    # 2. do write without dataintegrity=true
    cmd = 'bin/ycsb load dynamodb -P workloads/workloada -threads 5 -p recordcount=10000 -p fieldcount=10 -p fieldlength=512'
    ycsb_thread1 = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=TEST_PARAMS)

    def cleanup_thread1():
        ycsb_thread1.kill()
    request.addfinalizer(cleanup_thread1)

    ycsb_thread1.run()
    ycsb_thread1.get_results()
    ycsb_thread1.kill()

    # 3. do read with dataintegrity=true
    cmd = 'bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=10000 -p fieldcount=10 -p fieldlength=512 -p dataintegrity=true -p hdrhistogram.summary.addstatus=true -p operationcount=100000000'
    ycsb_thread2 = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=20, params=TEST_PARAMS)

    def cleanup_thread2():
        ycsb_thread2.kill()
    request.addfinalizer(cleanup_thread2)

    ycsb_thread2.run()

    # 4. wait for expected metrics to be available
    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r'^collectd_ycsb_verify_gauge.*?([0-9\.]*?)$', re.MULTILINE)

        assert 'collectd_ycsb_verify_gauge' in output
        assert 'UNEXPECTED_STATE' in output
        matches = regex.findall(output)
        assert all(float(i) >= 0 for i in matches), output

    check_metrics()
    ycsb_thread2.get_results()

    # 5. check that events with the expected error were raised
    critical_log_content_after = events.wait_for_event_log_change('critical.log', critical_log_content_before)
    assert 'UNEXPECTED_STATE' in critical_log_content_after
