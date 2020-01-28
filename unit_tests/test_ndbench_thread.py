import time

import pytest

from sdcm.ndbench_thread import NdBenchStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = pytest.mark.usefixtures('events')


def test_01_cql_api(docker_scylla):
    loader_set = LocalLoaderSetDummy()
    cmd = 'ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=20000000 ; numReaders=8; numWriters=8 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; readRateLimit=7200 ; writeRateLimit=1800'
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=20)
    ndbench_thread.run()
    ndbench_thread.get_results()


def test_02_cql_kill(docker_scylla):
    """
    verifies that kill command on the NdBenchStressThread is working
    """
    loader_set = LocalLoaderSetDummy()
    cmd = 'ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=20000000 ; numReaders=8; numWriters=8 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; readRateLimit=7200 ; writeRateLimit=1800'
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=500)
    ndbench_thread.run()
    time.sleep(3)
    ndbench_thread.kill()
    ndbench_thread.get_results()


def test_03_dynamodb_api(docker_scylla, events):
    """
    this test isn't working yet, since we didn't figured out a way to use ndbench with dynamodb
    """
    critical_log_content_before = events.get_event_log_file('critical.log')

    # start a command that would yield errors
    loader_set = LocalLoaderSetDummy()
    cmd = f'ndbench cli.clientName=DynamoDBKeyValue ; numKeys=20000000 ; numReaders=8; numWriters=8 ; readRateLimit=7200 ; writeRateLimit=1800; dynamodb.autoscaling=false; dynamodb.endpoint=http://{docker_scylla.internal_ip_address}:8000'
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=500)
    ndbench_thread.run()
    ndbench_thread.get_results()

    # check that events with the errors were sent out
    critical_log_content_after = events.wait_for_event_log_change('critical.log', critical_log_content_before)

    assert 'Encountered an exception when driving load' in critical_log_content_after
    assert 'BUILD FAILED' in critical_log_content_after
