import time

import pytest

from sdcm.ndbench_thread import NdBenchStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


def test_01_cql_api(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=20000000 ; numReaders=8; numWriters=8 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; readRateLimit=7200 ; writeRateLimit=1800"
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)
    ndbench_thread.run()
    ndbench_thread.get_results()


def test_02_cql_kill(request, docker_scylla, params):
    """
    verifies that kill command on the NdBenchStressThread is working
    """
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=20000000 ; numReaders=8; numWriters=8 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; readRateLimit=7200 ; writeRateLimit=1800"
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=500, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)
    ndbench_thread.run()
    time.sleep(3)
    ndbench_thread.kill()
    ndbench_thread.get_results()


def test_03_dynamodb_api(request, docker_scylla, events, params):
    """
    this test isn't working yet, since we didn't figure out a way to use ndbench with dynamodb
    """

    # start a command that would yield errors
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = f"ndbench cli.clientName=DynamoDBKeyValue ; numKeys=20000000 ; numReaders=8; numWriters=8 ; readRateLimit=7200 ; writeRateLimit=1800; dynamodb.autoscaling=false; dynamodb.endpoint=http://{docker_scylla.ip_address}:8000"
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=20, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)

    file_logger = events.get_events_logger()
    with events.wait_for_n_events(file_logger, count=4, timeout=60):
        ndbench_thread.run()
        ndbench_thread.get_results()

    # check that events with the errors were sent out
    cat = file_logger.get_events_by_category()
    assert len(cat["ERROR"]) >= 1
    assert any("Encountered an exception when driving load" in err for err in cat["ERROR"])

    assert len(cat["CRITICAL"]) >= 2
    assert any("BUILD FAILED" in critical for critical in cat["CRITICAL"])


def test_04_verify_data(request, docker_scylla, events, params):
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=30 ; readEnabled=false; numReaders=0; numWriters=1 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; generateChecksum=false"
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)

    ndbench_thread.run()
    ndbench_thread.get_results()

    cmd = "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=30 ; writeEnabled=false; numReaders=1; numWriters=0 ; cass.writeConsistencyLevel=QUORUM ; cass.readConsistencyLevel=QUORUM ; validateChecksum=true ;"
    ndbench_thread2 = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread2():
        ndbench_thread2.kill()

    request.addfinalizer(cleanup_thread2)

    file_logger = events.get_events_logger()
    with events.wait_for_n_events(file_logger, count=3, timeout=60):
        ndbench_thread2.run()

    cat = file_logger.get_events_by_category()

    assert any("Failed to process NdBench read operation" in err for err in cat["ERROR"])
