import time

import pytest

from sdcm.ndbench_thread import NdBenchStressThread
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


def test_01_cql_api(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = (
        "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=1000 ; "
        "numReaders=2; numWriters=2 ; cass.writeConsistencyLevel=QUORUM ; "
        "cass.readConsistencyLevel=QUORUM ; readRateLimit=500 ; writeRateLimit=500"
    )
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
    cmd = (
        "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=1000 ; "
        "numReaders=2; numWriters=2 ; cass.writeConsistencyLevel=QUORUM ; "
        "cass.readConsistencyLevel=QUORUM ; readRateLimit=500 ; writeRateLimit=500"
    )
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=500, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)
    ndbench_thread.run()
    time.sleep(3)
    ndbench_thread.kill()
    ndbench_thread.get_results()


def test_04_verify_data(request, docker_scylla, events, params):
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = (
        "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=30 ; "
        "readEnabled=false; numReaders=0; numWriters=1 ; cass.writeConsistencyLevel=QUORUM ; "
        "cass.readConsistencyLevel=QUORUM ; generateChecksum=false"
    )
    ndbench_thread = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread():
        ndbench_thread.kill()

    request.addfinalizer(cleanup_thread)

    ndbench_thread.run()
    ndbench_thread.get_results()

    cmd = (
        "ndbench cli.clientName=CassJavaDriverGeneric ; numKeys=30 ; "
        "writeEnabled=false; numReaders=1; numWriters=0 ; cass.writeConsistencyLevel=QUORUM ; "
        "cass.readConsistencyLevel=QUORUM ; validateChecksum=true ;"
    )
    ndbench_thread2 = NdBenchStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread2():
        ndbench_thread2.kill()

    request.addfinalizer(cleanup_thread2)

    file_logger = events.get_events_logger()
    with events.wait_for_n_events(file_logger, count=3, timeout=60):
        ndbench_thread2.run()

    cat = file_logger.get_events_by_category()

    assert any("Failed to process NdBench read operation" in err for err in cat["ERROR"])
