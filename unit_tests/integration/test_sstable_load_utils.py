import pytest

from sdcm.utils.sstable.load_utils import SstableLoadUtils

from sdcm.stress_thread import CassandraStressThread
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
    pytest.mark.xdist_group("docker_heavy"),
]


@pytest.mark.integration
def test_sstable_load_utils_usage(docker_scylla, params, request):
    """test the sstable load utils base on the refrash monkey flow localy with a docker based scylla"""

    loader_set = LocalLoaderSetDummy(params=params)

    ks = "keyspace_refresh"
    stress_cmd = (
        "cassandra-stress write n=40000 cl=ONE -mode native cql3 "
        f"-schema 'keyspace={ks} replication(strategy=NetworkTopologyStrategy,"
        f"replication_factor=1)' -log interval=5"
    )
    cs_thread = CassandraStressThread(loader_set, stress_cmd, node_list=[docker_scylla], timeout=120, params=params)

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()

    column_num = SstableLoadUtils.calculate_columns_count_in_table(
        docker_scylla, keyspace_name="keyspace_refresh", table_name="standard1"
    )

    assert column_num
    test_data = SstableLoadUtils.get_load_test_data_inventory(column_num, big_sstable=False, load_and_stream=False)

    result = docker_scylla.run_nodetool(sub_cmd="cfstats", args="keyspace_refresh.standard1")
    assert result is not None and result.exit_status == 0, "cfstats failed, table keyspace_refresh.standard1 not ready"

    key = "0x32373131364f334f3830"
    query_verify = f"SELECT * FROM keyspace_refresh.standard1 WHERE key={key}"
    result = docker_scylla.run_cqlsh(query_verify)
    assert "(0 rows)" in result.stdout

    # Executing refresh
    SstableLoadUtils.upload_sstables(
        docker_scylla,
        test_data=test_data[0],
        table_name="standard1",
        is_cloud_cluster=False,
    )
    docker_scylla.run_nodetool(sub_cmd="refresh", args="-- keyspace_refresh standard1")

    # Verify that the special key is loaded by SELECT query
    result = docker_scylla.run_cqlsh(query_verify)
    assert "(1 rows)" in result.stdout, f"The key {key} is not loaded by `nodetool refresh`"
