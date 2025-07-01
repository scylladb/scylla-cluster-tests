# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2021 ScyllaDB

import re

import pytest
import requests
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module

from sdcm.utils import alternator
from sdcm.utils.decorators import timeout
from sdcm.utils.docker_utils import running_in_docker
from sdcm.ycsb_thread import YcsbStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy
from unit_tests.lib.alternator_utils import ALTERNATOR_PORT, ALTERNATOR, TEST_PARAMS

pytestmark = [
    pytest.mark.usefixtures("events", "create_table", "create_cql_ks_and_table"),
    pytest.mark.skip(reason="those are integration tests only"),
]


@pytest.fixture(scope="session")
def create_table(docker_scylla):
    def create_endpoint_url(node):
        if running_in_docker():
            endpoint_url = f"http://{node.internal_ip_address}:{ALTERNATOR_PORT}"
        else:
            address = node.get_port(f"{ALTERNATOR_PORT}")
            endpoint_url = f"http://{address}"
        return endpoint_url

    setattr(docker_scylla, "name", "mock-node")

    ALTERNATOR.create_endpoint_url = create_endpoint_url
    ALTERNATOR.create_table(node=docker_scylla, table_name=alternator.consts.TABLE_NAME)


@pytest.fixture(scope="session")
def create_cql_ks_and_table(docker_scylla):
    if running_in_docker():
        address = f"{docker_scylla.internal_ip_address}:9042"
    else:
        address = docker_scylla.get_port("9042")
    node_ip, port = address.split(":")
    port = int(port)

    cluster_driver = Cluster([node_ip], port=port)
    session = cluster_driver.connect()
    session.execute(
        """create keyspace ycsb WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1 };"""
    )
    session.execute(
        """CREATE TABLE ycsb.usertable (
                        y_id varchar primary key,
                        field0 varchar,
                        field1 varchar,
                        field2 varchar,
                        field3 varchar,
                        field4 varchar,
                        field5 varchar,
                        field6 varchar,
                        field7 varchar,
                        field8 varchar,
                        field9 varchar);"""
    )


@pytest.mark.docker_scylla_args(docker_network="ycsb_net")
def test_01_dynamodb_api(request, docker_scylla, prom_address, params):
    loader_set = LocalLoaderSetDummy()
    params.update(TEST_PARAMS)

    cmd = (
        "bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=1000000 "
        "-p fieldcount=10 -p fieldlength=1024 -p operationcount=200200300 -s"
    )
    ycsb_thread = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        ycsb_thread.kill()

    request.addfinalizer(cleanup_thread)

    ycsb_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r"^sct_ycsb_read_gauge.*?([0-9\.]*?)$", re.MULTILINE)
        assert "sct_ycsb_read_gauge" in output
        assert "sct_ycsb_update_gauge" in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()

    output = ycsb_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_02_dynamodb_api_dataintegrity(request, docker_scylla, prom_address, events, params):
    loader_set = LocalLoaderSetDummy()
    params.update(TEST_PARAMS)

    # 2. do write without dataintegrity=true
    cmd = (
        "bin/ycsb load dynamodb -P workloads/workloada -threads 5 -p recordcount=50 -p fieldcount=1 -p fieldlength=100"
    )
    ycsb_thread1 = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread1():
        ycsb_thread1.kill()

    request.addfinalizer(cleanup_thread1)

    ycsb_thread1.run()
    ycsb_thread1.get_results()
    ycsb_thread1.kill()

    # 3. do read with dataintegrity=true
    cmd = (
        "bin/ycsb run dynamodb -P workloads/workloada -threads 5 -p recordcount=100 "
        "-p fieldcount=10 -p fieldlength=512 -p dataintegrity=true -p operationcount=30000"
    )
    ycsb_thread2 = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=30, params=params)

    def cleanup_thread2():
        ycsb_thread2.kill()

    request.addfinalizer(cleanup_thread2)

    # 4. wait for expected metrics to be available
    @timeout(timeout=120)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r"^sct_ycsb_verify_gauge.*?([0-9\.]*?)$", re.MULTILINE)

        assert "sct_ycsb_verify_gauge" in output
        assert 'type="UNEXPECTED_STATE"' in output
        assert 'type="ERROR"' in output
        matches = regex.findall(output)
        assert all(float(i) >= 0 for i in matches), output

    file_logger = events.get_events_logger()
    with events.wait_for_n_events(file_logger, count=3, timeout=60):
        ycsb_thread2.run()
        check_metrics()
        ycsb_thread2.get_results()

    # 5. check that events with the expected error were raised
    cat = file_logger.get_events_by_category()
    assert len(cat["ERROR"]) == 2
    assert "=UNEXPECTED_STATE" in cat["ERROR"][0]
    assert "=ERROR" in cat["ERROR"][1]


def test_03_cql(request, docker_scylla, prom_address, params):
    loader_set = LocalLoaderSetDummy()
    params.update(TEST_PARAMS)

    cmd = (
        "bin/ycsb load scylla -P workloads/workloada -threads 5 -p recordcount=1000000 "
        f"-p fieldcount=10 -p fieldlength=1024 -p operationcount=200200300 -p scylla.hosts={docker_scylla.ip_address} -s"
    )
    ycsb_thread = YcsbStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        ycsb_thread.kill()

    request.addfinalizer(cleanup_thread)

    ycsb_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r"^sct_ycsb_read_gauge.*?([0-9\.]*?)$", re.MULTILINE)
        assert "sct_ycsb_read_gauge" in output
        assert "sct_ycsb_update_gauge" in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()
    ycsb_thread.get_results()


def test_04_insert_new_data(docker_scylla):
    schema = alternator.schemas.HASH_AND_STR_RANGE_SCHEMA
    schema_keys = [key_details["AttributeName"] for key_details in schema["KeySchema"]]
    new_items = [
        {schema_keys[0]: "test_0", schema_keys[1]: "NFinQpNuCnaNOxsAkyrZ"},
        {schema_keys[0]: "test_1", schema_keys[1]: "hScfTVnCctqqTQcLrIQd"},
        {schema_keys[0]: "test_2", schema_keys[1]: "OpvrbHJNNMHptWYQSWvm"},
        {schema_keys[0]: "test_3", schema_keys[1]: "nzxHPebRwNaxLlXUbbCW"},
        {schema_keys[0]: "test_4", schema_keys[1]: "WfHQIwRNHflFHYWwOcFA"},
        {schema_keys[0]: "test_5", schema_keys[1]: "ipcTlIvLbcbrOFDynEBU"},
        {schema_keys[0]: "test_6", schema_keys[1]: "judYKbqgDAejlpPdqLdx"},
        {schema_keys[0]: "test_7", schema_keys[1]: "mMYdekljccLeOMWLBTLL"},
        {schema_keys[0]: "test_8", schema_keys[1]: "NqsNVTtJeWRzrjHmOwop"},
        {schema_keys[0]: "test_9", schema_keys[1]: "YrRvsqXAtppgCLiHhiQn"},
    ]

    ALTERNATOR.batch_write_actions(
        node=docker_scylla,
        new_items=new_items,
        schema=alternator.schemas.HASH_AND_STR_RANGE_SCHEMA,
    )
    diff = ALTERNATOR.compare_table_data(node=docker_scylla, table_data=new_items)
    assert diff
