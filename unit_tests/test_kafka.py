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
# Copyright (c) 2023 ScyllaDB
import os
import logging

import pytest

from sdcm.stress_thread import CassandraStressThread
from sdcm.kafka.kafka_cluster import LocalKafkaCluster
from sdcm.kafka.kafka_consumer import KafkaCDCReaderThread
from sdcm.utils.issues import SkipPerIssues
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.integration,
]

LOGGER = logging.getLogger(__name__)


@pytest.fixture(name="kafka_cluster", scope="function")
def fixture_kafka_cluster(tmp_path_factory, params):
    os.environ["_SCT_TEST_LOGDIR"] = str(tmp_path_factory.mktemp("logs"))
    kafka = LocalKafkaCluster(params=params)

    kafka.start()

    yield kafka

    kafka.stop()


@pytest.mark.docker_scylla_args(docker_network="kafka-stack-docker-compose_default",
                                image="scylladb/scylla-nightly:latest")
@pytest.mark.sct_config(
    files="unit_tests/test_data/kafka_connectors/scylla-cdc-source-connector.yaml"
)
@pytest.mark.skip("https://github.com/scylladb/scylla-cluster-tests/issues/11628")
def test_01_kafka_cdc_source_connector(request, docker_scylla, kafka_cluster, params, events):
    """
    setup kafka with scylla-cdc-source-connector with docker based scylla node
    - from confluent-hub
    - from GitHub release url
    """

    params['kafka_backend'] = 'localstack'

    disable_tablets = ""
    if SkipPerIssues("scylladb/scylladb#16317", params):
        disable_tablets = "AND tablets = {'enabled': false}"
    docker_scylla.run_cqlsh(
        "CREATE KEYSPACE IF NOT EXISTS keyspace1 WITH REPLICATION = "
        f"{{'class' : 'NetworkTopologyStrategy', 'replication_factor': 1 }} {disable_tablets};"
    )
    docker_scylla.run_cqlsh(
        'CREATE TABLE IF NOT EXISTS keyspace1.standard1 (key blob PRIMARY KEY, '
        '"C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob) WITH  '
        "cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 600}"
    )
    docker_scylla.parent_cluster.nodes = [docker_scylla]
    connector_config = params.get("kafka_connectors")[0]
    kafka_cluster.create_connector(
        db_cluster=docker_scylla.parent_cluster, connector_config=connector_config
    )

    loader_set = LocalLoaderSetDummy(params=params)

    cmd = (
        """cassandra-stress write cl=ONE n=500 -mode cql3 native -rate threads=10 """
    )

    cs_thread = CassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )
    request.addfinalizer(lambda: cs_thread.kill())

    cs_thread.run()
    LOGGER.info(cs_thread.get_results())

    reader_thread = KafkaCDCReaderThread(tester=None, params=params, connector_index=0, read_number_of_key=500)
    request.addfinalizer(lambda: reader_thread.stop())

    reader_thread.start()

    reader_thread.join()


@pytest.mark.docker_scylla_args(docker_network="kafka-stack-docker-compose_default")
@pytest.mark.sct_config(
    files="unit_tests/test_data/kafka_connectors/kafka-connect-scylladb.yaml"
)
def test_02_kafka_scylla_sink_connector(docker_scylla, kafka_cluster, params):
    """
    setup kafka with kafka-connect-scylladb with docker based scylla node
    - from confluent-hub
    - from GitHub release url
    """

    # docker_scylla.run_cqlsh(
    #    "CREATE KEYSPACE IF NOT EXISTS keyspace1 WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1 };")
    # docker_scylla.run_cqlsh(
    #    "CREATE TABLE IF NOT EXISTS keyspace1.default1 (key blob PRIMARY KEY, C0 blob, C1 blob, C2 blob, C3 blob, C4 blob) WITH  "
    #    "cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 600}")

    docker_scylla.parent_cluster.nodes = [docker_scylla]
    for connector_config in params.get("kafka_connectors"):
        kafka_cluster.create_connector(
            db_cluster=docker_scylla.parent_cluster, connector_config=connector_config
        )
