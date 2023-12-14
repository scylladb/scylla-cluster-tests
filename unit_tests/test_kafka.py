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
import pytest

from sdcm.kafka.kafka_cluster import LocalKafkaCluster

pytestmark = [
    pytest.mark.integration,
]


@pytest.fixture(name="kafka_cluster", scope="session")
def fixture_kafka_cluster(tmp_path_factory):
    os.environ["_SCT_TEST_LOGDIR"] = str(tmp_path_factory.mktemp("logs"))
    kafka = LocalKafkaCluster()

    kafka.start()

    yield kafka

    kafka.stop()


@pytest.mark.docker_scylla_args(docker_network="kafka-stack-docker-compose_default")
@pytest.mark.sct_config(
    files="unit_tests/test_data/kafka_connectors/scylla-cdc-source-connector.yaml"
)
def test_01_kafka_cdc_source_connector(docker_scylla, kafka_cluster, params):
    """
    setup kafka with scylla-cdc-source-connector with docker based scylla node
    - from confluent-hub
    - from GitHub release url
    """
    docker_scylla.run_cqlsh(
        "CREATE KEYSPACE IF NOT EXISTS keyspace1 WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1 };"
    )
    docker_scylla.run_cqlsh(
        'CREATE TABLE IF NOT EXISTS keyspace1.standard1 (key blob PRIMARY KEY, '
        '"C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob) WITH  '
        "cdc = {'enabled': true, 'preimage': false, 'postimage': true, 'ttl': 600}"
    )

    docker_scylla.parent_cluster.nodes = [docker_scylla]
    for connector_config in params.get("kafka_connectors"):
        kafka_cluster.create_connector(
            db_cluster=docker_scylla.parent_cluster, connector_config=connector_config
        )


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
