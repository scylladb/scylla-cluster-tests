# Kafka / CDC connectors

[← All configuration options](configuration_options.md)

Kafka deployment and connector configuration for CDC testing.

**2 options.**


## **kafka_backend** / SCT_KAFKA_BACKEND

Type of Kafka backend to use

**default:** N/A

**type:** Literal['localstack', 'vm', 'msk']


## **kafka_connectors** / SCT_KAFKA_CONNECTORS

Kafka Connect connectors to deploy, as a list of connector definitions.<br><br>Each entry has a `source` (where to fetch the connector from -- a Confluent Hub<br>coordinate or a release URL), a unique `name`, and a `config` block whose keys are the<br>connector's own dotted options, passed through as-is.<br><br>Example -- the Scylla CDC source connector:<br><br>[`kafka_connectors`](#kafka_connectors):<br>- source: 'hub:scylladb/scylla-cdc-source-connector:1.1.2'<br>name: 'cdc-connector'<br>config:<br>connector.class: 'com.scylladb.cdc.debezium.connector.ScyllaConnector'<br>scylla.name: 'test-cluster'<br>scylla.table.names: 'keyspace1.table1'<br>scylla.user: 'cassandra'<br>scylla.password: 'cassandra'<br><br>See `docs/kafka.md` for how SCT deploys Kafka, and the connectors' own documentation<br>for the full option set:<br>https://github.com/scylladb/scylla-cdc-source-connector#configuration (source<br>connector) and<br>https://github.com/scylladb/kafka-connect-scylladb/blob/master/documentation/CONFIG.md<br>(sink connector). The accepted keys are modelled in<br>`sdcm.kafka.kafka_config.ConnectorConfiguration`.

**default:** []

**type:** list[sdcm.kafka.kafka_config.SctKafkaConfiguration]
