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
# Copyright (c) 2024 ScyllaDB

import json
import logging
import random
import time
from string import ascii_letters
from threading import Event, Thread

from confluent_kafka import Producer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer, StringDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.wait import wait_for


LOGGER = logging.getLogger(__name__)


def filter_kafka_options(**options: dict) -> dict:
    """
    Filters a dictionary of possible python thread options to keep only known Kafka options

    :param options: dict, a dictionary of all options of a python thread

    :return: dict, the known Kafka options.
    """
    known_options = {
        'broker-list',
        'bootstrap-server',
        'topic',
        'property',
        'key-schema',
        'value-schema',
        'schema-registry',
        'producer-property',
        'consumer-property',
        'compression-codec',
        'batch-size',
        'linger-ms',
        'acks',
        'key-separator',
        'parse.key',
    }

    filtered_options = {}
    for option, value in options.items():
        if option in known_options:
            # Handle nested options
            if option in ['property', 'producer-property', 'consumer-property']:
                if isinstance(value, dict):
                    filtered_options[option] = value
                elif isinstance(value, str) and '=' in value:
                    sub_option, sub_value = value.split('=', 1)
                    filtered_options.setdefault(option, {})[sub_option] = sub_value
                else:
                    LOGGER.error("Invalid format or type for %s: %s", option, value)
            else:
                filtered_options[option] = value
        else:
            LOGGER.warning("Unknown Kafka option %s encountered.", option)

    return filtered_options


class KafkaThreadBase(Thread):
    def __init__(self, tester, params: SCTConfiguration, timeout: int, **kwargs):
        super().__init__(daemon=True)
        self.tester = tester
        self.params = params
        self.timeout = timeout
        self.termination_event = Event()
        self.record_format = kwargs.get('record-format', 'json-with-schema')
        self.num_records = int(kwargs.get('num-records', 1000))

        self.kafka_options = filter_kafka_options(**kwargs)

        self.kafka_addresses = None
        if 'broker-list' in self.kafka_options:
            self.kafka_addresses = self.kafka_options['broker-list'].split(',')
        elif 'bootstrap-server' in self.kafka_options:
            self.kafka_addresses = self.kafka_options['bootstrap-server'].split(',')
        elif self.params.get('kafka_backend') == 'localstack':
            self.kafka_addresses = ['localhost:9092']

        self.schema_registry_url = None
        if 'schema-registry' in self.kafka_options:
            self.schema_registry_url = self.kafka_options['schema-registry']
        elif self.params.get('kafka_backend') == 'localstack':
            self.schema_registry_url = 'http://localhost:8081'

        self.topic = self.kafka_options.get('topic')
        self.key_schema = self.kafka_options.get('key-schema')
        self.value_schema = self.kafka_options.get('value-schema')

        if self.record_format == 'avro':
            if not self.schema_registry_url:
                raise ValueError("schema_registry_url is required for Avro record format.")
            self.schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})

            self.key_serializer = AvroSerializer(
                self.schema_registry_client, self.key_schema, lambda obj, ctx: obj,
            ) if self.key_schema else None
            self.value_serializer = AvroSerializer(
                self.schema_registry_client, self.value_schema, lambda obj, ctx: obj,
            ) if self.value_schema else None

            self.key_deserializer = AvroDeserializer(self.schema_registry_client)
            self.value_deserializer = AvroDeserializer(self.schema_registry_client)
        else:
            self.key_serializer = StringSerializer('utf_8')
            self.value_serializer = StringSerializer('utf_8')
            self.key_deserializer = StringDeserializer('utf_8')
            self.value_deserializer = StringDeserializer('utf_8')

    def generate_data_from_schema(self, schema: dict, seed: int) -> dict | int | float | str | bool | bytes | None:
        rand_gen = random.Random(seed)
        if schema['type'] in ('record', 'struct'):  # 'record' type of schema for AVRO record format, 'struct' for JSON
            data = {}
            for field in schema['fields']:
                # 'name' is used in AVRO type records, 'field' in JSON ones
                field_name = field.get('name') or field.get('field')
                if field_name == 'id':
                    data[field_name] = seed
                else:
                    data[field_name] = self.generate_field_value(field['type'], rand_gen)
            return data
        else:
            return self.generate_field_value(schema['type'], rand_gen)

    def generate_field_value(self, field_type: str, rand_gen: random.Random) -> int | float | str | bool | bytes | None:
        type_mapping = {
            'int8': 'int',
            'int16': 'int',
            'int32': 'int',
            'int64': 'long',
            'int': 'int',
            'long': 'long',
            'float32': 'float',
            'float64': 'double',
            'float': 'float',
            'double': 'double',
            'boolean': 'boolean',
            'string': 'string',
        }
        normalized_type = type_mapping.get(field_type, field_type)

        value = None
        if normalized_type == 'int':
            value = rand_gen.randint(0, 1000000)
        elif normalized_type == 'long':
            value = rand_gen.randint(0, 10000000000)
        elif normalized_type == 'string':
            value = ''.join(rand_gen.choices(ascii_letters, k=10))
        elif normalized_type == 'boolean':
            value = rand_gen.choice([True, False])
        elif normalized_type == 'float':
            value = rand_gen.uniform(0, 1000000)
        elif normalized_type == 'double':
            value = rand_gen.uniform(0, 10000000000)
        elif normalized_type == 'bytes':
            value = ''.join(rand_gen.choices(ascii_letters, k=10)).encode('utf-8')
        else:
            LOGGER.warning("Unsupported field type: %s. Using None.", field_type)

        return value


class KafkaProducerThread(KafkaThreadBase):
    """
    Thread that produces records to a Kafka topic.
    Supports 'avro' and 'json-with-schema' record formats.
    """

    def __init__(self, tester, params: SCTConfiguration, timeout: int, **kwargs):
        super().__init__(tester, params, timeout, **kwargs)
        self.produced_count = 0
        self.failed_count = 0
        self.current_key = 1

        self.record_size = int(self.kafka_options.get('record-size', 1024))

        producer_properties = {}
        if 'producer-property' in self.kafka_options:
            if isinstance(self.kafka_options['producer-property'], dict):
                producer_properties.update(self.kafka_options['producer-property'])
            elif isinstance(self.kafka_options['producer-property'], list):
                for prop in self.kafka_options['producer-property']:
                    producer_properties.update(prop)
        if 'property' in self.kafka_options:
            if isinstance(self.kafka_options['property'], dict):
                producer_properties.update(self.kafka_options['property'])
            elif isinstance(self.kafka_options['property'], list):
                for prop in self.kafka_options['property']:
                    producer_properties.update(prop)

        producer_config = {
            'bootstrap.servers': ','.join(self.kafka_addresses),
        }
        # The following parameters can be used to tune/optimize performance and reliability of the producer
        # producer_config['linger.ms'] = int(producer_properties.get('linger.ms', kwargs.get('linger_ms', 5)))
        # producer_config['batch.num.messages'] = int(
        #     producer_properties.get('batch.num.messages', kwargs.get('batch_num_messages', 10000)))
        # producer_config['acks'] = producer_properties.get('acks', kwargs.get('acks', '1'))
        # producer_config['compression.type'] = producer_properties.get(
        #     'compression.type', kwargs.get('compression_type', 'none'))

        producer_config.update(producer_properties)

        self.producer = Producer(producer_config)
        self.wait_for_topic(self.topic, timeout=60)

    def get_topics(self) -> list[str]:
        admin_client = Producer({'bootstrap.servers': ','.join(self.kafka_addresses)})
        topics = admin_client.list_topics(timeout=10).topics.keys()
        LOGGER.debug("Available topics: %s", topics)
        return topics

    def wait_for_topic(self, topic: str, timeout: int) -> None:
        wait_for(lambda: topic in self.get_topics(), text=f"waiting for topic={topic}", timeout=timeout)

    def delivery_report(self, err: str, record: Message) -> None:
        if err:
            self.failed_count += 1
            LOGGER.error("Kafka record delivery failed: %s", err)
        else:
            self.produced_count += 1
            LOGGER.debug(
                "Kafka record delivered to %s [%s] at offset %s", record.topic(), record.partition(), record.offset())

    def run(self) -> None:
        start_time = time.time()
        while not self.termination_event.is_set():
            if self.num_records and self.produced_count >= self.num_records:
                LOGGER.info("Requested number of records is produced, stopping Kafka producer thread.")
                self.stop()
                break

            if self.record_format == 'avro':
                key = self.generate_avro_key(self.current_key)
                value = self.generate_avro_value(self.current_key)
                key_serialized = (
                    self.key_serializer(
                        key, SerializationContext(self.topic, MessageField.KEY)
                    ) if self.key_serializer else None)
                value_serialized = (
                    self.value_serializer(
                        value, SerializationContext(self.topic, MessageField.VALUE)
                    ) if self.value_serializer else None)
            elif self.record_format == 'json-with-schema':
                if self.key_schema:
                    key = self.generate_json_with_schema_key(self.current_key)
                    key_serialized = self.key_serializer(key, SerializationContext(self.topic, MessageField.KEY))
                else:
                    key_serialized = None
                value = self.generate_json_with_schema_value(self.current_key)
                value_serialized = self.value_serializer(value, SerializationContext(self.topic, MessageField.VALUE))

            self.producer.produce(self.topic, value=value_serialized, key=key_serialized, callback=self.delivery_report)
            self.current_key += 1
            self.producer.poll(0)

            if self.timeout and (time.time() - start_time) >= self.timeout:
                LOGGER.warning("Duration exceeded, stopping Kafka producer thread.")
                self.stop()
                break

        self.producer.flush()

    def generate_avro_key(self, seed: int) -> dict | int | float | str | bool | bytes | None:
        if self.key_schema:
            key_schema = json.loads(self.key_schema)
            return self.generate_data_from_schema(key_schema, seed)
        return None

    def generate_avro_value(self, seed: int) -> dict | int | float | str | bool | bytes | None:
        if self.value_schema:
            value_schema = json.loads(self.value_schema)
            return self.generate_data_from_schema(value_schema, seed)
        return None

    def generate_json_with_schema_value(self, seed: int) -> str:
        schema = json.loads(self.value_schema)
        payload = self.generate_data_from_schema(schema, seed)

        value_record = {
            "schema": schema,
            "payload": payload
        }
        value_str = json.dumps(value_record)
        return value_str

    def generate_json_with_schema_key(self, seed: int) -> str:
        schema = json.loads(self.key_schema)
        payload = self.generate_data_from_schema(schema, seed)

        key_record = {
            "schema": schema,
            "payload": payload
        }
        key_str = json.dumps(key_record)
        return key_str

    def stop(self) -> None:
        self.termination_event.set()

    def kill(self) -> None:
        self.stop()

    def verify_results(self) -> (list[dict | None], list[str | None]):
        self.join(self.timeout)
        errors = []
        if self.failed_count > 0:
            msg = f"{self.failed_count} records failed to deliver."
            TestFrameworkEvent(source=self.__class__.__name__, message=msg, severity=Severity.ERROR).publish()
            errors.append(msg)
        return [{'produced_count': self.produced_count}], errors


class KafkaValidatorThread(KafkaThreadBase):
    """
    Thread to validate that records added to Kafka are correctly written to ScyllaDB.
    """

    def __init__(self, tester, params: SCTConfiguration, timeout: int, **kwargs):
        super().__init__(tester, params, timeout, **kwargs)
        self.mismatches = []
        self.batch_size = int(kwargs.get('batch-size', 1000))
        self.keyspace = kwargs.get('keyspace', None)
        self.table = kwargs.get('table', None)
        self.current_key = 1
        self.batch_wait_time = 10
        self.batch_poll_interval = 2
        self.validated_rows = 0

    def run(self) -> None:
        while not self.termination_event.is_set():
            batch_end = min(self.current_key + self.batch_size - 1, self.num_records)
            batch_size = batch_end - self.current_key + 1
            batch_start_time = time.time()
            while True:
                rows = self.read_rows_from_scylla(self.current_key, batch_end)
                if len(rows) < batch_size:
                    if time.time() - batch_start_time > self.batch_wait_time:
                        error_msg = (f"Timeout waiting for records from {self.current_key} to {batch_end} "
                                     f"to be in ScyllaDB. Expected {batch_size}, found {len(rows)}")
                        LOGGER.error(error_msg)
                        self.mismatches.append(error_msg)
                        break  # move to next batch
                    LOGGER.debug("Only %s out of %s records found in ScyllaDB for keys %s to %s. "
                                 "Waiting...", len(rows), batch_size, self.current_key, batch_end)
                    time.sleep(self.batch_poll_interval)
                    continue
                self.compare_rows(rows)
                break  # move to next batch
            self.current_key = batch_end + 1

            if self.current_key >= self.num_records:
                LOGGER.info("All records processed, stopping Kafka validator thread.")
                self.stop()

    def read_rows_from_scylla(self, range_start: int, range_end: int) -> list:
        LOGGER.info("Reading rows from ScyllaDB for keys %s to %s.", range_start, range_end)
        with self.tester.db_cluster.cql_connection_patient(self.tester.db_cluster.nodes[0]) as session:
            query = f"SELECT * FROM {self.keyspace}.{self.table} WHERE id >= %s AND id <= %s ALLOW FILTERING"
            rows = list(session.execute(query, parameters=(range_start, range_end)))
            LOGGER.debug("Retrieved %d rows from ScyllaDB for the current batch.", len(rows))
            return rows

    def compare_rows(self, rows: list) -> None:
        LOGGER.info("Comparing batch of ScyllaDB rows with expected data.")
        for row in rows:
            id_value = row.id
            expected_data = self.generate_data_from_schema(json.loads(self.value_schema), id_value)
            expected_data['id'] = id_value

            row_dict = dict(row._asdict())
            if row_dict != expected_data:
                error_msg = f"Mismatch found for key '{id_value}'."
                LOGGER.warning(error_msg)
                self.mismatches.append(error_msg)

    def stop(self) -> None:
        self.termination_event.set()

    def verify_results(self) -> (list[dict | None], list[str | None]):
        self.join(self.timeout)
        errors = []
        if self.mismatches:
            TestFrameworkEvent(
                source=self.__class__.__name__,
                message=f"Kafka records do not match ScyllaDB rows. Mismatches: {self.mismatches}",
                severity=Severity.ERROR).publish()
            errors.extend(self.mismatches)
        return [{'mismatches': self.mismatches}], errors
