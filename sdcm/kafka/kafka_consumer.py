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

import base64
import json
import logging

from threading import Event, Thread

import kafka

from sdcm.sct_config import SCTConfiguration
from sdcm.kafka.kafka_config import SctKafkaConfiguration
from sdcm.utils.common import generate_random_string
from sdcm.wait import wait_for


LOGGER = logging.getLogger(__name__)


class KafkaCDCReaderThread(Thread):  # pylint: disable=too-many-instance-attributes
    """
    thread that listen on kafka topic, and list all the unique key
    received, so we can validate how many unique key we got
    """

    def __init__(self, tester, params: SCTConfiguration, kafka_addresses: list | None = None,  # pylint: disable=too-many-arguments
                 connector_index: int = 0, group_id: str = None, duration: int | None = None, **kwargs):
        self.keys = set()
        self.termination_event = Event()
        self.params = params
        self.tester = tester
        self.duration = duration
        self._kafka_addresses = kafka_addresses
        self.group_id = group_id or generate_random_string(16)
        self.read_number_of_key = int(kwargs.get('read_number_of_key', 0))

        connector_config: SctKafkaConfiguration = params.get("kafka_connectors")[connector_index]

        # TODO: handle setup of multiple tables
        topic = f'{connector_config.config.scylla_name}.{connector_config.config.scylla_table_names}'
        self.wait_for_topic(topic, timeout=60)
        self.consumer = kafka.KafkaConsumer(
            topic,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            group_id=self.group_id,
            bootstrap_servers=self.kafka_addresses,
        )

        super().__init__(daemon=True)

    @property
    def kafka_addresses(self):
        if self.params.get('kafka_backend') == 'localstack':
            return ['localhost']
        elif self._kafka_addresses:
            return self._kafka_addresses
        return None

    def get_topics(self):
        admin_client = kafka.KafkaAdminClient(bootstrap_servers=self.kafka_addresses)
        topics = admin_client.list_topics()
        LOGGER.debug(topics)
        return topics

    def wait_for_topic(self, topic, timeout):
        def check_topic_exists():
            topics = self.get_topics()
            return topic in topics

        wait_for(check_topic_exists, text=f"waiting for topic={topic}", timeout=timeout)

    def run(self):
        while not self.termination_event.is_set():
            records = self.consumer.poll(timeout_ms=1000)
            for _, consumer_records in records.items():
                for msg in consumer_records:
                    data = json.loads(msg.value).get('payload', {}).get('after', {})
                    key = base64.b64decode(data.get('key')).decode()
                    self.keys.add(key)

            if len(self.keys) >= self.read_number_of_key:
                LOGGER.info("reach `read_number_of_key` stopping reader thread")
                self.stop()

    def stop(self):
        self.termination_event.set()
        self.consumer.close()

    def kill(self):
        self.stop()

    def verify_results(self) -> (list[dict | None], list[str | None]):
        self.join(self.duration)
        return [], []
