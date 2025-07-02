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
# Copyright (c) 2022 ScyllaDB

import logging
import time

from sdcm.sct_events import Severity
from sdcm.sct_events.handlers import EventHandler
from sdcm.sct_events.loaders import CassandraStressLogEvent, SchemaDisagreementErrorEvent
from sdcm.utils.argus import create_proxy_argus_s3_url
from sdcm.utils.sstable.s3_uploader import upload_sstables_to_s3

LOGGER = logging.getLogger(__name__)


class SchemaDisagreementHandler(EventHandler):
    """Collects relevant data for schema mismatch investigation."""

    def __init__(self):
        super().__init__()
        self._sstables_uploaded_time = 0

    def _should_upload_sstables(self):
        # upload sstables once per hour as usually these events come multiple times for some period of time
        return time.time() - 3600 > self._sstables_uploaded_time

    def handle(self, event: CassandraStressLogEvent, tester_obj: "sdcm.tester.ClusterTester"):  # noqa: F821
        if self._should_upload_sstables():
            self._sstables_uploaded_time = time.time()
            # create new event with details to make them visible in error events log/argus
            event = SchemaDisagreementErrorEvent(severity=Severity.ERROR)
            gossip_info = {}
            peers_info = {}
            for node in list(tester_obj.db_cluster.nodes):
                LOGGER.info("Collecting data related to schema disagreement for node: %s", node.name)
                gossip_info = gossip_info or node.get_gossip_info()
                peers_info = peers_info or node.get_peers_info()
                try:
                    link = upload_sstables_to_s3(node, keyspace='system_schema',
                                                 test_id=tester_obj.test_id, public=False)
                    event.add_sstable_link(create_proxy_argus_s3_url(link, True))
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("failed to upload system_schema sstables for node %s: %s", node.name, exc)
            event.add_gossip_info(gossip_info)
            event.add_peers_info(peers_info)
            event.ready_to_publish()
            event.publish()
        else:
            LOGGER.debug("Skipping SchemaDisagreement handler")
