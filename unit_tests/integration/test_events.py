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
# Copyright (c) 2020 ScyllaDB


import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.sct_events.setup import enable_default_filters
from sdcm.utils.context_managers import environment

from unit_tests.unit.test_events import BaseEventsTest

pytestmark = [
    pytest.mark.integration,
]


class TestDefaultFilters(BaseEventsTest):
    """Integration tests for SCT event default filters that require SCTConfiguration."""

    def test_default_filters(self):
        with environment(SCT_CLUSTER_BACKEND="docker"):
            enable_default_filters(SCTConfiguration())

        with self.wait_for_n_events(self.get_events_logger(), count=5):
            DatabaseLogEvent.BACKTRACE().add_info(
                node="A",
                line_number=22,
                line="Jul 01 03:37:31 ip-10-0-127-151.eu-west-1.compute.internal"
                " scylla[6026]:Rate-limit: supressed 4294967292 backtraces on shard 5",
            ).publish()
            DatabaseLogEvent.BACKTRACE().add_info(
                node="A", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()

            DatabaseLogEvent.DATABASE_ERROR().add_info(
                node="A",
                line_number=22,
                line="ERROR 2023-12-18 12:45:25,673 [shard 5] view - Error applying view update to 172.20.196.153 "
                "(view: keyspace1.standard1_c4_nemesis_index, base token: 3003228260188484921, view token: "
                "-268424650415671818): data_dictionary::no_such_column_family (Can't find a column family with "
                "UUID 277dc241-9da2-11ee-a7ab-9c797a34c82c)",
            ).publish()

            CassandraStressLogEvent.ConsistencyError().add_info(
                node="A",
                line_number=22,
                line="ERROR 18:04:32,556 Authentication error on host rolling-upgrade--ubuntu-focal-db-node-24508405-0-3"
                ".c.sct-project-1.internal/10.142.1.155:9042: Cannot achieve consistency level for cl ONE. Requires 1, alive 0",
            ).publish()

            DatabaseLogEvent.RUNTIME_ERROR().add_info(
                node="A",
                line_number=22,
                line="ERROR 2023-12-18 12:45:25,673 [shard 0: gms] raft_topology - topology change coordinator fiber got error std::runtime_error "
                "(raft topology: exec_global_command(barrier) failed with seastar::rpc::closed_error (connection is closed))",
            ).publish()

        log_content = self.get_event_log_file("events.log")

        self.assertIn("other back trace", log_content)
        self.assertNotIn("supressed", log_content)

        warnings_log_content = self.get_event_log_file("warning.log")
        assert "data_dictionary::no_such_column_family" in warnings_log_content
        assert "Authentication error" not in warnings_log_content

        error_log_content = self.get_event_log_file("error.log")
        assert "data_dictionary::no_such_column_family" not in error_log_content
        assert "Authentication error" in error_log_content
        assert "topology change coordinator fiber got error" not in error_log_content
