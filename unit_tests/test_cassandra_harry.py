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

import pytest

from sdcm.cassandra_harry_thread import CassandraHarryThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


def test_01_cassandra_harry(request, docker_scylla, events, params):
    """
    Test to help integrated new version/docker images of cassandra-harry

    TODO: Test isn't really running cassandra-harry correctly, since there only one node

    DEBUG    LocalCmdRunner:base.py:222 com.datastax.driver.core.exceptions.NoHostAvailableException:
    All host(s) tried for query failed (tried: /172.17.0.2:9042 (com.datastax.driver.core.exceptions.UnavailableException:
    Not enough replicas available for query at consistency QUORUM (2 required but only 1 alive)))
    """
    loader_set = LocalLoaderSetDummy()

    cmd = "cassandra-harry -run-time 1 -run-time-unit MINUTES"
    harry_thread = CassandraHarryThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params
    )

    def cleanup_thread():
        harry_thread.kill()

    request.addfinalizer(cleanup_thread)

    file_logger = events.get_events_logger()
    with events.wait_for_n_events(file_logger, count=2, timeout=120):
        harry_thread.run()

    # 5. check that events with the expected error were raised
    cat = file_logger.get_events_by_category()
    assert len(cat["ERROR"]) == 2
    assert "=UNEXPECTED_STATE" in cat["ERROR"][0]
    assert "=ERROR" in cat["ERROR"][1]
