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

import pytest

pytestmark = [
    pytest.mark.integration,
]


def test_01_cqlsh_check_escaping(docker_scylla):
    cql_cmd = ('create keyspace if not exists "10gb_keyspace" with replication = '
               '{\'class\': \'NetworkTopologyStrategy\', \'replication_factor\': 1}')

    res = docker_scylla.run_cqlsh(cql_cmd)
    assert res.ok

    cql_cmd = 'describe keyspace "10gb_keyspace"'
    res = docker_scylla.run_cqlsh(cql_cmd)
    assert 'CREATE KEYSPACE "10gb_keyspace"' in res.stdout
