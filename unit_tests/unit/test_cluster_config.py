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
# Copyright (c) 2026 ScyllaDB

from unittest.mock import MagicMock, call

import pytest
from cassandra import InvalidRequest

from sdcm.utils.cluster_config import apply_cluster_config


@pytest.fixture()
def db_cluster():
    """Cluster whose CQL session records the statements it runs."""
    return MagicMock()


@pytest.fixture()
def session(db_cluster):
    """The CQL session apply_cluster_config runs its statements on."""
    return db_cluster.cql_connection_patient.return_value.__enter__.return_value


def test_apply_cluster_config_runs_alter_cluster_per_option(db_cluster, session):
    """The cluster_config values of configurations/auto-repair-1h.yaml must reach the cluster as
    ALTER CLUSTER statements, YAML booleans as CQL true/false."""
    apply_cluster_config(db_cluster, {"auto_repair_enabled": True, "auto_repair_threshold_in_seconds": 3600})
    assert session.execute.call_args_list == [
        call("ALTER CLUSTER WITH auto_repair_enabled = true"),
        call("ALTER CLUSTER WITH auto_repair_threshold_in_seconds = 3600"),
    ]


def test_apply_cluster_config_raises_when_rejected(db_cluster, session):
    """A rejected option (e.g. a Scylla release without CQL cluster configuration) must fail the
    setup instead of letting the test run without the requested setting."""
    session.execute.side_effect = InvalidRequest("Unknown option auto_repair_enabled")
    with pytest.raises(InvalidRequest, match="auto_repair_enabled"):
        apply_cluster_config(db_cluster, {"auto_repair_enabled": True})
