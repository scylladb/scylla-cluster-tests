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

from unittest.mock import MagicMock

import pytest
from cassandra import InvalidRequest

from sdcm.sct_config import ClusterConfig
from sdcm.utils.cluster_config import apply_cluster_config


@pytest.fixture()
def cql_session():
    """CQL session that records the statements sent to the cluster."""
    return MagicMock()


@pytest.fixture()
def db_cluster(cql_session):
    """Cluster that hands out the recording CQL session."""
    cluster = MagicMock()
    cluster.cql_connection_patient.return_value.__enter__.return_value = cql_session
    return cluster


@pytest.mark.parametrize(
    "cluster_config,statement",
    [
        pytest.param({"auto_repair_enabled": True}, "ALTER CLUSTER WITH auto_repair_enabled = true", id="bool"),
        pytest.param(
            {"auto_repair_threshold_in_seconds": 3600},
            "ALTER CLUSTER WITH auto_repair_threshold_in_seconds = 3600",
            id="int",
        ),
        pytest.param({"auto_repair_enabled": "true"}, "ALTER CLUSTER WITH auto_repair_enabled = 'true'", id="string"),
    ],
)
def test_apply_cluster_config_runs_alter_cluster(db_cluster, cql_session, cluster_config, statement):
    """Each option must reach the cluster as ALTER CLUSTER, with a string value sent as a quoted CQL literal."""
    apply_cluster_config(db_cluster, cluster_config)
    cql_session.execute.assert_called_once_with(statement)


def test_apply_cluster_config_raises_when_rejected(db_cluster, cql_session):
    """A rejected option must fail the setup instead of letting the test run without the requested setting."""
    cql_session.execute.side_effect = InvalidRequest("Unknown cluster config option 'nonexistent_option'")
    with pytest.raises(InvalidRequest, match="nonexistent_option"):
        apply_cluster_config(db_cluster, {"nonexistent_option": True})


@pytest.mark.parametrize(
    "input_val,match_pattern",
    [
        pytest.param({"auto repair": True}, "String should match pattern", id="invalid-option-name"),
        pytest.param({"auto_repair_enabled": [True]}, "Input should be a valid boolean", id="invalid-value"),
    ],
)
def test_cluster_config_model_rejects_invalid_input(input_val, match_pattern):
    """A malformed cluster_config must fail when the configuration is loaded, not after the cluster is set up."""
    with pytest.raises(ValueError, match=match_pattern):
        ClusterConfig.model_validate(input_val)
