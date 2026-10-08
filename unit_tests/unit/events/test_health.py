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

import pickle

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent, DataValidatorEvent


@pytest.mark.parametrize(
    "subevent_name",
    [
        pytest.param("NodeStatus", id="node-status"),
        pytest.param("NodePeersNulls", id="node-peers-nulls"),
        pytest.param("NodeSchemaVersion", id="node-schema-version"),
        pytest.param("NodesNemesis", id="nodes-nemesis"),
        pytest.param("ScyllaCloudClusterServerDiagnostic", id="scylla-cloud-cluster-server-diagnostic"),
    ],
)
def test_cluster_health_validator_event(subevent_name):
    assert hasattr(ClusterHealthValidatorEvent, subevent_name)
    assert issubclass(getattr(ClusterHealthValidatorEvent, subevent_name), ClusterHealthValidatorEvent)


def test_cluster_health_validator_event_msgfmt():
    chc_event = ClusterHealthValidatorEvent()
    chc_event.publish_event = False
    chc_event.event_id = "7208cfbb-a083-4b7a-b0db-1982d88f6da0"
    chc_event.begin_event()
    assert str(chc_event) == (
        "(ClusterHealthValidatorEvent Severity.NORMAL) period_type=begin event_id=7208cfbb-a083-4b7a-b0db-1982d88f6da0"
    )

    chc_event.message = "Cluster health check finished"
    chc_event.duration = 5
    chc_event.end_event()
    assert str(chc_event) == (
        "(ClusterHealthValidatorEvent Severity.NORMAL) period_type=end "
        "event_id=7208cfbb-a083-4b7a-b0db-1982d88f6da0 duration=5s message=Cluster health check finished"
    )


@pytest.mark.parametrize(
    "event_class, kwargs, expected",
    [
        pytest.param(
            ClusterHealthValidatorEvent.NodeStatus,
            {"severity": Severity.CRITICAL, "node": "n1", "error": "e1"},
            "(ClusterHealthValidatorEvent Severity.CRITICAL) period_type=one-time "
            "event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodeStatus node=n1 error=e1",
            id="node-status",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodePeersNulls,
            {"severity": Severity.ERROR, "node": "n2", "error": "e2"},
            "(ClusterHealthValidatorEvent Severity.ERROR) period_type=one-time "
            "event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodePeersNulls node=n2 error=e2",
            id="node-peers-nulls",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodeSchemaVersion,
            {"severity": Severity.WARNING, "node": "n3", "message": "m3"},
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time "
            "event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodeSchemaVersion node=n3 message=m3",
            id="node-schema-version",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodesNemesis,
            {"severity": Severity.WARNING, "node": "n4", "message": "m4"},
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time "
            "event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodesNemesis node=n4 message=m4",
            id="nodes-nemesis",
        ),
    ],
)
def test_cluster_health_validator_subevent_msgfmt(event_class, kwargs, expected):
    event = event_class(**kwargs)
    event.event_id = "712128d0-4837-4213-8a60-d6e2ec106c52"
    assert str(event) == expected
    assert event == pickle.loads(pickle.dumps(event))


@pytest.mark.parametrize(
    "subevent_name",
    [
        pytest.param("DataValidator", id="data-validator"),
        pytest.param("ImmutableRowsValidator", id="immutable-rows-validator"),
        pytest.param("UpdatedRowsValidator", id="updated-rows-validator"),
        pytest.param("DeletedRowsValidator", id="deleted-rows-validator"),
    ],
)
def test_data_validator_event(subevent_name):
    assert hasattr(DataValidatorEvent, subevent_name)
    assert issubclass(getattr(DataValidatorEvent, subevent_name), DataValidatorEvent)


@pytest.mark.parametrize(
    "event_class, kwargs, expected",
    [
        pytest.param(
            DataValidatorEvent.DataValidator,
            {"severity": Severity.ERROR, "error": "e1"},
            "(DataValidatorEvent Severity.ERROR) period_type=one-time "
            "event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=DataValidator error=e1",
            id="data-validator",
        ),
        pytest.param(
            DataValidatorEvent.ImmutableRowsValidator,
            {"severity": Severity.ERROR, "error": "e2"},
            "(DataValidatorEvent Severity.ERROR) period_type=one-time "
            "event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=ImmutableRowsValidator error=e2",
            id="immutable-rows-validator",
        ),
        pytest.param(
            DataValidatorEvent.UpdatedRowsValidator,
            {"severity": Severity.WARNING, "message": "m3"},
            "(DataValidatorEvent Severity.WARNING) period_type=one-time "
            "event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=UpdatedRowsValidator message=m3",
            id="updated-rows-validator",
        ),
        pytest.param(
            DataValidatorEvent.DeletedRowsValidator,
            {"severity": Severity.NORMAL, "message": "m4"},
            "(DataValidatorEvent Severity.NORMAL) period_type=one-time "
            "event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=DeletedRowsValidator message=m4",
            id="deleted-rows-validator",
        ),
    ],
)
def test_data_validator_event_msgfmt(event_class, kwargs, expected):
    event = event_class(**kwargs)
    event.event_id = "3916da00-643c-4886-bdd0-963d3ebac536"
    assert str(event) == expected
    assert event == pickle.loads(pickle.dumps(event))
