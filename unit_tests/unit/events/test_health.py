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

EVENT_ID = "712128d0-4837-4213-8a60-d6e2ec106c52"


def test_cluster_health_validator_event_msgfmt():
    """A cluster health check renders as begin, then as end with its duration and message."""
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
            f"event_id={EVENT_ID}: type=NodeStatus node=n1 error=e1",
            id="cluster-health-node-status",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodePeersNulls,
            {"severity": Severity.ERROR, "node": "n2", "error": "e2"},
            "(ClusterHealthValidatorEvent Severity.ERROR) period_type=one-time "
            f"event_id={EVENT_ID}: type=NodePeersNulls node=n2 error=e2",
            id="cluster-health-node-peers-nulls",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodeSchemaVersion,
            {"severity": Severity.WARNING, "node": "n3", "message": "m3"},
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time "
            f"event_id={EVENT_ID}: type=NodeSchemaVersion node=n3 message=m3",
            id="cluster-health-node-schema-version",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.NodesNemesis,
            {"severity": Severity.WARNING, "node": "n4", "message": "m4"},
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time "
            f"event_id={EVENT_ID}: type=NodesNemesis node=n4 message=m4",
            id="cluster-health-nodes-nemesis",
        ),
        pytest.param(
            ClusterHealthValidatorEvent.ScyllaCloudClusterServerDiagnostic,
            {"severity": Severity.ERROR, "node": "n5", "error": "e5"},
            "(ClusterHealthValidatorEvent Severity.ERROR) period_type=one-time "
            f"event_id={EVENT_ID}: type=ScyllaCloudClusterServerDiagnostic node=n5 error=e5",
            id="cluster-health-scylla-cloud-cluster-server-diagnostic",
        ),
        pytest.param(
            DataValidatorEvent.DataValidator,
            {"severity": Severity.ERROR, "error": "e1"},
            "(DataValidatorEvent Severity.ERROR) period_type=one-time "
            f"event_id={EVENT_ID}: type=DataValidator error=e1",
            id="data-validator-data-validator",
        ),
        pytest.param(
            DataValidatorEvent.ImmutableRowsValidator,
            {"severity": Severity.ERROR, "error": "e2"},
            "(DataValidatorEvent Severity.ERROR) period_type=one-time "
            f"event_id={EVENT_ID}: type=ImmutableRowsValidator error=e2",
            id="data-validator-immutable-rows-validator",
        ),
        pytest.param(
            DataValidatorEvent.UpdatedRowsValidator,
            {"severity": Severity.WARNING, "message": "m3"},
            "(DataValidatorEvent Severity.WARNING) period_type=one-time "
            f"event_id={EVENT_ID}: type=UpdatedRowsValidator message=m3",
            id="data-validator-updated-rows-validator",
        ),
        pytest.param(
            DataValidatorEvent.DeletedRowsValidator,
            {"severity": Severity.NORMAL, "message": "m4"},
            "(DataValidatorEvent Severity.NORMAL) period_type=one-time "
            f"event_id={EVENT_ID}: type=DeletedRowsValidator message=m4",
            id="data-validator-deleted-rows-validator",
        ),
    ],
)
def test_health_subevent_msgfmt(event_class, kwargs, expected):
    """Each health sub-event renders its type and fields in the log format and survives a pickle round trip."""
    event = event_class(**kwargs)
    event.event_id = EVENT_ID
    assert str(event) == expected
    assert event == pickle.loads(pickle.dumps(event))
