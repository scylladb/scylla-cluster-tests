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
import unittest
from sdcm.sct_events import Severity

from sdcm.sct_events.health import ClusterHealthValidatorEvent, DataValidatorEvent


class TestValidators(unittest.TestCase):
    def test_cluster_health_validator_event(self):
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "NodeStatus"))
        self.assertTrue(issubclass(ClusterHealthValidatorEvent.NodeStatus, ClusterHealthValidatorEvent))
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "NodePeersNulls"))
        self.assertTrue(issubclass(ClusterHealthValidatorEvent.NodePeersNulls, ClusterHealthValidatorEvent))
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "NodeSchemaVersion"))
        self.assertTrue(issubclass(ClusterHealthValidatorEvent.NodeSchemaVersion, ClusterHealthValidatorEvent))
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "NodesNemesis"))
        self.assertTrue(issubclass(ClusterHealthValidatorEvent.NodesNemesis, ClusterHealthValidatorEvent))
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "ScyllaCloudClusterServerDiagnostic"))
        self.assertTrue(
            issubclass(ClusterHealthValidatorEvent.ScyllaCloudClusterServerDiagnostic, ClusterHealthValidatorEvent)
        )

    def test_cluster_health_validator_event_msgfmt(self):
        chc_event = ClusterHealthValidatorEvent()
        chc_event.publish_event = False
        chc_event.event_id = "7208cfbb-a083-4b7a-b0db-1982d88f6da0"
        chc_event.begin_event()
        self.assertEqual(
            str(chc_event),
            "(ClusterHealthValidatorEvent Severity.NORMAL) period_type=begin event_id=7208cfbb-a083-4b7a-b0db-1982d88f6da0",
        )

        critical_event = ClusterHealthValidatorEvent.NodeStatus(severity=Severity.CRITICAL, node="n1", error="e1")
        critical_event.event_id = "712128d0-4837-4213-8a60-d6e2ec106c52"
        self.assertEqual(
            str(critical_event),
            "(ClusterHealthValidatorEvent Severity.CRITICAL) period_type=one-time event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodeStatus node=n1 error=e1",
        )
        self.assertEqual(critical_event, pickle.loads(pickle.dumps(critical_event)))

        error_event = ClusterHealthValidatorEvent.NodePeersNulls(severity=Severity.ERROR, node="n2", error="e2")
        error_event.event_id = "712128d0-4837-4213-8a60-d6e2ec106c52"
        self.assertEqual(
            str(error_event),
            "(ClusterHealthValidatorEvent Severity.ERROR) period_type=one-time event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodePeersNulls node=n2 error=e2",
        )
        self.assertEqual(error_event, pickle.loads(pickle.dumps(error_event)))

        warning_event = ClusterHealthValidatorEvent.NodeSchemaVersion(
            severity=Severity.WARNING, node="n3", message="m3"
        )
        warning_event.event_id = "712128d0-4837-4213-8a60-d6e2ec106c52"
        self.assertEqual(
            str(warning_event),
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodeSchemaVersion node=n3 message=m3",
        )
        self.assertEqual(warning_event, pickle.loads(pickle.dumps(warning_event)))

        info_event = ClusterHealthValidatorEvent.NodesNemesis(severity=Severity.WARNING, node="n4", message="m4")
        info_event.event_id = "712128d0-4837-4213-8a60-d6e2ec106c52"
        self.assertEqual(
            str(info_event),
            "(ClusterHealthValidatorEvent Severity.WARNING) period_type=one-time event_id=712128d0-4837-4213-8a60-d6e2ec106c52: type=NodesNemesis node=n4 message=m4",
        )
        self.assertEqual(info_event, pickle.loads(pickle.dumps(info_event)))

        chc_event.message = "Cluster health check finished"
        chc_event.duration = 5
        chc_event.end_event()
        self.assertEqual(
            str(chc_event),
            "(ClusterHealthValidatorEvent Severity.NORMAL) period_type=end event_id=7208cfbb-a083-4b7a-b0db-1982d88f6da0 duration=5s message=Cluster health check finished",
        )

    def test_data_validator_event(self):
        self.assertTrue(hasattr(DataValidatorEvent, "DataValidator"))
        self.assertTrue(issubclass(DataValidatorEvent.DataValidator, DataValidatorEvent))
        self.assertTrue(hasattr(DataValidatorEvent, "ImmutableRowsValidator"))
        self.assertTrue(issubclass(DataValidatorEvent.ImmutableRowsValidator, DataValidatorEvent))
        self.assertTrue(hasattr(DataValidatorEvent, "UpdatedRowsValidator"))
        self.assertTrue(issubclass(DataValidatorEvent.UpdatedRowsValidator, DataValidatorEvent))
        self.assertTrue(hasattr(DataValidatorEvent, "DeletedRowsValidator"))
        self.assertTrue(issubclass(DataValidatorEvent.DeletedRowsValidator, DataValidatorEvent))

    def test_data_validator_event_msgfmt(self):
        critical_event = DataValidatorEvent.DataValidator(severity=Severity.ERROR, error="e1")
        critical_event.event_id = "3916da00-643c-4886-bdd0-963d3ebac536"
        self.assertEqual(
            str(critical_event),
            "(DataValidatorEvent Severity.ERROR) period_type=one-time event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=DataValidator error=e1",
        )
        self.assertEqual(critical_event, pickle.loads(pickle.dumps(critical_event)))

        error_event = DataValidatorEvent.ImmutableRowsValidator(severity=Severity.ERROR, error="e2")
        error_event.event_id = "3916da00-643c-4886-bdd0-963d3ebac536"
        self.assertEqual(
            str(error_event),
            "(DataValidatorEvent Severity.ERROR) period_type=one-time event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=ImmutableRowsValidator error=e2",
        )
        self.assertEqual(error_event, pickle.loads(pickle.dumps(error_event)))

        warning_event = DataValidatorEvent.UpdatedRowsValidator(severity=Severity.WARNING, message="m3")
        warning_event.event_id = "3916da00-643c-4886-bdd0-963d3ebac536"
        self.assertEqual(
            str(warning_event),
            "(DataValidatorEvent Severity.WARNING) period_type=one-time event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=UpdatedRowsValidator message=m3",
        )
        self.assertEqual(warning_event, pickle.loads(pickle.dumps(warning_event)))

        info_event = DataValidatorEvent.DeletedRowsValidator(severity=Severity.NORMAL, message="m4")
        info_event.event_id = "3916da00-643c-4886-bdd0-963d3ebac536"
        self.assertEqual(
            str(info_event),
            "(DataValidatorEvent Severity.NORMAL) period_type=one-time event_id=3916da00-643c-4886-bdd0-963d3ebac536: type=DeletedRowsValidator message=m4",
        )
        self.assertEqual(info_event, pickle.loads(pickle.dumps(info_event)))
