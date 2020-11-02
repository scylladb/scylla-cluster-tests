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
        self.assertTrue(hasattr(ClusterHealthValidatorEvent, "ClusterHealthCheck"))
        self.assertTrue(issubclass(ClusterHealthValidatorEvent.ClusterHealthCheck, ClusterHealthValidatorEvent))

    def test_cluster_health_validator_event_msgfmt(self):
        critical_event = ClusterHealthValidatorEvent.NodeStatus.CRITICAL(node="n1", error="e1")
        self.assertEqual(
            str(critical_event),
            "(ClusterHealthValidatorEvent Severity.CRITICAL): type=NodeStatus subtype=CRITICAL node=n1 error=e1"
        )
        self.assertEqual(critical_event, pickle.loads(pickle.dumps(critical_event)))

        error_event = ClusterHealthValidatorEvent.NodePeersNulls.ERROR(node="n2", error="e2")
        self.assertEqual(
            str(error_event),
            "(ClusterHealthValidatorEvent Severity.ERROR): type=NodePeersNulls subtype=ERROR node=n2 error=e2"
        )
        self.assertEqual(error_event, pickle.loads(pickle.dumps(error_event)))

        warning_event = ClusterHealthValidatorEvent.NodeSchemaVersion.WARNING(node="n3", message="m3")
        self.assertEqual(
            str(warning_event),
            "(ClusterHealthValidatorEvent Severity.WARNING): type=NodeSchemaVersion subtype=WARNING node=n3 message=m3"
        )
        self.assertEqual(warning_event, pickle.loads(pickle.dumps(warning_event)))

        info_event = ClusterHealthValidatorEvent.NodesNemesis.INFO(node="n4", message="m4")
        self.assertEqual(
            str(info_event),
            "(ClusterHealthValidatorEvent Severity.NORMAL): type=NodesNemesis subtype=INFO node=n4 message=m4"
        )
        self.assertEqual(info_event, pickle.loads(pickle.dumps(info_event)))

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
        critical_event = DataValidatorEvent.DataValidator.CRITICAL(error="e1")
        self.assertEqual(
            str(critical_event),
            "(DataValidatorEvent Severity.CRITICAL): type=DataValidator subtype=CRITICAL error=e1"
        )
        self.assertEqual(critical_event, pickle.loads(pickle.dumps(critical_event)))

        error_event = DataValidatorEvent.ImmutableRowsValidator.ERROR(error="e2")
        self.assertEqual(
            str(error_event),
            "(DataValidatorEvent Severity.ERROR): type=ImmutableRowsValidator subtype=ERROR error=e2"
        )
        self.assertEqual(error_event, pickle.loads(pickle.dumps(error_event)))

        warning_event = DataValidatorEvent.UpdatedRowsValidator.WARNING(message="m3")
        self.assertEqual(
            str(warning_event),
            "(DataValidatorEvent Severity.WARNING): type=UpdatedRowsValidator subtype=WARNING message=m3"
        )
        self.assertEqual(warning_event, pickle.loads(pickle.dumps(warning_event)))

        info_event = DataValidatorEvent.DeletedRowsValidator.INFO(message="m4")
        self.assertEqual(
            str(info_event),
            "(DataValidatorEvent Severity.NORMAL): type=DeletedRowsValidator subtype=INFO message=m4"
        )
        self.assertEqual(info_event, pickle.loads(pickle.dumps(info_event)))
