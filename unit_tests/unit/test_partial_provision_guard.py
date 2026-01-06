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

"""Tests that partial provisioning is detected and raises errors.

Covers SCT-501: when pre-provisioning partially fails (e.g. zone exhaustion
after creating some instances), the test setUp should not silently continue
with fewer nodes than requested.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_aws import AWSCluster
from sdcm.cluster_gce import GCECluster
from sdcm.provision.provisioner import ProvisionError, ZoneResourcesExhaustedError
from sdcm.sct_events import Severity
from sdcm.tester import teardown_on_exception


# ---------------------------------------------------------------------------
# GCE: add_nodes count validation
# ---------------------------------------------------------------------------


def _make_gce_cluster(instance_count, requested_count, is_reuse=False):
    """Create a minimally-mocked GCECluster and invoke add_nodes.

    Args:
        instance_count: how many pre-provisioned instances _get_instances returns.
        requested_count: the count passed to add_nodes.
        is_reuse: whether REUSE_CLUSTER is set.
    """
    fake_instances = [MagicMock(name=f"instance-{i}") for i in range(instance_count)]

    cluster = MagicMock(spec=GCECluster)
    cluster.log = MagicMock()
    cluster.params = MagicMock()
    cluster.params.get.side_effect = lambda key, *a, **kw: {
        "simulated_regions": False,
    }.get(key)
    cluster._node_index = 0
    cluster.nodes = []
    cluster.racks_count = 1

    test_config = MagicMock()
    test_config.REUSE_CLUSTER = is_reuse
    test_config.test_id.return_value = "test-id-123"
    cluster.test_config = test_config

    cluster._get_instances = MagicMock(return_value=fake_instances)
    cluster._create_node = MagicMock(
        side_effect=lambda inst, idx, dc, rack, after_config=None: MagicMock(name=f"node-{idx}")
    )

    # Call the real add_nodes with self=cluster
    return GCECluster.add_nodes.__wrapped__(
        cluster,
        count=requested_count,
        dc_idx=0,
        rack=0,
    )


def test_gce_add_nodes_partial_provision_raises_error():
    """add_nodes should raise ProvisionError when fewer instances are found than requested."""
    with pytest.raises(ProvisionError, match=r"Found only 2.*but 9 were requested"):
        _make_gce_cluster(instance_count=2, requested_count=9)


def test_gce_add_nodes_exact_count_succeeds():
    """add_nodes should succeed when exactly the requested number of instances are found."""
    result = _make_gce_cluster(instance_count=3, requested_count=3)
    assert len(result) == 3


def test_gce_add_nodes_more_instances_than_requested_succeeds():
    """add_nodes should succeed when more instances are found than requested."""
    result = _make_gce_cluster(instance_count=5, requested_count=3)
    assert len(result) >= 3


def test_gce_add_nodes_no_instances_provisions_inline():
    """add_nodes should fall through to inline provisioning when no instances are found."""
    cluster = MagicMock(spec=GCECluster)
    cluster.log = MagicMock()
    cluster.params = MagicMock()
    cluster.params.get.side_effect = lambda key, *a, **kw: {
        "simulated_regions": False,
    }.get(key)
    cluster._node_index = 0
    cluster.nodes = []
    cluster.racks_count = 1

    test_config = MagicMock()
    test_config.REUSE_CLUSTER = False
    cluster.test_config = test_config

    # _get_instances returns empty list -> falls through to _create_instances
    cluster._get_instances = MagicMock(return_value=[])

    fake_vms = [MagicMock(name=f"vm-{i}") for i in range(3)]
    cluster._create_instances = MagicMock(return_value=fake_vms)
    cluster._get_instance_with_retry = MagicMock(side_effect=lambda name, dc_idx: MagicMock(name=name))
    cluster._create_node = MagicMock(
        side_effect=lambda inst, idx, dc, rack, after_config=None: MagicMock(name=f"node-{idx}")
    )

    result = GCECluster.add_nodes.__wrapped__(cluster, count=3, dc_idx=0, rack=0)
    cluster._create_instances.assert_called_once_with(3, 0, instance_type=None)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# AWS: _create_or_find_instances count validation
# ---------------------------------------------------------------------------


def test_aws_create_or_find_partial_provision_raises_error():
    """AWS _create_or_find_instances should raise ProvisionError when fewer instances found than requested."""
    cluster = MagicMock(spec=AWSCluster)
    cluster.log = MagicMock()
    cluster.params = MagicMock()
    cluster.params.get.side_effect = lambda key, *a, **kw: {
        "simulated_racks": False,
    }.get(key)
    cluster.nodes = []

    test_config = MagicMock()
    test_config.REUSE_CLUSTER = False
    cluster.test_config = test_config

    fake_instances = [MagicMock() for _ in range(2)]
    cluster._get_instances = MagicMock(return_value=fake_instances)

    with pytest.raises(ProvisionError, match=r"Found only 2.*but 5 were requested"):
        AWSCluster._create_or_find_instances(cluster, count=5, ec2_user_data="", dc_idx=0, az_idx=0)


def test_aws_create_or_find_exact_count_succeeds():
    """AWS _create_or_find_instances should succeed when exactly the requested count is found."""
    cluster = MagicMock(spec=AWSCluster)
    cluster.log = MagicMock()
    cluster.params = MagicMock()
    cluster.params.get.side_effect = lambda key, *a, **kw: {
        "simulated_racks": False,
    }.get(key)
    cluster.nodes = []

    test_config = MagicMock()
    test_config.REUSE_CLUSTER = False
    cluster.test_config = test_config

    fake_instances = [MagicMock() for _ in range(5)]
    cluster._get_instances = MagicMock(return_value=fake_instances)

    result = AWSCluster._create_or_find_instances(cluster, count=5, ec2_user_data="", dc_idx=0, az_idx=0)
    assert len(result) == 5


# ---------------------------------------------------------------------------
# teardown_on_exception: severity escalation for provisioning errors
# ---------------------------------------------------------------------------


def _make_tester_mock():
    """Create a minimal mock of ClusterTester for teardown_on_exception tests."""
    tester = MagicMock()
    tester.__class__.__name__ = "ClusterTester"
    tester.params = MagicMock()
    tester.argus_heartbeat_stop_signal = MagicMock()
    tester.tearDown = MagicMock()
    return tester


def test_teardown_on_exception_provision_error_publishes_critical():
    """ProvisionError in setUp should publish TestFrameworkEvent with CRITICAL severity."""
    tester = _make_tester_mock()

    @teardown_on_exception
    def fake_setup(self):
        raise ProvisionError("Failed to create instances")

    with patch("sdcm.tester.TestFrameworkEvent") as mock_event_cls:
        mock_event_cls.return_value.publish_or_dump = MagicMock()
        with pytest.raises(ProvisionError):
            fake_setup(tester)
        mock_event_cls.assert_called_once()
        _, kwargs = mock_event_cls.call_args
        assert kwargs["severity"] == Severity.CRITICAL


def test_teardown_on_exception_zone_exhausted_publishes_critical():
    """ZoneResourcesExhaustedError in setUp should publish TestFrameworkEvent with CRITICAL severity."""
    tester = _make_tester_mock()

    @teardown_on_exception
    def fake_setup(self):
        raise ZoneResourcesExhaustedError("Zone us-east1-d exhausted")

    with patch("sdcm.tester.TestFrameworkEvent") as mock_event_cls:
        mock_event_cls.return_value.publish_or_dump = MagicMock()
        with pytest.raises(ZoneResourcesExhaustedError):
            fake_setup(tester)
        mock_event_cls.assert_called_once()
        _, kwargs = mock_event_cls.call_args
        assert kwargs["severity"] == Severity.CRITICAL


def test_teardown_on_exception_generic_error_publishes_default_severity():
    """Non-provisioning errors in setUp should publish TestFrameworkEvent with default (None) severity."""
    tester = _make_tester_mock()

    @teardown_on_exception
    def fake_setup(self):
        raise RuntimeError("Something else went wrong")

    with patch("sdcm.tester.TestFrameworkEvent") as mock_event_cls:
        mock_event_cls.return_value.publish_or_dump = MagicMock()
        with pytest.raises(RuntimeError):
            fake_setup(tester)
        mock_event_cls.assert_called_once()
        _, kwargs = mock_event_cls.call_args
        assert kwargs["severity"] is None


def test_teardown_on_exception_preserves_severity_from_exception():
    """If the exception carries a severity attribute, it should be forwarded to the event."""
    tester = _make_tester_mock()

    class ErrorWithSeverity(Exception):
        severity = Severity.WARNING

    @teardown_on_exception
    def fake_setup(self):
        raise ErrorWithSeverity("Warning-level error")

    with patch("sdcm.tester.TestFrameworkEvent") as mock_event_cls:
        mock_event_cls.return_value.publish_or_dump = MagicMock()
        with pytest.raises(ErrorWithSeverity):
            fake_setup(tester)
        mock_event_cls.assert_called_once()
        _, kwargs = mock_event_cls.call_args
        assert kwargs["severity"] == Severity.WARNING
