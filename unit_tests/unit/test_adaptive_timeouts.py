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
import json
import logging
import time
import uuid
from typing import Any
from unittest import mock

import pytest
from invoke import Result

from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_config import AdaptiveTimeoutMultipliers, SCTConfiguration
from sdcm.utils.adaptive_timeouts.load_info_store import (
    AdaptiveTimeoutStore,
    NodeLoadInfoService,
    NodeLoadInfoServices,
    _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC,
    _I4I_LARGE_SHARD_COUNT,
)
from sdcm.utils.adaptive_timeouts import (
    _STREAMING_OVERHEAD,
    _get_operation_timeout_factor,
    Operations,
    adaptive_timeout,
    TABLETS_HARD_TIMEOUT,
)
from unit_tests.lib.fake_cluster import DummyDbCluster

LOGGER = logging.getLogger(__name__)


class FakeNode:
    def __init__(self, name: str, remoter):
        self.name = name
        self.remoter = remoter
        self.scylla_version_detailed = "2042.1.12-0.20220620.e23889f17"
        params = SCTConfiguration()
        params["n_db_nodes"] = 1
        self.parent_cluster = DummyDbCluster(nodes=[self], params=params)


class MemoryAdaptiveTimeoutStore(AdaptiveTimeoutStore):
    def __init__(self):
        self._data = {}

    def store(
        self, metrics: dict[str, Any], operation: str, duration: int, timeout: int, timeout_occurred: bool
    ) -> None:
        metrics.update(
            {"operation": operation, "duration": duration, "timeout": timeout, "timeout_occurred": timeout_occurred}
        )
        key = str(uuid.uuid4())
        self._data[key] = metrics

    def get(self, operation: str | None, timeout_occurred: bool = False):
        matching_items = []
        for item in self._data.values():
            if (operation is None or item["operation"] == operation) and item["timeout_occurred"] == timeout_occurred:
                matching_items.append(item)
        return matching_items

    def clear(self):
        self._data = {}


@pytest.fixture
def adaptive_timeout_remoter(fake_remoter):
    io_properties = """
       disks:
         - mountpoint: /var/lib/scylla
           read_iops: 540317
           read_bandwidth: 1914761472
           write_iops: 300319
           write_bandwidth: 116202240
    """
    scylla_metrics = """
scylla_lsa_free_space{shard="0"} 3749052416.000000
scylla_lsa_free_space{shard="1"} 3750100992.000000
scylla_lsa_free_space{shard="2"} 3750887424.000000
    """
    # 107374182400 bytes == 102400 MB == 100 GB
    node_exporter_metrics = """
node_load1 1.20
node_load5 2.30
node_load15 1.60
node_boot_time_seconds 1678343052.0
node_filesystem_size_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/var/lib/scylla"} 107374182400.0
node_filesystem_avail_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/var/lib/scylla"} 53687091200.0
    """
    nodetool_info = """
Using /etc/scylla/scylla.yaml as the config file
ID                     : aa7409d6-4129-4e6a-96f8-e571abdabe7c
Gossip active          : true
Thrift active          : true
Native Transport active: true
Load                   : 100.00 GB
Generation No          : 1678343052
Uptime (seconds)       : 35061
Heap Memory (MB)       : 40.31 / 247.50
Off Heap Memory (MB)   : 4.76
Data Center            : datacenter1
Rack                   : rack1
Exceptions             : 0
Key Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, 0.000 recent hit rate, 0 save period in seconds
Row Cache              : entries 72, size 72 bytes, capacity 477.08 KiB, 4402 hits, 5043 requests, 0.873 recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, 0.000 recent hit rate, 0 save period in seconds
Percent Repaired       : 0.0%
Token                  : (invoke with -T/--tokens to see all 256 tokens)
"""
    fake_remoter.result_map = {
        r"cat /etc/scylla.d/io_properties.yaml": Result(stdout=io_properties, stderr="", exited=0),
        r"curl -s localhost:9180/metrics": Result(stdout=scylla_metrics, exited=0),
        r"curl -s localhost:9100/metrics": Result(stdout=node_exporter_metrics, exited=0),
        r"nodetool info": Result(stdout=nodetool_info, exited=0),
        r"uptime": Result(stdout=" 10:00:00 up 1 day,  1:00,  1 user,  load average: 1.20, 2.30, 1.60", exited=0),
        r"cat /etc/scylla/scylla.yaml": Result(stdout="", exited=0),
    }
    remoter = RemoteCmdRunnerBase.create_remoter("test-node-host")
    yield remoter
    # Cleanup: stop the remoter to prevent any background threads
    if hasattr(remoter, "stop"):
        remoter.stop()


@pytest.fixture
def fake_node(adaptive_timeout_remoter):
    return FakeNode("test-node", adaptive_timeout_remoter)


@pytest.fixture
def adaptive_timeout_store():
    store = MemoryAdaptiveTimeoutStore()
    store.clear()
    return store


@pytest.fixture(autouse=True)
def mock_tablets_feature():
    with mock.patch("sdcm.utils.adaptive_timeouts.is_tablets_feature_enabled") as mock_feature:
        mock_feature.return_value = False
        yield mock_feature


@pytest.fixture(autouse=True)
def clear_node_load_info_services_singleton():
    """Clear the NodeLoadInfoServices Singleton cache after each test to prevent cross-test pollution."""
    yield
    NodeLoadInfoServices()._services.clear()


@mock.patch("sdcm.sct_events.base.SctEvent.publish_or_dump")
def test_soft_timeout_is_raised_when_timeout_reached(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(
        operation=Operations.SOFT_TIMEOUT, node=fake_node, timeout=0.1, stats_storage=adaptive_timeout_store
    ) as timeout:
        assert timeout == 0.1
        time.sleep(0.2)
    publish_or_dump.assert_called_once()
    metrics = adaptive_timeout_store.get(operation=Operations.SOFT_TIMEOUT.name, timeout_occurred=True)
    assert metrics[0]["duration"] > 0.2
    assert metrics[0]["timeout"] == 0.1
    assert metrics[0]["timeout_occurred"] is True
    assert metrics[0]["operation"] == "SOFT_TIMEOUT"
    assert metrics[0]["node_name"] == "test-node"
    assert metrics[0]["shards_count"] == 3


@mock.patch("sdcm.sct_events.base.SctEvent.publish_or_dump")
def test_soft_timeout_is_not_raised_when_timeout_not_reached(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(
        operation=Operations.SOFT_TIMEOUT, node=fake_node, timeout=1, stats_storage=adaptive_timeout_store
    ) as timeout:
        assert timeout == 1
        time.sleep(0.2)
    publish_or_dump.assert_not_called()
    # still we store the metrics even when there's no timeout
    assert adaptive_timeout_store.get(operation="SOFT_TIMEOUT", timeout_occurred=False)


@mock.patch("sdcm.sct_events.base.SctEvent.publish_or_dump")
def test_decommission_timeout_is_calculated_and_stored(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(
        operation=Operations.DECOMMISSION, node=fake_node, stats_storage=adaptive_timeout_store
    ) as timeout:
        assert timeout == 7200  # based on data size
    publish_or_dump.assert_not_called()
    assert adaptive_timeout_store.get(operation=Operations.DECOMMISSION.name, timeout_occurred=False)


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_tablets_decommission_timeout_is_calculated_from_data_size(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store, mock_tablets_feature
):
    mock_tablets_feature.return_value = True
    with adaptive_timeout(
        operation=Operations.DECOMMISSION, node=fake_node, stats_storage=adaptive_timeout_store
    ) as timeout:
        # node_data_size_mb=102400, expected_throughput=69/2*3=103.5 → estimated≈989.4s
        # hard = int(estimated) * 4 + 600 (10-minute overhead) = 4556
        throughput = _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC / _I4I_LARGE_SHARD_COUNT * 3
        estimated = int(102400 / throughput)
        expected_hard = estimated * 4 + _STREAMING_OVERHEAD
        assert timeout == expected_hard

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()
    metrics = adaptive_timeout_store.get(operation=Operations.DECOMMISSION.name)
    assert metrics[0].get("tablets_enabled") is True


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_tablets_new_node_uses_predefined_timeouts(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store, mock_tablets_feature
):
    mock_tablets_feature.return_value = True
    with adaptive_timeout(
        operation=Operations.NEW_NODE, node=fake_node, stats_storage=adaptive_timeout_store, timeout=9999
    ) as timeout:
        assert timeout == TABLETS_HARD_TIMEOUT

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()
    metrics = adaptive_timeout_store.get(operation=Operations.NEW_NODE.name)
    assert metrics[0].get("tablets_enabled") is True


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_tablets_tablet_migration_timeout_is_calculated_from_disk_size(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store, mock_tablets_feature
):
    mock_tablets_feature.return_value = True
    with adaptive_timeout(
        operation=Operations.TABLET_MIGRATION, node=fake_node, stats_storage=adaptive_timeout_store, timeout=3600
    ) as timeout:
        # node_disk_size_mb=102400, expected_throughput=69/2*3=103.5
        # estimated = 102400 * 0.9 / 103.5 ≈ 889.9s
        # hard = int(estimated) * 4 + 600 (10-minute overhead) = 4160
        throughput = _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC / _I4I_LARGE_SHARD_COUNT * 3
        estimated = int(102400 * 0.9 / throughput)
        expected_hard = estimated * 4 + _STREAMING_OVERHEAD
        assert timeout == expected_hard

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()
    metrics = adaptive_timeout_store.get(operation=Operations.TABLET_MIGRATION.name)
    assert metrics[0].get("tablets_enabled") is True


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_tablet_migration_timeout_fallback_uses_caller_timeout(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store, mock_tablets_feature
):
    mock_tablets_feature.return_value = True
    # Force metrics gathering to fail by making node_disk_size_mb raise
    with mock.patch.object(
        NodeLoadInfoService, "node_disk_size_mb", new_callable=mock.PropertyMock, side_effect=ValueError("no disk")
    ):
        with adaptive_timeout(
            operation=Operations.TABLET_MIGRATION, node=fake_node, stats_storage=adaptive_timeout_store, timeout=3600
        ) as timeout:
            assert timeout == 3600  # fallback returns caller timeout (or 6 hours if not provided)


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_tablet_migration_uses_formula_when_tablets_disabled(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store
):
    # With tablets disabled, non-tablets formula: max(int(102400 MB * 0.9 * 0.03), 7200) = max(2764, 7200) = 7200 s
    # hard_timeout is None, so yields soft_timeout directly.
    with adaptive_timeout(
        operation=Operations.TABLET_MIGRATION, node=fake_node, stats_storage=adaptive_timeout_store, timeout=7200
    ) as timeout:
        assert timeout == 7200

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()


def test_node_disk_size_mb(fake_node):
    """node_disk_size_mb should return total /var/lib/scylla filesystem size in MB.

    The fixture provides node_filesystem_size_bytes = 107374182400 bytes (100 GB = 102400 MB).
    """
    service = NodeLoadInfoService(
        remoter=fake_node.remoter,
        name=fake_node.name,
        scylla_version=fake_node.scylla_version_detailed,
        node_idx="0",
    )
    # 107374182400 bytes / 1024 / 1024 == 102400 MB
    assert service.node_disk_size_mb == 102400


def _make_service(fake_node, scylla_yaml_content: str) -> NodeLoadInfoService:
    """Helper that builds a NodeLoadInfoService with a custom scylla.yaml mock."""
    fake_node.remoter.result_map[r"cat /etc/scylla/scylla.yaml"] = Result(stdout=scylla_yaml_content, exited=0)
    return NodeLoadInfoService(
        remoter=fake_node.remoter,
        name=fake_node.name,
        scylla_version=fake_node.scylla_version_detailed,
        node_idx="0",
    )


def test_expected_throughput_uses_scylla_yaml_value(fake_node):
    """When stream_io_throughput_mb_per_sec is set and non-zero, return it directly."""
    service = _make_service(fake_node, "stream_io_throughput_mb_per_sec: 200\n")
    assert service.expected_throughput == 200.0


def test_expected_throughput_falls_back_to_estimate_when_missing(fake_node):
    """When stream_io_throughput_mb_per_sec is absent, estimate from shard count.

    The fixture has 3 shards, so the estimate is _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC / _I4I_LARGE_SHARD_COUNT * 3 == 103.5.
    """
    service = _make_service(fake_node, "cluster_name: test\n")
    # shards_count == 3 (3 scylla_lsa_free_space entries in the metrics fixture)
    assert service.expected_throughput == _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC / _I4I_LARGE_SHARD_COUNT * 3


def test_expected_throughput_falls_back_to_estimate_when_zero(fake_node):
    """When stream_io_throughput_mb_per_sec is explicitly 0, fall back to the estimate."""
    service = _make_service(fake_node, "stream_io_throughput_mb_per_sec: 0\n")
    assert service.expected_throughput == _I4I_LARGE_BASELINE_THROUGHPUT_MB_PER_SEC / _I4I_LARGE_SHARD_COUNT * 3


# ===== Adaptive timeout multipliers tests =====


@pytest.mark.parametrize(
    ("operation", "config_value", "expected"),
    [
        pytest.param(Operations.DECOMMISSION, {"decommission": 4}, 4, id="decommission"),
        pytest.param(Operations.REMOVE_NODE, {"remove_node": 3}, 3, id="remove_node"),
        pytest.param(Operations.NEW_NODE, {"new_node": 2}, 2, id="new_node"),
        pytest.param(Operations.NEW_NODE, {"decommission": 4}, 1.0, id="missing-key-defaults-to-1"),
        pytest.param(Operations.SOFT_TIMEOUT, {"decommission": 4}, 1.0, id="unconfigured-operation"),
        pytest.param(Operations.REMOVE_NODE, {}, 1.0, id="empty-config"),
    ],
)
def test_get_operation_timeout_factor(operation, config_value, expected):
    params = SCTConfiguration()
    params["adaptive_timeout_multipliers"] = config_value
    assert _get_operation_timeout_factor(params, operation) == expected


@pytest.mark.parametrize(
    "input_val,expected_key,expected_val",
    [
        pytest.param({"decommission": 4, "remove_node": 2}, "decommission", 4, id="dict-input"),
        pytest.param({"new_node": 3}, "new_node", 3, id="single-key-dict"),
        pytest.param({"decommission": 5, "new_node": 2.5}, "decommission", 5, id="multiple-keys"),
    ],
)
def test_multiplier_model_validates_input(input_val, expected_key, expected_val):
    model = AdaptiveTimeoutMultipliers.model_validate(input_val)
    assert model.root[expected_key] == expected_val


@pytest.mark.parametrize(
    "input_val,match_pattern",
    [
        pytest.param({"unknown": 2}, "Unknown operation key 'unknown'", id="unknown-key"),
        pytest.param({"decommission": -1}, "Input should be greater than 0", id="negative-value"),
        pytest.param({"decommission": 0}, "Input should be greater than 0", id="zero-value"),
    ],
)
def test_multiplier_model_rejects_invalid_input(input_val, match_pattern):
    with pytest.raises(ValueError, match=match_pattern):
        AdaptiveTimeoutMultipliers.model_validate(input_val)


@pytest.mark.parametrize(
    "multipliers,operation,timeout_arg,expected",
    [
        pytest.param({"new_node": 3}, Operations.NEW_NODE, 600, 1800, id="multiplier-scales-timeout"),
        pytest.param({"decommission": 4}, Operations.REMOVE_NODE, 600, 600, id="missing-key-unscaled"),
        pytest.param({}, Operations.DECOMMISSION, None, 7200, id="empty-config-preserves-base"),
    ],
)
@mock.patch("sdcm.sct_events.base.SctEvent.publish_or_dump")
def test_multiplier_scaling(
    publish_or_dump, fake_node, adaptive_timeout_store, multipliers, operation, timeout_arg, expected
):
    fake_node.parent_cluster.params["adaptive_timeout_multipliers"] = multipliers

    kwargs = dict(operation=operation, node=fake_node, stats_storage=adaptive_timeout_store)
    if timeout_arg is not None:
        kwargs["timeout"] = timeout_arg

    with adaptive_timeout(**kwargs) as timeout:
        assert timeout == expected


@mock.patch("sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump")
@mock.patch("sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump")
def test_decommission_multiplier_scales_hard_timeout_for_tablets(
    hard_timeout_mock, soft_timeout_mock, fake_node, adaptive_timeout_store, mock_tablets_feature
):
    mock_tablets_feature.return_value = True

    # Get base timeout without multiplier
    fake_node.parent_cluster.params["adaptive_timeout_multipliers"] = {}
    with adaptive_timeout(
        operation=Operations.DECOMMISSION, node=fake_node, stats_storage=adaptive_timeout_store
    ) as base_timeout:
        pass

    # Apply multiplier and verify scaling
    fake_node.parent_cluster.params["adaptive_timeout_multipliers"] = {"decommission": 2}
    with adaptive_timeout(
        operation=Operations.DECOMMISSION, node=fake_node, stats_storage=adaptive_timeout_store
    ) as scaled_timeout:
        assert scaled_timeout == base_timeout * 2


@pytest.mark.parametrize(
    "env_value,expected_field,expected_val",
    [
        pytest.param("{'decommission': 4, 'new_node': 3}", "decommission", 4, id="python-dict-string"),
        pytest.param('{"remove_node": 5}', "remove_node", 5, id="json-string"),
        pytest.param(None, None, None, id="unset-env-uses-empty-default"),
    ],
)
def test_multiplier_env_var_loaded_by_sct_config(monkeypatch, env_value, expected_field, expected_val):
    """SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS env var is parsed into the model; unset uses empty dict default."""
    if env_value is not None:
        monkeypatch.setenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", env_value)
    else:
        monkeypatch.delenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", raising=False)
    config = SCTConfiguration()
    multipliers = config.get("adaptive_timeout_multipliers")
    if expected_field is not None:
        assert multipliers.root[expected_field] == expected_val
    else:
        # When env is unset, defaults/test_default.yaml provides {} which
        # becomes an empty AdaptiveTimeoutMultipliers
        assert multipliers.root == {}


@pytest.mark.parametrize(
    "env_value,match_pattern",
    [
        pytest.param("", "failed to parse SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", id="empty-string"),
        pytest.param("   ", "failed to parse SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", id="whitespace-only"),
        pytest.param("not_a_dict", "failed to parse SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", id="plain-string"),
        pytest.param("[1, 2, 3]", "isn't a dict, str or Pydantic model", id="list-value"),
        pytest.param("{'unknown_op': 2}", "Unknown operation key 'unknown_op'", id="invalid-operation-key"),
        pytest.param("{'decommission': -1}", "Input should be greater than 0", id="negative-multiplier"),
        pytest.param("{'decommission': 0}", "Input should be greater than 0", id="zero-multiplier"),
    ],
)
def test_multiplier_invalid_env_var_raises(monkeypatch, env_value, match_pattern):
    """SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS with invalid values is rejected during config init."""
    monkeypatch.setenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", env_value)
    with pytest.raises(Exception, match=match_pattern):
        SCTConfiguration()


@pytest.mark.parametrize(
    "env_vars,expected_multipliers",
    [
        pytest.param(
            {"SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission": "4"},
            {"decommission": 4.0, "repair": 1.0},
            id="single-operation",
        ),
        pytest.param(
            {"SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission": "4", "SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.new_node": "3"},
            {"decommission": 4.0, "new_node": 3.0, "repair": 1.0},
            id="multiple-operations",
        ),
        pytest.param(
            {"SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.remove_node": "2.5"},
            {"remove_node": 2.5, "decommission": 1.0},
            id="float-value",
        ),
    ],
)
def test_multiplier_dot_notation_env_var(monkeypatch, env_vars, expected_multipliers):
    """SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.operation=value dot-notation is parsed into the model.

    This mirrors the pattern used by SCT_STRESS_IMAGE.cassandra-stress=... in pipelines.
    Values arrive as strings and are coerced to floats by Pydantic.
    """
    monkeypatch.delenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", raising=False)
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    config = SCTConfiguration()
    multipliers = config.get("adaptive_timeout_multipliers")
    for operation, expected in expected_multipliers.items():
        assert multipliers.get_multiplier(operation) == expected


def test_sct_config_with_multipliers_is_json_serializable(monkeypatch):
    """Regression test: SCTConfiguration with adaptive_timeout_multipliers must be
    JSON-serializable via model_dump_json().

    Previously dict(config) + json.dumps() raised:
        ValueError: Circular reference detected
        when serializing method object
        when serializing sdcm.sct_config.AdaptiveTimeoutMultipliers object
        when serializing dict item 'adaptive_timeout_multipliers'

    The fix uses model_dump_json() which properly serializes nested Pydantic
    RootModel instances. See test run efd56534 (TabletSplitMergeTest, Python 3.14).
    """
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS", "{'decommission': 5, 'new_node': 5}")

    config = SCTConfiguration()

    # This is what tester.py init_argus_run() now does — must not raise
    json_str = config.model_dump_json()
    assert json_str  # non-empty

    # Verify the multipliers are present and correctly serialized in the output
    parsed = json.loads(json_str)
    assert parsed["adaptive_timeout_multipliers"] == {"decommission": 5.0, "new_node": 5.0}
