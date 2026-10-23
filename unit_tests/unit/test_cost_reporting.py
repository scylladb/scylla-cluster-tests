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

"""What SCT sends to Argus about a run's cost, and when it sends nothing."""

import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from sdcm.cluster import BaseNode
from sdcm.cluster_gce import GCENode
from sdcm.provision.oci.virtual_machine_provider import VirtualMachineProvider
from sdcm.utils.cloud_catalog.cost import get_hourly_rate
from utils import cloud_cleanup
from sdcm.provision.gce.utils import tags_to_gce_labels
from sdcm.utils.cloud_catalog.cost import InstanceRate, RunCostEstimate
from sdcm.utils.cost_reporting import (
    PRICE_TAG,
    PRICING_TIER_TAG,
    instance_cost_item,
    cost_item_from_tags,
    price_tags,
    rate_from_tags,
    report_cost_items,
    report_estimated_cost,
    report_costs_from_tags,
)


class _FakeArgus:
    run_id = "6c1fbb6f-0000-4000-8000-000000000001"

    def __init__(self, error: Exception | None = None):
        self.estimates: list[tuple[str, float]] = []
        self.items: list = []
        self._error = error

    def set_estimated_cost(self, run_id, value):
        if self._error:
            raise self._error
        self.estimates.append((run_id, value))

    def submit_cost_items(self, run_id, items):
        if self._error:
            raise self._error
        self.items.extend(items)


def _estimate(**overrides) -> RunCostEstimate:
    priced = replace(RunCostEstimate.unavailable(duration_hours=4.25, is_spot=True), total=12.2328, partial=False)
    return replace(priced, **overrides)


def test_the_estimate_is_sent_against_the_run_rounded_to_cents():
    client = _FakeArgus()
    assert report_estimated_cost(client, _estimate()) is True
    assert client.estimates == [(_FakeArgus.run_id, 12.23)]


def test_the_lifecycle_figure_is_sent_not_the_on_demand_ceiling():
    client = _FakeArgus()
    report_estimated_cost(client, _estimate(on_demand_total=20.99, fallback_to_on_demand=True))
    assert client.estimates[0][1] == 12.23


@pytest.mark.parametrize(
    "estimate",
    [
        pytest.param(RunCostEstimate.unavailable(), id="nothing-priced"),
        pytest.param(_estimate(partial=True), id="partial-floor"),
    ],
)
def test_an_unknown_or_partial_estimate_sends_nothing(estimate):
    """Argus stores any number as real, so a floor would be compared as if it were the answer."""
    client = _FakeArgus()
    assert report_estimated_cost(client, estimate) is False
    assert client.estimates == []


def test_an_argus_failure_is_logged_never_raised():
    client = _FakeArgus(error=ConnectionError("argus is down"))
    assert report_estimated_cost(client, _estimate()) is False


# --- the price written on an instance when it is created ---------------------------------


def _rate(price, is_spot=False) -> InstanceRate:
    return InstanceRate.from_raw(price, is_spot=is_spot)


def test_price_tags_round_trip():
    tags = price_tags(_rate(0.4397333, is_spot=True))
    assert tags == {PRICE_TAG: "439733", PRICING_TIER_TAG: "spot"}
    assert rate_from_tags(tags) == (0.439733, "spot")


def test_price_tags_survive_gce_label_normalisation():
    """GCE lowercases keys and restricts values; the price tags must come through unchanged."""
    tags = price_tags(_rate(1.373))
    assert tags_to_gce_labels(tags) == tags


def test_an_unknown_rate_writes_no_tags():
    """No tag reads back as unknown; a zero tag would be reported as a real, free instance."""
    assert price_tags(InstanceRate.unknown()) == {}


@pytest.mark.parametrize("tags", [None, {}, {PRICE_TAG: "0"}, {PRICE_TAG: "abc"}, {"Name": "db-1"}])
def test_missing_or_unusable_price_tags_read_as_unknown(tags):
    assert rate_from_tags(tags) is None


# --- turning a price into a final cost --------------------------------------------------

START = datetime(2026, 10, 4, 8, 0, tzinfo=UTC)


def test_cost_is_price_times_hours_run():
    item = instance_cost_item("db-1", "scylla-db", 1.5, "on-demand", START, ended_at=START + timedelta(hours=2))
    assert (item.name, item.category, item.cost, item.pricing_tier, item.leaked) == (
        "db-1",
        "db_node",
        3.0,
        "on-demand",
        False,
    )


@pytest.mark.parametrize(
    "node_type, category",
    [
        ("zero-token-db", "zero_db_node"),
        ("scylla-db", "db_node"),
        ("db", "db_node"),
        ("oracle-db", "oracle_db_node"),
        ("loader", "loader"),
        (None, "unknown"),
    ],
)
def test_categories(node_type, category):
    assert instance_cost_item("n", node_type, 1.0, None, START, ended_at=START).category == category


def test_a_start_after_the_end_sends_nothing():
    assert instance_cost_item("db-1", "db", 1.0, None, START, ended_at=START - timedelta(minutes=1)) is None


def test_a_naive_start_is_read_as_utc():
    item = instance_cost_item("db-1", "db", 1.0, None, START.replace(tzinfo=None), ended_at=START + timedelta(hours=1))
    assert item.cost == 1.0


def test_cleanup_prices_a_leaked_instance_from_its_tags():
    tags = {"NodeType": "loader", **price_tags(_rate(0.5, is_spot=True))}
    item = cost_item_from_tags("loader-1", tags, datetime.now(tz=UTC) - timedelta(hours=4), leaked=True)
    assert item.leaked is True
    assert item.category == "loader"
    assert item.pricing_tier == "spot"
    assert item.cost == pytest.approx(2.0, rel=1e-3)


@pytest.mark.parametrize("key", ["ZeroTokenNode", "zerotokennode"], ids=["aws-tag", "gce-label"])
def test_cleanup_prices_a_zero_token_node_in_its_own_category(key):
    tags = {"NodeType": "scylla-db", key: "True", **price_tags(_rate(1.0))}
    assert cost_item_from_tags("db-1", tags, START, leaked=False).category == "zero_db_node"


def test_cleanup_reads_the_node_type_from_lowercased_gce_labels():
    labels = tags_to_gce_labels({"NodeType": "scylla-db", **price_tags(_rate(1.0))})
    assert cost_item_from_tags("db-1", labels, START, leaked=True).category == "db_node"


@pytest.mark.parametrize(
    "tags, started_at",
    [({"NodeType": "loader"}, START), (price_tags(_rate(1.0)), None)],
    ids=["created-before-price-tags", "no-launch-time"],
)
def test_cleanup_sends_nothing_it_cannot_price(tags, started_at):
    assert cost_item_from_tags("n", tags, started_at, leaked=True) is None


def test_cleanup_accepts_gce_string_launch_times():
    item = cost_item_from_tags("db-1", price_tags(_rate(1.0)), "2026-10-04T01:00:00.000-07:00", leaked=False)
    assert item.cost > 0


def test_cleanup_cost_reporting_never_raises_into_a_deletion():
    """It runs between deleting an instance and recording the deletion; it must not stop either."""
    client = _FakeArgus()
    assert report_costs_from_tags(client, [("db-1", price_tags(_rate(1.0)), object())], leaked=False) is False
    assert client.items == []


@pytest.mark.parametrize("leaked", [False, True], ids=["job-cleanup-stage", "scheduled-sweep"])
def test_cleanup_reports_each_priced_instance_with_the_callers_leaked_flag(leaked):
    """Only the scheduled sweep marks cost as leaked; a job's own cleanup is a normal teardown."""
    client = _FakeArgus()
    tags = {"NodeType": "loader", **price_tags(_rate(1.0))}
    report_costs_from_tags(client, [("loader-1", tags, START), ("old-node", {"NodeType": "loader"}, START)], leaked)
    assert [(i.name, i.leaked) for i in client.items] == [("loader-1", leaked)]


def test_the_scheduled_sweep_reports_cost_as_leaked(monkeypatch):
    client = _FakeArgus()
    client.terminate_resource = lambda name, reason: None
    monkeypatch.setattr(cloud_cleanup, "argus_client_factory", lambda: lambda test_id: client)
    tags = {"NodeType": "scylla-db", **price_tags(_rate(1.0))}
    cloud_cleanup.update_argus_resource_status("test-id", "db-1", "terminate", tags, START)
    assert [(i.name, i.leaked) for i in client.items] == [("db-1", True)]


def test_report_cost_items_drops_unpriced_items_and_sends_the_rest():
    client = _FakeArgus()
    priced = instance_cost_item("db-1", "db", 1.0, None, START, ended_at=START)
    assert report_cost_items(client, [None, priced]) is True
    assert client.items == [priced]
    assert report_cost_items(client, [None]) is False


def test_report_cost_items_never_raises():
    client = _FakeArgus(error=ConnectionError("argus is down"))
    assert report_cost_items(client, [instance_cost_item("n", "db", 1.0, None, START, ended_at=START)]) is False


# --- a node prices and tags itself, and reports at teardown -----------------------------


def _node(backend="aws", instance_type="i4i.4xlarge", is_spot=True, add_tags=None):
    tagged = {}
    return SimpleNamespace(
        parent_cluster=SimpleNamespace(params={"cluster_backend": backend}, node_type="scylla-db"),
        _instance_type=instance_type,
        vm_region="eu-west-1",
        is_spot=is_spot,
        name="db-node-1",
        hourly_rate=None,
        _created_at=datetime.now(tz=UTC) - timedelta(hours=2),
        log=logging.getLogger("test-node"),
        _add_tags=add_tags or tagged.update,
        tagged=tagged,
        _is_zero_token_node=False,
    )


@pytest.fixture(name="priced_at")
def fixture_priced_at(monkeypatch):
    calls = []

    def fake_rate(cloud, region, instance_type, is_spot=False, catalog_only=False):
        calls.append((cloud, region, instance_type, is_spot))
        return _rate(0.5, is_spot=is_spot)

    monkeypatch.setattr("sdcm.cluster.get_hourly_rate", fake_rate)
    return calls


def test_a_node_is_priced_at_the_lifecycle_it_got_and_tagged(priced_at):
    node = _node(is_spot=False)
    BaseNode._record_hourly_rate(node)
    assert priced_at == [("aws", "eu-west-1", "i4i.4xlarge", False)]
    assert node.tagged == {PRICE_TAG: "500000", PRICING_TIER_TAG: "on-demand"}


@pytest.mark.parametrize("backend, instance_type", [("docker", "i4i.4xlarge"), ("aws", "N/A")])
def test_a_node_with_nothing_to_price_is_left_alone(priced_at, backend, instance_type):
    node = _node(backend=backend, instance_type=instance_type)
    BaseNode._record_hourly_rate(node)
    assert priced_at == []
    assert node.hourly_rate is None


def test_a_node_that_cannot_be_tagged_still_keeps_its_rate(priced_at):
    def refuse(_tags):
        raise RuntimeError("tag namespace not defined")

    node = _node(add_tags=refuse)
    BaseNode._record_hourly_rate(node)
    assert node.hourly_rate.price_per_hour == 0.5


def test_teardown_reports_the_node_cost(priced_at):
    node = _node()
    BaseNode._record_hourly_rate(node)
    client = _FakeArgus()
    BaseNode._report_cost_in_argus(node, client)
    [item] = client.items
    assert (item.name, item.category, item.pricing_tier, item.leaked) == ("db-node-1", "db_node", "spot", False)
    assert item.cost == pytest.approx(1.0, rel=1e-3)


def test_teardown_of_an_unpriced_node_sends_nothing():
    client = _FakeArgus()
    BaseNode._report_cost_in_argus(_node(), client)
    assert client.items == []


def test_teardown_reports_a_zero_token_node_in_its_own_category(priced_at):
    node = _node()
    node._is_zero_token_node = True
    BaseNode._record_hourly_rate(node)
    client = _FakeArgus()
    BaseNode._report_cost_in_argus(node, client)
    assert [i.category for i in client.items] == ["zero_db_node"]


# --- the instance details pricing depends on --------------------------------------------


def _oci_instance(shape, ocpus):
    return SimpleNamespace(
        id="ocid1.instance.oc1..test",
        display_name="db-node-1",
        region="iad",
        shape=shape,
        shape_config=SimpleNamespace(ocpus=ocpus),
        preemptible_instance_config=None,
        defined_tags={},
        image_id="ocid1.image.oc1..test",
        time_created=START,
    )


def test_an_oci_flex_node_is_typed_with_its_ocpu_count_so_it_can_be_priced():
    """The catalog keys Flex shapes as <shape>:<ocpus>; the bare shape name prices as unknown."""
    provider = SimpleNamespace(
        get_ip_address=lambda *_a, **_k: "10.0.0.1", get_private_dns_name=lambda _id: "", _region="us-ashburn-1"
    )
    vm = VirtualMachineProvider.convert_to_vm_instance(provider, _oci_instance("VM.Standard.E4.Flex", 16.0), None)
    assert vm.instance_type == "VM.Standard.E4.Flex:16"
    assert get_hourly_rate("oci", "us-ashburn-1", vm.instance_type).known


def test_a_fixed_oci_shape_keeps_its_name():
    provider = SimpleNamespace(
        get_ip_address=lambda *_a, **_k: "10.0.0.1", get_private_dns_name=lambda _id: "", _region="us-ashburn-1"
    )
    vm = VirtualMachineProvider.convert_to_vm_instance(provider, _oci_instance("BM.DenseIO.E4.128", 128.0), None)
    assert vm.instance_type == "BM.DenseIO.E4.128"


@pytest.mark.parametrize(
    "provisioning_model, preemptible, expected",
    [("SPOT", False, True), ("STANDARD", True, True), ("STANDARD", False, False)],
    ids=["spot-vm", "legacy-preemptible", "on-demand"],
)
def test_gce_spot_is_read_from_the_provisioning_model(provisioning_model, preemptible, expected):
    """SCT requests GCE spot through the provisioning model only, never the legacy flag."""
    node = SimpleNamespace(
        _instance=SimpleNamespace(
            scheduling=SimpleNamespace(provisioning_model=provisioning_model, preemptible=preemptible)
        )
    )
    assert GCENode.is_spot.fget(node) is expected
