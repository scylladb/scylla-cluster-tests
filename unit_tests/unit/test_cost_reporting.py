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
from sdcm.provision.gce.utils import tags_to_gce_labels
from sdcm.provision.oci.virtual_machine_provider import VirtualMachineProvider
from sdcm.utils.cloud_catalog.cost import InstanceRate, RunCostEstimate, get_hourly_rate
from sdcm.utils.cost_reporting import (
    PRICE_TAG,
    PRICING_TIER_TAG,
    cost_item_from_tags,
    instance_cost_item,
    price_tags,
    rate_from_tags,
    report_cost_items,
    report_costs_from_tags,
    report_estimated_cost,
)
from utils import cloud_cleanup

START = datetime(2026, 10, 4, 8, 0, tzinfo=UTC)


@pytest.fixture
def priced_at(monkeypatch):
    """Price every rate lookup at 0.5 per hour; return the list of lookups made."""
    calls = []

    def fake_rate(cloud, region, instance_type, is_spot=False, catalog_only=False):
        calls.append((cloud, region, instance_type, is_spot))
        return make_rate(0.5, is_spot=is_spot)

    monkeypatch.setattr("sdcm.cluster.get_hourly_rate", fake_rate)
    return calls


class FakeArgus:
    """Argus client double that records cost estimates and items, or raises the given error on every call."""

    run_id = "6c1fbb6f-0000-4000-8000-000000000001"

    def __init__(self, error: Exception | None = None):
        """Start with nothing recorded."""
        self.estimates: list[tuple[str, float]] = []
        self.items: list = []
        self._error = error

    def set_estimated_cost(self, run_id, value):
        """Record the estimate, or raise the configured error."""
        if self._error:
            raise self._error
        self.estimates.append((run_id, value))

    def submit_cost_items(self, run_id, items):
        """Record the cost items, or raise the configured error."""
        if self._error:
            raise self._error
        self.items.extend(items)


def make_estimate(**overrides) -> RunCostEstimate:
    """A priced 4.25-hour spot estimate of 12.2328, with the overrides applied."""
    priced = replace(RunCostEstimate.unavailable(duration_hours=4.25, is_spot=True), total=12.2328, partial=False)
    return replace(priced, **overrides)


def make_rate(price, is_spot=False) -> InstanceRate:
    """An InstanceRate for the given raw price per hour."""
    return InstanceRate.from_raw(price, is_spot=is_spot)


def make_node(backend="aws", instance_type="i4i.4xlarge", is_spot=True, add_tags=None):
    """A stand-in for a BaseNode created two hours ago; its tags are captured in ``tagged``."""
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


def make_oci_instance(shape, ocpus):
    """An OCI instance SimpleNamespace of the given shape and OCPU count."""
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


def test_the_estimate_is_sent_against_the_run_rounded_to_cents():
    """The estimate goes to Argus rounded to cents against the run id."""
    client = FakeArgus()
    assert report_estimated_cost(client, make_estimate()) is True
    assert client.estimates == [(FakeArgus.run_id, 12.23)]


def test_the_lifecycle_figure_is_sent_not_the_on_demand_ceiling():
    """The lifecycle cost, not the on-demand ceiling, is what Argus receives."""
    client = FakeArgus()
    report_estimated_cost(client, make_estimate(on_demand_total=20.99, fallback_to_on_demand=True))
    assert client.estimates[0][1] == 12.23


@pytest.mark.parametrize(
    "estimate",
    [
        pytest.param(RunCostEstimate.unavailable(), id="nothing-priced"),
        pytest.param(make_estimate(partial=True), id="partial-floor"),
    ],
)
def test_an_unknown_or_partial_estimate_sends_nothing(estimate):
    """Argus stores any number as real, so a floor would be compared as if it were the answer."""
    client = FakeArgus()
    assert report_estimated_cost(client, estimate) is False
    assert client.estimates == []


def test_an_argus_failure_is_logged_never_raised():
    """An Argus error is logged and reported as False, never raised."""
    client = FakeArgus(error=ConnectionError("argus is down"))
    assert report_estimated_cost(client, make_estimate()) is False


# --- the price written on an instance when it is created ---------------------------------


def test_price_tags_round_trip():
    """Price tags hold micro-dollars per hour and read back to the same rate and tier."""
    tags = price_tags(make_rate(0.4397333, is_spot=True))
    assert tags == {PRICE_TAG: "439733", PRICING_TIER_TAG: "spot"}
    assert rate_from_tags(tags) == (0.439733, "spot")


def test_price_tags_survive_gce_label_normalisation():
    """GCE lowercases keys and restricts values; the price tags must come through unchanged."""
    tags = price_tags(make_rate(1.373))
    assert tags_to_gce_labels(tags) == tags


def test_an_unknown_rate_writes_no_tags():
    """No tag reads back as unknown; a zero tag would be reported as a real, free instance."""
    assert price_tags(InstanceRate.unknown()) == {}


@pytest.mark.parametrize(
    "tags",
    [
        pytest.param(None, id="no-tags"),
        pytest.param({}, id="empty-tags"),
        pytest.param({PRICE_TAG: "0"}, id="zero-price"),
        pytest.param({PRICE_TAG: "abc"}, id="non-numeric-price"),
        pytest.param({"Name": "db-1"}, id="no-price-tag"),
    ],
)
def test_missing_or_unusable_price_tags_read_as_unknown(tags):
    """Missing, zero, or non-numeric price tags read as an unknown rate."""
    assert rate_from_tags(tags) is None


# --- turning a price into a final cost --------------------------------------------------


def test_cost_is_price_times_hours_run():
    """The cost is the rate times the hours between start and end."""
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
        pytest.param("zero-token-db", "zero_db_node", id="zero-token-db"),
        pytest.param("scylla-db", "db_node", id="scylla-db"),
        pytest.param("db", "db_node", id="db"),
        pytest.param("oracle-db", "oracle_db_node", id="oracle-db"),
        pytest.param("loader", "loader", id="loader"),
        pytest.param(None, "unknown", id="no-node-type"),
    ],
)
def test_categories(node_type, category):
    """Node type maps to its cost category; no node type maps to unknown."""
    assert instance_cost_item("n", node_type, 1.0, None, START, ended_at=START).category == category


def test_a_start_after_the_end_sends_nothing():
    """An end before the start yields no cost item."""
    assert instance_cost_item("db-1", "db", 1.0, None, START, ended_at=START - timedelta(minutes=1)) is None


def test_a_naive_start_is_read_as_utc():
    """A naive start time is treated as UTC."""
    item = instance_cost_item("db-1", "db", 1.0, None, START.replace(tzinfo=None), ended_at=START + timedelta(hours=1))
    assert item.cost == 1.0


def test_cleanup_prices_a_leaked_instance_from_its_tags():
    """A leaked instance is priced from its tags and flagged as leaked."""
    tags = {"NodeType": "loader", **price_tags(make_rate(0.5, is_spot=True))}
    item = cost_item_from_tags("loader-1", tags, datetime.now(tz=UTC) - timedelta(hours=4), leaked=True)
    assert item.leaked is True
    assert item.category == "loader"
    assert item.pricing_tier == "spot"
    assert item.cost == pytest.approx(2.0, rel=1e-3)


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("ZeroTokenNode", id="aws-tag"),
        pytest.param("zerotokennode", id="gce-label"),
    ],
)
def test_cleanup_prices_a_zero_token_node_in_its_own_category(key):
    """A zero-token node is priced into the zero_db_node category for both tag spellings."""
    tags = {"NodeType": "scylla-db", key: "True", **price_tags(make_rate(1.0))}
    assert cost_item_from_tags("db-1", tags, START, leaked=False).category == "zero_db_node"


def test_cleanup_reads_the_node_type_from_lowercased_gce_labels():
    """GCE lowercased labels still resolve the node type to its category."""
    labels = tags_to_gce_labels({"NodeType": "scylla-db", **price_tags(make_rate(1.0))})
    assert cost_item_from_tags("db-1", labels, START, leaked=True).category == "db_node"


@pytest.mark.parametrize(
    "tags, started_at",
    [
        pytest.param({"NodeType": "loader"}, START, id="created-before-price-tags"),
        pytest.param(price_tags(make_rate(1.0)), None, id="no-launch-time"),
    ],
)
def test_cleanup_sends_nothing_it_cannot_price(tags, started_at):
    """Instances without a launch time or price tags are not priced."""
    assert cost_item_from_tags("n", tags, started_at, leaked=True) is None


def test_cleanup_accepts_gce_string_launch_times():
    """GCE string launch times are parsed and priced."""
    item = cost_item_from_tags("db-1", price_tags(make_rate(1.0)), "2026-10-04T01:00:00.000-07:00", leaked=False)
    assert item.cost > 0


def test_cleanup_cost_reporting_never_raises_into_a_deletion():
    """It runs between deleting an instance and recording the deletion; it must not stop either."""
    client = FakeArgus()
    assert report_costs_from_tags(client, [("db-1", price_tags(make_rate(1.0)), object())], leaked=False) is False
    assert client.items == []


@pytest.mark.parametrize(
    "leaked",
    [
        pytest.param(False, id="job-cleanup-stage"),
        pytest.param(True, id="scheduled-sweep"),
    ],
)
def test_cleanup_reports_each_priced_instance_with_the_callers_leaked_flag(leaked):
    """Only the scheduled sweep marks cost as leaked; a job's own cleanup is a normal teardown."""
    client = FakeArgus()
    tags = {"NodeType": "loader", **price_tags(make_rate(1.0))}
    report_costs_from_tags(client, [("loader-1", tags, START), ("old-node", {"NodeType": "loader"}, START)], leaked)
    assert [(i.name, i.leaked) for i in client.items] == [("loader-1", leaked)]


def test_the_scheduled_sweep_reports_cost_as_leaked(monkeypatch):
    """The scheduled sweep reports the cost of a terminated instance as leaked."""
    client = FakeArgus()
    client.terminate_resource = lambda name, reason: None
    monkeypatch.setattr(cloud_cleanup, "argus_client_factory", lambda: lambda test_id: client)
    tags = {"NodeType": "scylla-db", **price_tags(make_rate(1.0))}
    cloud_cleanup.update_argus_resource_status("test-id", "db-1", "terminate", tags, START)
    assert [(i.name, i.leaked) for i in client.items] == [("db-1", True)]


def test_report_cost_items_drops_unpriced_items_and_sends_the_rest():
    """Unpriced items are dropped and the priced rest is submitted."""
    client = FakeArgus()
    priced = instance_cost_item("db-1", "db", 1.0, None, START, ended_at=START)
    assert report_cost_items(client, [None, priced]) is True
    assert client.items == [priced]
    assert report_cost_items(client, [None]) is False


def test_report_cost_items_never_raises():
    """A failing Argus call is swallowed when reporting cost items."""
    client = FakeArgus(error=ConnectionError("argus is down"))
    assert report_cost_items(client, [instance_cost_item("n", "db", 1.0, None, START, ended_at=START)]) is False


# --- a node prices and tags itself, and reports at teardown -----------------------------


def test_a_node_is_priced_at_the_lifecycle_it_got_and_tagged(priced_at):
    """A node records the rate for its lifecycle and writes it as tags."""
    node = make_node(is_spot=False)
    BaseNode._record_hourly_rate(node)
    assert priced_at == [("aws", "eu-west-1", "i4i.4xlarge", False)]
    assert node.tagged == {PRICE_TAG: "500000", PRICING_TIER_TAG: "on-demand"}


@pytest.mark.parametrize(
    "backend, instance_type",
    [
        pytest.param("docker", "i4i.4xlarge", id="docker-backend"),
        pytest.param("aws", "N/A", id="no-instance-type"),
    ],
)
def test_a_node_with_nothing_to_price_is_left_alone(priced_at, backend, instance_type):
    """Nodes on docker or without an instance type are not priced."""
    node = make_node(backend=backend, instance_type=instance_type)
    BaseNode._record_hourly_rate(node)
    assert priced_at == []
    assert node.hourly_rate is None


def test_a_node_that_cannot_be_tagged_still_keeps_its_rate(priced_at):
    """A node whose tagging fails keeps its in-memory rate."""

    def refuse(_tags):
        raise RuntimeError("tag namespace not defined")

    node = make_node(add_tags=refuse)
    BaseNode._record_hourly_rate(node)
    assert node.hourly_rate.price_per_hour == 0.5


def test_teardown_reports_the_node_cost(priced_at):
    """Teardown reports the node's cost in the db_node category."""
    node = make_node()
    BaseNode._record_hourly_rate(node)
    client = FakeArgus()
    BaseNode._report_cost_in_argus(node, client)
    [item] = client.items
    assert (item.name, item.category, item.pricing_tier, item.leaked) == ("db-node-1", "db_node", "spot", False)
    assert item.cost == pytest.approx(1.0, rel=1e-3)


def test_teardown_of_an_unpriced_node_sends_nothing():
    """Teardown of an unpriced node sends nothing."""
    client = FakeArgus()
    BaseNode._report_cost_in_argus(make_node(), client)
    assert client.items == []


def test_teardown_reports_a_zero_token_node_in_its_own_category(priced_at):
    """A zero-token node is reported in its own category at teardown."""
    node = make_node()
    node._is_zero_token_node = True
    BaseNode._record_hourly_rate(node)
    client = FakeArgus()
    BaseNode._report_cost_in_argus(node, client)
    assert [i.category for i in client.items] == ["zero_db_node"]


# --- the instance details pricing depends on --------------------------------------------


def test_an_oci_flex_node_is_typed_with_its_ocpu_count_so_it_can_be_priced():
    """The catalog keys Flex shapes as <shape>:<ocpus>; the bare shape name prices as unknown."""
    provider = SimpleNamespace(
        get_ip_address=lambda *_a, **_k: "10.0.0.1", get_private_dns_name=lambda _id: "", _region="us-ashburn-1"
    )
    vm = VirtualMachineProvider.convert_to_vm_instance(provider, make_oci_instance("VM.Standard.E4.Flex", 16.0), None)
    assert vm.instance_type == "VM.Standard.E4.Flex:16"
    assert get_hourly_rate("oci", "us-ashburn-1", vm.instance_type).known


def test_a_fixed_oci_shape_keeps_its_name():
    """A fixed OCI shape keeps its name as the instance type."""
    provider = SimpleNamespace(
        get_ip_address=lambda *_a, **_k: "10.0.0.1", get_private_dns_name=lambda _id: "", _region="us-ashburn-1"
    )
    vm = VirtualMachineProvider.convert_to_vm_instance(provider, make_oci_instance("BM.DenseIO.E4.128", 128.0), None)
    assert vm.instance_type == "BM.DenseIO.E4.128"


@pytest.mark.parametrize(
    "provisioning_model, preemptible, expected",
    [
        pytest.param("SPOT", False, True, id="spot-vm"),
        pytest.param("STANDARD", True, True, id="legacy-preemptible"),
        pytest.param("STANDARD", False, False, id="on-demand"),
    ],
)
def test_gce_spot_is_read_from_the_provisioning_model(provisioning_model, preemptible, expected):
    """SCT requests GCE spot through the provisioning model only, never the legacy flag."""
    node = SimpleNamespace(
        _instance=SimpleNamespace(
            scheduling=SimpleNamespace(provisioning_model=provisioning_model, preemptible=preemptible)
        )
    )
    assert GCENode.is_spot.fget(node) is expected
