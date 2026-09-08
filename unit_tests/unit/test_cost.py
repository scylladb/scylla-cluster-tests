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

"""Tests for instance-hour cost estimation.

Deliberately offline: on-demand lookups are answered by the checked-in catalog under
data/instance_catalog/, so these assert on real prices without touching the network.
"""

import pytest

from sdcm.utils.cloud_catalog.cost import (
    InstanceRate,
    RunCostEstimate,
    cost_for,
    estimate_run_cost,
    get_hourly_rate,
)


class FakeParams(dict):
    """Stands in for SCTConfiguration: estimate_run_cost only ever calls .get()."""


# --- the 0-means-unknown rule -------------------------------------------------------


@pytest.mark.parametrize("raw", [0, 0.0, -1, None, "", "not-a-number"])
def test_non_positive_price_is_unknown_not_free(raw):
    rate = InstanceRate.from_raw(raw, is_spot=False, source="catalog")
    assert rate.price_per_hour is None
    assert rate.source == "unknown"
    assert not rate.known
    # The whole point: an unknown price must never become a zero cost.
    assert cost_for(rate, 3600) is None


def test_positive_price_is_kept():
    rate = InstanceRate.from_raw(1.5, is_spot=True, source="spot-api")
    assert rate.price_per_hour == 1.5
    assert rate.is_spot is True
    assert rate.known


# --- rate lookup, against the real catalog ------------------------------------------


@pytest.mark.parametrize(
    "backend, region, instance_type, expected",
    [
        ("aws", "eu-west-1", "i4i.4xlarge", 1.373),
        ("gce", "us-east1", "n2-highmem-32", 2.096224),
        ("azure", "eastus", "Standard_L8s_v3", 0.696),
    ],
)
def test_on_demand_rate_from_catalog(backend, region, instance_type, expected):
    rate = get_hourly_rate(backend, region, instance_type)
    assert rate.price_per_hour == pytest.approx(expected)
    assert rate.source == "catalog"
    assert rate.is_spot is False


def test_oci_has_no_pricing_and_reports_unknown():
    # OCIPricing returns 0 unconditionally; that must surface as unknown, not as free.
    assert get_hourly_rate("oci", "eu-frankfurt-1", "VM.Standard.E4.Flex").price_per_hour is None


@pytest.mark.parametrize("instance_type", ["c3-standard-8", "c4-standard-8", "n4-standard-8", "t2a-standard-4"])
def test_gce_spot_families_missing_from_the_table_are_unknown(instance_type):
    # The hardcoded GCE spot table only covers c2/e2/f1/g1/m1/n1/n2/n2d.
    rate = get_hourly_rate("gce", "us-east1", instance_type, is_spot=True)
    assert rate.price_per_hour is None
    assert rate.is_spot is True


def test_gce_spot_family_present_in_the_table_is_priced():
    # Guards the test above from passing for the wrong reason.
    rate = get_hourly_rate("gce", "us-east1", "n2-highmem-32", is_spot=True)
    assert rate.price_per_hour == pytest.approx(0.5073)


@pytest.mark.parametrize("backend", ["docker", "k8s-local-kind", "unknown-backend", ""])
def test_backends_without_pricing_report_unknown(backend):
    assert get_hourly_rate(backend, "somewhere", "some.type").price_per_hour is None


def test_unknown_instance_type_is_unknown():
    assert get_hourly_rate("aws", "eu-west-1", "totally.made.up").price_per_hour is None


def test_pricing_failure_never_propagates(monkeypatch):
    def explode(*_args, **_kwargs):
        raise RuntimeError("pricing API down")

    monkeypatch.setattr("sdcm.utils.cloud_catalog.pricing.GCEPricing.get_instance_price", explode)
    assert get_hourly_rate("gce", "us-east1", "n2-standard-8").price_per_hour is None


# --- cost math ----------------------------------------------------------------------


def test_cost_uses_fractional_hours_not_whole_ones():
    rate = InstanceRate.from_raw(2.0, is_spot=False, source="catalog")
    # 90 minutes at $2/h is $3, not $4 as a ceil-to-whole-hours implementation would give.
    assert cost_for(rate, 90 * 60) == pytest.approx(3.0)


@pytest.mark.parametrize("seconds", [0, 1, 12345.678])
def test_cost_scales_linearly(seconds):
    rate = InstanceRate.from_raw(3.0, is_spot=False, source="catalog")
    assert cost_for(rate, seconds) == pytest.approx(3.0 * seconds / 3600)


def test_cost_of_none_or_negative_is_none():
    rate = InstanceRate.from_raw(1.0, is_spot=False, source="catalog")
    assert cost_for(None, 3600) is None
    assert cost_for(rate, -5) is None


# --- run-level estimate -------------------------------------------------------------


def _aws_params(**overrides):
    params = FakeParams(
        cluster_backend="aws",
        region_name="eu-west-1",
        test_duration=120,
        instance_provision="on_demand",
        instance_type_db="i4i.4xlarge",
        n_db_nodes=3,
        instance_type_loader="c6i.2xlarge",
        n_loaders=2,
        instance_type_monitor="t3.large",
        n_monitor_nodes=1,
    )
    params.update(overrides)
    return params


def test_estimate_sums_nodes_times_rate_times_duration():
    est = estimate_run_cost(_aws_params())
    assert isinstance(est, RunCostEstimate)
    assert est.duration_hours == pytest.approx(2.0)
    assert not est.partial

    db = next(r for r in est.roles if r.role == "db")
    assert db.node_count == 3
    assert db.cost == pytest.approx(1.373 * 3 * 2.0)
    assert est.total == pytest.approx(sum(r.cost for r in est.roles))


def test_multi_dc_node_counts_are_summed():
    est = estimate_run_cost(_aws_params(n_db_nodes="3 3"))
    assert next(r for r in est.roles if r.role == "db").node_count == 6


def test_duration_override_wins_over_config():
    est = estimate_run_cost(_aws_params(), duration_minutes=30)
    assert est.duration_hours == pytest.approx(0.5)


def test_unpriced_role_makes_the_estimate_partial_not_silently_low():
    est = estimate_run_cost(_aws_params(instance_type_loader="totally.made.up"))
    assert est.partial
    assert "loader" in est.unpriced_roles
    # The priced roles still contribute; the total is a floor, flagged as incomplete.
    assert est.total == pytest.approx(sum(r.cost for r in est.roles if r.known))


def test_roles_with_zero_nodes_are_skipped():
    est = estimate_run_cost(_aws_params(n_loaders=0))
    assert "loader" not in {r.role for r in est.roles}


def test_spot_run_is_flagged_but_priced_at_on_demand():
    on_demand = estimate_run_cost(_aws_params())
    spot = estimate_run_cost(_aws_params(instance_provision="spot"))
    assert spot.is_spot is True
    # Priced identically: an upper bound is the safe direction for a gate.
    assert spot.total == pytest.approx(on_demand.total)


def test_backend_without_pricing_yields_an_unknown_total():
    est = estimate_run_cost(FakeParams(cluster_backend="docker", test_duration=60))
    assert est.total is None
    assert est.partial


def test_as_dict_is_json_serialisable_and_keeps_nulls():
    import json  # noqa: PLC0415 — local to the one test that needs it

    payload = json.loads(json.dumps(estimate_run_cost(_aws_params(instance_type_db="made.up")).as_dict()))
    assert payload["currency"] == "USD"
    assert payload["partial"] is True
    assert "db" in payload["unpriced_roles"]
    db = next(r for r in payload["roles"] if r["role"] == "db")
    assert db["cost"] is None and db["price_per_hour"] is None


# --- no cloud APIs ------------------------------------------------------------------


def test_catalog_only_does_not_fall_through_to_a_pricing_api(monkeypatch):
    def explode(*_args, **_kwargs):
        raise AssertionError("a live pricing API was called")

    monkeypatch.setattr("sdcm.utils.cloud_catalog.pricing.AWSPricing.get_on_demand_instance_price", explode)
    # An instance type the catalog does not know: without catalog_only this would query AWS.
    rate = get_hourly_rate("aws", "eu-west-1", "m5.24xlarge", catalog_only=True)
    assert rate.price_per_hour is None
    assert rate.source == "unknown"


def test_estimate_never_calls_a_cloud_api(monkeypatch):
    """The estimate runs on a builder before anything exists; it must not need the network.

    Guards both directions: a catalog hit obviously must not call out, and a catalog *miss*
    must report unknown rather than quietly falling through to a live pricing API.
    """
    calls = []

    def record_boto(self, operation_name, *args, **kwargs):
        calls.append(operation_name)
        raise AssertionError(f"boto call during estimate: {operation_name}")

    def record_http(self, method, url, *args, **kwargs):
        calls.append(f"{method} {url}")
        raise AssertionError(f"http call during estimate: {method} {url}")

    monkeypatch.setattr("botocore.client.BaseClient._make_api_call", record_boto)
    monkeypatch.setattr("requests.sessions.Session.request", record_http)

    priced = estimate_run_cost(_aws_params())
    assert priced.total is not None

    unpriced = estimate_run_cost(_aws_params(instance_type_db="m5.24xlarge"))
    assert unpriced.partial
    assert "db" in unpriced.unpriced_roles

    assert not calls
