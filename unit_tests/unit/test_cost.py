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

from sdcm.utils.cloud_catalog.pricing import AWSPricing, AzurePricing, GCEPricing, OCIPricing
from sdcm.utils.cloud_catalog.cost import (
    BACKEND_TO_CLOUD,
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


def test_oci_shape_without_an_ocpu_count_is_unknown():
    """A bare flex shape names no size, so it has no price — unknown, not free.

    OCI *is* priced now; this is about a shape that cannot be resolved, not about the cloud.
    """
    assert get_hourly_rate("oci", "eu-frankfurt-1", "VM.Standard.E4.Flex").price_per_hour is None


@pytest.mark.parametrize("instance_type", ["c3-standard-8", "c4-standard-8", "n4-standard-8", "t2a-standard-4"])
def test_gce_spot_for_an_uncatalogued_type_is_unknown(instance_type):
    rate = get_hourly_rate("gce", "us-east1", instance_type, is_spot=True, catalog_only=True)
    assert rate.price_per_hour is None
    assert rate.is_spot is True


def test_gce_spot_comes_from_the_catalog_and_is_below_on_demand():
    """Guards the test above from passing for the wrong reason."""
    spot = get_hourly_rate("gce", "us-east1", "n2-highmem-32", is_spot=True, catalog_only=True)
    on_demand = get_hourly_rate("gce", "us-east1", "n2-highmem-32", catalog_only=True)
    assert spot.price_per_hour is not None
    assert spot.price_per_hour < on_demand.price_per_hour


def test_gce_spot_discount_is_region_specific():
    """A single global discount would be wrong: measured spread is roughly 40%-78%."""
    rates = {
        region: get_hourly_rate("gce", region, "n2-standard-16", is_spot=True, catalog_only=True).price_per_hour
        / get_hourly_rate("gce", region, "n2-standard-16", catalog_only=True).price_per_hour
        for region in ("us-east1", "europe-west1", "asia-northeast1")
    }
    assert len(set(round(r, 3) for r in rates.values())) > 1, rates


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


def test_spot_run_is_priced_at_spot_and_carries_an_on_demand_ceiling(monkeypatch):
    """Pricing spot at on-demand overstated the common case by 2-4x; the ceiling is kept."""
    monkeypatch.setattr(
        "sdcm.utils.cloud_catalog.cost.resolve_spot_rates",
        lambda backend, region, types: {
            t: InstanceRate(price_per_hour=0.25, is_spot=True, source="spot-api") for t in types
        },
    )
    on_demand = estimate_run_cost(_aws_params())
    spot = estimate_run_cost(_aws_params(instance_provision="spot"))

    assert spot.is_spot is True
    assert spot.total < on_demand.total
    # The ceiling is what a gate acts on: spot can fall back to on-demand.
    assert spot.on_demand_total == pytest.approx(on_demand.total)


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


def test_on_demand_estimate_never_calls_a_cloud_api(monkeypatch):
    """An on-demand estimate must not need the network at all.

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


def test_unavailable_estimate_is_well_formed():
    """A configuration that will not load must still yield a usable, honest result.

    `estimate-cost` runs as a pipeline pre-flight step, so it reports the failure and exits 0
    rather than turning an advisory stage into a second, noisier build failure.
    """
    payload = RunCostEstimate.unavailable().as_dict()
    assert payload["total"] is None
    assert payload["partial"] is True
    assert payload["roles"] == []
    assert payload["currency"] == "USD"


# --- flex shapes and unresolved roles -----------------------------------------------


@pytest.mark.parametrize(
    "instance_type, expected",
    [
        ("VM.Standard.E4.Flex:2", 0.098),  # catalogued form
        ("VM.Standard.E4.Flex:2:8", 0.098),  # configured form: <shape>:<ocpus>:<memory_gb>
        ("VM.Standard.E4.Flex:1:8", 0.049),
    ],
)
def test_oci_flex_shapes_resolve_to_the_base_shape(instance_type, expected):
    """OCI is priceable: flex shapes just need the memory suffix stripped.

    Configs name them `<shape>:<ocpus>:<memory_gb>` while the catalog stores
    `<shape>:<ocpus>`, so an exact-match-only lookup made every OCI run look unpriceable.
    """
    rate = get_hourly_rate("oci", "us-ashburn-1", instance_type, catalog_only=True)
    assert rate.price_per_hour == pytest.approx(expected)


def test_unknown_flex_shape_is_still_unknown():
    assert get_hourly_rate("oci", "us-ashburn-1", "VM.Bogus.Flex:9:9", catalog_only=True).price_per_hour is None


def test_configured_nodes_with_no_instance_type_are_reported_not_dropped():
    """A role whose type never resolved must not vanish from the estimate.

    Silently skipping it yields a confident-looking total that is missing an entire
    cluster - far worse than admitting the gap.
    """
    est = estimate_run_cost(_aws_params(instance_type_db=""))
    db = next(r for r in est.roles if r.role == "db")
    assert db.node_count == 3
    assert db.cost is None
    assert est.partial
    assert "db" in est.unpriced_roles


def test_oracle_nodes_are_ignored_unless_the_run_is_mixed():
    """`n_test_oracle_db_nodes` defaults to 1 even when no oracle cluster is used."""
    plain = estimate_run_cost(_aws_params(n_test_oracle_db_nodes=1))
    assert "db_oracle" not in {r.role for r in plain.roles}
    assert not plain.partial

    mixed = estimate_run_cost(
        _aws_params(db_type="mixed_scylla", n_test_oracle_db_nodes=1, instance_type_db_oracle="i4i.4xlarge")
    )
    oracle = next(r for r in mixed.roles if r.role == "db_oracle")
    assert oracle.cost == pytest.approx(1.373 * 1 * 2.0)


# --- how many API calls a run costs --------------------------------------------------
#
# The whole design rests on DescribeSpotPriceHistory taking a *list* of instance types, so
# the unit of work is a region rather than an instance type. These pin that: if a refactor
# ever moves the call inside the per-role loop, the count changes and these fail.


class _FakeEC2:
    """Counts describe_spot_price_history calls and answers with per-AZ prices."""

    def __init__(self, price=0.2):
        self.calls: list[list[str]] = []
        self._price = price

    def describe_spot_price_history(self, **kwargs):
        requested = list(kwargs["InstanceTypes"])
        self.calls.append(requested)
        return {
            "SpotPriceHistory": [
                {"InstanceType": itype, "AvailabilityZone": f"{az}", "SpotPrice": str(self._price)}
                for itype in requested
                for az in ("eu-west-1a", "eu-west-1b")
            ]
        }


@pytest.fixture(name="fake_ec2")
def fixture_fake_ec2(monkeypatch):
    fake = _FakeEC2()
    monkeypatch.setattr("sdcm.utils.cloud_catalog.pricing.boto3.client", lambda *a, **kw: fake)
    # AWSPricing memoises per region on the instance, so each test gets a fresh one.
    monkeypatch.setattr("sdcm.utils.cloud_catalog.cost._pricing_for", _fresh_pricing_for)
    return fake


def _fresh_pricing_for(cloud):
    return {"aws": AWSPricing, "gce": GCEPricing, "azure": AzurePricing, "oci": OCIPricing}[cloud]()


def test_a_spot_run_costs_exactly_one_aws_call(fake_ec2):
    estimate_run_cost(_aws_params(instance_provision="spot"))
    assert len(fake_ec2.calls) == 1
    # One call carried every distinct instance type the run needs.
    assert set(fake_ec2.calls[0]) == {"i4i.4xlarge", "c6i.2xlarge", "t3.large"}


def test_call_count_does_not_grow_with_node_count(fake_ec2):
    estimate_run_cost(_aws_params(instance_provision="spot", n_db_nodes=60, n_loaders=20, n_monitor_nodes=3))
    assert len(fake_ec2.calls) == 1


def test_an_on_demand_run_makes_no_spot_call_at_all(fake_ec2):
    """Cheap is not free — a lookup nothing asked for is pure waste."""
    estimate = estimate_run_cost(_aws_params(instance_provision="on_demand"))
    assert fake_ec2.calls == []
    assert estimate.total is not None


def test_spot_rates_are_shared_across_repeated_lookups(fake_ec2):
    """A provisioning burst must collapse into one lookup, not one per node."""
    pricing = AWSPricing()
    for _ in range(10):
        pricing.get_spot_instance_prices("eu-west-1", ["i4i.4xlarge", "c6i.2xlarge"])
    assert len(fake_ec2.calls) == 1


def test_unreachable_spot_api_yields_unknown_not_a_wrong_number(monkeypatch):
    """No credentials on a laptop must not fail the estimate, nor invent a price."""

    def explode(*_args, **_kwargs):
        raise RuntimeError("no credentials")

    monkeypatch.setattr("sdcm.utils.cloud_catalog.pricing.boto3.client", explode)
    monkeypatch.setattr("sdcm.utils.cloud_catalog.cost._pricing_for", _fresh_pricing_for)

    estimate = estimate_run_cost(_aws_params(instance_provision="spot"))
    assert estimate.partial
    assert estimate.total is None
    # The ceiling still comes from the catalog, so the run is not left with nothing.
    assert estimate.on_demand_total is not None


def test_a_missing_spot_price_never_falls_back_to_on_demand(monkeypatch):
    """Quoting the on-demand price under a "spot" label would be several times too high."""
    monkeypatch.setattr(
        "sdcm.utils.cloud_catalog.cost.resolve_spot_rates",
        lambda backend, region, types: {t: InstanceRate.unknown(is_spot=True) for t in types},
    )
    estimate = estimate_run_cost(_aws_params(instance_provision="spot"))
    assert estimate.total is None
    assert set(estimate.unpriced_roles) == {"db", "loader", "monitor"}


def test_gce_spot_estimate_makes_no_network_call(monkeypatch):
    """GCE spot is administered and lives in the catalog; a run must never look it up."""

    def explode(*_args, **_kwargs):
        raise AssertionError("a network call was made during a GCE estimate")

    monkeypatch.setattr("botocore.client.BaseClient._make_api_call", explode)
    monkeypatch.setattr("requests.sessions.Session.request", explode)

    estimate = estimate_run_cost(
        FakeParams(
            cluster_backend="gce",
            gce_datacenter="us-east1",
            test_duration=60,
            instance_provision="spot",
            gce_instance_type_db="n2-highmem-32",
            n_db_nodes=3,
            gce_instance_type_loader="n2-standard-16",
            n_loaders=1,
            gce_instance_type_monitor="e2-standard-8",
            n_monitor_nodes=1,
        )
    )
    assert estimate.total is not None
    assert estimate.is_spot is True
    assert estimate.total < estimate.on_demand_total


# --- Azure and OCI ---------------------------------------------------------------------


def test_azure_spot_comes_from_the_catalog_and_is_well_below_on_demand():
    """Azure Spot rows arrive in a response the generator already fetches — a ~79% saving."""
    spot = get_hourly_rate("azure", "eastus", "Standard_L8s_v3", is_spot=True, catalog_only=True)
    on_demand = get_hourly_rate("azure", "eastus", "Standard_L8s_v3", catalog_only=True)
    assert spot.price_per_hour is not None
    assert spot.price_per_hour < on_demand.price_per_hour / 2


def test_oci_on_demand_is_priced_rather_than_reported_as_zero():
    """OCIPricing used to return 0 unconditionally, leaving oci.yaml's real prices unused."""
    rate = get_hourly_rate("oci", "us-ashburn-1", "BM.DenseIO.E4.128", catalog_only=True)
    assert rate.price_per_hour is not None
    assert rate.price_per_hour > 0


def test_oci_preemptible_is_exactly_half_of_on_demand():
    """OCI has no spot market: preemptible is a flat, documented 50% off."""
    spot = get_hourly_rate("oci", "us-ashburn-1", "BM.DenseIO.E4.128", is_spot=True, catalog_only=True)
    on_demand = get_hourly_rate("oci", "us-ashburn-1", "BM.DenseIO.E4.128", catalog_only=True)
    assert spot.price_per_hour == pytest.approx(on_demand.price_per_hour * 0.5, rel=1e-4)


@pytest.mark.parametrize("backend", ["azure", "oci"])
def test_azure_and_oci_spot_estimates_make_no_network_call(monkeypatch, backend):
    def explode(*_args, **_kwargs):
        raise AssertionError("a network call was made during an estimate")

    monkeypatch.setattr("botocore.client.BaseClient._make_api_call", explode)
    monkeypatch.setattr("requests.sessions.Session.request", explode)

    params = {
        "azure": FakeParams(
            cluster_backend="azure",
            azure_region_name="eastus",
            azure_instance_type_db="Standard_L8s_v3",
            n_db_nodes=3,
            azure_instance_type_loader="Standard_D8s_v5",
            n_loaders=1,
            azure_instance_type_monitor="Standard_D8s_v5",
            n_monitor_nodes=1,
        ),
        "oci": FakeParams(
            cluster_backend="oci",
            oci_region_name="us-ashburn-1",
            oci_instance_type_db="BM.DenseIO.E4.128",
            n_db_nodes=3,
            oci_instance_type_loader="BM.DenseIO.E4.128",
            n_loaders=1,
            oci_instance_type_monitor="BM.DenseIO.E4.128",
            n_monitor_nodes=1,
        ),
    }[backend]
    params.update(test_duration=60, instance_provision="spot")

    estimate = estimate_run_cost(params)
    assert estimate.is_spot is True
    assert estimate.total is not None
    assert estimate.total < estimate.on_demand_total


# --- the on-demand ceiling is only real when fallback is enabled ------------------------


def _spot_estimate(monkeypatch, *, fallback):
    monkeypatch.setattr(
        "sdcm.utils.cloud_catalog.cost.resolve_spot_rates",
        lambda backend, region, types: {
            t: InstanceRate(price_per_hour=0.25, is_spot=True, source="spot-api") for t in types
        },
    )
    return estimate_run_cost(_aws_params(instance_provision="spot", instance_provision_fallback_on_demand=fallback))


def test_ceiling_is_a_real_risk_only_when_fallback_is_enabled(monkeypatch):
    assert _spot_estimate(monkeypatch, fallback=True).may_fall_back is True


def test_ceiling_is_hypothetical_when_fallback_is_disabled(monkeypatch):
    """With fallback off the run cannot reach the on-demand figure, so we must not imply it can."""
    estimate = _spot_estimate(monkeypatch, fallback=False)
    assert estimate.may_fall_back is False
    # Still computed and exposed — callers may want it; it just is not a risk to announce.
    assert estimate.on_demand_total is not None


def test_an_on_demand_run_never_claims_it_may_fall_back():
    assert estimate_run_cost(_aws_params(instance_provision="on_demand")).may_fall_back is False


def test_fallback_flag_is_carried_into_the_json(monkeypatch):
    assert _spot_estimate(monkeypatch, fallback=True).as_dict()["fallback_to_on_demand"] is True


def test_duration_is_carried_on_the_estimate_so_output_can_state_it():
    """A cost with no timespan attached reads as a rate, or as a shorter run than it is."""
    estimate = estimate_run_cost(_aws_params(test_duration=255))
    assert estimate.duration_hours == pytest.approx(4.25)
    assert estimate.as_dict()["duration_hours"] == pytest.approx(4.25)


def test_duration_override_is_reflected_in_both_the_hours_and_the_total():
    short = estimate_run_cost(_aws_params(), duration_minutes=60)
    long = estimate_run_cost(_aws_params(), duration_minutes=600)
    assert (short.duration_hours, long.duration_hours) == (1.0, 10.0)
    assert long.total == pytest.approx(short.total * 10)


@pytest.mark.parametrize("backend", ["xcloud", "k8s-local-kind", "k8s-local-kind-aws", "baremetal", "docker"])
def test_unpriceable_backends_yield_an_honest_estimate_rather_than_an_error(backend):
    """A backend with no priceable instances must degrade, not break the pre-flight stage."""
    estimate = estimate_run_cost(FakeParams(cluster_backend=backend, test_duration=60))
    assert estimate.total is None
    assert estimate.partial
    assert estimate.as_dict()["total"] is None


@pytest.mark.parametrize("backend", ["k8s-eks", "k8s-gke", "aws-siren", "gce-siren"])
def test_managed_backends_price_through_their_underlying_cloud(backend):
    """k8s-eks and aws-siren are AWS underneath; k8s-gke and gce-siren are GCE."""
    assert BACKEND_TO_CLOUD[backend] in ("aws", "gce")
