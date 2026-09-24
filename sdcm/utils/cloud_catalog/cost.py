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

"""Instance-hour cost estimation.

Scope is deliberately narrow: `hourly_rate * running_time` per instance. No network
egress, no storage, no managed-service overhead.

The one rule that matters everywhere here: a price of `0` from the pricing layer means
*unknown*, not *free* (OCI returns 0 unconditionally, GCE spot has no entry for families
newer than n2d, and any catalog miss yields 0). Unknown must stay `None` all the way out,
so a missing price is never reported as a zero cost.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from sdcm.utils.cloud_catalog.lifecycle import InstanceLifecycle

if TYPE_CHECKING:
    pass

LOGGER = logging.getLogger(__name__)

SECONDS_PER_HOUR = 3600.0

#: SCT cluster_backend -> the cloud key used by the pricing classes and the catalog.
BACKEND_TO_CLOUD = {
    "aws": "aws",
    "aws-siren": "aws",
    "k8s-eks": "aws",
    "gce": "gce",
    "gce-siren": "gce",
    "k8s-gke": "gce",
    "azure": "azure",
    "oci": "oci",
}


@dataclass(frozen=True)
class InstanceRate:
    """An hourly rate for one instance type, or an explicit "we don't know"."""

    price_per_hour: float | None
    is_spot: bool
    source: str  # "catalog" | "spot-api" | "unknown"

    @classmethod
    def from_raw(cls, raw: Any, *, is_spot: bool, source: str) -> InstanceRate:
        """The single place that owns the 0-means-unknown rule."""
        try:
            price = float(raw)
        except TypeError, ValueError:
            price = 0.0
        if price <= 0:
            return cls(price_per_hour=None, is_spot=is_spot, source="unknown")
        return cls(price_per_hour=price, is_spot=is_spot, source=source)

    @classmethod
    def unknown(cls, *, is_spot: bool = False) -> InstanceRate:
        return cls(price_per_hour=None, is_spot=is_spot, source="unknown")

    @property
    def known(self) -> bool:
        return self.price_per_hour is not None


@lru_cache(maxsize=None)
def _pricing_for(cloud: str):
    """Build a pricing class lazily.

    Import-local on purpose: `AWSPricing.__init__` constructs a boto3 pricing client, so
    instantiating eagerly would put a network client in the import path of every caller.
    """
    from sdcm.utils.cloud_catalog.pricing import (  # noqa: PLC0415 — see docstring
        AWSPricing,
        AzurePricing,
        GCEPricing,
        OCIPricing,
    )

    return {"aws": AWSPricing, "gce": GCEPricing, "azure": AzurePricing, "oci": OCIPricing}[cloud]()


def _catalog_lookup(cloud: str, region: str, instance_type: str, is_spot: bool = False) -> float | None:
    """Catalog price for an instance type, resolving memory-customised flex shapes.

    OCI flex shapes are configured as `<shape>:<ocpus>:<memory_gb>` but catalogued as
    `<shape>:<ocpus>`, so an exact lookup misses and the resource looks unpriceable. Fall
    back to the base shape, which is what `sct sizing preview` already does. The price is
    then the base shape's default memory allocation — an over-estimate for a shape trimmed
    to less memory, which is the safe direction for an estimate.

    A spot lookup never falls back to the on-demand price: a missing spot rate is reported
    as unknown so the caller can say so, rather than quietly quoting a number two to four
    times too high under a "spot" label.
    """
    from sdcm.utils.cloud_catalog.pricing import (  # noqa: PLC0415 — lazy, see _pricing_for
        _catalog_price,
        _catalog_spot_price,
    )

    lookup = _catalog_spot_price if is_spot else _catalog_price
    price = lookup(cloud, region, instance_type)
    if price is not None:
        return price
    parts = instance_type.split(":")
    if len(parts) == 3:
        return lookup(cloud, region, f"{parts[0]}:{parts[1]}")
    return None


def get_hourly_rate(
    backend: str, region: str, instance_type: str, is_spot: bool = False, catalog_only: bool = False
) -> InstanceRate:
    """Look up the hourly rate for one instance. Never raises, never blocks a test.

    With `catalog_only`, answer purely from the checked-in catalog and report unknown on a
    miss. The pricing classes fall through to live cloud pricing APIs when the catalog has no
    entry, which is fine for a long-lived test process but not for a pre-flight estimate: that
    runs on a builder before anything exists, and must not depend on a cloud API being
    reachable to tell someone what a run will cost.
    """
    cloud = BACKEND_TO_CLOUD.get(backend)
    if not cloud or not instance_type:
        return InstanceRate.unknown(is_spot=is_spot)

    if catalog_only:
        try:
            raw = _catalog_lookup(cloud, region, instance_type, is_spot=is_spot)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Catalog lookup failed for %s/%s in %s", cloud, instance_type, region, exc_info=True)
            return InstanceRate.unknown(is_spot=is_spot)
        return InstanceRate.from_raw(raw, is_spot=is_spot, source="catalog")

    lifecycle = InstanceLifecycle.SPOT if is_spot else InstanceLifecycle.ON_DEMAND
    try:
        raw = _pricing_for(cloud).get_instance_price(
            region=region, instance_type=instance_type, state="running", lifecycle=lifecycle
        )
    except Exception:  # noqa: BLE001 — a price is never worth failing a test over
        LOGGER.warning("Could not price %s/%s in %s", cloud, instance_type, region, exc_info=True)
        return InstanceRate.unknown(is_spot=is_spot)

    return InstanceRate.from_raw(raw, is_spot=is_spot, source="spot-api" if is_spot else "catalog")


def resolve_spot_rates(backend: str, region: str, instance_types: Sequence[str]) -> dict[str, InstanceRate]:
    """Spot rates for every instance type a run needs, in as few calls as possible.

    Each cloud is asked the way it is cheapest to ask:

    * **AWS** in one live call per region — `DescribeSpotPriceHistory` takes a list, so the
      cost is per region, not per instance type, and prices move too fast to cache to disk.
    * **Everything else** from the checked-in catalog, with no network at all. GCP, Azure and
      OCI all set spot administratively, so a live lookup would buy nothing.

    Never raises. If AWS cannot be reached — no credentials on a developer laptop, for
    instance — every rate comes back unknown and the caller decides what to say, because a
    price is never worth failing a run over.
    """
    cloud = BACKEND_TO_CLOUD.get(backend)
    wanted = [t for t in dict.fromkeys(instance_types) if t]
    if not cloud or not wanted:
        return {}

    if cloud != "aws":
        return {t: get_hourly_rate(backend, region, t, is_spot=True, catalog_only=True) for t in wanted}

    try:
        prices = _pricing_for("aws").get_spot_instance_prices(region, wanted)
    except Exception:  # noqa: BLE001 — see docstring
        LOGGER.warning("Could not fetch AWS spot prices in %s", region, exc_info=True)
        return {t: InstanceRate.unknown(is_spot=True) for t in wanted}

    return {t: InstanceRate.from_raw(prices.get(t), is_spot=True, source="spot-api") for t in wanted}


def cost_for(rate: InstanceRate | None, seconds: float) -> float | None:
    """Cost of running one instance at `rate` for `seconds`. Fractional hours, not whole ones."""
    if rate is None or not rate.known or seconds is None or seconds < 0:
        return None
    return rate.price_per_hour * (seconds / SECONDS_PER_HOUR)


#: cloud -> {role: (instance-type param, node-count param)}. Mirrors ROLE_PARAMS in
#: sdcm/sct_config.py, which is what resolves sizing constraints into these very params.
_ROLE_PARAMS: dict[str, dict[str, tuple[str, str]]] = {
    "aws": {
        "db": ("instance_type_db", "n_db_nodes"),
        "db_oracle": ("instance_type_db_oracle", "n_test_oracle_db_nodes"),
        "loader": ("instance_type_loader", "n_loaders"),
        "monitor": ("instance_type_monitor", "n_monitor_nodes"),
    },
    "gce": {
        "db": ("gce_instance_type_db", "n_db_nodes"),
        "db_oracle": ("gce_instance_type_db_oracle", "n_test_oracle_db_nodes"),
        "loader": ("gce_instance_type_loader", "n_loaders"),
        "monitor": ("gce_instance_type_monitor", "n_monitor_nodes"),
    },
    "azure": {
        "db": ("azure_instance_type_db", "n_db_nodes"),
        "db_oracle": ("azure_instance_type_db_oracle", "n_test_oracle_db_nodes"),
        "loader": ("azure_instance_type_loader", "n_loaders"),
        "monitor": ("azure_instance_type_monitor", "n_monitor_nodes"),
    },
    "oci": {
        "db": ("oci_instance_type_db", "n_db_nodes"),
        "db_oracle": ("oci_instance_type_db_oracle", "n_test_oracle_db_nodes"),
        "loader": ("oci_instance_type_loader", "n_loaders"),
        "monitor": ("oci_instance_type_monitor", "n_monitor_nodes"),
    },
}

_REGION_PARAMS = {
    "aws": "region_name",
    "gce": "gce_datacenter",
    "azure": "azure_region_name",
    "oci": "oci_region_name",
}


@dataclass(frozen=True)
class RoleCost:
    role: str
    instance_type: str
    node_count: int
    rate: InstanceRate
    cost: float | None

    @property
    def known(self) -> bool:
        return self.cost is not None


@dataclass(frozen=True)
class RunCostEstimate:
    """A pre-run estimate. `partial` means at least one role could not be priced."""

    total: float | None
    currency: str
    duration_hours: float
    is_spot: bool
    roles: tuple[RoleCost, ...]
    partial: bool
    #: What the same run would cost entirely on-demand. On a spot run *with fallback enabled*
    #: this is the ceiling: the run really can end up on-demand, so `total` is a lower bound
    #: and this is the figure a gate should act on. On an on-demand run it equals `total`.
    on_demand_total: float | None = None
    #: Whether the run may silently become an on-demand run when spot capacity is short.
    #: Without it the spot figure is the whole story and the ceiling is hypothetical.
    fallback_to_on_demand: bool = False

    @classmethod
    def unavailable(cls) -> "RunCostEstimate":
        """No estimate could be produced at all (e.g. the configuration would not load)."""
        return cls(total=None, currency="USD", duration_hours=0.0, is_spot=False, roles=(), partial=True)

    @property
    def may_fall_back(self) -> bool:
        """True when the on-demand total is a real risk rather than a hypothetical."""
        return self.is_spot and self.fallback_to_on_demand and self.on_demand_total is not None

    @property
    def unpriced_roles(self) -> tuple[str, ...]:
        return tuple(r.role for r in self.roles if not r.known)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": round(self.total, 4) if self.total is not None else None,
            "currency": self.currency,
            "duration_hours": round(self.duration_hours, 4),
            "is_spot": self.is_spot,
            "on_demand_total": round(self.on_demand_total, 4) if self.on_demand_total is not None else None,
            "fallback_to_on_demand": self.fallback_to_on_demand,
            "partial": self.partial,
            "unpriced_roles": list(self.unpriced_roles),
            "roles": [
                {
                    "role": r.role,
                    "instance_type": r.instance_type,
                    "node_count": r.node_count,
                    "price_per_hour": r.rate.price_per_hour,
                    "source": r.rate.source,
                    "cost": round(r.cost, 4) if r.cost is not None else None,
                }
                for r in self.roles
            ],
        }


def _sum_counts(value: Any) -> int:
    """Node counts may be an int, "3", or "3 3" for a multi-DC/region layout."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, (list, tuple)):
        return sum(_sum_counts(v) for v in value)
    total = 0
    for part in str(value).replace(",", " ").split():
        try:
            total += int(part)
        except ValueError:
            continue
    return total


def _first_region(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return str(value[0]) if value else ""
    return str(value).split()[0] if value and str(value).split() else ""


def estimate_run_cost(params: Any, duration_minutes: float | None = None) -> RunCostEstimate:  # noqa: PLR0914
    """Estimate a run's instance-hour cost from an already-resolved configuration.

    Reads only config, so it works on a Jenkins builder before anything is provisioned —
    which is what makes it usable as a pre-flight gate.

    A spot run is priced at spot rates, because pricing it at on-demand overstated the
    common case by two to four times and taught people to ignore the number. The on-demand
    total is still computed and returned as a ceiling: spot can fall back to on-demand, so
    the spot figure is a lower bound, and a gate should act on the ceiling.

    Only a spot run costs an API call, and only on AWS, and only one per region — see
    `resolve_spot_rates`. An `on_demand` run touches no pricing API at all, and neither does
    any other backend. Nothing here raises: an unreachable API yields unknown rates, which
    are reported as unknown rather than guessed.
    """
    backend = str(params.get("cluster_backend") or "")
    cloud = BACKEND_TO_CLOUD.get(backend)
    duration_min = duration_minutes if duration_minutes is not None else params.get("test_duration") or 0
    duration_hours = float(duration_min) / 60.0
    is_spot = "spot" in str(params.get("instance_provision") or "").lower()
    fallback = bool(params.get("instance_provision_fallback_on_demand"))

    if not cloud:
        return RunCostEstimate(
            total=None,
            currency="USD",
            duration_hours=duration_hours,
            is_spot=is_spot,
            roles=(),
            partial=True,
            fallback_to_on_demand=fallback,
        )

    region = _first_region(params.get(_REGION_PARAMS[cloud]))
    # An oracle cluster only exists for mixed runs, but its node count parameter still
    # defaults to 1 - the same condition sdcm/sct_config.py applies when resolving sizing.
    db_type = str(params.get("db_type") or "")

    # Gather every instance type first so spot can be resolved in one batch. Resolving per
    # role would turn one API call into one per role, which is the whole thing this avoids.
    planned: list[tuple[str, str, int]] = []
    for role, (type_param, count_param) in _ROLE_PARAMS[cloud].items():
        if role == "db_oracle" and db_type not in ("mixed_scylla", "mixed_cassandra"):
            continue
        node_count = _sum_counts(params.get(count_param))
        if node_count > 0:
            planned.append((role, str(params.get(type_param) or "").strip(), node_count))

    spot_rates: dict[str, InstanceRate] = {}
    if is_spot:
        spot_rates = resolve_spot_rates(backend, region, [t for _, t, _ in planned if t])

    roles: list[RoleCost] = []
    ceilings: list[float | None] = []
    for role, (type_param, count_param) in _ROLE_PARAMS[cloud].items():
        if role == "db_oracle" and db_type not in ("mixed_scylla", "mixed_cassandra"):
            continue
        instance_type = str(params.get(type_param) or "").strip()
        node_count = _sum_counts(params.get(count_param))
        if node_count <= 0:
            continue
        if not instance_type:
            # Nodes are configured but their instance type never resolved. Skipping the role
            # would drop them from the total silently, which is how you get a confident-looking
            # estimate that is missing the db cluster. Report it as unpriced instead.
            roles.append(
                RoleCost(
                    role=role,
                    instance_type="(unresolved)",
                    node_count=node_count,
                    rate=InstanceRate.unknown(),
                    cost=None,
                )
            )
            ceilings.append(None)
            continue
        on_demand_rate = get_hourly_rate(backend, region, instance_type, is_spot=False, catalog_only=True)
        # A spot rate we could not resolve must not silently become the on-demand price:
        # that would report a number under a "spot" label that is several times too high.
        rate = spot_rates.get(instance_type, InstanceRate.unknown(is_spot=True)) if is_spot else on_demand_rate
        per_node = cost_for(rate, duration_hours * SECONDS_PER_HOUR)
        on_demand_per_node = cost_for(on_demand_rate, duration_hours * SECONDS_PER_HOUR)
        ceilings.append(on_demand_per_node * node_count if on_demand_per_node is not None else None)
        roles.append(
            RoleCost(
                role=role,
                instance_type=instance_type,
                node_count=node_count,
                rate=rate,
                cost=per_node * node_count if per_node is not None else None,
            )
        )

    priced = [r.cost for r in roles if r.known]
    known_ceilings = [c for c in ceilings if c is not None]
    return RunCostEstimate(
        total=sum(priced) if priced else None,
        currency="USD",
        duration_hours=duration_hours,
        is_spot=is_spot,
        roles=tuple(roles),
        partial=any(not r.known for r in roles) or not roles,
        on_demand_total=sum(known_ceilings) if known_ceilings else None,
        fallback_to_on_demand=fallback,
    )
