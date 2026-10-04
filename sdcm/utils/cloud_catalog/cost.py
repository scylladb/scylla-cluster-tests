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
*unknown*, not *free* (a type missing from the catalog yields 0). Unknown must stay `None`
all the way out, so a missing price is never reported as a zero cost.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from sdcm.sct_config.config import (
    SIZING_ROLE_PARAMS,
    SCTConfiguration,
    backend_to_cloud,
    oracle_cluster_in_use,
)
from sdcm.provision.gce.constants import BUNDLED_LOCAL_SSD_FAMILIES
from sdcm.utils.cloud_catalog.lifecycle import InstanceLifecycle
from sdcm.utils.cloud_catalog.pricing import (
    AWSPricing,
    AzurePricing,
    GCEPricing,
    OCIPricing,
    catalog_price,
    catalog_spot_price,
)

LOGGER = logging.getLogger(__name__)

SECONDS_PER_HOUR = 3600.0


@dataclass(frozen=True)
class InstanceRate:
    """An hourly rate for one instance type, or an explicit "we don't know"."""

    price_per_hour: float | None
    is_spot: bool

    @classmethod
    def from_raw(cls, raw: Any, *, is_spot: bool) -> InstanceRate:
        """The single place that owns the 0-means-unknown rule."""
        try:
            price = float(raw)
        except TypeError, ValueError:
            price = 0.0
        if price <= 0:
            return cls(price_per_hour=None, is_spot=is_spot)
        return cls(price_per_hour=price, is_spot=is_spot)

    @classmethod
    def unknown(cls, *, is_spot: bool = False) -> InstanceRate:
        return cls(price_per_hour=None, is_spot=is_spot)

    @property
    def known(self) -> bool:
        return self.price_per_hour is not None


@lru_cache(maxsize=None)
def _pricing_for(cloud: str):
    """Build a pricing class on first use.

    Not at import time: `AWSPricing.__init__` constructs a boto3 pricing client, so building
    them eagerly would put a network client in the import path of every caller.
    """
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
    lookup = catalog_spot_price if is_spot else catalog_price
    price = lookup(cloud, region, instance_type)
    if price is not None:
        return price
    parts = instance_type.split(":")
    if len(parts) == 3:
        return lookup(cloud, region, f"{parts[0]}:{parts[1]}")
    return None


def get_hourly_rate(
    cloud: str, region: str, instance_type: str, is_spot: bool = False, catalog_only: bool = False
) -> InstanceRate:
    """Look up the hourly rate for one instance. Never raises, never blocks a test.

    With `catalog_only`, answer purely from the checked-in catalog and report unknown on a
    miss. The pricing classes fall through to live cloud pricing APIs when the catalog has no
    entry, which is fine for a long-lived test process but not for a pre-flight estimate: that
    runs on a builder before anything exists, and must not depend on a cloud API being
    reachable to tell someone what a run will cost.
    """
    if cloud not in _REGION_PARAMS or not instance_type:
        return InstanceRate.unknown(is_spot=is_spot)

    if catalog_only:
        try:
            raw = _catalog_lookup(cloud, region, instance_type, is_spot=is_spot)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Catalog lookup failed for %s/%s in %s", cloud, instance_type, region, exc_info=True)
            return InstanceRate.unknown(is_spot=is_spot)
        return InstanceRate.from_raw(raw, is_spot=is_spot)

    lifecycle = InstanceLifecycle.SPOT if is_spot else InstanceLifecycle.ON_DEMAND
    try:
        raw = _pricing_for(cloud).get_instance_price(
            region=region, instance_type=instance_type, state="running", lifecycle=lifecycle
        )
    except Exception:  # noqa: BLE001 — a price is never worth failing a test over
        LOGGER.warning("Could not price %s/%s in %s", cloud, instance_type, region, exc_info=True)
        return InstanceRate.unknown(is_spot=is_spot)

    return InstanceRate.from_raw(raw, is_spot=is_spot)


def resolve_spot_rates(cloud: str, region: str, instance_types: Sequence[str]) -> dict[str, InstanceRate]:
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
    wanted = [t for t in dict.fromkeys(instance_types) if t]
    if cloud not in _REGION_PARAMS or not wanted:
        return {}

    if cloud != "aws":
        return {t: get_hourly_rate(cloud, region, t, is_spot=True, catalog_only=True) for t in wanted}

    try:
        prices = _pricing_for("aws").get_spot_instance_prices(region, wanted)
    except Exception:  # noqa: BLE001 — see docstring
        LOGGER.warning("Could not fetch AWS spot prices in %s", region, exc_info=True)
        return {t: InstanceRate.unknown(is_spot=True) for t in wanted}

    return {t: InstanceRate.from_raw(prices.get(t), is_spot=True) for t in wanted}


def cost_for(rate: InstanceRate | None, seconds: float) -> float | None:
    """Cost of running one instance at `rate` for `seconds`. Fractional hours, not whole ones."""
    if rate is None or not rate.known or seconds is None or seconds < 0:
        return None
    return rate.price_per_hour * (seconds / SECONDS_PER_HOUR)


#: How many nodes each role has. The instance-type half of this comes from config's own
#: table, so a role added there is priced here automatically rather than silently omitted —
#: which is how `zero_token` was being left out of every estimate.
_ROLE_NODE_COUNTS: dict[str, str] = {
    "db": "n_db_nodes",
    "db_oracle": "n_test_oracle_db_nodes",
    "zero_token": "n_db_zero_token_nodes",
    "loader": "n_loaders",
    "monitor": "n_monitor_nodes",
}

_ROLE_PARAMS: dict[str, dict[str, tuple[str, str]]] = {
    cloud: {
        role: (type_param, _ROLE_NODE_COUNTS[role]) for role, type_param in roles.items() if role in _ROLE_NODE_COUNTS
    }
    for cloud, roles in SIZING_ROLE_PARAMS.items()
}

#: Roles that Scylla Cloud runs for an xcloud backend. It never runs them on spot (`CloudNode.is_spot`
#: is always False), so `instance_provision` - which xcloud inherits from the provider's defaults,
#: where it is spot - only reaches the loaders and monitor SCT provisions itself.
_XCLOUD_MANAGED_ROLES = frozenset({"db", "zero_token"})

_REGION_PARAMS = {
    "aws": "region_name",
    "gce": "gce_datacenter",
    "azure": "azure_region_name",
    "oci": "oci_region_name",
}


@dataclass(frozen=True)
class RoleCost:
    """One role's cost at the run's lifecycle (`rate`), and the same nodes all on-demand."""

    role: str
    instance_type: str
    node_count: int
    rate: InstanceRate
    cost: float | None
    on_demand_rate: InstanceRate
    on_demand_cost: float | None

    @property
    def known(self) -> bool:
        return self.cost is not None


@dataclass(frozen=True)
class RunCostEstimate:
    """A pre-run estimate. `partial` means at least one role could not be priced."""

    total: float | None
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
    def unavailable(
        cls, *, duration_hours: float = 0.0, is_spot: bool = False, fallback_to_on_demand: bool = False
    ) -> "RunCostEstimate":
        """No estimate could be produced. Keeps whatever context the caller does have."""
        return cls(
            total=None,
            duration_hours=duration_hours,
            is_spot=is_spot,
            roles=(),
            partial=True,
            fallback_to_on_demand=fallback_to_on_demand,
        )

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
                    "on_demand_price_per_hour": r.on_demand_rate.price_per_hour,
                    "cost": round(r.cost, 4) if r.cost is not None else None,
                    "on_demand_cost": round(r.on_demand_cost, 4) if r.on_demand_cost is not None else None,
                }
                for r in self.roles
            ],
        }


def _node_count(config: SCTConfiguration, role: str, param: str) -> int:
    """Total nodes across every DC/region; config resolves counts to a list of ints.

    A scale test grows the db cluster to `cluster_target_size`, so the db role is priced at that
    peak - as capacity reservation sizes it - rather than at its starting size.
    """
    count = sum(config.get(param) or [])
    if role == "db":
        return max(count, sum(config.get("cluster_target_size") or []))
    return count


def _first_region(config: SCTConfiguration, param: str) -> str:
    """First region the run provisions in.

    Not simply `config.get(param)[0]`: `get()` space-joins the backend's multi-region params
    (`region_name`, `gce_datacenter`) back into one string, and a single list item can itself
    hold several space-separated regions. Splitting covers both, and the plain list the other
    clouds return.
    """
    value = config.get(param) or ""
    if isinstance(value, (list, tuple)):
        value = " ".join(str(v) for v in value)
    regions = str(value).split()
    return regions[0] if regions else ""


def estimate_run_cost(config: SCTConfiguration, duration_minutes: float | None = None) -> RunCostEstimate:  # noqa: PLR0914
    """Estimate a run's instance-hour cost from a resolved `SCTConfiguration`.

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
    backend = str(config.get("cluster_backend") or "")
    # xcloud has no cloud of its own; config resolves it from xcloud_provider, and it loads
    # that provider's defaults, so the role parameters below are the provider's too.
    cloud = backend_to_cloud(backend, config.get("xcloud_provider"))
    # The test's real runtime: `stress_duration`, when set, drives it rather than `test_duration`.
    duration_min = duration_minutes if duration_minutes is not None else config.effective_test_duration()
    duration_hours = float(duration_min) / 60.0
    is_spot = "spot" in str(config.get("instance_provision") or "").lower()
    fallback = bool(config.get("instance_provision_fallback_on_demand"))

    if not cloud:
        return RunCostEstimate.unavailable(
            duration_hours=duration_hours, is_spot=is_spot, fallback_to_on_demand=fallback
        )

    region = _first_region(config, _REGION_PARAMS[cloud])
    db_type = str(config.get("db_type") or "")

    # Gather every instance type first so spot can be resolved in one batch. Resolving per
    # role would turn one API call into one per role, which is the whole thing this avoids.
    def role_is_spot(role: str, instance_type: str) -> bool:
        if backend == "xcloud" and role in _XCLOUD_MANAGED_ROLES:
            return False
        # GCE types with bundled local SSD (z3) need on_host_maintenance=MIGRATE, which spot
        # cannot have, so SCT's GCE provisioning always requests them on-demand.
        if cloud == "gce" and instance_type.split("-", maxsplit=1)[0] in BUNDLED_LOCAL_SSD_FAMILIES:
            return False
        return is_spot

    planned: list[tuple[str, str, int]] = []
    for role, (type_param, count_param) in _ROLE_PARAMS[cloud].items():
        if role == "db_oracle" and not oracle_cluster_in_use(db_type):
            continue
        node_count = _node_count(config, role, count_param)
        if node_count > 0:
            planned.append((role, str(config.get(type_param) or "").strip(), node_count))

    spot_rates: dict[str, InstanceRate] = {}
    if spot_types := [t for role, t, _ in planned if t and role_is_spot(role, t)]:
        spot_rates = resolve_spot_rates(cloud, region, spot_types)

    roles: list[RoleCost] = []
    for role, (type_param, count_param) in _ROLE_PARAMS[cloud].items():
        if role == "db_oracle" and not oracle_cluster_in_use(db_type):
            continue
        instance_type = str(config.get(type_param) or "").strip()
        node_count = _node_count(config, role, count_param)
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
                    on_demand_rate=InstanceRate.unknown(),
                    on_demand_cost=None,
                )
            )
            continue
        on_demand_rate = get_hourly_rate(cloud, region, instance_type, is_spot=False, catalog_only=True)
        # A spot rate we could not resolve must not silently become the on-demand price:
        # that would report a number under a "spot" label that is several times too high.
        rate = (
            spot_rates.get(instance_type, InstanceRate.unknown(is_spot=True))
            if role_is_spot(role, instance_type)
            else on_demand_rate
        )
        per_node = cost_for(rate, duration_hours * SECONDS_PER_HOUR)
        on_demand_per_node = cost_for(on_demand_rate, duration_hours * SECONDS_PER_HOUR)
        roles.append(
            RoleCost(
                role=role,
                instance_type=instance_type,
                node_count=node_count,
                rate=rate,
                cost=per_node * node_count if per_node is not None else None,
                on_demand_rate=on_demand_rate,
                on_demand_cost=on_demand_per_node * node_count if on_demand_per_node is not None else None,
            )
        )

    priced = [r.cost for r in roles if r.known]
    known_ceilings = [r.on_demand_cost for r in roles if r.on_demand_cost is not None]
    return RunCostEstimate(
        total=sum(priced) if priced else None,
        duration_hours=duration_hours,
        is_spot=is_spot,
        roles=tuple(roles),
        partial=any(not r.known for r in roles) or not roles,
        on_demand_total=sum(known_ceilings) if known_ceilings else None,
        fallback_to_on_demand=fallback,
    )
