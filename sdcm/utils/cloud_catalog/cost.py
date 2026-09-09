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
        from sdcm.utils.cloud_catalog.pricing import _catalog_price  # noqa: PLC0415 — lazy, see _pricing_for

        try:
            raw = _catalog_price(cloud, region, instance_type)
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

    @classmethod
    def unavailable(cls) -> "RunCostEstimate":
        """No estimate could be produced at all (e.g. the configuration would not load)."""
        return cls(total=None, currency="USD", duration_hours=0.0, is_spot=False, roles=(), partial=True)

    @property
    def unpriced_roles(self) -> tuple[str, ...]:
        return tuple(r.role for r in self.roles if not r.known)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": round(self.total, 4) if self.total is not None else None,
            "currency": self.currency,
            "duration_hours": round(self.duration_hours, 4),
            "is_spot": self.is_spot,
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


def estimate_run_cost(params: Any, duration_minutes: float | None = None) -> RunCostEstimate:
    """Estimate a run's instance-hour cost from an already-resolved configuration.

    Reads only config, so it works on a Jenkins builder before anything is provisioned —
    which is what makes it usable as a pre-flight gate.

    Spot rates are not knowable ahead of a run, so this deliberately prices everything at
    the on-demand rate and reports `is_spot` alongside: an upper bound is the safe
    direction for a number a gate may act on.

    Rates come from the checked-in catalog only. No cloud pricing API is called, so this
    cannot hang or fail on someone else's availability.
    """
    backend = str(params.get("cluster_backend") or "")
    cloud = BACKEND_TO_CLOUD.get(backend)
    duration_min = duration_minutes if duration_minutes is not None else params.get("test_duration") or 0
    duration_hours = float(duration_min) / 60.0
    is_spot = "spot" in str(params.get("instance_provision") or "").lower()

    if not cloud:
        return RunCostEstimate(
            total=None,
            currency="USD",
            duration_hours=duration_hours,
            is_spot=is_spot,
            roles=(),
            partial=True,
        )

    region = _first_region(params.get(_REGION_PARAMS[cloud]))
    roles: list[RoleCost] = []
    for role, (type_param, count_param) in _ROLE_PARAMS[cloud].items():
        instance_type = str(params.get(type_param) or "").strip()
        node_count = _sum_counts(params.get(count_param))
        if not instance_type or node_count <= 0:
            continue
        rate = get_hourly_rate(backend, region, instance_type, is_spot=False, catalog_only=True)
        per_node = cost_for(rate, duration_hours * SECONDS_PER_HOUR)
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
    return RunCostEstimate(
        total=sum(priced) if priced else None,
        currency="USD",
        duration_hours=duration_hours,
        is_spot=is_spot,
        roles=tuple(roles),
        partial=any(not r.known for r in roles) or not roles,
    )
