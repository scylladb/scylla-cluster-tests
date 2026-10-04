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

"""Send what SCT knows about a run's cost to Argus.

All the arithmetic happens in SCT: Argus stores what it is told and sums it. Argus also
stores a zero as a real figure, so an unknown cost is never sent - not as zero, and not as a
partial total that would read as the whole answer.

Every call here is advisory. A failure is logged and swallowed; cost reporting must never
change the outcome of a run.

Kept out of `sdcm.utils.argus` on purpose: that module is imported by `sdcm.test_config`,
which `sdcm.sct_config` imports, and the cost module imports `sdcm.sct_config` - so importing
cost from there would close a cycle.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from argus.client.sct.client import ArgusSCTClient
from argus.client.types import CostItem

from sdcm.utils.cloud_catalog.cost import InstanceRate, RunCostEstimate

LOGGER = logging.getLogger(__name__)

#: Written on every instance SCT can price, when it is created, so whatever terminates it later -
#: the test, or a cleanup or reaper job in another process - can cost it from the instance alone.
#: Keys and values stay within lowercase letters, digits, `_` and `-`, so the same pair is a valid
#: AWS tag, GCE label, Azure tag and OCI defined tag. The price is an integer number of micro-USD
#: per hour because a GCE label value cannot hold a decimal point.
PRICE_TAG = "price_per_hour_micro_usd"
PRICING_TIER_TAG = "pricing_tier"
SPOT = "spot"
ON_DEMAND = "on-demand"


def report_estimated_cost(client: ArgusSCTClient, estimate: RunCostEstimate) -> bool:
    """Send the run's pre-provisioning estimate. Returns whether anything was sent.

    The figure sent is the estimate at the lifecycle the run is configured for. The on-demand
    ceiling is not sent: Argus keeps one estimate per run, and it is compared against the
    actual cost, which a spot run should come close to unless it fell back.

    A partial estimate is not sent. It is a floor that leaves out whole roles, and once stored
    it would be compared against the real total as if it were the expected one.
    """
    if estimate.total is None or estimate.partial:
        LOGGER.info(
            "Not reporting the cost estimate to Argus: %s",
            f"no price for {', '.join(estimate.unpriced_roles)}" if estimate.unpriced_roles else "nothing priced",
        )
        return False
    try:
        client.set_estimated_cost(run_id=client.run_id, value=round(estimate.total, 2))
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to report the cost estimate to Argus", exc_info=True)
        return False
    LOGGER.info("Reported estimated cost $%.2f to Argus run %s", estimate.total, client.run_id)
    return True


def price_tags(rate: InstanceRate) -> dict[str, str]:
    """Tags recording an instance's hourly price and the lifecycle it was priced at.

    Empty for an unknown rate: no tag means "unknown", which is what a later reader must see,
    rather than a zero it would report as a real cost.
    """
    if not rate.known:
        return {}
    return {
        PRICE_TAG: str(round(rate.price_per_hour * 1_000_000)),
        PRICING_TIER_TAG: SPOT if rate.is_spot else ON_DEMAND,
    }


def rate_from_tags(tags: dict[str, str] | None) -> tuple[float, str | None] | None:
    """The hourly price and pricing tier written by `price_tags`, or None if absent or unusable."""
    raw = (tags or {}).get(PRICE_TAG)
    try:
        price = int(raw) / 1_000_000 if raw else 0.0
    except ValueError:
        return None
    if price <= 0:
        return None
    return price, (tags or {}).get(PRICING_TIER_TAG) or None


def cost_category(node_type: str | None) -> str:
    """Argus cost category for an SCT node type, as carried by the `NodeType` tag or the node."""
    node_type = (node_type or "").lower()
    if "zero" in node_type:
        return "zero_db_node"
    if "oracle" in node_type:
        return "oracle_db_node"
    if "db" in node_type:
        return "db_node"
    return node_type or "unknown"


def instance_cost_item(  # noqa: PLR0913
    name: str,
    node_type: str | None,
    price_per_hour: float,
    pricing_tier: str | None,
    started_at: datetime,
    ended_at: datetime | None = None,
    leaked: bool = False,
) -> CostItem | None:
    """The final cost of one instance: its hourly price times how long it ran.

    None when the running time cannot be trusted - a start in the future, or no start at all -
    because Argus would store whatever number is sent as real.
    """
    if not started_at:
        return None
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    ended_at = ended_at or datetime.now(tz=UTC)
    hours = (ended_at - started_at).total_seconds() / 3600.0
    if hours < 0:
        return None
    return CostItem(
        name=name,
        category=cost_category(node_type),
        cost=round(price_per_hour * hours, 4),
        pricing_tier=pricing_tier,
        leaked=leaked,
    )


def cost_item_from_tags(
    name: str, tags: dict[str, str] | None, started_at: datetime | str | None, leaked: bool
) -> CostItem | None:
    """Cost of an instance terminated by cleanup, priced from the tags written when it was created.

    Cleanup sees only the live cloud instance, so the price must already be on it. An instance
    created before price tags existed, or one that could not be priced, yields None.
    `started_at` is the cloud's launch time; GCE gives it as an ISO-8601 string.
    """
    if isinstance(started_at, str):
        started_at = datetime.fromisoformat(started_at)
    rate = rate_from_tags(tags)
    if rate is None or started_at is None:
        return None
    price, tier = rate
    # GCE lowercases label keys, so `NodeType` arrives there as `nodetype`.
    lowered = {k.lower(): v for k, v in (tags or {}).items()}
    node_type = "zero-token-db" if str(lowered.get("zerotokennode")).lower() == "true" else lowered.get("nodetype")
    return instance_cost_item(
        name=name,
        node_type=node_type,
        price_per_hour=price,
        pricing_tier=tier,
        started_at=started_at,
        leaked=leaked,
    )


def report_cost_items(client: ArgusSCTClient, items: list[CostItem | None]) -> bool:
    """Send final per-resource costs. Returns whether anything was sent; never raises.

    Items are keyed by name in Argus, so re-sending one replaces it - a retry cannot
    double-count.
    """
    items = [item for item in items if item is not None]
    if not items:
        return False
    try:
        client.submit_cost_items(run_id=client.run_id, items=items)
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to report %d cost item(s) to Argus", len(items), exc_info=True)
        return False
    LOGGER.debug("Reported cost of %s to Argus", ", ".join(f"{i.name}=${i.cost:.2f}" for i in items))
    return True


def report_costs_from_tags(
    client: ArgusSCTClient,
    instances: Iterable[tuple[str, dict[str, str] | None, datetime | str | None]],
    leaked: bool,
) -> bool:
    """Price instances cleanup is terminating from their tags, and send their final cost.

    Takes `(name, tags, launch_time)` per instance. `leaked` is for the scheduled cloud sweep,
    which only reaches what a test failed to clean up; a job's own cleanup stage is the normal
    end of a run's resources and reports them as not leaked. Never raises: this runs in the
    middle of deleting cloud resources, and a cost report must never stop a deletion.
    """
    try:
        items = [cost_item_from_tags(name, tags, started_at, leaked) for name, tags, started_at in instances]
    except Exception:  # noqa: BLE001
        LOGGER.warning("Could not price instances for Argus", exc_info=True)
        return False
    return report_cost_items(client, items)
