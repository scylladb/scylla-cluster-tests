import json
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import lru_cache
from logging import getLogger
from pathlib import Path
from statistics import mean

import boto3
import requests
from mypy_boto3_pricing import PricingClient

from sdcm.utils.cloud_catalog.lifecycle import InstanceLifecycle

LOGGER = getLogger(__name__)

_CATALOG = None


def _get_catalog():
    global _CATALOG  # noqa: PLW0603
    if _CATALOG is None:
        from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog  # noqa: PLC0415 — avoid circular import

        catalog_dir = Path(__file__).parent.parent.parent.parent / "data" / "instance_catalog"
        if catalog_dir.exists():
            _CATALOG = InstanceCatalog.from_directory(catalog_dir)
        else:
            _CATALOG = InstanceCatalog()
    return _CATALOG


def _catalog_price(cloud: str, region: str, instance_type: str) -> float | None:
    catalog = _get_catalog()
    inst = catalog.get_instance(cloud, instance_type)
    if inst:
        return inst.get_price(region)
    return None


def _catalog_spot_price(cloud: str, region: str, instance_type: str) -> float | None:
    catalog = _get_catalog()
    inst = catalog.get_instance(cloud, instance_type)
    if inst:
        return inst.get_spot_price(region)
    return None


class AWSPricing:
    # AWS spot moves a few times a day per AZ, so a short window keeps a long test roughly
    # current while still collapsing a provisioning burst into a single call.
    SPOT_PRICE_TTL = 15 * 60

    def __init__(self):
        # region -> (fetched_at, {instance_type: {az: price}})
        self._spot_cache: dict[str, tuple[float, dict[str, dict[str, float]]]] = {}
        self.pricing_client: PricingClient = boto3.client("pricing", region_name="us-east-1")

    @lru_cache(maxsize=None)
    def get_on_demand_instance_price(self, region_name: str, instance_type: str):
        price = _catalog_price("aws", region_name, instance_type)
        if price is not None:
            return price

        regions_names_map = {
            "af-south-1": "Africa (Cape Town)",
            "ap-east-1": "Asia Pacific (Hong Kong)",
            "ap-south-2": "Asia Pacific (Hyderabad)",
            "ap-southeast-3": "Asia Pacific (Jakarta)",
            "ap-southeast-4": "Asia Pacific (Melbourne)",
            "ap-south-1": "Asia Pacific (Mumbai)",
            "ap-northeast-3": "Asia Pacific (Osaka)",
            "ap-northeast-2": "Asia Pacific (Seoul)",
            "ap-southeast-1": "Asia Pacific (Singapore)",
            "ap-southeast-2": "Asia Pacific (Sydney)",
            "ap-northeast-1": "Asia Pacific (Tokyo)",
            "ca-central-1": "Canada (Central)",
            "eu-central-1": "EU (Frankfurt)",
            "eu-west-1": "EU (Ireland)",
            "eu-west-2": "EU (London)",
            "eu-south-1": "EU (Milan)",
            "eu-west-3": "EU (Paris)",
            "eu-north-1": "EU (Stockholm)",
            "eu-south-2": "Europe (Spain)",
            "eu-central-2": "Europe (Zurich)",
            "il-central-1": "Israel (Tel Aviv)",
            "me-south-1": "Middle East (Bahrain)",
            "me-central-1": "Middle East (UAE)",
            "sa-east-1": "South America (Sao Paulo)",
            "us-east-1": "US East (N. Virginia)",
            "us-east-2": "US East (Ohio)",
            "us-west-1": "US West (N.California)",
            "us-west-2": "US West (Oregon)",
        }

        response = self.pricing_client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "location", "Value": regions_names_map[region_name]},
            ],
            MaxResults=10,
        )
        assert response["PriceList"], f"failed to get price for {instance_type} in {region_name}"
        price = response["PriceList"][0]
        price_dimensions = next(iter(json.loads(price)["terms"]["OnDemand"].values()))["priceDimensions"]
        instance_price = next(iter(price_dimensions.values()))["pricePerUnit"]["USD"]
        return float(instance_price)

    def get_spot_instance_prices(self, region_name: str, instance_types: Sequence[str]) -> dict[str, float]:
        """Current spot price per instance type, averaged over the region's AZs.

        `DescribeSpotPriceHistory` accepts a *list* of instance types, so the unit of work
        is a region, not an instance type: pricing three types costs the same one call as
        pricing fifteen. Everything else here exists to keep it that way — callers ask for
        every type they need at once, and a whole cluster costs one call no matter how many
        nodes it has.

        Results are cached per region for `SPOT_PRICE_TTL`, so a burst of node creations
        shares one lookup while a long test still picks up drift. AWS spot moves a few times
        a day per AZ, which is why this is never cached to disk.

        Prices vary across AZs by tens of percent, and which AZ a node lands in is not known
        here, so the mean is the honest answer; `get_spot_price_spread` exposes the rest.
        """
        wanted = [t for t in dict.fromkeys(instance_types) if t]
        if not wanted:
            return {}

        now = time.monotonic()
        cached = self._spot_cache.get(region_name)
        if cached and now - cached[0] < self.SPOT_PRICE_TTL and all(t in cached[1] for t in wanted):
            return {t: mean(cached[1][t].values()) for t in wanted if cached[1][t]}

        client = boto3.client("ec2", region_name=region_name)
        timestamp = datetime.now(UTC)
        result = client.describe_spot_price_history(
            InstanceTypes=wanted,
            ProductDescriptions=["Linux/UNIX (Amazon VPC)", "Linux/UNIX"],
            # A zero-width window asks for the price as it stands now rather than a history
            # to average, which keeps the response to one row per type per AZ.
            StartTime=timestamp,
            EndTime=timestamp,
            MaxResults=1000,
        )

        per_az: dict[str, dict[str, float]] = {t: {} for t in wanted}
        for entry in result.get("SpotPriceHistory", []):
            itype = entry["InstanceType"]
            if itype in per_az:
                per_az[itype][entry["AvailabilityZone"]] = float(entry["SpotPrice"])

        merged = dict(cached[1]) if cached and now - cached[0] < self.SPOT_PRICE_TTL else {}
        merged.update(per_az)
        self._spot_cache[region_name] = (now, merged)

        missing = [t for t, azs in per_az.items() if not azs]
        if missing:
            LOGGER.warning("No spot price returned for %s in '%s'", ", ".join(missing), region_name)
        return {t: mean(azs.values()) for t, azs in per_az.items() if azs}

    def get_spot_instance_price(self, region_name: str, instance_type: str) -> float:
        """Spot price for a single instance type. Returns 0 when unknown, as callers expect."""
        return self.get_spot_instance_prices(region_name, [instance_type]).get(instance_type, 0)

    def get_spot_price_spread(self, region_name: str, instance_type: str) -> float | None:
        """Relative min-to-max spread across AZs, or None if not looked up yet.

        Reported rather than smoothed away: it is routinely wider than the error from a
        stale price, so quoting a single spot figure without it is false precision.
        """
        cached = self._spot_cache.get(region_name)
        azs = cached[1].get(instance_type) if cached else None
        if not azs or len(azs) < 2:
            return None
        low, high = min(azs.values()), max(azs.values())
        return (high - low) / low if low else None

    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state == "running":
            if lifecycle == InstanceLifecycle.ON_DEMAND:
                return self.get_on_demand_instance_price(region_name=region, instance_type=instance_type)
            if lifecycle == InstanceLifecycle.SPOT:
                spot_price = self.get_spot_instance_price(region_name=region, instance_type=instance_type)
                return spot_price
            else:
                raise Exception("Unsupported instance lifecycle")
        else:
            return 0


class GCEPricing:
    """GCE pricing.

    Both on-demand and spot come from the checked-in catalog: GCP publishes spot rates as
    administered prices that move at most monthly, so there is nothing to gain from a live
    lookup, and its pricing API has no server-side filter — the only way to ask is to list
    every Compute Engine SKU. That belongs in catalog generation, not in a test run.
    """

    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state == "running":
            if lifecycle == InstanceLifecycle.ON_DEMAND:
                price = _catalog_price("gce", region, instance_type)
                if price is not None:
                    return price
                LOGGER.warning("No catalog price for GCE %s in %s", instance_type, region)
                return 0
            if lifecycle == InstanceLifecycle.SPOT:
                # GCP sets spot rates administratively and changes them at most monthly, and
                # the discount is strongly region-dependent (roughly 40%-78%), so the answer
                # comes from the per-region catalog rather than a global ratio or a live call.
                price = _catalog_spot_price("gce", region, instance_type)
                if price is not None:
                    return price
                LOGGER.warning("No catalog spot price for GCE %s in %s", instance_type, region)
                return 0
            LOGGER.warning("No price for %s", instance_type)
            return 0
        else:
            return 0


class AzurePricing:
    """Azure pricing.

    Both rates come from the catalog when it has them. Spot is free to catalogue: the
    Retail Prices response the generator already fetches carries the Spot rows alongside the
    on-demand ones, so there is no extra call to make here or at generation time.
    """

    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state == "running":
            if lifecycle == InstanceLifecycle.ON_DEMAND:
                price = _catalog_price("azure", region, instance_type)
            else:
                price = _catalog_spot_price("azure", region, instance_type)
            if price is not None:
                return price
            prices = self._get_sku_prices(instance_type, region)
            if not prices:
                return 0
            try:
                if lifecycle == InstanceLifecycle.ON_DEMAND:
                    return [
                        price["retailPrice"]
                        for price in prices
                        if "Spot" not in price["meterName"] and "Low" not in price["meterName"]
                    ][0]
                else:
                    return [price["retailPrice"] for price in prices if "Spot" in price["meterName"]][0]
            except KeyError, IndexError:
                LOGGER.warning("Failed to get price from prices: %s", prices)
                return 0
        else:
            return 0

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_sku_prices(instance_type: str, region):
        resp = requests.get(
            f"https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines' "
            f"and armSkuName eq '{instance_type}' and armRegionName eq '{region}' and priceType eq 'consumption'",
            timeout=30,
        )
        if not resp.ok:
            LOGGER.warning("Failed to fetch prices for %s in location: %s", instance_type, region)
            return []
        return [item for item in resp.json()["Items"] if "Windows" not in item["productName"]]


class OCIPricing:
    """OCI pricing, from the catalog plus a fixed preemptible ratio.

    OCI has no spot market. Preemptible instances are a flat, deliberately predictable
    discount off the on-demand rate rather than a demand-driven price, and Oracle's public
    pricing API lists no preemptible products at all — so the rate is derived, not fetched.

    The on-demand side is a plain catalog lookup. It used to return 0 unconditionally, which
    left OCI unpriced even though `oci.yaml` has carried real prices all along.
    """

    #: Oracle prices preemptible capacity at half the on-demand rate, uniformly.
    #: https://blogs.oracle.com/cloud-infrastructure/post/announcing-preemptible-instances-a-new-kind-of-compute-instance-available-at-a-50-discount
    PREEMPTIBLE_RATIO = 0.5

    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state != "running":
            return 0
        price = _catalog_price("oci", region, instance_type)
        if price is None:
            LOGGER.warning("No catalog price for OCI %s in %s", instance_type, region)
            return 0
        if lifecycle == InstanceLifecycle.SPOT:
            return price * self.PREEMPTIBLE_RATIO
        return price
