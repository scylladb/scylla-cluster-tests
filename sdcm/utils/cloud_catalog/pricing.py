import json
from datetime import datetime, timedelta
from functools import lru_cache
from logging import getLogger
from pathlib import Path

import boto3
import requests
from mypy_boto3_pricing import PricingClient

from sdcm.utils.cloud_monitor.common import InstanceLifecycle

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


class AWSPricing:
    def __init__(self):
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

    @staticmethod
    @lru_cache(maxsize=None)
    def get_spot_instance_price(region_name, instance_type):
        """currently doesn't take AZ into consideration"""
        client = boto3.client("ec2", region_name=region_name)
        result = client.describe_spot_price_history(
            InstanceTypes=[instance_type],
            ProductDescriptions=["Linux/UNIX (Amazon VPC)", "Linux/UNIX"],
            StartTime=datetime.now() - timedelta(hours=3),
            EndTime=datetime.now(),
        )
        prices = result["SpotPriceHistory"]
        if prices:
            all_prices = [float(p["SpotPrice"]) for p in prices]
            return sum(all_prices) / len(all_prices)
        else:
            LOGGER.warning("Spot price not found for '%s' in '%s':\n%s", instance_type, region_name, result)
            return 0

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
    prices = {
        InstanceLifecycle.SPOT: {
            "n1-standard-1": 0.0100,
            "n1-standard-2": 0.0200,
            "n1-standard-4": 0.0400,
            "n1-standard-8": 0.0800,
            "n1-standard-16": 0.1600,
            "n1-standard-32": 0.3200,
            "n1-standard-64": 0.6400,
            "n1-standard-96": 0.9600,
            "n2-standard-2": 0.0235,
            "n2-standard-4": 0.0470,
            "n2-standard-8": 0.0940,
            "n2-standard-16": 0.1880,
            "n2-standard-32": 0.3760,
            "n2-standard-48": 0.5640,
            "n2-standard-64": 0.7520,
            "n2-standard-80": 0.9400,
            "n2-highmem-2": 0.0317,
            "n2-highmem-4": 0.0634,
            "n2-highmem-8": 0.1268,
            "n2-highmem-16": 0.2536,
            "n2-highmem-32": 0.5073,
            "n2-highmem-48": 0.7609,
            "n2-highmem-64": 1.0145,
            "n2-highmem-80": 1.2681,
            "n2-highcpu-2": 0.0173,
            "n2-highcpu-4": 0.0347,
            "n2-highcpu-8": 0.0694,
            "n2-highcpu-16": 0.1388,
            "n2-highcpu-32": 0.2776,
            "n2-highcpu-48": 0.4164,
            "n2-highcpu-64": 0.5552,
            "n2-highcpu-80": 0.6940,
            "e2-standard-2": 0.02010,
            "e2-standard-4": 0.04021,
            "e2-standard-8": 0.08041,
            "e2-standard-16": 0.16083,
            "e2-micro": 0.00251,
            "e2-small": 0.00503,
            "e2-medium": 0.01005,
            "f1-micro": 0.0035,
            "g1-small": 0.0070,
            "m1-ultramem-40": 1.3311,
            "m1-ultramem-80": 2.6622,
            "m1-ultramem-160": 5.3244,
            "m1-megamem-96": 2.2600,
            "n1-highmem-2": 0.0250,
            "n1-highmem-4": 0.0500,
            "n1-highmem-8": 0.1000,
            "n1-highmem-16": 0.2000,
            "n1-highmem-32": 0.4000,
            "n1-highmem-64": 0.8000,
            "n1-highmem-96": 1.2000,
            "c2-standard-4": 0.0505,
            "c2-standard-8": 0.1011,
            "c2-standard-16": 0.2021,
            "c2-standard-30": 0.3790,
            "c2-standard-60": 0.7579,
            "n2d-standard-2": 0.0204,
            "n2d-standard-4": 0.0409,
            "n2d-standard-8": 0.0818,
            "n2d-standard-16": 0.1636,
            "n2d-standard-32": 0.3271,
            "n2d-standard-48": 0.4907,
            "n2d-standard-64": 0.6543,
            "n2d-standard-80": 0.8178,
            "n2d-standard-96": 0.9814,
            "n2d-standard-128": 1.3085,
            "n2d-standard-224": 2.2900,
        },
    }

    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state == "running":
            if lifecycle == InstanceLifecycle.ON_DEMAND:
                price = _catalog_price("gce", region, instance_type)
                if price is not None:
                    return price
                LOGGER.warning("No catalog price for GCE %s in %s", instance_type, region)
                return 0
            if lifecycle == InstanceLifecycle.SPOT:
                return self.prices[InstanceLifecycle.SPOT].get(instance_type, 0)
            LOGGER.warning("No price for %s", instance_type)
            return 0
        else:
            return 0


class AzurePricing:
    def get_instance_price(self, region, instance_type, state, lifecycle):
        if state == "running":
            price = _catalog_price("azure", region, instance_type)
            if price is not None and lifecycle == InstanceLifecycle.ON_DEMAND:
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
            except (KeyError, IndexError):
                LOGGER.warning("Failed to get price from prices: %s", prices)
                return 0
        else:
            return 0

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_sku_prices(instance_type: str, region):
        resp = requests.get(
            f"https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines' "
            f"and armSkuName eq '{instance_type}' and armRegionName eq '{region}' and priceType eq 'consumption'"
        )
        if not resp.ok:
            LOGGER.warning("Failed to fetch prices for %s in location: %s", instance_type, region)
            return []
        return [item for item in resp.json()["Items"] if "Windows" not in item["productName"]]


class OCIPricing:
    """OCI pricing - returns 0 as OCI does not have a public retail pricing API like AWS/Azure."""

    def get_instance_price(self, region, instance_type, state, lifecycle):
        # OCI does not expose a simple public pricing API for compute instances.
        # Return 0 to indicate pricing is unavailable rather than failing.
        return 0
