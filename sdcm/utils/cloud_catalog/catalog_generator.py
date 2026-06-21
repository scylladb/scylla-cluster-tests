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

"""Catalog generator for cloud instance types.

Provides functions to generate InstanceTypeInfo catalogs from live cloud APIs
for AWS, GCE, Azure, and OCI. Intended as a developer tool to refresh the
static YAML catalog files in data/instance_catalog/.

Cloud SDK imports are deferred inside each function (try/except at function
level) because these SDKs are optional dependencies — not all clouds are
needed in every environment.
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
from pathlib import Path
from typing import Any

import requests
import yaml

from sdcm.utils.cloud_catalog.instance_catalog import InstanceTypeInfo

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------

_AWS_ARCH_MAP = {
    "x86_64": "x86_64",
    "i386": "x86_64",
    "arm64": "arm64",
}


def _aws_get_price(pricing_client: Any, instance_type: str, region: str) -> float | None:
    """Fetch on-demand Linux price for an instance type from AWS Pricing API.

    Args:
        pricing_client: boto3 pricing client (must be in us-east-1).
        instance_type: AWS instance type string (e.g. "i8g.2xlarge").
        region: AWS region long name (e.g. "US East (N. Virginia)").

    Returns:
        Hourly price as float, or None if not found.
    """
    try:
        response = pricing_client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": region},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            ],
            MaxResults=1,
        )
        price_list = response.get("PriceList", [])
        if not price_list:
            return None
        product = json.loads(price_list[0])
        on_demand = product.get("terms", {}).get("OnDemand", {})
        for term in on_demand.values():
            for dim in term.get("priceDimensions", {}).values():
                price_str = dim.get("pricePerUnit", {}).get("USD", "")
                if price_str:
                    return float(price_str)
    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOG.debug("Could not fetch price for %s: %s", instance_type, exc)
    return None


# Mapping from AWS region code to the long name used by the Pricing API.
_AWS_REGION_LONG_NAMES: dict[str, str] = {
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "eu-central-1": "EU (Frankfurt)",
    "eu-north-1": "EU (Stockholm)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "sa-east-1": "South America (Sao Paulo)",
    "ca-central-1": "Canada (Central)",
}


def generate_aws_catalog(  # noqa: PLR0914
    families: list[str], region: str = "us-east-1", regions: list[str] | None = None
) -> list[InstanceTypeInfo]:
    """Generate InstanceTypeInfo list for AWS instance families.

    Queries the EC2 DescribeInstanceTypes API and the AWS Pricing API to build
    a catalog of instance types for the requested families.

    Cloud SDK imports are inside this function because boto3 is an optional
    dependency in some environments (e.g. GCE-only setups).

    Args:
        families: List of instance family prefixes to include (e.g. ["i8g", "i7i"]).
        region: AWS region to query for instance specs (default "us-east-1").
        regions: List of regions for pricing. If None, uses [region].

    Returns:
        List of InstanceTypeInfo, one per matching instance type. Returns an
        empty list if credentials are missing or an error occurs.
    """
    try:
        import boto3  # noqa: PLC0415
        from botocore.exceptions import BotoCoreError, ClientError  # noqa: PLC0415
    except ImportError:
        LOG.error("boto3 is not installed — cannot generate AWS catalog")
        return []

    price_regions = regions or [region]
    results: list[InstanceTypeInfo] = []

    try:
        ec2 = boto3.client("ec2", region_name=region)
        pricing = boto3.client("pricing", region_name="us-east-1")

        for family in families:
            paginator = ec2.get_paginator("describe_instance_types")
            pages = paginator.paginate(Filters=[{"Name": "instance-type", "Values": [f"{family}.*"]}])
            for page in pages:
                for it in page.get("InstanceTypes", []):
                    itype = it["InstanceType"]
                    vcpus = it.get("VCpuInfo", {}).get("DefaultVCpus", 0)
                    mem_mib = it.get("MemoryInfo", {}).get("SizeInMiB", 0)
                    memory_gb = round(mem_mib / 1024, 2)

                    # Local storage
                    storage = it.get("InstanceStorageInfo", {})
                    local_disk_gb = float(storage.get("TotalSizeInGB", 0))
                    disks = storage.get("Disks", [])
                    local_disk_count = sum(d.get("Count", 0) for d in disks)

                    # Architecture — take first supported arch
                    raw_archs = it.get("ProcessorInfo", {}).get("SupportedArchitectures", ["x86_64"])
                    arch = _AWS_ARCH_MAP.get(raw_archs[0], "x86_64")

                    # Fetch prices for all requested regions
                    prices: dict[str, float] = {}
                    for pr in price_regions:
                        long_name = _AWS_REGION_LONG_NAMES.get(pr)
                        if not long_name:
                            LOG.warning("No long name mapping for region %s — skipping pricing", pr)
                            continue
                        p = _aws_get_price(pricing, itype, long_name)
                        if p is not None:
                            prices[pr] = p

                    # Single region → float; multiple → dict; none → None
                    if len(prices) == 1:
                        price_per_hour: float | dict[str, float] | None = next(iter(prices.values()))
                    elif prices:
                        price_per_hour = prices
                    else:
                        price_per_hour = None

                    results.append(
                        InstanceTypeInfo(
                            instance_type=itype,
                            cloud="aws",
                            family=family,
                            vcpus=vcpus,
                            memory_gb=memory_gb,
                            local_disk_gb=local_disk_gb,
                            local_disk_count=local_disk_count,
                            arch=arch,
                            price_per_hour=price_per_hour,
                        )
                    )

    except (BotoCoreError, ClientError) as exc:
        LOG.error("AWS API error while generating catalog: %s", exc)
    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOG.error("Unexpected error generating AWS catalog: %s", exc)

    LOG.info("AWS catalog: found %d instances for families %s", len(results), families)
    return results


# ---------------------------------------------------------------------------
# GCE
# ---------------------------------------------------------------------------

# Known local SSD specs for GCE families that have local NVMe.
# GCE machine-type API does not expose local SSD info; these are from docs.
_GCE_LOCAL_SSD_SPECS: dict[str, dict[str, Any]] = {
    "n2": {"local_disk_gb": 1500.0, "local_disk_count": 4},
    "n2d": {"local_disk_gb": 375.0, "local_disk_count": 1},
}

_Z3_DISK_SIZE_GIB = 3000.0
_Z3_METAL_DISK_SIZE_GIB = 6000.0

# z3 local SSD partition counts per instance (from GCE docs).
# Used as fallback when the API doesn't return bundled_local_ssds.
_Z3_PARTITION_COUNTS: dict[str, int] = {
    "z3-highmem-8-highlssd": 2,
    "z3-highmem-16-highlssd": 4,
    "z3-highmem-22-highlssd": 6,
    "z3-highmem-32-highlssd": 8,
    "z3-highmem-44-highlssd": 12,
    "z3-highmem-88-highlssd": 24,
    "z3-highmem-192-highlssd-metal": 24,
    "z3-highmem-14-standardlssd": 2,
    "z3-highmem-22-standardlssd": 4,
    "z3-highmem-44-standardlssd": 8,
    "z3-highmem-88-standardlssd": 16,
    "z3-highmem-176-standardlssd": 16,
}

# GCE resource-based pricing: per-vCPU/hr and per-GB-RAM/hr rates (USD, us-east1).
# Derived from DevZero pricing aggregator cross-validated against GCP published prices.
_GCE_GENERAL_PURPOSE_PRICING_URL = "https://cloud.google.com/products/compute/pricing/general-purpose"

# Section name -> actual instance types contained (Google's naming is shifted by one)
_GCE_SECTION_MAP: dict[str, list[str]] = {
    # Each family has 3 data sections with per-instance per-region prices.
    # The section name does NOT match the instance type sub-family it contains.
    # Mapping discovered by inspecting the AF_initDataCallback data blob.
    "e2": [
        "e2-machine-types",  # contains e2-standard-*
        "e2-standard-machine-types",  # contains e2-highmem-*
        "e2-high-memory-machine-types",  # contains e2-highcpu-*
    ],
    "n2": [
        "n2-machine-types",  # contains n2-standard-*
        "n2-standard-machine-types",  # contains n2-highmem-*
        "n2-high-memory-machine-types",  # contains n2-highcpu-*
    ],
    "n2d": [
        "n2d-machine-types",  # contains n2d-standard-*
        "n2d-standard-machine-types",  # contains n2d-highmem-*
        "n2d-high-memory-machine-types",  # contains n2d-highcpu-*
    ],
}

_GCE_Z3_PRICING_URL = "https://cloud.google.com/products/compute/pricing/storage-optimized"

# z3 machine type names by sub-family (used for scraping validation).
_Z3_HIGHLSSD_TYPES = [
    "z3-highmem-8-highlssd",
    "z3-highmem-16-highlssd",
    "z3-highmem-22-highlssd",
    "z3-highmem-32-highlssd",
    "z3-highmem-44-highlssd",
    "z3-highmem-88-highlssd",
    "z3-highmem-192-highlssd-metal",
]
_Z3_STANDARDLSSD_TYPES = [
    "z3-highmem-14-standardlssd",
    "z3-highmem-22-standardlssd",
    "z3-highmem-44-standardlssd",
    "z3-highmem-88-standardlssd",
    "z3-highmem-176-standardlssd",
]


def _scrape_gce_z3_prices() -> dict[str, dict[str, float]]:  # noqa: PLR0914
    """Scrape per-region bundled z3 on-demand prices from GCE pricing page.

    Returns:
        Dict mapping machine type name -> {region: price_per_hour}.
        Empty dict on fetch/parse failure.
    """
    try:
        req = urllib.request.Request(_GCE_Z3_PRICING_URL, headers={"User-Agent": "SCT-CatalogGenerator/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            page_html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as exc:
        LOG.error("Failed to fetch GCE z3 pricing page: %s", exc)
        return {}

    # Extract the AF_initDataCallback data blob containing pricing tables.
    af_match = re.search(r"AF_initDataCallback\((.*?)\);", page_html, re.DOTALL)
    if not af_match:
        LOG.error("GCE pricing page: AF_initDataCallback not found")
        return {}

    data_str = af_match.group(1)
    results: dict[str, dict[str, float]] = {}

    # Extract prices for each z3 sub-family section.
    # Page structure: machine type prices appear before their region label.
    # Sections are bounded by markers like "z3-highmem-with-highlssd".
    for section_types, section_marker in [
        (_Z3_STANDARDLSSD_TYPES, '"z3-highmem-with-standardlssd"'),
        (_Z3_HIGHLSSD_TYPES, '"z3-highmem-with-highlssd"'),
    ]:
        first_type = section_types[0]
        section_start = data_str.find(first_type)
        section_end = data_str.find(section_marker)
        if section_start < 0 or section_end < 0 or section_start >= section_end:
            LOG.warning("GCE z3 pricing: could not find section for %s", first_type)
            continue

        section = data_str[section_start:section_end]

        # For each machine type, find all "$price / 1 hour" entries and map to regions.
        for machine_type in section_types:
            # Find all (position, price) for this machine type
            price_pattern = re.escape(machine_type) + r".*?\$(\d+\.\d+) / 1 hour"
            price_positions = [(m.start(), float(m.group(1))) for m in re.finditer(price_pattern, section)]

            # Find all region labels: "Location (region-id)"
            region_re = r'"[\w\s/]+ \(((?:us|europe|asia|australia|southamerica|northamerica|me|africa)-[\w-]+\d)\)"'
            region_positions = [(m.start(), m.group(1)) for m in re.finditer(region_re, section)]

            # Map each price to the next region label that follows it.
            all_events: list[tuple[str, int, str | float]] = []
            for pos, price in price_positions:
                all_events.append(("P", pos, price))
            for pos, region in region_positions:
                all_events.append(("R", pos, region))
            all_events.sort(key=lambda x: x[1])

            region_prices: dict[str, float] = {}
            last_price: float | None = None
            for etype, _, val in all_events:
                if etype == "P":
                    last_price = val  # type: ignore[assignment]
                elif etype == "R" and last_price is not None:
                    region = val  # type: ignore[assignment]
                    if region not in region_prices:
                        region_prices[region] = last_price
                    last_price = None

            if region_prices:
                results[machine_type] = region_prices

    if results:
        LOG.info(
            "GCE z3 pricing: scraped %d machine types across %d regions",
            len(results),
            max(len(v) for v in results.values()),
        )
    else:
        LOG.error("GCE z3 pricing: scraping returned no results")

    return results


def _scrape_gce_general_prices(families: list[str]) -> dict[str, dict[str, float]]:
    """Scrape per-region on-demand prices for general-purpose GCE families.

    Parses the AF_initDataCallback data blob from the GCE general-purpose
    pricing page. Each section contains per-instance per-region prices
    organized as repeated tables separated by region labels.

    Args:
        families: Family prefixes to scrape (e.g. ["e2", "n2", "n2d"]).

    Returns:
        Dict mapping instance_type_name -> {region: price_per_hour}.
        Empty dict on fetch/parse failure.
    """
    scraped_families = [f for f in families if f in _GCE_SECTION_MAP]
    if not scraped_families:
        return {}

    try:
        req = urllib.request.Request(
            _GCE_GENERAL_PURPOSE_PRICING_URL,
            headers={"User-Agent": "SCT-CatalogGenerator/1.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            page_html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as exc:
        LOG.error("Failed to fetch GCE general-purpose pricing page: %s", exc)
        return {}

    af_match = page_html.find("AF_initDataCallback(")
    if af_match < 0:
        LOG.error("GCE general-purpose pricing: AF_initDataCallback not found")
        return {}

    af_data = page_html[af_match:]
    all_sections = [(m.start(), m.group(1)) for m in re.finditer(r'"([\w-]+-machine-types)"', af_data)]
    sec_positions = [p for p, _ in all_sections]

    results: dict[str, dict[str, float]] = {}

    for family in scraped_families:
        for section_name in _GCE_SECTION_MAP[family]:
            section_prices = _extract_section_on_demand_prices(af_data, section_name, sec_positions)
            for region, type_prices in section_prices.items():
                for instance_type, price in type_prices.items():
                    if instance_type not in results:
                        results[instance_type] = {}
                    results[instance_type][region] = price

    if results:
        LOG.info("GCE general-purpose pricing: scraped %d instance types", len(results))
    else:
        LOG.error("GCE general-purpose pricing: scraping returned no results")

    return results


def _extract_section_on_demand_prices(  # noqa: PLR0914
    af_data: str,
    section_name: str,
    sec_positions: list[int],
) -> dict[str, dict[str, float]]:
    """Extract per-region on-demand prices from one AF_initDataCallback section.

    Returns:
        Dict mapping region -> {instance_type: on_demand_price}.
    """
    sec_pos = af_data.find(f'"{section_name}"')
    if sec_pos < 0:
        return {}

    next_after = min((p for p in sec_positions if p > sec_pos), default=len(af_data))
    section = af_data[sec_pos:next_after]

    region_match = re.search(r"\[\[1,2,3\],\[(.*?)\]\]", section[:4000])
    if not region_match:
        return {}
    header_regions = re.findall(r"\(([\w-]+\d\w*)\)", region_match.group(1))
    if not header_regions:
        return {}

    region_labels = [
        (m.start(), m.group(1)) for m in re.finditer(r'"[\w\s/]+ \(([\w-]+\d\w*)\)"', section) if m.start() > 3000
    ]
    price_entries = [(m.start(), float(m.group(1))) for m in re.finditer(r"\$(\d+\.\d+) / 1 hour", section)]
    type_pattern = r"((?:e2|n2|n2d|c3|c4|t2a)-(?:standard|highcpu|highmem)-\d+)"
    type_positions = [(m.start(), m.group(1)) for m in re.finditer(type_pattern, section)]

    events: list[tuple[str, int, str | float]] = []
    for pos, region in region_labels:
        events.append(("R", pos, region))
    for pos, price in price_entries:
        events.append(("P", pos, price))
    events.sort(key=lambda x: x[1])

    price_to_type: dict[int, str | None] = {}
    for price_pos, _ in price_entries:
        closest = None
        for tp, tn in type_positions:
            if tp < price_pos:
                closest = tn
            else:
                break
        price_to_type[price_pos] = closest

    current_region = header_regions[0]
    region_type_prices: dict[str, dict[str, list[float]]] = {}

    for etype, pos, val in events:
        if etype == "R":
            current_region = val  # type: ignore[assignment]
        elif etype == "P":
            mt = price_to_type.get(pos)
            if mt:
                region_type_prices.setdefault(current_region, {}).setdefault(mt, []).append(val)  # type: ignore[arg-type]

    result: dict[str, dict[str, float]] = {}
    for region, types in region_type_prices.items():
        result[region] = {mt: prices[0] for mt, prices in types.items() if prices}

    return result


def generate_gce_catalog(  # noqa: PLR0914
    families: list[str],
    project: str = "",
    zone: str = "us-east1-b",
    regions: list[str] | None = None,
) -> list[InstanceTypeInfo]:
    """Generate InstanceTypeInfo list for GCE machine type families.

    Queries the GCE Machine Types API to build a catalog. Local SSD info is
    not exposed by the API and is filled from known specs for supported families.
    Pricing is fetched from the Cloud Billing Catalog API (resource-based:
    separate vCPU + RAM rates per family).

    Cloud SDK import is deferred because google-cloud-compute is optional.

    Args:
        families: List of machine type family prefixes (e.g. ["n2", "z3"]).
        project: GCP project ID. If empty, reads from application default credentials.
        zone: GCP zone to query (default "us-east1-b").

    Returns:
        List of InstanceTypeInfo. Returns empty list on credential/import error.
    """
    try:
        from google.cloud import compute_v1  # noqa: PLC0415
        from google.auth.exceptions import DefaultCredentialsError  # noqa: PLC0415
    except ImportError:
        LOG.error("google-cloud-compute is not installed — cannot generate GCE catalog")
        return []

    results: list[InstanceTypeInfo] = []
    z3_prices = _scrape_gce_z3_prices() if any(f.startswith("z3") for f in families) else {}
    general_prices = _scrape_gce_general_prices(families)

    try:
        from google.oauth2 import service_account as sa_module  # noqa: PLC0415
        from sdcm.keystore import KeyStore  # noqa: PLC0415

        info = KeyStore().get_gcp_credentials()
        credentials = sa_module.Credentials.from_service_account_info(info)
        client = compute_v1.MachineTypesClient(credentials=credentials)

        if not project:
            project = info.get("project_id", "")

        request = compute_v1.ListMachineTypesRequest(project=project, zone=zone)
        page_result = client.list(request=request)

        for machine_type in page_result:
            name = machine_type.name
            family = next((f for f in families if name.startswith(f)), None)
            if family is None:
                continue

            vcpus = machine_type.guest_cpus
            memory_gb = round(machine_type.memory_mb / 1024, 2)

            if name.startswith("z3"):
                bundled = getattr(machine_type, "bundled_local_ssds", None)
                partition_count = bundled.partition_count if bundled else _Z3_PARTITION_COUNTS.get(name, 0)
                disk_size = _Z3_METAL_DISK_SIZE_GIB if "metal" in name else _Z3_DISK_SIZE_GIB
                local_disk_count = partition_count
                local_disk_gb = partition_count * disk_size
                if "highlssd" in name:
                    family = "z3-highmem-highlssd"
                elif "standardlssd" in name:
                    family = "z3-highmem-standardlssd"
                else:
                    continue
            else:
                ssd_spec = _GCE_LOCAL_SSD_SPECS.get(family, {})
                local_disk_gb = float(ssd_spec.get("local_disk_gb", 0))
                local_disk_count = int(ssd_spec.get("local_disk_count", 0))

            arch = "arm64" if family in ("t2a",) else "x86_64"

            price: float | dict[str, float] | None = z3_prices.get(name) or general_prices.get(name)

            results.append(
                InstanceTypeInfo(
                    instance_type=name,
                    cloud="gce",
                    family=family,
                    vcpus=vcpus,
                    memory_gb=memory_gb,
                    local_disk_gb=local_disk_gb,
                    local_disk_count=local_disk_count,
                    arch=arch,
                    price_per_hour=price,
                )
            )

    except (DefaultCredentialsError, Exception) as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOG.error("GCE credentials/catalog error: %s", exc)

    LOG.info("GCE catalog: found %d instances for families %s", len(results), families)
    return results


# ---------------------------------------------------------------------------
# Azure
# ---------------------------------------------------------------------------

# Azure VM family memory-per-vCPU ratios (GB RAM per vCPU) and local disk info.
# Source: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/overview
_AZURE_FAMILY_SPECS: dict[str, dict[str, Any]] = {
    "Standard_L": {
        "mem_per_vcpu": 8.0,  # Lsv2/Lsv3/Lsv4: 8 GB/vCPU
        "local_disk_per_vcpu": 240.0,  # ~1.92 TB NVMe per 8 vCPUs
        "disk_count_divisor": 8,  # 1 NVMe per 8 vCPUs
    },
    "Standard_D": {
        "mem_per_vcpu": 4.0,  # D-series: 4 GB/vCPU (standard)
        "local_disk_per_vcpu": 0.0,
        "disk_count_divisor": 0,
    },
    "Standard_E": {
        "mem_per_vcpu": 8.0,  # E-series: 8 GB/vCPU (memory-optimized)
        "local_disk_per_vcpu": 0.0,
        "disk_count_divisor": 0,
    },
    "Standard_F": {
        "mem_per_vcpu": 2.0,  # F-series: 2 GB/vCPU (compute-optimized)
        "local_disk_per_vcpu": 0.0,
        "disk_count_divisor": 0,
    },
}

# Azure VM sub-family local NVMe disk specs, keyed by suffix pattern.
# In Azure naming, "d" before "s" in the suffix (e.g. "pds", "ds", "ads")
# indicates the VM has local NVMe temp storage.
# Source: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dpdsv6-series
#         https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dpdsv5-series
# Used as fallback when Azure Compute Management API is unavailable.
_AZURE_LOCAL_DISK_SUFFIX_SPECS: dict[str, dict[str, float]] = {
    # Dpdsv6: ARM Cobalt 100 + NVMe, 55 GiB per vCPU, 1 disk per 8 vCPUs
    "pds_v6": {"local_disk_per_vcpu": 55.0, "disk_count_divisor": 8},
    # Dpdsv5: ARM Ampere Altra + NVMe, 37.5 GiB per vCPU, 1 disk per 24 vCPUs
    "pds_v5": {"local_disk_per_vcpu": 37.5, "disk_count_divisor": 24},
}

# Regex to extract vCPU count from Azure SKU names.
# Examples:
#   Standard_L8s_v3     → 8
#   Standard_L48as_v4   → 48
#   Standard_D16ds_v5   → 16
#   Standard_E32-16s_v5 → 32 (full vCPUs; -16 is a constrained variant)
#   Standard_DC8as_v5   → 8 (DC = confidential compute D-series)
#   Standard_E372ids_v7 → 372
_AZURE_VCPU_RE = re.compile(
    r"Standard_"
    r"[A-Z]+"  # family letter(s): L, D, E, DC, EC, etc.
    r"(\d+)"  # vCPU count (first number after family letters)
)


def _azure_parse_sku_specs(sku_name: str, family_prefix: str) -> dict[str, Any]:
    """Parse approximate specs from an Azure SKU name using known family ratios.

    This is a fallback when the Azure Compute Management API is unavailable.
    It extracts vCPU count from the SKU name and infers memory and disk from
    known family ratios. For sub-families with local NVMe temp storage (suffix
    containing "d" before "s", e.g. "pds_v6"), applies suffix-specific disk specs.

    Args:
        sku_name: Azure ARM SKU name (e.g. "Standard_L8s_v3").
        family_prefix: The family prefix used to look up ratios (e.g. "Standard_L").

    Returns:
        Dict with keys: vcpus, memory_gb, local_disk_gb, local_disk_count.
        Returns zeros if parsing fails.
    """
    match = _AZURE_VCPU_RE.match(sku_name)
    if not match:
        return {"vcpus": 0, "memory_gb": 0.0, "local_disk_gb": 0.0, "local_disk_count": 0}

    vcpus = int(match.group(1))
    family_specs = _AZURE_FAMILY_SPECS.get(family_prefix, {})
    mem_per_vcpu = family_specs.get("mem_per_vcpu", 4.0)
    memory_gb = round(vcpus * mem_per_vcpu, 2)

    local_disk_per_vcpu = family_specs.get("local_disk_per_vcpu", 0.0)
    disk_divisor = family_specs.get("disk_count_divisor", 0)

    after_digits = re.sub(r"^Standard_[A-Z]+\d+", "", sku_name)
    for suffix, suffix_specs in _AZURE_LOCAL_DISK_SUFFIX_SPECS.items():
        if after_digits == suffix:
            local_disk_per_vcpu = suffix_specs["local_disk_per_vcpu"]
            disk_divisor = int(suffix_specs["disk_count_divisor"])
            break

    local_disk_gb = round(vcpus * local_disk_per_vcpu, 1) if local_disk_per_vcpu else 0.0
    local_disk_count = max(1, vcpus // disk_divisor) if disk_divisor else 0

    return {
        "vcpus": vcpus,
        "memory_gb": memory_gb,
        "local_disk_gb": local_disk_gb,
        "local_disk_count": local_disk_count,
    }


def _azure_fetch_prices(sku_prefix: str, region: str) -> dict[str, float]:
    """Fetch Azure retail prices for a SKU prefix from the Azure Retail Prices API.

    Args:
        sku_prefix: ARM SKU name prefix to filter (e.g. "Standard_L").
        region: Azure region (armRegionName) to filter (e.g. "eastus").

    Returns:
        Dict mapping armSkuName -> hourly price (USD).
    """
    prices: dict[str, float] = {}
    url = "https://prices.azure.com/api/retail/prices"
    params = {
        "$filter": (
            f"armRegionName eq '{region}' "
            f"and startswith(armSkuName, '{sku_prefix}') "
            "and priceType eq 'Consumption' "
            "and contains(productName, 'Windows') eq false"
        )
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOG.warning("Azure Retail Prices API error: %s", exc)
            break

        for item in data.get("Items", []):
            sku = item.get("armSkuName", "")
            sku_name = item.get("skuName", "")
            if "Spot" in sku_name or "Low Priority" in sku_name:
                continue
            price = item.get("retailPrice")
            if sku and price is not None and sku not in prices:
                prices[sku] = float(price)

        url = data.get("NextPageLink")
        params = {}  # NextPageLink already contains query params

    return prices


def generate_azure_catalog(  # noqa: PLR0914
    families: list[str],
    region: str = "eastus",
    series_patterns: list[str] | None = None,
    regions: list[str] | None = None,
) -> list[InstanceTypeInfo]:
    """Generate InstanceTypeInfo list for Azure VM families.

    Uses the Azure Retail Prices REST API (no credentials required) to fetch
    pricing and basic VM specs. For detailed vCPU/memory info, falls back to
    the Azure Compute Management client if credentials are available.

    Cloud SDK import is deferred because azure-mgmt-compute is optional.

    Args:
        families: List of Azure VM family prefixes (e.g. ["Standard_L", "Standard_D"]).
        region: Azure region (default "eastus").
        series_patterns: Optional list of regex patterns to filter instance names
            (e.g. ["s_v[345]$", "as_v[56]$"]). Only instances whose name matches
            at least one pattern are kept. If None, all instances are included.

    Returns:
        List of InstanceTypeInfo. Returns empty list on error.
    """
    results: list[InstanceTypeInfo] = []
    compiled_patterns = [re.compile(p) for p in series_patterns] if series_patterns else None
    price_regions = regions or [region]

    # Try to get detailed specs from Azure Compute Management API
    vm_specs: dict[str, dict[str, Any]] = {}
    try:
        from azure.identity import DefaultAzureCredential  # noqa: PLC0415
        from azure.mgmt.compute import ComputeManagementClient  # noqa: PLC0415

        subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
        if subscription_id:
            credential = DefaultAzureCredential()
            compute_client = ComputeManagementClient(credential, subscription_id)
            for vm_size in compute_client.virtual_machine_sizes.list(location=region):
                resource_disk_mb = getattr(vm_size, "resource_disk_size_in_mb", 0) or 0
                vm_specs[vm_size.name] = {
                    "vcpus": vm_size.number_of_cores,
                    "memory_gb": round(vm_size.memory_in_mb / 1024, 2),
                    "local_disk_gb": round(resource_disk_mb / 1024, 2),
                }
    except ImportError:
        LOG.debug("azure-mgmt-compute not installed; will use pricing API for specs")
    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOG.debug("Azure Compute API unavailable: %s", exc)

    try:
        for family in families:
            all_region_prices: dict[str, dict[str, float]] = {}
            for pr in price_regions:
                prices = _azure_fetch_prices(family, pr)
                for sku_name, price in prices.items():
                    all_region_prices.setdefault(sku_name, {})[pr] = price

            if not all_region_prices:
                LOG.warning("No Azure prices found for family prefix '%s' in %s", family, price_regions)
                continue

            for sku_name, region_prices in all_region_prices.items():
                specs = vm_specs.get(sku_name)
                if specs:
                    vcpus = specs["vcpus"]
                    memory_gb = specs["memory_gb"]
                    api_local_disk_gb = specs.get("local_disk_gb", 0.0)
                    parsed = _azure_parse_sku_specs(sku_name, family)
                    local_disk_gb = api_local_disk_gb if api_local_disk_gb > 0 else parsed["local_disk_gb"]
                    local_disk_count = parsed["local_disk_count"]
                else:
                    parsed = _azure_parse_sku_specs(sku_name, family)
                    vcpus = parsed["vcpus"]
                    memory_gb = parsed["memory_gb"]
                    local_disk_gb = parsed["local_disk_gb"]
                    local_disk_count = parsed["local_disk_count"]

                if vcpus == 0:
                    LOG.debug("Skipping %s: could not determine vCPU count", sku_name)
                    continue

                if compiled_patterns and not any(p.search(sku_name) for p in compiled_patterns):
                    continue

                after_digits = re.sub(r"^Standard_[A-Z]+\d+", "", sku_name)
                arch = "arm64" if after_digits.startswith("p") else "x86_64"

                if len(region_prices) == 1:
                    price_per_hour: float | dict[str, float] | None = next(iter(region_prices.values()))
                else:
                    price_per_hour = region_prices

                results.append(
                    InstanceTypeInfo(
                        instance_type=sku_name,
                        cloud="azure",
                        family=family,
                        vcpus=vcpus,
                        memory_gb=memory_gb,
                        local_disk_gb=local_disk_gb,
                        local_disk_count=local_disk_count,
                        arch=arch,
                        price_per_hour=price_per_hour,
                    )
                )

    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOG.error("Unexpected error generating Azure catalog: %s", exc)

    LOG.info("Azure catalog: found %d instances for families %s", len(results), families)
    return results


# ---------------------------------------------------------------------------
# OCI
# ---------------------------------------------------------------------------

# OCI pricing API endpoint (public, no auth required).
_OCI_PRICING_API = "https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/"

# Mapping from OCI shape series to pricing API display name patterns.
# Each series needs OCPU and Memory SKUs; DenseIO also needs NVMe.
_OCI_PRICING_SKU_MAP: dict[str, dict[str, str]] = {
    "E3": {"ocpu": "Standard - E3 - OCPU", "mem": "Standard - E3 - Memory"},
    "E5": {"ocpu": "Standard - E5 - OCPU", "mem": "Standard - E5 - Memory"},
    "A1": {"ocpu": "Standard - A1 - OCPU", "mem": "Standard - A1 - Memory"},
    "E4": {"ocpu": "Standard - E4 - OCPU", "mem": "Standard - E4  - Memory"},
    "E6": {"ocpu": "Standard - E6 - OCPU", "mem": "Standard - E6 - Memory"},
    "A2": {"ocpu": "Standard - A2 OCPU", "mem": "Standard - A2 Memory"},
    "A4": {"ocpu": "Standard - A4 - OCPU", "mem": "Standard - A4 - Memory"},
    "DenseIO.E4": {"ocpu": "Dense I/O - E4 - OCPU", "mem": "Dense I/O - E4 - Memory", "nvme": "Dense I/O - E4 - NVMe"},
    "DenseIO.E5": {"ocpu": "Dense I/O - E5 OCPU", "mem": "Dense I/O - E5 Memory", "nvme": "Dense I/O - E5 NVMe"},
}


def _oci_fetch_pricing() -> dict[str, dict[str, float]]:
    """Fetch OCI compute pricing from the public pricing API.

    Returns dict mapping series -> {ocpu_hr, mem_gb_hr, nvme_tb_hr (optional)}.
    Raises on failure.
    """
    resp = requests.get(_OCI_PRICING_API, params={"currencyCode": "USD"}, timeout=30)
    resp.raise_for_status()
    items = resp.json().get("items", [])

    def _find_rate(display_substr: str) -> float | None:
        for item in items:
            name = item.get("displayName", "")
            if "Cloud@Customer" in name or "Dedicated" in name or "Burstable" in name:
                continue
            if display_substr in name:
                prices = item.get("currencyCodeLocalizations", [{}])[0].get("prices", [])
                payg_rates = [p["value"] for p in prices if p.get("model") == "PAY_AS_YOU_GO" and p["value"] > 0]
                if payg_rates:
                    return max(payg_rates)
        return None

    result: dict[str, dict[str, float]] = {}
    for series, skus in _OCI_PRICING_SKU_MAP.items():
        ocpu = _find_rate(skus["ocpu"])
        mem = _find_rate(skus["mem"])
        if ocpu is None or mem is None:
            raise RuntimeError(f"OCI pricing API: missing rates for series '{series}'")
        entry: dict[str, float] = {"ocpu_hr": ocpu, "mem_gb_hr": mem}
        nvme_sku = skus.get("nvme")
        if nvme_sku:
            nvme = _find_rate(nvme_sku)
            if nvme is not None:
                entry["nvme_tb_hr"] = nvme
        result[series] = entry

    LOG.info("OCI pricing fetched: %s", {s: {k: f"${v}" for k, v in r.items()} for s, r in result.items()})
    return result


# OCI Flex shapes with their configurable OCPU ranges and per-OCPU memory/disk.
# instance_type uses SCT notation: "Shape:OCPUs" or "Shape:OCPUs:MemoryGB"
_OCI_FLEX_SHAPES: list[dict[str, Any]] = [
    {
        "base_shape": "VM.DenseIO.E5.Flex",
        "family": "DenseIO.E5",
        "series": "DenseIO.E5",
        "ocpu_options": [8, 16, 24, 32, 40, 48],
        "mem_per_ocpu": 12.0,
        "local_disk_gb_per_ocpu": 800.0,
        "local_disk_count": 1,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.DenseIO.E4.Flex",
        "family": "VM.DenseIO",
        "series": "DenseIO.E4",
        "ocpu_options": [8, 16, 24, 32],
        "mem_per_ocpu": 12.0,
        "local_disk_gb_per_ocpu": 800.0,
        "local_disk_count": 1,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.Standard.A1.Flex",
        "family": "VM.Standard",
        "series": "A1",
        "ocpu_options": [1, 2, 4, 8],
        "mem_per_ocpu": 6.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 6.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "arm64",
    },
    {
        "base_shape": "VM.Standard3.Flex",
        "family": "VM.Standard",
        "series": "E3",
        "ocpu_options": [1, 2, 4, 8, 16, 32],
        "mem_per_ocpu": 16.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 64.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.Standard.E4.Flex",
        "family": "VM.Standard",
        "series": "E4",
        "ocpu_options": [1, 2, 4, 8, 16, 32, 64],
        "mem_per_ocpu": 16.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 16.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.Standard.E5.Flex",
        "family": "VM.Standard",
        "series": "E5",
        "ocpu_options": [1, 2, 4, 8, 16, 32, 64, 94],
        "mem_per_ocpu": 16.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 16.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.Standard.E6.Flex",
        "family": "VM.Standard",
        "series": "E6",
        "ocpu_options": [1, 2, 4, 8, 16, 32, 64],
        "mem_per_ocpu": 16.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 16.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "x86_64",
    },
    {
        "base_shape": "VM.Standard.A4.Flex",
        "family": "VM.Standard",
        "series": "A4",
        "ocpu_options": [1, 2, 4, 8, 16, 32],
        "mem_per_ocpu": 16.0,
        "min_mem_per_ocpu": 1.0,
        "max_mem_per_ocpu": 16.0,
        "local_disk_gb_per_ocpu": 0.0,
        "local_disk_count": 0,
        "arch": "arm64",
    },
]

# OCI fixed (non-Flex) shapes — OCI OCPUs × 2 = vCPUs
_OCI_FIXED_SHAPES: list[dict[str, Any]] = [
    {
        "instance_type": "BM.DenseIO.E4.128",
        "family": "BM.DenseIO",
        "series": "DenseIO.E4",
        "ocpus": 128,
        "memory_gb": 2048.0,
        "local_disk_gb": 54400.0,
        "local_disk_count": 8,
        "arch": "x86_64",
    },
    {
        "instance_type": "BM.DenseIO.E5.128",
        "family": "BM.DenseIO",
        "series": "DenseIO.E5",
        "ocpus": 128,
        "memory_gb": 2048.0,
        "local_disk_gb": 54400.0,
        "local_disk_count": 8,
        "arch": "x86_64",
    },
    {
        "instance_type": "BM.DenseIO2.52",
        "family": "BM.DenseIO",
        "series": "E3",
        "ocpus": 52,
        "memory_gb": 768.0,
        "local_disk_gb": 51200.0,
        "local_disk_count": 8,
        "arch": "x86_64",
    },
    {
        "instance_type": "VM.DenseIO2.8",
        "family": "VM.DenseIO",
        "series": "E3",
        "ocpus": 8,
        "memory_gb": 120.0,
        "local_disk_gb": 6400.0,
        "local_disk_count": 1,
        "arch": "x86_64",
    },
    {
        "instance_type": "VM.DenseIO2.16",
        "family": "VM.DenseIO",
        "series": "E3",
        "ocpus": 16,
        "memory_gb": 240.0,
        "local_disk_gb": 12800.0,
        "local_disk_count": 2,
        "arch": "x86_64",
    },
    {
        "instance_type": "VM.DenseIO2.24",
        "family": "VM.DenseIO",
        "series": "E3",
        "ocpus": 24,
        "memory_gb": 320.0,
        "local_disk_gb": 25600.0,
        "local_disk_count": 4,
        "arch": "x86_64",
    },
]


_oci_pricing_cache: dict[str, dict[str, float]] | None = None


def _oci_get_pricing() -> dict[str, dict[str, float]]:
    """Return cached OCI pricing rates, fetching on first call."""
    global _oci_pricing_cache  # noqa: PLW0603
    if _oci_pricing_cache is None:
        _oci_pricing_cache = _oci_fetch_pricing()
    return _oci_pricing_cache


def _oci_calc_price(series: str, ocpus: int, memory_gb: float, local_disk_tb: float = 0.0) -> float | None:
    """Calculate OCI hourly price: OCPU + memory + NVMe (if DenseIO).

    Args:
        series: Shape series key (e.g. "E4", "DenseIO.E5").
        ocpus: Number of OCPUs.
        memory_gb: Total memory in GB.
        local_disk_tb: Total local NVMe storage in TB (0 for non-DenseIO).
    """
    rates = _oci_get_pricing().get(series)
    if not rates:
        return None
    price = ocpus * rates["ocpu_hr"] + memory_gb * rates["mem_gb_hr"]
    if local_disk_tb > 0 and "nvme_tb_hr" in rates:
        price += local_disk_tb * rates["nvme_tb_hr"]
    return round(price, 6)


def generate_oci_catalog(
    families: list[str],
    compartment_id: str = "",
) -> list[InstanceTypeInfo]:
    """Generate InstanceTypeInfo list for OCI shapes.

    For Flex shapes, generates entries at common OCPU counts using SCT notation
    (e.g. "VM.DenseIO.E5.Flex:8" for 8 OCPUs). Fixed shapes use their standard
    names. Pricing uses known OCPU + memory rates per shape series.

    Attempts the OCI SDK for live shape discovery; falls back to hardcoded specs.

    Args:
        families: List of OCI shape family prefixes (e.g. ["BM.DenseIO", "VM.DenseIO"]).
        compartment_id: OCI compartment OCID. If empty, uses tenancy root from config.

    Returns:
        List of InstanceTypeInfo.
    """
    results: list[InstanceTypeInfo] = []

    # Generate Flex shape entries at common OCPU counts
    for flex in _OCI_FLEX_SHAPES:
        if not any(flex["base_shape"].startswith(f) for f in families):
            continue
        for ocpus in flex["ocpu_options"]:
            vcpus = ocpus * 2
            memory_gb = ocpus * flex["mem_per_ocpu"]
            local_disk_gb = ocpus * flex["local_disk_gb_per_ocpu"]
            instance_type = f"{flex['base_shape']}:{ocpus}"
            min_memory_gb = ocpus * flex.get("min_mem_per_ocpu", flex["mem_per_ocpu"])
            max_memory_gb = ocpus * flex.get("max_mem_per_ocpu", flex["mem_per_ocpu"])

            results.append(
                InstanceTypeInfo(
                    instance_type=instance_type,
                    cloud="oci",
                    family=flex["family"],
                    vcpus=vcpus,
                    memory_gb=memory_gb,
                    local_disk_gb=local_disk_gb,
                    local_disk_count=flex["local_disk_count"],
                    arch=flex["arch"],
                    price_per_hour=_oci_calc_price(flex["series"], ocpus, memory_gb, local_disk_gb / 1000.0),
                    min_memory_gb=min_memory_gb if min_memory_gb != max_memory_gb else None,
                    max_memory_gb=max_memory_gb if min_memory_gb != max_memory_gb else None,
                )
            )

    # Add fixed (non-Flex) shapes
    for shape in _OCI_FIXED_SHAPES:
        if not any(shape["instance_type"].startswith(f) for f in families):
            continue
        ocpus = shape["ocpus"]
        vcpus = ocpus * 2
        results.append(
            InstanceTypeInfo(
                instance_type=shape["instance_type"],
                cloud="oci",
                family=shape["family"],
                vcpus=vcpus,
                memory_gb=shape["memory_gb"],
                local_disk_gb=shape["local_disk_gb"],
                local_disk_count=shape["local_disk_count"],
                arch=shape["arch"],
                price_per_hour=_oci_calc_price(
                    shape["series"], ocpus, shape["memory_gb"], shape["local_disk_gb"] / 1000.0
                ),
            )
        )

    LOG.info("OCI catalog: found %d instances for families %s", len(results), families)
    return results


# ---------------------------------------------------------------------------
# YAML writer
# ---------------------------------------------------------------------------


def write_catalog_file(
    instances: list[InstanceTypeInfo],
    cloud: str,
    output_path: Path,
) -> None:
    """Write a catalog YAML file with instance data only.

    Args:
        instances: List of InstanceTypeInfo to write.
        cloud: Cloud provider name (e.g. "aws").
        output_path: Destination file path. Parent directories are created if needed.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    instances = sorted(instances, key=lambda i: (i.family, i.instance_type))

    instance_dicts = []
    for inst in instances:
        d: dict[str, Any] = {
            "instance_type": inst.instance_type,
            "family": inst.family,
            "vcpus": inst.vcpus,
            "memory_gb": inst.memory_gb,
            "local_disk_gb": inst.local_disk_gb,
            "local_disk_count": inst.local_disk_count,
            "arch": inst.arch,
        }
        if isinstance(inst.price_per_hour, dict):
            d["price_per_hour"] = inst.price_per_hour
        elif inst.price_per_hour is not None:
            d["price_per_hour"] = inst.price_per_hour
        else:
            d["price_per_hour"] = None
        if inst.min_memory_gb is not None:
            d["min_memory_gb"] = inst.min_memory_gb
        if inst.max_memory_gb is not None:
            d["max_memory_gb"] = inst.max_memory_gb
        instance_dicts.append(d)

    data: dict[str, Any] = {
        "cloud": cloud,
        "instances": instance_dicts,
    }

    with output_path.open("w", encoding="utf-8") as fh:
        yaml.dump(data, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    LOG.info("Wrote %d instances to %s", len(instances), output_path)


# ---------------------------------------------------------------------------
# High-level update_catalog entry point
# ---------------------------------------------------------------------------

_CLOUD_GENERATORS = {
    "aws": generate_aws_catalog,
    "gce": generate_gce_catalog,
    "azure": generate_azure_catalog,
    "oci": generate_oci_catalog,
}


def _load_cloud_configs(config_path: Path) -> dict[str, dict[str, Any]]:
    """Load cloud configurations from sizing_config.yaml."""
    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    clouds = config.get("clouds", {})
    result = {}
    for cloud_name, cloud_cfg in clouds.items():
        result[cloud_name] = {
            "families": cloud_cfg["families"],
            "series_patterns": cloud_cfg.get("series_patterns"),
            "filename": f"{cloud_name}.yaml",
        }
    return result


def update_catalog(
    cloud: str | None = None,
    output_dir: Path = Path("data/instance_catalog"),
) -> None:
    """Refresh catalog YAML files from live cloud APIs.

    If cloud is None, all four clouds are updated.

    Args:
        cloud: Cloud to update ("aws", "gce", "azure", "oci"), or None for all.
        output_dir: Directory where catalog YAML files are written.
    """
    config_path = output_dir / "sizing_config.yaml"
    cloud_configs = _load_cloud_configs(config_path)

    clouds_to_update = [cloud] if cloud else list(cloud_configs.keys())

    for cloud_name in clouds_to_update:
        cfg = cloud_configs[cloud_name]
        output_path = output_dir / cfg["filename"]

        generator = _CLOUD_GENERATORS[cloud_name]
        kwargs: dict[str, Any] = {"families": cfg["families"]}
        if cfg.get("series_patterns"):
            kwargs["series_patterns"] = cfg["series_patterns"]
        instances = generator(**kwargs)

        if not instances:
            LOG.warning("No instances generated for %s — skipping file write", cloud_name)
            continue

        write_catalog_file(
            instances=instances,
            cloud=cloud_name,
            output_path=output_path,
        )
