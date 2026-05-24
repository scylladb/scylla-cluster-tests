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

"""Instance catalog data model and YAML loader.

Provides InstanceTypeInfo dataclass and InstanceCatalog class for loading
and querying cloud instance type information from YAML files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class RoleConfig:
    category: str
    implicit_constraints: dict[str, str | int | float]
    arch_source: str
    arch: str | None = None


@dataclass
class SizingConfig:
    roles: dict[str, RoleConfig]
    sort_order: list[str]
    flex_defaults: dict[str, float]
    cloud_defaults: dict[str, dict]
    preferred_families: dict[str, dict[str, list[str]]]

    @classmethod
    def from_file(cls, path: Path) -> SizingConfig:
        if not path.exists():
            raise FileNotFoundError(f"Sizing config not found: {path}")
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)

        roles = {}
        for name, cfg in (data.get("roles") or {}).items():
            roles[name] = RoleConfig(
                category=cfg.get("category", ""),
                implicit_constraints=dict(cfg.get("implicit_constraints") or {}),
                arch_source=cfg.get("arch_source", "fixed"),
                arch=cfg.get("arch"),
            )

        cloud_defaults: dict[str, dict] = {}
        preferred_families: dict[str, dict[str, list[str]]] = {}
        for cloud_name, cloud_cfg in (data.get("clouds") or {}).items():
            if "cloud_defaults" in cloud_cfg:
                cloud_defaults[cloud_name] = dict(cloud_cfg["cloud_defaults"])
            for role, families in (cloud_cfg.get("preferred_families") or {}).items():
                preferred_families.setdefault(role, {})
                preferred_families[role][cloud_name] = list(families or [])

        return cls(
            roles=roles,
            sort_order=list(data.get("sort_order") or ["preferred_family_rank", "price_per_hour", "vcpus"]),
            flex_defaults=dict(data.get("flex_defaults") or {"mem_per_vcpu": 4.0}),
            cloud_defaults=cloud_defaults,
            preferred_families=preferred_families,
        )

    def storage_roles(self) -> set[str]:
        return {name for name, cfg in self.roles.items() if cfg.category == "storage"}

    def compute_roles(self) -> set[str]:
        return {name for name, cfg in self.roles.items() if cfg.category == "compute"}


@dataclass
class InstanceTypeInfo:
    """Metadata for a single cloud instance type.

    Attributes:
        instance_type: Full instance type name (e.g. "i8g.2xlarge").
        cloud: Cloud provider name (e.g. "aws", "gce", "azure", "oci").
        family: Instance family prefix (e.g. "i8g").
        vcpus: Number of virtual CPUs.
        memory_gb: Amount of RAM in gigabytes.
        local_disk_gb: Total local/ephemeral disk in gigabytes (0 if none).
        local_disk_count: Number of local disks (0 if none).
        arch: CPU architecture ("x86_64" or "arm64").
        price_per_hour: On-demand price in USD per hour. Either a single float
            (legacy/simple) or a dict mapping region -> price for multi-region
            catalogs. None if unknown.
    """

    instance_type: str
    cloud: str
    family: str
    vcpus: int
    memory_gb: float
    local_disk_gb: float
    local_disk_count: int
    arch: str
    price_per_hour: float | dict[str, float] | None = None
    min_memory_gb: float | None = None
    max_memory_gb: float | None = None

    def get_price(self, region: str | None = None) -> float | None:
        """Get the on-demand price for a specific region.

        Args:
            region: Cloud region name. If None, returns the first available price.

        Returns:
            Price in USD/hr, or None if unknown.
        """
        if self.price_per_hour is None:
            return None
        if isinstance(self.price_per_hour, (int, float)):
            return float(self.price_per_hour)
        if region and region in self.price_per_hour:
            return self.price_per_hour[region]
        # Fallback: return first available region price
        if self.price_per_hour:
            return next(iter(self.price_per_hour.values()))
        return None


class InstanceCatalog:
    """Catalog of cloud instance types loaded from YAML files.

    Attributes:
        instances: Flat list of all loaded InstanceTypeInfo objects.
        preferred_families: Nested dict mapping role -> cloud -> list of families.
            Example: {"db": {"aws": ["i8g", "i7i"]}, "loader": {"aws": ["c6i"]}}
        cloud_defaults: Dict mapping cloud -> default settings dict.
            Example: {"aws": {"arch": "arm64"}, "gce": {"arch": "x86_64"}}
    """

    def __init__(self) -> None:
        self.instances: list[InstanceTypeInfo] = []
        self.preferred_families: dict[str, dict[str, list[str]]] = {}
        self.cloud_defaults: dict[str, dict] = {}
        self.sizing_config: SizingConfig | None = None

    @classmethod
    def from_file(cls, path: Path) -> InstanceCatalog:
        """Load an InstanceCatalog from a single YAML file.

        Args:
            path: Path to the YAML file to load.

        Returns:
            A new InstanceCatalog populated from the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not path.exists():
            raise FileNotFoundError(f"Instance catalog file not found: {path}")

        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)

        catalog = cls()
        if not data:
            return catalog

        cloud = data.get("cloud", "")

        for item in data.get("instances") or []:
            raw_price = item.get("price_per_hour")
            if isinstance(raw_price, dict):
                price_per_hour = {k: float(v) for k, v in raw_price.items()}
            elif raw_price is not None:
                price_per_hour = float(raw_price)
            else:
                price_per_hour = None

            info = InstanceTypeInfo(
                instance_type=item["instance_type"],
                cloud=cloud,
                family=item["family"],
                vcpus=int(item["vcpus"]),
                memory_gb=float(item["memory_gb"]),
                local_disk_gb=float(item.get("local_disk_gb", 0)),
                local_disk_count=int(item.get("local_disk_count", 0)),
                arch=item["arch"],
                price_per_hour=price_per_hour,
                min_memory_gb=float(item["min_memory_gb"]) if "min_memory_gb" in item else None,
                max_memory_gb=float(item["max_memory_gb"]) if "max_memory_gb" in item else None,
            )
            catalog.instances.append(info)

        return catalog

    @classmethod
    def from_directory(cls, dir_path: Path) -> InstanceCatalog:
        """Load and merge all *.yaml files from a directory.

        Args:
            dir_path: Directory containing YAML catalog files.

        Returns:
            A merged InstanceCatalog from all YAML files found.
        """
        merged = cls()
        sizing_config_path = dir_path / "sizing_config.yaml"
        if sizing_config_path.exists():
            merged.sizing_config = SizingConfig.from_file(sizing_config_path)
            merged.cloud_defaults = dict(merged.sizing_config.cloud_defaults)
            merged.preferred_families = dict(merged.sizing_config.preferred_families)
        for yaml_file in sorted(dir_path.glob("*.yaml")):
            if yaml_file.name == "sizing_config.yaml":
                continue
            partial = cls.from_file(yaml_file)
            merged.instances.extend(partial.instances)
        return merged

    def get_instances(self, cloud: str) -> list[InstanceTypeInfo]:
        """Return all instances for the given cloud provider.

        Args:
            cloud: Cloud provider name (e.g. "aws", "gce", "azure", "oci").

        Returns:
            List of InstanceTypeInfo for that cloud (may be empty).
        """
        return [inst for inst in self.instances if inst.cloud == cloud]

    def get_instances_by_family(self, cloud: str, family: str) -> list[InstanceTypeInfo]:
        """Return all instances for the given cloud provider and family.

        Args:
            cloud: Cloud provider name.
            family: Instance family prefix (e.g. "i8g").

        Returns:
            List of InstanceTypeInfo matching cloud and family (may be empty).
        """
        return [inst for inst in self.instances if inst.cloud == cloud and inst.family == family]

    def get_instance(self, cloud: str, instance_type: str) -> InstanceTypeInfo | None:
        """Return a specific instance by cloud and instance type name.

        Args:
            cloud: Cloud provider name (e.g. "aws", "gce", "azure", "oci").
            instance_type: Instance type name (e.g. "i4i.xlarge", "n2-standard-8").

        Returns:
            InstanceTypeInfo if found, None otherwise.
        """
        for inst in self.instances:
            if inst.cloud == cloud and inst.instance_type == instance_type:
                return inst
        return None
