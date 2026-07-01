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

from pathlib import Path

import pytest

from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo


AWS_YAML = """\
cloud: aws
instances:
  - instance_type: i8g.large
    family: i8g
    vcpus: 2
    memory_gb: 16.0
    local_disk_gb: 468.0
    local_disk_count: 1
    arch: arm64
    price_per_hour: null
  - instance_type: i8g.xlarge
    family: i8g
    vcpus: 4
    memory_gb: 32.0
    local_disk_gb: 937.0
    local_disk_count: 1
    arch: arm64
    price_per_hour: 0.499
  - instance_type: c6i.large
    family: c6i
    vcpus: 2
    memory_gb: 4.0
    local_disk_gb: 0.0
    local_disk_count: 0
    arch: x86_64
    price_per_hour: 0.085
"""

GCE_YAML = """\
cloud: gce
instances:
  - instance_type: n2-highmem-4
    family: n2-highmem
    vcpus: 4
    memory_gb: 32.0
    local_disk_gb: 0.0
    local_disk_count: 0
    arch: x86_64
    price_per_hour: 0.237
"""

SIZING_CONFIG_YAML = """\
roles:
  db:
    category: storage
    implicit_constraints:
      local_disk_count: "> 0"
    arch_source: cloud_default
  loader:
    category: compute
    implicit_constraints:
      local_disk_count: 0
    arch_source: fixed
    arch: x86_64
sort_order:
  - preferred_family_rank
  - vcpus
  - price_per_hour
flex_defaults:
  mem_per_vcpu: 4.0
clouds:
  aws:
    families: [i8g, c6i]
    cloud_defaults:
      arch: arm64
    preferred_families:
      db: [i8g, i7i, i4i]
      loader: [c6i]
      monitor: [t3, m6i]
  gce:
    families: [n2]
    cloud_defaults:
      arch: x86_64
    preferred_families:
      db: [n2-highmem]
      loader: [n2-standard]
"""

EMPTY_YAML = """\
cloud: aws
"""

NO_INSTANCES_YAML = """\
cloud: aws
instances: []
"""


@pytest.fixture()
def aws_yaml_file(tmp_path: Path) -> Path:
    p = tmp_path / "aws.yaml"
    p.write_text(AWS_YAML)
    return p


@pytest.fixture()
def multi_cloud_dir(tmp_path: Path) -> Path:
    (tmp_path / "aws.yaml").write_text(AWS_YAML)
    (tmp_path / "gce.yaml").write_text(GCE_YAML)
    (tmp_path / "sizing_config.yaml").write_text(SIZING_CONFIG_YAML)
    return tmp_path


def test_from_file_loads_instances(aws_yaml_file: Path):
    catalog = InstanceCatalog.from_file(aws_yaml_file)
    assert len(catalog.instances) == 3


def test_from_file_instance_fields(aws_yaml_file: Path):
    catalog = InstanceCatalog.from_file(aws_yaml_file)
    large = next(i for i in catalog.instances if i.instance_type == "i8g.large")
    assert large.cloud == "aws"
    assert large.family == "i8g"
    assert large.vcpus == 2
    assert large.memory_gb == 16.0
    assert large.local_disk_gb == 468.0
    assert large.local_disk_count == 1
    assert large.arch == "arm64"
    assert large.price_per_hour is None


def test_from_file_price_per_hour_set(aws_yaml_file: Path):
    catalog = InstanceCatalog.from_file(aws_yaml_file)
    xlarge = next(i for i in catalog.instances if i.instance_type == "i8g.xlarge")
    assert xlarge.price_per_hour == pytest.approx(0.499)


def test_from_file_cloud_defaults(aws_yaml_file: Path):
    catalog = InstanceCatalog.from_file(aws_yaml_file)
    assert catalog.cloud_defaults == {}


def test_from_file_preferred_families(aws_yaml_file: Path):
    catalog = InstanceCatalog.from_file(aws_yaml_file)
    assert catalog.preferred_families == {}


@pytest.mark.parametrize(
    "yaml_content,filename",
    [
        pytest.param(EMPTY_YAML, "empty.yaml", id="no-instances-key"),
        pytest.param(NO_INSTANCES_YAML, "no_inst.yaml", id="empty-instances-list"),
    ],
)
def test_from_file_empty_catalog(tmp_path: Path, yaml_content: str, filename: str):
    p = tmp_path / filename
    p.write_text(yaml_content)
    catalog = InstanceCatalog.from_file(p)
    assert catalog.instances == []


def test_from_file_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        InstanceCatalog.from_file(tmp_path / "nonexistent.yaml")


def test_from_directory_merges_instances(multi_cloud_dir: Path):
    catalog = InstanceCatalog.from_directory(multi_cloud_dir)
    assert len(catalog.instances) == 4  # 3 aws + 1 gce


def test_from_directory_merges_cloud_defaults(multi_cloud_dir: Path):
    catalog = InstanceCatalog.from_directory(multi_cloud_dir)
    assert catalog.cloud_defaults["aws"]["arch"] == "arm64"
    assert catalog.cloud_defaults["gce"]["arch"] == "x86_64"


def test_from_directory_merges_preferred_families(multi_cloud_dir: Path):
    catalog = InstanceCatalog.from_directory(multi_cloud_dir)
    assert "aws" in catalog.preferred_families["db"]
    assert "gce" in catalog.preferred_families["db"]
    assert catalog.preferred_families["db"]["gce"] == ["n2-highmem"]


def test_from_directory_empty_dir(tmp_path: Path):
    catalog = InstanceCatalog.from_directory(tmp_path)
    assert catalog.instances == []
    assert catalog.preferred_families == {}
    assert catalog.cloud_defaults == {}


@pytest.mark.parametrize(
    "cloud,expected_count",
    [
        pytest.param("aws", 3, id="aws-instances"),
        pytest.param("gce", 1, id="gce-instances"),
        pytest.param("azure", 0, id="azure-no-instances"),
    ],
)
def test_get_instances_filters_by_cloud(multi_cloud_dir: Path, cloud: str, expected_count: int):
    catalog = InstanceCatalog.from_directory(multi_cloud_dir)
    result = catalog.get_instances(cloud)
    assert len(result) == expected_count
    assert all(inst.cloud == cloud for inst in result)


@pytest.mark.parametrize(
    "cloud,family,expected_count",
    [
        pytest.param("aws", "i8g", 2, id="aws-i8g-two-sizes"),
        pytest.param("aws", "c6i", 1, id="aws-c6i-one-size"),
        pytest.param("aws", "m6i", 0, id="aws-m6i-not-in-instances"),
        pytest.param("gce", "n2-highmem", 1, id="gce-n2-highmem"),
        pytest.param("gce", "i8g", 0, id="gce-i8g-wrong-cloud"),
    ],
)
def test_get_instances_by_family(multi_cloud_dir: Path, cloud: str, family: str, expected_count: int):
    catalog = InstanceCatalog.from_directory(multi_cloud_dir)
    result = catalog.get_instances_by_family(cloud, family)
    assert len(result) == expected_count
    assert all(inst.cloud == cloud and inst.family == family for inst in result)


def test_real_azure_catalog_excludes_scsi_only_dpdsv5():
    # Dpdsv5 (Ampere Altra) exposes local disk via SCSI (not NVMe), so it must not
    # appear in the Azure pricing/sizing catalog
    catalog_dir = Path(__file__).parents[2] / "data" / "instance_catalog"
    catalog = InstanceCatalog.from_directory(catalog_dir)
    dpdsv5 = [inst.instance_type for inst in catalog.get_instances("azure") if "pds_v5" in inst.instance_type]
    assert not dpdsv5, f"SCSI-only Dpdsv5 types must not appear in the Azure catalog: {dpdsv5}"


def test_instance_type_info_is_dataclass():
    info = InstanceTypeInfo(
        instance_type="t3.micro",
        cloud="aws",
        family="t3",
        vcpus=2,
        memory_gb=1.0,
        local_disk_gb=0.0,
        local_disk_count=0,
        arch="x86_64",
    )
    assert info.price_per_hour is None
    assert info.instance_type == "t3.micro"
