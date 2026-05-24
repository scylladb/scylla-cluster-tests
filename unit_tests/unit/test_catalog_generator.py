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

from sdcm.utils.cloud_catalog.catalog_generator import generate_oci_catalog, write_catalog_file
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo


@pytest.fixture()
def sample_instances() -> list[InstanceTypeInfo]:
    return [
        InstanceTypeInfo(
            instance_type="i8g.2xlarge",
            cloud="aws",
            family="i8g",
            vcpus=8,
            memory_gb=64.0,
            local_disk_gb=1875.0,
            local_disk_count=1,
            arch="arm64",
            price_per_hour=0.702,
        ),
        InstanceTypeInfo(
            instance_type="i8g.4xlarge",
            cloud="aws",
            family="i8g",
            vcpus=16,
            memory_gb=128.0,
            local_disk_gb=3750.0,
            local_disk_count=2,
            arch="arm64",
            price_per_hour=None,
        ),
    ]


def test_write_catalog_file_round_trip(tmp_path: Path, sample_instances: list[InstanceTypeInfo]):
    output = tmp_path / "aws.yaml"
    write_catalog_file(
        instances=sample_instances,
        cloud="aws",
        output_path=output,
    )

    assert output.exists()
    catalog = InstanceCatalog.from_file(output)
    assert len(catalog.instances) == 2


def test_write_catalog_file_instance_fields_preserved(tmp_path: Path, sample_instances: list[InstanceTypeInfo]):
    output = tmp_path / "aws.yaml"
    write_catalog_file(
        instances=sample_instances,
        cloud="aws",
        output_path=output,
    )

    catalog = InstanceCatalog.from_file(output)
    inst = next(i for i in catalog.instances if i.instance_type == "i8g.2xlarge")
    assert inst.vcpus == 8
    assert inst.memory_gb == 64.0
    assert inst.local_disk_gb == 1875.0
    assert inst.local_disk_count == 1
    assert inst.arch == "arm64"
    assert inst.price_per_hour == pytest.approx(0.702)


def test_write_catalog_file_none_price_preserved(tmp_path: Path, sample_instances: list[InstanceTypeInfo]):
    output = tmp_path / "aws.yaml"
    write_catalog_file(
        instances=sample_instances,
        cloud="aws",
        output_path=output,
    )

    catalog = InstanceCatalog.from_file(output)
    inst = next(i for i in catalog.instances if i.instance_type == "i8g.4xlarge")
    assert inst.price_per_hour is None


def test_write_catalog_file_creates_parent_dirs(tmp_path: Path, sample_instances: list[InstanceTypeInfo]):
    output = tmp_path / "nested" / "deep" / "aws.yaml"
    write_catalog_file(
        instances=sample_instances,
        cloud="aws",
        output_path=output,
    )
    assert output.exists()


def test_write_catalog_file_empty_instances(tmp_path: Path):
    output = tmp_path / "aws.yaml"
    write_catalog_file(
        instances=[],
        cloud="aws",
        output_path=output,
    )
    catalog = InstanceCatalog.from_file(output)
    assert catalog.instances == []


def test_generate_oci_catalog_fallback_to_known_shapes():
    result = generate_oci_catalog(["BM.DenseIO", "VM.DenseIO"])
    assert len(result) > 0
    assert all(r.cloud == "oci" for r in result)
    assert all(r.vcpus > 0 for r in result)
    assert all(r.local_disk_gb > 0 for r in result)


def test_generate_oci_catalog_family_filter():
    result = generate_oci_catalog(["BM.DenseIO"])
    assert all(r.family == "BM.DenseIO" for r in result)


def test_generate_oci_catalog_unknown_family_returns_empty():
    result = generate_oci_catalog(["NonExistentFamily99"])
    assert result == []
