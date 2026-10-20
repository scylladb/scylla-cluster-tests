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
from types import SimpleNamespace

import pytest

from sdcm.utils.cloud_catalog import catalog_generator
from sdcm.utils.cloud_catalog.catalog_generator import (
    _fetch_gce_resource_rates,
    _gce_price_for,
    generate_gce_catalog,
    generate_oci_catalog,
    write_catalog_file,
)
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo

_FAKE_OCI_PRICING = {
    "E4": {"ocpu_hr": 0.025, "mem_gb_hr": 0.0015},
    "E5": {"ocpu_hr": 0.026, "mem_gb_hr": 0.0016},
    "DenseIO.E4": {"ocpu_hr": 0.025, "mem_gb_hr": 0.0015, "nvme_tb_hr": 0.026},
    "DenseIO.E5": {"ocpu_hr": 0.026, "mem_gb_hr": 0.0016, "nvme_tb_hr": 0.026},
}


@pytest.fixture(autouse=True)
def _mock_oci_pricing(monkeypatch):
    monkeypatch.setattr(
        "sdcm.utils.cloud_catalog.catalog_generator._oci_get_pricing",
        lambda: _FAKE_OCI_PRICING,
    )


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


@pytest.mark.parametrize(
    "instance_type,expected_vcpus",
    [
        ("VM.Standard.A1.Flex:16", 16),
        ("VM.Standard.A4.Flex:8", 16),
        ("VM.Standard3.Flex:8", 16),
        ("VM.DenseIO.E5.Flex:8", 16),
    ],
)
def test_generate_oci_catalog_vcpus_per_ocpu(instance_type, expected_vcpus):
    result = {r.instance_type: r for r in generate_oci_catalog(["VM.Standard", "VM.DenseIO"])}
    assert result[instance_type].vcpus == expected_vcpus


def test_generate_oci_catalog_unknown_family_returns_empty():
    result = generate_oci_catalog(["NonExistentFamily99"])
    assert result == []


def test_gce_spot_price_is_vcpu_plus_ram_per_region():
    rates = {"n2": {"us-central1": {"core": 0.01, "ram": 0.002}, "us-east1": {"core": 0.01}}}
    assert _gce_price_for(rates, "n2-highmem", 8, 64.0) == {"us-central1": pytest.approx(8 * 0.01 + 64 * 0.002)}


def _sku(description: str, unit: str, nanos: int) -> dict:
    return {
        "description": description,
        "category": {"usageType": "Preemptible" if description.startswith("Spot ") else "OnDemand"},
        "serviceRegions": ["us-central1"],
        "pricingInfo": [
            {"pricingExpression": {"usageUnit": unit, "tieredRates": [{"unitPrice": {"units": "0", "nanos": nanos}}]}}
        ],
    }


# Recorded from the Cloud Billing Catalog API (2026-10-19).
_SKUS = [
    _sku("Spot Preemptible C4 Instance Core running in Americas", "h", 20740000),
    _sku("Spot Preemptible C4 Instance Ram running in Americas", "GiBy.h", 2357000),
    _sku("Spot Preemptible C4 Instance Local SSD running in Americas", "GiBy.mo", 95800000),
    _sku("Spot Preemptible C4D Instance Ram running in Americas", "GBy.h", 1480000),
    _sku("C4 Instance Core running in Americas", "h", 34650000),
]


def test_gce_resource_rates_parse_local_ssd_and_per_gb_ram(monkeypatch):
    session = SimpleNamespace(
        get=lambda *_, **__: SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"skus": _SKUS})
    )
    monkeypatch.setattr(catalog_generator, "KeyStore", lambda: SimpleNamespace(get_gcp_credentials=dict))
    monkeypatch.setattr(catalog_generator.sa_module.Credentials, "from_service_account_info", lambda *_, **__: None)
    monkeypatch.setattr(catalog_generator.google_requests, "AuthorizedSession", lambda _: session)

    rates, on_demand_rates = _fetch_gce_resource_rates()

    assert on_demand_rates["c4"]["us-central1"] == {"core": pytest.approx(0.03465)}
    assert rates["c4"]["us-central1"] == {
        "core": pytest.approx(0.02074),
        "ram": pytest.approx(0.002357),
        "ssd": pytest.approx(0.0958 / 730),
    }
    assert rates["c4d"]["us-central1"]["ram"] == pytest.approx(0.00148 * 2**30 / 1e9)


def test_gce_catalog_lssd_machine_types(monkeypatch):
    def machine_type(name, vcpus, memory_gb, partitions=0):
        return SimpleNamespace(
            name=name,
            guest_cpus=vcpus,
            memory_mb=memory_gb * 1024,
            architecture="X86_64",
            bundled_local_ssds=SimpleNamespace(partition_count=partitions),
        )

    machine_types = [
        machine_type("c4-highmem-4-lssd", 4, 31, 1),
        machine_type("c4-highmem-288-lssd-metal", 288, 2232, 6),
        machine_type("c4-highmem-4", 4, 31),
        machine_type("z3-highmem-8-highlssd", 8, 64, 1),
    ]
    spot_rates = {
        "c4": {
            "us-central1": {"core": 0.02, "ram": 0.002, "ssd": 0.0001},
            "us-east1": {"core": 0.02, "ram": 0.002},
        },
        "z3": {"us-central1": {"core": 0.02, "ram": 0.002, "ssd": 0.0001}},
    }
    monkeypatch.setattr(catalog_generator, "_scrape_gce_z3_prices", lambda: {"z3-highmem-8-highlssd": {"r": 1.0}})
    on_demand_rates = {"c4": {"us-central1": {"core": 0.03, "ram": 0.004, "ssd": 0.0002}}}
    monkeypatch.setattr(catalog_generator, "_scrape_gce_general_prices", lambda _: {})
    monkeypatch.setattr(catalog_generator, "_fetch_gce_resource_rates", lambda: (spot_rates, on_demand_rates))
    monkeypatch.setattr(catalog_generator, "KeyStore", lambda: SimpleNamespace(get_gcp_credentials=dict))
    monkeypatch.setattr(catalog_generator.sa_module.Credentials, "from_service_account_info", lambda _: None)
    monkeypatch.setattr(
        catalog_generator.compute_v1,
        "MachineTypesClient",
        lambda **_: SimpleNamespace(list=lambda request: machine_types),
    )

    result = {i.instance_type: i for i in generate_gce_catalog(["z3", "c4-highmem"], project="p")}

    assert set(result) == {"c4-highmem-4-lssd", "c4-highmem-288-lssd-metal", "z3-highmem-8-highlssd"}
    lssd = result["c4-highmem-4-lssd"]
    assert (lssd.family, lssd.local_disk_count, lssd.local_disk_gb) == ("c4-highmem-lssd", 1, 375.0)
    assert lssd.price_per_hour == {"us-central1": pytest.approx(4 * 0.03 + 31 * 0.004 + 375 * 0.0002)}
    # us-east1 has no spot SSD rate, and a spot price without the bundled SSD would be too low.
    assert lssd.spot_price_per_hour == {"us-central1": pytest.approx(4 * 0.02 + 31 * 0.002 + 375 * 0.0001)}
    metal = result["c4-highmem-288-lssd-metal"]
    assert (metal.family, metal.local_disk_count, metal.local_disk_gb) == ("c4-highmem-lssd", 6, 18000.0)
    z3 = result["z3-highmem-8-highlssd"]
    assert (z3.family, z3.local_disk_count, z3.local_disk_gb) == ("z3-highmem-highlssd", 1, 3000.0)
    assert z3.price_per_hour == {"r": 1.0}
    assert z3.spot_price_per_hour is None
