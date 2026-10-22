"""Tests for interruption-aware spot sizing (SCT-707).

For a spot run, sizing must prefer the less-interrupted candidate of the closest size, and must never trade
size for a lower interruption bucket: a bucket-0 24xlarge winning over a bucket-4 xlarge would multiply cost.
"""

from unittest.mock import MagicMock

import pytest
import yaml

from sdcm.utils.cloud_catalog import interruption
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo
from sdcm.utils.cloud_catalog.instance_matcher import select_instance


def _inst(name, family, vcpus, price, buckets=None):
    return InstanceTypeInfo(
        name, "aws", family, vcpus, vcpus * 8.0, vcpus * 234.0, 1, "arm64", price, interruption_bucket=buckets
    )


@pytest.fixture
def catalog():
    cat = InstanceCatalog()
    cat.cloud_defaults = {"aws": {"arch": "arm64"}}
    cat.preferred_families = {"db": {"aws": ["i8g", "i8ge"]}}
    cat.instances = [
        _inst("i8g.xlarge", "i8g", 4, 0.34, {"us-east-1": 4, "eu-west-1": 0}),
        _inst("i8ge.xlarge", "i8ge", 4, 0.47, {"us-east-1": 0, "eu-west-1": 2}),
        _inst("i8g.2xlarge", "i8g", 8, 0.69, {"us-east-1": 1}),
        _inst("i8ge.2xlarge", "i8ge", 8, 0.94, {"us-east-1": 1}),
        _inst("i8g.24xlarge", "i8g", 96, 8.24, {"us-east-1": 0}),
    ]
    return cat


def test_on_demand_order_is_unchanged(catalog):
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}).instance_type == "i8g.xlarge"


def test_spot_prefers_the_less_interrupted_type_of_the_same_size(catalog):
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="us-east-1").instance_type == "i8ge.xlarge"


def test_spot_ranking_is_per_region(catalog):
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="eu-west-1").instance_type == "i8g.xlarge"


def test_spot_never_trades_size_for_a_lower_bucket(catalog):
    """`>=4` must still pick a 4-vCPU type, not the bucket-0 i8g.24xlarge."""
    result = select_instance(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region="us-east-1")
    assert result.vcpus == 4


def test_equal_buckets_keep_the_family_order(catalog):
    assert select_instance(catalog, "db", "aws", {"vcpu": 8}, spot_region="us-east-1").instance_type == "i8g.2xlarge"


def test_region_without_data_keeps_the_family_order(catalog):
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="ap-south-1").instance_type == "i8g.xlarge"


def test_from_directory_attaches_buckets_and_skips_the_file_as_a_catalog(tmp_path):
    (tmp_path / "aws.yaml").write_text(
        yaml.safe_dump(
            {
                "cloud": "aws",
                "instances": [
                    {
                        "instance_type": "i8g.xlarge",
                        "family": "i8g",
                        "vcpus": 4,
                        "memory_gb": 32.0,
                        "arch": "arm64",
                        "price_per_hour": 0.34,
                    }
                ],
            }
        )
    )
    interruption.write_interruption_file(
        tmp_path / interruption.INTERRUPTION_FILE, {"aws": {"us-east-1": {"i8g.xlarge": 3}}}
    )
    InstanceCatalog.from_directory.cache_clear()

    catalog = InstanceCatalog.from_directory(tmp_path)

    [inst] = catalog.get_instances("aws")
    assert inst.get_interruption_bucket("us-east-1") == 3
    assert inst.get_interruption_bucket("eu-west-1") is None
    assert inst.get_interruption_bucket(None) is None


def test_fetch_aws_buckets_keeps_catalog_types_only(monkeypatch):
    response = MagicMock()
    response.json.return_value = {
        "spot_advisor": {
            "us-east-1": {"Linux": {"i8g.xlarge": {"s": 66, "r": 4}, "x2gd.metal": {"s": 50, "r": 1}}},
        }
    }
    get = MagicMock(return_value=response)
    monkeypatch.setattr(interruption.requests, "get", get)

    buckets = interruption.fetch_aws_buckets({"i8g.xlarge", "i8ge.xlarge"}, regions=("us-east-1", "eu-west-1"))

    assert buckets == {"us-east-1": {"i8g.xlarge": 4}, "eu-west-1": {}}
    assert get.call_args.args[0] == interruption.SPOT_ADVISOR_URL
