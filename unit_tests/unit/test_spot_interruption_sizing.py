"""Tests for interruption-aware spot sizing (SCT-707).

For a spot run, sizing must prefer the less-interrupted candidate of the closest size, and must never trade
size for a lower interruption bucket: a bucket-0 24xlarge winning over a bucket-4 xlarge would multiply cost.
"""

from unittest.mock import MagicMock

import pytest
import requests

from sdcm.utils.cloud_catalog import interruption
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo
from sdcm.utils.cloud_catalog.instance_matcher import select_instance

BUCKETS = {
    "us-east-1": {"i8g.xlarge": 4, "i8ge.xlarge": 0, "i8g.2xlarge": 1, "i8ge.2xlarge": 1, "i8g.24xlarge": 0},
    "eu-west-1": {"i8g.xlarge": 0, "i8ge.xlarge": 2},
}


def _inst(name, family, vcpus, price, cloud="aws"):
    return InstanceTypeInfo(name, cloud, family, vcpus, vcpus * 8.0, vcpus * 234.0, 1, "arm64", price)


@pytest.fixture
def catalog(monkeypatch):
    monkeypatch.setattr(interruption, "aws_spot_buckets", lambda: BUCKETS)
    cat = InstanceCatalog()
    cat.cloud_defaults = {"aws": {"arch": "arm64"}, "gce": {"arch": "arm64"}}
    cat.preferred_families = {"db": {"aws": ["i8g", "i8ge"], "gce": ["i8g", "i8ge"]}}
    cat.instances = [
        _inst("i8g.xlarge", "i8g", 4, 0.34),
        _inst("i8ge.xlarge", "i8ge", 4, 0.47),
        _inst("i8g.2xlarge", "i8g", 8, 0.69),
        _inst("i8ge.2xlarge", "i8ge", 8, 0.94),
        _inst("i8g.24xlarge", "i8g", 96, 8.24),
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


def test_no_data_does_not_trade_the_family_order_for_size(catalog):
    """Like GCE: no buckets at all, so the first family wins even though a later family has a smaller type."""
    catalog.instances = [_inst("i8g.2xlarge", "i8g", 8, 0.69), _inst("i8ge.large", "i8ge", 2, 0.23)]

    result = select_instance(catalog, "db", "aws", {"vcpu": ">=2"}, spot_region="ap-south-1")

    assert result.instance_type == "i8g.2xlarge"


def test_other_clouds_never_use_the_aws_buckets(catalog):
    catalog.instances = [_inst("i8g.xlarge", "i8g", 4, 0.34, "gce"), _inst("i8ge.xlarge", "i8ge", 4, 0.47, "gce")]

    assert select_instance(catalog, "db", "gce", {"vcpu": 4}, spot_region="us-east-1").instance_type == "i8g.xlarge"


@pytest.fixture
def advisor(monkeypatch):
    """The real aws_spot_buckets (the autouse stub removed) with `requests.get` mocked."""
    monkeypatch.undo()
    interruption.aws_spot_buckets.cache_clear()
    get = MagicMock()
    monkeypatch.setattr(interruption.requests, "get", get)
    yield get
    interruption.aws_spot_buckets.cache_clear()


def test_aws_spot_buckets_reads_linux_buckets_once(advisor):
    advisor.return_value.json.return_value = {
        "spot_advisor": {"us-east-1": {"Linux": {"i8g.xlarge": {"s": 66, "r": 4}}, "Windows": {"i8g.xlarge": {"r": 0}}}}
    }

    assert interruption.aws_spot_buckets() == {"us-east-1": {"i8g.xlarge": 4}}
    assert interruption.aws_spot_buckets() == {"us-east-1": {"i8g.xlarge": 4}}
    advisor.assert_called_once()
    assert advisor.call_args.args[0] == interruption.SPOT_ADVISOR_URL


def test_aws_spot_buckets_is_empty_when_the_advisor_is_unreachable(advisor):
    advisor.side_effect = requests.ConnectionError("no route")

    assert interruption.aws_spot_buckets() == {}
