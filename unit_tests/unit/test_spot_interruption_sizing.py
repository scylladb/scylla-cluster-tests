"""Tests for interruption-aware spot sizing (SCT-707).

For a spot run, sizing swaps the normal pick for a less-interrupted candidate of the same size, and never
trades size for a lower interruption bucket.
"""

from unittest.mock import MagicMock

import pytest
import requests

from sdcm.utils.cloud_catalog import interruption
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo
from sdcm.utils.cloud_catalog.instance_matcher import select_instance

REAL_AWS_SPOT_BUCKETS = interruption.aws_spot_buckets  # before the autouse stub replaces it

BUCKETS = {
    "us-east-1": {"i8g.xlarge": 4, "i8ge.xlarge": 0, "i8g.2xlarge": 1, "i8ge.2xlarge": 1, "i8g.24xlarge": 0},
    "eu-west-1": {"i8g.xlarge": 0, "i8ge.xlarge": 2},
}


def make_instance(name, family, vcpus, price, cloud="aws"):
    """Build an arm64 InstanceTypeInfo whose memory and disk scale with vcpus."""
    return InstanceTypeInfo(name, cloud, family, vcpus, vcpus * 8.0, vcpus * 234.0, 1, "arm64", price)


@pytest.fixture
def catalog(monkeypatch):
    """InstanceCatalog with i8g/i8ge types for db, and the spot buckets stubbed to BUCKETS."""
    monkeypatch.setattr(interruption, "aws_spot_buckets", lambda: BUCKETS)
    cat = InstanceCatalog()
    cat.cloud_defaults = {"aws": {"arch": "arm64"}, "gce": {"arch": "arm64"}}
    cat.preferred_families = {"db": {"aws": ["i8g", "i8ge"], "gce": ["i8g", "i8ge"]}}
    cat.instances = [
        make_instance("i8g.xlarge", "i8g", 4, 0.34),
        make_instance("i8ge.xlarge", "i8ge", 4, 0.47),
        make_instance("i8g.2xlarge", "i8g", 8, 0.69),
        make_instance("i8ge.2xlarge", "i8ge", 8, 0.94),
        make_instance("i8g.24xlarge", "i8g", 96, 8.24),
    ]
    return cat


@pytest.fixture
def advisor(monkeypatch):
    """The real aws_spot_buckets (the autouse stub removed) with the retry session's `get` mocked."""
    monkeypatch.setattr(interruption, "aws_spot_buckets", REAL_AWS_SPOT_BUCKETS)
    interruption.aws_spot_buckets.cache_clear()
    session = MagicMock()
    monkeypatch.setattr(interruption, "create_retry_session", lambda: session)
    yield session.get
    interruption.aws_spot_buckets.cache_clear()


def test_on_demand_order_is_unchanged(catalog):
    """Without spot_region the normal family order wins: i8g.xlarge."""
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}).instance_type == "i8g.xlarge"


def test_spot_prefers_the_less_interrupted_type_of_the_same_size(catalog):
    """Spot picks the less-interrupted i8ge.xlarge over i8g.xlarge of the same size."""
    result = select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="us-east-1")
    assert result.instance_type == "i8ge.xlarge"


def test_spot_ranking_is_per_region(catalog):
    """Bucket data is per region: eu-west-1 ranks i8g.xlarge first."""
    assert select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="eu-west-1").instance_type == "i8g.xlarge"


def test_spot_never_trades_size_for_a_lower_bucket(catalog):
    """`>=4` must still pick a 4-vCPU type, not the bucket-0 i8g.24xlarge."""
    result = select_instance(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region="us-east-1")
    assert result.vcpus == 4


def test_spot_keeps_the_size_of_the_normal_pick(catalog, monkeypatch):
    """A smaller type of a later family, with no data, must not replace a bucket-0 normal pick (2TB_dataset)."""
    catalog.instances = [make_instance("i8g.2xlarge", "i8g", 8, 0.69), make_instance("i8ge.xlarge", "i8ge", 4, 0.47)]
    monkeypatch.setattr(interruption, "aws_spot_buckets", lambda: {"us-east-1": {"i8g.2xlarge": 0}})

    result = select_instance(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region="us-east-1")

    assert result.instance_type == "i8g.2xlarge"


def test_equal_buckets_keep_the_family_order(catalog):
    """Equal buckets keep the family order: i8g.2xlarge beats i8ge.2xlarge."""
    result = select_instance(catalog, "db", "aws", {"vcpu": 8}, spot_region="us-east-1")
    assert result.instance_type == "i8g.2xlarge"


def test_region_without_data_keeps_the_family_order(catalog):
    """A region without advisor data keeps the family order: i8g.xlarge."""
    result = select_instance(catalog, "db", "aws", {"vcpu": 4}, spot_region="ap-south-1")
    assert result.instance_type == "i8g.xlarge"


def test_no_data_does_not_trade_the_family_order_for_size(catalog):
    """Like GCE: no buckets at all, so the first family wins even though a later family has a smaller type."""
    catalog.instances = [make_instance("i8g.2xlarge", "i8g", 8, 0.69), make_instance("i8ge.large", "i8ge", 2, 0.23)]

    result = select_instance(catalog, "db", "aws", {"vcpu": ">=2"}, spot_region="ap-south-1")

    assert result.instance_type == "i8g.2xlarge"


def test_other_clouds_never_use_the_aws_buckets(catalog):
    """GCE selection ignores the AWS spot buckets and keeps the family order."""
    catalog.instances = [
        make_instance("i8g.xlarge", "i8g", 4, 0.34, "gce"),
        make_instance("i8ge.xlarge", "i8ge", 4, 0.47, "gce"),
    ]

    assert select_instance(catalog, "db", "gce", {"vcpu": 4}, spot_region="us-east-1").instance_type == "i8g.xlarge"


def test_aws_spot_buckets_reads_linux_buckets_once(advisor):
    """aws_spot_buckets fetches the advisor once, keeps Linux only, and caches the result."""
    advisor.return_value.json.return_value = {
        "spot_advisor": {"us-east-1": {"Linux": {"i8g.xlarge": {"s": 66, "r": 4}}, "Windows": {"i8g.xlarge": {"r": 0}}}}
    }

    assert interruption.aws_spot_buckets() == {"us-east-1": {"i8g.xlarge": 4}}
    assert interruption.aws_spot_buckets() == {"us-east-1": {"i8g.xlarge": 4}}
    advisor.assert_called_once_with(interruption.SPOT_ADVISOR_URL, timeout=10)
    assert advisor.call_args.args[0] == interruption.SPOT_ADVISOR_URL


def test_aws_spot_buckets_is_empty_when_the_advisor_is_unreachable(advisor):
    """A connection error from the advisor yields empty buckets, not an exception."""
    advisor.side_effect = requests.ConnectionError("no route")

    assert interruption.aws_spot_buckets() == {}


@pytest.mark.parametrize(
    "info",
    [
        pytest.param({"s": 66}, id="no-rank-key"),
        pytest.param({"r": None}, id="null-rank"),
        pytest.param({"r": "high"}, id="string-rank"),
    ],
)
def test_aws_spot_buckets_is_empty_when_the_advisor_schema_changes(advisor, info):
    """A malformed advisor entry yields empty buckets, so the normal order is kept instead of failing every AWS spot config."""
    advisor.return_value.json.return_value = {
        "spot_advisor": {"us-east-1": {"Linux": {"i8g.xlarge": info, "i8ge.xlarge": {"r": 0}}}}
    }

    assert interruption.aws_spot_buckets() == {}
