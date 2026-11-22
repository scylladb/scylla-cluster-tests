"""Tests for interruption-aware spot sizing (SCT-707).

For a spot run, sizing swaps the normal pick for a less-interrupted candidate of the same size, and never
trades size for a lower interruption bucket.
"""

from unittest.mock import MagicMock

import pytest
import requests

from sdcm.utils.cloud_catalog import interruption
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo
from sdcm.utils.cloud_catalog.instance_matcher import rank_instances, select_instance

_REAL_AWS_SPOT_BUCKETS = interruption.aws_spot_buckets  # before the autouse stub replaces it

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


def test_spot_keeps_the_size_of_the_normal_pick(catalog, monkeypatch):
    """A smaller type of a later family, with no data, must not replace a bucket-0 normal pick (2TB_dataset)."""
    catalog.instances = [_inst("i8g.2xlarge", "i8g", 8, 0.69), _inst("i8ge.xlarge", "i8ge", 4, 0.47)]
    monkeypatch.setattr(interruption, "aws_spot_buckets", lambda: {"us-east-1": {"i8g.2xlarge": 0}})

    result = select_instance(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region="us-east-1")

    assert result.instance_type == "i8g.2xlarge"


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
    """The real aws_spot_buckets (the autouse stub removed) with the retry session's `get` mocked."""
    monkeypatch.setattr(interruption, "aws_spot_buckets", _REAL_AWS_SPOT_BUCKETS)
    interruption.aws_spot_buckets.cache_clear()
    session = MagicMock()
    monkeypatch.setattr(interruption, "create_retry_session", lambda: session)
    yield session.get
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


@pytest.mark.parametrize("info", [{"s": 66}, {"r": None}, {"r": "high"}])
def test_aws_spot_buckets_is_empty_when_the_advisor_schema_changes(advisor, info):
    """A malformed payload must fall back to the normal order, not fail every AWS spot config."""
    advisor.return_value.json.return_value = {
        "spot_advisor": {"us-east-1": {"Linux": {"i8g.xlarge": info, "i8ge.xlarge": {"r": 0}}}}
    }

    assert interruption.aws_spot_buckets() == {}


@pytest.mark.parametrize("spot_region", [None, "us-east-1", "eu-west-1"])
def test_ranking_starts_with_the_single_pick(catalog, spot_region):
    """`rank_instances` is the full order `select_instance` takes its pick from."""
    ranked = rank_instances(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region=spot_region)

    assert (
        ranked[0].instance_type
        == select_instance(catalog, "db", "aws", {"vcpu": ">=4"}, spot_region=spot_region).instance_type
    )
    assert len(ranked) == len(catalog.instances)


def test_spot_ranking_orders_same_size_siblings_by_interruption(catalog):
    ranked = rank_instances(catalog, "db", "aws", {"vcpu": 4}, spot_region="us-east-1")

    # i8ge.xlarge is bucket 0, i8g.xlarge bucket 4 - the reverse of the on-demand order
    assert [inst.instance_type for inst in ranked] == ["i8ge.xlarge", "i8g.xlarge"]


def test_non_preferred_families_rank_last_and_only_on_request(catalog):
    catalog.instances.append(_inst("im4gn.xlarge", "im4gn", 4, 0.30))  # cheapest, but not a preferred family

    preferred = rank_instances(catalog, "db", "aws", {"vcpu": 4})
    everything = rank_instances(catalog, "db", "aws", {"vcpu": 4}, preferred_families_only=False)

    assert [inst.instance_type for inst in preferred] == ["i8g.xlarge", "i8ge.xlarge"]
    assert [inst.instance_type for inst in everything] == ["i8g.xlarge", "i8ge.xlarge", "im4gn.xlarge"]
