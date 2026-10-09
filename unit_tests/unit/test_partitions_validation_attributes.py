"""Tests for PartitionsValidationAttributes partition-range attributes.

The partition-range attributes are read unconditionally by the delete nemesis
(sdcm/nemesis/monkey/data_operations.py), so they must always be defined - also when
data_validation configures no validated range at all.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.utils.database_query_utils import PartitionsValidationAttributes


def build_attrs(max_partitions=None, partition_range=None):
    return PartitionsValidationAttributes(
        tester=MagicMock(),
        table_name="scylla_bench.test",
        primary_key_column="pk",
        max_partitions_in_test_table=max_partitions,
        partition_range_with_data_validation=partition_range,
    )


def test_partition_range_attributes_default_without_validated_range():
    attrs = build_attrs(max_partitions="1000")

    assert attrs.partition_start_range == 0
    assert attrs.partition_end_range == 0
    # No protected range, so the whole table counts as non-validated.
    assert attrs.non_validated_partitions == 1000


def test_partition_range_attributes_default_without_any_data_validation():
    attrs = build_attrs()

    assert attrs.max_partitions_in_test_table is None
    assert attrs.partition_start_range == 0
    assert attrs.partition_end_range == 0
    assert attrs.non_validated_partitions == 0


def test_partition_range_attributes_with_validated_range():
    attrs = build_attrs(max_partitions="1000", partition_range="0-800")

    assert attrs.partition_start_range == 0
    assert attrs.partition_end_range == 800
    assert attrs.non_validated_partitions == 200


def test_non_validated_partitions_stays_zero_when_max_partitions_unset():
    attrs = build_attrs(partition_range="0-800")

    assert attrs.partition_end_range == 800
    assert attrs.non_validated_partitions == 0


@pytest.mark.parametrize("max_partitions,partition_range", [("400", "0-100"), ("10", "0-5")])
def test_partition_range_attributes_are_always_defined(max_partitions, partition_range):
    """Every combination used by the shipped test-cases must expose all three attributes."""
    attrs = build_attrs(max_partitions=max_partitions, partition_range=partition_range)

    assert attrs.non_validated_partitions == int(max_partitions) - attrs.partition_end_range
