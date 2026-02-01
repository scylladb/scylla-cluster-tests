"""Test delete nemesis validation logic"""

import pytest
from unittest.mock import MagicMock

from sdcm.nemesis import NemesisRunner
from sdcm.exceptions import UnsupportedNemesis
from sdcm.utils.database_query_utils import PartitionsValidationAttributes
from unit_tests.nemesis.fake_cluster import FakeTester


class TestDeleteNemesisValidation:
    """Tests for verify_initial_inputs_for_delete_nemesis validation"""

    def test_verify_delete_nemesis_requires_scylla_bench_keyspace(self):
        """Test that nemesis raises UnsupportedNemesis when scylla_bench keyspace is not present"""
        tester = FakeTester()
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return keyspaces without scylla_bench
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["keyspace1", "keyspace2"])

        with pytest.raises(UnsupportedNemesis, match="This nemesis can run on scylla_bench test only"):
            nemesis.verify_initial_inputs_for_delete_nemesis()

    def test_verify_delete_nemesis_requires_max_partitions(self):
        """Test that nemesis raises UnsupportedNemesis when max_partitions_in_test_table is not set"""
        tester = FakeTester()
        tester.partitions_attrs = None
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return scylla_bench keyspace
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["scylla_bench"])

        with pytest.raises(UnsupportedNemesis, match='expects "max_partitions_in_test_table"'):
            nemesis.verify_initial_inputs_for_delete_nemesis()

    def test_verify_delete_nemesis_insufficient_non_validated_partitions(self):
        """Test that nemesis raises UnsupportedNemesis when there are insufficient non-validated partitions"""
        tester = FakeTester()
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return scylla_bench keyspace
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["scylla_bench"])

        # Create partitions_attrs with insufficient non-validated partitions
        # This simulates provision test scenario: partition_range 0-10 with max 10 partitions
        # Results in 0 non-validated partitions: 10 - (10 - 0) = 0
        partitions_attrs = PartitionsValidationAttributes(
            tester=tester,
            table_name="scylla_bench.test",
            primary_key_column="pk",
            max_partitions_in_test_table="10",
            partition_range_with_data_validation="0-10",
        )
        tester.partitions_attrs = partitions_attrs

        with pytest.raises(UnsupportedNemesis, match="This nemesis requires at least .* non-validated partitions"):
            nemesis.verify_initial_inputs_for_delete_nemesis()

    def test_verify_delete_nemesis_with_minimum_required_partitions(self):
        """Test that nemesis succeeds when non-validated partitions equals num_deletions_factor (minimum threshold)"""
        tester = FakeTester()
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return scylla_bench keyspace
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["scylla_bench"])

        # Create partitions_attrs with exactly num_deletions_factor (5) non-validated partitions
        # partition_range 0-10 with max 15 partitions = 15 - (10 - 0) = 5
        # This tests the boundary condition where we have exactly the minimum required partitions
        partitions_attrs = PartitionsValidationAttributes(
            tester=tester,
            table_name="scylla_bench.test",
            primary_key_column="pk",
            max_partitions_in_test_table="15",
            partition_range_with_data_validation="0-10",
        )
        tester.partitions_attrs = partitions_attrs

        # Verify that having exactly num_deletions_factor partitions is sufficient
        # The validation check is: if non_validated_partitions < min_required_partitions
        # With 5 partitions and min_required of 5, this evaluates to False, so no exception is raised
        try:
            nemesis.verify_initial_inputs_for_delete_nemesis()
            # This should succeed - having exactly the minimum is acceptable
        except UnsupportedNemesis:
            pytest.fail("Should not raise UnsupportedNemesis with exactly num_deletions_factor partitions")

    def test_verify_delete_nemesis_sufficient_non_validated_partitions(self):
        """Test that nemesis succeeds when there are sufficient non-validated partitions"""
        tester = FakeTester()
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return scylla_bench keyspace
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["scylla_bench"])

        # Create partitions_attrs with plenty of non-validated partitions
        # partition_range 0-10 with max 100 partitions = 100 - (10 - 0) = 90
        partitions_attrs = PartitionsValidationAttributes(
            tester=tester,
            table_name="scylla_bench.test",
            primary_key_column="pk",
            max_partitions_in_test_table="100",
            partition_range_with_data_validation="0-10",
        )
        tester.partitions_attrs = partitions_attrs

        # Should not raise any exception
        try:
            nemesis.verify_initial_inputs_for_delete_nemesis()
        except UnsupportedNemesis:
            pytest.fail("Should not raise UnsupportedNemesis with sufficient non-validated partitions")

    def test_verify_delete_nemesis_no_partition_range_validation(self):
        """Test that nemesis succeeds when partition_range_with_data_validation is not set"""
        tester = FakeTester()
        nemesis = NemesisRunner(tester, None)

        # Mock cluster to return scylla_bench keyspace
        nemesis.cluster.get_test_keyspaces = MagicMock(return_value=["scylla_bench"])

        # Create partitions_attrs without partition_range_with_data_validation
        partitions_attrs = PartitionsValidationAttributes(
            tester=tester,
            table_name="scylla_bench.test",
            primary_key_column="pk",
            max_partitions_in_test_table="100",
            partition_range_with_data_validation=None,
        )
        tester.partitions_attrs = partitions_attrs

        # Should not raise any exception since partition_range_with_data_validation is not set
        try:
            nemesis.verify_initial_inputs_for_delete_nemesis()
        except UnsupportedNemesis:
            pytest.fail("Should not raise UnsupportedNemesis when partition_range_with_data_validation is not set")
