"""This module tests specific nemesis and is heavily dependent on the implementation"""

import logging
import pytest
from unittest.mock import patch

from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from sdcm.nemesis import CategoricalMonkey
from unit_tests.nemesis.fake_cluster import FakeTester
from unit_tests.nemesis.test_sisyphus import TestNemesisClass

LOGGER = logging.getLogger(__name__)


class FakeCategorialMonkey(CategoricalMonkey, TestNemesisClass):
    """Override CategoricalMonkey with a new disruption tree"""


@pytest.mark.parametrize(
    "parent, result",
    [
        (LocalMinimalScyllaPodCluster, True),
        (GkeScyllaPodCluster, True),
        (EksScyllaPodCluster, True),
        (ScyllaGCECluster, False),
        (ScyllaAWSCluster, False),
        (ScyllaDockerCluster, False),
    ],
)
def test_is_it_on_kubernetes(parent, result):
    """Tests is_it_on_kubernetes on different Cluster types"""

    class FakeClass(parent):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    params = {"nemesis_interval": 10, "nemesis_filter_seeds": 1}
    nemesis = TestNemesisClass(FakeTester(db_cluster=FakeClass(), params=params), None)
    assert nemesis._is_it_on_kubernetes() == result


@pytest.mark.parametrize(
    "dist, output",
    [
        ({"CustomNemesisA": 1}, "called test function a\n"),
        ({"CustomNemesisC": 0.5}, "called test function c\n"),
        ({"CustomNemesisAD": 1, "CustomNemesisA": 0}, "called test function d\n"),
    ],
)
def test_categorical_monkey_simple(dist, output, capsys):
    nemesis = FakeCategorialMonkey(FakeTester(), None, dist, default_weight=0)
    method = nemesis.select_next_nemesis()

    method.disrupt()
    captured = capsys.readouterr()
    assert output in captured.out


def test_add_drop_column_cleanup():
    """Test that AddDropColumnMonkey cleanup drops all remaining columns."""
    # Create a test nemesis instance
    nemesis = TestNemesisClass(FakeTester(), None)

    # Set up target table
    nemesis._add_drop_column_target_table = ["keyspace1", "table1"]

    # Simulate columns being added during nemesis execution
    nemesis._add_drop_column_columns_info = {
        "keyspace1": {"table1": {"column_names": {"col1": "int", "col2": "text", "col3": "uuid"}, "column_types": {}}}
    }

    # Mock the CQL query execution
    with patch.object(nemesis, "_add_drop_column_run_cql_query", return_value=True) as mock_cql:
        # Call the cleanup method
        nemesis._add_drop_column_cleanup()

        # Verify CQL query was called to drop all columns
        mock_cql.assert_called_once()
        call_args = mock_cql.call_args
        assert call_args[0][1] == "keyspace1"  # keyspace
        # Verify the command drops all three columns
        cmd = call_args[0][0]
        assert "ALTER TABLE table1 DROP" in cmd
        assert "col1" in cmd
        assert "col2" in cmd
        assert "col3" in cmd

    # Verify the tracking dict was cleared
    assert nemesis._add_drop_column_columns_info["keyspace1"]["table1"]["column_names"] == {}


def test_add_drop_column_cleanup_no_columns():
    """Test that cleanup does nothing when no columns were added."""
    nemesis = TestNemesisClass(FakeTester(), None)

    # Set up target table with no added columns
    nemesis._add_drop_column_target_table = ["keyspace1", "table1"]
    nemesis._add_drop_column_columns_info = {"keyspace1": {"table1": {"column_names": {}, "column_types": {}}}}

    # Mock the CQL query execution
    with patch.object(nemesis, "_add_drop_column_run_cql_query") as mock_cql:
        # Call the cleanup method
        nemesis._add_drop_column_cleanup()

        # Verify CQL query was NOT called since there are no columns to drop
        mock_cql.assert_not_called()


def test_add_drop_column_cleanup_no_target_table():
    """Test that cleanup does nothing when there's no target table."""
    nemesis = TestNemesisClass(FakeTester(), None)

    # No target table set
    nemesis._add_drop_column_target_table = None
    nemesis._add_drop_column_columns_info = {}

    # Mock the CQL query execution
    with patch.object(nemesis, "_add_drop_column_run_cql_query") as mock_cql:
        # Call the cleanup method
        nemesis._add_drop_column_cleanup()

        # Verify CQL query was NOT called
        mock_cql.assert_not_called()
