"""
Tests for get_db_tables function in sdcm.utils.common
"""
import os
import pytest
from unittest.mock import MagicMock

from sdcm.utils.common import get_db_tables


@pytest.fixture
def schema_content():
    """Fixture to load test schema content from file."""
    test_schema_path = os.path.join(
        os.path.dirname(__file__),
        'test_data',
        'schema_with_internals.log'
    )
    with open(test_schema_path, 'r', encoding='utf-8') as file:
        return file.read()


@pytest.fixture
def mock_node(schema_content):
    """Fixture for a mock node with run_cqlsh method."""
    mock_node = MagicMock()
    mock_node.run_cqlsh.return_value.stdout = schema_content
    return mock_node


def test_get_db_tables_keyspace1_compact_storage(mock_node):
    """Test get_db_tables returns no tables for keyspace1 with compact storage."""
    tables = get_db_tables(
        keyspace_name="keyspace1",
        node=mock_node,
        with_compact_storage=True
    )

    assert tables == [], f"Expected no tables with compact storage, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_with_compact_storage_filter_returns_empty_list(mock_node):
    """Test get_db_tables returns no tables when filtering for compact storage in a keyspace without them."""
    tables = get_db_tables(
        keyspace_name="keyspace1",
        node=mock_node,
        with_compact_storage=True
    )

    assert tables == [], f"Expected no tables with compact storage, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_with_non_compact_storage_filter(mock_node):
    """Test get_db_tables returns only non-compact storage tables."""
    tables = get_db_tables(
        keyspace_name="keyspace1",
        node=mock_node,
        with_compact_storage=False
    )

    expected_tables = {"standard1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_without_storage_filter(mock_node):
    """Test get_db_tables returns all tables when no storage filter is applied."""
    tables = get_db_tables(
        keyspace_name="keyspace1",
        node=mock_node,
        with_compact_storage=None
    )

    expected_tables = {"standard1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_returns_correct_tables_for_keyspace(mock_node):
    """Test get_db_tables returns only tables from the specified keyspace."""
    tables = get_db_tables(
        keyspace_name="feeds",
        node=mock_node,
        with_compact_storage=None
    )

    expected_tables = {"table0", "table1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"
    assert "standard1" not in tables, "Found table from wrong keyspace"
    assert "standard1_view" not in tables, "Found materialized view"


def test_get_db_tables_filters_for_compact_storage_tables(mock_node):
    """Test get_db_tables correctly filters for compact storage tables."""
    tables = get_db_tables(
        keyspace_name="feeds",
        node=mock_node,
        with_compact_storage=True
    )

    expected_tables = {"table0"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_filters_for_non_compact_storage_tables(mock_node):
    """Test get_db_tables correctly filters for non-compact storage tables."""
    tables = get_db_tables(
        keyspace_name="feeds",
        node=mock_node,
        with_compact_storage=False
    )

    expected_tables = {"table1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"
