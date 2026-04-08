"""
Tests for get_db_tables function in sdcm.utils.common
"""

import pytest
from unittest.mock import MagicMock

from sdcm.utils.common import get_db_tables


class MockRow:
    """Mock row object for system_schema query results."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class MockResult:
    """Mock result object with current_rows attribute."""

    def __init__(self, rows):
        self.current_rows = rows


@pytest.fixture
def mock_node():
    """Fixture for a mock node with parent_cluster and cql_connection_patient"""
    mock_node = MagicMock()
    mock_node.parent_cluster = MagicMock()

    def mock_execute(query):
        """Mock session.execute() to return results based on query"""
        if "system_schema.views" in query:
            return MockResult([])
        if "system_schema.tables" in query:
            if "keyspace1" in query:
                return MockResult([MockRow(table_name="standard1", flags={"compound"})])
            if "feeds" in query:
                return MockResult(
                    [MockRow(table_name="table0", flags={"dense"}), MockRow(table_name="table1", flags={"compound"})]
                )

        return MockResult([])

    # setup context manager for cql_connection_patient
    mock_session = MagicMock()
    mock_session.execute.side_effect = mock_execute
    mock_node.parent_cluster.cql_connection_patient.return_value.__enter__.return_value = mock_session

    return mock_node


def test_get_db_tables_keyspace1_compact_storage(mock_node):
    """Test get_db_tables returns no tables for keyspace1 with compact storage."""
    tables = get_db_tables(keyspace_name="keyspace1", node=mock_node, with_compact_storage=True)

    assert tables == [], f"Expected no tables with compact storage, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_with_compact_storage_filter_returns_empty_list(mock_node):
    """Test get_db_tables returns no tables when filtering for compact storage in a keyspace without them."""
    tables = get_db_tables(keyspace_name="keyspace1", node=mock_node, with_compact_storage=True)

    assert tables == [], f"Expected no tables with compact storage, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_with_non_compact_storage_filter(mock_node):
    """Test get_db_tables returns only non-compact storage tables."""
    tables = get_db_tables(keyspace_name="keyspace1", node=mock_node, with_compact_storage=False)

    expected_tables = {"standard1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_without_storage_filter(mock_node):
    """Test get_db_tables returns all tables when no storage filter is applied."""
    tables = get_db_tables(keyspace_name="keyspace1", node=mock_node, with_compact_storage=None)

    expected_tables = {"standard1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_returns_correct_tables_for_keyspace(mock_node):
    """Test get_db_tables returns only tables from the specified keyspace."""
    tables = get_db_tables(keyspace_name="feeds", node=mock_node, with_compact_storage=None)

    expected_tables = {"table0", "table1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"
    assert "standard1" not in tables, "Found table from wrong keyspace"
    assert "standard1_view" not in tables, "Found materialized view"


def test_get_db_tables_filters_for_compact_storage_tables(mock_node):
    """Test get_db_tables correctly filters for compact storage tables."""
    tables = get_db_tables(keyspace_name="feeds", node=mock_node, with_compact_storage=True)

    expected_tables = {"table0"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"


def test_get_db_tables_filters_for_non_compact_storage_tables(mock_node):
    """Test get_db_tables correctly filters for non-compact storage tables."""
    tables = get_db_tables(keyspace_name="feeds", node=mock_node, with_compact_storage=False)

    expected_tables = {"table1"}
    assert set(tables) == expected_tables, f"Expected tables {expected_tables}, got {tables}"
    assert all("_view" not in table for table in tables), f"Found materialized views in {tables}"
