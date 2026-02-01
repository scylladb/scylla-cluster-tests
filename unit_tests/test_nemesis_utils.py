"""Unit tests for nemesis utility functions."""

import pytest

from sdcm.nemesis import safe_split_keyspace_table


class TestSafeSplitKeyspaceTable:
    """Test cases for safe_split_keyspace_table helper function."""

    def test_valid_keyspace_table_string(self):
        """Test with valid 'keyspace.table' string."""
        keyspace, table = safe_split_keyspace_table("test_keyspace.test_table")
        assert keyspace == "test_keyspace"
        assert table == "test_table"

    def test_valid_keyspace_table_with_quoted_names(self):
        """Test with quoted identifiers."""
        keyspace, table = safe_split_keyspace_table('"MyKeyspace"."MyTable"')
        assert keyspace == '"MyKeyspace"'
        assert table == '"MyTable"'

    def test_keyspace_table_with_multiple_dots(self):
        """Test with table name containing dots (edge case)."""
        keyspace, table = safe_split_keyspace_table("keyspace.table.with.dots")
        assert keyspace == "keyspace"
        assert table == "table.with.dots"

    def test_list_with_valid_string(self, caplog):
        """Test when keyspace_table is a list containing a valid string - logs warning."""
        import logging
        with caplog.at_level(logging.WARNING):
            keyspace, table = safe_split_keyspace_table(["keyspace.table"])
            assert keyspace == "keyspace"
            assert table == "table"
            # Verify warning was logged
            assert "keyspace_table is a list instead of string" in caplog.text
            assert "using first element" in caplog.text

    def test_empty_list(self):
        """Test when keyspace_table is an empty list - should raise ValueError."""
        with pytest.raises(ValueError, match="keyspace_table is an empty list"):
            safe_split_keyspace_table([])

    def test_list_with_multiple_items(self, caplog):
        """Test when keyspace_table is a list with multiple items - should use first and log warning."""
        import logging
        with caplog.at_level(logging.WARNING):
            keyspace, table = safe_split_keyspace_table(["ks1.table1", "ks2.table2"])
            assert keyspace == "ks1"
            assert table == "table1"
            # Verify warning was logged
            assert "keyspace_table is a list instead of string" in caplog.text

    def test_invalid_type(self):
        """Test when keyspace_table is not a string or list."""
        with pytest.raises(ValueError, match="keyspace_table must be a string"):
            safe_split_keyspace_table(123)

    def test_missing_dot_separator(self):
        """Test when keyspace_table doesn't contain a dot."""
        with pytest.raises(ValueError, match="must contain a dot separator"):
            safe_split_keyspace_table("invalid_format")

    def test_empty_keyspace(self):
        """Test when keyspace part is empty."""
        with pytest.raises(ValueError, match="keyspace and table cannot be empty"):
            safe_split_keyspace_table(".table")

    def test_empty_table(self):
        """Test when table part is empty."""
        with pytest.raises(ValueError, match="keyspace and table cannot be empty"):
            safe_split_keyspace_table("keyspace.")

    def test_only_dot(self):
        """Test when keyspace_table is just a dot."""
        with pytest.raises(ValueError, match="keyspace and table cannot be empty"):
            safe_split_keyspace_table(".")

    def test_none_value(self):
        """Test when keyspace_table is None."""
        with pytest.raises(ValueError, match="keyspace_table must be a string"):
            safe_split_keyspace_table(None)

    def test_list_with_nested_list(self):
        """Test when keyspace_table is a list containing another list."""
        with pytest.raises(ValueError, match="keyspace_table must be a string"):
            safe_split_keyspace_table([["keyspace.table"]])

    def test_list_with_empty_nested_list(self):
        """Test when keyspace_table is a list containing an empty list."""
        with pytest.raises(ValueError, match="keyspace_table must be a string"):
            safe_split_keyspace_table([[]])
