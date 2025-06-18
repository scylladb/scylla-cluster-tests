import unittest
from unittest.mock import MagicMock
from sdcm.utils.nemesis_utils.indexes import get_column_names


class TestGetColumnNamesWithCQLTypes(unittest.TestCase):
    def setUp(self):
        # Mocked result of SELECT column_name, type FROM system_schema.columns
        self.session = MagicMock()
        self.session.execute.return_value = [
            MagicMock(column_name='id', type='int'),
            MagicMock(column_name='username', type='text'),
            MagicMock(column_name='tags', type='list<text>'),
            MagicMock(column_name='scores', type='set<int>'),
            MagicMock(column_name='metadata', type='map<text, text>'),
            MagicMock(column_name='counter_col', type='counter'),
            MagicMock(column_name='duration_col', type='duration'),
        ]

    def test_no_filters(self):
        """All columns should be returned when no filters are applied."""
        result = get_column_names(self.session, 'ks', 'cf')
        self.assertCountEqual(result, [
            'id', 'username', 'tags', 'scores',
            'metadata', 'counter_col', 'duration_col'
        ])

    def test_filter_out_collections(self):
        """Should exclude collection types (list, set, map)."""
        result = get_column_names(self.session, 'ks', 'cf', filter_out_collections=True)
        self.assertNotIn('tags', result)
        self.assertNotIn('scores', result)
        self.assertNotIn('metadata', result)
        self.assertIn('id', result)
        self.assertIn('username', result)
        self.assertIn('counter_col', result)
        self.assertIn('duration_col', result)

    def test_filter_out_unsupported_types(self):
        """Should exclude unsupported types like counter and duration."""
        result = get_column_names(
            self.session, 'ks', 'cf',
            unsupported_primary_key_columns=['counter', 'duration']
        )
        self.assertNotIn('counter_col', result)
        self.assertNotIn('duration_col', result)
        self.assertIn('tags', result)
        self.assertIn('id', result)
        self.assertIn('scores', result)
        self.assertIn('metadata', result)

    def test_combined_filters(self):
        """Should exclude both collections and unsupported types."""
        result = get_column_names(
            self.session, 'ks', 'cf',
            filter_out_collections=True,
            unsupported_primary_key_columns=['counter', 'duration']
        )
        self.assertNotIn('tags', result)
        self.assertNotIn('scores', result)
        self.assertNotIn('metadata', result)
        self.assertNotIn('counter_col', result)
        self.assertNotIn('duration_col', result)
        self.assertIn('id', result)
        self.assertIn('username', result)
