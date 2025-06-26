import pytest
from unittest.mock import MagicMock
from sdcm.utils.nemesis_utils.indexes import get_column_names


@pytest.fixture
def mocked_session():
    session = MagicMock()
    session.execute.return_value = [
        MagicMock(column_name='id', type='int'),
        MagicMock(column_name='username', type='text'),
        MagicMock(column_name='tags', type='list<text>'),
        MagicMock(column_name='scores', type='set<int>'),
        MagicMock(column_name='metadata', type='map<text, text>'),
        MagicMock(column_name='counter_col', type='counter'),
        MagicMock(column_name='duration_col', type='duration'),
    ]
    return session


def test_get_column_names_no_filters(mocked_session):
    result = set(get_column_names(mocked_session, 'ks', 'cf'))
    expected = {'id', 'username', 'tags', 'scores', 'metadata', 'counter_col', 'duration_col'}
    assert result == expected


def test_get_column_names_filter_collections(mocked_session):
    result = set(get_column_names(mocked_session, 'ks', 'cf', filter_out_collections=True))
    excluded = {'tags', 'scores', 'metadata'}
    included = {'id', 'username', 'counter_col', 'duration_col'}
    assert excluded.isdisjoint(result)
    assert included.issubset(result)


def test_get_column_names_filter_unsupported(mocked_session):
    result = set(get_column_names(
        mocked_session, 'ks', 'cf',
        filter_out_column_types=['counter', 'duration']
    ))
    excluded = {'counter_col', 'duration_col'}
    included = {'id', 'username', 'tags', 'scores', 'metadata'}
    assert excluded.isdisjoint(result)
    assert included.issubset(result)


def test_get_column_names_combined_filters(mocked_session):
    result = set(get_column_names(
        mocked_session, 'ks', 'cf',
        filter_out_collections=True,
        filter_out_column_types=['counter', 'duration']
    ))
    expected = {'id', 'username'}
    assert result == expected
