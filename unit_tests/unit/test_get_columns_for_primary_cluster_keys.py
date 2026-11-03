import random
from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.utils.indexes import get_column_names, get_random_column_name


@pytest.fixture
def mocked_session():
    session = MagicMock()
    session.execute.return_value = [
        MagicMock(column_name="id", type="int"),
        MagicMock(column_name="username", type="text"),
        MagicMock(column_name="tags", type="list<text>"),
        MagicMock(column_name="scores", type="set<int>"),
        MagicMock(column_name="metadata", type="map<text, text>"),
        MagicMock(column_name="counter_col", type="counter"),
        MagicMock(column_name="duration_col", type="duration"),
    ]
    return session


def test_get_column_names_no_filters(mocked_session):
    result = set(get_column_names(mocked_session, "ks", "cf"))
    expected = {"id", "username", "tags", "scores", "metadata", "counter_col", "duration_col"}
    assert result == expected


def test_get_column_names_filter_collections(mocked_session):
    result = set(get_column_names(mocked_session, "ks", "cf", filter_out_collections=True))
    excluded = {"tags", "scores", "metadata"}
    included = {"id", "username", "counter_col", "duration_col"}
    assert excluded.isdisjoint(result)
    assert included.issubset(result)


def test_get_column_names_filter_unsupported(mocked_session):
    result = set(get_column_names(mocked_session, "ks", "cf", filter_out_column_types=["counter", "duration"]))
    excluded = {"counter_col", "duration_col"}
    included = {"id", "username", "tags", "scores", "metadata"}
    assert excluded.isdisjoint(result)
    assert included.issubset(result)


def test_get_column_names_combined_filters(mocked_session):
    result = set(
        get_column_names(
            mocked_session, "ks", "cf", filter_out_collections=True, filter_out_column_types=["counter", "duration"]
        )
    )
    expected = {"id", "username"}
    assert result == expected


def test_get_random_column_name_with_random_generator_is_reproducible(mocked_session):
    """Two generators with the same seed pick the same columns in the same order, so an index or
    view nemesis replays from nemesis_seed; the picks stay within the table's columns."""
    generator_a, generator_b = random.Random(9), random.Random(9)

    picks_a = [get_random_column_name(mocked_session, "ks", "cf", random_generator=generator_a) for _ in range(10)]
    picks_b = [get_random_column_name(mocked_session, "ks", "cf", random_generator=generator_b) for _ in range(10)]

    assert picks_a == picks_b
    assert set(picks_a) <= {"id", "username", "tags", "scores", "metadata", "counter_col", "duration_col"}
