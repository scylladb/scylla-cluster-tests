import random
from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.utils.indexes import cql_type_references, get_column_names, get_random_column_name

COLUMNS = [
    MagicMock(column_name="id", type="int"),
    MagicMock(column_name="username", type="text"),
    MagicMock(column_name="tags", type="list<text>"),
    MagicMock(column_name="scores", type="set<int>"),
    MagicMock(column_name="metadata", type="map<text, text>"),
    MagicMock(column_name="counter_col", type="counter"),
    MagicMock(column_name="duration_col", type="duration"),
]

# The column types that gemini generates for a gemini-1tb-10h schema.
GEMINI_COLUMNS = [
    MagicMock(column_name="col0", type="bigint"),
    MagicMock(column_name="col1", type="frozen<tuple<int, duration>>"),
    MagicMock(column_name="col2", type="frozen<udt_1234>"),
    MagicMock(column_name="col3", type="frozen<map<int, duration>>"),
    MagicMock(column_name="col4", type="frozen<udt_5678>"),
    MagicMock(column_name="col5", type="frozen<list<timeuuid>>"),
]

GEMINI_USER_TYPES = [
    MagicMock(type_name="udt_1234", field_types=["text", "duration"]),
    MagicMock(type_name="udt_5678", field_types=["int", "text"]),
]


def make_session(columns, user_types=()):
    """Return a mocked session that answers the system_schema.columns and system_schema.types queries."""
    session = MagicMock()
    session.execute.side_effect = lambda query: list(user_types if "system_schema.types" in query else columns)
    return session


@pytest.fixture
def mocked_session():
    return make_session(COLUMNS)


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
    view nemesis replays from nemesis_seed; the picks stay within the table's columns.
    """
    generator_a, generator_b = random.Random(9), random.Random(9)

    picks_a = [get_random_column_name(mocked_session, "ks", "cf", random_generator=generator_a) for _ in range(10)]
    picks_b = [get_random_column_name(mocked_session, "ks", "cf", random_generator=generator_b) for _ in range(10)]

    assert picks_a == picks_b
    assert set(picks_a) <= {"id", "username", "tags", "scores", "metadata", "counter_col", "duration_col"}


def test_get_column_names_filter_type_nested_in_gemini_types():
    """A materialized view cannot use a column that contains a duration in its primary key.
    The filter finds the duration in a tuple, a frozen map and the fields of a user-defined type.
    """
    session = make_session(GEMINI_COLUMNS, GEMINI_USER_TYPES)

    result = get_column_names(
        session, "ks1", "table1", filter_out_collections=True, filter_out_column_types=["duration", "counter"]
    )

    assert result == ["col0", "col4", "col5"]


@pytest.mark.parametrize(
    "cql_type, expected",
    [
        ("duration", True),
        ("int", False),
        ("frozen<list<duration>>", True),
        ('frozen<"MyType">', True),
        ("frozen<outer_udt>", True),
        ("frozen<looping_udt>", False),
        ("timeuuid", False),
    ],
)
def test_cql_type_references(cql_type, expected):
    user_types = {
        "MyType": ["duration"],
        "outer_udt": ["text", "frozen<inner_udt>"],
        "inner_udt": ["frozen<tuple<int, duration>>"],
        "looping_udt": ["frozen<looping_udt>", "int"],
    }

    assert cql_type_references(cql_type, {"duration"}, user_types) is expected
