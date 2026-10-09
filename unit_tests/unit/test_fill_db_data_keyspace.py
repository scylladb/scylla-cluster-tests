"""Tests that FillDatabaseData works on the control-connection fallback path.

With ControlConnectionQueryFallback.SkipPoolCreation, the driver rejects USE statements and
Session.set_keyspace() with InvalidRequest. A session gets its keyspace only when it is opened.
"""

import logging
import re
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from cassandra import InvalidRequest

from sdcm.fill_db_data import FillDatabaseData

USE_STATEMENT = re.compile(r"^\s*USE\b", re.IGNORECASE)
STATEMENT_KEYS = ("create_tables", "truncates", "inserts", "queries", "invalid_queries")
TESTED_METHODS = (
    "fill_db_data",
    "fill_db_data_for_truncate_test",
    "prepare_keyspaces_and_tables",
    "verify_db_data",
    "run_db_queries",
)
RECORDED_STEPS = (
    "truncate_tables",
    "cql_insert_data_to_tables",
    "cql_create_tables",
    "cql_create_simple_tables",
    "cql_insert_data_to_simple_tables",
    "_run_db_queries",
)


class FallbackSession:
    """Session stub that applies the driver's keyspace rules for the fallback path."""

    def __init__(self, keyspace):
        self.keyspace = keyspace
        self.default_fetch_size = 5000

    def set_keyspace(self, keyspace):
        raise InvalidRequest("Cannot change keyspace while using control-connection fallback")

    def execute(self, query, *args, **kwargs):
        if USE_STATEMENT.match(query):
            raise InvalidRequest("Cannot change keyspace while using control-connection fallback")


class _Harness:
    """Minimal stand-in for FillDatabaseData that records the keyspace of each session step."""

    base_ks = FillDatabaseData.base_ks

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.steps = []
        self.db_cluster = MagicMock()
        self.db_cluster.nodes = [MagicMock()]
        self.db_cluster.cql_connection_patient = self._cql_connection_patient
        self.all_verification_items = [{"name": "item", "queries": [], "skip": ""}]
        self._execute_and_log = FillDatabaseData._execute_and_log.__get__(self)
        for name in TESTED_METHODS:
            setattr(self, name, getattr(FillDatabaseData, name).__get__(self))
        for name in RECORDED_STEPS:
            setattr(self, name, self._record(name))

    @contextmanager
    def _cql_connection_patient(self, node, keyspace=None, **kwargs):
        yield FallbackSession(keyspace)

    def _record(self, step):
        def record(*args, **kwargs):
            session = next(arg for arg in args if isinstance(arg, FallbackSession))
            self.steps.append((step, session.keyspace))

        return record


def test_no_verification_statement_changes_keyspace():
    offenders = [
        f"{item['name']}: {statement}"
        for item in FillDatabaseData.all_verification_items
        for key in STATEMENT_KEYS
        for statement in item.get(key, [])
        if isinstance(statement, str) and USE_STATEMENT.match(statement)
    ]

    assert not offenders


@pytest.mark.parametrize(
    ("method", "args", "expected_steps"),
    [
        pytest.param(
            "fill_db_data",
            (),
            [("truncate_tables", FillDatabaseData.base_ks), ("cql_insert_data_to_tables", FillDatabaseData.base_ks)],
            id="fill_db_data",
        ),
        pytest.param(
            "prepare_keyspaces_and_tables",
            (),
            [("cql_create_tables", FillDatabaseData.base_ks)],
            id="prepare_keyspaces_and_tables",
        ),
        pytest.param(
            "verify_db_data",
            (),
            [("_run_db_queries", FillDatabaseData.base_ks)],
            id="verify_db_data",
        ),
        pytest.param(
            "fill_db_data_for_truncate_test",
            (1,),
            [("cql_create_simple_tables", "truncate_ks"), ("cql_insert_data_to_simple_tables", "truncate_ks")],
            id="fill_db_data_for_truncate_test",
        ),
    ],
)
def test_steps_run_in_session_bound_to_keyspace(method, args, expected_steps):
    harness = _Harness()

    getattr(harness, method)(*args)

    assert harness.steps == expected_steps
