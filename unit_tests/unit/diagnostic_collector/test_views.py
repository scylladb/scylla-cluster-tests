"""Unit tests for MVDiagnosticCollector."""

import json

import pytest

from sdcm.utils.diagnostic_collector.views import MVDiagnosticCollector


# --- Mock session ---


class FakeSession:
    """Minimal stand-in for a Cassandra/Scylla session used by MVDiagnosticCollector."""

    def __init__(self, rows_by_query: dict | None = None, fail_queries: set | None = None):
        self.rows_by_query = rows_by_query or {}
        self.fail_queries = fail_queries or set()
        self.executed: list[str] = []

    def execute(self, query: str):
        self.executed.append(query)
        if query in self.fail_queries:
            raise RuntimeError(f"query failed: {query}")
        return self.rows_by_query.get(query, [])


@pytest.fixture
def make_collector(tmp_path):
    """Build a collector over a FakeSession, writing under an isolated tmp_path."""

    def _make(rows_by_query: dict | None = None, fail_queries: set | None = None) -> MVDiagnosticCollector:
        session = FakeSession(rows_by_query=rows_by_query, fail_queries=fail_queries)
        return MVDiagnosticCollector(session, dir_path=str(tmp_path))

    return _make


@pytest.fixture
def collector(make_collector):
    """Collector over a session where every query returns no rows."""
    return make_collector()


@pytest.fixture
def collector_with_rows(make_collector):
    """Collector over a session whose first query returns one row."""
    return make_collector(rows_by_query={MVDiagnosticCollector.queries[0]: [{"a": 1}]})


# --- collect() ---


def test_mv_collect_runs_all_queries(collector):
    """collect() runs every configured query in order."""
    result = collector.collect()

    assert set(result.keys()) == set(MVDiagnosticCollector.queries), (
        f"collect() should produce a result for every query, got {set(result.keys())}"
    )
    assert collector._session.executed == list(MVDiagnosticCollector.queries), (
        f"All queries should be executed in order, got {collector._session.executed}"
    )


def test_mv_collect_returns_fresh_independent_snapshot(collector):
    """Each collect() returns a NEW dict so manager history is not overwritten next cycle."""
    first = collector.collect()
    second = collector.collect()

    assert first is not second, "collect() must return a new dict each cycle, not a shared one"

    # Mutating the latest returned snapshot must not affect the earlier one.
    second.clear()
    assert first, "earlier snapshot must remain intact when a later snapshot is mutated"


def test_mv_clean_does_not_wipe_previously_returned_snapshot(collector):
    """clean() rebinds internal state instead of clearing the shared dict in place."""
    snapshot = collector.collect()
    assert snapshot, "collect() should return a non-empty snapshot"

    collector.clean()

    assert snapshot, "clean() must not empty a snapshot already handed out by collect()"
    assert collector._results == {}, "clean() should reset the collector's own state to an empty dict"


def test_mv_collect_returns_empty_rows_on_query_error(make_collector):
    """A failing query yields an empty list instead of raising, so collect() keeps going."""
    failing_query = MVDiagnosticCollector.queries[0]
    collector = make_collector(fail_queries={failing_query})

    result = collector.collect()

    assert result[failing_query] == [], "Failed query should produce an empty list, not raise"
    # The remaining queries still run.
    assert len(result) == len(MVDiagnosticCollector.queries), (
        f"All queries should be represented even when one fails, got {len(result)}"
    )


# --- store() ---


def test_mv_store_writes_jsonl(collector_with_rows):
    """store() appends one JSON line per query with the expected fields."""
    data = collector_with_rows.collect()
    collector_with_rows.store(data)

    assert collector_with_rows._save_path.exists(), (
        f"store() should create the log file at {collector_with_rows._save_path}"
    )
    lines = collector_with_rows._save_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(MVDiagnosticCollector.queries), (
        f"store() should write one line per query, got {len(lines)}"
    )
    record = json.loads(lines[0])
    assert set(record.keys()) == {"timestamp", "query", "data"}, (
        f"Each record should contain timestamp/query/data, got {set(record.keys())}"
    )


def test_mv_store_falls_back_to_last_snapshot_when_result_empty(collector_with_rows):
    """store(None) falls back to the collector's last collected snapshot."""
    collector_with_rows.collect()
    collector_with_rows.store(None)  # falsy -> fallback to self._results

    assert collector_with_rows._save_path.exists(), "store() fallback should still write the last snapshot to disk"
    lines = collector_with_rows._save_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(MVDiagnosticCollector.queries), (
        f"Fallback store should write one line per query, got {len(lines)}"
    )


def test_mv_store_empty_mapping_does_not_fall_back(collector_with_rows):
    """store({}) writes nothing instead of silently reusing the previous snapshot."""
    collector_with_rows.collect()  # populates self._results with a non-empty snapshot
    collector_with_rows.store({})  # empty-but-valid result must NOT fall back to the last snapshot

    if collector_with_rows._save_path.exists():
        lines = collector_with_rows._save_path.read_text(encoding="utf-8").strip().splitlines()
        assert lines == [], "An empty result must not write the previous snapshot to disk"


# --- output directory ---


def test_mv_output_dir_layout(collector, tmp_path):
    """Output is placed under <dir_path>/diagnostics/mv_si_diagnostics so it is collected with logs."""
    expected_dir = tmp_path / "diagnostics" / "mv_si_diagnostics"
    assert collector._dir == expected_dir, f"Collector dir should be {expected_dir}, got {collector._dir}"
    assert expected_dir.is_dir(), "Collector should create its output directory on init"
