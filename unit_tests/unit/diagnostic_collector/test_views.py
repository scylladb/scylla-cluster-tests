"""Unit tests for MVDiagnosticCollector."""

import json

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


# --- collect() ---


def test_mv_collect_runs_all_queries(tmp_path):
    """collect() runs every configured query in order."""
    session = FakeSession()
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    result = collector.collect()

    assert set(result.keys()) == set(MVDiagnosticCollector.queries), (
        f"collect() should produce a result for every query, got {set(result.keys())}"
    )
    assert session.executed == list(MVDiagnosticCollector.queries), (
        f"All queries should be executed in order, got {session.executed}"
    )


def test_mv_collect_returns_fresh_independent_snapshot(tmp_path):
    """Each collect() returns a NEW dict so manager history is not overwritten next cycle."""
    session = FakeSession()
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    first = collector.collect()
    second = collector.collect()

    assert first is not second, "collect() must return a new dict each cycle, not a shared one"

    # Mutating the latest returned snapshot must not affect the earlier one.
    second.clear()
    assert first, "earlier snapshot must remain intact when a later snapshot is mutated"


def test_mv_clean_does_not_wipe_previously_returned_snapshot(tmp_path):
    """clean() rebinds internal state instead of clearing the shared dict in place."""
    session = FakeSession()
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    snapshot = collector.collect()
    assert snapshot, "collect() should return a non-empty snapshot"

    collector.clean()

    assert snapshot, "clean() must not empty a snapshot already handed out by collect()"
    assert collector._results == {}, "clean() should reset the collector's own state to an empty dict"


def test_mv_execute_query_returns_empty_on_error(tmp_path):
    """A failing query yields an empty list instead of raising, so collect() keeps going."""
    failing_query = MVDiagnosticCollector.queries[0]
    session = FakeSession(fail_queries={failing_query})
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    result = collector.collect()

    assert result[failing_query] == [], "Failed query should produce an empty list, not raise"
    # The remaining queries still run.
    assert len(result) == len(MVDiagnosticCollector.queries), (
        f"All queries should be represented even when one fails, got {len(result)}"
    )


# --- store() ---


def test_mv_store_writes_jsonl(tmp_path):
    """store() appends one JSON line per query with the expected fields."""
    rows = {MVDiagnosticCollector.queries[0]: [{"a": 1}]}
    session = FakeSession(rows_by_query=rows)
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    data = collector.collect()
    collector.store(data)

    assert collector._save_path.exists(), f"store() should create the log file at {collector._save_path}"
    lines = collector._save_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(MVDiagnosticCollector.queries), (
        f"store() should write one line per query, got {len(lines)}"
    )
    record = json.loads(lines[0])
    assert set(record.keys()) == {"timestamp", "query", "data"}, (
        f"Each record should contain timestamp/query/data, got {set(record.keys())}"
    )


def test_mv_store_falls_back_to_last_snapshot_when_result_empty(tmp_path):
    """store(None) falls back to the collector's last collected snapshot."""
    rows = {MVDiagnosticCollector.queries[0]: [{"a": 1}]}
    session = FakeSession(rows_by_query=rows)
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    collector.collect()
    collector.store(None)  # falsy -> fallback to self._results

    assert collector._save_path.exists(), "store() fallback should still write the last snapshot to disk"
    lines = collector._save_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(MVDiagnosticCollector.queries), (
        f"Fallback store should write one line per query, got {len(lines)}"
    )


def test_mv_store_empty_mapping_does_not_fall_back(tmp_path):
    """store({}) writes nothing instead of silently reusing the previous snapshot."""
    rows = {MVDiagnosticCollector.queries[0]: [{"a": 1}]}
    session = FakeSession(rows_by_query=rows)
    collector = MVDiagnosticCollector(session, dir_path=str(tmp_path))

    collector.collect()  # populates self._results with a non-empty snapshot
    collector.store({})  # empty-but-valid result must NOT fall back to the last snapshot

    if collector._save_path.exists():
        lines = collector._save_path.read_text(encoding="utf-8").strip().splitlines()
        assert lines == [], "An empty result must not write the previous snapshot to disk"


# --- output directory ---


def test_mv_output_dir_layout(tmp_path):
    """Output is placed under <dir_path>/diagnostics/mv_si_diagnostics so it is collected with logs."""
    collector = MVDiagnosticCollector(FakeSession(), dir_path=str(tmp_path))

    expected_dir = tmp_path / "diagnostics" / "mv_si_diagnostics"
    assert collector._dir == expected_dir, f"Collector dir should be {expected_dir}, got {collector._dir}"
    assert expected_dir.is_dir(), "Collector should create its output directory on init"
