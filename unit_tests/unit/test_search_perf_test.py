# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Unit tests for the pure helpers of the shared search performance flow.

Exercised through a made-up workload rather than through the full-text one, so that a change in
what FTS happens to be called cannot make these pass or fail. fts_test's own descriptor is checked
in test_fts_test.py.
"""

import contextlib
import json
import logging
import os
import re
import types

import pytest

import search_perf_test
from sdcm.stress.latte_thread import get_latte_operation_type
from search_perf_test import (
    LatteScriptParams,
    SearchWorkload,
    _checked_data_file,
    _checked_name,
    _expected_p99_read_ms,
    QUERY_EXAMPLE_MAX_CHARS,
    _first_query_example,
    _parse_shard_spec,
    _query_params,
    _timeout_minutes,
    indexing_throughput,
    resolve_test_config_path,
    validate_plan_queries,
)

WORKLOAD = SearchWorkload(
    name="search_bench",
    base_dir="data_dir/latte/search_bench",
    script="data_dir/latte/search_bench/bench.rn",
    hdr_tag="fn--search",
    item_noun="docs",
    index_prefix="bench_idx",
    default_keyspace="bench",
    container_root="/tmp/bench",
    params=LatteScriptParams(
        dataset_dir="data_dir",
        records_file="records_file",
        record_count="record_count",
        queries_file="queries_file",
        qrels_file="qrels_file",
        search_limit="search_limit",
        compute_accuracy="compute_accuracy",
        index_name="index_name",
        max_index_wait="max_index_wait_secs",
        min_probes="min_successful_probes",
        schema_cleanup="schema_cleanup",
        drop_index="drop_index",
    ),
    step_records_file_key="records_file",
    default_records_file="records.tsv",
    default_shard_suffix="records_{:03d}.tsv",
)

DEFAULTS = {"limit": 5, "concurrency": 32, "rate": 0, "expected_p99_read_ms": 10}

# ---------------------------------------------------------------------------
# Resolving the expected latency
# ---------------------------------------------------------------------------


def test_expected_p99_read_ms_query_overrides_defaults():
    assert _expected_p99_read_ms({"set": "s", "expected_p99_read_ms": 50}, {"expected_p99_read_ms": 10}) == 50.0


def test_expected_p99_read_ms_falls_back_to_defaults():
    assert _expected_p99_read_ms({"set": "s"}, {"expected_p99_read_ms": 10}) == 10.0


def test_expected_p99_read_ms_missing_raises():
    with pytest.raises(ValueError, match="term_common"):
        _expected_p99_read_ms({"set": "term_common"}, {})


@pytest.mark.parametrize(
    "value",
    (True, 0, -1, float("nan"), float("inf"), "fast", [10]),
    ids=["bool", "zero", "negative", "nan", "inf", "string", "list"],
)
def test_expected_p99_read_ms_rejects_unusable_values(value):
    """The measured P99 is compared against this value. 'float()' alone accepts all of these: a bool
    from an unquoted YAML 'yes', or an expectation no run can meet or none can miss."""
    with pytest.raises(ValueError, match="expected a finite number > 0"):
        _expected_p99_read_ms({"set": "term_common", "expected_p99_read_ms": value}, {})


# ---------------------------------------------------------------------------
# Validating a plan's query entries up front
# ---------------------------------------------------------------------------


def _plan(query, defaults=None):
    return [{"name": "ds", "defaults": DEFAULTS if defaults is None else defaults, "steps": [{"queries": [query]}]}]


def test_validate_plan_queries_accepts_a_good_plan():
    validate_plan_queries(_plan({"set": "term_common", "limit": 100, "concurrency": 1, "rate": 50, "duration": "5m"}))


@pytest.mark.parametrize(
    "query, message",
    (
        ({"limit": 5}, "has no 'set'"),
        ({"set": "term common"}, "query set name"),
        ({"set": "t", "limit": 0}, "query limit"),
        ({"set": "t", "limit": "5; rm -rf /"}, "query limit"),
        ({"set": "t", "concurrency": 0}, "query concurrency"),
        ({"set": "t", "rate": -1}, "query rate"),
        ({"set": "t", "duration": "10"}, "query duration"),
        ({"set": "t", "duration": "10s; id"}, "query duration"),
    ),
)
def test_validate_plan_queries_rejects_a_bad_entry(query, message):
    """Every one of these reaches a latte command line, so it is checked before the run rather than
    quoted at each use."""
    with pytest.raises(ValueError, match=re.escape(message)):
        validate_plan_queries(_plan(query))


def test_validate_plan_queries_names_the_offending_step():
    with pytest.raises(ValueError, match=r"dataset 'ds', step #1"):
        validate_plan_queries(_plan({"set": "t"}, defaults={}))


def test_validate_plan_queries_ignores_steps_without_queries():
    validate_plan_queries([{"name": "ds", "steps": [{"shards": []}, {"documents_file": "documents.tsv"}]}])


def test_rate_zero_is_allowed_as_unthrottled():
    """0 means 'no --rate at all', not a missing value -- it is the DEFAULT_RATE."""
    assert _query_params({"set": "t", "rate": 0}, DEFAULTS)[2] == 0


def test_booleans_are_not_accepted_as_numbers():
    """'True' is an int in Python and would reach the command line as 'concurrency=True'."""
    with pytest.raises(ValueError, match="query concurrency"):
        _query_params({"set": "t", "concurrency": True}, DEFAULTS)


# ---------------------------------------------------------------------------
# Extracting a readable query example for a query record
# ---------------------------------------------------------------------------


def test_first_query_example_returns_the_text_column(tmp_path):
    (tmp_path / "queries_term_common.tsv").write_text("q1\tfirst query text\nq2\tsecond\n", encoding="utf-8")
    assert _first_query_example(str(tmp_path), "queries_term_common.tsv") == "first query text"


def test_first_query_example_skips_leading_blank_lines(tmp_path):
    (tmp_path / "queries_natural.tsv").write_text("\n\nq1\tactual first\n", encoding="utf-8")
    assert _first_query_example(str(tmp_path), "queries_natural.tsv") == "actual first"


def test_first_query_example_missing_file_returns_empty_string(tmp_path):
    assert _first_query_example(str(tmp_path), "queries_missing.tsv") == ""


def test_first_query_example_without_a_tab_returns_whole_line(tmp_path):
    (tmp_path / "queries_odd.tsv").write_text("just one column\n", encoding="utf-8")
    assert _first_query_example(str(tmp_path), "queries_odd.tsv") == "just one column"


def test_first_query_example_truncates_a_long_query(tmp_path):
    """The record's field is bounded here, not by the corpus the plan happens to name."""
    (tmp_path / "queries_long.tsv").write_text(f"q1\t{'a' * 900}\n", encoding="utf-8")

    example = _first_query_example(str(tmp_path), "queries_long.tsv")

    assert len(example) == QUERY_EXAMPLE_MAX_CHARS
    assert example.endswith("...")


def test_first_query_example_keeps_a_query_at_the_cap_intact(tmp_path):
    (tmp_path / "queries_exact.tsv").write_text(f"q1\t{'a' * QUERY_EXAMPLE_MAX_CHARS}\n", encoding="utf-8")

    example = _first_query_example(str(tmp_path), "queries_exact.tsv")

    assert example == "a" * QUERY_EXAMPLE_MAX_CHARS


# ---------------------------------------------------------------------------
# Waiting for the vector-store node to actually serve
# ---------------------------------------------------------------------------


class _FakeTester:
    """A minimal stand-in exposing just what the polling methods use, called via the unbound
    'SearchPerformanceTest' methods below -- not a subclass, so pytest's unittest collector (which
    matches any 'unittest.TestCase' subclass regardless of name) does not pick it up as a test.
    """

    def __init__(self, ready):
        self.log = logging.getLogger("test")
        self._ready = ready
        self.asked = {}

    def _vector_store_api_client(self):
        tester = self

        class _Client:
            @staticmethod
            def wait_for_ready(**kwargs):
                tester.asked = kwargs
                return tester._ready

        return _Client()


def test_wait_for_vector_store_serving_requires_serving_only():
    tester = _FakeTester(ready=True)
    search_perf_test.SearchPerformanceTest._wait_for_vector_store_serving(tester, timeout=10)
    assert tester.asked["required_statuses"] == ("SERVING",)


def test_wait_for_vector_store_serving_raises_when_it_never_serves():
    tester = _FakeTester(ready=False)
    with pytest.raises(RuntimeError, match="did not reach SERVING"):
        search_perf_test.SearchPerformanceTest._wait_for_vector_store_serving(tester, timeout=10)


# ---------------------------------------------------------------------------
# Resolving the plan param to a local file
# ---------------------------------------------------------------------------


def test_a_relative_path_resolves_from_the_sct_root():
    """The plan is named the way every other file a test case points at is -- repo-relative -- so
    a plan outside the workload's own data directory needs no new param."""
    resolved = resolve_test_config_path("data_dir/latte/search_bench/local_config.yaml")
    assert resolved.endswith(os.path.join("data_dir", "latte", "search_bench", "local_config.yaml"))
    assert os.path.isabs(resolved)


def test_absolute_path_is_used_as_is(tmp_path):
    plan = tmp_path / "my_plan.yaml"
    plan.write_text("datasets: []\n", encoding="utf-8")
    assert resolve_test_config_path(str(plan)) == str(plan)


# ---------------------------------------------------------------------------
# Shard specs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec, expected",
    (
        ([], []),
        ([0], [0]),
        ([0, 1, 2], [0, 1, 2]),
        (["0..2"], [0, 1, 2]),
        (["0..1", 5, "8..9"], [0, 1, 5, 8, 9]),
        (["3..3"], [3]),
    ),
)
def test_shard_spec_normalizes_ints_and_ranges(spec, expected):
    assert _parse_shard_spec(spec) == expected


@pytest.mark.parametrize("spec", (["0-2"], ["0..a"], ["..2"], ["3..1"], [1.5], [None], [{"from": 0}], [True]))
def test_shard_spec_rejects_unrecognised_entries(spec):
    """Dropping these silently would load a smaller corpus than the plan asked for, and the run
    would then report plausible numbers against the wrong record count."""
    with pytest.raises(ValueError):
        _parse_shard_spec(spec)


@pytest.mark.parametrize(
    "spec",
    ([1, 1], ["0..2", 2], ["0..2", "2..4"], [3, "0..5"]),
    ids=["repeated-int", "int-inside-a-range", "overlapping-ranges", "int-covered-by-a-later-range"],
)
def test_shard_spec_rejects_duplicates(spec):
    """Loading a shard twice inflates the record count, and the throughput derived from it, while
    the table itself gains nothing -- the same silent misreport as an under-load, in the other
    direction."""
    with pytest.raises(ValueError, match="appear more than once"):
        _parse_shard_spec(spec)


# ---------------------------------------------------------------------------
# Validation of plan supplied names
#
# The plan's names reach a CQL index name, a shell command and a local path. Validate them on the way in rather than quoting for three contexts at once.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ("10M_20tok", "local_tiny", "term_common", "natural"))
def test_plain_names_are_accepted(name):
    assert _checked_name(name, "dataset name") == name


@pytest.mark.parametrize(
    "name",
    (
        'evil"; rm -rf /; echo "',
        "with space",
        "with-dash",
        "with.dot",
        "../escape",
        "",
        None,
    ),
)
def test_unsafe_names_are_rejected(name):
    with pytest.raises(ValueError, match="Invalid dataset name"):
        _checked_name(name, "dataset name")


@pytest.mark.parametrize("name", ("documents.tsv", "shards/documents_000.tsv", "a-b_c.1.tsv"))
def test_plain_data_file_names_are_accepted(name):
    assert _checked_data_file(name, "records file") == name


@pytest.mark.parametrize(
    "name",
    (
        "../../../etc/passwd",
        "/etc/passwd",
        "shards/../../escape.tsv",
        "$(id).tsv",
        "with space.tsv",
        "",
    ),
)
def test_unsafe_data_file_names_are_rejected(name):
    with pytest.raises(ValueError, match="Invalid records file"):
        _checked_data_file(name, "records file")


# ---------------------------------------------------------------------------
# Phase timeouts
#
# Without an explicit duration 'run_latte_thread' falls back to the whole 'test_duration', so the
# phases carrying no '--duration' pass one of their own.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "seconds, expected",
    ((0, 1), (1, 1), (60, 1), (61, 2), (600, 10), (1800, 30), (3600, 60)),
)
def test_timeout_minutes_rounds_up_and_never_returns_zero(seconds, expected):
    assert _timeout_minutes(seconds) == expected


# ---------------------------------------------------------------------------
# Every phase runs on a single loader
#
# 'run_latte_thread' fans a thread out to every loader unless it is asked not to, and each of these
# phases is one command -- so a multi-loader cluster would run all of them once per loader.
# ---------------------------------------------------------------------------


class _RecordingTester:
    """Records what '_run_latte' asks 'run_latte_thread' for."""

    def __init__(self):
        self.asked = {}

    def run_latte_thread(self, **kwargs):
        self.asked = kwargs
        return "thread"

    def verify_stress_thread(self, thread):
        assert thread == "thread"


def test_every_phase_runs_on_one_loader():
    tester = _RecordingTester()
    search_perf_test.SearchPerformanceTest._run_latte(tester, "latte schema bench.rn", duration=1)
    assert tester.asked["round_robin"] is True


def test_a_missing_shard_is_named_in_the_error(tmp_path):
    """Rather than a bare 'No such file' from counting its lines."""

    class _Tester:
        WORKLOAD = WORKLOAD
        log = logging.getLogger("test")

    with pytest.raises(FileNotFoundError, match=r"Shard shards/records_000\.tsv is not in the dataset directory"):
        search_perf_test.SearchPerformanceTest._load_step_shards(
            _Tester, {"shards": [0]}, str(tmp_path), "/tmp/bench/ds", 60
        )


class _PhaseTester:
    """Records the kwargs each phase hands '_run_latte'."""

    WORKLOAD = WORKLOAD

    def __init__(self):
        self.log = logging.getLogger("test")
        self.asked = []

    def _run_latte(self, stress_cmd, **kwargs):
        self.asked.append({"stress_cmd": stress_cmd, **kwargs})

    def _vector_store_node(self):
        return types.SimpleNamespace(system_log="system.log")

    def _load_shard(self, *args):
        return search_perf_test.SearchPerformanceTest._load_shard(self, *args)


def test_setup_phases_record_no_hdr(monkeypatch):
    monkeypatch.setattr(search_perf_test, "DbEventsFilter", lambda **_: contextlib.nullcontext())
    monkeypatch.setattr(search_perf_test, "wait_for_index_build_seconds", lambda *_: 1.0)
    tester = _PhaseTester()

    search_perf_test.SearchPerformanceTest._load_shard(tester, "/local", "/remote", "records.tsv", 10, 60)
    search_perf_test.SearchPerformanceTest._build_index(tester, 10, 60, "bench_idx_1", "bench")

    assert [asked["record_hdr"] for asked in tester.asked] == [False, False]


def _load(tester, local_ds_dir, *shard_ids):
    """Load one step of *shard_ids*, written into *local_ds_dir* first."""
    shards = local_ds_dir / "shards"
    shards.mkdir(exist_ok=True)
    for shard in shard_ids:
        (shards / f"records_{shard:03d}.tsv").write_text(f"doc_{shard}\tbody\n", encoding="utf-8")
    step = {"shards": list(shard_ids)}
    return search_perf_test.SearchPerformanceTest._load_step_shards(
        tester, step, str(local_ds_dir), "/tmp/bench/ds", 60
    )


def test_each_shard_is_staged_for_the_load_that_reads_it(tmp_path):
    tester = _PhaseTester()

    assert _load(tester, tmp_path, 0, 1) == 2

    assert [asked["files_to_stage"] for asked in tester.asked] == [
        [(f"{tmp_path}/shards/records_000.tsv", "/tmp/bench/ds/shards/records_000.tsv")],
        [(f"{tmp_path}/shards/records_001.tsv", "/tmp/bench/ds/shards/records_001.tsv")],
    ]


def test_a_query_set_stages_its_queries_and_qrels(monkeypatch):
    monkeypatch.setattr(search_perf_test, "DbEventsFilter", lambda **_: contextlib.nullcontext())
    tester = _PhaseTester()
    tester.get_hdrhistogram = lambda **_: {}

    search_perf_test.SearchPerformanceTest._run_search(
        tester,
        "/local/ds",
        "/tmp/bench/ds",
        "queries/queries.tsv",
        limit=10,
        concurrency=1,
        rate=None,
        search_duration="10s",
        qrels_file="queries/qrels.tsv",
    )

    (asked,) = tester.asked
    assert asked["files_to_stage"] == [
        ("/local/ds/queries/queries.tsv", "/tmp/bench/ds/queries/queries.tsv"),
        ("/local/ds/queries/qrels.tsv", "/tmp/bench/ds/queries/qrels.tsv"),
    ]


def _search_command():
    """Run one search; return the command it asked latte to run."""
    tester = _PhaseTester()
    tester.get_hdrhistogram = lambda **_: {}
    search_perf_test.SearchPerformanceTest._run_search(
        tester,
        "/local/ds",
        "/tmp/bench/ds",
        "queries_natural.tsv",
        limit=5,
        concurrency=1,
        rate=0,
        search_duration="10s",
    )
    (asked,) = tester.asked
    return asked["stress_cmd"]


def test_a_search_retries_as_little_as_latte_allows():
    """latte waits between retries inside the measured call, and a rejected query never succeeds.
    0 would not run the search function at all."""
    assert "--retry-number 1 " in _search_command()


def _filtered_log_lines(monkeypatch, ignore_parse_errors):
    """Run one search; return the database log lines it kept from failing the test."""
    filtered = []

    def events_filter(line, **_):
        filtered.append(line)
        return contextlib.nullcontext()

    monkeypatch.setattr(search_perf_test, "DbEventsFilter", events_filter)
    tester = _PhaseTester()
    tester.get_hdrhistogram = lambda **_: {}
    search_perf_test.SearchPerformanceTest._run_search(
        tester,
        "/local/ds",
        "/tmp/bench/ds",
        "queries_natural.tsv",
        limit=5,
        concurrency=1,
        rate=0,
        search_duration="10s",
        ignore_parse_errors=ignore_parse_errors,
    )
    return filtered


def test_a_query_parse_error_fails_the_test_by_default(monkeypatch):
    assert _filtered_log_lines(monkeypatch, ignore_parse_errors=False) == []


def test_a_query_parse_error_can_be_tolerated(monkeypatch):
    assert _filtered_log_lines(monkeypatch, ignore_parse_errors=True) == [r"failed to parse query"]


def test_a_search_is_a_read_to_latte():
    """Otherwise the HDR exporter cannot classify its 'fn--search' tag and fails the command."""
    assert get_latte_operation_type(_search_command()) == "read"


# ---------------------------------------------------------------------------
# Plan validation
# ---------------------------------------------------------------------------


class _PlanTester:
    """Enough of the flow to load a plan: the datasets it would run are recorded, not run."""

    WORKLOAD = WORKLOAD

    def __init__(self, plan_path, use_hdrhistogram=True):
        self.log = logging.getLogger("test")
        self.params = {search_perf_test.TEST_CONFIG_PARAM: str(plan_path), "use_hdrhistogram": use_hdrhistogram}
        self.ran = []

    def _wait_for_vector_store_serving(self):
        pass

    def _run_dataset(self, dataset):
        self.ran.append(dataset["name"])


def _run_plan(tmp_path, plan, **kwargs):
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(plan, encoding="utf-8")
    tester = _PlanTester(plan_path, **kwargs)
    search_perf_test.SearchPerformanceTest.run_search_benchmark(tester)
    return tester


def test_a_plan_runs_its_datasets_in_order(tmp_path):
    tester = _run_plan(tmp_path, "datasets:\n  - name: first\n  - name: second\n")
    assert tester.ran == ["first", "second"]


QUERYING_PLAN = """\
datasets:
  - name: first
    defaults: {expected_p99_read_ms: 10}
    steps:
      - queries: [{set: natural}]
"""


def test_a_plan_with_queries_needs_hdr_histograms(tmp_path):
    """Without them every query would be recorded with null latencies, after the corpus was loaded."""
    with pytest.raises(ValueError, match="use_hdrhistogram"):
        _run_plan(tmp_path, QUERYING_PLAN, use_hdrhistogram=False)


def test_has_invalid_queries_must_be_a_boolean(tmp_path):
    """A quoted "false" would read as true, and quietly let unparsable queries through."""
    with pytest.raises(ValueError, match="has_invalid_queries"):
        _run_plan(tmp_path, "datasets:\n  - name: first\n    has_invalid_queries: 'false'\n")


def test_a_plan_without_queries_runs_without_hdr_histograms(tmp_path):
    tester = _run_plan(tmp_path, "datasets:\n  - name: first\n", use_hdrhistogram=False)
    assert tester.ran == ["first"]


def test_repeated_dataset_names_are_rejected(tmp_path):
    """An index is named after its dataset and step, and its build time is read from the first
    matching 'full scan' pair in the log -- so a repeated name would silently re-report the first
    dataset's build times against the second one's record counts."""
    with pytest.raises(ValueError, match="Duplicate dataset names.*same"):
        _run_plan(tmp_path, "datasets:\n  - name: same\n  - name: other\n  - name: same\n")


@pytest.mark.parametrize(
    "plan, expected",
    (
        ("datasets: []\n", "no datasets to run"),
        ("{}\n", "no datasets to run"),
        ("# only a comment\n", "not a YAML mapping"),
        ("- name: first\n", "not a YAML mapping"),
    ),
)
def test_a_plan_that_would_run_nothing_is_rejected(tmp_path, plan, expected):
    """Such a run loads nothing, reports nothing and still finishes green, which reads as a passing
    benchmark rather than as the misconfiguration it is."""
    with pytest.raises(ValueError, match=expected):
        _run_plan(tmp_path, plan)


def test_an_unset_plan_is_rejected(tmp_path):
    tester = _PlanTester(tmp_path / "plan.yaml")
    tester.params = {search_perf_test.TEST_CONFIG_PARAM: ""}
    with pytest.raises(ValueError, match="is not set"):
        search_perf_test.SearchPerformanceTest.run_search_benchmark(tester)


def test_a_missing_plan_file_is_rejected(tmp_path):
    tester = _PlanTester(tmp_path / "absent.yaml")
    with pytest.raises(FileNotFoundError, match="not found at"):
        search_perf_test.SearchPerformanceTest.run_search_benchmark(tester)


def test_a_dataset_with_no_local_corpus_says_which_directory_is_missing():
    """The corpora are generated, not tracked, so a fresh clone that skipped the generator has to
    hear which directory to produce rather than an open() failure on a shard inside it."""
    tester = _PlanTester("unused")
    with pytest.raises(FileNotFoundError, match="no local directory"):
        search_perf_test.SearchPerformanceTest._run_dataset(
            tester, {"name": "never_generated", "steps": [{"shards": [0]}]}
        )


def test_a_dataset_with_no_steps_is_rejected():
    """It would drop and recreate the table, build nothing and report nothing, so the run would
    pass while measuring an index it never built."""
    tester = _PlanTester("unused")
    with pytest.raises(ValueError, match="no steps to run"):
        search_perf_test.SearchPerformanceTest._run_dataset(tester, {"name": "local_tiny", "steps": []})


# ---------------------------------------------------------------------------
# The results file
# ---------------------------------------------------------------------------


class _ResultsTester:
    """Enough of the flow to record results into *logdir*."""

    WORKLOAD = WORKLOAD

    def __init__(self, logdir):
        self.log = logging.getLogger("test")
        self.logdir = str(logdir)

    def _record_result(self, phase, **fields):
        search_perf_test.SearchPerformanceTest._record_result(self, phase, **fields)


def _results(logdir):
    with open(os.path.join(logdir, search_perf_test.RESULTS_FILE_NAME), encoding="utf-8") as results:
        return [json.loads(line) for line in results]


def test_index_builds_are_appended_one_json_object_per_line(tmp_path):
    tester = _ResultsTester(tmp_path)
    record_index_build = search_perf_test.SearchPerformanceTest._record_index_build

    record_index_build(tester, "local_tiny", 1, "bench_idx_local_tiny_0", 1000, 2.5)
    record_index_build(tester, "local_tiny", 2, "bench_idx_local_tiny_1", 3000, 5.0)

    records = _results(tmp_path)
    assert [{k: v for k, v in record.items() if k != "time"} for record in records] == [
        {
            "workload": "search_bench",
            "phase": "index_build",
            "dataset": "local_tiny",
            "step": 1,
            "index": "bench_idx_local_tiny_0",
            "record_count": 1000,
            "build_time_s": 2.5,
            "indexing_throughput": 400.0,
        },
        {
            "workload": "search_bench",
            "phase": "index_build",
            "dataset": "local_tiny",
            "step": 2,
            "index": "bench_idx_local_tiny_1",
            "record_count": 3000,
            "build_time_s": 5.0,
            "indexing_throughput": 600.0,
        },
    ]
    assert all(record["time"] for record in records)


def test_an_unmeasured_build_is_recorded_with_nulls(tmp_path):
    """A missing measurement has to stay visible, not drop out of the results."""
    search_perf_test.SearchPerformanceTest._record_index_build(
        _ResultsTester(tmp_path), "local_tiny", 1, "bench_idx_local_tiny_0", 1000, None
    )

    (record,) = _results(tmp_path)
    assert record["build_time_s"] is None
    assert record["indexing_throughput"] is None


@pytest.mark.parametrize("record_count, build_seconds", ((1000, None), (1000, 0.0), (0, 2.5)))
def test_no_throughput_is_derived_without_a_build_time_or_records(record_count, build_seconds):
    assert indexing_throughput(record_count, build_seconds) is None


def test_a_search_is_recorded_with_its_configuration_and_latency(tmp_path):
    tester = _ResultsTester(tmp_path)
    search_perf_test.SearchPerformanceTest._record_search(
        tester,
        expected_p99_read_ms=10.0,
        latency={"percentile_50": 2.08, "percentile_99": 3.86, "percentile_99_9": 5.0, "throughput": 50},
        dataset="local_tiny",
        step=1,
        query_index=2,
        query_set="term_common",
        limit=5,
        concurrency=1,
        rate=50,
    )

    (record,) = _results(tmp_path)
    assert {k: v for k, v in record.items() if k != "time"} == {
        "workload": "search_bench",
        "phase": "search",
        "dataset": "local_tiny",
        "step": 1,
        "query_index": 2,
        "query_set": "term_common",
        "limit": 5,
        "concurrency": 1,
        "rate": 50,
        "expected_p99_read_ms": 10.0,
        "p50_ms": 2.08,
        "p90_ms": None,
        "p95_ms": None,
        "p99_ms": 3.86,
        "p99_9_ms": 5.0,
        "p99_99_ms": None,
        "throughput_ops": 50,
        "p99_within_expectation": True,
    }


@pytest.mark.parametrize("latency, within", (({"percentile_99": 12.0}, False), ({}, None)))
def test_a_search_says_whether_it_met_its_expectation(tmp_path, latency, within):
    """Unknown when nothing was measured -- not a pass, and not a miss either."""
    search_perf_test.SearchPerformanceTest._record_search(
        _ResultsTester(tmp_path), expected_p99_read_ms=10.0, latency=latency, query_set="natural"
    )
    (record,) = _results(tmp_path)
    assert record["p99_within_expectation"] is within


class _StepTester(_ResultsTester):
    """Runs a step's queries with '_run_search' answering from a canned HDR summary."""

    def __init__(self, logdir):
        super().__init__(logdir)
        self.searched = []

    def _run_search(self, local_ds_dir, container_ds_dir, **kwargs):
        self.searched.append(kwargs)
        return {"percentile_99": 4.0, "throughput": 50}

    def _record_search(self, **fields):
        search_perf_test.SearchPerformanceTest._record_search(self, **fields)


def test_every_query_of_a_step_is_recorded_in_order(tmp_path):
    """Two entries with the same configuration stay apart by their position in the step."""
    (tmp_path / "queries_term_common.tsv").write_text("q1\tthe query\n", encoding="utf-8")
    tester = _StepTester(tmp_path)
    step = {"queries": [{"set": "term_common"}, {"set": "term_common", "limit": 100}, {"set": "term_common"}]}

    search_perf_test.SearchPerformanceTest._run_step_queries(
        tester, step, "ds", DEFAULTS, record_count=900, step_number=2, local_ds_dir=str(tmp_path), container_ds_dir="/r"
    )

    assert [searched["limit"] for searched in tester.searched] == [5, 100, 5]
    records = _results(tmp_path)
    assert [(r["step"], r["query_index"], r["limit"], r["record_count"]) for r in records] == [
        (2, 1, 5, 900),
        (2, 2, 100, 900),
        (2, 3, 5, 900),
    ]
    assert {r["query_example"] for r in records} == {"the query"}
    assert {r["p99_ms"] for r in records} == {4.0}


class _DatasetTester:
    """Runs '_run_dataset' with every phase stubbed, recording how each step's queries are run."""

    WORKLOAD = WORKLOAD

    def __init__(self):
        self.log = logging.getLogger("test")
        self.params = {}
        self.ignore_parse_errors = []

    def _drop_table(self):
        pass

    def _create_schema(self):
        pass

    def _drop_index(self, *_):
        pass

    def _load_step_shards(self, *_):
        return 0

    def _build_index(self, *_, **__):
        return 1.0

    def _record_index_build(self, *_):
        pass

    def _run_step_queries(self, *_, ignore_parse_errors=False):
        self.ignore_parse_errors.append(ignore_parse_errors)


@pytest.mark.parametrize("has_invalid_queries", [False, True])
def test_a_dataset_says_how_its_queries_handle_parse_errors(tmp_path, monkeypatch, has_invalid_queries):
    monkeypatch.setattr(search_perf_test, "_local_path", lambda *_: str(tmp_path))
    tester = _DatasetTester()

    search_perf_test.SearchPerformanceTest._run_dataset(
        tester, {"name": "ds", "has_invalid_queries": has_invalid_queries, "steps": [{"shards": []}, {"shards": []}]}
    )

    assert tester.ignore_parse_errors == [has_invalid_queries] * 2


@pytest.mark.parametrize("ignore_parse_errors", [False, True])
def test_every_query_of_a_step_gets_the_same_parse_error_handling(tmp_path, ignore_parse_errors):
    (tmp_path / "queries_term_common.tsv").write_text("q1\tthe query\n", encoding="utf-8")
    tester = _StepTester(tmp_path)
    step = {"queries": [{"set": "term_common"}, {"set": "term_common"}]}

    search_perf_test.SearchPerformanceTest._run_step_queries(
        tester,
        step,
        "ds",
        DEFAULTS,
        record_count=900,
        step_number=1,
        local_ds_dir=str(tmp_path),
        container_ds_dir="/r",
        ignore_parse_errors=ignore_parse_errors,
    )

    assert [searched["ignore_parse_errors"] for searched in tester.searched] == [ignore_parse_errors] * 2
