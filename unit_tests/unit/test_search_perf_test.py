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

import logging
import os

import pytest

import search_perf_test
from search_perf_test import (
    LatteScriptParams,
    SearchWorkload,
    _checked_data_file,
    _checked_name,
    _parse_shard_spec,
    _timeout_minutes,
    resolve_test_config_path,
)

WORKLOAD = SearchWorkload(
    name="search_bench",
    base_dir="data_dir/latte/search_bench",
    script="data_dir/latte/search_bench/bench.rn",
    item_noun="docs",
    index_prefix="bench_idx",
    default_keyspace="bench",
    build_result_table=object,
    build_count_column="record_count",
    params=LatteScriptParams(
        dataset_dir="data_dir",
        records_file="records_file",
        record_count="record_count",
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

# ---------------------------------------------------------------------------
# Waiting for the vector-store node to actually serve
# ---------------------------------------------------------------------------


class _FakeTester:
    """A minimal stand-in exposing just what the polling methods use, called via the unbound
    'SearchPerformanceTest' methods below -- not a subclass, so pytest's unittest collector (which
    matches any 'unittest.TestCase' subclass regardless of name) does not pick it up as a test.
    """

    def __init__(self, ready):
        self.log = __import__("logging").getLogger("test")
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
# The plan may come from an arbitrary S3 URL, and its names reach a CQL index name, a shell command
# and a local path. Validate them on the way in rather than quoting for three contexts at once.
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


# ---------------------------------------------------------------------------
# Plan validation
# ---------------------------------------------------------------------------


class _PlanTester:
    """Enough of the flow to load a plan: the datasets it would run are recorded, not run."""

    WORKLOAD = WORKLOAD

    def __init__(self, plan_path):
        self.log = logging.getLogger("test")
        self.params = {search_perf_test.TEST_CONFIG_PARAM: str(plan_path)}
        self.ran = []

    def _wait_for_vector_store_serving(self):
        pass

    def _run_dataset(self, dataset):
        self.ran.append(dataset["name"])


def _run_plan(tmp_path, plan):
    plan_path = tmp_path / "plan.yaml"
    plan_path.write_text(plan, encoding="utf-8")
    tester = _PlanTester(plan_path)
    search_perf_test.SearchPerformanceTest.run_search_benchmark(tester)
    return tester


def test_a_plan_runs_its_datasets_in_order(tmp_path):
    tester = _run_plan(tmp_path, "datasets:\n  - name: first\n  - name: second\n")
    assert tester.ran == ["first", "second"]


def test_repeated_dataset_names_are_rejected(tmp_path):
    """An index is named after its dataset and step, and its build time is read from the first
    matching 'full scan' pair in the log -- so a repeated name would silently re-report the first
    dataset's build times against the second one's document counts."""
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
