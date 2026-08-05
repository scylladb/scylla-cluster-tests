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

"""Workload-agnostic search performance flow, shared by the search benchmarks.

The flow is the same whichever index is under test -- full-text (BM25 over documents) or vector
(ANN over embeddings) -- because vector-store serves both and latte drives both:

    a YAML plan names datasets; a dataset is a sequence of steps; a step loads more shards on top
    of the previous ones, rebuilds the index and runs its query sets, so one dataset yields results
    at several corpus sizes. Index build time comes from vector-store's own log (see
    sdcm.utils.vector_store_index), query latency from latte's HDR output, and every measurement is
    appended to RESULTS_FILE_NAME in the run's log directory as it is taken, one JSON object per line.

What differs per workload is the rune script, the vocabulary and the names things are reported
under. All of it is declared in a 'SearchWorkload', which a subclass points 'WORKLOAD' at; the
subclass then only owns its 'test_*' entry point. See fts_test.py for a worked
example, and docs/fts-search-test.md for the plan format.
"""

import json
import math
import os
import posixpath
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath

import yaml  # type: ignore

from performance_regression_test import PerformanceRegressionTest
from sdcm import sct_abs_path
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.utils.vector_store_client import VectorStoreClient
from sdcm.utils.vector_store_index import wait_for_index_build_seconds

# Collected with the runner's logs, see 'BaseSCTLogCollector'.
RESULTS_FILE_NAME = "search_results.jsonl"

# The SCT param naming the plan to run. One option for every search workload rather than one per
# test: the plan format is the flow's, not any single workload's, so a vector-search test reuses it.
TEST_CONFIG_PARAM = "search_test_config"

DEFAULT_MAX_INDEX_WAIT = 1800
DEFAULT_RATE = 0
DEFAULT_DURATION = "60s"
DEFAULT_LIMIT = 5
DEFAULT_CONCURRENCY = 2

# How often SCT re-checks the vector-store state it is waiting on over the API: the node reporting
# SERVING, and a dropped index disappearing. Neither is on a measured path -- the build time comes
# from log timestamps, not from when a poll happened to notice -- so this only bounds how long the
# test lingers past the event, and a shorter interval would only add requests.
VECTOR_STORE_STATUS_POLL_INTERVAL_SECS = 1.0
# How long a run waits for the vector-store node to report SERVING before the first index build.
# Cluster init already waits for it (see 'VectorStoreClusterMixin.wait_for_init'), so this is a
# second, narrower gate against the specific "actually serving, not just bootstrapping" state index
# timing needs, not a full readiness wait.
DEFAULT_VECTOR_STORE_SERVING_WAIT = 300

# Timeouts for the latte commands that carry no '--duration', in seconds. They are needed because
# without one 'run_latte_thread' falls back to the whole 'test_duration', which turns any
# hung phase into a run that sits on its cluster until the Jenkins job times out.
DEFAULT_SCHEMA_TIMEOUT = 600
# Per shard, not per step -- shards are loaded one latte invocation at a time.
DEFAULT_MAX_SHARD_LOAD = 3600
# Added to the index build phase's own wait before it becomes SCT's outer timeout, so the script's
# give-up path always wins the tie -- see '_build_index'.
INDEX_BUILD_TIMEOUT_GRACE_SECS = 120

# Names taken from the plan which end up in a CQL identifier, in a shell command or in a file path.
# Validated once on the way in instead of being quoted differently at each of those three places.
SAFE_NAME_RE = re.compile(r"[A-Za-z0-9_]+")
SAFE_DATA_FILE_RE = re.compile(r"[A-Za-z0-9_.\-/]+")
# A latte '--duration': digits and one of its unit suffixes, the form 'get_timeout_from_stress_cmd'
# parses a phase timeout out of.
SAFE_DURATION_RE = re.compile(r"\d+[hms]")

# What a query record keeps of the HDR summary 'get_hdrhistogram' builds, under the record's names.
HDR_SUMMARY_FIELDS = {
    "percentile_50": "p50_ms",
    "percentile_90": "p90_ms",
    "percentile_95": "p95_ms",
    "percentile_99": "p99_ms",
    "percentile_99_9": "p99_9_ms",
    "percentile_99_99": "p99_99_ms",
    "throughput": "throughput_ops",
}

# Cap on the 'query_example' field of a query record. The value comes from a corpus the plan names
# rather than from anything validated here.
QUERY_EXAMPLE_MAX_CHARS = 256


@dataclass(frozen=True)
class LatteScriptParams:
    """The '-P' parameter names of a workload's rune script.

    Each rune script keeps its own vocabulary -- the full-text one talks about documents -- and the
    scripts are mirrored from scylladb/vector-store rather than owned here, so the flow maps its
    neutral notion of a record onto whatever the script calls it. Declared per workload instead of
    fixed, so adding a workload never means renaming parameters in a script that lives elsewhere.

    Every field is a name the script must accept; nothing else belongs here, so a test can check the
    whole descriptor against the script it names.
    """

    dataset_dir: str
    records_file: str
    record_count: str
    queries_file: str
    qrels_file: str
    search_limit: str
    compute_accuracy: str
    index_name: str
    max_index_wait: str
    min_probes: str
    schema_cleanup: str
    drop_index: str


@dataclass(frozen=True)
class SearchWorkload:
    """Everything that makes a search benchmark specific to one kind of index."""

    name: str  # 'fts_search' -- names the workload in the results file
    base_dir: str  # holds the rune script, the tracked plans and the local datasets
    script: str  # the rune script latte runs
    hdr_tag: str  # HDR tag its search function emits, e.g. 'fn--search'
    item_noun: str  # 'docs' -- the unit record counts are logged in
    index_prefix: str  # 'fts_idx' -- prefixes every index this test builds
    default_keyspace: str
    container_root: str  # container directory datasets are staged under
    params: LatteScriptParams
    # Plan keys and defaults naming the data files of a step, in the same vocabulary as the script.
    step_records_file_key: str
    default_records_file: str
    default_shard_suffix: str


def _local_path(workload: SearchWorkload, *parts: str) -> str:
    """Return the absolute path to a file inside the workload's data directory."""
    return sct_abs_path(os.path.join(workload.base_dir, *parts))


def indexing_throughput(record_count: int, build_seconds: float | None) -> float | None:
    """Records indexed per second, or None where no rate can be derived."""
    if not build_seconds or build_seconds <= 0 or record_count <= 0:
        return None
    return round(record_count / build_seconds, 1)


def _timeout_minutes(seconds: int) -> int:
    """Convert a phase budget in seconds to the whole minutes 'run_latte_thread' takes."""
    return max(1, math.ceil(seconds / 60))


def _checked_name(name: str, kind: str) -> str:
    """Validate a plan-supplied name used as a CQL identifier and as a path component."""
    if not isinstance(name, str) or not SAFE_NAME_RE.fullmatch(name):
        raise ValueError(f"Invalid {kind} {name!r}: expected only letters, digits and underscores")
    return name


def _checked_data_file(name: str, kind: str) -> str:
    """Validate a plan-supplied, dataset-relative data file name.

    The name is interpolated into the shell commands that copy the file into the loader container,
    and joined onto the local dataset directory, so reject both shell metacharacters and anything
    that escapes the dataset directory.
    """
    if not isinstance(name, str) or not SAFE_DATA_FILE_RE.fullmatch(name):
        raise ValueError(f"Invalid {kind} {name!r}: expected only letters, digits, '.', '-', '_' and '/'")
    parts = PurePosixPath(name).parts
    if name.startswith("/") or ".." in parts:
        raise ValueError(f"Invalid {kind} {name!r}: must be a relative path inside the dataset directory")
    return name


def _checked_int(value, kind: str, minimum: int = 0) -> int:
    """Validate a plan-supplied number that is interpolated into a latte command line."""
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"Invalid {kind} {value!r}: expected an integer >= {minimum}")
    return value


def _checked_positive_float(value, kind: str) -> float:
    """Validate a plan-supplied latency expectation: a finite number > 0, and not a bool."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"Invalid {kind} {value!r}: expected a finite number > 0")
    return float(value)


def _checked_duration(value, kind: str) -> str:
    """Validate a plan-supplied latte duration in the time form ('60s', '5m', '1h'): the only one
    'get_timeout_from_stress_cmd' parses, and the phase timeout otherwise becomes 'test_duration'."""
    if not isinstance(value, str) or not SAFE_DURATION_RE.fullmatch(value):
        raise ValueError(f"Invalid {kind} {value!r}: expected a latte duration like '60s', '5m' or '1h'")
    return value


def _count_tsv_lines(path: str) -> int:
    """Count the records in a shard: its non-empty lines.

    The format has no header -- every record is one '<id>\t<body>' line, and the rune script
    rejects a non-empty line without a tab -- so this is also the number of rows the load inserts.
    """
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _first_query_example(local_ds_dir: str, queries_file: str) -> str:
    """Return the text of the first query in a 'queries_<set>.tsv' file, or "" if there is none.

    Truncated, since natural-language queries can run long.
    """
    path = os.path.join(local_ds_dir, queries_file)
    try:
        with open(path, encoding="utf-8") as f:
            for raw_line in f:
                stripped = raw_line.strip()
                if not stripped:
                    continue
                parts = stripped.split("\t", 1)
                text = parts[1] if len(parts) > 1 else parts[0]
                if len(text) <= QUERY_EXAMPLE_MAX_CHARS:
                    return text
                return text[: QUERY_EXAMPLE_MAX_CHARS - 3] + "..."
    except FileNotFoundError:
        pass
    return ""


def _parse_shard_spec(shards: list) -> list[int]:
    """Normalize shard spec into a flat list of ints.

    Accepts ints and 'start..end' range strings, e.g. [0..9, 10, 11..99]. Anything else raises:
    silently dropping it would load a smaller corpus than the plan asked for, and the run would
    report perfectly plausible numbers for the wrong record count.
    """
    result: list[int] = []
    for item in shards:
        # NOTE: bool before int -- 'isinstance(True, int)' holds, and YAML turns an unquoted
        #       'yes'/'no'/'on'/'off' into a bool, so those would otherwise pass as shard 0/1.
        if isinstance(item, bool) or not isinstance(item, (int, str)):
            raise ValueError(f"Invalid shard spec entry {item!r}: expected an int or a 'start..end' string")
        if isinstance(item, int):
            result.append(item)
        else:
            m = re.fullmatch(r"(\d+)\.\.(\d+)", item)
            if not m:
                raise ValueError(f"Invalid shard range: {item!r}")
            start, end = int(m.group(1)), int(m.group(2))
            # A descending range is the same silent under-load as a dropped entry: it expands to
            # nothing, and the run reports plausible numbers for a corpus it never loaded.
            if end < start:
                raise ValueError(f"Invalid shard range: {item!r} ends before it starts")
            result.extend(range(start, end + 1))
    # A repeated shard is the mirror image of a dropped one, and just as quiet: loading it twice
    # upserts the same record ids, so the table gains nothing while 'record_count' counts both
    # invocations -- inflating the reported corpus size and the indexing throughput derived from it.
    if len(result) != len(set(result)):
        duplicates = sorted({shard for shard in result if result.count(shard) > 1})
        raise ValueError(f"Invalid shard spec {shards!r}: shard(s) {duplicates} appear more than once")
    return result


def resolve_test_config_path(config: str) -> str:
    """Resolve the plan param to a local file.

    Two accepted forms, so that pointing a run at a different plan is a single overridable
    param (SCT_SEARCH_TEST_CONFIG):

      /a/local/path                        used as-is
      data_dir/latte/fts_search/plan.yaml  relative to the SCT root, like every other file a
                                           test case names -- see 'scylla_d_overrides_files' and
                                           the '.rn' paths inside latte stress commands
    """
    if os.path.isabs(config):
        return config
    return sct_abs_path(config)


# ---------------------------------------------------------------------------
# Query discovery
# ---------------------------------------------------------------------------


def _query_params(query: dict, defaults: dict) -> tuple:
    """Resolve (limit, concurrency, rate) for a query entry, applying dataset defaults.

    Checked, not just read: all three are interpolated into the latte command line, the same reason
    '_checked_name' exists for the plan's names.
    """
    return (
        _checked_int(query.get("limit", defaults.get("limit", DEFAULT_LIMIT)), "query limit", minimum=1),
        _checked_int(
            query.get("concurrency", defaults.get("concurrency", DEFAULT_CONCURRENCY)), "query concurrency", minimum=1
        ),
        # 0 is 'unthrottled' -- '_run_search' drops '--rate' entirely for it.
        _checked_int(query.get("rate", defaults.get("rate", DEFAULT_RATE)), "query rate"),
    )


def _query_duration(query: dict, defaults: dict) -> str:
    """Resolve the latte '--duration' for a query entry, applying dataset defaults."""
    return _checked_duration(query.get("duration", defaults.get("duration", DEFAULT_DURATION)), "query duration")


def _expected_p99_read_ms(query: dict, defaults: dict) -> float:
    """Resolve the expected P99 read latency (ms) for a query entry.

    Required, on the entry or in the dataset's 'defaults': the expectation belongs to the plan, so
    SCT has no default for it.
    """
    expected = query.get("expected_p99_read_ms", defaults.get("expected_p99_read_ms"))
    if expected is None:
        raise ValueError(
            f"Query set {query.get('set')!r} has no 'expected_p99_read_ms' "
            f"(set it on the query entry or the dataset's 'defaults')"
        )
    return _checked_positive_float(expected, f"expected_p99_read_ms for query set {query.get('set')!r}")


def validate_plan_queries(datasets: list) -> None:
    """Check every query entry of every dataset, so a bad plan fails before anything runs.

    At query time the step has already loaded and indexed its corpus: tens of minutes on a real one.
    """
    for dataset in datasets:
        defaults = dataset.get("defaults", {})
        for step_idx, step in enumerate(dataset.get("steps", [])):
            for query in step.get("queries", []):
                where = f"dataset {dataset.get('name')!r}, step #{step_idx + 1}"
                if "set" not in query:
                    raise ValueError(f"Query entry in {where} has no 'set'")
                try:
                    _checked_name(query["set"], "query set name")
                    _query_params(query, defaults)
                    _query_duration(query, defaults)
                    _expected_p99_read_ms(query, defaults)
                except ValueError as exc:
                    raise ValueError(f"{exc} (in {where})") from exc


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class SearchPerformanceTest(PerformanceRegressionTest):
    """Multi-dataset, multi-step search benchmark driven by a YAML plan.

    Subclasses declare their index in ``WORKLOAD`` and expose a ``test_*`` method calling
    ``run_search_benchmark``; everything below is the same for every search workload.
    """

    WORKLOAD: SearchWorkload = None

    def _run_latte(self, stress_cmd, files_to_stage=None, **kwargs):
        """Run one latte command to completion and return its stress thread.

        Bypasses `run_stress_thread` so that the per-run data files of this command are staged into
        the loader container. `files_to_stage` is a list of `(local_path, container_path)` pairs.
        """
        thread = self.run_latte_thread(
            stress_cmd=stress_cmd,
            extra_files_to_stage=files_to_stage or [],
            # NOTE: every phase here is a single command -- one schema change, one shard, one index
            #       build -- so it belongs on one loader. Without this the thread fans out to *all*
            #       of them ('DockerBasedStressThread.configure_executer'), which on a multi-loader
            #       cluster would load every shard once per loader and report a record count, and
            #       the indexing throughput derived from it, off by that factor.
            round_robin=True,
            **kwargs,
        )
        self.verify_stress_thread(thread)
        return thread

    def _vector_store_node(self):
        """Return the vector-store node this test talks to.

        Raises rather than returning None: every search test case sets 'n_vector_store_nodes: 1', so
        a missing cluster is a misconfiguration, and failing here beats an AttributeError deep
        inside an index build.
        """
        vs_cluster = self.db_cluster.vector_store_cluster
        if not vs_cluster or not vs_cluster.nodes:
            raise RuntimeError("No vector-store node available; a search test requires 'n_vector_store_nodes' >= 1")
        return vs_cluster.nodes[0]

    def _vector_store_api_client(self) -> VectorStoreClient:
        return self._vector_store_node().get_vector_store_api_client()

    def _wait_for_vector_store_serving(self, timeout: float = DEFAULT_VECTOR_STORE_SERVING_WAIT):
        """Block until the vector-store node reports node-level status SERVING.

        Cluster init already waits for the node to be ready (see
        'VectorStoreClusterMixin.wait_for_init'), but that accepts BOOTSTRAPPING too. Index timing
        needs the node actually serving before it issues 'CREATE INDEX', so gate on that explicitly
        rather than relying on the broader cluster-readiness check.
        """
        vs_client = self._vector_store_api_client()
        if not vs_client.wait_for_ready(
            timeout=timeout,
            check_interval=VECTOR_STORE_STATUS_POLL_INTERVAL_SECS,
            required_statuses=("SERVING",),
        ):
            raise RuntimeError(f"Vector store did not reach SERVING within {timeout}s")
        self.log.info("Vector store is SERVING")

    def _create_schema(self):
        """Create keyspace and table for a dataset."""
        self.log.info("Creating schema")
        self._run_latte(f"latte schema {self.WORKLOAD.script}", duration=_timeout_minutes(DEFAULT_SCHEMA_TIMEOUT))

    def _load_shard(self, local_ds_dir, container_ds_dir, shard_file, shard_records, max_load_wait):
        """Load a single shard file into Scylla via the loader container."""
        params = self.WORKLOAD.params
        self.log.info("Loading shard %s (%d %s)", shard_file, shard_records, self.WORKLOAD.item_noun)
        self._run_latte(
            # NOTE: latte's '-d' is a cycle count here, not a duration, so the phase gets an
            #       explicit 'duration' -- see DEFAULT_MAX_SHARD_LOAD.
            stress_cmd=(
                f"latte run -f load "
                f"-d {shard_records} "
                rf"-P {params.dataset_dir}=\"{container_ds_dir}\" "
                rf"-P {params.records_file}=\"{shard_file}\" "
                f"{self.WORKLOAD.script} "
            ),
            files_to_stage=[
                (os.path.join(local_ds_dir, shard_file), posixpath.join(container_ds_dir, shard_file)),
            ],
            duration=_timeout_minutes(max_load_wait),
            # NOTE: nothing reads the latency of a setup phase, and its 'fn--load' tag is neither a
            #       read nor a write to the HDR exporter, which fails the command on such a tag.
            record_hdr=False,
        )

    def _load_step_shards(self, step, local_ds_dir, container_ds_dir, max_load_wait):
        """Load all shard files for a step. Returns the record count."""
        workload = self.WORKLOAD
        if "shards" in step:
            shard_ids = _parse_shard_spec(step["shards"])
            shard_suffix = step.get("shard_suffix", workload.default_shard_suffix)
            shard_files = ["shards/" + shard_suffix.format(sid) for sid in shard_ids]
        else:
            shard_files = [step.get(workload.step_records_file_key, workload.default_records_file)]
        shard_files = [_checked_data_file(shard_file, "records file") for shard_file in shard_files]

        record_count = 0
        for shard_file in shard_files:
            local_shard_path = os.path.join(local_ds_dir, shard_file)
            if not os.path.isfile(local_shard_path):
                raise FileNotFoundError(f"Shard {shard_file} is not in the dataset directory {local_ds_dir}")
            shard_records = _count_tsv_lines(local_shard_path)
            if shard_records == 0:
                self.log.warning("Shard file %s is empty, skipping", local_shard_path)
                continue

            self._load_shard(local_ds_dir, container_ds_dir, shard_file, shard_records, max_load_wait)
            record_count += shard_records
        return record_count

    def _build_index(self, record_count, max_index_wait, index_name, keyspace) -> float | None:
        """Build the index on the current table and return how long the build took, in seconds.

        latte still owns the DDL and decides when the index is usable (its 'build_index' probes the
        index until it answers), but the reported duration does not come from latte: its clock is
        whole seconds, and its probe loop is paced by retry backoff, so what it measures is "when a
        retry after readiness happened to fire", several seconds late. Instead SCT reads
        vector-store's own 'full scan' log lines afterwards -- see 'wait_for_index_build_seconds'.

        Returns None if those lines never turn up, which is reported as a missing measurement rather
        than failing the build: the index is queryable either way.
        """
        params = self.WORKLOAD.params
        self.log.info("Building index '%s' (%d %s)", index_name, record_count, self.WORKLOAD.item_noun)
        with DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line=r"vector_store_client.*(?:missing index|is not available yet)",
            extra_time_to_expiration=120,
        ):
            self._run_latte(
                stress_cmd=(
                    f"latte run -f build_index "
                    f"-d 1 "
                    f'-P {params.index_name}=\\"{index_name}\\" '
                    f"-P {params.record_count}={record_count} "
                    f"-P {params.max_index_wait}={max_index_wait} "
                    f"-P {params.min_probes}=3 "
                    f"{self.WORKLOAD.script} "
                ),
                # An outer bound past the script's own give-up, so a clean "index never built" is
                # not turned into a killed loader by both expiring in the same second.
                duration=_timeout_minutes(max_index_wait + INDEX_BUILD_TIMEOUT_GRACE_SECS),
                record_hdr=False,  # a setup phase, as in '_load_shard'
            )
        return wait_for_index_build_seconds(self._vector_store_node().system_log, keyspace, index_name)

    def _drop_index(self, index_name, keyspace, max_index_wait):
        """Drop the current index via schema, and wait until vector-store confirms it is gone."""
        params = self.WORKLOAD.params
        self.log.info("Dropping index '%s'", index_name)
        self._run_latte(
            f"latte schema {self.WORKLOAD.script} "
            f"-P {params.drop_index}=true "
            f'-P {params.index_name}=\\"{index_name}\\" ',
            duration=_timeout_minutes(DEFAULT_SCHEMA_TIMEOUT),
        )
        # NOTE: folded as vector-store knows them; unfolded, the query 404s and reads as "dropped".
        self._vector_store_api_client().wait_for_index_absent(
            keyspace.lower(),
            index_name.lower(),
            timeout=max_index_wait,
            check_interval=VECTOR_STORE_STATUS_POLL_INTERVAL_SECS,
        )

    def _drop_table(self):
        """Drop the current table via schema."""
        params = self.WORKLOAD.params
        self.log.info("Dropping table")
        self._run_latte(
            f"latte schema {self.WORKLOAD.script} -P {params.schema_cleanup}=true ",
            duration=_timeout_minutes(DEFAULT_SCHEMA_TIMEOUT),
        )

    def _run_search(
        self,
        local_ds_dir,
        container_ds_dir,
        queries_file,
        limit,
        concurrency,
        rate,
        search_duration,
        qrels_file=None,
    ) -> dict:
        """Run a single search configuration and return its latency summary.

        Passing `qrels_file` turns on the relevance (accuracy) metrics of the rune script. The
        summary is what 'get_hdrhistogram' builds from latte's HDR output for the search function's
        tag over the run's time window -- percentiles in ms, throughput in ops/s -- and is empty if
        no histogram was written for it.
        """
        workload = self.WORKLOAD
        params = workload.params
        files_to_stage = [
            (os.path.join(local_ds_dir, queries_file), os.path.join(container_ds_dir, queries_file)),
        ]
        qrels_param = ""
        if qrels_file:
            files_to_stage.append((os.path.join(local_ds_dir, qrels_file), os.path.join(container_ds_dir, qrels_file)))
            qrels_param = rf"-P {params.qrels_file}=\"{qrels_file}\" "
        rate_param = f"--rate={rate} " if rate else ""

        start = time.time()
        self._run_latte(
            # NOTE: '--duration' spelled out rather than '-d': 'get_timeout_from_stress_cmd'
            #       only recognises the long form, and without it the phase would fall back to
            #       the whole 'test_duration' as its timeout. '--retry-number 1' is the lowest
            #       that runs at all: latte counts it as attempts per cycle but as retries per
            #       query, so a rejected query is still retried once (QATOOLS-629).
            stress_cmd=(
                f"latte run -f search "
                f"--duration {search_duration} "
                rf"-P {params.dataset_dir}=\"{container_ds_dir}/\" "
                rf"-P {params.queries_file}=\"{queries_file}\" "
                f"{qrels_param}"
                f"-P {params.compute_accuracy}={'true' if qrels_file else 'false'} "
                f"-P {params.search_limit}={limit} "
                f"{rate_param}--concurrency={concurrency} --retry-number 1 "
                f"{workload.script} "
            ),
            files_to_stage=files_to_stage,
        )
        end = time.time()
        summary = self.get_hdrhistogram(
            hdr_tags=[workload.hdr_tag], stress_operation="read", start_time=start, end_time=end
        )
        # One entry, keyed '<operation>--<tag>', for the one tag asked for.
        return next(iter(summary.values()), {})

    def run_search_benchmark(self):
        """Run every dataset of the plan 'search_test_config' points at."""
        self._wait_for_vector_store_serving()

        # NOTE: no fallback plan. Which datasets, shards and query sets to run is the whole
        #       definition of the test, and it is test-case specific, so there is nothing sensible
        #       to default to -- say so instead of failing later on a missing file.
        config_name = self.params.get(TEST_CONFIG_PARAM)
        if not config_name:
            raise ValueError(f"'{TEST_CONFIG_PARAM}' is not set: the search test needs a plan to run")
        config_path = resolve_test_config_path(config_name)
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Test config '{config_name}' not found at {config_path}")

        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        self.log.info("Loaded search test config: %s", config_path)

        # An empty plan is a misconfiguration, not a zero-dataset run: it would load nothing, report
        # nothing and still finish green, which is the one outcome nobody reads twice.
        if not isinstance(config, dict):
            raise ValueError(f"Test config '{config_name}' is not a YAML mapping")
        datasets = config.get("datasets")
        if not datasets:
            raise ValueError(f"Test config '{config_name}' has no datasets to run")
        # NOTE: the names have to be distinct. An index is named after its dataset and step, and its
        #       build time is read from the first matching 'full scan' pair in the log, so two
        #       datasets called the same thing would re-report the first one's build times.
        names = [_checked_name(dataset["name"], "dataset name") for dataset in datasets]
        if duplicates := sorted({name for name in names if names.count(name) > 1}):
            raise ValueError(f"Duplicate dataset names in '{config_name}': {duplicates}")
        validate_plan_queries(datasets)
        # Query latency comes from latte's HDR output, which is only written with 'use_hdrhistogram'.
        if not self.params.get("use_hdrhistogram") and any(
            step.get("queries") for dataset in datasets for step in dataset.get("steps") or []
        ):
            raise ValueError(f"Test config '{config_name}' runs query sets, but 'use_hdrhistogram' is off")

        for dataset in datasets:
            self._run_dataset(dataset)

    def _run_dataset(self, dataset):
        """Load, index and query one dataset.

        Every dataset uses the keyspace/table named by 'latte_schema_parameters', dropped and
        recreated here so each one starts from an empty table.

        Each step adds more shards on top of the previous ones and rebuilds the index, so that one
        dataset yields index build and search results at several corpus sizes. An *empty* 'shards'
        list loads nothing and only rebuilds on the corpus already there, e.g. to sample build-time
        variance; an *absent* one falls back to the step's single-file key (an unsharded corpus --
        see 'local_smoke' in data_dir/latte/fts_search/local_config.yaml).
        """
        workload = self.WORKLOAD
        dataset_name = _checked_name(dataset["name"], "dataset name")
        # A dataset is its steps: without one there is nothing to build and nothing to report, so
        # say so here rather than dropping the table and finishing green.
        steps = dataset.get("steps")
        if not steps:
            raise ValueError(f"Dataset '{dataset_name}' has no steps to run")

        local_ds_dir = _local_path(workload, dataset_name)
        container_ds_dir = f"{workload.container_root}/{dataset_name}"
        # The corpora are generated rather than tracked, so name the directory that is missing
        # instead of failing further down on an unhelpful open() of a shard inside it.
        if not os.path.isdir(local_ds_dir):
            raise FileNotFoundError(f"Dataset '{dataset_name}' has no local directory at {local_ds_dir}")

        max_index_wait = dataset.get("max_index_wait_secs", DEFAULT_MAX_INDEX_WAIT)
        max_load_wait = dataset.get("max_shard_load_secs", DEFAULT_MAX_SHARD_LOAD)
        defaults = dataset.get("defaults", {})
        keyspace = (self.params.get("latte_schema_parameters") or {}).get("keyspace") or workload.default_keyspace

        # NOTE: the script drops with 'IF EXISTS', so on a fresh cluster this is a no-op.
        self._drop_table()
        self._create_schema()

        total_record_count = 0
        index_name = None
        for step_idx, step in enumerate(steps):
            if index_name is not None:
                self._drop_index(index_name, keyspace, max_index_wait)

            total_record_count += self._load_step_shards(step, local_ds_dir, container_ds_dir, max_load_wait)

            index_name = f"{workload.index_prefix}_{dataset_name}_{step_idx}"
            build_seconds = self._build_index(
                total_record_count, max_index_wait, index_name=index_name, keyspace=keyspace
            )
            self._record_index_build(dataset_name, step_idx + 1, index_name, total_record_count, build_seconds)

            self._run_step_queries(
                step, dataset_name, defaults, total_record_count, step_idx + 1, local_ds_dir, container_ds_dir
            )

        if index_name is not None:
            self._drop_index(index_name, keyspace, max_index_wait)

    def _run_step_queries(
        self, step, dataset_name, defaults, record_count, step_number, local_ds_dir, container_ds_dir
    ):
        """Run every query set of a step against the index that was just built, recording each."""
        for query_index, query in enumerate(step.get("queries", []), start=1):
            qset = _checked_name(query["set"], "query set name")
            limit, concurrency, rate = _query_params(query, defaults)
            search_duration = _query_duration(query, defaults)
            expected_p99_read_ms = _expected_p99_read_ms(query, defaults)
            queries_file = f"queries_{qset}.tsv"
            qrels_file = f"qrels_{qset}.tsv" if query.get("qrels") else None
            # Checked here to name the query set that asked for a missing file, rather than fail
            # on the copy to the loader.
            for needed in (queries_file, qrels_file):
                if needed and not os.path.isfile(os.path.join(local_ds_dir, needed)):
                    raise ValueError(
                        f"Query set {qset!r} of step #{step_number} needs {needed!r}, which is not in {local_ds_dir}"
                    )

            latency = self._run_search(
                local_ds_dir,
                container_ds_dir,
                queries_file=queries_file,
                limit=limit,
                concurrency=concurrency,
                rate=rate,
                search_duration=search_duration,
                qrels_file=qrels_file,
            )
            if not latency:
                self.log.warning("No HDR latency summary for query set %s of step #%d", qset, step_number)
            self._record_search(
                dataset=dataset_name,
                step=step_number,
                record_count=record_count,
                # Position in the step: two entries with the same configuration are told apart by it.
                query_index=query_index,
                query_set=qset,
                limit=limit,
                concurrency=concurrency,
                rate=rate,
                duration=search_duration,
                qrels=qrels_file is not None,
                query_example=_first_query_example(local_ds_dir, queries_file),
                expected_p99_read_ms=expected_p99_read_ms,
                latency=latency,
            )

    def _record_search(self, expected_p99_read_ms: float, latency: dict, **fields):
        """Record one query configuration: what ran, and the latency and throughput it measured.

        An empty *latency* -- no histogram for the run -- is recorded too, with nulls.
        """
        measured = {field: latency.get(key) for key, field in HDR_SUMMARY_FIELDS.items()}
        p99 = measured["p99_ms"]
        self._record_result(
            "search",
            **fields,
            expected_p99_read_ms=expected_p99_read_ms,
            **measured,
            p99_within_expectation=None if p99 is None else p99 <= expected_p99_read_ms,
        )

    def _record_result(self, phase: str, **fields):
        """Append one measurement to the run's results file."""
        record = {
            "workload": self.WORKLOAD.name,
            "phase": phase,
            "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **fields,
        }
        with open(os.path.join(self.logdir, RESULTS_FILE_NAME), "a", encoding="utf-8") as results:
            results.write(json.dumps(record) + "\n")

    def _record_index_build(self, dataset_name, step_number, index_name, record_count, build_seconds: float | None):
        """Record the build time and indexing throughput of one step's index.

        A build whose time was not measured is recorded too, with nulls, so the gap stays visible.
        """
        if build_seconds is None:
            self.log.warning("No index build time measured for %s", index_name)
        else:
            self.log.info("Index build time (vector-store full scan): %.4fs (%s)", build_seconds, index_name)
        self._record_result(
            "index_build",
            dataset=dataset_name,
            step=step_number,
            index=index_name,
            record_count=record_count,
            build_time_s=build_seconds,
            indexing_throughput=indexing_throughput(record_count, build_seconds),
        )
