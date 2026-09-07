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

"""Unit tests for the FTS half of the search performance flow: the workload descriptor.

The flow itself is covered by test_search_perf_test.py. What is left to check here is that the
descriptor still matches the two things it names -- fts.rn and the Argus table -- because both can
drift out from under it. fts.rn is mirrored from scylladb/vector-store, so a rename there would
otherwise only surface as a latte run that ignores every '-P' it was given, hours into an AWS run.
"""

import dataclasses
import re

import pytest

# NOTE: the test class is reached through the module rather than imported by name -- pytest's
#       unittest collector picks up any 'unittest.TestCase' subclass in a test module's namespace,
#       and 'FtsSearchTest' is one through ClusterTester.
import fts_test
from fts_test import FTS_BUILD_COUNT_COLUMN, FTS_WORKLOAD, FtsIndexBuildResult
from sdcm import sct_abs_path

RUNE_PARAM_RE = re.compile(r"""latte::param!\(\s*["'](?P<name>[^"']+)["']""")
RUNE_FUNCTION_RE = re.compile(r"^pub async fn (?P<name>\w+)", re.MULTILINE)


@pytest.fixture(scope="module")
def script_source():
    with open(sct_abs_path(FTS_WORKLOAD.script), encoding="utf-8") as script:
        return script.read()


def test_every_mapped_parameter_is_a_parameter_of_the_script(script_source):
    """The descriptor's whole point is mapping onto fts.rn's own names, so check it does."""
    declared = set(RUNE_PARAM_RE.findall(script_source))
    mapped = {getattr(FTS_WORKLOAD.params, field.name) for field in dataclasses.fields(FTS_WORKLOAD.params)}
    assert mapped <= declared, f"not parameters of {FTS_WORKLOAD.script}: {sorted(mapped - declared)}"


def test_the_hdr_tag_names_a_function_of_the_script(script_source):
    """The tag is 'fn--<function>'; latte emits nothing under it if that function is not there,
    and the latency table would come out empty."""
    function = FTS_WORKLOAD.hdr_tag.removeprefix("fn--")
    assert function in set(RUNE_FUNCTION_RE.findall(script_source))


def test_the_phases_the_flow_invokes_exist(script_source):
    """'load', 'build_index' and 'search' are the contract between the flow and any rune script."""
    functions = set(RUNE_FUNCTION_RE.findall(script_source))
    assert {"load", "build_index", "search"} <= functions


def test_the_step_record_file_key_shares_the_scripts_vocabulary():
    """A plan says 'documents_file:' because fts.rn calls it that; the two must not drift apart."""
    assert FTS_WORKLOAD.step_records_file_key == FTS_WORKLOAD.params.records_file


def test_index_build_table_counts_documents():
    """The count column is named once and reused, since Argus keys the table's history by it."""
    columns = FtsIndexBuildResult.Meta.Columns
    assert FTS_BUILD_COUNT_COLUMN in {column.name for column in columns}
    assert FTS_WORKLOAD.build_count_column == FTS_BUILD_COUNT_COLUMN


def test_argus_names_are_the_ones_the_history_is_under():
    """Renaming any of these silently starts a new, empty history in Argus."""
    assert FtsIndexBuildResult.Meta.name == "FTS Index Build Time"
    assert FTS_WORKLOAD.name == "fts_search"  # cycle names: 'fts_search_p99_10ms'
    assert FTS_WORKLOAD.item_noun == "docs"  # row labels: 'ds | 900 docs | term_common'
    assert FTS_WORKLOAD.index_prefix == "fts_idx"


def test_the_test_case_entry_point_is_the_one_the_pipelines_call():
    """jenkins-pipelines/performance_staging/fts-search-test.jenkinsfile names this sub_test, and
    docs/fts-search-test.md tells you to run it. Renaming it breaks both."""
    assert callable(fts_test.FtsSearchTest.test_fts_search)
    assert fts_test.FtsSearchTest.WORKLOAD is FTS_WORKLOAD
