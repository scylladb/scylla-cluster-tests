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

"""Full-Text Search (BM25) performance test.

The flow -- plan, datasets, cumulative shard steps, index build timing -- lives in
search_perf_test.py and is shared with the other search benchmarks. This module is the full-text
half of it: the rune script to run, the vocabulary to report in, and the names to report under.

Driven by a YAML plan (the `search_test_config` param) naming the datasets and the shards to load. See
docs/fts-search-test.md.

Results go to Argus: one row per index build in the "FTS Index Build Time" table. The query phase,
which adds a latency row per query configuration, lands on top of this.
"""

from argus.client.generic_result import StaticGenericResultTable

from search_perf_test import LatteScriptParams, SearchPerformanceTest, SearchWorkload
from sdcm.utils.vector_store_index import index_build_columns

FTS_BASE_DIR = "data_dir/latte/fts_search"

# The column of the index build table counting what was indexed. Named once: it goes both into the
# table definition and into every row submitted, and Argus keys the table's history by it.
FTS_BUILD_COUNT_COLUMN = "document_count"


class FtsIndexBuildResult(StaticGenericResultTable):
    class Meta:
        name = "FTS Index Build Time"
        description = "Full-text search index build time and throughput"
        Columns = index_build_columns(FTS_BUILD_COUNT_COLUMN, "docs")


FTS_WORKLOAD = SearchWorkload(
    name="fts_search",
    base_dir=FTS_BASE_DIR,
    script=f"{FTS_BASE_DIR}/fts.rn",
    item_noun="docs",
    index_prefix="fts_idx",
    default_keyspace="fts_bench",
    build_result_table=FtsIndexBuildResult,
    build_count_column=FTS_BUILD_COUNT_COLUMN,
    # The names fts.rn uses. It is mirrored from scylladb/vector-store, so they are its to choose --
    # test_fts_test.py checks that each one is still a parameter of the script.
    params=LatteScriptParams(
        dataset_dir="fts_data_dir",
        records_file="documents_file",
        record_count="document_count",
        index_name="index_name",
        max_index_wait="max_index_wait_secs",
        min_probes="min_successful_probes",
        schema_cleanup="schema_cleanup",
        drop_index="drop_index",
    ),
    step_records_file_key="documents_file",
    default_records_file="documents.tsv",
    default_shard_suffix="documents_{:03d}.tsv",
)


class FtsSearchTest(SearchPerformanceTest):
    """FTS (Full-Text Search / BM25) performance test.

    Runs multi-dataset, multi-step FTS benchmarks from a YAML plan in the repo (see
    'resolve_test_config_path'): per-shard loading, index building and Argus reporting all come from
    'SearchPerformanceTest'.
    """

    WORKLOAD = FTS_WORKLOAD

    def test_fts_search(self):
        self.run_search_benchmark()
