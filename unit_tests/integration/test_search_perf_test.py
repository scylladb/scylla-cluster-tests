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

"""Integration tests for the search performance flow, against real ScyllaDB and vector-store.

Two levels, because they need different things to run:

  - staging a dataset file and loading it, which need only a ScyllaDB container: the unit tests can
    only assert what 'LatteStressThread' sends to the loader and mounts, but whether the file arrives
    where the rune script looks for it depends on the mount itself, the '-P' names the script declares
    and the path the flow interpolates, none of which a mock can check;
  - the whole per-dataset cycle -- schema, load, index build, build time, index drop -- which needs a
    ScyllaDB with full-text indexes (2026.3+) and a vector-store that serves them. It runs the 'latest'
    images by default; override either with SCT_FTS_IT_SCYLLA_IMAGE / SCT_FTS_IT_VECTOR_STORE_IMAGE,
    e.g. to test a local vector-store build.

The second one runs the flow's own methods, not a re-implementation of them. Only one seam is
replaced: '_run_latte', which would otherwise need ClusterTester's loader provisioning. Everything
behind it -- staging each shard on the loader's host, the step loop, the '-P' mapping, the index
naming, reading the build time out of vector-store's log, the results file, waiting for the drop --
is the code that ships.
"""

import json
import logging
import os

import pytest

import fts_test
import search_perf_test
from fts_test import FTS_WORKLOAD
from sdcm.stress.latte_thread import LatteStressThread
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]

LOGGER = logging.getLogger(__name__)

# Full-text indexes are in ScyllaDB since 2026.3, served by vector-store since 1.11.0. Until
# scylladb/vector-store#635 is fixed, vector-store's 'latest' is 1.5.1: set
# SCT_FTS_IT_VECTOR_STORE_IMAGE=scylladb/vector-store:1.11.0.
FTS_SCYLLA_IMAGE = os.environ.get("SCT_FTS_IT_SCYLLA_IMAGE", "scylladb/scylla:latest")
FTS_VECTOR_STORE_IMAGE = os.environ.get("SCT_FTS_IT_VECTOR_STORE_IMAGE", "scylladb/vector-store:latest")

# Bodies distinctive enough that reading one back proves the row came from the staged file rather
# than from anything the script might have defaulted to.
DOCUMENTS = (
    ("doc_000001", "quasar obsidian zephyr staged from the test"),
    ("doc_000002", "obsidian zephyr and nothing else"),
    ("doc_000003", "zephyr alone"),
)


@pytest.fixture(name="staged_corpus")
def fixture_staged_corpus(tmp_path):
    """A tiny documents TSV in the layout generate_local_dataset.py produces: '<id>\\t<body>'."""
    corpus = tmp_path / "documents_000.tsv"
    corpus.write_text("".join(f"{doc_id}\t{body}\n" for doc_id, body in DOCUMENTS), encoding="utf-8")
    return corpus


def test_a_staged_corpus_is_loaded_by_the_rune_script(request, docker_scylla, params, staged_corpus):
    """The whole staging path end to end: the file reaches the container and latte loads it."""
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    workload = FTS_WORKLOAD
    remote_dir = f"{workload.container_root}/integration"
    remote_path = f"{remote_dir}/{staged_corpus.name}"
    stress_cmd = (
        f"latte run -f load {workload.script} "
        f"-d {len(DOCUMENTS)} "
        rf"-P {workload.params.dataset_dir}=\"{remote_dir}\" "
        rf"-P {workload.params.records_file}=\"{staged_corpus.name}\" "
    )

    latte_thread = LatteStressThread(
        loader_set,
        stress_cmd,
        node_list=[docker_scylla],
        timeout=5,
        params=params,
        extra_files_to_stage=[(str(staged_corpus), remote_path)],
    )
    request.addfinalizer(latte_thread.kill)

    latte_thread.run()
    latte_thread.get_results()

    keyspace, table = workload.default_keyspace, "documents"
    with docker_scylla.parent_cluster.cql_connection_patient(docker_scylla) as session:
        rows = {row.doc_id: row.body for row in session.execute(f"SELECT doc_id, body FROM {keyspace}.{table}")}

    assert rows == dict(DOCUMENTS), (
        "the rows in ScyllaDB must be exactly the staged file's -- a mismatch means the corpus latte "
        "read was not the one this test staged"
    )


def test_an_unstaged_corpus_loads_nothing(request, docker_scylla, params, staged_corpus):
    """The negative half.

    Without staging, the file never reaches the container and the rune script's 'prepare' cannot read
    it -- latte aborts. If this ever loads rows, the positive test above is no longer proving that the
    corpus came from where it thinks.
    """
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    workload = FTS_WORKLOAD
    stress_cmd = (
        f"latte run -f load {workload.script} "
        f"-d {len(DOCUMENTS)} "
        rf"-P {workload.params.dataset_dir}=\"{workload.container_root}/absent\" "
        rf"-P {workload.params.records_file}=\"{staged_corpus.name}\" "
    )

    latte_thread = LatteStressThread(
        loader_set,
        stress_cmd,
        node_list=[docker_scylla],
        timeout=5,
        params=params,
    )
    request.addfinalizer(latte_thread.kill)

    latte_thread.run()
    _, errors = latte_thread.parse_results()
    assert errors, "a load whose corpus never reached the container must be reported as an error"

    keyspace, table = workload.default_keyspace, "documents"
    with docker_scylla.parent_cluster.cql_connection_patient(docker_scylla) as session:
        loaded = list(session.execute(f"SELECT doc_id FROM {keyspace}.{table}"))
    assert not loaded, "nothing can have been loaded from a file that was never staged"


def _flow_over(params, docker_scylla, vs_cluster, loader_set, logdir):
    """The real FtsSearchTest with its infrastructure seam replaced.

    Built with '__new__' rather than instantiated: ClusterTester's constructor is unittest's, and what
    the phase methods actually use is a handful of attributes.
    """
    flow = fts_test.FtsSearchTest.__new__(fts_test.FtsSearchTest)
    flow.params = params
    flow.log = LOGGER
    flow.logdir = str(logdir)
    flow.loaders = loader_set
    flow.db_cluster = docker_scylla.parent_cluster
    flow.db_cluster.vector_store_cluster = vs_cluster

    def run_latte(stress_cmd, files_to_stage=None, **_kwargs):
        thread = LatteStressThread(
            loader_set,
            stress_cmd,
            node_list=[docker_scylla],
            timeout=10,
            params=params,
            extra_files_to_stage=files_to_stage or [],
        )
        thread.run()
        _, errors = thread.parse_results()
        assert not errors, f"latte reported errors for {stress_cmd!r}: {errors}"
        return thread

    flow._run_latte = run_latte
    return flow


@pytest.mark.docker_scylla_args(scylla_docker_image=FTS_SCYLLA_IMAGE, vs_docker_image=FTS_VECTOR_STORE_IMAGE)
@pytest.mark.xdist_group("docker_heavy")
def test_a_dataset_is_loaded_indexed_and_reported(request, docker_scylla, docker_vector_store, params, tmp_path):
    """The per-dataset cycle the whole test is built on, against a live vector-store.

    One dataset, two steps: load a shard and build an index over it, then load a second shard and
    rebuild, so the cumulative record count and the drop-then-rebuild path are both exercised. What is
    asserted is what a run records -- a build record per step with a positive build time -- plus the rows
    actually in ScyllaDB and the index being gone at the end.
    """
    assert docker_vector_store, "the vector-store fixture did not start"

    params["enable_argus"] = False
    dataset_name = "integration"
    dataset_dir = tmp_path / dataset_name
    (dataset_dir / "shards").mkdir(parents=True)
    for shard, documents in enumerate((DOCUMENTS, DOCUMENTS[:2])):
        (dataset_dir / "shards" / f"documents_{shard:03d}.tsv").write_text(
            "".join(f"{doc_id}_{shard}\t{body}\n" for doc_id, body in documents), encoding="utf-8"
        )
    # The flow resolves a dataset directory inside the repo; keep this run's data out of the tree.
    request.getfixturevalue("monkeypatch").setattr(
        search_perf_test, "_local_path", lambda _workload, *parts: str(tmp_path.joinpath(*parts))
    )

    logdir = tmp_path / "logs"
    logdir.mkdir()
    flow = _flow_over(params, docker_scylla, docker_vector_store, LocalLoaderSetDummy(params=params), logdir)
    flow._run_dataset(
        {
            "name": dataset_name,
            "max_index_wait_secs": 300,
            "steps": [{"shards": [0]}, {"shards": [1]}],
        }
    )

    with open(logdir / search_perf_test.RESULTS_FILE_NAME, encoding="utf-8") as results:
        builds = [json.loads(line) for line in results]
    assert [(build["dataset"], build["step"], build["record_count"]) for build in builds] == [
        (dataset_name, 1, len(DOCUMENTS)),
        (dataset_name, 2, len(DOCUMENTS) + 2),
    ], f"one build record per step, with cumulative counts: {builds}"
    assert all(build["build_time_s"] > 0 for build in builds), (
        f"every build time comes from vector-store's 'full scan' lines and must be positive: {builds}"
    )

    keyspace = FTS_WORKLOAD.default_keyspace
    with flow.db_cluster.cql_connection_patient(docker_scylla) as session:
        loaded = list(session.execute(f"SELECT doc_id FROM {keyspace}.documents"))
    assert len(loaded) == len(DOCUMENTS) + 2, "both shards must be in the table"

    vs_client = docker_vector_store.nodes[0].get_vector_store_api_client()
    last_index = f"{FTS_WORKLOAD.index_prefix}_{dataset_name}_1"
    assert vs_client.get_index_status_or_none(keyspace, last_index.lower()) is None, (
        "the dataset loop drops the index it built last"
    )
