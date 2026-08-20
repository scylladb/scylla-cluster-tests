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

  - dataset staging and the load, which need only a ScyllaDB container: the unit tests can assert that
    'LatteStressThread' calls 'send_files' for what it was given to stage, but whether the file arrives
    where the rune script looks for it depends on the container's working directory, the '-P' names the
    script declares and the path the flow interpolates, none of which a mock can check;
  - the whole per-dataset cycle -- schema, load, index build, build time, index drop -- which needs a
    vector-store that serves a full-text index, i.e. a nightly ScyllaDB and a vector-store built from
    source, so it skips unless both images are already local. Override either with
    SCT_FTS_IT_SCYLLA_IMAGE / SCT_FTS_IT_VECTOR_STORE_IMAGE.

The second one runs the flow's own methods, not a re-implementation of them. Only two seams are
replaced: '_run_latte', which would otherwise need ClusterTester's loader provisioning, and
'_report_build_metrics', so the rows can be asserted instead of submitted to Argus. Everything between
them -- the step loop, the '-P' mapping, the index naming, reading the build time out of vector-store's
log, waiting for the drop -- is the code that ships.
"""

import logging
import os
import subprocess

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

# A full-text index needs a ScyllaDB that has one and a vector-store that serves it. Neither the
# released ScyllaDB nor the released vector-store does yet, and 'local/vector-store:fts' is built by
# hand (see docs/fts-search-test.md), so the cycle test skips rather than fails where either is
# absent -- both are checked, since a pull attempt during fixture setup is an error, not a skip.
FTS_SCYLLA_IMAGE = os.environ.get("SCT_FTS_IT_SCYLLA_IMAGE", "scylladb/scylla-nightly:latest")
FTS_VECTOR_STORE_IMAGE = os.environ.get("SCT_FTS_IT_VECTOR_STORE_IMAGE", "local/vector-store:fts")

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
    remote_dir = f"{workload.remote_root}/integration"
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
    """The negative half, and the reason the thread subclass exists at all.

    Without staging, the file never reaches the container and the rune script's 'prepare' cannot read
    it -- latte aborts. If this ever loads rows, the positive test above is no longer proving that the
    corpus came from where it thinks.

    Asserted as "no rows", not as a raised exception: a failing latte command surfaces as an error
    *event*, which 'ClusterTester.verify_stress_thread' turns into a test failure through
    'parse_results()'. 'get_results()' on its own returns normally, so a run that skipped
    'verify_stress_thread' would not notice. The flow always calls it -- see '_run_latte'.
    """
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    workload = FTS_WORKLOAD
    stress_cmd = (
        f"latte run -f load {workload.script} "
        f"-d {len(DOCUMENTS)} "
        rf"-P {workload.params.dataset_dir}=\"{workload.remote_root}/absent\" "
        rf"-P {workload.params.records_file}=\"{staged_corpus.name}\" "
    )

    latte_thread = LatteStressThread(
        loader_set,
        stress_cmd,
        node_list=[docker_scylla],
        timeout=5,
        params=params,
        extra_files_to_stage=[],
    )
    request.addfinalizer(latte_thread.kill)

    latte_thread.run()
    _, errors = latte_thread.parse_results()
    assert errors, "a load whose corpus never reached the container must be reported as an error"

    keyspace, table = workload.default_keyspace, "documents"
    with docker_scylla.parent_cluster.cql_connection_patient(docker_scylla) as session:
        loaded = list(session.execute(f"SELECT doc_id FROM {keyspace}.{table}"))
    assert not loaded, "nothing can have been loaded from a file that was never staged"


def _image_present(image: str) -> bool:
    """Is the image already local? Checked at collection time, because the fixture that starts the
    container would otherwise try to pull it during setup -- and a pull failure is an error, not a
    skip."""
    try:
        return subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=False).returncode == 0
    except OSError:  # no docker on this machine at all
        return False


def _flow_over(params, docker_scylla, vs_cluster, loader_set, build_rows):
    """The real FtsSearchTest with its two infrastructure seams replaced.

    Built with '__new__' rather than instantiated: ClusterTester's constructor is unittest's, and what
    the phase methods actually use is a handful of attributes.
    """
    flow = fts_test.FtsSearchTest.__new__(fts_test.FtsSearchTest)
    flow.params = params
    flow.log = LOGGER
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
    flow._report_build_metrics = lambda build_time, record_count, row_key: build_rows.append(
        (row_key, build_time, record_count)
    )
    return flow


@pytest.mark.skipif(
    not (_image_present(FTS_VECTOR_STORE_IMAGE) and _image_present(FTS_SCYLLA_IMAGE)),
    reason=(
        f"{FTS_VECTOR_STORE_IMAGE} or {FTS_SCYLLA_IMAGE} is not available locally; "
        "build or pull them per docs/fts-search-test.md"
    ),
)
@pytest.mark.docker_scylla_args(scylla_docker_image=FTS_SCYLLA_IMAGE, vs_docker_image=FTS_VECTOR_STORE_IMAGE)
@pytest.mark.xdist_group("docker_heavy")
def test_a_dataset_is_loaded_indexed_and_reported(request, docker_scylla, docker_vector_store, params, tmp_path):
    """The per-dataset cycle the whole test is built on, against a live vector-store.

    One dataset, two steps: load a shard and build an index over it, then load a second shard and
    rebuild, so the cumulative record count and the drop-then-rebuild path are both exercised. What is
    asserted is what a run reports -- a build row per step with a positive build time -- plus the rows
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

    build_rows = []
    flow = _flow_over(params, docker_scylla, docker_vector_store, LocalLoaderSetDummy(params=params), build_rows)
    flow._run_dataset(
        {
            "name": dataset_name,
            "max_index_wait_secs": 300,
            "steps": [{"shards": [0]}, {"shards": [1]}],
        }
    )

    assert [row[0] for row in build_rows] == [
        f"{dataset_name} | {len(DOCUMENTS)} docs | build #1",
        f"{dataset_name} | {len(DOCUMENTS) + 2} docs | build #2",
    ], f"one build row per step, with cumulative counts: {build_rows}"
    assert all(build_time > 0 for _, build_time, _ in build_rows), (
        f"every build time comes from vector-store's 'full scan' lines and must be positive: {build_rows}"
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
