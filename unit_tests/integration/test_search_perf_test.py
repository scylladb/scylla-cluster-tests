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

"""Integration test for the per-run dataset staging of the search performance flow.

The unit tests can only assert that 'LatteStressThread' calls 'send_files' for what it was given to
stage -- whether the file actually arrives where the rune script looks for it depends on the
container's working directory, the '-P' names the script declares and the path the flow interpolates,
none of which a mock can check. So this loads a corpus for real: stage a TSV, run 'latte run -f load'
against a live ScyllaDB, and read the rows back over CQL.

No vector-store and no index are involved -- fts.rn's 'load' branch creates the keyspace and table
itself and only INSERTs -- so this runs against a plain ScyllaDB container.
"""

import logging

import pytest

from sdcm.stress.latte_thread import LatteStressThread
from fts_test import FTS_WORKLOAD
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]

LOGGER = logging.getLogger(__name__)

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
