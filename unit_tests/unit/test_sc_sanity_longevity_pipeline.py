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

"""Config-chain regression tests for the Strong Consistency sanity longevity pipeline.

The job is assembled from four YAML fragments and its correctness depends on merge order: dict
options such as append_scylla_yaml are merged key by key, while string options such as
append_scylla_args are replaced by the last fragment that sets them. Dropping or reordering a
fragment fails silently - the run starts, passes, and proves nothing about Strong Consistency.
Every assertion here covers one of those silent failures.

The chain is read from the jenkinsfile itself, so a pipeline edit that breaks an invariant fails
here rather than four hours into a run.
"""

import json
import re
from pathlib import Path

import pytest

from sdcm import sct_config

JENKINSFILE = Path("jenkins-pipelines/oss/longevity/rust/longevity-100gb-4h-cql-stress-sc.jenkinsfile")
TEST_CASE = "test-cases/longevity/longevity-100gb-4h-cql-stress-sc.yaml"

# set by configurations/strong_consistency/enable_experimental_sc.yaml, which must come after the
# test-case in the chain
EXPECTED_SCYLLA_ARGS = (
    "--blocked-reactor-notify-ms 50 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc "
    "--abort-on-internal-error 0 --abort-on-ebadf 1"
)
LEADER_AWARE_IMAGE = "aleksbykov/cql-stress:leader-aware-strong-consistency"


def _config_chain() -> list:
    content = JENKINSFILE.read_text(encoding="utf-8")
    match = re.search(r"test_config:\s*'''(\[.*?\])'''", content, re.DOTALL)
    assert match, f"no test_config found in {JENKINSFILE}"
    return json.loads(match.group(1))


@pytest.fixture(name="resolved_config", scope="module")
def fixture_resolved_config():
    """Resolve the pipeline's config chain once, without touching any cloud API.

    ami_id_db_scylla is pinned so that SCTConfiguration skips the AMI lookup; nothing in these
    assertions depends on the image.
    """
    with pytest.MonkeyPatch.context() as monkey:
        monkey.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkey.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
        monkey.setenv("SCT_CONFIG_FILES", json.dumps(_config_chain()))
        return sct_config.SCTConfiguration()


def test_chain_starts_with_the_test_case():
    """enable_experimental_sc.yaml replaces append_scylla_args, so it must come after the test-case."""
    assert _config_chain()[0] == TEST_CASE


def test_strong_consistency_feature_is_enabled(resolved_config):
    assert resolved_config.get("experimental_features") == ["strongly-consistent-tables"]


def test_scylla_args_come_from_the_sc_fragment(resolved_config):
    """A reordered chain would leave the default --blocked-reactor-notify-ms 25 in place."""
    assert resolved_config.get("append_scylla_args") == EXPECTED_SCYLLA_ARGS


def test_rest_api_is_reachable_from_loaders(resolved_config):
    """Leader-aware load balancing queries Raft leader info over the REST API from the loaders."""
    assert resolved_config.get("append_scylla_yaml")["api_address"] == "0.0.0.0"


def test_commitlog_sync_batch_is_applied(resolved_config):
    append_scylla_yaml = resolved_config.get("append_scylla_yaml")
    assert append_scylla_yaml["commitlog_sync"] == "batch"
    assert append_scylla_yaml["commitlog_sync_batch_window_in_ms"] == 100


def test_leader_aware_stress_image_wins(resolved_config):
    """Without the custom image every SC write pays an extra hop to the Raft leader."""
    assert resolved_config.get("stress_image")["cql-stress-cassandra-stress"] == LEADER_AWARE_IMAGE


def test_keyspace_is_strongly_consistent_on_tablets(resolved_config):
    statements = resolved_config.get("pre_create_keyspace")
    assert len(statements) == 1
    statement = statements[0]
    assert "consistency = 'global'" in statement
    assert "tablets = {'enabled': true}" in statement


def test_every_stress_command_uses_cql_stress_at_quorum(resolved_config):
    """cl=ALL does not work with the leader-aware policy (cql-stress commit 1505752b6)."""
    commands = list(resolved_config.get("prepare_write_cmd")) + list(resolved_config.get("stress_cmd"))
    assert commands
    for command in commands:
        assert command.startswith("cql-stress-cassandra-stress"), command
        assert "cl=QUORUM" in command, command


def test_encryption_is_off(resolved_config):
    """cql-stress does not support encryption; longevity-100gb-4h already covers the encrypted path."""
    assert resolved_config.get("server_encrypt") is False
    assert resolved_config.get("client_encrypt") is False


def test_nemesis_is_configured(resolved_config):
    """The point of this job over the SC performance jobs is that chaos runs against SC tables."""
    assert resolved_config.get("nemesis_class_name") == ["SisyphusMonkey"]
