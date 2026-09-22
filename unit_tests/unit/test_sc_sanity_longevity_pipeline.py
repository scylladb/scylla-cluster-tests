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


def _all_stress_commands(config) -> list:
    """Every command that reaches a loader, prepare phase and main load alike."""
    return list(config.get("prepare_write_cmd")) + list(config.get("stress_cmd"))


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


def test_keyspace_is_created_by_cql_stress_not_by_sct(resolved_config):
    """SCT must not create keyspace1 first.

    tester.create_keyspace(), which _pre_create_schema() calls, emits no consistency clause, and
    cql-stress issues CREATE KEYSPACE IF NOT EXISTS - which does not upgrade an existing eventually
    consistent keyspace. Either option being set turns this into an EC run that still passes.
    """
    assert resolved_config.get("pre_create_schema") is False
    assert not resolved_config.get("pre_create_keyspace")


def test_tablets_are_enabled(resolved_config):
    """Tablets carry the strongly-consistent-tables feature, and cql-stress cannot emit a
    "tablets = {...}" clause, so the cluster default is what decides it."""
    append_scylla_yaml = resolved_config.get("append_scylla_yaml")
    assert append_scylla_yaml["enable_tablets"] is True
    assert append_scylla_yaml["tablets_mode_for_new_keyspaces"] == "enabled"


def test_every_stress_command_requests_a_strongly_consistent_keyspace(resolved_config):
    """consistency=global is what makes this an SC run at all.

    It is a cql-stress extension to the replication(...) sub-parameters, lifted out of the CQL
    replication map into a top-level keyspace property. NetworkTopologyStrategy must accompany it:
    the cql-stress default is SimpleStrategy, which 2026.2+ rejects because it cannot do tablet
    replication, and a non-tablet keyspace cannot carry the consistency option.
    """
    for command in _all_stress_commands(resolved_config):
        assert "consistency=global" in command, command
        assert "strategy=NetworkTopologyStrategy" in command, command
        assert "SimpleStrategy" not in command, command


def test_the_prepare_phase_creates_the_keyspace(resolved_config):
    """Only write and counterwrite emit the CREATE KEYSPACE DDL. A read-only command expects the
    keyspace to already exist, so at least one prepare command has to be a write."""
    prepare_commands = list(resolved_config.get("prepare_write_cmd"))
    assert prepare_commands
    assert all(" write " in command for command in prepare_commands), prepare_commands


def test_every_stress_command_uses_cql_stress_at_quorum(resolved_config):
    """The cql-stress default is local_one, which an SC keyspace rejects for writes - the run would
    fail at startup. Reads accept ONE/LOCAL_ONE but that turns leader-aware routing off."""
    commands = _all_stress_commands(resolved_config)
    assert commands
    for command in commands:
        assert command.startswith("cql-stress-cassandra-stress"), command
        assert "cl=QUORUM" in command, command


def test_coordinators_are_logged(resolved_config):
    """Without this there is no way to confirm after a run that leader-aware routing actually sent
    requests to the Raft leader rather than silently falling back to round-robin."""
    for command in _all_stress_commands(resolved_config):
        assert "coordinators=true" in command, command


def test_stress_errors_do_not_kill_the_run(resolved_config):
    """cql-stress defaults to fail-fast, which a nemesis job cannot survive: one operation that
    exhausts its retries ends the benchmark with exit 1, SCT raises a CRITICAL, and the test dies
    on chaos it was built to run. SC sharpens it further - an SC timeout arrives as
    WriteTimeout(SIMPLE), which the rust driver's default retry policy will not retry elsewhere
    (SCYLLADB-4671)."""
    for command in _all_stress_commands(resolved_config):
        assert "-errors ignore" in command, command


def test_user_profiles_are_not_used(resolved_config):
    """consistency=global is rejected at parse time in user mode: a user profile runs its own
    keyspace_definition and never executes the -schema DDL."""
    assert not resolved_config.get("cs_user_profiles")
    assert not resolved_config.get("prepare_cs_user_profiles")


def test_encryption_is_off(resolved_config):
    """cql-stress does not support encryption; longevity-100gb-4h already covers the encrypted path."""
    assert resolved_config.get("server_encrypt") is False
    assert resolved_config.get("client_encrypt") is False


def test_nemesis_is_configured(resolved_config):
    """The point of this job over the SC performance jobs is that chaos runs against SC tables.

    CategorySweepMonkey rather than SisyphusMonkey: this job is a coverage pass, so it runs every
    nemesis once, category by category, instead of cycling a shuffled set forever.
    """
    assert resolved_config.get("nemesis_class_name") == ["CategorySweepMonkey"]
