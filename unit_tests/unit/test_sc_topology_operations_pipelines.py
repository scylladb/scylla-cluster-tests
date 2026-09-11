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

"""
Config-chain regression tests for the SC/EC "latency during topology operations" pipelines.

These jobs are assembled from ten YAML fragments each, and their correctness depends on merge
order: dict options such as append_scylla_yaml are merged key by key, while string options such
as append_scylla_args are replaced by the last fragment that sets them. Reordering or dropping a
fragment therefore fails silently - the run starts, produces numbers, and measures the wrong
thing. Every assertion here covers one of those silent failures.

The chains are read from the jenkinsfiles themselves, so a pipeline edit that breaks an
invariant fails here rather than 14 hours into a run.
"""

import json
import re
from pathlib import Path

import pytest

from sdcm import sct_config

PIPELINE_DIR = Path("jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression")
PIPELINE_PREFIX = "scylla-enterprise-perf-regression-latency-650gb-with-nemesis-cql-stress-"

SC_PIPELINES = {
    "i4i-sc": (PIPELINE_PREFIX + "tablets-sc.jenkinsfile", "i4i.4xlarge"),
    "i8g-sc": (PIPELINE_PREFIX + "i8g-tablets-sc.jenkinsfile", "i8g.4xlarge"),
}
EC_PIPELINES = {
    "i4i-ec": (PIPELINE_PREFIX + "tablets-ec.jenkinsfile", "i4i.4xlarge"),
    "i8g-ec": (PIPELINE_PREFIX + "i8g-tablets-ec.jenkinsfile", "i8g.4xlarge"),
}
ALL_PIPELINES = {**SC_PIPELINES, **EC_PIPELINES}

# set by enable_experimental_sc.yaml for SC, restored by ec_baseline_align_with_sc.yaml for EC
EXPECTED_SCYLLA_ARGS = (
    "--blocked-reactor-notify-ms 50 --abort-on-lsa-bad-alloc 1 --abort-on-seastar-bad-alloc "
    "--abort-on-internal-error 0 --abort-on-ebadf 1"
)
STRESS_CMD_KEYS = ("prepare_write_cmd", "stress_cmd_w", "stress_cmd_r", "stress_cmd_m")


def _config_chain(jenkinsfile_name: str) -> list:
    content = (PIPELINE_DIR / jenkinsfile_name).read_text(encoding="utf-8")
    match = re.search(r"test_config:\s*'''(\[.*?\])'''", content, re.DOTALL)
    assert match, f"no test_config found in {jenkinsfile_name}"
    return json.loads(match.group(1))


def _as_list(value) -> list:
    return [value] if isinstance(value, str) else list(value or [])


@pytest.fixture(name="resolved_configs", scope="module")
def fixture_resolved_configs():
    """Resolve every pipeline's config chain once, without touching any cloud API.

    ami_id_db_scylla is pinned so that SCTConfiguration skips the AMI lookup; nothing in these
    assertions depends on the image.
    """
    configs = {}
    with pytest.MonkeyPatch.context() as monkey:
        monkey.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkey.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
        for name, (jenkinsfile_name, _) in ALL_PIPELINES.items():
            monkey.setenv("SCT_CONFIG_FILES", json.dumps(_config_chain(jenkinsfile_name)))
            configs[name] = sct_config.SCTConfiguration()
    return configs


@pytest.fixture(name="conf")
def fixture_conf(request, resolved_configs):
    return resolved_configs[request.param]


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_append_scylla_yaml_options_are_merged_not_replaced(conf):
    """api_address and commitlog_sync come from different fragments and must both survive."""
    append_scylla_yaml = conf.get("append_scylla_yaml") or {}
    # the leader-aware cql-stress driver reads Raft leader info from the Scylla REST API
    assert append_scylla_yaml.get("api_address") == "0.0.0.0"
    assert append_scylla_yaml.get("commitlog_sync") == "batch"
    assert append_scylla_yaml.get("commitlog_sync_batch_window_in_ms") == 100


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_append_scylla_args_is_identical_for_sc_and_ec(conf):
    """A string option: the last fragment wins, so a reorder silently changes the db settings."""
    assert conf.get("append_scylla_args") == EXPECTED_SCYLLA_ARGS


@pytest.mark.parametrize("conf", list(SC_PIPELINES), indirect=True)
def test_sc_pipelines_enable_strongly_consistent_tables(conf):
    assert conf.get("experimental_features") == ["strongly-consistent-tables"]
    assert "consistency = 'global'" in conf.get("pre_create_keyspace")[0]


@pytest.mark.parametrize("conf", list(EC_PIPELINES), indirect=True)
def test_ec_pipelines_are_the_baseline(conf):
    assert not conf.get("experimental_features")
    assert "consistency" not in conf.get("pre_create_keyspace")[0]


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_keyspace_uses_tablets(conf):
    assert "tablets = {'enabled': true}" in conf.get("pre_create_keyspace")[0]


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_every_stress_command_is_leader_aware_cql_stress(conf):
    """A leftover cassandra-stress command would route SC writes away from the Raft leader."""
    assert conf.get("stress_image")["cql-stress-cassandra-stress"].endswith(":leader-aware-strong-consistency")
    for key in STRESS_CMD_KEYS:
        commands = _as_list(conf.get(key))
        assert commands, f"{key} is empty"
        for cmd in commands:
            assert "cql-stress-cassandra-stress" in cmd, f"{key} is not driven by cql-stress: {cmd}"
            # the leader-aware driver does not support cl=ALL
            assert "cl=QUORUM" in cmd, f"{key} does not use cl=QUORUM: {cmd}"


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_steady_state_load_holds_1000_connections_per_shard(conf):
    """250 connections per shard per loader x 4 loaders = 1000 per shard in aggregate."""
    assert conf.get("n_loaders") == [4]
    for key in ("stress_cmd_w", "stress_cmd_r", "stress_cmd_m"):
        for cmd in _as_list(conf.get(key)):
            assert "connectionsPerShard=250" in cmd, f"{key} does not set connectionsPerShard=250: {cmd}"


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_loader_shape_can_drive_that_connection_count(conf):
    """The c5.2xlarge of the base test-case cannot; nemesis_650gb_cql_stress_base.yaml overrides it."""
    assert conf.get("instance_type_loader") == "c7i.8xlarge"


@pytest.mark.parametrize(
    "conf, expected_db_instance",
    [pytest.param(name, instance, id=name) for name, (_, instance) in ALL_PIPELINES.items()],
    indirect=["conf"],
)
def test_db_instance_type(conf, expected_db_instance):
    assert conf.get("instance_type_db") == expected_db_instance


@pytest.mark.parametrize("conf", list(ALL_PIPELINES), indirect=True)
def test_topology_nemesis_is_inherited_from_the_base_test_case(conf):
    """NemesisSequence is the disruption that grows, replaces and shrinks the cluster."""
    assert conf.get("nemesis_class_name") == ["SisyphusMonkey"]
    assert conf.get("nemesis_selector") == ["NemesisSequence"]
