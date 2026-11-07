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

"""Tests for vector-store source-build mode inference, defaulting and reporting.

Source mode is inferred from either 'vector_store_source_repo' or 'vector_store_source_ref' being
set. The repo-only case is the one worth pinning down: it is a complete request ("build this fork's
default branch"), and keying off the ref alone would silently provision from the AMI instead.
"""

import logging
import shlex
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_aws import VectorStoreAWSNode
from sdcm.reporting.tooling_reporter import VectorStoreVersionReporter
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.vector_store_utils import (
    DEFAULT_VECTOR_STORE_SOURCE_REF,
    DEFAULT_VECTOR_STORE_SOURCE_REPO,
    VectorStoreClusterMixin,
    is_vector_store_source_build,
    resolve_vector_store_source,
)

FORK = "https://github.com/someone/vector-store.git"


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({}, False),
        ({"vector_store_source_repo": "", "vector_store_source_ref": ""}, False),
        ({"vector_store_source_repo": FORK}, True),
        ({"vector_store_source_ref": "my-branch"}, True),
        ({"vector_store_source_repo": FORK, "vector_store_source_ref": "my-branch"}, True),
    ],
    ids=["unset", "both-empty", "repo-only", "ref-only", "both"],
)
def test_source_build_inference(params, expected):
    assert is_vector_store_source_build(params) is expected


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"vector_store_source_repo": FORK}, (FORK, DEFAULT_VECTOR_STORE_SOURCE_REF)),
        ({"vector_store_source_ref": "my-branch"}, (DEFAULT_VECTOR_STORE_SOURCE_REPO, "my-branch")),
        ({"vector_store_source_repo": FORK, "vector_store_source_ref": "v1.2.3"}, (FORK, "v1.2.3")),
    ],
    ids=["repo-only-defaults-ref", "ref-only-defaults-repo", "both-kept"],
)
def test_resolve_applies_defaults(params, expected):
    assert resolve_vector_store_source(params) == expected


def test_resolve_defaults_to_upstream_master():
    """With neither set the resolver still returns something usable.

    Callers only reach it once is_vector_store_source_build() said yes, but a silent '' repo
    would turn into an unexplained clone failure on the node.
    """
    assert resolve_vector_store_source({}) == (
        DEFAULT_VECTOR_STORE_SOURCE_REPO,
        DEFAULT_VECTOR_STORE_SOURCE_REF,
    )


def test_default_repo_is_upstream():
    assert DEFAULT_VECTOR_STORE_SOURCE_REPO == "https://github.com/scylladb/vector-store.git"
    assert DEFAULT_VECTOR_STORE_SOURCE_REF == "master"


class _FakeConfig(dict):
    """Minimal stand-in for SCTConfiguration: the validation only reads params via .get()."""

    _validate_vector_store_source_params = SCTConfiguration._validate_vector_store_source_params


@pytest.mark.parametrize("backend", ["docker", "gce", "xcloud"])
def test_source_build_rejected_off_aws(backend):
    """The message must point at the docker image params, not at vector_store_version.

    On the non-aws backends 'vector_store_version' is an image tag, so mentioning it first would
    send the reader after the wrong param -- hence the backend check runs first.
    """
    config = _FakeConfig(cluster_backend=backend, vector_store_source_ref="master", vector_store_version="fts")
    with pytest.raises(ValueError, match="only supported on the aws backend") as excinfo:
        config._validate_vector_store_source_params()
    assert "vector_store_docker_image" in str(excinfo.value)


def test_source_build_and_version_are_mutually_exclusive():
    config = _FakeConfig(cluster_backend="aws", vector_store_source_ref="master", vector_store_version="0.5.0")
    with pytest.raises(ValueError, match="can't be used together with 'vector_store_version'"):
        config._validate_vector_store_source_params()


@pytest.mark.parametrize(
    "params",
    [
        {"vector_store_source_repo": FORK},
        {"vector_store_source_ref": "my-branch"},
        {"vector_store_source_repo": FORK, "vector_store_source_ref": "my-branch"},
    ],
    ids=["repo-only", "ref-only", "both"],
)
def test_source_build_accepted_on_aws_without_a_pinned_ami(params):
    """No 'ami_id_vector_store' required: the base AMI is resolved automatically."""
    _FakeConfig(cluster_backend="aws", **params)._validate_vector_store_source_params()


def test_ami_mode_is_not_validated():
    """With neither source param set the whole check is a no-op, version or not."""
    _FakeConfig(cluster_backend="docker", vector_store_version="fts")._validate_vector_store_source_params()


@pytest.mark.parametrize("backend", [None, ""], ids=["unset", "empty"])
def test_no_backend_skips_the_provisioning_checks(backend):
    """A backend-less config provisions nothing, so it must not be rejected.

    'upload', 'send-email' and the other utility subcommands build an SCTConfiguration just to read
    params, and the Jenkins steps behind them export SCT_CONFIG_FILES without SCT_CLUSTER_BACKEND
    (vars/collectBuilderLogs.groovy, vars/runSendEmail.groovy).
    """
    config = _FakeConfig(cluster_backend=backend, vector_store_source_ref="master", vector_store_version="fts")
    config._validate_vector_store_source_params()


def test_no_backend_key_at_all_skips_the_provisioning_checks():
    """'.get()' returns None for an absent key, same as an unset backend."""
    _FakeConfig(vector_store_source_ref="master")._validate_vector_store_source_params()


# ---------------------------------------------------------------------------
# Resolving the vector-store AMI
#
# A full SCTConfiguration: the lookup is in its __init__ (step 6.2), not in the validation.
# ---------------------------------------------------------------------------


def _resolve_calls(
    monkeypatch, backend: str | None, vs_nodes=1, source_build=True, version="", calls: list | None = None
) -> list:
    """Build an SCTConfiguration; record the vector-store AMI lookups it makes, as their versions."""
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    if backend:
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    if source_build:
        monkeypatch.setenv("SCT_VECTOR_STORE_SOURCE_REF", "master")
    if version:
        monkeypatch.setenv("SCT_VECTOR_STORE_VERSION", version)
    monkeypatch.setenv("SCT_N_VECTOR_STORE_NODES", str(vs_nodes))

    calls = [] if calls is None else calls
    monkeypatch.setattr(
        SCTConfiguration,
        "_resolve_vector_store_amis",
        lambda _self, region_names, version: calls.append(version) or "ami-vs",
    )
    with patch("sdcm.sct_config.config.convert_name_to_ami_if_needed", side_effect=lambda param, _regions: param):
        SCTConfiguration()
    return calls


def test_base_ami_is_not_resolved_without_a_backend(monkeypatch):
    """No AWS lookup for a backend-less config ('upload', 'send-email'), which provisions nothing."""
    assert _resolve_calls(monkeypatch, None) == []


def test_a_source_build_starts_from_the_newest_ami(monkeypatch):
    assert _resolve_calls(monkeypatch, "aws") == [None]


def test_the_newest_ami_is_the_default(monkeypatch):
    assert _resolve_calls(monkeypatch, "aws", source_build=False) == [None]


def test_a_pinned_version_wins(monkeypatch):
    assert _resolve_calls(monkeypatch, "aws", source_build=False, version="1.14.0") == ["1.14.0"]


def test_a_source_build_with_a_version_is_rejected_before_any_lookup(monkeypatch):
    """A misconfiguration reads as one, not as a failed cloud API call."""
    calls = []
    with pytest.raises(ValueError, match="can't be used together with 'vector_store_version'"):
        _resolve_calls(monkeypatch, "aws", version="1.14.0", calls=calls)
    assert calls == []


def test_no_lookup_without_vector_store_nodes(monkeypatch):
    """Every other aws test would pay for an AMI lookup it does not use."""
    assert _resolve_calls(monkeypatch, "aws", vs_nodes=0, source_build=False) == []


# ---------------------------------------------------------------------------
# Reporting a source build to Argus
# ---------------------------------------------------------------------------

SHA = "b346ea5d0315960e54b5476c82aec09318e726be"


def _reporter(version="0.0.0-dev", **source):
    reporter = VectorStoreVersionReporter(vector_store_client=MagicMock(), **source)
    reporter.vector_store_client.get_info.return_value = {"version": version}
    reporter._collect_version_info()
    return reporter


def test_ami_build_reports_the_plain_version():
    """Without a source build nothing extra is recorded -- this is the prebuilt-AMI path."""
    reporter = _reporter("1.14.0")
    assert reporter.version == "1.14.0"
    assert reporter.revision_id is None
    assert reporter.additional_data is None


def test_source_build_records_repo_ref_and_commit():
    """The package version, "0.0.0-dev" on master, does not say what was built."""
    reporter = _reporter(source_repo=FORK, source_sha=SHA, source_ref="my-branch")
    assert reporter.version == "0.0.0-dev (my-branch)"
    assert reporter.revision_id == SHA
    assert reporter.additional_data == f"{FORK}@my-branch"


def test_a_source_build_without_its_marker_is_reported_with_a_warning(caplog):
    """Otherwise Argus shows a source build as a plain '0.0.0-dev', and nothing says why."""
    cluster = VectorStoreClusterMixin.__new__(VectorStoreClusterMixin)
    node = MagicMock()
    node.name = "vs-node-1"
    node.get_vector_store_source_build_info.return_value = ("", "")
    cluster.nodes = [node]
    cluster.params = {"vector_store_source_ref": "master"}
    cluster.test_config = MagicMock()

    with patch("sdcm.utils.vector_store_utils.VectorStoreVersionReporter"), caplog.at_level(logging.WARNING):
        cluster._report_vector_store_version()

    assert "No source-build marker on vs-node-1" in caplog.text


# ---------------------------------------------------------------------------
# Invoking the installer on the node
# ---------------------------------------------------------------------------


def _install_cmd(repo: str, ref: str) -> str:
    """Return the command 'install_vector_store_from_source' would run on the node."""
    node = MagicMock()
    node.parent_cluster.params = {
        "vector_store_source_repo": repo,
        "vector_store_source_ref": ref,
        "vector_store_source_build_timeout": 3600,
    }
    node.vector_store_user = "ubuntu"
    node.vector_store_install_dir = "/home/ubuntu/vector-store"
    VectorStoreAWSNode.install_vector_store_from_source(node)
    return node.remoter.run.call_args.args[0]


def test_install_command_passes_the_repo_and_ref():
    cmd = _install_cmd(FORK, "my-branch")
    assert f"--repo {FORK}" in cmd
    assert "--ref my-branch" in cmd


@pytest.mark.parametrize(
    "ref",
    ["a-branch; rm -rf /", "$(id)", "with space", "quote'and\"more"],
    ids=["semicolon", "substitution", "space", "quotes"],
)
def test_install_command_quotes_a_hostile_ref(ref):
    """Both params come from test config, so they must not be able to break out of the argument."""
    cmd = _install_cmd(FORK, ref)
    assert shlex.split(cmd)[shlex.split(cmd).index("--ref") + 1] == ref
