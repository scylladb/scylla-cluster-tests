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

"""Tests of the Jenkins params -> SCT_* env mapping, generated from the SCT configuration model (SCT-1492)."""

import json
import os
import re
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from sct_pipeline import pipeline
from sdcm.sct_config import config as sct_config
from sdcm.sct_config.helpers import branch_desc
from sdcm.utils.get_username import get_username
from sdcm.utils.pipeline_env import PipelineInput, option_params, pipeline_env

REPO = Path(__file__).parents[2]
# runSctTest.groovy as it was before the mapping moved to Python: the reference for its values
GROOVY_RUN_SCT_TEST = REPO / "unit_tests/test_data/pipeline_env/runSctTest.groovy"
BUILDER_REGION = "us-east-1"


def env_of(params=None, pipeline_params=None, environ=None):
    return pipeline_env(PipelineInput(params or {}, pipeline_params or {}, BUILDER_REGION), environ)


# --- generated from the model ------------------------------------------------------------------


def test_no_groovy_maps_a_param_to_sct_env():
    mapping = re.compile(
        r"export\s+(SCT_\w+|BUILD_USER_REQUESTED_BY|PYTEST_ADDOPTS)=|SCT_\w+\s*=\s*\"?\$\{(params|pipelineParams)\."
    )
    found = [
        f"{path.name}:{number}"
        for path in sorted((REPO / "vars").glob("*.groovy"))
        for number, line in enumerate(path.read_text().splitlines(), 1)
        if mapping.search(line)
    ]

    assert found == [], "give the job parameter the option's name (or a jenkins_param) and use pipelineEnv"


def test_every_sct_option_is_set_by_the_param_of_its_name():
    env = env_of({option: f"v-{option}" for option in option_params()})

    assert env == {f"SCT_{option.upper()}": f"v-{option}" for option in option_params()}


def test_jenkins_param_sets_its_option():
    aliased = {option: params[1:] for option, params in option_params().items() if len(params) > 1}

    assert aliased["cluster_backend"] == ("backend",)
    for option, aliases in aliased.items():
        for alias in aliases:
            assert env_of({alias: "v"})[f"SCT_{option.upper()}"] == "v", (option, alias)


def test_jenkins_param_is_only_declared_for_a_different_name():
    for option, params in option_params().items():
        for alias in params[1:]:
            assert alias != option
            assert alias not in option_params(), f"{option}: '{alias}' is an option of its own"


def test_param_of_the_option_name_wins_over_its_jenkins_param():
    assert env_of({"docker_image": "own", "scylla_docker_image": "alias"})["SCT_DOCKER_IMAGE"] == "own"


def test_params_win_over_pipeline_params():
    env = env_of({"n_db_nodes": 3}, {"n_db_nodes": 6, "k8s_deploy_monitoring": True})

    assert env["SCT_N_DB_NODES"] == "3"
    assert env["SCT_K8S_DEPLOY_MONITORING"] == "true"


def test_value_in_the_build_environment_wins():
    env = env_of({"gce_project": "param", "n_db_nodes": 3}, environ={"SCT_GCE_PROJECT": "build-env"})

    assert "SCT_GCE_PROJECT" not in env
    assert env["SCT_N_DB_NODES"] == "3"


@pytest.mark.parametrize("value", ["", None, False])
def test_unset_param_is_not_exported(value):
    assert "SCT_N_DB_NODES" not in env_of({"n_db_nodes": value})


def test_random_region_is_the_builder_region():
    env = env_of({"region": "random", "oci_region_name": "random"})

    assert env["SCT_REGION_NAME"] == env["SCT_OCI_REGION_NAME"] == env["SCT_MINICLOUD_REGIONS"] == BUILDER_REGION


def test_values_keep_their_form():
    title = """nightly "run" it's $HOME"""

    env = env_of(
        {
            "test_email_title": title,
            "azure_region_name": '["eastus", "westus"]',
            "email_recipients": ["a@b.c"],
            "k8s_enable_tls": True,
        }
    )

    assert env["SCT_EMAIL_SUBJECT_POSTFIX"] == title
    assert env["SCT_AZURE_REGION_NAME"] == '["eastus", "westus"]'
    assert env["SCT_EMAIL_RECIPIENTS"] == '["a@b.c"]'
    assert env["SCT_K8S_ENABLE_TLS"] == "true"


# --- runSctTest: the same values as the Groovy it replaced ----------------------------------------

# runSctTest's own values that now come from somewhere else; everything else must be equal
RENAMED = {"BUILD_USER_REQUESTED_BY": "SCT_REQUESTED_BY_USER", "PYTEST_ADDOPTS": "SCT_PYTEST_ADDOPTS"}
MOVED = {
    "SCT_COLLECT_LOGS",  # false is the default
    "SCT_AMI_ID_DB_SCYLLA_DESC",  # the configuration computes it from GIT_BRANCH
    "SCT_USE_MGMT",  # the configuration turns it off with a unified package
}


def groovy_truth(value):
    return value not in (None, "", False, 0)


def gstring(value):
    """`"${value}"` in Groovy."""
    return "null" if value is None else str(value).lower() if isinstance(value, bool) else str(value)


def init_aws_region_param(value):
    """vars/initAwsRegionParam.groovy"""
    return BUILDER_REGION if value == "random" else json.dumps(value)


def evaluate(expr, params, pipeline_params, local_vars):
    """The few Groovy expressions that runSctTest interpolated into its export block."""
    maps = {"params": params, "pipelineParams": pipeline_params}
    if match := re.fullmatch(r"(params|pipelineParams)\.(\w+)", expr):
        return gstring(maps[match[1]].get(match[2]))
    if match := re.fullmatch(r"(params|pipelineParams)\.(\w+) (?:\? \1\.\2 : |\?: )''", expr):
        value = maps[match[1]].get(match[2])
        return gstring(value) if groovy_truth(value) else ""
    if match := re.fullmatch(r"params\.(\w+) \|\| false", expr):
        return gstring(groovy_truth(params.get(match[1])))
    if expr in local_vars:
        return local_vars[expr]
    raise AssertionError(f"cannot evaluate the Groovy expression ${{{expr}}}")


def groovy_env(params, pipeline_params, tmp_path):
    """Run the export block of the former runSctTest.groovy in bash, and return what it exported."""
    text = GROOVY_RUN_SCT_TEST.read_text()
    block = next(block for block in re.findall(r'sh """(#!/bin/bash.*?)"""', text, re.S) if "export SCT_" in block)
    block = block[: block.index('echo "start test')]
    local_vars = {
        "test_config": json.dumps(params.get("test_config")),
        "email_recipients": json.dumps(params.get("email_recipients") or ""),
        "current_region": init_aws_region_param(params.get("region")),
        "current_gce_datacenter": params.get("gce_datacenter") or "",
        "current_oci_region": init_aws_region_param(params["oci_region_name"]) if params.get("oci_region_name") else "",
    }
    script = re.sub(
        r"(?<!\\)\$\{([^{}]*)\}", lambda match: evaluate(match[1].strip(), params, pipeline_params, local_vars), block
    ).replace("\\$", "$")
    return env_from_bash(script, tmp_path)


def env_from_bash(script, tmp_path):
    env_file = tmp_path / "env"
    subprocess.run(
        ["bash", "-c", f"{script}\nenv -0 > {env_file}\n"],
        env={"PATH": os.environ["PATH"], "GIT_BRANCH": "origin/branch-2026.1"},
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    pairs = (item.split("=", 1) for item in env_file.read_text().split("\0") if item)
    return {name: value for name, value in pairs if name.startswith(("SCT_", *RENAMED))}


def all_params_of_run_sct_test():
    """Every parameter that runSctTest read, set, so every branch of its export block runs."""
    text = GROOVY_RUN_SCT_TEST.read_text()
    params = {name: f"v-{name}" for name in re.findall(r"\bparams\.(\w+)", text)}
    pipeline_params = {name: f"v-{name}" for name in re.findall(r"\bpipelineParams\.(\w+)", text)}
    params.update(backend="xcloud", stop_on_hw_perf_failure=True, update_db_packages="true", region="random")
    return params, pipeline_params


AWS_PARAMS = {
    "backend": "aws",
    "test_config": '["test-cases/longevity/longevity-100gb-4h.yaml", "configurations/arm.yaml"]',
    "region": '["eu-west-1", "us-east-1"]',
    "availability_zone": "a",
    "gce_datacenter": '["us-east1", "us-west1"]',
    "scylla_version": "2026.1.0",
    "provision_type": "spot",
    "stress_duration": 240,
    "k8s_enable_tls": False,
    "disable_raft": "",
    "update_db_packages": False,
    "email_recipients": "qa@scylladb.com",
    "requested_by_user": "someone",
    "pytest_addopts": "-k test_x",
}


@pytest.mark.parametrize(
    ("params", "pipeline_params"),
    [
        pytest.param(*all_params_of_run_sct_test(), id="every-param-set"),
        pytest.param(AWS_PARAMS, {}, id="aws-lists-and-falsy-values"),
        pytest.param({"backend": "gce", "test_config": "test-cases/a.yaml"}, {}, id="minimal"),
        pytest.param(
            {"backend": "k8s-eks", "test_config": "a.yaml", "k8s_version": "1.33", "oci_region_name": "random"},
            {"k8s_deploy_monitoring": True, "k8s_log_api_calls": False},
            id="k8s-pipeline-params",
        ),
    ],
)
def test_run_sct_test_env_keeps_the_values_of_its_former_groovy(params, pipeline_params, tmp_path):
    expected = {RENAMED.get(name, name): value for name, value in groovy_env(params, pipeline_params, tmp_path).items()}
    expected = {name: value for name, value in expected.items() if name not in MOVED}

    assert expected.items() <= env_of(params, pipeline_params).items()


# --- what moved into the configuration --------------------------------------------------------------


@pytest.mark.parametrize(
    ("git_branch", "desc"),
    [("origin/branch-2026.1", "2026-1"), ("origin/master", "master"), ("origin/manager-3.13", "manager-")],
)
def test_branch_desc(git_branch, desc):
    assert branch_desc(git_branch) == desc


@pytest.fixture
def docker_config(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA_DESC", raising=False)
    return monkeypatch


def test_config_takes_ami_desc_from_git_branch(docker_config):
    docker_config.setenv("GIT_BRANCH", "origin/branch-2026.1")

    assert sct_config.SCTConfiguration().get("ami_id_db_scylla_desc") == "2026-1"


def test_config_without_git_branch_has_no_ami_desc(docker_config):
    docker_config.delenv("GIT_BRANCH", raising=False)

    assert not sct_config.SCTConfiguration().get("ami_id_db_scylla_desc")


def test_config_turns_manager_off_with_unified_package(docker_config):
    docker_config.setenv("SCT_UNIFIED_PACKAGE", "https://example.com/scylla-unified.tar.gz")
    docker_config.setenv("SCT_USE_MGMT", "true")

    assert sct_config.SCTConfiguration().get("use_mgmt") is False


def test_username_from_requested_by_user_option(monkeypatch):
    monkeypatch.delenv("BUILD_USER_REQUESTED_BY", raising=False)
    monkeypatch.setenv("SCT_REQUESTED_BY_USER", "someone")

    assert get_username() == "someone"


# --- the command --------------------------------------------------------------------------------


@pytest.fixture
def params_file(tmp_path):
    path = tmp_path / "params.json"
    path.write_text(json.dumps({"params": AWS_PARAMS, "builder_region": BUILDER_REGION}))
    return path


def test_cli_json_output_file(params_file, tmp_path, monkeypatch):
    monkeypatch.delenv("SCT_CLUSTER_BACKEND", raising=False)
    output = tmp_path / "env.json"

    result = CliRunner().invoke(pipeline, ["env", "--params-file", str(params_file), "--json", "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert json.loads(output.read_text())["SCT_CLUSTER_BACKEND"] == "aws"


def test_cli_leaves_out_what_the_environment_has(params_file, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")

    result = CliRunner().invoke(pipeline, ["env", "--params-file", str(params_file), "--json"])

    assert result.exit_code == 0, result.output
    assert "SCT_CLUSTER_BACKEND" not in json.loads(result.output)


def test_cli_export_lines_survive_bash(params_file, tmp_path, monkeypatch):
    for name in [name for name in os.environ if name.startswith("SCT_")]:
        monkeypatch.delenv(name)

    result = CliRunner().invoke(pipeline, ["env", "--params-file", str(params_file)])

    assert result.exit_code == 0, result.output
    (tmp_path / "env.sh").write_text(result.output)
    assert env_from_bash(f"source {tmp_path / 'env.sh'}", tmp_path) == env_of(AWS_PARAMS)
