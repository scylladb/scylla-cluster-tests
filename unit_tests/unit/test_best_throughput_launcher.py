"""Unit tests for the best-throughput launcher: request handling, job naming, configs and Jenkins calls."""

import json
import re
from dataclasses import fields
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sdcm.utils import best_throughput_launcher as launcher
from sdcm.utils.best_throughput_launcher import (
    BEST_THROUGHPUT_OVERLAY,
    DEFAULT_TEST_CASE_OVERLAYS,
    DEFAULT_TEST_CONFIG,
    TABLETS_DISABLED,
    BestThroughputRequest,
    BestThroughputRequestError,
    parse_properties,
)
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.thread_search import ThreadSearchConfig

LAUNCHER_GROOVY = get_sct_root_path() / "vars/bestThroughputRequestPipeline.groovy"
GENERIC_JENKINSFILE = get_sct_root_path() / launcher.GENERIC_JENKINSFILE
LWT_TEST_CASE = "test-cases/performance/perf-regression-predefined-throughput-steps-lwt-light.yaml"


def _request(**overrides) -> BestThroughputRequest:
    return BestThroughputRequest.from_dict(
        {"backend": "aws", "instance_type_db": "i8g.4xlarge", "scylla_version": "2026.2.0"} | overrides
    )


@pytest.mark.parametrize(
    "overrides, expected",
    [
        pytest.param({}, "aws-i8g.4xlarge-3n-tablets", id="defaults"),
        pytest.param({"n_db_nodes": "6", "tablets": False}, "aws-i8g.4xlarge-6n-vnodes", id="vnodes_6_nodes"),
        pytest.param({"job_name_suffix": "lcs"}, "aws-i8g.4xlarge-3n-tablets-lcs", id="suffix"),
    ],
)
def test_job_name(overrides, expected):
    request = _request(**overrides)
    assert request.job_name == expected
    assert request.job_path == f"best-throughput/{expected}"


def test_scylla_version_is_not_part_of_the_job_name():
    assert _request(scylla_version="2026.1.0").job_name == _request(scylla_version="master:latest").job_name


def test_launcher_strings_are_parsed():
    request = _request(
        n_db_nodes="4",
        tablets="false",
        n_loaders=" ",
        p99_limit_ms="12.5",
        test_config='["configurations/disable_kms.yaml"]',
        sub_tests='["test_read_gradual_increase_load"]',
    )
    assert (request.n_db_nodes, request.tablets, request.n_loaders, request.p99_limit_ms) == (4, False, None, 12.5)
    assert request.test_config == ["configurations/disable_kms.yaml"]
    assert request.sub_tests == ["test_read_gradual_increase_load"]


@pytest.mark.parametrize(
    "raw, match",
    [
        pytest.param({"instance_type": "i8g.4xlarge"}, "Unknown request parameters", id="unknown_parameter"),
        pytest.param({"n_db_nodes": "three"}, "Malformed request parameter", id="not_a_number"),
        pytest.param({"test_config": "[not json"}, "Malformed request parameter", id="bad_json"),
    ],
)
def test_malformed_request(raw, match):
    with pytest.raises(BestThroughputRequestError, match=match):
        BestThroughputRequest.from_dict({"backend": "aws", "instance_type_db": "i8g.4xlarge"} | raw)


def test_default_test_config_gets_every_overlay():
    assert _request().final_test_config() == [
        *DEFAULT_TEST_CONFIG,
        *DEFAULT_TEST_CASE_OVERLAYS,
        BEST_THROUGHPUT_OVERLAY,
    ]


def test_vnodes_and_request_overlays():
    request = _request(tablets=False, test_config=[*DEFAULT_TEST_CONFIG, "configurations/c-s-driver-version-4.yaml"])
    test_config = request.final_test_config()
    assert test_config.index("configurations/c-s-driver-version-4.yaml") < test_config.index(TABLETS_DISABLED)
    assert test_config[-1] == BEST_THROUGHPUT_OVERLAY


def test_other_test_case_gets_no_cassandra_stress_overlays():
    test_config = _request(test_config=[LWT_TEST_CASE]).final_test_config()
    assert test_config == [LWT_TEST_CASE, BEST_THROUGHPUT_OVERLAY]


def test_overlay_listed_in_the_request_still_comes_last():
    test_config = _request(
        test_config=[*DEFAULT_TEST_CONFIG, BEST_THROUGHPUT_OVERLAY, TABLETS_DISABLED]
    ).final_test_config()
    assert test_config[-1] == BEST_THROUGHPUT_OVERLAY
    assert test_config.count(BEST_THROUGHPUT_OVERLAY) == 1
    # The tablets parameter, part of the job name, decides vnodes; a listed tablets_disabled.yaml doesn't.
    assert TABLETS_DISABLED not in test_config


def test_parse_properties():
    text = "# comment\nSCT_A=1\n\nSCT_B = 'quoted value'\nSCT_C={\"x\": 1}\n"
    assert parse_properties(text) == {"SCT_A": "1", "SCT_B": "quoted value", "SCT_C": '{"x": 1}'}


def test_environment_variables_minimal_request():
    assert parse_properties(_request().final_environment_variables()) == {
        "SCT_INSTANCE_TYPE_DB": "i8g.4xlarge",
        "SCT_N_DB_NODES": "3",
    }


def test_environment_variables_full_request():
    request = _request(
        extra_environment_variables="SCT_C_S_DRIVER_VERSION=4\nSCT_N_LOADERS=2",
        instance_type_loader="c7i.16xlarge",
        n_loaders="6",
        p99_limit_ms="15",
        step_duration="20m",
        start_threads_per_shard="8",
        max_search_steps="10",
    )
    variables = parse_properties(request.final_environment_variables())

    assert variables["SCT_C_S_DRIVER_VERSION"] == "4"
    assert variables["SCT_INSTANCE_TYPE_LOADER"] == "c7i.16xlarge"
    assert variables["SCT_N_LOADERS"] == "6", "the structured parameter wins over extra_environment_variables"
    assert json.loads(variables["SCT_PERF_GRADUAL_LATENCY_LIMIT"]) == {"default": {"percentile_99": 15.0}}
    assert json.loads(variables["SCT_PERF_GRADUAL_STEP_DURATION"]) == dict.fromkeys(
        ("read", "write", "mixed", "read_disk_only"), "20m"
    )
    assert json.loads(variables["SCT_PERF_BEST_THROUGHPUT_SEARCH"]) == {
        "default": {"start_threads_per_shard": 8, "max_steps": 10}
    }


def test_build_parameters():
    parameters = _request(region="us-west-2", requested_by_user="someone").build_parameters()
    assert parameters["backend"] == "aws"
    assert parameters["region"] == "us-west-2"
    assert parameters["scylla_version"] == "2026.2.0"
    assert parameters["requested_by_user"] == "someone"
    assert json.loads(parameters["test_config"])[-1] == BEST_THROUGHPUT_OVERLAY
    assert len(json.loads(parameters["sub_tests"])) == 4
    assert "scylla_ami_id" not in parameters, "unset image parameters are not passed"


def test_region_defaults_to_the_backend_region():
    assert _request().build_parameters()["region"] == "us-east-1"


def test_valid_request():
    _request().validate()
    _request(
        scylla_version="", scylla_ami_id="ami-0123", extra_environment_variables="SCT_INSTANCE_TYPE_DB=i8g.4xlarge"
    ).validate()


@pytest.mark.parametrize(
    "overrides, match",
    [
        pytest.param({"backend": "gce"}, "backend 'gce' is not supported yet", id="backend"),
        pytest.param({"instance_type_db": ""}, "instance_type_db is required", id="no_instance_type"),
        pytest.param({"scylla_version": ""}, "one of .*scylla_version.* is required", id="no_scylla_image"),
        pytest.param({"n_db_nodes": "0"}, "n_db_nodes should be positive", id="no_nodes"),
        pytest.param({"job_name_suffix": "my run"}, "job_name_suffix may hold only", id="bad_suffix"),
        pytest.param({"sub_tests": '["test_scan"]'}, "unknown sub_tests", id="unknown_sub_test"),
        pytest.param({"overload_factors": "1.3,x"}, "overload_factors should be", id="overload_not_numbers"),
        pytest.param({"overload_factors": "0.5"}, "overload_factors should be", id="overload_below_1"),
        pytest.param(
            {"sub_tests": '["test_read_gradual_increase_load", "test_read_gradual_increase_load"]'},
            "more than once",
            id="duplicate_sub_test",
        ),
        pytest.param(
            {"test_config": '["configurations/missing.yaml"]'}, "test_config files not found", id="missing_file"
        ),
        pytest.param(
            {"extra_environment_variables": "SCT_INSTANCE_TYPE_DB=i4i.4xlarge"},
            "the job name would not describe what runs",
            id="conflicting_instance_type",
        ),
        pytest.param(
            {"extra_environment_variables": "SCT_N_DB_NODES=6"},
            "the job name would not describe what runs",
            id="conflicting_node_count",
        ),
    ],
)
def test_invalid_request(overrides, match):
    with pytest.raises(BestThroughputRequestError, match=match):
        _request(**overrides).validate()


@pytest.fixture
def jenkins(monkeypatch):
    """Mock Jenkins: job creation through JenkinsPipelines, builds through the trigger-matrix client."""
    pipelines = MagicMock()
    created = []

    def create_pipeline_job(**kwargs):
        created.append(kwargs)
        pipelines.jenkins.job_exists.return_value = True

    pipelines.create_pipeline_job.side_effect = create_pipeline_job
    pipelines.jenkins.get_job_info.return_value = {"lastCompletedBuild": {"number": 1}}
    client = MagicMock()
    client.trigger.return_value = True
    monkeypatch.setattr(launcher, "JenkinsPipelines", MagicMock(return_value=pipelines))
    monkeypatch.setattr(launcher, "JenkinsClient", MagicMock(return_value=client))
    monkeypatch.setattr(launcher, "aws_check_instance_type_supported", MagicMock(return_value=True))
    return SimpleJenkins(pipelines=pipelines, client=client, created=created)


class SimpleJenkins:
    def __init__(self, pipelines, client, created):
        self.pipelines, self.client, self.created = pipelines, client, created


def test_first_request_creates_the_job_then_starts_it(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = False
    request = _request()

    assert launcher.launch(request, sct_branch="branch-perf-v17", sct_repo="repo") == request.job_path

    (created,) = jenkins.created
    assert created["group_name"] == "best-throughput"
    assert created["job_name"] == "aws-i8g.4xlarge-3n-tablets"
    assert created["job_name_suffix"] == ""
    assert Path(created["jenkins_file"]) == GENERIC_JENKINSFILE
    jenkins.pipelines.jenkins.get_job_info.assert_called_with(request.job_path)
    jenkins.client.trigger.assert_called_once_with(request.job_path, request.build_parameters())


def test_next_request_reuses_the_job(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = True
    request = _request()

    launcher.launch(request, sct_branch="branch-perf-v17", sct_repo="repo")

    assert not jenkins.created
    jenkins.client.trigger.assert_called_once_with(request.job_path, request.build_parameters())


def test_dry_run_does_not_touch_jenkins(jenkins):
    launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo", dry_run=True)
    assert not jenkins.created
    jenkins.client.trigger.assert_not_called()


@pytest.mark.parametrize("dry_run", [False, True])
def test_instance_type_not_offered_in_the_region(jenkins, monkeypatch, dry_run):
    monkeypatch.setattr(launcher, "aws_check_instance_type_supported", MagicMock(return_value=False))
    with pytest.raises(BestThroughputRequestError, match="not offered in us-east-1"):
        launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo", dry_run=dry_run)
    jenkins.client.trigger.assert_not_called()


def test_rejected_build_fails_the_launcher(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = True
    jenkins.client.trigger.return_value = False
    with pytest.raises(BestThroughputRequestError, match="did not accept the build"):
        launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo")


def test_launcher_groovy_passes_every_request_field():
    groovy = LAUNCHER_GROOVY.read_text(encoding="utf-8")
    request_names = re.search(r"def requestNames = \[(.*?)\]", groovy, re.DOTALL).group(1)
    assert set(re.findall(r"'(\w+)'", request_names)) == {
        request_field.name for request_field in fields(BestThroughputRequest)
    }


def test_every_config_file_listed_in_the_launcher_exists():
    groovy = LAUNCHER_GROOVY.read_text(encoding="utf-8")
    listed = set(re.findall(r"(?:configurations|test-cases)/[\w./-]+\.yaml", groovy))
    assert len(listed) > 20
    assert not [path for path in sorted(listed) if not (get_sct_root_path() / path).is_file()]


def test_generic_job_defaults_match_the_launcher_default_request():
    jenkinsfile = GENERIC_JENKINSFILE.read_text(encoding="utf-8")
    default_test_config = json.loads(re.search(r"test_config: '''(.*?)'''", jenkinsfile).group(1))
    assert default_test_config == _request().final_test_config()


def test_sct_accepts_what_the_launcher_builds(monkeypatch):
    request = _request(
        tablets=False,
        extra_environment_variables="SCT_C_S_DRIVER_VERSION=4",
        p99_limit_ms="15",
        step_duration="20m",
        start_threads_per_shard="8",
    )
    parameters = request.build_parameters()
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_CONFIG_FILES", parameters["test_config"])
    for key, value in parse_properties(parameters["extra_environment_variables"]).items():
        monkeypatch.setenv(key, value)

    config = SCTConfiguration()
    config.verify_configuration()

    assert config.get("instance_type_db") == "i8g.4xlarge"
    assert config.get("append_scylla_yaml")["enable_tablets"] is False
    assert config.get("perf_gradual_latency_limit") == {"default": {"percentile_99": 15.0}}
    assert config.get("perf_gradual_step_duration")["write"] == "20m"
    assert (
        ThreadSearchConfig.from_params(config.get("perf_best_throughput_search"), "read").start_threads_per_shard == 8
    )
    assert config.get("perf_dataset_sizing") is not None


def test_search_precision_reaches_sct_under_its_config_key():
    variables = parse_properties(_request(search_precision_pct="2.5").final_environment_variables())
    assert json.loads(variables["SCT_PERF_BEST_THROUGHPUT_SEARCH"]) == {"default": {"precision_pct": 2.5}}


def test_overload_factors_reach_sct():
    variables = parse_properties(_request(overload_factors="1.3, 2").final_environment_variables())
    assert json.loads(variables["SCT_PERF_OVERLOAD_STEPS"]) == {"default": {"factors": [1.3, 2.0]}}
    assert "SCT_PERF_OVERLOAD_STEPS" not in parse_properties(_request().final_environment_variables())
