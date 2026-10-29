"""Unit tests for the best-throughput launcher: request handling, job naming, configs and Jenkins calls."""

import json
import re
from dataclasses import fields
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

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
from sdcm.wait import WaitForTimeoutError
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.thread_search import ThreadSearchConfig

LAUNCHER_GROOVY = get_sct_root_path() / "vars/bestThroughputRequestPipeline.groovy"
GENERIC_JENKINSFILE = get_sct_root_path() / launcher.GENERIC_JENKINSFILE
LWT_TEST_CASE = "test-cases/performance/perf-regression-predefined-throughput-steps-lwt-light.yaml"
JOB_PATH = f"{launcher.JOB_FOLDER}/aws-i8g.4xlarge-3n-tablets"


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


def test_empty_environment_variables_are_ignored():
    request = _request(extra_environment_variables="SCT_INSTANCE_TYPE_DB=\nSCT_C_S_DRIVER_VERSION=''")
    request.validate()
    assert parse_properties(request.final_environment_variables()) == {
        "SCT_INSTANCE_TYPE_DB": "i8g.4xlarge",
        "SCT_N_DB_NODES": "3",
        "SCT_TEST_DURATION": "780",
    }


def test_environment_variables_minimal_request():
    assert parse_properties(_request().final_environment_variables()) == {
        "SCT_INSTANCE_TYPE_DB": "i8g.4xlarge",
        "SCT_N_DB_NODES": "3",
        "SCT_TEST_DURATION": "780",
    }


@pytest.mark.parametrize(
    "request_params, expected_minutes",
    [
        # 180 for setup, preload and teardown, then (13 search steps: 1 thread to the 4096 cap, + 2 reruns)
        # x (30-minute steps + 10 minutes of waits).
        pytest.param({}, 180 + 15 * 40, id="defaults"),
        pytest.param({"max_search_steps": "20"}, 180 + 22 * 40, id="budget_above_the_doubling_steps"),
        pytest.param({"step_duration": "1h"}, 180 + 15 * 70, id="longer_steps"),
        pytest.param({"overload_factors": "1.3,2"}, 180 + 17 * 40, id="overload_steps"),
        pytest.param(
            {
                "extra_environment_variables": 'SCT_PERF_BEST_THROUGHPUT_SEARCH={"read": {"max_threads_per_command": 512}}'
            },
            180 + 15 * 40,
            id="the_longest_workload_counts",
        ),
        pytest.param(
            {
                "sub_tests": '["test_read_gradual_increase_load"]',
                "extra_environment_variables": 'SCT_PERF_BEST_THROUGHPUT_SEARCH={"read": {"max_threads_per_command": 512}}',
            },
            180 + 12 * 40,
            id="lower_cap",
        ),
    ],
)
def test_test_duration_covers_the_longest_search(request_params, expected_minutes):
    variables = parse_properties(_request(**request_params).final_environment_variables())
    assert int(variables["SCT_TEST_DURATION"]) == expected_minutes


def test_requested_test_duration_is_kept():
    variables = parse_properties(
        _request(extra_environment_variables="SCT_TEST_DURATION=2000").final_environment_variables()
    )
    assert variables["SCT_TEST_DURATION"] == "2000"


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


@pytest.mark.parametrize(
    "request_params, expected_throttling, expected_category",
    [
        # The perf pipeline then uses its region's category, SCT-perf-<region>, which allows one build at a time.
        pytest.param({}, True, None, id="default_one_sub_test_at_a_time"),
        # Not passed at all: the job's default, false. A string "False" would read as true in the pipeline.
        pytest.param({"use_job_throttling": "false"}, None, None, id="parallel"),
        pytest.param(
            {"job_throttle_category": "SCT-perf-us-east-1-i8g"}, True, "SCT-perf-us-east-1-i8g", id="category"
        ),
    ],
)
def test_sub_tests_are_throttled(request_params, expected_throttling, expected_category):
    parameters = _request(**request_params).build_parameters()
    assert parameters.get("use_job_throttling") is expected_throttling
    assert parameters.get("job_throttle_category") == expected_category


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
        pytest.param({"overload_factors": ","}, "overload_factors should be", id="overload_empty_list"),
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
    argus_test_exists = MagicMock(return_value=True)
    monkeypatch.setattr(launcher, "argus_test_exists", argus_test_exists)
    monkeypatch.setattr(launcher, "wait_for", _fake_wait_for)
    return SimpleJenkins(pipelines=pipelines, client=client, created=created, argus_test_exists=argus_test_exists)


def _fake_wait_for(func, timeout, **_):
    """wait_for without the sleeping: up to 3 polls, then the timeout."""
    for _ in range(3):
        if result := func():
            return result
    raise WaitForTimeoutError(f"timeout - {timeout} seconds - expired")


class SimpleJenkins:
    def __init__(self, pipelines, client, created, argus_test_exists):
        self.pipelines, self.client, self.created = pipelines, client, created
        self.argus_test_exists = argus_test_exists


def test_first_request_creates_the_job_then_starts_it(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = False
    request = _request()

    job_path = JOB_PATH
    assert launcher.launch(request, sct_branch="branch-perf-v17", sct_repo="repo") == job_path

    (created,) = jenkins.created
    assert created["group_name"] == launcher.JOB_FOLDER
    assert created["job_name"] == "aws-i8g.4xlarge-3n-tablets"
    assert created["job_name_suffix"] == ""
    assert Path(created["jenkins_file"]) == GENERIC_JENKINSFILE
    jenkins.pipelines.jenkins.get_job_info.assert_called_with(job_path)
    jenkins.client.trigger.assert_called_once_with(job_path, request.build_parameters())


def test_next_request_reuses_the_job(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = True
    request = _request()

    launcher.launch(request, sct_branch="branch-perf-v17", sct_repo="repo")

    assert not jenkins.created
    jenkins.client.trigger.assert_called_once_with(JOB_PATH, request.build_parameters())


def test_job_is_created_in_the_launcher_folder(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = False
    request = _request()
    folder = "scylla-staging/yulia/performance/Best-Throughput"

    job_path = launcher.launch(request, sct_branch="feature", sct_repo="fork", job_folder=folder)

    assert job_path == f"{folder}/aws-i8g.4xlarge-3n-tablets"
    assert jenkins.created[0]["group_name"] == folder
    jenkins.client.trigger.assert_called_once_with(job_path, request.build_parameters())


def test_dry_run_does_not_touch_jenkins(jenkins):
    launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo", dry_run=True)
    assert not jenkins.created
    jenkins.client.trigger.assert_not_called()
    jenkins.argus_test_exists.assert_not_called()


def test_default_job_folder_is_scanned_by_argus():
    """Argus imports jobs as tests only from monitored folders, such as scylla-enterprise and its subfolders."""
    assert launcher.JOB_FOLDER.split("/")[0] == "scylla-enterprise"
    assert GENERIC_JENKINSFILE.is_file()
    assert GENERIC_JENKINSFILE.parent.relative_to(
        get_sct_root_path() / "jenkins-pipelines/performance/branch-perf-v17"
    ) == Path(launcher.JOB_FOLDER)


def test_new_job_starts_only_once_argus_has_its_test(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = False
    calls = []

    def argus_test_exists(job_path):
        calls.append(("argus", job_path))
        return len(calls) == 2  # imported by the second poll

    jenkins.argus_test_exists.side_effect = argus_test_exists
    jenkins.client.trigger.side_effect = lambda *args: calls.append(("trigger", args[0])) or True

    launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo")

    assert calls == [("argus", JOB_PATH), ("argus", JOB_PATH), ("trigger", JOB_PATH)]


@pytest.mark.parametrize("job_exists", [False, True], ids=["new_job", "reused_job"])
def test_job_without_argus_test_is_not_started(jenkins, job_exists):
    jenkins.pipelines.jenkins.job_exists.return_value = job_exists
    jenkins.argus_test_exists.return_value = False

    with pytest.raises(BestThroughputRequestError, match="Argus has no test for .* after 30 minutes"):
        launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo")
    jenkins.client.trigger.assert_not_called()


def test_failed_argus_lookups_are_polled_again(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = True
    jenkins.argus_test_exists.side_effect = [requests.HTTPError("502 Bad Gateway"), True]

    launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo")

    jenkins.client.trigger.assert_called_once()


def test_argus_lookup_error_is_named_on_timeout(jenkins):
    jenkins.pipelines.jenkins.job_exists.return_value = True
    jenkins.argus_test_exists.side_effect = requests.HTTPError("401 Unauthorized")

    with pytest.raises(BestThroughputRequestError, match="The last lookup failed: 401 Unauthorized"):
        launcher.launch(_request(), sct_branch="branch-perf-v17", sct_repo="repo")
    jenkins.client.trigger.assert_not_called()


@pytest.mark.parametrize(
    "hits, expected",
    [
        pytest.param([{"type": "test", "build_system_id": JOB_PATH}], True, id="imported"),
        pytest.param([], False, id="not_imported"),
        pytest.param(
            [{"type": "test", "build_system_id": "scylla-staging/x/aws-i8g.4xlarge-3n-tablets"}],
            False,
            id="same_name_in_another_folder",
        ),
        pytest.param([{"type": "group", "build_system_id": JOB_PATH}], False, id="not_a_test"),
    ],
)
def test_argus_test_exists(monkeypatch, hits, expected):
    keystore = MagicMock()
    keystore.get_argus_rest_credentials_per_provider.return_value = {"baseUrl": "https://argus", "token": "t"}
    monkeypatch.setattr(launcher, "KeyStore", MagicMock(return_value=keystore))
    response = MagicMock()
    response.json.return_value = {"status": "ok", "response": {"hits": hits, "total": len(hits)}}
    session = MagicMock()
    get = session.get
    get.return_value = response
    monkeypatch.setattr(launcher, "create_retry_session", MagicMock(return_value=session))

    assert launcher.argus_test_exists(JOB_PATH) is expected
    assert get.call_args.args[0] == "https://argus/api/v1/planning/search"
    assert get.call_args.kwargs["params"]["query"] == "aws-i8g.4xlarge-3n-tablets"


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


def test_launcher_default_test_config_is_the_python_default():
    groovy = LAUNCHER_GROOVY.read_text(encoding="utf-8")
    default = json.loads(re.search(r"name: 'test_config', defaultValue: '''(.*?)'''", groovy).group(1))
    assert default == list(DEFAULT_TEST_CONFIG)
    assert not [path for path in default if not (get_sct_root_path() / path).is_file()]


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
