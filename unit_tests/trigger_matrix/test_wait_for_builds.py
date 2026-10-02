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

"""Tests for JenkinsClient's wait-mode machinery.

Wait mode is what gating triggers rely on: the trigger job blocks until the downstream builds
finish, collects their artifacts and reports pass/fail. Every branch here ends in a BuildResult
that a human reads in an email, or that fails a gating pipeline, so getting `result` wrong is
expensive and invisible.

Real runs poll for hours, so the clock is faked: `sleep()` advances `time()` instead of waiting,
which makes the timeout branches deterministic rather than slow.
"""

import json
from unittest.mock import MagicMock, patch

import jenkins as jenkins_lib
import pytest

from sdcm.utils.trigger_matrix import JenkinsClient, JenkinsTriggerError
from sdcm.utils.trigger_matrix.models import _PendingWaitJob

URL = "https://jenkins.example.com"


class FakeClock:
    """A clock that only moves when the code under test sleeps."""

    def __init__(self, start: float = 1000.0):
        self.now = start
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture()
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr("sdcm.utils.trigger_matrix.jenkins_client.time", fake)
    return fake


@pytest.fixture()
def client():
    """A connected JenkinsClient whose Jenkins API is a MagicMock."""
    api = MagicMock()
    jenkins = JenkinsClient()
    with patch("sdcm.utils.trigger_matrix.jenkins_client._get_jenkins_client", return_value=(api, URL)):
        jenkins._connect()
    return jenkins, api


def _responses(*payloads):
    """Turn dicts into the JSON strings `jenkins_open` returns."""
    return [json.dumps(p) if isinstance(p, (dict, list)) else p for p in payloads]


def pending(name="tier1/longevity", queue_url=f"{URL}/queue/item/1/", collect=None, timeout=3600, fail_on_error=False):
    return _PendingWaitJob(
        job_name=name,
        queue_url=queue_url,
        collect_results=collect if collect is not None else [],
        timeout=timeout,
        fail_on_error=fail_on_error,
    )


# --------------------------------------------------------------------------------------
# _wait_for_build_start
# --------------------------------------------------------------------------------------


def test_build_start_returns_number_once_queue_item_is_executed(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({}, {"executable": {"number": 17}})
    assert jenkins._wait_for_build_start(f"{URL}/queue/item/1/") == 17


def test_build_start_polls_until_a_number_appears(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({}, {"executable": None}, {"executable": {"number": 3}})
    assert jenkins._wait_for_build_start(f"{URL}/queue/item/1/") == 3
    assert api.jenkins_open.call_count == 3
    assert clock.slept == [5, 5]


def test_build_start_raises_when_the_queue_item_is_cancelled(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({"cancelled": True})
    with pytest.raises(JenkinsTriggerError, match="cancelled"):
        jenkins._wait_for_build_start(f"{URL}/queue/item/1/")


def test_build_start_times_out(client, clock):
    jenkins, api = client
    api.jenkins_open.return_value = json.dumps({})
    with pytest.raises(JenkinsTriggerError, match="did not start within 20s"):
        jenkins._wait_for_build_start(f"{URL}/queue/item/1/", timeout=20)


def test_build_start_survives_a_transient_jenkins_error(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = [
        jenkins_lib.JenkinsException("502"),
        json.dumps({"executable": {"number": 9}}),
    ]
    assert jenkins._wait_for_build_start(f"{URL}/queue/item/1/") == 9


def test_build_start_strips_a_trailing_slash_once(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({"executable": {"number": 1}})
    jenkins._wait_for_build_start(f"{URL}/queue/item/1/")
    request = api.jenkins_open.call_args[0][0]
    assert request.url == f"{URL}/queue/item/1/api/json"


# --------------------------------------------------------------------------------------
# _collect_artifacts
# --------------------------------------------------------------------------------------


def test_no_patterns_skips_the_api_entirely(client):
    jenkins, api = client
    assert jenkins._collect_artifacts("job", 1, []) == []
    api.jenkins_open.assert_not_called()


def test_artifacts_are_matched_by_filename_glob(client):
    jenkins, api = client
    api.jenkins_open.return_value = json.dumps(
        {
            "artifacts": [
                {"relativePath": "results/report.html"},
                {"relativePath": "results/latency.json"},
                {"relativePath": "build.log"},
            ]
        }
    )
    assert jenkins._collect_artifacts("folder/job", 5, ["*.json"]) == [
        f"{URL}/job/folder/job/5/artifact/results/latency.json"
    ]


def test_glob_matches_the_basename_not_the_path(client):
    """Patterns are matched against the filename, so an exact name finds a nested artifact.

    `fnmatch` lets `*` span `/`, so a `*.log` pattern would pass either way -- an exact
    `build.log` is what actually distinguishes matching the basename from the whole path.
    """
    jenkins, api = client
    api.jenkins_open.return_value = json.dumps({"artifacts": [{"relativePath": "nested/dir/build.log"}]})
    assert jenkins._collect_artifacts("job", 1, ["build.log"]) == [f"{URL}/job/job/1/artifact/nested/dir/build.log"]


def test_multiple_patterns_and_no_match(client):
    jenkins, api = client
    api.jenkins_open.return_value = json.dumps({"artifacts": [{"relativePath": "a.txt"}, {"relativePath": "b.json"}]})
    assert len(jenkins._collect_artifacts("job", 1, ["*.json", "*.txt"])) == 2
    assert jenkins._collect_artifacts("job", 1, ["*.zip"]) == []


def test_empty_or_failing_artifact_response_is_not_fatal(client):
    jenkins, api = client
    api.jenkins_open.return_value = ""
    assert jenkins._collect_artifacts("job", 1, ["*.json"]) == []

    api.jenkins_open.side_effect = jenkins_lib.JenkinsException("boom")
    assert jenkins._collect_artifacts("job", 1, ["*.json"]) == []


# --------------------------------------------------------------------------------------
# wait_for_builds
# --------------------------------------------------------------------------------------


def test_dry_run_reports_synthetic_success_without_calling_jenkins():
    jenkins = JenkinsClient()
    results = jenkins.wait_for_builds([pending(name="a"), pending(name="b")], dry_run=True)
    assert [r.job_name for r in results] == ["a", "b"]
    assert all(r.success and r.build_number == 0 for r in results)


def test_no_pending_jobs_returns_empty():
    assert JenkinsClient().wait_for_builds([], dry_run=False) == []


def test_successful_build_is_reported_with_artifacts(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses(
        {"executable": {"number": 12}},  # queue poll
        {"building": False, "result": "SUCCESS"},  # build poll
        {"artifacts": [{"relativePath": "out/latency.json"}]},  # artifact fetch
    )
    (result,) = jenkins.wait_for_builds([pending(collect=["*.json"])], poll_interval=30)
    assert result.result == "SUCCESS"
    assert result.success
    assert result.build_number == 12
    assert result.build_url == f"{URL}/job/tier1/job/longevity/12"
    assert result.artifacts == [f"{URL}/job/tier1/job/longevity/12/artifact/out/latency.json"]


@pytest.mark.parametrize("verdict", ["FAILURE", "ABORTED", "UNSTABLE"])
def test_non_success_verdicts_are_passed_through(client, clock, verdict):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({"executable": {"number": 1}}, {"building": False, "result": verdict})
    (result,) = jenkins.wait_for_builds([pending()])
    assert result.result == verdict
    assert not result.success


def test_a_still_building_job_is_polled_again(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses(
        {"executable": {"number": 4}},
        {"building": True},
        {"building": True},
        {"building": False, "result": "SUCCESS"},
    )
    (result,) = jenkins.wait_for_builds([pending()], poll_interval=30)
    assert result.result == "SUCCESS"
    assert clock.slept == [30, 30]


def test_build_that_never_finishes_is_reported_as_timeout(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = [json.dumps({"executable": {"number": 8}})] + [json.dumps({"building": True})] * 50
    (result,) = jenkins.wait_for_builds([pending(timeout=90)], poll_interval=30)
    assert result.result == "TIMEOUT"
    assert result.build_number == 8
    assert result.build_url == f"{URL}/job/tier1/job/longevity/8"


def test_job_that_never_leaves_the_queue_is_reported_as_failure(client, clock):
    jenkins, api = client
    api.jenkins_open.return_value = json.dumps({})  # never gets an executable
    (result,) = jenkins.wait_for_builds([pending(timeout=20)])
    assert result.result == "FAILURE"
    assert result.build_number == 0
    assert result.build_url == f"{URL}/job/tier1/job/longevity"


def test_one_queue_failure_does_not_abandon_the_other_jobs(client, clock):
    """A job stuck in the queue must not stop the rest of the matrix being waited on."""
    jenkins, api = client

    def respond(request):
        if "queue/item/bad" in request.url:
            return json.dumps({})  # never starts -> times out
        if "queue/item/good" in request.url:
            return json.dumps({"executable": {"number": 2}})
        return json.dumps({"building": False, "result": "SUCCESS"})

    api.jenkins_open.side_effect = respond
    results = jenkins.wait_for_builds(
        [
            pending(name="bad", queue_url=f"{URL}/queue/item/bad/", timeout=20),
            pending(name="good", queue_url=f"{URL}/queue/item/good/"),
        ]
    )
    by_name = {r.job_name: r.result for r in results}
    assert by_name == {"bad": "FAILURE", "good": "SUCCESS"}


def test_polling_error_is_retried_rather_than_failing_the_build(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = [
        json.dumps({"executable": {"number": 6}}),
        jenkins_lib.JenkinsException("503"),
        json.dumps({"building": False, "result": "SUCCESS"}),
    ]
    (result,) = jenkins.wait_for_builds([pending()], poll_interval=30)
    assert result.result == "SUCCESS"


def test_payload_without_a_building_field_is_treated_as_still_running(client, clock):
    """`building` defaults to True: a truncated payload must not be read as "finished".

    Reporting a build as complete on a malformed response would invent a result for a job that
    is still going, and a gating pipeline would act on it.
    """
    jenkins, api = client
    api.jenkins_open.side_effect = _responses(
        {"executable": {"number": 2}},
        {},  # no "building" key at all
        {"building": False, "result": "SUCCESS"},
    )
    (result,) = jenkins.wait_for_builds([pending()], poll_interval=30)
    assert result.result == "SUCCESS"
    assert clock.slept == [30]


def test_missing_result_field_becomes_unknown(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({"executable": {"number": 1}}, {"building": False})
    (result,) = jenkins.wait_for_builds([pending()])
    assert result.result == "UNKNOWN"
    assert not result.success


def test_slash_in_job_name_becomes_a_jenkins_job_path(client, clock):
    jenkins, api = client
    api.jenkins_open.side_effect = _responses({"executable": {"number": 3}}, {"building": False, "result": "SUCCESS"})
    (result,) = jenkins.wait_for_builds([pending(name="scylla-2025.4/tier1/longevity")])
    assert result.build_url == f"{URL}/job/scylla-2025.4/job/tier1/job/longevity/3"
