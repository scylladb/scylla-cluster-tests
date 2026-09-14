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

"""Tests for the Groovy script sent to Jenkins' Script Console.

This script is what actually schedules every build, yet it is assembled as an f-string with
literal Groovy braces doubled and job names and parameter values interpolated into single-quoted
Groovy literals. Nothing downstream validates it: Jenkins either runs it or prints a stack trace
into the console log, long after the trigger job reported success.

The escaping in particular is load-bearing -- a matrix YAML is allowed to carry apostrophes and
backslashes in parameter values, and an unescaped one would silently change the script's meaning.
"""

import pytest

from sdcm.utils.trigger_matrix.groovy import _build_trigger_groovy, _escape_groovy_string


@pytest.fixture(autouse=True)
def _no_upstream_env(monkeypatch):
    """Default to "not running under Jenkins" so tests opt in to the upstream-cause branch."""
    monkeypatch.delenv("JOB_NAME", raising=False)
    monkeypatch.delenv("BUILD_NUMBER", raising=False)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("plain", "plain"),
        ("it's", r"it\'s"),
        (r"back\slash", r"back\\slash"),
        # a backslash already followed by a quote must not end up escaping the escape
        (r"mix\'ed", r"mix\\\'ed"),
        ("", ""),
    ],
)
def test_escape_groovy_string(raw, expected):
    assert _escape_groovy_string(raw) == expected


def test_backslash_is_escaped_before_quote():
    """Order matters: escaping quotes first would double-escape the backslashes it introduces."""
    assert _escape_groovy_string("\\") == "\\\\"
    assert _escape_groovy_string("'") == "\\'"


def test_job_name_quote_cannot_break_out_of_the_literal():
    script = _build_trigger_groovy("folder/job'; println 'pwned", {})
    assert "getItemByFullName('folder/job\\'; println \\'pwned')" in script
    # no bare apostrophe survives to terminate the Groovy literal early
    assert "job'; println" not in script


def test_parameter_value_quote_is_escaped():
    script = _build_trigger_groovy("job", {"desc": "it's fine"})
    assert r"new StringParameterValue('desc', 'it\'s fine')" in script


def test_parameters_render_in_order():
    script = _build_trigger_groovy("job", {"a": "1", "b": "2"})
    assert "def params = [new StringParameterValue('a', '1'), new StringParameterValue('b', '2')]" in script


def test_non_string_parameter_values_are_stringified():
    script = _build_trigger_groovy("job", {"stress_duration": 1440, "enabled": True})
    assert "new StringParameterValue('stress_duration', '1440')" in script
    assert "new StringParameterValue('enabled', 'True')" in script


def test_no_parameters_produces_empty_list():
    script = _build_trigger_groovy("job", {})
    assert "def params = []" in script


def test_template_braces_in_values_survive_verbatim():
    """`{branch}` placeholders reach Jenkins unexpanded when a matrix leaves them unresolved."""
    script = _build_trigger_groovy("job", {"new_scylla_repo": "http://x/{branch_id}/scylla.repo"})
    assert "'http://x/{branch_id}/scylla.repo'" in script


def test_remote_cause_when_not_running_under_jenkins():
    script = _build_trigger_groovy("job", {})
    assert "hudson.model.Cause.RemoteCause('trigger-matrix'" in script
    assert "UpstreamCause" not in script


def test_upstream_cause_when_jenkins_env_present(monkeypatch):
    monkeypatch.setenv("JOB_NAME", "trigger/tier1")
    monkeypatch.setenv("BUILD_NUMBER", "42")
    script = _build_trigger_groovy("job", {})
    assert "getItemByFullName('trigger/tier1')" in script
    assert "getBuildByNumber(42)" in script
    assert "new hudson.model.Cause.UpstreamCause((hudson.model.Run) upstreamBuild)" in script
    # and a fallback for a build that has since been deleted
    assert "Build #42 (upstream build not found)" in script


@pytest.mark.parametrize("present", ["JOB_NAME", "BUILD_NUMBER"])
def test_half_set_jenkins_env_falls_back_to_remote_cause(monkeypatch, present):
    """Both variables are required; one alone must not produce `getBuildByNumber()`."""
    monkeypatch.setenv(present, "something")
    script = _build_trigger_groovy("job", {})
    assert "RemoteCause('trigger-matrix'" in script
    assert "UpstreamCause" not in script


def test_upstream_job_name_is_escaped(monkeypatch):
    monkeypatch.setenv("JOB_NAME", "trigger/o'brien")
    monkeypatch.setenv("BUILD_NUMBER", "7")
    script = _build_trigger_groovy("job", {})
    assert r"getItemByFullName('trigger/o\'brien')" in script


def test_queue_url_mode_returns_queue_item():
    script = _build_trigger_groovy("job", {}, return_queue_url=True)
    assert "Jenkins.instance.queue.getItems().find" in script
    assert 'println "TRIGGERED:${rootUrl}queue/item/${queueItem.id}/"' in script


def test_default_mode_returns_job_url():
    script = _build_trigger_groovy("job", {}, return_queue_url=False)
    assert 'println "TRIGGERED:${targetJob.getAbsoluteUrl()}"' in script
    assert "queue.getItems()" not in script


@pytest.mark.parametrize("return_queue_url", [False, True])
def test_script_reports_the_two_failures_the_client_parses(return_queue_url):
    """`JenkinsClient._run_trigger_script` keys off the TRIGGERED:/ERROR: prefixes."""
    script = _build_trigger_groovy("folder/job", {"a": "1"}, return_queue_url=return_queue_url)
    assert 'println "ERROR: Job not found: folder/job"' in script
    assert 'println "ERROR: scheduleBuild2 returned null for folder/job"' in script
    assert "TRIGGERED:" in script


@pytest.mark.parametrize("return_queue_url", [False, True])
@pytest.mark.parametrize("params", [{}, {"a": "1"}])
def test_braces_are_balanced(return_queue_url, params):
    """Guards the f-string's doubled braces: an undoubled one unbalances the script."""
    script = _build_trigger_groovy("job", params, return_queue_url=return_queue_url)
    assert script.count("{") == script.count("}"), "unbalanced braces — check f-string escaping"
    assert "{{" not in script and "}}" not in script, "doubled braces leaked into the output"


def test_script_has_the_imports_it_needs():
    script = _build_trigger_groovy("job", {})
    assert "import jenkins.model.Jenkins" in script
    assert "import hudson.model.*" in script
