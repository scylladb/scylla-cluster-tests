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

from pathlib import Path

import pytest

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.groovy import _build_trigger_groovy, _escape_groovy_string
from sdcm.utils.trigger_matrix.parameters import build_job_parameters

TRIGGERS_DIR = Path(__file__).parents[3] / "configurations" / "triggers"


@pytest.fixture(autouse=True)
def _no_upstream_env(monkeypatch):
    """Default to "not running under Jenkins" so tests opt in to the upstream-cause branch.

    The unit tests themselves run as a Jenkins job, which sets JOB_NAME and BUILD_NUMBER, so
    without clearing them the remote-cause cases pass locally and fail on CI.
    """
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
        # order matters: escaping quotes first would double-escape the backslashes it introduces
        ("\\", "\\\\"),
        ("'", "\\'"),
        # a single-quoted Groovy literal cannot span lines
        ("A=1\nB=2", r"A=1\nB=2"),
        ("A=1\r\nB=2", r"A=1\r\nB=2"),
        # a backslash followed by an n is not a line break, and stays a backslash
        (r"A=1\nB=2", r"A=1\\nB=2"),
    ],
)
def test_escape_groovy_string(raw, expected):
    assert _escape_groovy_string(raw) == expected


@pytest.mark.parametrize(
    ("job", "params", "env", "kwargs", "included", "excluded"),
    [
        pytest.param(
            "folder/job'; println 'pwned",
            {},
            {},
            {},
            ["getItemByFullName('folder/job\\'; println \\'pwned')"],
            # no bare apostrophe survives to terminate the Groovy literal early
            ["job'; println"],
            id="job-name-quote-cannot-break-out",
        ),
        pytest.param(
            "job",
            {"desc": "it's fine"},
            {},
            {},
            [r"new StringParameterValue('desc', 'it\'s fine')"],
            [],
            id="parameter-value-quote-escaped",
        ),
        pytest.param(
            "job",
            {"a": "1", "b": "2"},
            {},
            {},
            ["def params = [new StringParameterValue('a', '1'), new StringParameterValue('b', '2')]"],
            [],
            id="parameters-in-order",
        ),
        pytest.param(
            "job",
            {"stress_duration": 1440, "enabled": True},
            {},
            {},
            ["new StringParameterValue('stress_duration', '1440')", "new StringParameterValue('enabled', 'True')"],
            [],
            id="non-string-values-stringified",
        ),
        pytest.param("job", {}, {}, {}, ["def params = []"], [], id="no-parameters"),
        pytest.param(
            "job",
            # `{branch}` placeholders reach Jenkins unexpanded when a matrix leaves them unresolved
            {"new_scylla_repo": "http://x/{branch_id}/scylla.repo"},
            {},
            {},
            ["'http://x/{branch_id}/scylla.repo'"],
            [],
            id="template-braces-verbatim",
        ),
        pytest.param(
            "job",
            {},
            {},
            {},
            ["hudson.model.Cause.RemoteCause('trigger-matrix'"],
            ["UpstreamCause"],
            id="remote-cause-outside-jenkins",
        ),
        pytest.param(
            "job",
            {},
            {"JOB_NAME": "trigger/tier1", "BUILD_NUMBER": "42"},
            {},
            [
                "getItemByFullName('trigger/tier1')",
                "getBuildByNumber(42)",
                "new hudson.model.Cause.UpstreamCause((hudson.model.Run) upstreamBuild)",
                # and a fallback for a build that has since been deleted
                "Build #42 (upstream build not found)",
            ],
            [],
            id="upstream-cause-under-jenkins",
        ),
        # both variables are required; one alone must not produce `getBuildByNumber()`
        pytest.param(
            "job",
            {},
            {"JOB_NAME": "something"},
            {},
            ["RemoteCause('trigger-matrix'"],
            ["UpstreamCause"],
            id="only-job-name-set",
        ),
        pytest.param(
            "job",
            {},
            {"BUILD_NUMBER": "something"},
            {},
            ["RemoteCause('trigger-matrix'"],
            ["UpstreamCause"],
            id="only-build-number-set",
        ),
        pytest.param(
            "job",
            {},
            {"JOB_NAME": "trigger/o'brien", "BUILD_NUMBER": "7"},
            {},
            [r"getItemByFullName('trigger/o\'brien')"],
            [],
            id="upstream-job-name-escaped",
        ),
        pytest.param(
            "job",
            {},
            {},
            {"return_queue_url": True},
            ["Jenkins.instance.queue.schedule2(targetJob", 'println "TRIGGERED:${rootUrl}queue/item/${queueItem.id}/"'],
            ["queue.getItems()", "getAbsoluteUrl"],
            id="queue-url-mode",
        ),
        pytest.param(
            "job",
            {},
            {},
            {"return_queue_url": False},
            ['println "TRIGGERED:${targetJob.getAbsoluteUrl()}"'],
            ["queue.getItems()"],
            id="job-url-mode",
        ),
        # `JenkinsClient._run_trigger_script` keys off the TRIGGERED:/ERROR: prefixes
        *(
            pytest.param(
                "folder/job",
                {"a": "1"},
                {},
                {"return_queue_url": return_queue_url},
                [
                    'println "ERROR: Job not found: folder/job"',
                    'println "ERROR: build was not queued for folder/job"',
                    "TRIGGERED:",
                ],
                [],
                id=f"reports-parsed-failures-queue-url-{return_queue_url}",
            )
            for return_queue_url in (False, True)
        ),
        pytest.param("job", {}, {}, {}, ["import jenkins.model.Jenkins", "import hudson.model.*"], [], id="imports"),
    ],
)
def test_trigger_script_content(monkeypatch, job, params, env, kwargs, included, excluded):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    script = _build_trigger_groovy(job, params, **kwargs)
    for text in included:
        assert text in script
    for text in excluded:
        assert text not in script


@pytest.mark.parametrize("return_queue_url", [False, True])
@pytest.mark.parametrize("params", [{}, {"a": "1"}])
def test_braces_are_balanced(return_queue_url, params):
    """Guards the f-string's doubled braces: an undoubled one unbalances the script."""
    script = _build_trigger_groovy("job", params, return_queue_url=return_queue_url)
    assert script.count("{") == script.count("}"), "unbalanced braces — check f-string escaping"
    assert "{{" not in script and "}}" not in script, "doubled braces leaked into the output"


@pytest.mark.parametrize("matrix_file", sorted(TRIGGERS_DIR.glob("*.yaml")), ids=lambda path: path.name)
def test_every_parameter_literal_stays_on_one_line(matrix_file):
    """No parameter of a real trigger matrix may break the script across lines.

    A raw line break inside a single-quoted literal fails the whole Script Console call with
    "expecting ''', found '\\n'", and the job is not triggered.
    """
    config = load_matrix_config(matrix_file)
    for job in config.jobs:
        params = build_job_parameters(job, config.defaults, "master", {"unified_package": "https://x/pkg.tar.gz"})
        script = _build_trigger_groovy(job.job_name, params)
        [params_line] = [line for line in script.splitlines() if "def params = [" in line]
        assert params_line.rstrip().endswith("]"), f"{job.job_name}: a parameter breaks the script across lines"
