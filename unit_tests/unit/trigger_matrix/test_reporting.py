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

from sdcm.utils.trigger_matrix import reporting
from sdcm.utils.trigger_matrix.models import BuildResult
from sdcm.utils.trigger_matrix.reporting import send_trigger_matrix_email

TARBALL = "https://s3.amazonaws.com/bucket/sd/scylla-doctor-1.15.0.tar.gz"


def sent():
    return reporting.Email.return_value.send.call_args.kwargs


def test_subject_names_the_matrix_and_the_tested_build():
    send_trigger_matrix_email(
        build_results=[BuildResult(job_name="gating/job-1", build_number=3, result="SUCCESS")],
        scylla_version="2025.4.3, 2026.1.1",
        matrix_file="configurations/triggers/scylla-doctor-gating.yaml",
        run_params={"scylla_doctor_full_tarball_url": TARBALL, "provision_type": "spot", "region": None},
        subject_params=["scylla_doctor_full_tarball_url"],
    )
    assert sent()["subject"] == (
        "[Trigger Matrix] PASSED — scylla-doctor-gating: scylla-doctor-1.15.0.tar.gz, 2025.4.3, 2026.1.1"
    )
    assert f"<b>scylla_doctor_full_tarball_url:</b> {TARBALL}<br/>" in sent()["content"]
    assert "<b>provision_type:</b> spot<br/>" in sent()["content"]
    assert "region" not in sent()["content"]


def test_subject_without_run_params():
    send_trigger_matrix_email(
        build_results=[BuildResult(job_name="tier1/job", build_number=1, result="FAILURE")],
        scylla_version="2025.4.3",
        matrix_file="configurations/triggers/tier1.yaml",
    )
    assert sent()["subject"] == "[Trigger Matrix] FAILED — tier1: 2025.4.3"


def test_results_are_a_grid_of_jobs_by_version():
    url = "https://jenkins/job/a/7"
    send_trigger_matrix_email(
        build_results=[
            BuildResult(
                "gating/a",
                7,
                "SUCCESS",
                artifacts=[f"{url}/artifact/x/sd_analysis.json"],
                build_url=url,
                scylla_version="2025.4.3",
            ),
            BuildResult("gating/b", 8, "FAILURE", scylla_version="2025.4.3"),
            BuildResult("gating/a", 9, "UNSTABLE", scylla_version="2026.1.1"),
        ],
        scylla_version="2025.4.3, 2026.1.1",
        matrix_file="m.yaml",
    )
    content = sent()["content"]
    assert "<tr><th>Job</th><th>2025.4.3</th><th>2026.1.1</th></tr>" in content
    assert (
        f'<tr><td>gating/a</td><td><a href="{url}">✅ SUCCESS</a> #7'
        f'<br/><small><a href="{url}/artifact/x/sd_analysis.json">sd_analysis.json</a></small></td>'
        "<td>❌ UNSTABLE</td></tr>"
    ) in content
    assert "<tr><td>gating/b</td><td>❌ FAILURE</td><td>—</td></tr>" in content


def test_failures_section_says_why_each_build_failed():
    send_trigger_matrix_email(
        build_results=[
            BuildResult("gating/a", 7, "SUCCESS", scylla_version="2025.4.3"),
            BuildResult(
                "gating/b",
                8,
                "FAILURE",
                build_url="https://j/b/8",
                scylla_version="2025.4.3",
                description="analyzer &lt;X&gt; failed",
            ),
        ],
        scylla_version="2025.4.3",
        matrix_file="m.yaml",
    )
    assert (
        '<h3>Failures</h3>\n<ul><li><b>gating/b</b> (2025.4.3) — FAILURE — <a href="https://j/b/8">View build</a>'
        "<br/><small>analyzer &lt;X&gt; failed</small></li></ul>"
    ) in sent()["content"]
    assert "All builds PASSED" not in sent()["content"]


def test_all_passed_banner():
    send_trigger_matrix_email(
        build_results=[BuildResult("gating/a", 7, "SUCCESS")], scylla_version="2025.4.3", matrix_file="m.yaml"
    )
    assert "✅ All builds PASSED." in sent()["content"]
    assert "Failures" not in sent()["content"]


def test_failure_shows_the_links_of_an_html_description():
    description = """
        <div style="margin: 12px 4px;">
            <a
                href='https://argus.scylladb.com/tests/scylla-cluster-tests/1888'
            >
                Argus: <span style='font-weight: 500'>1888</span>
            </a>
        </div>
    """
    send_trigger_matrix_email(
        build_results=[BuildResult("gating/b", 8, "FAILURE", description=description)],
        scylla_version="2025.4.3",
        matrix_file="m.yaml",
    )
    assert (
        '<br/><small><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/1888">Argus: 1888</a></small>'
        in sent()["content"]
    )
    assert "&lt;div" not in sent()["content"]
