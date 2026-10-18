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
