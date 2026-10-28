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

from pathlib import Path

import pytest
import yaml

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.models import job_arch

PROJECT_ROOT = Path(__file__).parents[3]
TRIGGERS_DIR = PROJECT_ROOT / "configurations" / "triggers"


@pytest.mark.parametrize(
    "filename,min_jobs",
    [
        pytest.param("tier1.yaml", 10, id="tier1"),
        pytest.param("sanity.yaml", 9, id="sanity"),
        pytest.param("perf-regression.yaml", 20, id="perf-regression"),
        pytest.param("scylla-doctor-gating.yaml", 4, id="scylla-doctor-gating"),
    ],
)
def test_yaml_loads_and_has_expected_job_count(filename, min_jobs):
    path = TRIGGERS_DIR / filename
    if not path.exists():
        pytest.skip(f"{filename} not found")
    config = load_matrix_config(path)
    assert len(config.jobs) >= min_jobs, f"{filename} has {len(config.jobs)} jobs, expected at least {min_jobs}"


@pytest.mark.parametrize("filename", ["tier1.yaml", "sanity.yaml", "perf-regression.yaml"])
def test_yaml_jobs_have_required_fields(filename):
    path = TRIGGERS_DIR / filename
    if not path.exists():
        pytest.skip(f"{filename} not found")
    config = load_matrix_config(path)
    for job in config.jobs:
        assert job.job_name, f"Job missing job_name in {filename}"
        assert job.backend in ("aws", "gce", "azure", "docker", "oci"), (
            f"Invalid backend '{job.backend}' for job '{job.job_name}' in {filename}"
        )


@pytest.mark.parametrize(
    "filename",
    [
        "configurations/triggers/pgo-offline-installer.yaml",
        "configurations/triggers/scylla-doctor-gating.yaml",
    ],
)
def test_new_yaml_loads_successfully(filename):
    path = PROJECT_ROOT / filename
    if not path.exists():
        pytest.skip(f"{filename} not found")
    config = load_matrix_config(path)
    assert len(config.jobs) > 0


def test_scylla_doctor_gating_defaults():
    path = PROJECT_ROOT / "configurations/triggers/scylla-doctor-gating.yaml"
    if not path.exists():
        pytest.skip("scylla-doctor-gating.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is True for job in config.jobs)
    assert all(job.fail_on_error is True for job in config.jobs)
    assert all(len(job.collect_results) > 0 for job in config.jobs)
    assert config.defaults["run_scylla_doctor_only"] == "true"
    assert config.defaults["email_recipients"] == ""
    assert config.defaults["requested_by_user"] == "scylla-doctor"
    assert config.supported_versions_only is True


def test_scylla_doctor_gating_runs_every_sd_release_gating_os_but_docker():
    config = load_matrix_config(PROJECT_ROOT / "configurations/triggers/scylla-doctor-gating.yaml")
    enabled = {job.job_name.rsplit("/", 1)[-1] for job in config.jobs if not job.disabled}
    disabled = {job.job_name.rsplit("/", 1)[-1] for job in config.jobs if job.disabled}
    assert enabled == {
        f"artifacts-{os_name}-test"
        for os_name in (
            "centos9",
            "rocky9",
            "ubuntu2404",
            "debian12",
            "ami",
            "amazon2023-arm",
            "ubuntu2204",
            "rhel10",
            "amazon2023",
            "centos9-arm",
            "ubuntu2204-arm",
            "ubuntu2404-arm",
            "ami-arm",
        )
    }
    assert disabled == {"artifacts-docker-test"}


def test_scylla_doctor_gating_jobs_are_defined_in_the_scylla_doctor_folder():
    symlinks = yaml.safe_load(
        (PROJECT_ROOT / "jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml").read_text()
    )
    defined = {f"/scylla-doctor/oss/artifacts/{link['name']}-test" for link in symlinks["symlinks"]}
    config = load_matrix_config(PROJECT_ROOT / "configurations/triggers/scylla-doctor-gating.yaml")
    assert {job.job_name for job in config.jobs} <= defined


def test_scylla_doctor_gating_arm_jobs_run_on_aarch64():
    config = load_matrix_config(PROJECT_ROOT / "configurations/triggers/scylla-doctor-gating.yaml")
    for job in config.jobs:
        expected = "aarch64" if job.job_name.endswith("-arm-test") else "x86_64"
        assert job_arch(job) == expected, job.job_name


def test_scylla_doctor_gating_jobs_have_their_os_label():
    config = load_matrix_config(PROJECT_ROOT / "configurations/triggers/scylla-doctor-gating.yaml")
    for job in config.jobs:
        os_name = job.job_name.removeprefix("/scylla-doctor/oss/artifacts/artifacts-").removesuffix("-test")
        assert [label for label in job.labels if label.startswith("os-")] == [f"os-{os_name}"], job.job_name


def test_pgo_has_wait_false():
    path = PROJECT_ROOT / "configurations/triggers/pgo-offline-installer.yaml"
    if not path.exists():
        pytest.skip("pgo-offline-installer.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is False for job in config.jobs)
