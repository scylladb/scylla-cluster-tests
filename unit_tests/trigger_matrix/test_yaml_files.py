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

from sdcm.utils.trigger_matrix import load_matrix_config

TRIGGERS_DIR = Path(__file__).parent.parent.parent / "configurations" / "triggers"


@pytest.mark.parametrize(
    "filename,min_jobs",
    [
        pytest.param("tier1.yaml", 10, id="tier1"),
        pytest.param("sanity.yaml", 9, id="sanity"),
        pytest.param("perf-regression.yaml", 20, id="perf-regression"),
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
    path = Path(__file__).parent.parent.parent / filename
    if not path.exists():
        pytest.skip(f"{filename} not found")
    config = load_matrix_config(path)
    assert len(config.jobs) > 0


def test_scylla_doctor_gating_has_wait_true():
    path = Path(__file__).parent.parent.parent / "configurations/triggers/scylla-doctor-gating.yaml"
    if not path.exists():
        pytest.skip("scylla-doctor-gating.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is True for job in config.jobs)
    assert all(job.fail_on_error is True for job in config.jobs)
    assert all(len(job.collect_results) > 0 for job in config.jobs)


def test_pgo_has_wait_false():
    path = Path(__file__).parent.parent.parent / "configurations/triggers/pgo-offline-installer.yaml"
    if not path.exists():
        pytest.skip("pgo-offline-installer.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is False for job in config.jobs)
