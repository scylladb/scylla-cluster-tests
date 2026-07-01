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

from sdcm.utils.trigger_matrix import filter_jobs, load_matrix_config

PERF_YAML = Path(__file__).parent.parent.parent / "configurations" / "triggers" / "perf-regression.yaml"


@pytest.fixture()
def perf_config():
    if not PERF_YAML.exists():
        pytest.skip("perf-regression.yaml not found")
    return load_matrix_config(PERF_YAML)


def test_master_weekly_selects_expected_jobs(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="master:latest",
        resolved_version="2026.3.0~dev-0.20260525.69a5b417d1dc",
        labels_selector="master-weekly",
    )
    assert len(result) == 5
    names = {j.job_name for j in result}
    assert any("i8g-tablets" in n for n in names)


def test_gce_custom_monthly_with_master_selects_latte(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="master:latest",
        resolved_version="2026.3.0~dev-0.20260525.69a5b417d1dc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 2
    assert all("latte" in j.job_name for j in result)


def test_gce_custom_monthly_non_rc_excluded(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="2025.1:latest",
        resolved_version="2025.1.3-0.20250525.abc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 0


def test_gce_custom_monthly_rc1_included(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="2025.1:latest",
        resolved_version="2025.1.3-rc1-0.20250525.abc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 2
