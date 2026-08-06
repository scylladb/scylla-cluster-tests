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

import pytest
import yaml

from sdcm.utils import trigger_matrix
from sdcm.utils.trigger_matrix import JobConfig

# Version the stubbed cloud lookups report for every backend, so tests that don't care about
# version resolution get a deterministic full tag instead of reaching a cloud image API.
STUB_RESOLVED_VERSION = "2025.4.1-0.20250601.abc123def456-1"


@pytest.fixture(autouse=True)
def stub_image_lookups(monkeypatch):
    """Keep version resolution offline — no unit test may query a cloud image API.

    Tests that exercise resolution patch these same names themselves; `unittest.mock.patch`
    inside a test takes precedence over this fixture.
    """
    monkeypatch.setattr(
        trigger_matrix, "_resolve_version_via_branched_ami", lambda *args, **kwargs: STUB_RESOLVED_VERSION
    )
    monkeypatch.setattr(
        trigger_matrix, "_resolve_version_via_branched_gce_image", lambda *args, **kwargs: STUB_RESOLVED_VERSION
    )
    monkeypatch.setattr(
        trigger_matrix, "_resolve_version_via_branched_azure_image", lambda *args, **kwargs: STUB_RESOLVED_VERSION
    )
    monkeypatch.setattr(
        trigger_matrix, "_resolve_version_via_branched_oci_image", lambda *args, **kwargs: STUB_RESOLVED_VERSION
    )
    monkeypatch.setattr(trigger_matrix, "version_exists_for_backend", lambda *args, **kwargs: True)
    monkeypatch.setattr(trigger_matrix, "_version_exists_in_region", lambda *args, **kwargs: True)


@pytest.fixture()
def sample_matrix_yaml(tmp_path):
    """Create a sample YAML matrix file and return its path."""
    data = {
        "defaults": {
            "provision_type": "spot",
            "post_behavior_db_nodes": "destroy",
        },
        "cron_triggers": [
            {"schedule": "00 06 * * 6", "params": {"scylla_version": "master:latest"}},
        ],
        "jobs": [
            {
                "job_name": "tier1/longevity-50gb-3days-test",
                "backend": "aws",
                "labels": ["weekly"],
                "exclude_versions": [],
                "params": {"region": "eu-west-1", "stress_duration": "4320"},
            },
            {
                "job_name": "tier1/longevity-1tb-5days-azure-test",
                "backend": "azure",
                "labels": ["weekly"],
                "exclude_versions": ["2024.1"],
                "params": {"region": "eastus"},
            },
            {
                "job_name": "longevity/longevity-10gb-3h-gce-test",
                "backend": "gce",
                "labels": [],
                "exclude_versions": [],
                "params": {"region": "us-east1"},
            },
            {
                "job_name": "/scylla-enterprise/perf-regression/perf-test",
                "backend": "aws",
                "labels": ["master-weekly", "additional"],
                "exclude_versions": ["master"],
                "params": {"region": "us-east-1", "sub_tests": '["test_read"]'},
            },
        ],
    }
    path = tmp_path / "test-matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


@pytest.fixture()
def sample_jobs():
    """Return a list of sample JobConfig objects for filter tests."""
    return [
        JobConfig(
            job_name="job-a", backend="aws", labels=["weekly"], exclude_versions=[], params={"region": "eu-west-1"}
        ),
        JobConfig(
            job_name="job-b",
            backend="azure",
            labels=["weekly", "additional"],
            exclude_versions=["2024.1"],
            params={"region": "eastus"},
        ),
        JobConfig(job_name="job-c", backend="gce", labels=[], exclude_versions=[], params={"region": "us-east1"}),
        JobConfig(
            job_name="job-d",
            backend="aws",
            labels=["master-weekly"],
            exclude_versions=["master"],
            params={"region": "us-east-1"},
        ),
    ]


@pytest.fixture()
def sample_jobs_with_arm():
    """Return sample jobs including aarch64-labeled ones for architecture filter tests."""
    return [
        JobConfig(job_name="job-x86", backend="aws", labels=["weekly"], params={"region": "us-east-1"}),
        JobConfig(job_name="job-arm", backend="aws", labels=["aarch64"], params={"region": "us-east-1"}),
        JobConfig(
            job_name="job-arm-weekly", backend="aws", labels=["aarch64", "weekly"], params={"region": "us-east-1"}
        ),
        JobConfig(
            job_name="job-arm-no-label",
            backend="aws",
            arch="aarch64",
            labels=["weekly"],
            params={"region": "us-east-1"},
        ),
    ]
