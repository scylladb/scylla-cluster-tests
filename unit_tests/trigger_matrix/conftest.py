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

from sdcm.utils.trigger_matrix import JobConfig


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
                "region": "eu-west-1",
                "labels": ["weekly"],
                "exclude_versions": [],
                "params": {"stress_duration": "4320"},
            },
            {
                "job_name": "tier1/longevity-1tb-5days-azure-test",
                "backend": "azure",
                "region": "eastus",
                "labels": ["weekly"],
                "exclude_versions": ["2024.1"],
            },
            {
                "job_name": "longevity/longevity-10gb-3h-gce-test",
                "backend": "gce",
                "region": "us-east1",
                "labels": [],
                "exclude_versions": [],
            },
            {
                "job_name": "/scylla-enterprise/perf-regression/perf-test",
                "backend": "aws",
                "region": "us-east-1",
                "labels": ["master-weekly", "additional"],
                "exclude_versions": ["master"],
                "params": {"sub_tests": '["test_read"]'},
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
        JobConfig(job_name="job-a", backend="aws", region="eu-west-1", labels=["weekly"], exclude_versions=[]),
        JobConfig(
            job_name="job-b",
            backend="azure",
            region="eastus",
            labels=["weekly", "additional"],
            exclude_versions=["2024.1"],
        ),
        JobConfig(job_name="job-c", backend="gce", region="us-east1", labels=[], exclude_versions=[]),
        JobConfig(
            job_name="job-d", backend="aws", region="us-east-1", labels=["master-weekly"], exclude_versions=["master"]
        ),
    ]
