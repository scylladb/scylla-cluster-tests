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

from sdcm.utils.trigger_matrix import JobConfig, build_job_parameters


def test_defaults_applied():
    job = JobConfig(job_name="test", backend="aws", region="eu-west-1")
    defaults = {"provision_type": "spot", "post_behavior_db_nodes": "destroy"}
    params = build_job_parameters(job, defaults, "master:latest", {})

    assert params["provision_type"] == "spot"
    assert params["scylla_version"] == "master:latest"
    assert params["region"] == "eu-west-1"


def test_job_params_override_defaults():
    job = JobConfig(job_name="test", backend="aws", region="eu-west-1", params={"provision_type": "on_demand"})
    defaults = {"provision_type": "spot"}
    params = build_job_parameters(job, defaults, "master:latest", {})

    assert params["provision_type"] == "on_demand"


def test_cli_overrides_take_precedence():
    job = JobConfig(job_name="test", backend="aws", region="eu-west-1", params={"provision_type": "on_demand"})
    defaults = {"provision_type": "spot"}
    overrides = {"provision_type": "spot_fleet", "stress_duration": "120"}
    params = build_job_parameters(job, defaults, "2025.4", overrides)

    assert params["provision_type"] == "spot_fleet"
    assert params["stress_duration"] == "120"
    assert params["scylla_version"] == "2025.4"


def test_version_always_included():
    job = JobConfig(job_name="test", backend="aws", region="")
    params = build_job_parameters(job, {}, "2024.2.5-0.20250221.cb9e2a54ae6d-1", {})
    assert params["scylla_version"] == "2024.2.5-0.20250221.cb9e2a54ae6d-1"


def test_none_overrides_ignored():
    job = JobConfig(job_name="test", backend="aws", region="eu-west-1")
    defaults = {"provision_type": "spot"}
    overrides = {"provision_type": None, "region": None}
    params = build_job_parameters(job, defaults, "master:latest", overrides)
    assert params["provision_type"] == "spot"
