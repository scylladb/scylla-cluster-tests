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

"""Tests for job selection and parameter building against a synthetic matrix.

These use an in-memory matrix (not configurations/triggers/perf-regression.yaml) so they
exercise filter_jobs()/build_job_parameters() logic and stay green when the real matrix
changes — see PR #15380 discussion. The retirement guards at the end of the module are the
exception: they pin the production perf-regression.yaml via the `production_perf_config` fixture
and must be updated together with an intentional change to that file.
"""

from pathlib import Path

import pytest
import yaml

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.filters import filter_jobs
from sdcm.utils.trigger_matrix.parameters import build_job_parameters

PERF_YAML = Path(__file__).parents[3] / "configurations" / "triggers" / "perf-regression.yaml"


def write_matrix(tmp_path, data) -> Path:
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


@pytest.fixture()
def perf_config(tmp_path):
    path = write_matrix(
        tmp_path,
        {
            "jobs": [
                {
                    "job_name": "aws-perf-i8g-tablets-master",
                    "backend": "aws",
                    "labels": ["master-2weeks"],
                },
                {
                    "job_name": "gce-perf-latte-1",
                    "backend": "gce",
                    "labels": ["gce-custom-monthly"],
                    "pre_release": ["rc1"],
                },
                {
                    "job_name": "gce-perf-latte-2",
                    "backend": "gce",
                    "labels": ["gce-custom-monthly"],
                    "pre_release": ["rc1"],
                },
                {
                    "job_name": "aws-rolling-upgrade",
                    "backend": "aws",
                    "params": {
                        "rolling_upgrade_test": "true",
                        "new_scylla_repo": "http://downloads.scylladb.com/deb/scylla/{branch_id}/deb/"
                        "unstable/scylladb-{branch}/scylla.list",
                    },
                },
                {
                    "job_name": "gce-rolling-upgrade",
                    "backend": "gce",
                    "params": {
                        "rolling_upgrade_test": "true",
                        "new_scylla_repo": "http://downloads.scylladb.com/deb/scylla/{branch_id}/deb/"
                        "unstable/scylladb-{branch}/scylla.list",
                    },
                },
            ]
        },
    )
    return load_matrix_config(path)


@pytest.fixture()
def production_perf_config():
    return load_matrix_config(PERF_YAML)


def test_master_2weeks_selects_expected_jobs(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="master:latest",
        resolved_version="2026.3.0~dev-0.20260525.69a5b417d1dc",
        labels_selector="master-2weeks",
    )
    assert len(result) == 1
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
        scylla_version="2026.1:latest",
        resolved_version="2026.1.3-rc1-0.20260525.abc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 2


RETIRED_X86_JOBS = [
    pytest.param("scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes", id="throughput-vnodes"),
    pytest.param(
        "scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes", id="throughput-write-vnodes"
    ),
    pytest.param("scylla-enterprise-perf-regression-latency-650gb-with-nemesis", id="latency-nemesis"),
    pytest.param("scylla-enterprise-perf-regression-predefined-throughput-steps-tablets", id="throughput-tablets"),
    pytest.param(
        "scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets", id="throughput-write-tablets"
    ),
    pytest.param(
        "scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-tablets", id="rolling-upgrade-tablets"
    ),
    pytest.param("scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets", id="latency-nemesis-tablets"),
]

RETIRED_X86_JOB_NAMES = [param.values[0] for param in RETIRED_X86_JOBS]

# Every 2024.x / 2025.x minor the trigger can be fired for, GA and rc alike.
RETIRED_VERSIONS = [
    pytest.param("2024.1:latest", "2024.1.15-0.20250115.abc", id="2024.1"),
    pytest.param("2024.2:latest", "2024.2.11-0.20250320.abc", id="2024.2"),
    pytest.param("2025.1:latest", "2025.1.3-0.20250525.abc", id="2025.1"),
    pytest.param("2025.1:latest", "2025.1.3-rc1-0.20250525.abc", id="2025.1-rc1"),
    pytest.param("2025.2:latest", "2025.2.2-0.20250825.abc", id="2025.2"),
    pytest.param("2025.3:latest", "2025.3.5-0.20251025.abc", id="2025.3"),
    pytest.param("2025.4:latest", "2025.4.1-0.20260125.abc", id="2025.4"),
    pytest.param("2025.4:latest", "2025.4.1-rc1-0.20260125.abc", id="2025.4-rc1"),
]

LIVE_VERSIONS = [
    pytest.param("2026.1:latest", "2026.1.2-0.20260525.abc", id="2026.1"),
    pytest.param("master:latest", "2026.3.0~dev-0.20260525.abc", id="master"),
]


@pytest.mark.parametrize("job_name", RETIRED_X86_JOBS)
def test_retired_x86_jobs_stay_disabled(production_perf_config, job_name):
    """The x86 (i4i/i3en) release variants are retired and kept in the matrix for reference
    only, so they must stay `disabled: true`. Emptying their `include_versions` instead does
    the opposite of switching them off: filter_jobs() treats an empty list as no filter, so
    the job would run for every version, master included.
    """
    entries = [job for job in production_perf_config.jobs if job.job_name.rsplit("/", 1)[-1] == job_name]
    assert entries, f"{job_name} not found in perf-regression.yaml"
    for job in entries:
        assert job.disabled, f"{job_name} is retired but not disabled"


@pytest.mark.parametrize("scylla_version,resolved_version", RETIRED_VERSIONS + LIVE_VERSIONS)
def test_retired_x86_jobs_never_selected(production_perf_config, scylla_version, resolved_version):
    """No version may select a retired x86 variant, on any labels selector."""
    for labels_selector in ("", "gce-custom-monthly", "master-monthly", "master-2weeks", "master-3weeks"):
        selected = {
            job.job_name.rsplit("/", 1)[-1]
            for job in filter_jobs(
                production_perf_config.jobs,
                scylla_version=scylla_version,
                resolved_version=resolved_version,
                labels_selector=labels_selector,
            )
        }
        retired = selected.intersection(RETIRED_X86_JOB_NAMES)
        assert not retired, f"{scylla_version} (labels_selector={labels_selector!r}) selects retired jobs {retired}"


@pytest.mark.parametrize("scylla_version,resolved_version", RETIRED_VERSIONS)
def test_2024_and_2025_releases_keep_microbenchmarks_only(production_perf_config, scylla_version, resolved_version):
    """No 2024.x or 2025.x minor runs a perf regression job any more: the x86 variants are
    retired and every version filter names all six of those branches, which leaves the weekly
    microbenchmarks as the only jobs such a release trigger selects.
    """
    for labels_selector in ("", "gce-custom-monthly", "master-monthly", "master-2weeks", "master-3weeks"):
        selected = [
            job.job_name.rsplit("/", 1)[-1]
            for job in filter_jobs(
                production_perf_config.jobs,
                scylla_version=scylla_version,
                resolved_version=resolved_version,
                labels_selector=labels_selector,
            )
        ]
        non_microbenchmark = [name for name in selected if "microbenchmark" not in name]
        assert not non_microbenchmark, (
            f"{scylla_version} (labels_selector={labels_selector!r}) also selects {non_microbenchmark}"
        )


MICROBENCHMARK_VARIANTS = ("x86_64", "x86_64-write", "arm64", "arm64-write")


def microbenchmark_jobs(tool: str) -> set[str]:
    return {f"scylla-enterprise-{tool}-weekly-microbenchmark_{variant}" for variant in MICROBENCHMARK_VARIANTS}


@pytest.fixture()
def production_pgo_config():
    return load_matrix_config(PERF_YAML.parent / "pgo-offline-installer.yaml")


@pytest.mark.parametrize(
    "scylla_version,resolved_version,cql_raw,alternator",
    [
        *(pytest.param(*version.values, False, False, id=version.id) for version in RETIRED_VERSIONS),
        pytest.param("2026.1:latest", "2026.1.2-0.20260525.abc", False, False, id="2026.1"),
        pytest.param("2025.1.16", "2025.1.16", False, False, id="2025.1.16-promote-release"),
        pytest.param("2026.2:latest", "2026.2.1-0.20261025.abc", True, False, id="2026.2"),
        pytest.param("2026.2.12", "2026.2.12", True, False, id="2026.2.12-promote-release"),
        pytest.param("2026.3:latest", "2026.3.1-0.20261025.abc", True, False, id="2026.3"),
        pytest.param("2026.3.4", "2026.3.4", True, False, id="2026.3.4-promote-release"),
        pytest.param("2026.4:latest", "2026.4.1-0.20261025.abc", True, True, id="2026.4"),
        pytest.param("2026.4.0-rc2", "2026.4.0-rc2", True, True, id="2026.4.0-rc2-promote-release"),
        pytest.param("2026.4.2", "2026.4.2", True, True, id="2026.4.2-promote-release"),
        pytest.param("2027.1.0-rc2", "2027.1.0-rc2", True, True, id="next-release"),
        pytest.param("master:latest", "2027.1.0~dev-0.20261025.abc", True, True, id="master-manual-run"),
    ],
)
def test_releases_that_select_the_in_process_server_microbenchmarks(
    production_perf_config, scylla_version, resolved_version, cql_raw, alternator
):
    """`scylla perf-cql-raw` does not exist before 2026.2, and perf-alternator runs from 2026.4, as
    only the numbers of new releases are worth tracking. promote-release sends a full version and no
    labels selector. No production firing sends master with no labels selector: the master case is
    a manual run.
    """
    selected = {
        job.job_name.rsplit("/", 1)[-1]
        for job in filter_jobs(
            production_perf_config.jobs, scylla_version=scylla_version, resolved_version=resolved_version
        )
    }
    for tool, expected in (("perf-cql-raw", cql_raw), ("perf-alternator", alternator)):
        jobs = microbenchmark_jobs(tool)
        assert selected & jobs == (jobs if expected else set()), tool


@pytest.mark.parametrize(
    "config_fixture,fields_that_differ",
    [
        pytest.param("production_perf_config", {"job_name", "exclude_versions"}, id="perf-regression"),
        pytest.param("production_pgo_config", {"job_name"}, id="pgo"),
    ],
)
@pytest.mark.parametrize("variant", MICROBENCHMARK_VARIANTS)
def test_perf_alternator_entry_equals_the_cql_raw_entry(request, config_fixture, fields_that_differ, variant):
    """The two tools need the same arch, labels, wait mode and params (region, the AMI of the right
    arch, unified_package), so an entry of one is a copy of the entry of the other.
    """
    jobs = {job.job_name.rsplit("/", 1)[-1]: job for job in request.getfixturevalue(config_fixture).jobs}
    alternator, cql_raw = (
        jobs[f"scylla-enterprise-{tool}-weekly-microbenchmark_{variant}"].model_dump(exclude=fields_that_differ)
        for tool in ("perf-alternator", "perf-cql-raw")
    )
    assert alternator == cql_raw
    assert (alternator["arch"] == "aarch64") == variant.startswith("arm64")


@pytest.mark.parametrize("labels_selector,arch_suffix", [("pgo-x86", "x86_64"), ("pgo-arm", "arm64")])
def test_pgo_trigger_selects_perf_alternator(production_pgo_config, labels_selector, arch_suffix):
    """A PGO build runs the perf-alternator microbenchmarks of its architecture, as it runs cql-raw."""
    selected = {
        job.job_name.rsplit("/", 1)[-1]
        for job in filter_jobs(production_pgo_config.jobs, scylla_version="master", labels_selector=labels_selector)
    }
    alternator = {name for name in selected if "perf-alternator" in name}
    assert alternator == {
        name for name in microbenchmark_jobs("perf-alternator") if name.endswith((arch_suffix, f"{arch_suffix}-write"))
    }


def test_master_keeps_the_vnodes_jobs(production_perf_config):
    """Vnodes coverage is kept on master: the monthly i8g vnodes entries must stay selectable
    even though no release branch runs a vnodes job any more.
    """
    selected = {
        job.job_name.rsplit("/", 1)[-1]
        for job in filter_jobs(
            production_perf_config.jobs,
            scylla_version="master:latest",
            resolved_version="2026.3.0~dev-0.20260525.abc",
            labels_selector="master-monthly",
        )
    }
    assert "scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-vnodes" in selected
    assert "scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-vnodes" in selected


def test_rolling_upgrade_jobs_resolve_new_scylla_repo(perf_config):
    """SCT-782: rolling_upgrade_test jobs must get scylla_version cleared and a fully
    resolved new_scylla_repo from build_job_parameters(), with the directory segment
    branch-prefixed (e.g. 'branch-2025.1' / 'master') and the filename segment bare
    (e.g. 'scylladb-2025.1' / 'scylladb-master').
    """
    rolling_upgrade_jobs = [
        job for job in perf_config.jobs if str(job.params.get("rolling_upgrade_test", "")).lower() == "true"
    ]
    assert len(rolling_upgrade_jobs) == 2, (
        f"Expected 2 rolling_upgrade_test jobs in the synthetic matrix, found {len(rolling_upgrade_jobs)}"
    )

    for job in rolling_upgrade_jobs:
        params = build_job_parameters(job, perf_config.defaults, "2025.1:latest", {})
        assert params["scylla_version"] == "", f"Job {job.job_name} should have blank scylla_version"
        new_scylla_repo = params.get("new_scylla_repo", "")
        assert new_scylla_repo, f"Job {job.job_name} missing new_scylla_repo"
        assert "{branch}" not in new_scylla_repo, (
            f"Job {job.job_name} has unresolved {{branch}} placeholder in new_scylla_repo: {new_scylla_repo}"
        )
        assert "{branch_id}" not in new_scylla_repo, (
            f"Job {job.job_name} has unresolved {{branch_id}} placeholder in new_scylla_repo: {new_scylla_repo}"
        )
        assert "/branch-2025.1/deb/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo directory segment must be branch-prefixed "
            f"('branch-2025.1'): {new_scylla_repo}"
        )
        assert "/scylladb-2025.1/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo filename segment must stay bare ('scylladb-2025.1'): {new_scylla_repo}"
        )

    for job in rolling_upgrade_jobs:
        params = build_job_parameters(job, perf_config.defaults, "master:latest", {})
        new_scylla_repo = params.get("new_scylla_repo", "")
        assert "/scylla/master/deb/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo directory segment for master must stay "
            f"unprefixed ('master', not 'branch-master'): {new_scylla_repo}"
        )
        assert "/scylladb-master/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo filename segment for master must be "
            f"'scylladb-master': {new_scylla_repo}"
        )
