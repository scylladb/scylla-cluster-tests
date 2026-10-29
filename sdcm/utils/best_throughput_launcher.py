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

"""Turn a best-throughput request into a Jenkins job for its hardware configuration, and start it.

The `best-throughput-request` launcher job passes the request here. The request's identity parameters (backend,
DB instance type, node count, tablets, optional suffix) name a job in the launcher's folder; the first
request for a configuration creates that job from the generic jenkinsfile, later ones reuse it, so each
configuration keeps its own Argus history across Scylla versions. Everything else in the request becomes the
build's parameters: the final `test_config` and `extra_environment_variables` assembled here, plus the Scylla
version and the perf pipeline's other parameters.

A job starts only once Argus has a test for it. Argus creates tests by scanning Jenkins, and a run of a job it
hasn't scanned yet can't report results, so the first request for a configuration waits for the next scan.
"""

import json
import logging
import re
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Self

import requests

from sdcm.keystore import KeyStore
from sdcm.utils.aws_utils import aws_check_instance_type_supported
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.session import create_retry_session
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient
from sdcm.wait import WaitForTimeoutError, wait_for
from utils.build_system.create_test_release_jobs import JenkinsPipelines

LOGGER = logging.getLogger(__name__)

# Under a folder Argus scans for new jobs (scylla-enterprise), so every generated job gets an Argus test.
JOB_FOLDER = "scylla-enterprise/perf-regression/best-throughput"
GENERIC_JENKINSFILE = (
    "jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/best-throughput/"
    "best-throughput-generic.jenkinsfile"
)
# Argus scans Jenkins for new jobs every 5 minutes; this leaves room for a few slow or failed scans.
ARGUS_TEST_TIMEOUT = 1800

DEFAULT_TEST_CASE = "test-cases/performance/perf-regression-predefined-throughput-steps.yaml"
# Based on the test_config of the i8g tablets predefined-steps job. Its step grid is inert here: the search ignores it.
DEFAULT_TEST_CONFIG = (
    DEFAULT_TEST_CASE,
    "configurations/performance/cassandra_stress_gradual_load_steps_i8g.yaml",
    "configurations/c-s-driver-version-4.yaml",
    "configurations/disable_kms.yaml",
    "configurations/disable_speculative_retry.yaml",
)
TABLETS_DISABLED = "configurations/tablets_disabled.yaml"
BEST_THROUGHPUT_OVERLAY = "configurations/performance/best-throughput/best_throughput.yaml"
# They rewrite the stress commands of the default test-case, so they only go with it.
DEFAULT_TEST_CASE_OVERLAYS = (
    "configurations/performance/best-throughput/cassandra_stress_time_bound_write.yaml",
    "configurations/performance/best-throughput/cassandra_stress_sized_dataset.yaml",
)

ALL_SUB_TESTS = (
    "test_read_gradual_increase_load",
    "test_mixed_gradual_increase_load",
    "test_read_disk_only_gradual_increase_load",
    "test_write_gradual_increase_load",
)
SEARCH_WORKLOADS = ("read", "write", "mixed", "read_disk_only")

# Backends the launcher supports, with their default region and the SCT variables of their instance types.
BACKENDS = {
    "aws": {
        "region": "us-east-1",
        "instance_type_db": "SCT_INSTANCE_TYPE_DB",
        "instance_type_loader": "SCT_INSTANCE_TYPE_LOADER",
    },
}
# Scylla image parameters of the perf pipeline; a request must set at least one.
SCYLLA_IMAGE_PARAMS = (
    "scylla_version",
    "scylla_ami_id",
    "gce_image_db",
    "azure_image_db",
    "scylla_repo",
    "new_scylla_repo",
    "update_db_packages",
)
JOB_NAME_SUFFIX = re.compile(r"^[A-Za-z0-9._-]*$")


class BestThroughputRequestError(ValueError):
    pass


def parse_properties(text: str) -> dict[str, str]:
    """Parse `extra_environment_variables` the way the perf pipeline does (`readProperties`): KEY=VALUE lines.

    A variable with an empty value is a placeholder left unfilled (e.g. the launcher's default `SCT_INSTANCE_TYPE_DB=`)
    and is dropped.
    """
    variables = {}
    for line in (text or "").splitlines():
        if not (line := line.strip()) or line.startswith(("#", "!")):
            continue
        key, _, value = line.partition("=")
        if value := re.sub(r"^(['\"])(.*)\1$", r"\2", value.strip()):
            variables[key.strip()] = value
    return variables


@dataclass(frozen=True)
class BestThroughputRequest:
    # Job identity: forms the job name.
    backend: str = "aws"
    instance_type_db: str = ""
    n_db_nodes: int = 3
    tablets: bool = True
    job_name_suffix: str = ""
    # Setup.
    test_config: list[str] = field(default_factory=list)
    extra_environment_variables: str = ""
    region: str = ""
    instance_type_loader: str = ""
    n_loaders: int | None = None
    sub_tests: list[str] = field(default_factory=lambda: list(ALL_SUB_TESTS))
    provision_type: str = "on_demand"
    # Search and result.
    p99_limit_ms: float | None = None
    step_duration: str = ""
    start_threads_per_shard: int | None = None
    max_search_steps: int | None = None
    search_precision_pct: float | None = None
    overload_factors: str = ""
    # Per run only.
    scylla_version: str = ""
    scylla_ami_id: str = ""
    gce_image_db: str = ""
    azure_image_db: str = ""
    scylla_repo: str = ""
    new_scylla_repo: str = ""
    update_db_packages: str = ""
    requested_by_user: str = ""
    email_recipients: str = ""

    @classmethod
    def from_dict(cls, request: dict) -> Self:
        """Build a request from the launcher's parameters; blank strings mean "use the default"."""
        known = {request_field.name: request_field for request_field in fields(cls)}
        if unknown := set(request) - set(known):
            raise BestThroughputRequestError(f"Unknown request parameters: {sorted(unknown)}")
        values = {}
        for name, value in request.items():
            if isinstance(value, str):
                value = value.strip()  # noqa: PLW2901
                if not value:
                    continue
            values[name] = value
        try:
            for name in ("n_db_nodes", "n_loaders", "start_threads_per_shard", "max_search_steps"):
                if name in values:
                    values[name] = int(values[name])
            for name in ("p99_limit_ms", "search_precision_pct"):
                if name in values:
                    values[name] = float(values[name])
            if isinstance(values.get("tablets"), str):
                values["tablets"] = values["tablets"].lower() in ("true", "1", "yes")
            for name in ("test_config", "sub_tests"):
                if isinstance(values.get(name), str):
                    values[name] = json.loads(values[name])
        except (TypeError, ValueError) as exc:
            raise BestThroughputRequestError(f"Malformed request parameter: {exc}") from exc
        return cls(**values)

    @property
    def backend_settings(self) -> dict:
        return BACKENDS[self.backend]

    @property
    def job_name(self) -> str:
        name = f"{self.backend}-{self.instance_type_db}-{self.n_db_nodes}n-{'tablets' if self.tablets else 'vnodes'}"
        return f"{name}-{self.job_name_suffix}" if self.job_name_suffix else name

    def validate(self, sct_root: Path | None = None) -> None:
        """Reject a request the job could not run as named, before anything is created or provisioned."""
        errors = []
        if self.backend not in BACKENDS:
            errors.append(f"backend '{self.backend}' is not supported yet; supported: {sorted(BACKENDS)}")
        if not self.instance_type_db:
            errors.append("instance_type_db is required")
        if self.n_db_nodes < 1:
            errors.append(f"n_db_nodes should be positive, got {self.n_db_nodes}")
        if self.n_loaders is not None and self.n_loaders < 1:
            errors.append(f"n_loaders should be positive, got {self.n_loaders}")
        if not JOB_NAME_SUFFIX.match(self.job_name_suffix):
            errors.append(
                f"job_name_suffix may hold only letters, digits, '.', '_' and '-', got '{self.job_name_suffix}'"
            )
        if not any(getattr(self, param) for param in SCYLLA_IMAGE_PARAMS):
            errors.append(f"one of {list(SCYLLA_IMAGE_PARAMS)} is required")
        if unknown := set(self.sub_tests) - set(ALL_SUB_TESTS):
            errors.append(f"unknown sub_tests {sorted(unknown)}; available: {list(ALL_SUB_TESTS)}")
        if not self.sub_tests:
            errors.append("sub_tests should hold at least one sub-test")
        if len(self.sub_tests) != len(set(self.sub_tests)):
            errors.append(f"sub_tests lists a sub-test more than once: {self.sub_tests}")
        try:
            factors = self.parsed_overload_factors
            if self.overload_factors and (not factors or any(factor <= 1 for factor in factors)):
                raise ValueError
        except ValueError:
            errors.append(f"overload_factors should be comma-separated numbers above 1, got '{self.overload_factors}'")
        sct_root = sct_root or get_sct_root_path()
        if missing := [config for config in self.final_test_config() if not (sct_root / config).is_file()]:
            errors.append(f"test_config files not found: {missing}")
        if self.backend in BACKENDS:
            extra_variables = parse_properties(self.extra_environment_variables)
            for variable, value in self._identity_variables().items():
                if variable in extra_variables and extra_variables[variable] != value:
                    errors.append(
                        f"extra_environment_variables sets {variable}={extra_variables[variable]}, but the request's "
                        f"identity parameters say {value}: the job name would not describe what runs"
                    )
        if errors:
            raise BestThroughputRequestError("Invalid best-throughput request:\n  - " + "\n  - ".join(errors))

    def final_test_config(self) -> list[str]:
        """The request's configs (the predefined-steps base when empty), then the launcher's overlays."""
        launcher_overlays = {TABLETS_DISABLED, *DEFAULT_TEST_CASE_OVERLAYS, BEST_THROUGHPUT_OVERLAY}
        test_config = [config for config in self.test_config or DEFAULT_TEST_CONFIG if config not in launcher_overlays]
        if not self.tablets:
            test_config.append(TABLETS_DISABLED)
        if test_config and test_config[0] == DEFAULT_TEST_CASE:
            test_config.extend(DEFAULT_TEST_CASE_OVERLAYS)
        # Last, so no file in the request can turn the search or its validation settings off.
        test_config.append(BEST_THROUGHPUT_OVERLAY)
        return list(dict.fromkeys(test_config))

    @property
    def parsed_overload_factors(self) -> list[float]:
        return [float(factor) for factor in self.overload_factors.split(",") if factor.strip()]

    def _identity_variables(self) -> dict[str, str]:
        return {
            "SCT_CLUSTER_BACKEND": self.backend,
            self.backend_settings["instance_type_db"]: self.instance_type_db,
            "SCT_N_DB_NODES": str(self.n_db_nodes),
        }

    def launcher_variables(self) -> dict[str, str]:
        """The SCT variables the structured parameters translate to."""
        variables = {
            self.backend_settings["instance_type_db"]: self.instance_type_db,
            "SCT_N_DB_NODES": str(self.n_db_nodes),
        }
        # Loader settings only when requested, so a stress-tool file that picks its own loader keeps it.
        if self.instance_type_loader:
            variables[self.backend_settings["instance_type_loader"]] = self.instance_type_loader
        if self.n_loaders is not None:
            variables["SCT_N_LOADERS"] = str(self.n_loaders)
        if self.p99_limit_ms is not None:
            variables["SCT_PERF_GRADUAL_LATENCY_LIMIT"] = json.dumps({"default": {"percentile_99": self.p99_limit_ms}})
        if self.step_duration:
            variables["SCT_PERF_GRADUAL_STEP_DURATION"] = json.dumps(
                dict.fromkeys(SEARCH_WORKLOADS, self.step_duration)
            )
        search = {
            key: value
            for key, value in (
                ("start_threads_per_shard", self.start_threads_per_shard),
                ("max_steps", self.max_search_steps),
                ("precision_pct", self.search_precision_pct),
            )
            if value is not None
        }
        if search:
            variables["SCT_PERF_BEST_THROUGHPUT_SEARCH"] = json.dumps({"default": search})
        if self.overload_factors:
            variables["SCT_PERF_OVERLOAD_STEPS"] = json.dumps({"default": {"factors": self.parsed_overload_factors}})
        return variables

    def final_environment_variables(self) -> str:
        """The request's variables, then the launcher's; on a shared variable the structured parameter wins."""
        variables = parse_properties(self.extra_environment_variables) | self.launcher_variables()
        return "\n".join(f"{key}={value}" for key, value in variables.items())

    def build_parameters(self) -> dict[str, str]:
        """Parameters of the generated job's build, by their perf-pipeline names."""
        parameters = {
            "backend": self.backend,
            "region": self.region or self.backend_settings["region"],
            "provision_type": self.provision_type,
            "test_config": json.dumps(self.final_test_config()),
            "sub_tests": json.dumps(self.sub_tests),
            "extra_environment_variables": self.final_environment_variables(),
        }
        for param in (*SCYLLA_IMAGE_PARAMS, "requested_by_user", "email_recipients"):
            if value := getattr(self, param):
                parameters[param] = value
        return parameters

    def job_description(self) -> dict:
        """What the generated job's description records: the hardware configuration it stands for."""
        request = asdict(self)
        return {
            "best-throughput": {
                key: request[key] for key in ("backend", "instance_type_db", "n_db_nodes", "tablets", "job_name_suffix")
            }
        }


def wait_for_first_build(jenkins_client, job_path: str, timeout: int = 900) -> None:
    """Wait for build #1 of a new job: it only loads the job's parameters, and aborts itself."""
    wait_for(
        lambda: jenkins_client.get_job_info(job_path).get("lastCompletedBuild"),
        step=15,
        text=f"Waiting for the parameter-loading build #1 of {job_path}",
        timeout=timeout,
        throw_exc=True,
    )


def argus_test_exists(job_path: str) -> bool:
    """Whether Argus has a test for the job, which its runs need to report results."""
    creds = KeyStore().get_argus_rest_credentials_per_provider()
    response = create_retry_session().get(
        f"{creds['baseUrl']}/api/v1/planning/search",
        params={"query": job_path.rsplit("/", 1)[-1], "limit": 100},
        headers={
            "Authorization": f"token {creds['token']}",
            "Accept": "application/json",
            **(creds.get("extra_headers") or {}),
        },
        timeout=60,
    )
    response.raise_for_status()
    hits = response.json()["response"]["hits"]
    return any(hit.get("type") == "test" and hit.get("build_system_id") == job_path for hit in hits)


def wait_for_argus_test(job_path: str, timeout: int = ARGUS_TEST_TIMEOUT) -> None:
    """Wait until Argus has a test for the job: a run started before that would report nothing."""
    last_error = []

    def test_exists() -> bool:
        # A failed lookup is another poll without the test, so an Argus outage gets the same wait as a slow scan.
        try:
            return argus_test_exists(job_path)
        except requests.RequestException as exc:
            LOGGER.warning("Looking up the Argus test of %s failed: %s", job_path, exc)
            last_error[:] = [exc]
            return False

    try:
        wait_for(
            test_exists,
            step=60,
            text=f"Waiting for Argus to import {job_path} as a test",
            timeout=timeout,
            throw_exc=True,
        )
    except WaitForTimeoutError as exc:
        lookup_error = f" The last lookup failed: {last_error[0]}." if last_error else ""
        raise BestThroughputRequestError(
            f"Argus has no test for {job_path} after {timeout // 60} minutes, so its run could not report results."
            f"{lookup_error} Check that Argus scans this Jenkins folder (JENKINS_MONITORED_RELEASES), then rerun the "
            "request: the job is already created and will be reused"
        ) from exc


def launch(
    request: BestThroughputRequest,
    sct_branch: str,
    sct_repo: str,
    dry_run: bool = False,
    job_folder: str = JOB_FOLDER,
) -> str:
    """Create the request's job in `job_folder` if it is missing, start it with the request's parameters, return its path."""
    request.validate()
    job_path = f"{job_folder}/{request.job_name}"
    parameters = request.build_parameters()
    LOGGER.info("Best-throughput job: %s", job_path)
    LOGGER.info("Build parameters:\n%s", json.dumps(parameters, indent=2))
    # Before the dry-run exit: a dry run that passes should mean the real run gets this far.
    if request.backend == "aws":
        region = parameters["region"]
        if not aws_check_instance_type_supported(request.instance_type_db, region_name=region):
            raise BestThroughputRequestError(f"Instance type {request.instance_type_db} is not offered in {region}")
    if dry_run:
        LOGGER.info("[DRY-RUN] Not creating or starting %s", job_path)
        return job_path

    pipelines = JenkinsPipelines(base_job_dir="", sct_branch_name=sct_branch, sct_repo=sct_repo)
    if pipelines.jenkins.job_exists(job_path):
        LOGGER.info("Reusing %s", job_path)
    else:
        LOGGER.info("Creating %s", job_path)
        pipelines.create_pipeline_job(
            jenkins_file=get_sct_root_path() / GENERIC_JENKINSFILE,
            group_name=job_folder,
            job_name=request.job_name,
            job_name_suffix="",
            defines=request.job_description(),
        )
        if not pipelines.jenkins.job_exists(job_path):
            raise BestThroughputRequestError(f"Jenkins did not create {job_path}; see the log above")
        wait_for_first_build(pipelines.jenkins, job_path)
    # Also for a reused job: a request that timed out here left a job that Argus may still not know.
    wait_for_argus_test(job_path)

    if not JenkinsClient().trigger(job_path, parameters):
        raise BestThroughputRequestError(f"Jenkins did not accept the build of {job_path}")
    return job_path
