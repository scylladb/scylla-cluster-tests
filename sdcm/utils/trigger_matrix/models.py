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

"""Data models for the trigger matrix.

The pydantic models describing a matrix YAML, plus the small records the trigger and wait
phases pass around.

`JOB_LEVEL_KEYS` and `MATRIX_LEVEL_KEYS` are derived from `model_fields` at import time, so
they have to stay in this module, below the models they describe."""

import logging
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sdcm.utils.trigger_matrix.constants import (
    DEFAULT_ARCH,
    DEFAULT_VERSION_RESOLUTION,
    WAIT_TIMEOUT,
    VersionResolution,
)

logger = logging.getLogger(__name__)


class CronTriggerConfig(BaseModel):
    """Configuration for a cron-based trigger schedule."""

    model_config = ConfigDict(extra="forbid")

    schedule: str
    params: dict = Field(default_factory=dict)


class JenkinsfileEntry(BaseModel):
    """A Jenkinsfile generated from this matrix by generate_trigger_jenkinsfiles.py."""

    model_config = ConfigDict(extra="forbid")

    path: str
    labels_selector: str = ""


class JobConfig(BaseModel):
    """Configuration for a single Jenkins job in the trigger matrix.

    The fields here are *structural* — they decide whether and how the job is triggered.
    Everything the Jenkins job itself receives (region, availability_zone,
    instance types, sub_tests, ...) belongs under `params`.
    """

    model_config = ConfigDict(extra="forbid")

    job_name: str
    backend: Literal["aws", "gce", "azure", "docker", "oci"]
    # CPU architecture the job's DB nodes run on — images are published per architecture,
    # so ARM jobs must declare it to have their version resolved against ARM images.
    # Left empty it is inferred from the job's labels — see job_arch().
    arch: str = ""

    @field_validator("arch")
    @classmethod
    def validate_arch(cls, value: str) -> str:
        if value and value not in ("aarch64", "x86_64"):
            raise ValueError(f"arch must be 'aarch64', 'x86_64', or empty; got '{value}'")
        return value

    disabled: bool = False
    labels: list[str] = Field(default_factory=list)
    include_versions: list[str] = Field(default_factory=list)
    exclude_versions: list[str] = Field(default_factory=list)
    pre_release: list[str] = Field(default_factory=list)
    job_throttle_category: str = ""
    params: dict = Field(default_factory=dict)
    wait: bool = False
    wait_timeout: int = WAIT_TIMEOUT
    fail_on_error: bool = False
    # Globs are matched against the artifact's *filename*, not its relative path, so
    # "*.xml" finds reports/results.xml while "reports/*.xml" matches nothing.
    collect_results: list[str] = Field(default_factory=list)


def job_arch(job: JobConfig) -> str:
    """The architecture a job's DB nodes run on.

    Jobs that predate the `arch` field only say so through an "aarch64" label, so fall back
    to that before assuming the default. Used both to filter jobs against a supplied image's
    architecture and to look up per-backend images for version resolution — the two must
    agree on what a job's architecture is.
    """
    if job.arch:
        return job.arch
    return "aarch64" if "aarch64" in job.labels else DEFAULT_ARCH


class MatrixConfig(BaseModel):
    """Full trigger matrix configuration loaded from YAML."""

    model_config = ConfigDict(extra="forbid")

    jobs: list[JobConfig]
    defaults: dict = Field(default_factory=dict)
    default_scylla_version: str = ""
    version_resolution: VersionResolution = DEFAULT_VERSION_RESOLUTION
    cron_triggers: list[CronTriggerConfig] = Field(default_factory=list)
    email_recipients: list[str] = Field(default_factory=list)
    jenkinsfiles: list[JenkinsfileEntry] = Field(default_factory=list)


# Keys allowed directly on a job entry — anything else is a Jenkins parameter and
# must live under `params`.
JOB_LEVEL_KEYS = frozenset(JobConfig.model_fields)
MATRIX_LEVEL_KEYS = frozenset(MatrixConfig.model_fields)


@dataclass
class BuildResult:
    job_name: str
    build_number: int
    result: str  # SUCCESS, FAILURE, ABORTED, UNSTABLE
    artifacts: list[str] = field(default_factory=list)
    build_url: str = ""

    @property
    def success(self) -> bool:
        return self.result == "SUCCESS"


@dataclass
class _PendingWaitJob:
    """Internal: tracks a triggered job that needs to be waited on."""

    job_name: str
    queue_url: str
    collect_results: list[str]
    timeout: int
    fail_on_error: bool


@dataclass(frozen=True, order=True)
class BackendTarget:
    """A backend/region/arch combination that matrix jobs need an image for."""

    backend: str
    region: str = ""
    arch: str = DEFAULT_ARCH

    def __str__(self) -> str:
        return "/".join(part for part in (self.backend, self.region, self.arch) if part)
