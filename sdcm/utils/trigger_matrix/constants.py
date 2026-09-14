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

"""Constants shared across the trigger matrix package.

This module is a leaf on purpose. Several of these values are pydantic field defaults
(`JobConfig.wait_timeout`, `MatrixConfig.version_resolution`), so keeping them here is what
stops `models` from having to import the wait and resolution layers that import it back."""

from typing import Literal


VALID_BACKENDS = {"aws", "gce", "azure", "docker", "oci"}

# Backends whose Scylla build is published as an image SCT looks up by version
VALID_IMAGE_BACKENDS = {"aws", "gce", "azure", "oci"}

DEFAULT_ARCH = "x86_64"

MAX_TRIGGER_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# Default region used for AMI tag lookups
DEFAULT_AWS_REGION = "eu-west-1"
DEFAULT_AZURE_REGION = "eastus"

# Backends whose images are region scoped — both resolution and availability of a
# given build have to be checked in the region the job actually runs in.
REGIONAL_BACKENDS = {"aws", "azure", "oci"}

# How the version passed to the downstream jobs is picked:
#   per-backend — every backend resolves its own latest build (jobs may run different builds)
#   common      — one build for the whole matrix: the newest one published on *every* backend
#                 in the matrix (i.e. the lowest of the per-backend latest builds)
#   aws-strict  — the AWS latest build for everyone; jobs on a backend that doesn't have that
#                 exact build published are not triggered
VersionResolution = Literal["per-backend", "common", "aws-strict"]
VERSION_RESOLUTION_STRATEGIES: tuple[str, ...] = ("per-backend", "common", "aws-strict")
DEFAULT_VERSION_RESOLUTION: VersionResolution = "per-backend"

WAIT_POLL_INTERVAL = 30
WAIT_TIMEOUT = 14400
DEFAULT_EMAIL_RECIPIENTS = ["qa@scylladb.com"]

# Parameters that describe *where* a job runs. They are per-job by nature: a job that
# declares them in its `params` owns them, and a global CLI value only fills in for the
# jobs that don't. Without this, `--region us-east-1` would silently collapse a multi-DC
# job configured with region: '["eu-west-1", "eu-west-2"]' into a single region (SCT-693).
PER_JOB_LOCATION_PARAMS = ("region", "availability_zone")
