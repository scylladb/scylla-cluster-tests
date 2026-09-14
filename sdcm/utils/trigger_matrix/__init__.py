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

"""Centralized Jenkins trigger matrix.

The package is layered: `versions` parses version strings, `images` performs cloud image
lookups, `models`/`config` describe and load a matrix YAML, `filters` selects jobs,
`resolution` decides which build each backend runs, `parameters` builds the Jenkins
parameters, `jenkins_client` talks to Jenkins, and `matrix` orchestrates the lot.

The public surface below is deliberately narrow: it re-exports only the names used from
outside the package. Internal helpers are NOT re-exported here, so that a stale
``mock.patch("sdcm.utils.trigger_matrix.<name>")`` fails loudly with an AttributeError
instead of silently patching an attribute no call site ever reads.

Patch the module whose code *calls* the name. ``from x import f`` binds a copy of ``f`` in
the importing module's namespace, so patching the module that merely defines it leaves every
call site untouched. The cloud lookups are the exception that proves the rule: submodules
reach them as ``images.f(...)`` precisely so that one patch of
``sdcm.utils.trigger_matrix.images.f`` covers every caller -- which is what
``unit_tests/trigger_matrix/conftest.py`` relies on to keep the suite offline.
``unit_tests/trigger_matrix/test_patch_targets.py`` enforces both rules.
"""

from sdcm.utils.trigger_matrix.config import get_parameterized_cron, load_matrix_config
from sdcm.utils.trigger_matrix.constants import VERSION_RESOLUTION_STRATEGIES, VersionResolution
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError, MatrixValidationError, TriggerMatrixError
from sdcm.utils.trigger_matrix.images import resolve_image_architecture, resolve_scylla_version_from_image
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient
from sdcm.utils.trigger_matrix.matrix import trigger_matrix
from sdcm.utils.trigger_matrix.models import (
    BackendTarget,
    BuildResult,
    CronTriggerConfig,
    JenkinsfileEntry,
    JobConfig,
    MatrixConfig,
)
from sdcm.utils.trigger_matrix.resolution import resolve_to_full_version

__all__ = [
    "BackendTarget",
    "BuildResult",
    "CronTriggerConfig",
    "JenkinsClient",
    "JenkinsTriggerError",
    "JenkinsfileEntry",
    "JobConfig",
    "MatrixConfig",
    "MatrixValidationError",
    "TriggerMatrixError",
    "VERSION_RESOLUTION_STRATEGIES",
    "VersionResolution",
    "get_parameterized_cron",
    "load_matrix_config",
    "resolve_image_architecture",
    "resolve_scylla_version_from_image",
    "resolve_to_full_version",
    "trigger_matrix",
]
