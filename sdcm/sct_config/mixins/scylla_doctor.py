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

"""Scylla Doctor configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class ScyllaDoctorConfigMixin(BaseModel):
    """Scylla Doctor.

    The scylla-doctor diagnostic tool. It is both a subject under test (the artifact tests run it
    and assert on its findings) and a diagnostic collected on failure, which is why it is its own
    group rather than part of log collection.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Scylla Doctor"

    run_scylla_doctor: Boolean = SctField(
        description="Flag to run Scylla Doctor tool",
    )
    scylla_doctor_edition: Literal["basic", "full"] = SctField(
        description="""Scylla Doctor edition to use. Allowed values: 'basic', 'full'.
                'basic' fetches the free/open-source edition via HTTP.
                'full' fetches the full/enterprise edition from a private S3 bucket.""",
    )
    scylla_doctor_version: String = SctField(
        description="""Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')
                to hardcode the version, or leave empty to use the latest available version. For stability,
                artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.""",
    )
    use_scylla_doctor_on_failure: Boolean = SctField(
        description="Run scylla-doctor on test failure to collect additional diagnostics",
    )
