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
# Copyright (c) 2020 ScyllaDB

"""Jepsen tests configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String, StringOrList


class JepsenConfigMixin(BaseModel):
    """Jepsen tests.

    Jepsen consistency test runs.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Jepsen tests"

    jepsen_scylla_repo: String = SctField(
        description="Link to the git repository with Jepsen Scylla tests",
    )
    jepsen_test_cmd: StringOrList = SctField(
        description="Jepsen test command (e.g., 'test-all')",
    )
    jepsen_test_count: int = SctField(description="Possible number of reruns of single Jepsen test command")
    jepsen_test_run_policy: Literal["most", "any", "all"] = SctField(
        description="""
        Jepsen test run policy (i.e., what we want to consider as passed for a single test)

        'most' - most test runs are passed
        'any'  - one pass is enough
        'all'  - all test runs should pass
        """,
    )
