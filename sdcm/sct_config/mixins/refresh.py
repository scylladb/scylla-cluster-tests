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

"""Refresh (sstable loading) tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class RefreshConfigMixin(BaseModel):
    """Refresh (sstable loading) tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Refresh (sstable loading) tests"

    skip_download: Boolean = SctField(description="")
    sstable_file: String = SctField(description="")
    sstable_url: String = SctField(description="")
    sstable_md5: String = SctField(description="")
    flush_times: int = SctField(description="")
    flush_period: int = SctField(description="")
