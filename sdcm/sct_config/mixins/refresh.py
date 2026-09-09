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

"""Refresh (sstable loading) tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class RefreshConfigMixin(BaseModel):
    """Refresh (sstable loading) tests.

    Loading pre-built SSTables into a running cluster via nodetool refresh.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Refresh (sstable loading) tests"

    flush_period: int = SctField(description="Seconds to wait between the flushes controlled by 'flush_times'.")
    flush_times: int = SctField(description="How many times to flush the memtable to disk during the refresh test.")
    skip_download: Boolean = SctField(
        description="Skip downloading the SSTable archive and reuse a copy already on the node."
    )
    sstable_file: String = SctField(description="Local path of the SSTable archive to load with 'nodetool refresh'.")
    sstable_md5: String = SctField(
        description="Expected MD5 of the downloaded SSTable archive, used to verify the download."
    )
    sstable_url: String = SctField(
        description="URL the SSTable archive is downloaded from when it is not already on the node."
    )
