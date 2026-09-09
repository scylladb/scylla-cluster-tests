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

"""Auxiliary DB cluster (oracle / Cassandra) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import IntOrList, SctField, String


class AuxDbConfigMixin(BaseModel):
    """Auxiliary DB cluster (oracle / Cassandra).

    A second database cluster used for comparison or migration testing -- the 'oracle' cluster in
    Gemini runs, or a Cassandra cluster in migration tests. Named for the role, not for Gemini,
    since other test types use it too.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Auxiliary DB cluster (oracle / Cassandra)"

    append_scylla_args_oracle: String = SctField(
        description="More arguments to append to oracle command line",
    )
    n_test_oracle_db_nodes: IntOrList = SctField(
        description="Number list of oracle test nodes in multiple data centers.",
    )
    oracle_scylla_version: String = SctField(
        description="""Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'
                 Automatically looks up cloud images for formal versions.
                 WARNING: can't be used together with 'ami_id_db_oracle' and 'oci_image_db_oracle'""",
        appendable=False,
    )
    oracle_user_data_format_version: String = SctField(
        description="Same as 'user_data_format_version', but for the auxiliary oracle cluster's images.",
        appendable=False,
    )
