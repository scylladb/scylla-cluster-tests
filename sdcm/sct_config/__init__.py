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

"""
SCT configuration package.

The public surface is deliberately narrow: it re-exports only the names the rest of the repo
imports from ``sdcm.sct_config``.  Names that ``config.py`` merely imports for its own use
(``KeyStore``, ``convert_name_to_ami_if_needed``, ``get_branched_ami``, ...) are intentionally
NOT re-exported here, so that a stale ``mock.patch("sdcm.sct_config.<name>")`` fails loudly with
an AttributeError instead of silently patching an attribute no call site ever reads.  Patch
``sdcm.sct_config.config.<name>`` (or ``sdcm.sct_config.types.<name>``) instead.
"""

from sdcm.sct_config.config import (
    SCTConfiguration,
    init_and_verify_sct_config,
    is_arm_instance_type,
)
from sdcm.sct_config.defaults import AWS_SUPPORTED_REGIONS, available_backends
from sdcm.sct_config.helpers import simulated_racks_enabled
from sdcm.sct_config.types import (
    AdaptiveTimeoutMultipliers,
    IntOrList,
    SctField,
    StringOrList,
    boolean_or_space_separated_booleans,
    dict_or_str,
    int_or_space_separated_ints,
    is_multitenant_field,
    str_or_list_or_eval,
)

__all__ = [
    "AWS_SUPPORTED_REGIONS",
    "AdaptiveTimeoutMultipliers",
    "IntOrList",
    "SCTConfiguration",
    "SctField",
    "StringOrList",
    "available_backends",
    "boolean_or_space_separated_booleans",
    "dict_or_str",
    "init_and_verify_sct_config",
    "int_or_space_separated_ints",
    "is_arm_instance_type",
    "is_multitenant_field",
    "simulated_racks_enabled",
    "str_or_list_or_eval",
]
