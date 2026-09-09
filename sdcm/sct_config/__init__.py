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

"""
SCT configuration package.

The public surface is deliberately narrow: it re-exports only the names the rest of the repo
imports from ``sdcm.sct_config``.  Names that ``config.py`` merely imports for its own use
(``KeyStore``, ``convert_name_to_ami_if_needed``, ``get_branched_ami``, ...) are intentionally
NOT re-exported here, so that a stale ``mock.patch("sdcm.sct_config.<name>")`` fails loudly with
an AttributeError instead of silently patching an attribute no call site ever reads.

Patch the module whose code *calls* the name -- almost always ``sdcm.sct_config.config.<name>``,
even for names defined in ``types`` or ``helpers``: ``from .types import f`` binds ``f`` in
``config``'s namespace, so patching ``sdcm.sct_config.types.f`` leaves every call site untouched.
``unit_tests/unit/test_lint_patch_targets.py`` enforces this for the pipeline linter.
"""

from sdcm.sct_config.config import (
    SCTConfiguration,
    backend_to_cloud,
    init_and_verify_sct_config,
    is_arm_instance_type,
    substitute_arch_markers,
)
from sdcm.sct_config.defaults import AWS_SUPPORTED_REGIONS, BACKEND_IMAGE_FIELD, available_backends
from sdcm.sct_config.helpers import count_regions, simulated_racks_enabled
from sdcm.sct_config.types import (
    AdaptiveTimeoutMultipliers,
    IntOrList,
    SctField,
    StringOrList,
    boolean_or_space_separated_booleans,
    dict_or_str,
    int_or_space_separated_ints,
    str_or_list_or_eval,
)

__all__ = [
    "AWS_SUPPORTED_REGIONS",
    "BACKEND_IMAGE_FIELD",
    "AdaptiveTimeoutMultipliers",
    "IntOrList",
    "SCTConfiguration",
    "SctField",
    "StringOrList",
    "available_backends",
    "backend_to_cloud",
    "boolean_or_space_separated_booleans",
    "count_regions",
    "dict_or_str",
    "init_and_verify_sct_config",
    "int_or_space_separated_ints",
    "is_arm_instance_type",
    "simulated_racks_enabled",
    "str_or_list_or_eval",
    "substitute_arch_markers",
]
