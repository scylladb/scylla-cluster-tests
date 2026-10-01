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

"""Nemesis (chaos testing) configuration options."""

import ast
from typing import ClassVar

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated

from sdcm.sct_config.types import Boolean, InputType, IntOrList, SctField, String, StringOrList


def _list_of_string_lists(value: str | list | None) -> list[list[str]] | None:
    """Parse into ``list[list[str]]``: one ordered nemesis-name list per FixedOrderMonkey thread.

    A YAML list of lists comes through as-is. A string (e.g. an env var override) is parsed
    with ``ast.literal_eval``, matching how the other list-typed config options accept
    env-var strings. Always nested, even for a single thread, so a length-1
    ``nemesis_class_name`` still writes ``[["A", "B"]]`` rather than a bare ``["A", "B"]`` --
    one shape, no flat/nested ambiguity to disambiguate.
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = ast.literal_eval(value)
    if not isinstance(value, list) or not all(isinstance(item, list) for item in value):
        raise ValueError(f"nemesis_fixed_order must be a list of lists of strings, e.g. [['A', 'B'], ['C']]: {value!r}")
    return [[str(name) for name in order] for order in value]


#: One ordered nemesis-name list per FixedOrderMonkey thread, matched by position to
#: nemesis_class_name. Single-use type, scoped here rather than in the shared types module.
NemesisFixedOrder = Annotated[
    list[list[str]],
    BeforeValidator(_list_of_string_lists),
    InputType("list[list[str]] -- one ordered name-list per FixedOrderMonkey thread"),
]


class NemesisConfigMixin(BaseModel):
    """Nemesis (chaos testing).

    Which disruptions run, how often, and how targets are selected.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Nemesis (chaos testing)"

    nemesis_add_node_cnt: int = SctField(
        description="""Add/remove nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_class_name: StringOrList = SctField(
        description="""
                Nemesis class to use (possible types in sdcm.nemesis).
                Supported syntax:
                - nemesis_class_name: "NemesisName"
                  Run one nemesis in a single thread.
                - nemesis_class_name: ["NemesisA", "NemesisB"]
                  Run NemesisA and NemesisB each in their own thread.
                - nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]
                  Run two SisyphusMonkey threads in parallel.
                Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and
                space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no
                longer supported. Use an explicit YAML list instead.
        """,
    )
    nemesis_double_load_during_grow_shrink_duration: int = SctField(
        description="After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.",
    )
    nemesis_during_prepare: Boolean = SctField(
        description="""Run nemesis during prepare stage of the test""",
    )
    nemesis_filter_seeds: Boolean = SctField(
        description="""If true runs the nemesis only on non seed nodes""",
    )
    nemesis_fixed_order: NemesisFixedOrder = SctField(
        description="""
                One ordered list of nemesis class names per FixedOrderMonkey thread. Each
                thread executes its list in the given order, repeating from the start once
                exhausted. Consumed only by FixedOrderMonkey; every other runner ignores it.
                A name may repeat within a list to run that nemesis more than once per cycle.
                Matched by position to nemesis_class_name, like nemesis_selector/nemesis_seed:
                - nemesis_fixed_order: [["A", "B", "C"]]
                  One FixedOrderMonkey thread, running A, B, C, A, B, C, ...
                - nemesis_fixed_order: [["A", "B"], ["C", "D"]]
                  Two FixedOrderMonkey threads: the first runs A, B, ...; the second C, D, ...
        """,
    )
    nemesis_grow_shrink_instance_type: String = SctField(
        description="""Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_interval: int = SctField(
        description="""Nemesis sleep interval to use if None provided specifically in the test""",
    )
    nemesis_multiply_factor: int = SctField(
        description="Multiply the list of nemesis to execute by the specified factor",
    )
    nemesis_seed: IntOrList = SctField(
        description="""A seed number in order to repeat nemesis sequence as part of SisyphusMonkey""",
    )
    nemesis_selector: StringOrList = SctField(
        description="""nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has
        ALL the properties in that list which are set to true (the intersection of all properties).
        (In other words filters out all nemesis that doesn't ONE of these properties set to true)
        IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.""",
    )
    nemesis_sequence_sleep_between_ops: int = SctField(
        description="""Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests""",
    )
    # Temporary solution. We do not want to run SLA nemeses during not-SLA test until the feature is stable
    sla: Boolean = SctField(
        description="run SLA nemeses if the test is SLA only",
    )
