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

from typing import Annotated, ClassVar

from pydantic import BaseModel, BeforeValidator

from sdcm.sct_config.nemesis_runners import NemesisConfig, yaml_from_str
from sdcm.sct_config.types import Boolean, IntOrList, SctField, String, StringOrList


class NemesisConfigMixin(BaseModel):
    """Nemesis (chaos testing).

    Which disruptions run, how often, and how targets are selected.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Nemesis (chaos testing)"

    nemesis: Annotated[NemesisConfig, BeforeValidator(yaml_from_str)] = SctField(
        description="""
                Nemesis configuration: nemesis-wide settings, defaults shared by all runners, and the
                runners to start, one thread each. `runners` is a list of single-key mappings of a
                runner class to its options, e.g. `{SisyphusMonkey: {selector: disruptive, interval: 15}}`;
                the options accepted depend on the runner class. Options a runner leaves unset take
                the `nemesis` defaults; a runner without `seed` gets a random one. `selector` is a
                logical expression over nemesis flags filtering the disruptions a runner runs.
                Accepts YAML or JSON from the environment, for the whole option or one sub-key, e.g.
                SCT_NEMESIS__INTERVAL=10 or SCT_NEMESIS__RUNNERS='[{"SisyphusMonkey": {"selector": "disruptive"}}]'.
        """,
    )
    nemesis_add_node_cnt: int = SctField(
        description="Not read; replaced by `nemesis.grow_shrink.add_node_cnt`.",
    )
    nemesis_class_name: StringOrList = SctField(
        description="Not read; replaced by `nemesis.runners`.",
    )
    nemesis_double_load_during_grow_shrink_duration: int = SctField(
        description="Not read; replaced by `nemesis.grow_shrink.double_load_duration`.",
    )
    nemesis_during_prepare: Boolean = SctField(
        description="Not read; replaced by `nemesis.during_prepare`.",
    )
    nemesis_filter_seeds: Boolean = SctField(
        description="Not read; replaced by `nemesis.filter_seeds`.",
    )
    nemesis_grow_shrink_instance_type: String = SctField(
        description="Not read; replaced by `nemesis.grow_shrink.instance_type`.",
    )
    nemesis_interval: int = SctField(
        description="Not read; replaced by `nemesis.interval`.",
    )
    nemesis_multiply_factor: int = SctField(
        description="Not read; replaced by `nemesis.multiply_factor`.",
    )
    nemesis_seed: IntOrList = SctField(
        description="Not read; replaced by `nemesis.seed`.",
    )
    nemesis_selector: StringOrList = SctField(
        description="Not read; replaced by `nemesis.selector`.",
    )
    nemesis_sequence_sleep_between_ops: int = SctField(
        description="Not read; replaced by `nemesis.sequence_sleep_between_ops`.",
    )
    # Temporary solution. We do not want to run SLA nemeses during not-SLA test until the feature is stable
    sla: Boolean = SctField(
        description="run SLA nemeses if the test is SLA only",
    )
