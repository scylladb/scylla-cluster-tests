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

"""The `nemesis` section: shared settings, runner defaults, and a list of runners each validated by its class.

YAML form::

    nemesis:
      interval: 5
      runners:
        - SisyphusMonkey:
            selector: "disruptive"
            interval: 15
        - CategoricalMonkey:
            dist: {StopStartMonkey: 2}
        - FixedOrderMonkey:
            order: [StopStartMonkey, NoCorruptRepairMonkey]

Every runner entry is a single-key mapping ``{<RunnerClass>: {<options>}}``. The runner class is
looked up among the registered ``NemesisRunner`` subclasses and the entry is validated by that
class's ``config_model`` (a pydantic discriminated union, one member per runner class). Most
runners use ``NemesisRunnerConfig``; a runner with options of its own defines a model next to the
runner.
"""

from typing import Annotated, ClassVar, NamedTuple, Union

from pydantic import BeforeValidator, Discriminator, Field, PositiveInt, Tag, create_model, model_validator

from sdcm.nemesis import NemesisBaseClass, NemesisFlags, NemesisRunner, RunnerConfig
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.sct_config.sections import ConfigSection
from sdcm.sct_config.types import Boolean, InputType, SctField, String


class NemesisThread(NamedTuple):
    """A runner to start and its resolved config."""

    runner_class: type[NemesisRunner]
    config: RunnerConfig


def runner_entry_from_mapping(value):
    """Turn the YAML form ``{RunnerClass: {options}}`` into ``{"class_name": RunnerClass, **options}``.

    Entries that already carry ``class_name`` (e.g. a dumped config) and model instances pass through.
    """
    if isinstance(value, dict) and "class_name" not in value:
        if len(value) != 1:
            raise ValueError(f"A runner entry must have exactly one key, the runner class name; got {sorted(value)}")
        ((class_name, options),) = value.items()
        return {"class_name": class_name, **(options or {})}
    return value


def runner_class_name(value) -> str | None:
    """Return the runner class name of a runner entry, which picks the model validating it."""
    return value.get("class_name") if isinstance(value, dict) else getattr(value, "class_name", None)


def runner_entry_model(runner) -> type[RunnerConfig]:
    """Model of a `nemesis.runners` entry naming ``runner``: its ``config_model``, bound to ``runner``."""
    return type(f"{runner.__name__}Entry", (runner.config_model,), {"runner_class": runner, "__module__": __name__})


def runner_entry_type(runner_classes):
    """Type of one `nemesis.runners` entry: validated by the ``config_model`` of the runner class it names."""
    return Annotated[
        Union[tuple(Annotated[runner_entry_model(runner), Tag(runner.__name__)] for runner in runner_classes)],
        Discriminator(runner_class_name),
        BeforeValidator(runner_entry_from_mapping),
    ]


class GrowShrinkConfig(ConfigSection):
    """Options of the nemeses that add nodes and decommission them back (e.g. GrowShrinkCluster)."""

    add_node_cnt: int | None = SctField(description="Number of nodes added and removed")
    instance_type: String = SctField(
        description="Instance type of the added nodes; the cluster's DB instance type when unset"
    )
    double_load_duration: int | None = SctField(
        description="Minutes to double the load for after growing and before shrinking; 0 disables it"
    )


class NemesisConfigBase(ConfigSection):
    """The `nemesis` section: shared settings, defaults for options a runner's own entry leaves unset, and the runners.

    `nemesis_config_model` types the runners by the runner classes they may name.
    """

    #: Nemesis class names runner configs may name (e.g. CategoricalMonkey `dist` keys); empty skips the check.
    disruption_names: ClassVar[frozenset[str]] = frozenset()

    # Shared by all runners
    during_prepare: Boolean | None = SctField(description="Run nemesis during the prepare stage of the test")
    sequence_sleep_between_ops: PositiveInt | None = SctField(
        description="Minutes to sleep between the operations of sequence nemeses"
    )
    sla: Boolean | None = SctField(description="Run the SLA nemeses; set only in SLA tests")
    grow_shrink: GrowShrinkConfig = Field(
        default_factory=GrowShrinkConfig, description="Options of the nemeses that grow and shrink the cluster"
    )

    # Defaults for every runner
    interval: PositiveInt | None = SctField(description="Default for runner `interval`: minutes between disruptions")
    seed: int | None = SctField(description="Default for runner `seed`; a random one per runner when unset")
    filter_seeds: Boolean | None = SctField(description="Default for runner `filter_seeds`: target non-seed nodes only")
    selector: String = SctField(
        description="Default for runner `selector`: a logical expression over nemesis flags filtering the disruptions"
    )
    multiply_factor: PositiveInt | None = SctField(
        description="Default for runner `multiply_factor`: how many times the disruption list repeats before shuffling"
    )
    runners: list[RunnerConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_runner_disruptions(self):
        """Fail when a runner config names a nemesis class outside ``disruption_names``."""
        if self.disruption_names:
            for index, runner in enumerate(self.runners):
                try:
                    runner.check_disruptions(self.disruption_names)
                except ValueError as error:
                    raise ValueError(f"runners.{index}.{runner.class_name}: {error}") from error
        return self

    def resolve(self, runner: RunnerConfig) -> RunnerConfig:
        """Return a copy of ``runner`` with unset options taken from these defaults, and its random seed if still unset.

        Only options ``runner``'s model declares are filled. The stored runners are left untouched,
        so a later config source changing a default still applies to every runner.
        """
        update = {}
        for name in type(runner).model_fields:
            if getattr(runner, name) is None and (default := getattr(self, name, None)) is not None:
                update[name] = default
        if runner.seed is None and "seed" not in update:
            update["seed"] = runner.random_seed
        return runner.model_copy(update=update)

    def resolve_runners(self) -> list[NemesisThread]:
        """Return the runner class and resolved config of each runner, one nemesis thread each."""
        return [NemesisThread(runner.runner_class, self.resolve(runner)) for runner in self.runners]


def nemesis_config_model(runner_classes, disruption_classes=()) -> type[NemesisConfigBase]:
    """Build the `nemesis` section model whose runner entries may name any of ``runner_classes``.

    Runner configs may name only ``disruption_classes`` as disruptions.
    """
    model = create_model(
        "NemesisConfig",
        __base__=NemesisConfigBase,
        __module__=__name__,
        runners=(
            Annotated[
                list[runner_entry_type(runner_classes)],
                InputType("list of {<RunnerClass>: {<options>}} | YAML/JSON string"),
            ],
            SctField(
                default_factory=list,
                appendable=True,
                description="""Runners to start, one thread each. Every runner takes `interval`, `seed` and
                `filter_seeds`; most also take `selector` and `multiply_factor`; a runner with options of
                its own (e.g. CategoricalMonkey `dist`, FixedOrderMonkey `order`) declares them in its
                config model. See docs/nemesis.md.""",
            ),
        ),
    )
    model.disruption_names = frozenset(disruption.__name__ for disruption in disruption_classes)
    return model


NemesisConfig = nemesis_config_model(
    NemesisRegistry(base_class=NemesisRunner, flag_class=NemesisFlags).get_subclasses(),
    NemesisRegistry(base_class=NemesisBaseClass, flag_class=NemesisFlags).get_subclasses(),
)
