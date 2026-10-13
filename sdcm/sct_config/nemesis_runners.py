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

"""Nested nemesis configuration: shared defaults plus a list of runners, each validated by its class.

YAML form::

    nemesis:
      interval: 5
      runners:
        - SisyphusMonkey:
            selector: "disruptive"
            interval: 15
        - CategoricalMonkey:
            dist: {StopStartMonkey: 2}
        - ManagerRcloneBackup: {}

Every runner entry is a single-key mapping ``{<RunnerClass>: {<options>}}``. The runner class is
looked up among the registered ``NemesisRunner`` subclasses and the entry is validated by that
class's ``config_model`` (a pydantic discriminated union, one member per runner class). Most
runners use the shared ``NemesisRunnerConfig``; a runner with options of its own defines a
subclass of it next to the runner.
"""

import random
from typing import Annotated, NamedTuple, Union

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Discriminator, Field, PositiveInt, Tag, create_model

from sdcm.nemesis import NemesisFlags, NemesisRunner, NemesisRunnerConfig
from sdcm.nemesis.registry import NemesisRegistry


class NemesisThread(NamedTuple):
    """A runner to start and its resolved config."""

    runner_class: type[NemesisRunner]
    config: NemesisRunnerConfig


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


def yaml_from_str(value):
    """Parse a string value (e.g. from an ``SCT_NEMESIS__RUNNERS`` env var) as YAML/JSON."""
    if isinstance(value, str):
        return yaml.safe_load(value)
    return value


def runner_class_name(value) -> str | None:
    """Return the runner class name of a runner entry, which picks the model validating it."""
    return value.get("class_name") if isinstance(value, dict) else getattr(value, "class_name", None)


def runner_entry_model(runner) -> type[NemesisRunnerConfig]:
    """Model of a `nemesis.runners` entry naming ``runner``: its ``config_model``, bound to ``runner``."""
    return type(f"{runner.__name__}Entry", (runner.config_model,), {"runner_class": runner, "__module__": __name__})


def runner_entry_type(runner_classes):
    """Type of one `nemesis.runners` entry: validated by the ``config_model`` of the runner class it names."""
    return Annotated[
        Union[tuple(Annotated[runner_entry_model(runner), Tag(runner.__name__)] for runner in runner_classes)],
        Discriminator(runner_class_name),
        BeforeValidator(runner_entry_from_mapping),
    ]


class NemesisDefaults(BaseModel):
    """The `nemesis` option: defaults for options a runner's own entry leaves unset.

    `nemesis_config_model` adds the runners.
    """

    model_config = ConfigDict(extra="forbid")

    interval: PositiveInt = Field(default=5, description="Default for runner `interval`")
    seed: int | None = Field(default=None, description="Default for runner `seed`; a random one when unset")
    filter_seeds: bool = Field(default=False, description="Default for runner `filter_seeds`")
    selector: str | None = Field(default=None, description="Default for runner `selector`")
    multiply_factor: PositiveInt = Field(default=2, description="Default for runner `multiply_factor`")
    runners: list[NemesisRunnerConfig] = Field(default_factory=list)

    def resolve(self, runner: NemesisRunnerConfig) -> NemesisRunnerConfig:
        """Return a copy of ``runner`` with unset options taken from these defaults, and a random seed if still unset.

        The stored runners are left untouched, so a later config source changing a default still
        applies to every runner.
        """
        update = {}
        for name in type(runner).model_fields:
            if getattr(runner, name) is None and (default := getattr(self, name, None)) is not None:
                update[name] = default
        if runner.seed is None and "seed" not in update:
            update["seed"] = random.randint(0, 1000)
        return runner.model_copy(update=update)

    def nemesis_threads(self) -> list[NemesisThread]:
        """Return the runner class and resolved config of each runner, one nemesis thread each."""
        return [NemesisThread(runner.runner_class, self.resolve(runner)) for runner in self.runners]


def nemesis_config_model(runner_classes) -> type[NemesisDefaults]:
    """Build the `nemesis` option model whose runner entries may name any of ``runner_classes``."""
    return create_model(
        "NemesisConfig",
        __base__=NemesisDefaults,
        runners=(
            Annotated[list[runner_entry_type(runner_classes)], BeforeValidator(yaml_from_str)],
            Field(default_factory=list, description="Runners to start, one thread each"),
        ),
    )


NemesisConfig = nemesis_config_model(
    NemesisRegistry(base_class=NemesisRunner, flag_class=NemesisFlags).get_subclasses()
)
