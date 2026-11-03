#!/usr/bin/env python
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

"""Rewrite a group's flat options into its config section, and verify the rewrite against `master`.

Each group converted to a section (see ``sdcm.sct_config.sections``) adds its rewrite rules and its
snapshot of loaded values here. The script is deleted once the last group is converted.

Usage::

    # rewrite every tracked YAML file in place (comments and key order are kept)
    uv run python utils/migrate_config_section.py rewrite nemesis

    # load every pipeline and test case, once per checkout, and compare the loaded values
    PYTHONPATH=<master-checkout> uv run python utils/migrate_config_section.py snapshot nemesis master.json
    PYTHONPATH=. uv run python utils/migrate_config_section.py snapshot nemesis branch.json
    uv run python utils/migrate_config_section.py compare master.json branch.json
"""

import contextlib
import functools
import json
import logging
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch

import click
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.comments import CommentedMap

import sdcm
from sdcm.nemesis import NemesisFlags, NemesisRunner
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.lint.env_builder import build_env
from sdcm.utils.lint.jenkins_parser import discover_pipeline_files, parse_jenkinsfile
from sdcm.utils.lint.validator import _CLOUD_API_PATCHES

#: Checkout whose `sdcm` is imported (`PYTHONPATH` picks it), so a snapshot loads that checkout's files.
ROOT = Path(sdcm.__file__).resolve().parent.parent

#: Flat nemesis option -> path inside the `nemesis` section; runner options are placed by `nemesis_section`.
NEMESIS_PATHS = {
    "nemesis_class_name": ("runners",),
    "nemesis_selector": ("selector",),
    "nemesis_seed": ("seed",),
    "nemesis_interval": ("interval",),
    "nemesis_multiply_factor": ("multiply_factor",),
    "nemesis_filter_seeds": ("filter_seeds",),
    "nemesis_during_prepare": ("during_prepare",),
    "nemesis_sequence_sleep_between_ops": ("sequence_sleep_between_ops",),
    "sla": ("sla",),
    "nemesis_add_node_cnt": ("grow_shrink", "add_node_cnt"),
    "nemesis_grow_shrink_instance_type": ("grow_shrink", "instance_type"),
    "nemesis_double_load_during_grow_shrink_duration": ("grow_shrink", "double_load_duration"),
}

#: Flat options set per runner as index-aligned lists.
NEMESIS_RUNNER_LISTS = {"nemesis_selector": "selector", "nemesis_seed": "seed"}

#: `nemesis` defaults a runner entry may set for itself, when its config model declares them.
NEMESIS_RUNNER_DEFAULTS = ("selector", "seed", "interval", "multiply_factor", "filter_seeds")

#: Single-runner files that keep every runner option as a `nemesis` default: a config file stacked after
#: them either sets an option as a default, which the runner's own value would shadow, or replaces the
#: runners, which would drop the runner's own values. One form per file, so all options stay there.
NEMESIS_SECTION_LEVEL_FILES = (
    # every configuration stacks on it
    "defaults/test_default.yaml",
    # configurations/manager/manager_operations.yaml, toggle-audit-nemesis.yaml, toggle-audit-rules-nemesis.yaml
    "test-cases/longevity/longevity-100gb-4h.yaml",
    # configurations/manager/manager_operations.yaml
    "test-cases/longevity/longevity-50GB-3days-authorization-and-tls-ssl.yaml",
    "test-cases/longevity/longevity-alternator-3h.yaml",
    "test-cases/longevity/longevity-counters-3h.yaml",
    "test-cases/longevity/longevity-large-partition-3h.yaml",
    "test-cases/longevity/longevity-large-partition-8h.yaml",
    "test-cases/longevity/longevity-mv-si-4days.yaml",
    # configurations/operator/200GB-no-tls.yaml, configurations/rf1-non-disruptive.yaml
    "test-cases/longevity/longevity-200GB-48h-verifier-LimitedMonkey-tls.yaml",
    # configurations/operator/lwt-basic-24h.yaml
    "test-cases/longevity/longevity-lwt-basic-24h.yaml",
    # configurations/operator/twcs-basic-48h.yaml
    "test-cases/longevity/longevity-twcs-48h.yaml",
    # configurations/custom-d1-workload2-repair.yaml
    "test-cases/longevity/longevity-gce-custom-d1-workload2-hybrid-raid.yaml",
    # configurations/exclude-vnode-to-tablet-migration-limitations.yaml
    "test-cases/longevity/longevity-50-percent-utilization-4h.yaml",
)


class Leaf(NamedTuple):
    """One option of the new section: its YAML value text and the comments it carries."""

    text: str
    comment: str = ""
    above: tuple[str, ...] = ()


class FlatOption(NamedTuple):
    """A top-level option of a YAML file: its value, source text and place in the file."""

    value: object
    start: int  # first line of the comments right above the option
    key_line: int
    end: int  # line after the option's value
    text: str  # value text, when the value fits on the key's line
    comment: str  # end-of-line comment
    above: tuple[str, ...]  # comment lines right above the option


def scalar_text(value) -> str:
    """YAML text of a scalar taken out of a list."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, str):
        return json.dumps(value)
    return str(value)


def flat_options(lines: list[str], data: CommentedMap, keys) -> dict[str, FlatOption]:
    """Locate ``keys`` among the top-level options of a YAML file."""
    starts = sorted((data.lc.key(key)[0], key) for key in data)
    options = {}
    previous_end = 0
    for (key_line, key), (next_line, _) in zip(starts, [*starts[1:], (len(lines), None)]):
        end = next_line
        while end > key_line + 1 and (not lines[end - 1].strip() or lines[end - 1].startswith("#")):
            end -= 1
        if key in keys:
            start = key_line
            while start > previous_end and lines[start - 1].startswith("#"):
                start -= 1
            text, comment = "", ""
            if end == key_line + 1:
                text = lines[key_line].split(":", 1)[1]
                if (token := (data.ca.items.get(key) or [None] * 4)[2]) and token.start_mark.line == key_line:
                    text = lines[key_line][: token.start_mark.column].split(":", 1)[1]
                    comment = token.value.split("\n", 1)[0]
                text = text.strip()
            options[key] = FlatOption(data[key], start, key_line, end, text, comment, tuple(lines[start:key_line]))
        previous_end = end
    return options


def leaf(option: FlatOption, value=None) -> Leaf:
    """The option's own text when its value is kept whole, else ``value`` rendered."""
    if value is None and option.text:
        return Leaf(option.text, option.comment, option.above)
    value = option.value if value is None else value
    text = scalar_text(value) if not isinstance(value, list) else json.dumps(list(value))
    return Leaf(text, option.comment, option.above)


def per_runner_values(options: dict[str, FlatOption], runner_names: list | None, path: str) -> dict[str, list]:
    """``{runner option: one value per runner}`` of the index-aligned selector/seed lists that differ per runner."""
    per_runner = {}
    for flat_key, option in NEMESIS_RUNNER_LISTS.items():
        values = options[flat_key].value if flat_key in options else None
        if isinstance(values, list) and len(values) > 1 and len(set(map(str, values))) > 1:
            if runner_names is None or len(values) != len(runner_names):
                raise ValueError(f"{path}: {flat_key} lists one value per runner, but the runners are not set here")
            per_runner[option] = values
    return per_runner


def runner_entries(
    runner_names: list, per_runner: dict[str, list], shared: dict[str, Leaf] | None = None
) -> list[tuple[str, dict]]:
    """``(class name, {option: Leaf})`` per runner, each with the ``shared`` options too.

    FixedOrderMonkey's comma-separated selector becomes its `order`.
    """
    runners = []
    for index, class_name in enumerate(runner_names):
        runner_options = {option: Leaf(scalar_text(values[index])) for option, values in per_runner.items()}
        runner_options |= shared or {}
        if class_name == "FixedOrderMonkey" and "selector" in runner_options:
            selector = runner_options["selector"]
            order = [name.strip() for name in YAML(typ="safe").load(selector.text).split(",")]
            order_leaf = Leaf(json.dumps(order), selector.comment, selector.above)
            runner_options = {
                ("order" if option == "selector" else option): (order_leaf if option == "selector" else value)
                for option, value in runner_options.items()
            }
        runners.append((str(class_name), runner_options))
    return runners


@functools.cache
def runner_option_names(class_name: str) -> frozenset[str]:
    """`nemesis` defaults the runner's config model declares; FixedOrderMonkey takes `selector` as its `order`."""
    registry = NemesisRegistry(base_class=NemesisRunner, flag_class=NemesisFlags)
    runners = {runner.__name__: runner for runner in registry.get_subclasses()}
    fields = runners.get(class_name, NemesisRunner).config_model.model_fields
    names = {name for name in NEMESIS_RUNNER_DEFAULTS if name in fields}
    return frozenset(names | {"selector"} if class_name == "FixedOrderMonkey" else names)


def keeps_section_level(path: str) -> bool:
    """True if the file at ``path`` is one of `NEMESIS_SECTION_LEVEL_FILES`."""
    posix = Path(path).as_posix()
    return any(posix == name or posix.endswith(f"/{name}") for name in NEMESIS_SECTION_LEVEL_FILES)


def nemesis_section(options: dict[str, FlatOption], path: str) -> dict:
    """Build the `nemesis` section from the flat options of one YAML file, in their order of appearance.

    A file with a single runner (not appended with ``"++"``) sets each option that runner's config
    model declares in the runner's entry, unless the file is one of `NEMESIS_SECTION_LEVEL_FILES`. With several
    runners, index-aligned selector/seed lists become options of each runner entry, and a single
    value, or a list whose values are all equal, becomes the `nemesis` default.
    """
    class_names = None
    if "nemesis_class_name" in options:
        class_names = options["nemesis_class_name"].value
        class_names = list(class_names) if isinstance(class_names, list) else [class_names]
    appended = bool(class_names) and class_names[0] == "++"
    runner_names = class_names[1:] if appended else class_names
    per_runner = per_runner_values(options, runner_names, path)
    single_runner_names = set()
    if not appended and runner_names and len(runner_names) == 1 and not keeps_section_level(path):
        single_runner_names = runner_option_names(str(runner_names[0]))

    section = {}
    single_runner_options = {}
    runners_above = ()
    for flat_key, option in options.items():
        *parents, name = NEMESIS_PATHS[flat_key]
        if flat_key == "nemesis_class_name":
            value = (appended, runner_entries(runner_names, per_runner), option.comment, option.above)
        elif NEMESIS_RUNNER_LISTS.get(flat_key) in per_runner:
            runners_above += option.above
            continue
        elif flat_key in NEMESIS_RUNNER_LISTS and isinstance(option.value, list) and option.value:
            value = leaf(option, option.value[0])
        else:
            value = leaf(option)
        if name in single_runner_names:
            single_runner_options[name] = value
            continue
        target = section
        for parent in parents:
            target = target.setdefault(parent, {})
        target[name] = value
    if single_runner_options:
        appended, runners, comment, above = section["runners"]
        section["runners"] = (appended, runner_entries(runner_names, {}, single_runner_options), comment, above)
    if runners_above:
        appended, runners, comment, above = section["runners"]
        section["runners"] = (appended, runners, comment, above + runners_above)
    return section


def render(section: dict, indent: str) -> list[str]:
    """YAML lines of a section built by `nemesis_section`."""
    lines = []
    for name, value in section.items():
        if isinstance(value, Leaf):
            lines += [indent + line.lstrip() for line in value.above]
            comment = f"  {value.comment}" if value.comment else ""
            lines.append(f"{indent}{name}: {value.text}{comment}\n")
        elif isinstance(value, dict):
            lines.append(f"{indent}{name}:\n")
            lines += render(value, indent + "  ")
        else:
            appended, runners, comment, above = value
            lines += [indent + line.lstrip() for line in above]
            lines.append(f"{indent}{name}:{'  ' + comment if comment else ''}\n")
            if appended:
                lines.append(f'{indent}  - "++"\n')
            for class_name, runner_options in runners:
                if not runner_options:
                    lines.append(f"{indent}  - {class_name}: {{}}\n")
                    continue
                lines.append(f"{indent}  - {class_name}:\n")
                lines += render(runner_options, indent + "      ")
    return lines


def rewrite_nemesis(text: str, path: str) -> str | None:
    """Move the flat nemesis options of a YAML file into a `nemesis` section at the place of the first one.

    Only those lines change. Each option keeps its value text, its end-of-line comment and the
    comment lines right above it.
    """
    data = YAML().load(text)
    if not isinstance(data, CommentedMap) or not NEMESIS_PATHS.keys() & data.keys():
        return None
    if "nemesis" in data:
        raise ValueError(f"{path}: has both flat nemesis options and a `nemesis` section")
    lines = text.splitlines(keepends=True)
    options = flat_options(lines, data, NEMESIS_PATHS)
    options = dict(sorted(options.items(), key=lambda item: item[1].key_line))
    first_key, first = next(iter(options.items()))
    # the comments above the first option stay above the section
    options[first_key] = first._replace(above=())
    section_lines = [*first.above, "nemesis:\n", *render(nemesis_section(options, path), "  ")]
    replaced = {line: None for option in options.values() for line in range(option.start, option.end)}
    output = []
    for number, line in enumerate(lines):
        if number == first.start:
            output += section_lines
        if number in replaced:
            continue
        # drop the blank line a removed option leaves next to another one
        if not line.strip() and output and not output[-1].strip() and number - 1 in replaced:
            continue
        output.append(line)
    rewritten = "".join(output)
    # a removed last option leaves no blank line behind at the end of the file
    return rewritten.rstrip("\n") + "\n" if not text.endswith("\n\n") else rewritten


REWRITES = {"nemesis": rewrite_nemesis}


def nemesis_snapshot(config: SCTConfiguration) -> dict:
    """The nemesis values ``config`` loaded, in a form shared by the flat and the nested options."""
    if "nemesis" not in SCTConfiguration.model_fields:
        get = config.get
        class_names = get("nemesis_class_name") or []
        selectors = get("nemesis_selector") or []
        seeds = get("nemesis_seed") or []
        runners = []
        for index, class_name in enumerate(class_names):
            selector = selectors[0] if len(selectors) == 1 else (selectors[index] if selectors else None)
            seed = seeds[0] if len(seeds) == 1 else (seeds[index] if seeds else None)
            runner = {
                "class": class_name,
                "interval": get("nemesis_interval"),
                "seed": seed,
                "filter_seeds": get("nemesis_filter_seeds"),
            }
            if class_name == "FixedOrderMonkey":
                runner["order"] = [name.strip() for name in (selector or "").split(",") if name.strip()]
            else:
                runner |= {"selector": selector or None, "multiply_factor": get("nemesis_multiply_factor")}
            runners.append(runner)
        return {
            "runners": runners,
            "during_prepare": get("nemesis_during_prepare"),
            "sequence_sleep_between_ops": get("nemesis_sequence_sleep_between_ops"),
            "sla": get("sla"),
            "add_node_cnt": get("nemesis_add_node_cnt"),
            "grow_shrink_instance_type": get("nemesis_grow_shrink_instance_type"),
            "double_load_duration": get("nemesis_double_load_during_grow_shrink_duration"),
        }
    section = config.nemesis
    runners = []
    for runner in section.runners:
        resolved = section.resolve(runner).model_dump(exclude={"class_name"})
        resolved["seed"] = runner.seed if runner.seed is not None else section.seed
        resolved |= {"selector": resolved["selector"] or None} if "selector" in resolved else {}
        runners.append({"class": runner.class_name} | resolved)
    return {
        "runners": runners,
        "during_prepare": section.during_prepare,
        "sequence_sleep_between_ops": section.sequence_sleep_between_ops,
        "sla": section.sla,
        "add_node_cnt": section.grow_shrink.add_node_cnt,
        "grow_shrink_instance_type": section.grow_shrink.instance_type,
        "double_load_duration": section.grow_shrink.double_load_duration,
    }


def outside_nemesis_snapshot(config: SCTConfiguration) -> dict:
    """Every other option ``config`` loaded, to show converting the group changed nothing else."""
    return config.model_dump(mode="json", exclude={"nemesis", *NEMESIS_PATHS})


SNAPSHOTS = {"nemesis": nemesis_snapshot, "outside-nemesis": outside_nemesis_snapshot}


def load_snapshot(args: tuple[str, str, dict]) -> tuple[str, dict | str]:
    """Load one configuration from ``env`` like `lint-pipelines` does, and snapshot the group's values."""
    group, name, env = args
    logging.disable(logging.CRITICAL)
    try:
        with patch.dict(os.environ, env, clear=True), contextlib.ExitStack() as stack:
            for target, mock_fn in _CLOUD_API_PATCHES.items():
                stack.enter_context(patch(target, side_effect=mock_fn))
            stack.enter_context(patch.object(SCTConfiguration, "_resolve_xcloud_version_tag"))
            return name, SNAPSHOTS[group](SCTConfiguration())
    except Exception as exc:  # noqa: BLE001
        return name, f"failed to load: {type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}"


def snapshot_inputs(group: str) -> list[tuple[str, str, dict]]:
    """Every pipeline with the env Jenkins would set, and every test case alone on the docker backend."""
    inputs = []
    for pipeline in discover_pipeline_files(ROOT / "jenkins-pipelines"):
        if pipeline_config := parse_jenkinsfile(pipeline):
            inputs.append((group, str(pipeline.relative_to(ROOT)), build_env(pipeline_config)))
    for test_case in sorted((ROOT / "test-cases").rglob("*.yaml")):
        env = {"SCT_CLUSTER_BACKEND": "docker", "SCT_CONFIG_FILES": str(test_case.relative_to(ROOT))}
        inputs.append((group, str(test_case.relative_to(ROOT)), env | {"PATH": os.environ.get("PATH", "")}))
    return inputs


@click.group()
def cli():
    """Rewrite flat options into a config section and verify the result."""


@cli.command()
@click.argument("group", type=click.Choice(sorted(REWRITES)))
@click.argument("paths", nargs=-1, type=click.Path(exists=True, path_type=Path))
def rewrite(group: str, paths: tuple[Path, ...]):
    """Rewrite PATHS (default: every tracked YAML file outside docs/) in place."""
    if not paths:
        tracked = subprocess.run(
            ["git", "ls-files", "*.yaml", "*.yml"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.split()
        paths = tuple(ROOT / name for name in tracked if not name.startswith("docs/"))
    changed = 0
    for path in paths:
        try:
            rewritten = REWRITES[group](path.read_text(), str(path))
        except YAMLError:
            continue  # Jinja templates and multi-document files hold no flat options
        if rewritten is not None:
            path.write_text(rewritten)
            changed += 1
    click.echo(f"rewrote {changed} files")


@cli.command()
@click.argument("group", type=click.Choice(sorted(SNAPSHOTS)))
@click.argument("output", type=click.Path(path_type=Path))
def snapshot(group: str, output: Path):
    """Write the group's loaded values of every pipeline and test case to OUTPUT (JSON)."""
    with ProcessPoolExecutor() as executor:
        results = dict(executor.map(load_snapshot, snapshot_inputs(group), chunksize=8))
    output.write_text(json.dumps(results, indent=1, sort_keys=True))
    click.echo(f"snapshotted {len(results)} configurations")


@cli.command()
@click.argument("before", type=click.Path(exists=True, path_type=Path))
@click.argument("after", type=click.Path(exists=True, path_type=Path))
def compare(before: Path, after: Path):
    """Fail when a configuration loads different values in AFTER than in BEFORE."""
    old, new = json.loads(before.read_text()), json.loads(after.read_text())
    differences = [name for name in sorted(old.keys() | new.keys()) if old.get(name) != new.get(name)]
    for name in differences:
        click.echo(f"{name}:\n  before: {old.get(name)}\n  after:  {new.get(name)}")
    click.echo(f"{len(differences)} of {len(old.keys() | new.keys())} configurations differ")
    sys.exit(1 if differences else 0)


if __name__ == "__main__":
    cli()
