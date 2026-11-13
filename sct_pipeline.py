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

"""`sct.py pipeline ...`: the logic of the Jenkins `vars/` steps, runnable on a laptop (CI modernization, SCT-1490)."""

import json
import os
import shlex
from pathlib import Path

import click

from sdcm.utils.pipeline_env import PipelineInput, pipeline_env


@click.group(help="Jenkins pipeline steps as commands")
def pipeline():
    pass


@pipeline.command("env", help="Print the SCT_* environment of a Jenkins job, generated from the SCT configuration")
@click.option(
    "--params-file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help='JSON: {"params": {...}, "pipeline_params": {...}, "builder_region": "..."}',
)
@click.option("--json", "as_json", is_flag=True, help="Print a JSON object instead of `export` lines")
@click.option(
    "--output",
    type=click.Path(dir_okay=False),
    help="Write to this file instead of stdout (hydra.sh prints its own lines to stdout)",
)
def env_cmd(params_file, as_json, output):
    env = pipeline_env(PipelineInput.from_file(params_file), os.environ)
    if as_json:
        text = json.dumps(env, indent=2, sort_keys=True)
    else:
        text = "\n".join(f"export {name}={shlex.quote(value)}" for name, value in sorted(env.items()))
    if output:
        Path(output).write_text(text + "\n", encoding="utf-8")
    else:
        click.echo(text)
