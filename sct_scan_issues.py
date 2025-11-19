#!/usr/bin/env python3

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
# Copyright (c) 2024 ScyllaDB

import sys
import ast
import tempfile
import logging
from pathlib import Path

import click

from sdcm.utils.issues import SkipPerIssues
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.common import get_sct_root_path
from sdcm.utils.sct_cmd_helpers import add_file_logger
from sdcm.sct_events.setup import start_events_device, stop_events_device

LOGGER = logging.getLogger(__name__)


def get_value(node):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join([get_value(n) for n in node.values if get_value(n)])
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.List):
        return [get_value(elem) for elem in node.elts]
    if isinstance(node, ast.Attribute):
        return f"{get_value(node.value)}.{node.attr}"

    return None


@click.command("scan-skip-issues")
def scan_issue_skips():
    add_file_logger()
    unneeded_skips = False
    temp_dir = tempfile.mkdtemp()
    start_events_device(temp_dir)

    params = SCTConfiguration()
    for file_path in Path(get_sct_root_path()).glob("**/*.py"):
        if (
            file_path.name.startswith("test_")
            or file_path.name == Path(__file__).name
            or any(subdir in str(file_path) for subdir in (".tox/", ".venv/", ".env/"))
        ):
            # skip tests and virtual env files
            continue
        for node in ast.walk(ast.parse(file_path.read_text())):
            if isinstance(node, ast.Call):
                name = get_value(node.func)
                if name == "SkipPerIssues":
                    args = [get_value(n) for n in node.args]
                    click.secho(f"{file_path.name}:{node.lineno}: SkipPerIssues({repr(args)})", fg="green")
                    check = SkipPerIssues(*args[:1], params=params)
                    issues_opened = check.issues_opened()

                    issues_labels = sum([issue.labels for issue in check.issues], [])
                    issues_labeled = any(
                        label.name
                        for label in issues_labels
                        if label.name.startswith("sct-") and label.name.endswith("-skip")
                    )

                    if not (issues_opened or issues_labeled):
                        click.secho(f"{args[:1]} is closed, should be consider to be remove from code", fg="red")
                        unneeded_skips = True

    stop_events_device()
    if unneeded_skips:
        return sys.exit(1)
    return 0


if __name__ == "__main__":
    sys.exit(scan_issue_skips())
