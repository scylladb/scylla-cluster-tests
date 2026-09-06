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

"""Guards for the string monkeypatch targets in the pipeline linter.

`_CLOUD_API_PATCHES` stops the linter from making real cloud API calls. Its keys are plain
strings, so a target that no longer matches the code does not raise -- `mock.patch` happily
replaces an attribute nobody reads, the linter falls through to the real API, and the failure
shows up as a confusing credential or network error in CI (or, worse, passes locally because the
developer happens to have the credentials the patch was meant to skip).

These tests make that failure mode static and loud.
"""

import ast
import importlib
import pathlib

import pytest

from sdcm import sct_abs_path
from sdcm.utils.lint.validator import _CLOUD_API_PATCHES


def _target_ids():
    return sorted(_CLOUD_API_PATCHES)


@pytest.mark.parametrize("target", _target_ids())
def test_patch_target_attribute_exists(target):
    """The dotted path must resolve, or `mock.patch` raises at linting time."""
    module_path, _, attr = target.rpartition(".")
    module = importlib.import_module(module_path)
    assert hasattr(module, attr), f"{module_path} has no attribute {attr!r}"


@pytest.mark.parametrize("target", _target_ids())
def test_no_call_site_shadows_the_patch_target(target):
    """No module may `from <other> import <attr>` and call it bare while we patch `<other>`.

    `from x import f` copies the reference into the importing module's namespace, so patching
    `x.f` leaves that copy -- and every call through it -- untouched. Patching the *defining*
    module is only safe when callers reach the name as an attribute (`x.f()`), which resolves on
    the module object at call time.

    This is what made `sdcm.sct_config.types._check_file_exists` a silent no-op: the sole caller,
    `sdcm.sct_config.config.check_required_files`, had done `from ...types import _check_file_exists`.
    It passed locally only because the developer had the credential file the patch exists to skip.
    """
    module_path, _, attr = target.rpartition(".")

    shadowing = []
    sdcm_root = pathlib.Path(sct_abs_path("sdcm"))
    assert sdcm_root.is_dir(), f"cannot find the sdcm package at {sdcm_root}"
    for path in sorted(sdcm_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        module_name = str(path.relative_to(sdcm_root.parent).with_suffix("")).replace("/", ".")
        if module_name == module_path:
            continue
        imports_it = any(
            isinstance(node, ast.ImportFrom)
            and node.module == module_path
            and any(alias.name == attr and alias.asname is None for alias in node.names)
            for node in ast.walk(tree)
        )
        if not imports_it:
            continue
        calls_it_bare = any(
            isinstance(node, ast.Name) and node.id == attr and isinstance(node.ctx, ast.Load) for node in ast.walk(tree)
        )
        if calls_it_bare:
            shadowing.append(module_name)

    assert not shadowing, (
        f"{target} is patched, but {shadowing} do `from {module_path} import {attr}` and call it "
        f"directly, so the patch never reaches them. Patch {shadowing[0]}.{attr} instead."
    )
