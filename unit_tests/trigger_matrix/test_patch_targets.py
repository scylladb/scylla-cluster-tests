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

"""Guards for the string patch targets used by this test package.

`mock.patch` takes a dotted string, so a target that drifts from the code does not raise --
it happily replaces an attribute nobody reads. For these tests that failure mode is expensive:
the autouse `stub_image_lookups` fixture in conftest is the only thing keeping the suite off
the real AWS, GCE, Azure and OCI image APIs. If it silently stops applying, the tests still
pass, just slower and against live clouds.

Splitting `sdcm/utils/trigger_matrix` into a package is exactly the kind of change that breaks
these targets, so the rules are enforced here rather than left to review.
"""

import ast
import importlib
import pathlib

import pytest

from sdcm import sct_abs_path

PACKAGE = "sdcm.utils.trigger_matrix"
PACKAGE_DIR = pathlib.Path(sct_abs_path("sdcm/utils/trigger_matrix"))
TESTS_DIR = pathlib.Path(__file__).parent


def _patch_targets() -> list[str]:
    """Every `"sdcm.utils.trigger_matrix..."` string literal passed to a patch-like call."""
    targets = set()
    for path in sorted(TESTS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name not in ("patch", "object", "setattr"):
                continue
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    if arg.value.startswith(f"{PACKAGE}."):
                        targets.add(arg.value)
    return sorted(targets)


TARGETS = _patch_targets()


def test_patch_targets_were_found():
    """Guard the guard: an AST change that stops collecting targets must not pass silently."""
    assert TARGETS, "no trigger_matrix patch targets found -- has the collector drifted?"


def _resolve(target: str):
    """Resolve a patch target the way `mock.patch` does: longest importable prefix, then attrs.

    Targets like `...jenkins_client.time.sleep` reach through a module attribute, so the
    importable part is not simply everything before the last dot.
    """
    parts = target.split(".")
    for split in range(len(parts) - 1, 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:split]))
        except ImportError:
            continue
        for attr in parts[split:]:
            obj = getattr(obj, attr)
        return obj
    raise ImportError(f"no importable prefix in {target}")


@pytest.mark.parametrize("target", TARGETS)
def test_patch_target_resolves(target):
    """The dotted path must resolve, or `mock.patch` raises when the test runs."""
    assert _resolve(target) is not None


@pytest.mark.parametrize("target", TARGETS)
def test_patch_target_is_never_the_package_itself(target):
    """Patching the package re-exports nothing: `__init__` binds copies, callers read their own.

    Keeping every target on a submodule is what makes a narrow `__init__.py` load-bearing
    rather than decorative.
    """
    module_path, _, _ = target.rpartition(".")
    assert module_path != PACKAGE, (
        f"{target} patches the package façade, which only holds a copy of the name. "
        f"Patch the submodule whose code calls it instead."
    )


@pytest.mark.parametrize("target", TARGETS)
def test_no_submodule_shadows_the_patch_target(target):
    """No sibling may `from <target module> import <attr>` and then call it bare.

    `from x import f` copies the reference into the importing module's namespace, so patching
    `x.f` leaves that copy -- and every call through it -- untouched. Patching the defining
    module is only safe while callers reach the name as an attribute (`x.f()`), resolved on the
    module object at call time. That is why `resolution` and `matrix` import `images` itself.
    """
    module_path, _, attr = target.rpartition(".")
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        module_name = f"{PACKAGE}.{path.stem}" if path.stem != "__init__" else PACKAGE
        if module_name == module_path:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module == module_path:
                shadowed = [a.name for a in node.names if a.name == attr]
                assert not shadowed, (
                    f"{module_name} does `from {module_path} import {attr}`, so patching "
                    f"{target} would not affect its calls. Import the module and call "
                    f"`{path.stem}.{attr}(...)`, or patch {module_name}.{attr} instead."
                )
