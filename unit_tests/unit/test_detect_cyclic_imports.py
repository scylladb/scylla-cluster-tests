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

"""Unit tests for scripts/detect_cyclic_imports.py."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
from detect_cyclic_imports import extract_imports, find_cycles, load_allowlist  # noqa: E402


def _make_project(tmp_path: Path) -> Path:
    """Create a fake project root with a sdcm/ package. Returns the project root (tmp_path)."""
    sdcm_dir = tmp_path / "sdcm"
    sdcm_dir.mkdir(parents=True, exist_ok=True)
    return tmp_path


def _graph(tmp_path: Path) -> dict[str, set[str]]:
    """Extract imports and return just the graph dict."""
    graph, _edges = extract_imports(tmp_path)
    return graph


# ---------------------------------------------------------------------------
# Scenario 1 — basic import extraction
# ---------------------------------------------------------------------------


def test_extract_imports_basic(tmp_path):
    """extract_imports returns edge sdcm.foo -> sdcm.bar for a simple import."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text("from sdcm.bar import X\n", encoding="utf-8")
    (sdcm / "bar.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.foo" in graph
    assert "sdcm.bar" in graph["sdcm.foo"]


# ---------------------------------------------------------------------------
# Scenario 2 — TYPE_CHECKING guard excluded
# ---------------------------------------------------------------------------


def test_type_checking_imports_excluded(tmp_path):
    """Imports inside 'if TYPE_CHECKING:' must NOT appear as graph edges."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text(
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from sdcm.bar import Baz\n",
        encoding="utf-8",
    )
    (sdcm / "bar.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.foo" in graph
    assert "sdcm.bar" not in graph.get("sdcm.foo", set())


# ---------------------------------------------------------------------------
# Scenario 3 — relative import resolution
# ---------------------------------------------------------------------------


def test_relative_import_resolution(tmp_path):
    """'from .base import Foo' in sdcm.pkg.sub resolves to sdcm.pkg.base."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    pkg_dir = sdcm / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "sub.py").write_text("from .base import Foo\n", encoding="utf-8")
    (pkg_dir / "base.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.pkg.sub" in graph
    assert "sdcm.pkg.base" in graph["sdcm.pkg.sub"]


# ---------------------------------------------------------------------------
# Scenario 4 — cycle detection
# ---------------------------------------------------------------------------


def test_find_cycles_detects_cycle():
    """find_cycles reports both directed edges of a two-node cycle."""
    graph: dict[str, set[str]] = {
        "sdcm.a": {"sdcm.b"},
        "sdcm.b": {"sdcm.a"},
    }

    edges = find_cycles(graph)

    assert ("sdcm.a", "sdcm.b") in edges
    assert ("sdcm.b", "sdcm.a") in edges


# ---------------------------------------------------------------------------
# Scenario 5 — DAG has no false positives
# ---------------------------------------------------------------------------


def test_find_cycles_no_false_positives():
    """A simple directed acyclic graph must produce no cyclic edges."""
    graph: dict[str, set[str]] = {
        "sdcm.a": {"sdcm.b"},
        "sdcm.b": {"sdcm.c"},
        "sdcm.c": set(),
    }

    assert find_cycles(graph) == []


# ---------------------------------------------------------------------------
# Scenario 6 — allowlist suppresses known edges
# ---------------------------------------------------------------------------


def test_allowlist_suppresses_known_edges(tmp_path):
    """Edges present in the allowlist are excluded from new_edges."""
    allowlist_path = tmp_path / "known_cyclic_imports.yml"
    graph: dict[str, set[str]] = {
        "sdcm.a": {"sdcm.b"},
        "sdcm.b": {"sdcm.a"},
    }
    cyclic_edges = find_cycles(graph)

    data = {"edges": [{"source": src, "target": tgt} for src, tgt in cyclic_edges]}
    allowlist_path.write_text(yaml.safe_dump(data), encoding="utf-8")

    allowlist = load_allowlist(allowlist_path)
    new_edges = sorted(set(cyclic_edges) - allowlist)

    assert new_edges == []


def test_allowlist_reports_unlisted_edge(tmp_path):
    """An edge absent from the allowlist is reported as a new cycle."""
    allowlist_path = tmp_path / "known_cyclic_imports.yml"
    graph: dict[str, set[str]] = {
        "sdcm.a": {"sdcm.b"},
        "sdcm.b": {"sdcm.c"},
        "sdcm.c": {"sdcm.a"},
    }
    cyclic_edges = find_cycles(graph)

    partial_data = {
        "edges": [
            {"source": "sdcm.a", "target": "sdcm.b"},
            {"source": "sdcm.b", "target": "sdcm.a"},
        ]
    }
    allowlist_path.write_text(yaml.safe_dump(partial_data), encoding="utf-8")

    allowlist = load_allowlist(allowlist_path)
    new_edges = sorted(set(cyclic_edges) - allowlist)

    assert len(new_edges) > 0
    assert any("sdcm.c" in (src, tgt) for src, tgt in new_edges)


# ---------------------------------------------------------------------------
# Scenario 7 — __init__.py treated as package name
# ---------------------------------------------------------------------------


def test_init_py_treated_as_package(tmp_path):
    """__init__.py files appear as 'sdcm.pkg', never as 'sdcm.pkg.__init__'."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    pkg_dir = sdcm / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("from sdcm.other import X\n", encoding="utf-8")
    (sdcm / "other.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.pkg" in graph
    assert "sdcm.pkg.__init__" not in graph
    assert "sdcm.other" in graph["sdcm.pkg"]


# ---------------------------------------------------------------------------
# Bonus — stdlib / third-party imports not in graph
# ---------------------------------------------------------------------------


def test_stdlib_imports_not_in_graph(tmp_path):
    """Standard-library imports must not create graph edges."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text("import os\nimport sys\n", encoding="utf-8")

    graph = _graph(root)

    assert "os" not in graph
    assert "sys" not in graph
    assert graph.get("sdcm.foo", set()) == set()


def test_third_party_imports_not_in_graph(tmp_path):
    """Third-party imports must not create graph edges."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text("import boto3\nimport yaml\n", encoding="utf-8")

    graph = _graph(root)

    assert "boto3" not in graph
    assert "yaml" not in graph
    assert graph.get("sdcm.foo", set()) == set()


def test_syntax_error_file_skipped(tmp_path):
    """Files with SyntaxErrors are silently skipped; other files still processed."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "bad.py").write_text("def broken(:\n    pass\n", encoding="utf-8")
    (sdcm / "good.py").write_text("from sdcm.other import X\n", encoding="utf-8")
    (sdcm / "other.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.good" in graph
    assert "sdcm.other" in graph["sdcm.good"]
    assert graph.get("sdcm.bad", set()) == set()


def test_function_level_imports_excluded(tmp_path):
    """Imports inside function bodies are deferred and don't create import-time cycles."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text(
        "from sdcm.bar import top_level_thing\n"
        "\n"
        "def some_function():\n"
        "    from sdcm.baz import deferred_thing\n"
        "    return deferred_thing()\n",
        encoding="utf-8",
    )
    (sdcm / "bar.py").write_text("", encoding="utf-8")
    (sdcm / "baz.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.bar" in graph["sdcm.foo"]
    assert "sdcm.baz" not in graph["sdcm.foo"]


def test_method_level_imports_excluded(tmp_path):
    """Imports inside class methods are also deferred and excluded."""
    root = _make_project(tmp_path)
    sdcm = root / "sdcm"
    (sdcm / "foo.py").write_text(
        "from sdcm.bar import X\n"
        "\n"
        "class MyClass:\n"
        "    def method(self):\n"
        "        from sdcm.baz import Y\n"
        "        return Y()\n",
        encoding="utf-8",
    )
    (sdcm / "bar.py").write_text("", encoding="utf-8")
    (sdcm / "baz.py").write_text("", encoding="utf-8")

    graph = _graph(root)

    assert "sdcm.bar" in graph["sdcm.foo"]
    assert "sdcm.baz" not in graph["sdcm.foo"]
