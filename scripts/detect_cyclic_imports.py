#!/usr/bin/env python3
"""Detect cyclic imports among project Python modules.

Statically parses Python files with ast, builds a directed graph of
project-internal imports, and reports import edges that belong to
strongly connected components.
"""

from __future__ import annotations

import argparse
import ast
import os
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ALLOWLIST = REPO_ROOT / "data" / "known_cyclic_imports.yml"
ALLOWLIST_HEADER = """\
# Auto-generated allowlist of known cyclic import edges.
# Each entry is a directed edge (source -> target) that forms part of a cycle.
# An empty list means the repository has no import-time cycles left to grandfather;
# deferred (function-level) imports are out of scope, see --include-deferred.
# To add: run `python scripts/detect_cyclic_imports.py --update-allowlist`
# To fix: resolve the cycle and remove both directional edges.
"""

# Pruned only at the repository root, so that a nested package sharing one of these
# names (e.g. unit_tests/unit/docker/) still takes part in the graph.
EXCLUDED_TOP_LEVEL_DIRS = frozenset(
    {
        "docker",
        "docs",
        "jenkins-pipelines",
        "test-cases",
        "configurations",
        "defaults",
        "data",
        "data_dir",
        "templates",
        "argus_report_templates",
        "rule-tests",
        "rules",
        "skills",
        "tree-sitter-groovy",
        "kafka-stack-docker-compose",
        "jupyter",
        "examples",
        "scylla-qa-internal",
    }
)

# Pruned at any depth — these never hold importable project modules.
EXCLUDED_DIR_NAMES = frozenset({"__pycache__", "node_modules"})


@dataclass
class ImportEdge:
    source: str
    target: str
    file: str
    line: int


@dataclass
class ModuleImports:
    targets: dict[str, list[int]] = field(default_factory=dict)

    def add(self, target: str, line: int) -> None:
        self.targets.setdefault(target, []).append(line)


class ImportVisitor(ast.NodeVisitor):
    """Collect import targets with line numbers.

    Excludes:
    - Imports inside TYPE_CHECKING guards (not runtime imports)
    - Imports inside function/method bodies (deferred, not import-time cycles)
      unless include_deferred=True
    - Edges from a module to its own ancestor packages (implicit, never a real cycle)
    """

    def __init__(self, source: str, package: str, *, include_deferred: bool = False) -> None:
        self.source = source
        self.package = package
        self.imports = ModuleImports()
        self._include_deferred = include_deferred

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            self._add_target(alias.name, node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        target = self._resolve_import_from(node)
        if not target:
            return
        self._add_target(target, node.lineno)
        # `from pkg import mod` binds the submodule `pkg.mod`, not just `pkg`, so the
        # real edge points at `pkg.mod`. Names that are plain attributes (classes,
        # constants) resolve to module names that do not exist and get dropped by the
        # internal-module filter in extract_imports().
        for alias in node.names:
            if alias.name != "*":
                self._add_target(f"{target}.{alias.name}", node.lineno)

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        if _is_type_checking_guard(node.test):
            for item in node.orelse:
                self.visit(item)
            return
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        if self._include_deferred:
            self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        if self._include_deferred:
            self.generic_visit(node)

    def _add_target(self, target: str, line: int) -> None:
        # Importing a module always initialises its ancestor packages first, so an edge
        # from a module up to one of its own ancestors is structural rather than a real
        # dependency. Keeping it would make every re-exporting `__init__.py` look cyclic.
        if target == self.source or self.source.startswith(f"{target}."):
            return
        self.imports.add(target, line)

    def _resolve_import_from(self, node: ast.ImportFrom) -> str | None:
        module = node.module or ""
        if node.level == 0:
            return module

        package_parts = self.package.split(".")
        if node.level > len(package_parts):
            return None

        base_parts = package_parts[: len(package_parts) - node.level + 1]
        if module:
            base_parts.extend(module.split("."))
        return ".".join(base_parts)


def extract_imports(
    project_root: Path,
    *,
    include_deferred: bool = False,
) -> tuple[dict[str, set[str]], list[ImportEdge], list[str]]:
    """Walk project Python files.

    Returns (adjacency dict, edges with locations, unparseable files). A file that
    cannot be parsed contributes no edges, so the caller must treat a non-empty
    third element as "the graph is incomplete" rather than ignoring it.
    """
    graph: dict[str, set[str]] = {}
    edges: list[ImportEdge] = []
    parse_errors: list[str] = []
    project_root = project_root.resolve()

    internal_modules = _discover_internal_modules(project_root)

    for path in sorted(_iter_python_files(project_root)):
        source = _module_name(path, project_root)
        package = source if path.name == "__init__.py" else source.rsplit(".", 1)[0] if "." in source else ""
        graph.setdefault(source, set())

        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            parse_errors.append(f"{path.relative_to(project_root)}: {exc}")
            continue

        visitor = ImportVisitor(source=source, package=package, include_deferred=include_deferred)
        visitor.visit(tree)

        rel_path = str(path.relative_to(project_root))
        for target, lines in visitor.imports.targets.items():
            if target in internal_modules and target != source:
                graph[source].add(target)
                for line in lines:
                    edges.append(ImportEdge(source=source, target=target, file=rel_path, line=line))

    for targets in list(graph.values()):
        for target in targets:
            graph.setdefault(target, set())

    return graph, edges, parse_errors


def find_cycles(graph: dict[str, set[str]]) -> list[tuple[str, str]]:
    """Return list of (source, target) edges that participate in any cycle.

    An edge is cyclic only when both ends sit in the *same* strongly connected
    component. Testing against the union of all components would also report
    one-way bridges between two disjoint cycles, which are not cyclic themselves.
    """
    cyclic_edges = [
        (source, target)
        for component in _strongly_connected_components(graph)
        if len(component) > 1
        for source in component
        for target in graph.get(source, set()) & component
    ]
    return sorted(cyclic_edges)


def load_allowlist(path: Path) -> set[tuple[str, str]]:
    """Load known_cyclic_imports.yml, return set of (source, target) pairs."""
    if not path.exists():
        return set()

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    allowlist_edges = data.get("edges") or []
    return {
        (edge["source"], edge["target"])
        for edge in allowlist_edges
        if isinstance(edge, dict) and edge.get("source") and edge.get("target")
    }


def main() -> int:
    """CLI entry point. Returns exit code."""
    parser = argparse.ArgumentParser(description="Detect cyclic imports in Python project.")
    parser.add_argument("--update-allowlist", action="store_true", help="write current cyclic edges to allowlist")
    parser.add_argument("--verbose", action="store_true", help="print all cyclic edges with file locations")
    parser.add_argument(
        "--include-deferred",
        action="store_true",
        help="also count imports inside functions (for diagnostics, not used by pre-commit)",
    )
    parser.add_argument("--allowlist", type=Path, default=DEFAULT_ALLOWLIST, help="override allowlist path")
    args = parser.parse_args()

    if args.update_allowlist and args.include_deferred:
        # The pre-commit check always runs without --include-deferred, so writing the
        # deferred-inclusive edge set would grandfather import-time cycles that were
        # never actually present, silently disabling the hook.
        parser.error("--update-allowlist cannot be combined with --include-deferred")

    graph, all_edges, parse_errors = extract_imports(REPO_ROOT, include_deferred=args.include_deferred)
    if parse_errors:
        print("ERROR: could not parse the following file(s), import graph is incomplete:\n", file=sys.stderr)
        for message in parse_errors:
            print(f"  {message}", file=sys.stderr)
        return 1

    cyclic_edges = find_cycles(graph)

    if args.update_allowlist:
        _write_allowlist(args.allowlist, cyclic_edges)
        print(f"Wrote {len(cyclic_edges)} cyclic import edge(s) to {args.allowlist}")
        return 0

    allowlist = load_allowlist(args.allowlist)
    new_edges = sorted(set(cyclic_edges) - allowlist)

    if args.verbose:
        _print_verbose_cycles(cyclic_edges, all_edges)

    if new_edges:
        _print_new_cycles_error(new_edges, graph, all_edges)
        return 1

    return 0


def _is_type_checking_guard(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _discover_internal_modules(project_root: Path) -> set[str]:
    """Build the set of all module names that correspond to project files."""
    modules = set()
    for path in _iter_python_files(project_root):
        modules.add(_module_name(path, project_root))
    return modules


def _iter_python_files(project_root: Path) -> Iterator[Path]:
    """Yield .py files in the project, excluding non-source directories."""
    for dirpath, dirnames, filenames in os.walk(project_root):
        at_root = Path(dirpath) == project_root
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".")
            and "venv" not in d
            and d not in EXCLUDED_DIR_NAMES
            and not (at_root and d in EXCLUDED_TOP_LEVEL_DIRS)
        ]
        for filename in sorted(filenames):
            if filename.endswith(".py"):
                yield Path(dirpath) / filename


def _module_name(path: Path, project_root: Path) -> str:
    relative = path.relative_to(project_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else path.stem


def _strongly_connected_components(graph: dict[str, set[str]]) -> list[set[str]]:
    index = 0
    stack: list[str] = []
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    on_stack: set[str] = set()
    components: list[set[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for target in graph.get(node, set()):
            if target not in indices:
                strongconnect(target)
                lowlinks[node] = min(lowlinks[node], lowlinks[target])
            elif target in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[target])

        if lowlinks[node] == indices[node]:
            component = set()
            while True:
                target = stack.pop()
                on_stack.remove(target)
                component.add(target)
                if target == node:
                    break
            components.append(component)

    for node in sorted(graph):
        if node not in indices:
            strongconnect(node)

    return components


def _edges_by_pair(all_edges: list[ImportEdge]) -> dict[tuple[str, str], list[ImportEdge]]:
    by_pair: dict[tuple[str, str], list[ImportEdge]] = {}
    for edge in all_edges:
        by_pair.setdefault((edge.source, edge.target), []).append(edge)
    return by_pair


def _write_allowlist(path: Path, edges: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"edges": [{"source": source, "target": target} for source, target in sorted(edges)]}
    path.write_text(ALLOWLIST_HEADER + yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _print_verbose_cycles(cyclic_edges: list[tuple[str, str]], all_edges: list[ImportEdge]) -> None:
    by_pair = _edges_by_pair(all_edges)
    for source, target in cyclic_edges:
        locations = by_pair.get((source, target), [])
        if locations:
            for loc in locations:
                print(f"{source} -> {target}  ({loc.file}:{loc.line})")
        else:
            print(f"{source} -> {target}")


def _print_new_cycles_error(
    new_edges: list[tuple[str, str]],
    graph: dict[str, set[str]],
    all_edges: list[ImportEdge],
) -> None:
    by_pair = _edges_by_pair(all_edges)
    print("ERROR: New cyclic import edge(s) detected:\n", file=sys.stderr)
    for source, target in new_edges:
        locations = by_pair.get((source, target), [])
        if locations:
            for loc in locations:
                print(f"  {source} -> {target}  ({loc.file}:{loc.line})", file=sys.stderr)
        else:
            print(f"  {source} -> {target} (NEW — not in allowlist)", file=sys.stderr)
    print(file=sys.stderr)

    cycle_path = _find_cycle_path(new_edges[0][0], new_edges[0][1], graph)
    if cycle_path:
        print(f"Cycle path: {' -> '.join(cycle_path)}\n", file=sys.stderr)

    print("To add to allowlist: uv run python scripts/detect_cyclic_imports.py --update-allowlist", file=sys.stderr)
    print("To fix: break the cycle by making one import lazy or moving it under TYPE_CHECKING.", file=sys.stderr)


def _find_cycle_path(source: str, target: str, graph: dict[str, set[str]]) -> list[str]:
    path = _find_path(target, source, graph)
    if not path:
        return []
    return [source, *path]


def _find_path(start: str, end: str, graph: dict[str, set[str]]) -> list[str]:
    stack: list[tuple[str, Iterator[str], list[str]]] = [(start, iter(sorted(graph.get(start, set()))), [start])]
    visited = {start}

    while stack:
        node, targets, path = stack[-1]
        if node == end:
            return path
        for target in targets:
            if target not in visited:
                visited.add(target)
                stack.append((target, iter(sorted(graph.get(target, set()))), [*path, target]))
                break
        else:
            stack.pop()
    return []


if __name__ == "__main__":
    sys.exit(main())
