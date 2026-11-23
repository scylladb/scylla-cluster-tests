"""Find the issue references in this repo's branches, and read/write the ref manifests."""

import ast
import datetime
import json
import logging
import re
import subprocess
import warnings
from pathlib import Path

LOGGER = logging.getLogger(__name__)

MANIFEST_VERSION = 1
# Only issues in these orgs are cached and marked: the comment fallback must never
# post on third-party projects.
ALLOWED_OWNERS = {"scylladb"}

SCT_BRANCHES = r"^(master|branch-\d[\w.]*|branch-perf-v\d+|manager-[\d.]+)$"

GITHUB = re.compile(
    r"^(?:https?://github\.com/(?P<u1>[\w.-]+)/(?P<r1>[\w.-]+)/(?:issues|pull)/|(?P<u2>[\w.-]+)/(?P<r2>[\w.-]+)#)(?P<n>\d+)$",
    re.IGNORECASE,
)
JIRA = re.compile(r"^(?:jira:|https?://scylladb\.atlassian\.net/browse/)(?P<k>[A-Z][A-Z0-9]+-\d+)$", re.IGNORECASE)
YAML_ISSUE = re.compile(r"^\s*-?\s*issue:\s*['\"]?(?P<v>[^'\"#\s]+)")
# the end of a string piece that a ref built at runtime starts with, e.g. f"scylladb/scylladb#{n}",
# f"https://github.com/scylladb/{repo}/issues/{n}", f"jira:SCT-{n}":
# the literal scan can't see such a ref, so the scan reports where it is
RUNTIME_REF = re.compile(r"(?:/(?:issues|pull)/|[\w.-]+/[\w.-]+#|jira:[A-Z0-9-]*|browse/[A-Z0-9-]*)$", re.IGNORECASE)
# a %-conversion: %s, %d, %5d, %(issue)s, ...
PERCENT_CONVERSION = re.compile(r"%(?:\([^)]*\))?[-#0 +]*\d*(?:\.\d+)?[a-zA-Z]")
# the reader builds `jira:<key>` from its argument and holds no refs of its own;
# .github/ holds CI helpers that build GitHub API URLs (`/repos/{repo}/issues/{n}/labels`)
SKIPPED_PATHS = ("unit_tests/", "sdcm/utils/issues.py", ".github/")


def normalize(value: str) -> str | None:
    """Return `owner/repo#N` or `jira:KEY-N`, or None when `value` isn't a full issue reference.

    Only full forms (a URL, `owner/repo#N`, `jira:KEY-N`) count: no active SCT branch uses
    bare numbers, and a marker that does still works through the readers' live-API fallback.
    The repo name is kept as written, because readers look up `issues/<owner>_<repo>.csv`
    by the name in the ref.
    """
    value = value.strip()
    if m := JIRA.match(value):
        return f"jira:{m['k'].upper()}"
    if m := GITHUB.match(value):
        return f"{m['u1'] or m['u2']}/{m['r1'] or m['r2']}#{m['n']}"
    return None


def _is_str(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def _runtime_prefixes(node: ast.AST) -> list[str]:
    """The literal pieces that precede a value in f"...{x}", "..." + x, "...%s" % x or "...{}".format(x)."""
    if isinstance(node, ast.JoinedStr):
        return [v.value for v in node.values[:-1] if isinstance(v, ast.Constant)]
    if isinstance(node, ast.BinOp) and _is_str(node.left):
        return [node.left.value] if isinstance(node.op, ast.Add) else PERCENT_CONVERSION.split(node.left.value)[:-1]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        return re.split(r"\{[^}]*\}", node.func.value.value)[:-1] if _is_str(node.func.value) else []
    return []


def extract_python(source: str, path: str = "<source>") -> tuple[set[str], list[int]]:
    """Refs in a Python module: every string literal that is a whole issue reference, and
    the lines of refs built at runtime, which this scan can't resolve.

    Literals anywhere count, not only `SkipPerIssues` arguments: refs reach it through
    variables too (e.g. the nemesis `skip_issues` list).
    """
    try:
        # branch-2025.1 and older have invalid escapes in non-raw strings (tester.py, sla_tests.py, ...)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(source)
    except SyntaxError as exc:
        # e.g. syntax newer than this interpreter: its refs would be missed, so say so
        LOGGER.warning("skipping %s: %s", path, exc)
        return set(), []
    refs, runtime = set(), []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and (ref := normalize(node.value)):
            refs.add(ref)
        elif any(RUNTIME_REF.search(piece) for piece in _runtime_prefixes(node)):
            LOGGER.warning("%s:%d: issue ref built at runtime, not cached: %s", path, node.lineno, ast.unparse(node))
            runtime.append(node.lineno)
    return refs, sorted(runtime)


def extract_yaml(source: str) -> set[str]:
    """Refs in `issue:` entries, e.g. sdcm/utils/issues_by_keyword/issue_by_keyword.yaml."""
    return {ref for line in source.splitlines() if (m := YAML_ISSUE.match(line)) and (ref := normalize(m["v"]))}


def _git(repo_dir: Path, *args: str, stdin: bytes | None = None) -> bytes:
    return subprocess.run(["git", "-C", str(repo_dir), *args], input=stdin, capture_output=True, check=True).stdout


def select_branches(repo_dir: Path, pattern: str, max_age_days: int, remote: str = "origin") -> list[str]:
    """Remote branches matching `pattern` with a commit in the last `max_age_days` days."""
    cutoff = datetime.datetime.now(datetime.UTC).timestamp() - max_age_days * 86400
    out = _git(repo_dir, "for-each-ref", f"refs/remotes/{remote}", "--format=%(refname:lstrip=3) %(committerdate:unix)")
    branches = []
    for line in out.decode().splitlines():
        name, _, stamp = line.rpartition(" ")
        if re.match(pattern, name) and int(stamp) >= cutoff:
            branches.append(name)
    return sorted(branches)


def scan_branch(repo_dir: Path, ref: str) -> tuple[dict[str, set[str]], list[str]]:
    """Map each ref found on `ref` (a git revision) to the files it appears in; also return
    the `path:line` of each ref built at runtime (see `extract_python`).
    """
    paths = []
    for line in _git(repo_dir, "ls-tree", "-r", ref).decode().splitlines():
        meta, _, path = line.partition("\t")
        if meta.split()[1] != "blob":  # skip submodules
            continue
        if (path.endswith(".py") and not path.startswith(SKIPPED_PATHS)) or path.endswith("issue_by_keyword.yaml"):
            paths.append(path)
    blobs = _git(repo_dir, "cat-file", "--batch", stdin="".join(f"{ref}:{p}\n" for p in paths).encode())
    found: dict[str, set[str]] = {}
    unresolved: list[str] = []
    pos = 0
    for path in paths:
        header_end = blobs.index(b"\n", pos)
        size = int(blobs[pos:header_end].split()[2])
        source = blobs[header_end + 1 : header_end + 1 + size].decode(errors="replace")
        pos = header_end + 1 + size + 1
        if path.endswith(".yaml"):
            refs = extract_yaml(source)
        else:
            refs, runtime = extract_python(source, f"{ref}:{path}")
            unresolved += [f"{path}:{line}" for line in runtime]
        for r in refs:
            if r.startswith("jira:") or r.split("/", 1)[0].lower() in ALLOWED_OWNERS:
                found.setdefault(r, set()).add(path)
    return found, unresolved


def build_manifest(
    repo: str,
    label: str,
    label_description: str,
    per_branch: dict[str, tuple[dict[str, set[str]], list[str]]],
) -> dict:
    refs: dict[str, list[dict]] = {}
    unresolved: list[str] = []
    for branch, (found, runtime) in sorted(per_branch.items()):
        for ref, files in found.items():
            refs.setdefault(ref, []).extend({"branch": branch, "file": f} for f in sorted(files))
        unresolved += [f"{branch}:{site}" for site in runtime]
    return {
        "version": MANIFEST_VERSION,
        "repo": repo,
        "label": label,
        "label_description": label_description,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "refs": dict(sorted(refs.items())),
        "unresolved": unresolved,
    }


def load_manifests(refs_dir: Path) -> list[dict]:
    manifests = [json.loads(p.read_text()) for p in sorted(refs_dir.glob("*.json"))]
    if not manifests:
        raise SystemExit(f"no manifests in {refs_dir}: refusing to refresh from an empty union")
    for m in manifests:
        if m.get("version") != MANIFEST_VERSION:
            raise SystemExit(f"unsupported manifest version {m.get('version')} from {m.get('repo')}")
    return manifests


def github_union(manifests: list[dict]) -> dict[str, dict[str, list[str]]]:
    """Map each GitHub ref to {label: [branches]} across all consumers."""
    union: dict[str, dict[str, list[str]]] = {}
    for m in manifests:
        for ref, places in m["refs"].items():
            if ref.startswith("jira:"):
                continue
            union.setdefault(ref, {})[m["label"]] = sorted({p["branch"] for p in places})
    return union
