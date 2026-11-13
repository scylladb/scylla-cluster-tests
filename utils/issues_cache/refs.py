"""Find the issue references in this repo's branches, and read/write the ref manifests."""

import ast
import datetime
import json
import re
import subprocess
import warnings
from pathlib import Path

MANIFEST_VERSION = 1
DEFAULT_OWNER = "scylladb"
DEFAULT_REPO = "scylladb"
# Only issues in these orgs are cached and marked: the comment fallback must never
# post on third-party projects.
ALLOWED_OWNERS = {"scylladb"}

# SCT call sites whose arguments may use the short forms ("1234", "#1234", "repo#12").
SCT_CALLS = {"SkipPerIssues", "decorate_with_context_if_issues_open"}
SCT_BRANCHES = r"^(master|branch-\d[\w.]*|branch-perf-v\d+|manager-[\d.]+)$"

FULL_GITHUB = re.compile(
    r"^(?:https?://github\.com/(?P<u1>[\w.-]+)/(?P<r1>[\w.-]+)/(?:issues|pull)/|(?P<u2>[\w.-]+)/(?P<r2>[\w.-]+)#)(?P<n>\d+)$",
    re.IGNORECASE,
)
SHORT_GITHUB = re.compile(r"^(?:(?:(?P<u>[\w.-]+)/)?(?P<r>[\w.-]+)#|#)?(?P<n>\d+)$")
JIRA = re.compile(r"^(?:jira:|https?://scylladb\.atlassian\.net/browse/)(?P<k>[A-Z][A-Z0-9]+-\d+)$", re.IGNORECASE)
YAML_ISSUE = re.compile(r"^\s*-?\s*issue:\s*['\"]?(?P<v>[^'\"#\s]+)")


def normalize(value: str, short_forms: bool = False) -> str | None:
    """Return `owner/repo#N` or `jira:KEY-N`, or None when `value` isn't an issue reference.

    Same defaults as `parse_issue()` in sdcm/utils/issues.py. The repo name is kept as
    written, because readers look up `issues/<owner>_<repo>.csv` by the name in the ref.
    """
    value = value.strip()
    if m := JIRA.match(value):
        return f"jira:{m['k'].upper()}"
    if m := FULL_GITHUB.match(value):
        return f"{m['u1'] or m['u2']}/{m['r1'] or m['r2']}#{m['n']}"
    if short_forms and (m := SHORT_GITHUB.match(value)):
        return f"{m['u'] or DEFAULT_OWNER}/{m['r'] or DEFAULT_REPO}#{m['n']}"
    return None


def _call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def extract_python(source: str, calls: set[str]) -> set[str]:
    """Refs in a Python module.

    Full forms count anywhere as a whole string literal: refs reach `SkipPerIssues` through
    variables too (e.g. the nemesis `skip_issues` list). Short forms count only inside the
    given calls, where a bare number means an issue.
    """
    try:
        # branch-2025.1 and older have invalid escapes in non-raw strings (tester.py, sla_tests.py, ...)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(source)
    except SyntaxError:
        return set()
    refs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and (ref := normalize(node.value)):
            refs.add(ref)
        elif isinstance(node, ast.Call) and _call_name(node.func) in calls:
            for arg in [*node.args, *(k.value for k in node.keywords)]:
                for sub in ast.walk(arg):
                    if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                        if ref := normalize(sub.value, short_forms=True):
                            refs.add(ref)
    return refs


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


def scan_branch(repo_dir: Path, ref: str, calls: set[str]) -> dict[str, set[str]]:
    """Map each ref found on `ref` (a git revision) to the files it appears in."""
    paths = []
    for line in _git(repo_dir, "ls-tree", "-r", ref).decode().splitlines():
        meta, _, path = line.partition("\t")
        if meta.split()[1] != "blob":  # skip submodules
            continue
        if (path.endswith(".py") and not path.startswith("unit_tests/")) or path.endswith("issue_by_keyword.yaml"):
            paths.append(path)
    blobs = _git(repo_dir, "cat-file", "--batch", stdin="".join(f"{ref}:{p}\n" for p in paths).encode())
    found: dict[str, set[str]] = {}
    pos = 0
    for path in paths:
        header_end = blobs.index(b"\n", pos)
        size = int(blobs[pos:header_end].split()[2])
        source = blobs[header_end + 1 : header_end + 1 + size].decode(errors="replace")
        pos = header_end + 1 + size + 1
        refs = extract_yaml(source) if path.endswith(".yaml") else extract_python(source, calls)
        for r in refs:
            if r.startswith("jira:") or r.split("/", 1)[0].lower() in ALLOWED_OWNERS:
                found.setdefault(r, set()).add(path)
    return found


def build_manifest(repo: str, label: str, label_description: str, per_branch: dict[str, dict[str, set[str]]]) -> dict:
    refs: dict[str, list[dict]] = {}
    for branch, found in sorted(per_branch.items()):
        for ref, files in found.items():
            refs.setdefault(ref, []).extend({"branch": branch, "file": f} for f in sorted(files))
    return {
        "version": MANIFEST_VERSION,
        "repo": repo,
        "label": label,
        "label_description": label_description,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "refs": dict(sorted(refs.items())),
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
