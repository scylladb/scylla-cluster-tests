#!/usr/bin/env python3
"""Referenced-only GitHub issue cache (QAINFRA-104 / SCT-1172).

Instead of crawling every issue and PR of every repo, cache only the issues that
test code actually references, and mark each one as in use.

    scan            Scan this repo's branches for issue references and write the
                    ref manifest. Each consumer repo (SCT, dtest) runs its own copy of
                    the scanner, so neither needs to read the other's source.
    compare         Shadow check: compare the new CSVs with the old full cache.
    refresh-github  Read the union of all manifests, fetch the state of every GitHub
                    ref, write the reader-compatible CSVs and reconcile the in-use
                    labels (or one marker comment where labeling isn't allowed).

Stdlib only. S3 transfers are left to `aws s3` in the workflow.

Manifest format (version 1). dtest writes the same format, so keep the two in sync:

    {"version": 1, "repo": "scylladb/scylla-cluster-tests", "label": "used-by-sct",
     "label_description": "...", "generated_at": "2026-10-06T00:00:00+00:00",
     "refs": {"scylladb/scylladb#1234": [{"branch": "master", "file": "sdcm/tester.py"}],
              "jira:SCYLLADB-42": [...]}}

CSV contract (what `sdcm/utils/issues.py` and dtest's `tools/github_issues.py` read):
`<owner>_<repo>.csv`, no header, rows `number,state,labels(|-joined),title`, plus an
`pull-requests/<owner>_<repo>.csv` that must exist. All rows go into the issues file
and the PR file stays empty, because readers concatenate the two without a newline.

Local testing:
    python .github/scripts/issues_cache.py scan --repo-dir . --out /tmp/refs/sct.json
    GH_TOKEN=$(gh auth token) python .github/scripts/issues_cache.py refresh-github \
        --refs-dir /tmp/refs --out-dir /tmp/out --labels dry-run
"""

import argparse
import ast
import csv
import datetime
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

LOGGER = logging.getLogger("issues_cache")

MANIFEST_VERSION = 1
DEFAULT_OWNER = "scylladb"
DEFAULT_REPO = "scylladb"
# Only issues in these orgs are cached and marked: the comment fallback must never
# post on third-party projects.
ALLOWED_OWNERS = {"scylladb"}
MARKER = "<!-- sct-issue-cache -->"
LABEL_COLOR = "c5def5"
SHRINK_LIMIT = 0.5  # abort when the ref union shrinks by more than this
FAILURE_LIMIT = 0.2  # abort when more than this share of fetches fail

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

    Full forms count anywhere as a whole string literal. Short forms count only
    inside the given calls, where a bare number means an issue.
    """
    try:
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


def _owner(ref: str) -> str:
    return ref.split("/", 1)[0].lower()


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
            if r.startswith("jira:") or _owner(r) in ALLOWED_OWNERS:
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
            branches = sorted({p["branch"] for p in places})
            union.setdefault(ref, {})[m["label"]] = branches
    return union


class GitHub:
    """Minimal GitHub REST client; tests replace `request`."""

    def __init__(self, token: str, api: str = "https://api.github.com"):
        self.token, self.api = token, api

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, object]:
        req = urllib.request.Request(
            f"{self.api}{path}",
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as err:
            return err.code, None


def split_ref(ref: str) -> tuple[str, str, int]:
    owner_repo, number = ref.split("#")
    owner, repo = owner_repo.split("/")
    return owner, repo, int(number)


def fetch_states(gh: GitHub, refs: list[str]) -> tuple[dict[str, dict], list[str]]:
    states, failed = {}, []
    for ref in refs:
        owner, repo, number = split_ref(ref)
        status, data = gh.request("GET", f"/repos/{owner}/{repo}/issues/{number}")
        if status != 200:
            LOGGER.warning("can't fetch %s: HTTP %s", ref, status)
            failed.append(ref)
            continue
        merged = (data.get("pull_request") or {}).get("merged_at")
        states[ref] = {
            "state": "merged" if merged else data["state"],
            "labels": [label["name"] for label in data["labels"]],
            "title": data["title"],
            "is_pr": "pull_request" in data,
        }
    return states, failed


def write_csvs(out_dir: Path, states: dict[str, dict]) -> list[Path]:
    """Write the CSV contract described in the module docstring; return the written files."""
    rows: dict[str, list] = {}
    for ref, st in sorted(states.items(), key=lambda kv: (kv[0].split("#")[0], int(kv[0].split("#")[1]))):
        owner, repo, number = split_ref(ref)
        rows.setdefault(f"{owner}_{repo}", []).append([number, st["state"], "|".join(st["labels"]), st["title"]])
    (out_dir / "pull-requests").mkdir(parents=True, exist_ok=True)
    written = []
    for key, key_rows in rows.items():
        issues_file = out_dir / f"{key}.csv"
        with issues_file.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerows(key_rows)
        prs_file = out_dir / "pull-requests" / f"{key}.csv"
        prs_file.write_text("")
        written += [issues_file, prs_file]
    return written


def check_guards(union_size: int, previous_size: int, failed: int) -> None:
    if previous_size and union_size < previous_size * (1 - SHRINK_LIMIT):
        raise SystemExit(f"ref union shrank from {previous_size} to {union_size}: refusing to update")
    if union_size and failed / union_size > FAILURE_LIMIT:
        raise SystemExit(f"{failed}/{union_size} fetches failed: refusing to update")


def comment_body(wanted: dict[str, list[str]], repo_of_label: dict[str, str]) -> str:
    if not wanted:
        return f"{MARKER}\nNo longer used by test automation: no SCT or dtest references remain."
    lines = [
        MARKER,
        "**Used by test automation.** This item's open/closed state and its "
        "`sct-<version>-skip` / `dtest/<version>-skip` labels decide which tests run:",
        "",
    ]
    lines += [
        f"- **{label}**: {repo_of_label.get(label, label)} (branches: {', '.join(branches)})"
        for label, branches in sorted(wanted.items())
    ]
    lines += ["", "_Kept up to date automatically by the issue cache workflow in scylla-cluster-tests._"]
    return "\n".join(lines)


class Reconciler:
    """Mark each referenced item as in use: a label where allowed, otherwise one comment."""

    def __init__(self, gh: GitHub, mode: str, label_meta: dict[str, dict], state: dict):
        self.gh, self.mode, self.label_meta = gh, mode, label_meta
        self.state = state  # {ref: {"comment_id": int, "body": str}}, persisted between runs
        self.plan: list[str] = []
        self._can_label: dict[str, bool] = {}
        self._described: set[tuple[str, str]] = set()
        self._login: str | None = None

    @property
    def dry(self) -> bool:
        return self.mode != "on"

    def can_label(self, owner: str, repo: str) -> bool:
        key = f"{owner}/{repo}"
        if key not in self._can_label:
            status, data = self.gh.request("GET", f"/repos/{key}")
            perms = (data or {}).get("permissions", {}) if status == 200 else {}
            self._can_label[key] = any(perms.get(p) for p in ("admin", "maintain", "push", "triage"))
        return self._can_label[key]

    def login(self) -> str:
        if self._login is None:
            _, data = self.gh.request("GET", "/user")
            self._login = (data or {}).get("login", "")
        return self._login

    def reconcile(self, ref: str, wanted: dict[str, list[str]], current_labels: list[str]) -> None:
        owner, repo, number = split_ref(ref)
        if self.can_label(owner, repo):
            self._labels(ref, owner, repo, number, set(wanted), set(current_labels) & set(self.label_meta))
        else:
            self._comment(ref, owner, repo, number, wanted)

    def _labels(self, ref, owner, repo, number, wanted: set[str], current: set[str]) -> None:
        for label in sorted(wanted - current):
            self.plan.append(f"{ref}: add label {label}")
            if not self.dry:
                self.gh.request("POST", f"/repos/{owner}/{repo}/issues/{number}/labels", {"labels": [label]})
                self._describe(owner, repo, label)
        for label in sorted(current - wanted):
            self.plan.append(f"{ref}: remove label {label}")
            if not self.dry:
                self.gh.request("DELETE", f"/repos/{owner}/{repo}/issues/{number}/labels/{label}")

    def _describe(self, owner: str, repo: str, label: str) -> None:
        if (owner, repo, label) in self._described:
            return
        self._described.add((owner, repo, label))
        meta = self.label_meta[label]
        self.gh.request(
            "PATCH",
            f"/repos/{owner}/{repo}/labels/{label}",
            {"color": LABEL_COLOR, "description": meta["label_description"][:100]},
        )

    def _find_comment(self, owner: str, repo: str, number: int) -> int | None:
        status, comments = self.gh.request("GET", f"/repos/{owner}/{repo}/issues/{number}/comments?per_page=100")
        for c in comments if status == 200 and comments else []:
            if MARKER in (c.get("body") or "") and c["user"]["login"] == self.login():
                return c["id"]
        return None

    def _comment(self, ref, owner, repo, number, wanted: dict[str, list[str]]) -> None:
        body = comment_body(wanted, {label: m["repo"] for label, m in self.label_meta.items()})
        known = self.state.get(ref, {})
        if known.get("body") == body:
            return
        comment_id = known.get("comment_id") or self._find_comment(owner, repo, number)
        if comment_id:
            self.plan.append(f"{ref}: edit marker comment {comment_id}")
            if not self.dry:
                self.gh.request("PATCH", f"/repos/{owner}/{repo}/issues/comments/{comment_id}", {"body": body})
        elif wanted:
            self.plan.append(f"{ref}: add marker comment")
            if not self.dry:
                _, data = self.gh.request("POST", f"/repos/{owner}/{repo}/issues/{number}/comments", {"body": body})
                comment_id = (data or {}).get("id")
        else:
            return
        if not self.dry:
            self.state[ref] = {"comment_id": comment_id, "body": body}


def cmd_scan(args) -> None:
    repo_dir = Path(args.repo_dir)
    branches = select_branches(repo_dir, args.branch_regex, args.max_age_days, args.remote)
    if not branches:
        raise SystemExit("no branches selected: is this a full clone (fetch-depth: 0)?")
    per_branch = {b: scan_branch(repo_dir, f"refs/remotes/{args.remote}/{b}", SCT_CALLS) for b in branches}
    manifest = build_manifest(args.repo, args.label, args.label_description, per_branch)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(manifest, indent=1) + "\n")
    LOGGER.info("%d refs on %d branches -> %s", len(manifest["refs"]), len(branches), args.out)


def cmd_refresh_github(args) -> None:
    manifests = load_manifests(Path(args.refs_dir))
    union = github_union(manifests)
    state_file = Path(args.state) if args.state else None
    state = json.loads(state_file.read_text()) if state_file and state_file.exists() else {}
    previous = state.get("refs", {})

    states, failed = fetch_states(GitHub(os.environ["GH_TOKEN"]), sorted(union))
    check_guards(len(union), len(previous), len(failed))
    written = write_csvs(Path(args.out_dir), states)
    LOGGER.info("%d refs cached in %d files, %d failed", len(states), len(written) // 2, len(failed))

    plan = []
    if args.labels != "off":
        label_meta = {m["label"]: m for m in manifests}
        reconciler = Reconciler(GitHub(os.environ["GH_TOKEN"]), args.labels, label_meta, state.get("comments", {}))
        # refs that dropped out since last run still need their marker removed
        for ref in sorted(set(union) | set(previous)):
            if ref in states or ref in union:
                current = states.get(ref, {}).get("labels", [])
            else:
                fetched, _ = fetch_states(reconciler.gh, [ref])
                if ref not in fetched:
                    continue
                current = fetched[ref]["labels"]
            reconciler.reconcile(ref, union.get(ref, {}), current)
        plan = reconciler.plan
        no_perm = sorted(repo for repo, ok in reconciler._can_label.items() if not ok)
        plan.append(f"repos without label permission (marker comment instead): {', '.join(no_perm) or 'none'}")
        state["comments"] = reconciler.state

    for line in plan:
        LOGGER.info("plan: %s", line)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(f"### Issue cache ({args.labels} labels)\n\n- refs: {len(union)}, failed: {len(failed)}\n")
            fh.writelines(f"- {line}\n" for line in plan)
    if state_file:
        state["refs"] = {ref: sorted(labels) for ref, labels in union.items()}
        state_file.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n")


def read_cache_rows(cache_dir: Path, key: str) -> dict[str, tuple[str, set[str]]]:
    """Rows of one repo from a cache dir (new or old producer), as {number: (state, labels)}."""
    rows = {}
    for src in (cache_dir / f"{key}.csv", cache_dir / "pull-requests" / f"{key}.csv"):
        if src.exists():
            for row in csv.reader(src.read_text(encoding="utf-8").splitlines()):
                if len(row) >= 3:
                    rows[row[0]] = (row[1].lower(), set(filter(None, row[2].split("|"))))
    return rows


def cmd_compare(args) -> None:
    """Shadow check: every row of the new cache must match the old full cache."""
    new_dir, old_dir = Path(args.new_dir), Path(args.old_dir)
    matched, report = 0, []
    for f in sorted(new_dir.glob("*.csv")):
        old = read_cache_rows(old_dir, f.stem)
        for number, row in read_cache_rows(new_dir, f.stem).items():
            if number not in old:
                report.append(f"{f.stem}#{number}: not in the old cache (new coverage)")
            elif old[number] != row:
                report.append(f"{f.stem}#{number}: old {old[number]} != new {row}")
            else:
                matched += 1
    report.insert(0, f"{matched} rows match the old cache")
    for line in report:
        LOGGER.info("compare: %s", line)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("### Shadow compare\n\n" + "".join(f"- {line}\n" for line in report))


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan", help="scan this repo's branches and write the ref manifest")
    scan.add_argument("--repo-dir", default=".")
    scan.add_argument("--out", required=True)
    scan.add_argument("--repo", default="scylladb/scylla-cluster-tests")
    scan.add_argument("--label", default="used-by-sct")
    scan.add_argument("--label-description", default="Issue state controls SCT test skips (SkipPerIssues)")
    scan.add_argument("--branch-regex", default=SCT_BRANCHES)
    scan.add_argument("--max-age-days", type=int, default=365)
    scan.add_argument("--remote", default="origin", help="remote whose branches are scanned")
    scan.set_defaults(func=cmd_scan)

    refresh = sub.add_parser("refresh-github", help="refresh the GitHub CSVs and in-use markers from all manifests")
    refresh.add_argument("--refs-dir", required=True)
    refresh.add_argument("--out-dir", required=True)
    refresh.add_argument("--state", help="JSON state kept between runs (previous union, marker comments)")
    refresh.add_argument("--labels", choices=("off", "dry-run", "on"), default="dry-run")
    refresh.set_defaults(func=cmd_refresh_github)

    compare = sub.add_parser("compare", help="shadow check of a new cache dir against the old full cache")
    compare.add_argument("--new-dir", required=True)
    compare.add_argument("--old-dir", required=True)
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
