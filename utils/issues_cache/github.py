"""Refresh the GitHub issue cache and mark each referenced item as in use, through `gh api`."""

import csv
import json
import logging
import re
import subprocess
from collections.abc import Callable
from pathlib import Path

LOGGER = logging.getLogger(__name__)

MARKER = "<!-- sct-issue-cache -->"
LABEL_COLOR = "c5def5"
SHRINK_LIMIT = 0.5  # abort when the ref union shrinks by more than this
FAILURE_LIMIT = 0.2  # abort when more than this share of fetches fail

# (method, path, body=None, paginate=False) -> (HTTP status, decoded JSON); tests pass a fake
GhApi = Callable[..., tuple[int, object]]


def gh_api(method: str, path: str, body: dict | None = None, paginate: bool = False) -> tuple[int, object]:
    """Call the GitHub REST API with the `gh` CLI, which reads the token from GH_TOKEN."""
    cmd = ["gh", "api", "--method", method, path]
    if paginate:
        cmd += ["--paginate", "--slurp"]
    if body is not None:
        cmd += ["--input", "-"]
    proc = subprocess.run(
        cmd, input=json.dumps(body) if body is not None else None, capture_output=True, text=True, check=False
    )
    if proc.returncode:
        # gh reports HTTP failures on stderr as "gh: Not Found (HTTP 404)"
        status = re.search(r"HTTP (\d{3})", proc.stderr)
        return int(status[1]) if status else 0, None
    data = json.loads(proc.stdout) if proc.stdout.strip() else None
    if paginate:
        data = [item for page in data or [] for item in page]
    return 200, data


def split_ref(ref: str) -> tuple[str, str, int]:
    owner_repo, number = ref.split("#")
    owner, repo = owner_repo.split("/")
    return owner, repo, int(number)


def fetch_states(gh: GhApi, refs: list[str]) -> tuple[dict[str, dict], list[str]]:
    states, failed = {}, []
    for ref in refs:
        owner, repo, number = split_ref(ref)
        status, data = gh("GET", f"/repos/{owner}/{repo}/issues/{number}")
        if status != 200:
            LOGGER.warning("can't fetch %s: HTTP %s", ref, status)
            failed.append(ref)
            continue
        merged = (data.get("pull_request") or {}).get("merged_at")
        states[ref] = {
            "state": "merged" if merged else data["state"],
            "labels": [label["name"] for label in data["labels"]],
            "title": data["title"],
        }
    return states, failed


def write_csvs(out_dir: Path, states: dict[str, dict]) -> list[Path]:
    """Write the CSV contract described in the package docstring; return the written files."""
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


def read_cache_rows(cache_dir: Path, key: str) -> dict[str, tuple[str, set[str]]]:
    """Rows of one repo from a cache dir (new or old producer), as {number: (state, labels)}."""
    rows = {}
    for src in (cache_dir / f"{key}.csv", cache_dir / "pull-requests" / f"{key}.csv"):
        if src.exists():
            for row in csv.reader(src.read_text(encoding="utf-8").splitlines()):
                if len(row) >= 3:
                    rows[row[0]] = (row[1].lower(), set(filter(None, row[2].split("|"))))
    return rows


def check_guards(union_size: int, previous_size: int, failed: int, accept_shrink: bool = False) -> None:
    if not accept_shrink and previous_size and union_size < previous_size * (1 - SHRINK_LIMIT):
        raise SystemExit(
            f"ref union shrank from {previous_size} to {union_size}: refusing to update "
            "(rerun with --accept-shrink if the shrink is expected)"
        )
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

    def __init__(self, gh: GhApi, mode: str, label_meta: dict[str, dict], state: dict, managed: set[str] = frozenset()):
        self.gh, self.mode, self.label_meta = gh, mode, label_meta
        # labels this cache owns: today's manifests, plus labels of manifests that disappeared
        self.managed = set(label_meta) | set(managed)
        self.state = state  # {ref: {"comment_id": int, "body": str}}, persisted between runs
        self.plan: list[str] = []
        self.can_label_repo: dict[str, bool] = {}
        self._described: set[tuple[str, str, str]] = set()
        self._login: str | None = None

    @property
    def dry(self) -> bool:
        return self.mode != "on"

    def can_label(self, owner: str, repo: str) -> bool:
        key = f"{owner}/{repo}"
        if key not in self.can_label_repo:
            status, data = self.gh("GET", f"/repos/{key}")
            perms = (data or {}).get("permissions", {}) if status == 200 else {}
            self.can_label_repo[key] = any(perms.get(p) for p in ("admin", "maintain", "push", "triage"))
        return self.can_label_repo[key]

    def login(self) -> str:
        if self._login is None:
            _, data = self.gh("GET", "/user")
            self._login = (data or {}).get("login", "")
        return self._login

    def reconcile(self, ref: str, wanted: dict[str, list[str]], current_labels: list[str]) -> None:
        owner, repo, number = split_ref(ref)
        if self.can_label(owner, repo):
            self._labels(ref, owner, repo, number, set(wanted), set(current_labels) & self.managed)
        else:
            self._comment(ref, owner, repo, number, wanted)

    def _labels(self, ref, owner, repo, number, wanted: set[str], current: set[str]) -> None:
        for label in sorted(wanted - current):
            self.plan.append(f"{ref}: add label {label}")
            if not self.dry:
                if self._write("POST", f"/repos/{owner}/{repo}/issues/{number}/labels", {"labels": [label]}):
                    self._describe(owner, repo, label)
        for label in sorted(current - wanted):
            self.plan.append(f"{ref}: remove label {label}")
            if not self.dry:
                self._write("DELETE", f"/repos/{owner}/{repo}/issues/{number}/labels/{label}")

    def _write(self, method: str, path: str, body: dict | None = None) -> dict | bool:
        """Send a write; on failure log it and return False, so the next run retries."""
        status, data = self.gh(method, path, body)
        if not 200 <= status < 300:
            LOGGER.warning("%s %s failed: HTTP %s", method, path, status)
            return False
        return data or True

    def _describe(self, owner: str, repo: str, label: str) -> None:
        if (owner, repo, label) in self._described:
            return
        self._described.add((owner, repo, label))
        meta = self.label_meta[label]
        self._write(
            "PATCH",
            f"/repos/{owner}/{repo}/labels/{label}",
            {"color": LABEL_COLOR, "description": meta["label_description"][:100]},
        )

    def _find_comment(self, owner: str, repo: str, number: int) -> int | None:
        status, comments = self.gh("GET", f"/repos/{owner}/{repo}/issues/{number}/comments", paginate=True)
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
            if self.dry:
                return
            if not self._write("PATCH", f"/repos/{owner}/{repo}/issues/comments/{comment_id}", {"body": body}):
                self.state.pop(ref, None)  # e.g. the comment was deleted: look it up again next run
                return
        elif wanted:
            self.plan.append(f"{ref}: add marker comment")
            if self.dry or not (
                data := self._write("POST", f"/repos/{owner}/{repo}/issues/{number}/comments", {"body": body})
            ):
                return
            comment_id = data["id"]
        else:
            return
        # recorded only after a successful write, so a failed one is retried next run
        self.state[ref] = {"comment_id": comment_id, "body": body}
