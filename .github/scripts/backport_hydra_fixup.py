#!/usr/bin/env python3
"""Fix the hydra image commit and uv.lock on backport PRs opened by the backport bot.

A master PR that changes dependencies ends with ``chore(hydra): create image <tag>``,
written by build-docker-image.yaml. The backport bot cherry-picks it to release
branches together with the real change. That points the release branch at master's
image, usually conflicts on docker/env/version, and makes the build workflow skip.
The dependency commit itself carries master's uv.lock hunks, which conflict too.

For every open bot backport that touches the hydra image inputs, this script:

1. rebases the PR onto its merge-base with the release branch, dropping every
   ``chore(hydra): create image`` commit and re-locking uv.lock inside each commit
   that changes it, starting from the release branch's lock and upgrading only the
   packages the original master commit bumped;
2. force-pushes, with a lease on the head SHA it read, so a concurrent push wins;
3. when no conflict markers are left and every re-lock worked, removes ``conflicts``,
   marks the PR ready and adds ``New Hydra Version``, which makes
   build-docker-image.yaml build an image for the release branch;
4. comments with the outcome. That comment marks the PR as handled.

uv runs as ``uvx uv@<constraints.uv from renovate.json>``, the version Renovate uses,
so the re-locked file has no noise from a different uv.

Local testing (needs `gh auth login` and uv):

  # what would be done to every open bot backport; pushes and edits nothing
  python .github/scripts/backport_hydra_fixup.py --dry-run

  # one PR, for real, pushing over SSH with your own key
  python .github/scripts/backport_hydra_fixup.py --pr 16400 \\
      --push-url git@github.com:scylladbbot/scylla-cluster-tests.git

  # rewrite the current local branch in place, no GitHub at all
  python .github/scripts/backport_hydra_fixup.py local --base upstream/branch-2026.1
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

REPO = "scylladb/scylla-cluster-tests"
BOT = "scylladbbot"
HYDRA_SUBJECT = "chore(hydra): create image"
HYDRA_LABEL = "New Hydra Version"
CONFLICTS_LABEL = "conflicts"
IMAGE_INPUTS = {"Dockerfile", "docker/env/build_n_push.sh", "uv.lock", "pyproject.toml"}
MARKER = "<!-- backport-hydra-fixup -->"
CHERRY_PICK_RE = re.compile(r"cherry picked from commit ([0-9a-f]{7,40})")
CONFLICT_RE = re.compile(r"^(<{7}|>{7})( |$)", re.MULTILINE)
RENOVATE_JSON = Path(__file__).resolve().parents[2] / "renovate.json"
FAILED_FILE_ENV = "BACKPORT_HYDRA_FIXUP_FAILED"


def run(*cmd, cwd=None, check=True, env=None) -> str:
    result = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True, check=False)
    if check and result.returncode:
        raise RuntimeError(f"{' '.join(cmd)} failed ({result.returncode}):\n{result.stderr or result.stdout}")
    return result.stdout


def uv_version() -> str:
    version = json.loads(RENOVATE_JSON.read_text()).get("constraints", {}).get("uv")
    if not version:
        raise SystemExit(f"no constraints.uv in {RENOVATE_JSON}")
    return version


def lock_bumps(old_lock: str, new_lock: str) -> list[str]:
    """``name==version`` for every locked package whose version changed or was added."""

    def pins(text):
        return {p["name"]: p.get("version") for p in tomllib.loads(text).get("package", [])}

    old, new = pins(old_lock), pins(new_lock)
    return sorted(f"{name}=={version}" for name, version in new.items() if version and old.get(name) != version)


def candidates(prs: list[dict], only_pr: int | None = None) -> list[dict]:
    """Open bot backports touching the image inputs that haven't been handled yet."""
    picked = []
    for pr in prs:
        if only_pr is not None and pr["number"] != only_pr:
            continue
        if not pr["headRefName"].startswith("backport/"):
            continue
        if not IMAGE_INPUTS & {f["path"] for f in pr["files"]}:
            continue
        if only_pr is None and (
            HYDRA_LABEL in {label["name"] for label in pr["labels"]}
            or any(c["body"].startswith(MARKER) for c in pr["comments"])
        ):
            continue
        picked.append(pr)
    return picked


def edit_todo(todo_path: str, script: str) -> None:
    """Rebase sequence editor: drop hydra commits, re-lock after every other commit."""
    lines = []
    for line in Path(todo_path).read_text().splitlines():
        if line.startswith("pick "):
            if HYDRA_SUBJECT in line:
                continue
            lines += [line, f"exec {sys.executable} {script} relock"]
        elif line and not line.startswith("#"):
            lines.append(line)
    Path(todo_path).write_text("\n".join(lines) + "\n")


def relock() -> None:
    """Run by ``git rebase --exec`` after each commit. Never fails the rebase: problems are recorded."""
    if not run("git", "diff", "--name-only", "HEAD~1", "HEAD", "--", "uv.lock").strip():
        return
    subject = run("git", "log", "-1", "--format=%s").strip()
    try:
        problem = _relock()
    except Exception as exc:  # noqa: BLE001 - a crash here would stop the rebase half-way
        problem = f"re-lock crashed: {exc}"
        # a half-done re-lock leaves uv.lock modified, and git stops the rebase on a dirty tree
        subprocess.run(["git", "checkout", "HEAD", "--", "uv.lock"], capture_output=True, check=False)
    if problem:
        with Path(os.environ[FAILED_FILE_ENV]).open("a") as f:
            f.write(f"{subject}: {problem}\n")


def _relock() -> str | None:
    """Re-lock HEAD's uv.lock from HEAD~1's. Returns what went wrong, or None."""
    if subprocess.run(["git", "cat-file", "-e", "HEAD~1:uv.lock"], capture_output=True, check=False).returncode:
        return "the release branch has no uv.lock, but this commit adds one; drop it from the commit by hand"
    found = CHERRY_PICK_RE.findall(run("git", "log", "-1", "--format=%B"))
    if not found:
        return "no `cherry picked from` trailer to find the original commit"
    orig = found[-1]
    bumps = lock_bumps(run("git", "show", f"{orig}^:uv.lock"), run("git", "show", f"{orig}:uv.lock"))
    release_lock = run("git", "show", "HEAD~1:uv.lock")
    if not bumps:
        # uv leaves an unchanged lock in whatever format wrote it; a no-op upgrade makes it
        # rewrite the lock in its own format, which is the point of a format-only commit
        first = tomllib.loads(release_lock)["package"][0]
        bumps = [f"{first['name']}=={first['version']}"]
    Path("uv.lock").write_text(release_lock)
    upgrade = [arg for bump in bumps for arg in ("--upgrade-package", bump)]
    result = subprocess.run(
        ["uvx", f"uv@{uv_version()}", "lock", *upgrade], text=True, capture_output=True, check=False
    )
    if result.returncode:
        run("git", "checkout", "HEAD", "--", "uv.lock")
        return f"`uv lock {' '.join(upgrade)}` failed: {result.stderr.strip().splitlines()[-1:]}"
    run("git", "commit", "--amend", "--no-edit", "--no-verify", "--allow-empty", "--", "uv.lock")
    return None


def fix_branch(workdir: str, base_ref: str) -> tuple[str, list[str], list[str], list[str]]:
    """Rewrite HEAD in place. Returns (merge-base, dropped commits, relock failures, files with markers)."""
    merge_base = run("git", "merge-base", "HEAD", base_ref, cwd=workdir).strip()
    dropped = [
        line
        for line in run("git", "log", "--format=%h %s", f"{merge_base}..HEAD", cwd=workdir).splitlines()
        if HYDRA_SUBJECT in line
    ]
    with tempfile.NamedTemporaryFile("w", suffix=".failed", delete=False) as failed:
        pass
    script = str(Path(__file__).resolve())
    env = os.environ | {
        "GIT_SEQUENCE_EDITOR": f"{sys.executable} {script} edit-todo",
        FAILED_FILE_ENV: failed.name,
    }
    rebase = subprocess.run(
        ["git", "rebase", "-i", "--keep-empty", merge_base],
        cwd=workdir,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if rebase.returncode:
        run("git", "rebase", "--abort", cwd=workdir, check=False)
        raise RuntimeError(f"rebase failed:\n{rebase.stderr or rebase.stdout}")
    failures = Path(failed.name).read_text().splitlines()
    os.unlink(failed.name)
    changed = run("git", "diff", "--name-only", merge_base, "HEAD", cwd=workdir).split()
    markers = [
        path
        for path in changed
        if (Path(workdir) / path).is_file() and CONFLICT_RE.search((Path(workdir) / path).read_text(errors="ignore"))
    ]
    return merge_base, dropped, failures, markers


def gh(*args, repo=REPO) -> str:
    return run("gh", *args, "-R", repo)


def process(pr: dict, workdir: str, repo: str, dry_run: bool, push_url: str | None) -> None:
    number, head, base = pr["number"], pr["headRefName"], pr["baseRefName"]
    print(f"::group::#{number} {head} -> {base}")
    run("git", "fetch", "-q", "origin", f"pull/{number}/head", base, cwd=workdir)
    run("git", "checkout", "-q", "-B", "fixup", "FETCH_HEAD", cwd=workdir)
    run("git", "reset", "-q", "--hard", pr["headRefOid"], cwd=workdir)
    old_head = pr["headRefOid"]
    try:
        merge_base, dropped, failures, markers = fix_branch(workdir, f"origin/{base}")
    except RuntimeError as exc:
        print(exc)
        if not dry_run:
            gh(
                "pr",
                "comment",
                str(number),
                "--body",
                f"{MARKER}\nCouldn't rewrite this backport automatically:\n```\n{exc}\n```",
                repo=repo,
            )
        print("::endgroup::")
        return
    new_head = run("git", "rev-parse", "HEAD", cwd=workdir).strip()
    print(run("git", "log", "--format=%h %s", f"{merge_base}..HEAD", cwd=workdir))
    print(f"dropped: {dropped or 'none'}\nrelock failures: {failures or 'none'}\nconflict markers: {markers or 'none'}")
    clean = not failures and not markers
    if dry_run:
        print(f"dry run: would {'push and ' if new_head != old_head else ''}{'label' if clean else 'comment only'}")
        print("::endgroup::")
        return
    if new_head != old_head:
        url = push_url or (
            f"https://x-access-token:{os.environ['GH_TOKEN']}@github.com/"
            f"{pr['headRepositoryOwner']['login']}/{pr['headRepository']['name']}.git"
        )
        run(
            "git",
            "push",
            "-q",
            f"--force-with-lease=refs/heads/{head}:{old_head}",
            url,
            f"HEAD:refs/heads/{head}",
            cwd=workdir,
        )
    lines = [MARKER]
    if dropped:
        lines.append("Dropped master's image commit: " + ", ".join(f"`{d}`" for d in dropped))
    if new_head != old_head:
        lines.append(f"Re-locked `uv.lock` from `{base}` with uv {uv_version()} inside each dependency commit.")
    if clean:
        if CONFLICTS_LABEL in {label["name"] for label in pr["labels"]}:
            gh("pr", "edit", str(number), "--remove-label", CONFLICTS_LABEL, repo=repo)
        if pr["isDraft"]:
            gh("pr", "ready", str(number), repo=repo)
        gh("pr", "edit", str(number), "--add-label", HYDRA_LABEL, repo=repo)
        lines.append(f"Added `{HYDRA_LABEL}` to build an image for `{base}`.")
    else:
        lines.append("Still needs a person:")
        lines += [f"- relock: {f}" for f in failures] + [f"- conflict markers in `{m}`" for m in markers]
        lines.append(f"After fixing, add `{HYDRA_LABEL}` to build the image.")
    gh("pr", "comment", str(number), "--body", "\n".join(lines), repo=repo)
    print("::endgroup::")


def scan(args) -> None:
    prs = json.loads(
        gh(
            "pr",
            "list",
            "--author",
            args.bot,
            "--state",
            "open",
            "--limit",
            "200",
            "--json",
            "number,headRefName,headRefOid,baseRefName,headRepositoryOwner,headRepository,isDraft,labels,files,comments",
            repo=args.repo,
        )
    )
    todo = candidates(prs, args.pr)
    print(f"{len(todo)} backport(s) to fix: {[pr['number'] for pr in todo]}")
    if not todo:
        return
    uv_version()  # fail before cloning when the pin is missing
    with tempfile.TemporaryDirectory() as workdir:
        run("git", "clone", "-q", "--filter=blob:none", "--no-checkout", f"https://github.com/{args.repo}.git", workdir)
        failed = []
        for pr in todo:
            try:
                process(pr, workdir, args.repo, args.dry_run, args.push_url)
            except RuntimeError as exc:
                # e.g. the lease rejected a push because someone pushed meanwhile; the next run retries
                print(f"::endgroup::\n::error::#{pr['number']}: {exc}")
                failed.append(pr["number"])
    if failed:
        raise SystemExit(f"failed on {failed}")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "relock":
        return relock()
    if len(sys.argv) > 2 and sys.argv[1] == "edit-todo":
        return edit_todo(sys.argv[2], str(Path(__file__).resolve()))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command")
    local = sub.add_parser("local", help="rewrite the current branch in place")
    local.add_argument("--base", required=True, help="release branch ref, e.g. upstream/branch-2026.1")
    parser.add_argument("--pr", type=int, help="only this PR (also re-runs one already handled)")
    parser.add_argument("--dry-run", action="store_true", help="rewrite in a temp clone, push and edit nothing")
    parser.add_argument("--repo", default=REPO)
    parser.add_argument("--bot", default=BOT, help="author of the backport PRs")
    parser.add_argument("--push-url", help="push here instead of the PR head repo over HTTPS with GH_TOKEN")
    args = parser.parse_args()
    if args.command == "local":
        _, dropped, failures, markers = fix_branch(os.getcwd(), args.base)
        print(
            f"dropped: {dropped or 'none'}\nrelock failures: {failures or 'none'}\nconflict markers: {markers or 'none'}"
        )
        return
    scan(args)


if __name__ == "__main__":
    main()
