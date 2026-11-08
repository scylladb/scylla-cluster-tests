"""CLI of the referenced-only issue cache; see the package docstring."""

import argparse
import json
import logging
import os
from pathlib import Path

from utils.issues_cache.github import (
    Reconciler,
    check_guards,
    fetch_states,
    gh_api,
    read_cache_rows,
    write_csvs,
)
from utils.issues_cache.refs import (
    SCT_BRANCHES,
    SCT_CALLS,
    build_manifest,
    github_union,
    load_manifests,
    scan_branch,
    select_branches,
)

LOGGER = logging.getLogger("utils.issues_cache")


def _summary(text: str) -> None:
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(text)


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

    states, failed = fetch_states(gh_api, sorted(union))
    check_guards(len(union), len(previous), len(failed), args.accept_shrink)
    written = write_csvs(Path(args.out_dir), states)
    LOGGER.info("%d refs cached in %d files, %d failed", len(states), len(written) // 2, len(failed))

    plan, dropped_failed = [], []
    if args.labels != "off":
        reconciler = Reconciler(
            gh_api,
            args.labels,
            {m["label"]: m for m in manifests},
            state.get("comments", {}),
            managed={label for labels in previous.values() for label in labels},
        )
        # refs that dropped out since last run still need their marker removed
        dropped, dropped_failed = fetch_states(gh_api, sorted(set(previous) - set(union)))
        for ref in sorted(set(union) | set(dropped)):
            current = (states.get(ref) or dropped.get(ref) or {}).get("labels", [])
            reconciler.reconcile(ref, union.get(ref, {}), current)
        no_perm = sorted(repo for repo, ok in reconciler.can_label_repo.items() if not ok)
        plan = [
            *reconciler.plan,
            f"repos without label permission (marker comment instead): {', '.join(no_perm) or 'none'}",
        ]
        state["comments"] = reconciler.state

    for line in plan:
        LOGGER.info("plan: %s", line)
    _summary(
        f"### Issue cache ({args.labels} labels)\n\n- refs: {len(union)}, failed: {len(failed)}\n"
        + "".join(f"- {line}\n" for line in plan)
    )
    if state_file:
        state["refs"] = {ref: sorted(labels) for ref, labels in union.items()}
        # a dropped ref that failed to fetch still has its marker: keep it so the next run retries
        state["refs"] |= {ref: previous[ref] for ref in dropped_failed}
        state_file.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n")


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
    _summary("### Shadow compare\n\n" + "".join(f"- {line}\n" for line in report))


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="python -m utils.issues_cache", description="Referenced-only issue cache")
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
    refresh.add_argument(
        "--accept-shrink", action="store_true", help="skip the shrink guard once, for an expected drop"
    )
    refresh.set_defaults(func=cmd_refresh_github)

    compare = sub.add_parser("compare", help="shadow check of a new cache dir against the old full cache")
    compare.add_argument("--new-dir", required=True)
    compare.add_argument("--old-dir", required=True)
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
