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
    split_ref,
    with_last_good,
    write_csvs,
)
from utils.issues_cache.refs import (
    SCT_BRANCHES,
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
    per_branch = {b: scan_branch(repo_dir, f"refs/remotes/{args.remote}/{b}") for b in branches}
    manifest = build_manifest(args.repo, args.label, args.label_description, per_branch)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(manifest, indent=1) + "\n")
    LOGGER.info("%d refs on %d branches -> %s", len(manifest["refs"]), len(branches), args.out)
    if manifest["unresolved"]:
        _summary(
            "### Issue refs built at runtime (not cached, `compare` fails until they are literals)\n\n"
            + "".join(f"- {site}\n" for site in manifest["unresolved"])
        )


def _reconcile_issues(reconciler: Reconciler, union: dict, fetched: dict[str, dict]) -> None:
    """Reconcile each fetched issue once, under its canonical repo.

    Aliases of a renamed repo (`scylladb/scylla#N`, `scylladb/scylladb#N`) are one issue:
    reconciled one by one, each would remove the label the other wants. A ref that failed
    to fetch is skipped and retried next run.
    """
    issues: dict[str, list[str]] = {}
    for ref, st in sorted(fetched.items()):
        issues.setdefault(f"{st['repo']}#{split_ref(ref)[2]}", []).append(ref)
    for issue, aliases in issues.items():
        wanted: dict[str, set[str]] = {}
        for ref in aliases:
            for label, branches in union.get(ref, {}).items():
                wanted.setdefault(label, set()).update(branches)
        reconciler.reconcile(issue, {lb: sorted(b) for lb, b in wanted.items()}, fetched[aliases[0]]["labels"])
        if issue in reconciler.failed:
            reconciler.failed.update(aliases)


def cmd_refresh_github(args) -> None:
    manifests = load_manifests(Path(args.refs_dir))
    union = github_union(manifests)
    state_file = Path(args.state) if args.state else None
    state = json.loads(state_file.read_text()) if state_file and state_file.exists() else {}
    previous = state.get("refs", {})  # last run's union, for the shrink guard
    # refs that left the union and may still carry a marker: {ref: labels}
    pending = {ref: labels for ref, labels in (state.get("retry", {}) | previous).items() if ref not in union}

    states, failed = fetch_states(gh_api, sorted(union))
    check_guards(len(union), len(previous), len(failed), args.accept_shrink)
    state["rows"], state["carried"] = with_last_good(states, failed, state.get("rows", {}), state.get("carried", {}))
    written = write_csvs(Path(args.out_dir), state["rows"])
    LOGGER.info("%d refs cached in %d files, %d failed", len(state["rows"]), len(written) // 2, len(failed))

    plan = [f"{ref}: fetch failed, last good row kept ({n} runs)" for ref, n in sorted(state["carried"].items())]
    cleaned = set()
    if args.labels != "off":
        reconciler = Reconciler(
            gh_api,
            args.labels,
            {m["label"]: m for m in manifests},
            state.get("comments", {}),
            managed={label for labels in [*previous.values(), *pending.values()] for label in labels},
        )
        dropped, dropped_failed = fetch_states(gh_api, sorted(pending))
        _reconcile_issues(reconciler, union, states | dropped)
        # deleted (410) or no longer visible (404): no marker left that we could remove
        cleaned = {ref for ref, status in dropped_failed.items() if status in (404, 410)}
        if args.labels == "on":
            cleaned |= set(dropped) - reconciler.failed
        plan += [
            *reconciler.plan,
            "repos without label permission (marker comment instead): "
            + (", ".join(sorted(repo for repo, ok in reconciler.can_label_repo.items() if ok is False)) or "none"),
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
        # a dropped ref leaves only once its marker is really gone (fetched, written in `on` mode)
        state["retry"] = {ref: labels for ref, labels in pending.items() if ref not in cleaned}
        state_file.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n")


def cmd_compare(args) -> None:
    """Shadow check, the gate of the cutover; exits non-zero on any of:

    - a row of the new cache that differs from the old full cache;
    - a ref of the manifests with no row in the new cache;
    - a ref the scans could not resolve (built at runtime, see `refs.extract_python`):
      the only way test code can use a ref that the manifests miss.
    """
    new_dir, old_dir = Path(args.new_dir), Path(args.old_dir)
    manifests = load_manifests(Path(args.refs_dir))
    matched, problems, report = 0, [], []
    for f in sorted(new_dir.glob("*.csv")):
        old = read_cache_rows(old_dir, f.stem)
        for number, row in read_cache_rows(new_dir, f.stem).items():
            if number not in old:
                report.append(f"{f.stem}#{number}: not in the old cache (new coverage)")
            elif old[number] != row:
                problems.append(f"{f.stem}#{number}: old {old[number]} != new {row}")
            else:
                matched += 1
    for ref in sorted(github_union(manifests)):
        owner, repo, number = split_ref(ref)
        if str(number) not in read_cache_rows(new_dir, f"{owner}_{repo}"):
            problems.append(f"{ref}: referenced, but not in the new cache")
    problems += [
        f"{m['repo']}: issue ref built at runtime: {site}" for m in manifests for site in m.get("unresolved", [])
    ]
    report = [f"{matched} rows match the old cache, {len(problems)} problems", *problems, *report]
    for line in report:
        LOGGER.info("compare: %s", line)
    _summary("### Shadow compare\n\n" + "".join(f"- {line}\n" for line in report))
    if problems:
        raise SystemExit(1)


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
    compare.add_argument("--refs-dir", required=True, help="the manifests the new cache was built from")
    compare.set_defaults(func=cmd_compare)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
