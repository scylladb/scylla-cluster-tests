"""Per-test rows of the test activity report: run counts, triage, cost, duration and linked issues.

Every function here works on plain Argus JSON records, so the rules can be tested without Argus.
"""

import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from sdcm.sct_config.config import backend_to_cloud

EXCLUDED_RELEASES = frozenset({"scylla-staging", "sct-github-PRs-scan", "local-runs", "untracked"})
FAIL_STATUSES = frozenset({"failed", "test_error", "error"})
DONE_STATUSES = FAIL_STATUSES | {"passed", "aborted"}
MIN_RUNS = 3

PRODUCT_REPOS = frozenset(
    {
        "scylladb/scylladb",
        "scylladb/scylla",
        "scylladb/scylla-enterprise",
        "scylladb/scylla-machine-image",
        "scylladb/scylla-enterprise-machine-image",
        "scylladb/siren",
        "scylladb/siren-frontend",
        "scylladb/siren-devops",
    }
)
FRAMEWORK_REPOS = frozenset({"scylladb/scylla-cluster-tests", "scylladb/scylla-qa-internal", "scylladb/argus"})
PRODUCT_JIRA = frozenset({"SCYLLADB", "SMI", "CLOUD", "SIREN"})
FRAMEWORK_JIRA = frozenset({"SCT", "QATOOLS", "ARGUS"})
# Seastar "oversized allocation" warnings are product issues, counted in their own group.
OVERSIZED_RE = re.compile(r"oversi[sz]ed?[\s_-]*alloc", re.IGNORECASE)
AREAS = ("product", "oversized", "framework", "other")

NON_CLOUD_BACKENDS = frozenset({"docker", "baremetal"})
NODE_ROLES = ("db_node", "loader_node", "monitor_node")

RateLookup = Callable[[str, str, str], float | None]


@dataclass(frozen=True)
class Issue:
    key: str
    title: str
    url: str
    area: str
    source: str
    state: str


def classify_issue(raw: dict) -> Issue:
    """Return the issue with its area: product, oversized, framework or other.

    The area comes from the GitHub repository or the Jira project, not from the root cause.
    """
    if raw.get("subtype") == "jira":
        project = (raw.get("project") or raw["key"].split("-")[0]).upper()
        key, title, url, source = raw["key"], raw.get("summary") or "", raw.get("permalink") or "", f"Jira {project}"
        area = "product" if project in PRODUCT_JIRA else "framework" if project in FRAMEWORK_JIRA else "other"
    else:
        source = f"{(raw.get('owner') or '').lower()}/{(raw.get('repo') or '').lower()}"
        key = f"{raw.get('owner')}/{raw.get('repo')}#{raw.get('number')}"
        title, url = raw.get("title") or "", raw.get("url") or ""
        area = "product" if source in PRODUCT_REPOS else "framework" if source in FRAMEWORK_REPOS else "other"
    if area == "product" and OVERSIZED_RE.search(title):
        area = "oversized"
    return Issue(key=key, title=title[:160], url=url, area=area, source=source, state=raw.get("state") or "")


def test_key(build_system_id: str, release_name: str) -> str:
    """Return the Jenkins folder and job name of an Argus test, without the release and the -test suffix.

    Some job trees add "-test" to the job name and some do not, so the key leaves the suffix out.
    """
    parts = build_system_id.removeprefix(release_name + "/").split("/")
    return "/".join(parts[-2:]).removesuffix("-test")


def _timestamp(value: str) -> float:
    """Argus times are UTC, with or without an offset."""
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (moment if moment.tzinfo else moment.replace(tzinfo=UTC)).timestamp()


def run_hours(run: dict) -> float | None:
    """Return the duration of a run in hours.

    A missing end time falls back to the last heartbeat. A run that stopped in setup has no end time
    and a heartbeat equal to its start, so it counts as 0 hours.
    """
    start = _timestamp(run["start_time"])
    end = _timestamp(run["end_time"]) if run.get("end_time") else 0.0
    if end <= start:
        end = float(run.get("heartbeat") or 0)
    if end <= start:
        return 0.0 if run["status"] in DONE_STATUSES else None
    return (end - start) / 3600


def estimate_run_cost(run: dict, hours: float | None, rate: RateLookup) -> tuple[float | None, str]:
    """Return the on-demand cost of the db, loader and monitor nodes of a run, and the reason when unknown."""
    setup = run.get("cloud_setup") or {}
    backend = setup.get("backend")
    if backend in NON_CLOUD_BACKENDS:
        return 0.0, "non-cloud"
    if not backend:
        return None, "no-setup"
    if hours is None:
        return None, "no-duration"
    region = (run.get("region_name") or [""])[0]
    total = 0.0
    for role in NODE_ROLES:
        node = setup.get(role) or {}
        count, instance_type = node.get("node_amount") or 0, node.get("instance_type")
        if count == 0:
            continue
        if count < 0 or not instance_type or instance_type == "UNKNOWN":
            return None, "no-instance-type"
        # Scylla Cloud runs record the AWS or GCE instance type. AWS types contain a dot.
        cloud = backend_to_cloud(backend, xcloud_provider="aws" if "." in instance_type else "gce")
        price = rate(cloud, region, instance_type) if cloud else None
        if price is None:
            return None, "no-price"
        total += price * count * hours
    return total, "estimate"


def price_runs(runs: Iterable[dict], actual_costs: dict[str, float], rate: RateLookup) -> Counter:
    """Set _hours, _cost and _kind on each run. Return the count of runs with no cost data, per reason.

    _kind is "actual" (the cost that SCT sent to Argus), "estimated", "non-cloud" ($0) or "no-data".
    """
    no_data = Counter()
    for run in runs:
        run["_hours"] = run_hours(run)
        actual = actual_costs.get(run["id"])
        if actual is not None:
            run["_cost"], run["_kind"] = actual, "actual"
            continue
        cost, reason = estimate_run_cost(run, run["_hours"], rate)
        if cost is None:
            run["_cost"], run["_kind"] = None, "no-data"
            no_data[reason] += 1
        else:
            run["_cost"], run["_kind"] = cost, "non-cloud" if reason == "non-cloud" else "estimated"
    return no_data


def build_row(key: str, runs: list[dict], run_issues: dict[str, set[str]], issues: dict[str, Issue]) -> dict:
    """Return the counts, issues, cost and duration of one test over the given runs.

    A cheap or short run often did not finish. So the most expensive run sets the cost per run, and the
    longest run sets the duration per run. The estimated totals multiply them by the number of runs.
    Each run that links to an issue counts as one issue found.
    """
    statuses = Counter(run["status"] for run in runs)
    failed = [run for run in runs if run["status"] in FAIL_STATUSES]
    looked = [r for r in failed if r.get("investigation_status") != "not_investigated" or run_issues.get(r["id"])]
    links = Counter()
    issue_runs = defaultdict(list)
    for run in runs:
        for issue_id in run_issues.get(run["id"], ()):
            links[issues[issue_id].area] += 1
            issue_runs[issue_id].append(run["id"])
    issue_ids = defaultdict(list)
    for issue_id in sorted(issue_runs):
        issue_ids[issues[issue_id].area].append(issue_id)
    hours = [run["_hours"] for run in runs if run["_hours"] is not None]
    costs = [run["_cost"] for run in runs if run["_cost"] is not None]
    kinds = {run["_kind"] for run in runs if run["_kind"] in ("actual", "estimated")}
    if len(kinds) == 2:
        kind = "mixed"
    elif kinds:
        kind = kinds.pop()
    else:
        kind = "non-cloud" if costs else "no-data"
    backends = Counter((run.get("cloud_setup") or {}).get("backend") or "?" for run in runs)
    return {
        "test": key,
        "backend": "/".join(name for name, _ in backends.most_common(2)),
        "runs": len(runs),
        "passed": statuses["passed"],
        "failed": len(failed),
        "aborted": statuses["aborted"],
        "running": statuses["running"] + statuses["created"],
        "timer_runs": sum(1 for run in runs if run.get("started_by") == "timer"),
        **{area: links[area] for area in AREAS},
        "issue_runs": dict(issue_runs),
        "issue_ids": dict(issue_ids),
        "failed_looked": len(looked),
        "looked_share": len(looked) / len(failed) if failed else None,
        "inv": dict(Counter(run.get("investigation_status") for run in failed)),
        "cost_max_run": round(max(costs), 2) if costs else None,
        "cost_est_total": round(max(costs) * len(runs), 2) if costs else None,
        "cost_runs": len(costs),
        "cost_actual_runs": sum(1 for run in runs if run["_kind"] == "actual"),
        "cost_kind": kind,
        "hours_measured": round(sum(hours), 1),
        "hours_est_total": round(max(hours) * len(runs), 1) if hours else None,
        "last_run": max(run["start_time"] for run in runs)[:10],
    }


def set_flags(row: dict) -> None:
    flags = []
    if row["runs"] - row["running"] < MIN_RUNS:
        flags.append("few-runs")
    if row["product"] == 0 and row["framework"] > 0:
        flags.append("framework-only")
    if row["failed"] >= 2 and not row["looked_share"]:
        flags.append("not-looked-at")
    if not any(row[area] for area in AREAS):
        flags.append("no-issues")
    row["flags"] = flags


def set_unique(rows: list[dict]) -> None:
    """Mark the product issues that are linked to runs of one test only, among the given rows."""
    tests_of_issue = defaultdict(set)
    for row in rows:
        for issue_id in row["issue_ids"].get("product", []):
            tests_of_issue[issue_id].add(row["test"])
    for row in rows:
        row["product_unique_ids"] = [i for i in row["issue_ids"].get("product", []) if len(tests_of_issue[i]) == 1]
        row["product_unique"] = len(row["product_unique_ids"])


def build_rows(
    runs: list[dict],
    tests: dict[str, dict],
    release_names: dict[str, str],
    run_issues: dict[str, set[str]],
    issues: dict[str, Issue],
) -> list[dict]:
    """Return one row per test key, summed over its releases, with a sub-row per release in by_release.

    A sub-row uses only the runs of its release, so a release filter changes every count, the cost,
    the hours and the issues. Unique product issues of a sub-row are unique among the tests of that release.
    """
    by_key = defaultdict(list)
    for run in runs:
        test = tests[run["test_id"]]
        by_key[test_key(test["build_system_id"], release_names[test["release_id"]])].append(run)
    rows = []
    for key, key_runs in by_key.items():
        row = build_row(key, key_runs, run_issues, issues)
        releases = Counter(release_names[run["release_id"]] for run in key_runs)
        row["releases"] = dict(releases.most_common())
        row["argus_tests"] = sorted({(release_names[run["release_id"]], run["test_id"]) for run in key_runs})
        row["jobs"] = len(row["argus_tests"])
        row["run_list"] = [
            [
                run["id"],
                release_names[run["release_id"]],
                run.get("build_number"),
                run["start_time"][:16].replace("T", " "),
                run["status"],
                run.get("started_by") or "",
            ]
            for run in sorted(key_runs, key=lambda run: run["start_time"], reverse=True)
        ]
        row["by_release"] = {
            release: build_row(
                key, [run for run in key_runs if release_names[run["release_id"]] == release], run_issues, issues
            )
            for release in releases
        }
        rows.append(row)
    for row in rows:
        set_flags(row)
        for sub_row in row["by_release"].values():
            set_flags(sub_row)
    set_unique(rows)
    for release in {release for row in rows for release in row["by_release"]}:
        set_unique([row["by_release"][release] for row in rows if release in row["by_release"]])
    return rows


def link_issues(raw_issues: list[dict], run_ids: set[str]) -> tuple[dict[str, set[str]], dict[str, Issue]]:
    """Return the issue ids of each run, and the issues linked to at least one of the given runs.

    Argus can hold two records of one issue with a different owner casing, so the id is the lowercase key.
    """
    run_issues = defaultdict(set)
    issues = {}
    for raw in raw_issues:
        issue = classify_issue(raw)
        issue_id = issue.key.lower()
        for link in raw.get("links") or []:
            if link["run_id"] in run_ids:
                run_issues[link["run_id"]].add(issue_id)
                issues.setdefault(issue_id, issue)
    return dict(run_issues), issues
