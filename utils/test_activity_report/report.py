"""Build the test activity report: read Argus and the SCT tree, compute the rows, write one HTML page."""

import json
import logging
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

from sdcm.utils.cloud_catalog.cost import get_hourly_rate
from utils.test_activity_report import analysis, argus_source, pipelines

LOGGER = logging.getLogger(__name__)
TEMPLATE = Path(__file__).parent / "template.html"


@cache
def on_demand_rate(cloud: str, region: str, instance_type: str) -> float | None:
    """On-demand price per hour: the SCT instance catalog, else the SCT pricing code for the cloud.

    The catalog lookup also prices OCI flex shapes with custom memory by their base shape.
    """
    rate = get_hourly_rate(cloud, region, instance_type, catalog_only=True)
    return (rate if rate.known else get_hourly_rate(cloud, region, instance_type)).price_per_hour


def owner_coverage(owners: list[dict]) -> dict:
    return {
        "team": sum(1 for o in owners if o["teams"]),
        "people_only": sum(1 for o in owners if o["people"] and not o["teams"]),
        "none": sum(1 for o in owners if not o["teams"] and not o["people"]),
    }


@dataclass
class ArgusTree:
    """The Argus releases, groups and tests, with the test key of each test."""

    release_names: dict[str, str]
    groups: list[dict]
    tests: dict[str, dict]

    def key_of(self, test: dict) -> str:
        return analysis.test_key(test["build_system_id"], self.release_names.get(test["release_id"], ""))

    def in_scope(self, item: dict) -> bool:
        return self.release_names.get(item["release_id"]) not in analysis.EXCLUDED_RELEASES


def fetch_tree(reader: argus_source.ArgusReader) -> ArgusTree:
    releases, groups, tests = argus_source.fetch_tests(reader)
    return ArgusTree({rel["id"]: rel["name"] for rel in releases}, groups, {test["id"]: test for test in tests})


def owners_from_argus(reader: argus_source.ArgusReader, tree: ArgusTree) -> tuple[dict[str, set[str]], dict]:
    """Return the release-plan assignees of each test key, and the in-scope Argus jobs of each test key."""
    assignees, users = argus_source.fetch_assignees(reader, [g["id"] for g in tree.groups if tree.in_scope(g)])
    people, jobs_of_key = {}, {}
    for test in tree.tests.values():
        if not tree.in_scope(test):
            continue
        key = tree.key_of(test)
        jobs_of_key.setdefault(key, []).append(test)
        for user_id in assignees.get(test["id"], []):
            people.setdefault(key, set()).add(users.get(user_id, user_id))
    return people, jobs_of_key


def not_run_entries(reader: argus_source.ArgusReader, sct_root: Path, no_run: list, owners: dict, context: dict):
    """Return one entry per defined pipeline with no run, with its Argus jobs and its latest run ever."""
    jobs_of_key = context["jobs_of_key"]
    deprecated = pipelines.deprecated_keys(sct_root)
    last_runs = argus_source.fetch_last_runs(reader, [job["id"] for p in no_run for job in jobs_of_key.get(p.key, [])])
    entries = []
    for p in no_run:
        jobs = jobs_of_key.get(p.key, [])
        last = max((last_runs[job["id"]] for job in jobs if job["id"] in last_runs), default=None)
        entries.append(
            {
                "test": p.key,
                "file": p.file,
                "status": "deprecated" if p.key in deprecated else "active",
                "argus_jobs": len(jobs),
                "argus_jobs_enabled": sum(1 for job in jobs if job["enabled"]),
                "ran_in_staging": p.key in context["staging_keys"],
                "last_run": last[0][:10] if last else None,
                "last_status": last[1] if last else None,
                "owner": owners[p.key],
            }
        )
    return entries


def idle_release_jobs(tree: ArgusTree, runs: list[dict], ran_keys: set[str], known_keys: set[str]) -> list[dict]:
    """Return the enabled SCT jobs, in the releases that had runs, with no run of their own."""
    active = {tree.release_names[run["release_id"]] for run in runs}
    ran_test_ids = {run["test_id"] for run in runs}
    return [
        {
            "job": test["build_system_id"],
            "release": tree.release_names[test["release_id"]],
            "ran_elsewhere": key in ran_keys,
        }
        for test in tree.tests.values()
        if tree.release_names.get(test["release_id"]) in active
        and test["enabled"]
        and test["id"] not in ran_test_ids
        and test.get("plugin_name") in {argus_source.SCT_PLUGIN, None}
        and (key := tree.key_of(test)) in known_keys
    ]


def run_summary(all_runs: list[dict], runs: list[dict], rows: list[dict], no_data_reasons: Counter) -> dict:
    kinds = Counter(run["_kind"] for run in runs)
    return {
        "runs_total_pulled": len(all_runs),
        "runs_in_scope": len(runs),
        "runs_excluded": len(all_runs) - len(runs),
        "excluded_releases": sorted(analysis.EXCLUDED_RELEASES),
        "tests_in_scope": len(rows),
        "tests_with_cost": sum(1 for row in rows if row["cost_runs"]),
        "runs_by_kind": {kind: kinds[kind] for kind in ("actual", "estimated", "non-cloud", "no-data")},
        "no_data_reasons": dict(no_data_reasons),
        "est_cost_with_data": round(sum(row["cost_est_total"] or 0 for row in rows)),
        "total_hours": round(sum(row["hours_measured"] for row in rows)),
        "runs_end_from_heartbeat": sum(
            1
            for run in runs
            if run["status"] in analysis.DONE_STATUSES and (run.get("end_time") or "1970").startswith("1970")
        ),
        "owner_rows": owner_coverage([row["owner"] for row in rows]),
    }


def pipeline_data(
    reader: argus_source.ArgusReader, sct_root: Path, tree: ArgusTree, rows: list[dict], all_runs
) -> dict:
    """Set the owner of each row. Return the pipelines with no run, the idle release jobs and their summary."""
    defined = pipelines.defined_pipelines(sct_root)
    cases = pipelines.jenkinsfile_test_cases(sct_root, defined)
    teams = pipelines.owning_teams(sct_root, defined, cases)
    people, jobs_of_key = owners_from_argus(reader, tree)
    ran_keys = {row["test"] for row in rows}
    keys = ran_keys | {p.key for p in defined}
    owners = {key: {"teams": teams.get(key, []), "people": sorted(people.get(key, ()))} for key in keys}
    for row in rows:
        row["owner"] = owners[row["test"]]
    staging_keys = {tree.key_of(tree.tests[run["test_id"]]) for run in all_runs if not tree.in_scope(run)}
    not_run_defined = not_run_entries(
        reader,
        sct_root,
        pipelines.pipelines_with_no_run(defined, ran_keys),
        owners,
        {"jobs_of_key": jobs_of_key, "staging_keys": staging_keys},
    )
    return {
        "not_run_defined": not_run_defined,
        "not_run_jobs": idle_release_jobs(tree, [run for run in all_runs if tree.in_scope(run)], ran_keys, keys),
        "unreferenced_cases": pipelines.unreferenced_test_cases(sct_root, cases),
        "summary": {
            "defined_jenkinsfiles": len({p.key for p in defined if not p.trigger}),
            "quarantine_dirs": list(pipelines.QUARANTINE_DIRS),
            "quarantined_pipelines": sorted({p.key for p in defined if p.quarantined}),
            "not_run_defined": len(not_run_defined),
            "owner_not_run": owner_coverage([entry["owner"] for entry in not_run_defined]),
            "test_cases_total": len(list((sct_root / "test-cases").rglob("*.y*ml"))),
        },
    }


def build_report_data(reader: argus_source.ArgusReader, sct_root: Path, since: datetime, until: datetime) -> dict:
    tree = fetch_tree(reader)
    all_runs = argus_source.fetch_runs(reader, list(tree.tests.values()), since, until)
    runs = [run for run in all_runs if tree.in_scope(run)]
    no_data_reasons = analysis.price_runs(runs, argus_source.fetch_actual_costs(reader, runs), on_demand_rate)
    run_issues, issues = analysis.link_issues(
        argus_source.fetch_issues(reader, sorted({run["release_id"] for run in runs})), {run["id"] for run in runs}
    )
    rows = analysis.build_rows(runs, tree.tests, tree.release_names, run_issues, issues)
    data = pipeline_data(reader, sct_root, tree, rows, all_runs)
    data["summary"] = {
        "period": [since.date().isoformat(), until.date().isoformat()],
        "generated": datetime.now(UTC).isoformat(timespec="minutes"),
        "argus_url": reader.base_url,
        "sct_commit": subprocess.run(
            ["git", "-C", str(sct_root), "rev-parse", "--short=9", "HEAD"], capture_output=True, text=True, check=False
        ).stdout.strip(),
        **run_summary(all_runs, runs, rows, no_data_reasons),
        "area_counts": dict(Counter(issue.area for issue in issues.values())),
        "other_repos": dict(Counter(i.source for i in issues.values() if i.area == "other").most_common()),
        **data["summary"],
    }
    return {**data, "rows": rows, "issues": issues}


def _counts(row: dict, run_pos: dict[str, int], issue_index: dict[str, int]) -> dict:
    """The fields of a row that change with the release filter."""
    unique = set(row["product_unique_ids"])
    linked = [
        [issue_index[issue_id], [run_pos[run_id] for run_id in row["issue_runs"][issue_id]], int(issue_id in unique)]
        for area in analysis.AREAS
        for issue_id in row["issue_ids"].get(area, [])
    ]
    return {
        "be": row["backend"], "n": row["runs"], "p": row["passed"], "f": row["failed"], "a": row["aborted"],
        "run": row["running"], "timer": row["timer_runs"], "P": row["product"], "U": row["product_unique"],
        "OV": row["oversized"], "F": row["framework"], "O": row["other"], "looked": row["looked_share"],
        "fl": row["failed_looked"], "inv": row["inv"], "mx": row["cost_max_run"], "est": row["cost_est_total"],
        "cn": row["cost_runs"], "cact": row["cost_actual_runs"], "kind": row["cost_kind"], "h": row["hours_est_total"],
        "last": row["last_run"], "flags": row["flags"], "iss": linked,
    }  # fmt: skip


def page_payload(data: dict) -> dict:
    """The compact JSON that the page script reads."""
    issue_ids = list(data["issues"])
    issue_index = {issue_id: i for i, issue_id in enumerate(issue_ids)}
    issues = [[i.key, i.title, i.url, i.area, i.state] for i in (data["issues"][issue_id] for issue_id in issue_ids)]
    rows = []
    for row in data["rows"]:
        run_pos = {run[0]: i for i, run in enumerate(row["run_list"])}
        rows.append({
            "t": row["test"], "rel": row["releases"], "jobs": row["jobs"], "own": row["owner"],
            "runs": row["run_list"], "tests": row["argus_tests"], **_counts(row, run_pos, issue_index),
            "byRel": {rel: _counts(sub, run_pos, issue_index) for rel, sub in row["by_release"].items()},
        })  # fmt: skip
    per_release = Counter(job["release"] for job in data["not_run_jobs"])
    elsewhere = Counter(job["release"] for job in data["not_run_jobs"] if job["ran_elsewhere"])
    not_run = [
        {"t": x["test"], "file": x["file"], "st": x["status"], "jobs": x["argus_jobs"], "en": x["argus_jobs_enabled"],
         "stg": x["ran_in_staging"], "last": x["last_run"], "ls": x["last_status"], "own": x["owner"]}
        for x in data["not_run_defined"]
    ]  # fmt: skip
    return {
        "summary": data["summary"],
        "rows": rows,
        "issues": issues,
        "notRun": not_run,
        "relNotRun": [[rel, count, elsewhere[rel]] for rel, count in per_release.most_common()],
        "jobsElsewhere": sorted([job["release"], job["job"]] for job in data["not_run_jobs"] if job["ran_elsewhere"]),
        "unrefCases": data["unreferenced_cases"],
    }


def render_html(data: dict) -> str:
    # "<" as \u003c keeps any Argus text from closing or opening a tag inside the script.
    payload = json.dumps(page_payload(data), separators=(",", ":")).replace("<", "\\u003c")
    return TEMPLATE.read_text().replace("/*__DATA__*/null", payload)


def write_report(sct_root: Path, since: datetime, until: datetime, output: Path, workers: int) -> dict:
    """Write the HTML report to output and return its summary."""
    reader = argus_source.ArgusReader(workers=workers)
    try:
        data = build_report_data(reader, sct_root, since, until)
    finally:
        reader.close()
    output.write_text(render_html(data))
    return data["summary"]
