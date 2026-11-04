#!/usr/bin/env python3
"""Export perf weekly-registry results from Argus to one long-format CSV row per (run, table, metric).

The test list is read from the Test Registry table in skills/perf-weekly-status-report/SKILL.md,
so the email report and the dashboard always track the same tests.

Usage:
    fetch_perf_trends.py OUTPUT_CSV [--days N] [--jobs N]

Example:
    fetch_perf_trends.py ~/perf-trends/perf-trends.csv --days 180
"""

import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[1] / "SKILL.md"
ROW_RE = re.compile(r"^\|\s*([\w.-]+)\s*\|\s*([0-9a-f-]{36})\s*\|\s*([^|]+?)\s*\|\s*$")
MICROBENCH = ("allocs_per_op", "cpu_cycles_per_op", "instructions_per_op")
FAILED = ("failed", "test_error", "error")
COLUMNS = [
    "test_order",
    "category",
    "test",
    "workload",
    "step",
    "metric",
    "table_desc",
    "value",
    "cell_status",
    "build_type",
    "branch",
    "scylla_version",
    "scylla_sha",
    "date",
    "build",
    "run_status",
    "run_id",
    "argus_url",
    "job_url",
    "test_info",
    "issues",
    "fetched_at",
]


def argus(*args):
    out = subprocess.run(["argus", "--non-interactive", *args], capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(f"argus {' '.join(args)}: {out.stderr.strip() or out.stdout.strip()}")
    return json.loads(out.stdout or "[]")


def read_registry():
    tests = [m.groups() for line in REGISTRY.read_text().splitlines() if (m := ROW_RE.match(line))]
    if not tests:
        sys.exit(f"Error: no tests found in the Test Registry table of {REGISTRY}")
    return tests


def branch_of(version):
    """`~dev` builds are master; X.Y.Z and X.Y.0~rcN belong to branch X.Y."""
    if "dev" in version:
        return "master", "master"
    m = re.match(r"^(\d{4}\.\d+)\.", version)
    return (m.group(1), "release") if m else (None, None)


def sha_of(run):
    for name in ("scylla-server-target", "scylla-server"):
        for pkg in run.get("packages") or []:
            if pkg.get("name") == name and pkg.get("revision_id"):
                return pkg["revision_id"]
    return ""


def describe_table(table):
    """The table's Argus description; microbenchmark JSON parameters become `key=value, ...`."""
    desc = table.get("description") or ""
    try:
        params = json.loads(desc)
    except ValueError:
        return desc
    if not isinstance(params, dict):
        return desc
    return ", ".join(f"{k}={v}" for k, v in params.items() if "," not in k)


def describe_test(run):
    """What the run exercised, from its Argus run object, for the dashboard's hover text."""
    cloud = run.get("cloud_setup") or {}
    nodes = [
        f"{role}: {n['node_amount']} x {n['instance_type']}"
        for role, key in (("DB nodes", "db_node"), ("loaders", "loader_node"))
        if (n := cloud.get(key)) and n.get("instance_type")
    ]
    lines = [
        f"Test method: {run['test_method']}" if run.get("test_method") else "",
        f"Cluster ({cloud.get('backend', '')}): {', '.join(nodes)}" if nodes else "",
        f"Config: {', '.join(run['config_files'])}" if run.get("config_files") else "",
        f"Jenkins job: {run['build_id']}" if run.get("build_id") else "",
    ]
    return "\n".join(line for line in lines if line)


def metrics_of(table):
    """Yield (workload, step, metric, value, status, description) for one Argus result table."""
    if "stalls" in table["name"]:
        return
    workload, _, step = table["name"].partition(" - ")
    step = step.removesuffix(" - latencies")
    values = {}
    for row in table.get("rows") or []:
        cells = row.get("cells") or {}
        throughput = [
            c["value"]
            for k, c in cells.items()
            if k.startswith("Throughput ") and isinstance(c.get("value"), (int, float))
        ]
        if step == "unthrottled" and throughput:
            values.setdefault("Throughput", []).append((sum(throughput), "UNSET"))
        duration = (cells.get("duration") or {}).get("value")
        # Operation steps (a nemesis, an upgrade, a node add) also chart how long the operation took.
        if isinstance(duration, (int, float)) and not step.isdigit() and step not in ("unthrottled", "Steady State"):
            values.setdefault("Duration", []).append((duration, "UNSET"))
        for key, cell in cells.items():
            if (key.startswith("P99 ") or key in MICROBENCH) and isinstance(cell.get("value"), (int, float)):
                values.setdefault(key, []).append((cell["value"], cell.get("status", "")))
    for metric, vals in values.items():
        nums = [v for v, _ in vals]
        # ponytail: several cycles of one step collapse to worst P99 / mean otherwise; per-cycle series if anyone needs them
        value = max(nums) if metric.startswith("P99") else statistics.fmean(nums)
        status = next((s for s in ("ERROR", "FAIL") if any(st == s for _, st in vals)), vals[0][1])
        yield workload, step, metric, round(value, 4), status, describe_table(table)


def issues_of(run):
    """Issues linked to a failed run, as compact JSON: [{key, title, url}]."""
    if run.get("status") not in FAILED:
        return ""
    try:
        issues = argus("issue", "list", "--run-id", run["id"])
    except RuntimeError as exc:
        print(f"warning: {exc}", file=sys.stderr)
        return ""
    keep = [{k: i.get(k, "") for k in ("key", "title", "url")} for i in issues]
    return json.dumps(keep, separators=(",", ":")) if keep else ""


def run_rows(order, category, test, run, fetched_at):
    branch, build_type = branch_of(run["scylla_version"])
    base = {
        "test_order": order,
        "category": category,
        "test": test,
        "build_type": build_type,
        "branch": branch,
        "scylla_version": run["scylla_version"],
        "scylla_sha": sha_of(run),
        "date": run.get("start_time", ""),
        "build": run.get("build_number", ""),
        "run_status": run.get("status", ""),
        "run_id": run["id"],
        "argus_url": f"https://argus.scylladb.com/tests/scylla-cluster-tests/{run['id']}",
        "job_url": run.get("build_job_url") or "",
        "test_info": describe_test(run),
        "issues": issues_of(run),
        "fetched_at": fetched_at,
    }
    try:
        tables = argus("run", "results", "--run-id", run["id"])
    except RuntimeError as exc:
        print(f"warning: {exc}", file=sys.stderr)
        tables = []
    rows = [
        dict(base, workload=w, step=s, metric=m, value=v, cell_status=st, table_desc=desc)
        for t in tables
        for w, s, m, v, st, desc in metrics_of(t)
    ]
    # A run without results still shows up in the weekly status table.
    rows = rows or [dict(base, workload="", step="", metric="", value="", cell_status="", table_desc="")]
    # test_info is per run and long: keep it on the first row only so the CSV stays small.
    for row in rows[1:]:
        row["test_info"] = ""
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("output_csv", type=Path)
    parser.add_argument("--days", type=int, default=180, help="how far back to fetch (default: 180)")
    parser.add_argument("--jobs", type=int, default=6, help="parallel argus calls (default: 6)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    fetched_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    after = str(int((now - timedelta(days=args.days)).timestamp()))
    work = []
    for order, (test, test_id, category) in enumerate(read_registry()):
        runs = argus("run", "list", "--test-id", test_id, "--after", after, "--limit", "2000", "--full")
        keep = [r for r in runs if r.get("status") != "running" and branch_of(r.get("scylla_version") or "")[0]]
        print(f"{test}: {len(keep)} runs")
        work += [(order, category, test, r) for r in keep]

    with ThreadPoolExecutor(args.jobs) as pool:
        rows = [row for rows in pool.map(lambda w: run_rows(*w, fetched_at), work) for row in rows]

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, COLUMNS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["test"], r["date"])))
    print(f"Wrote {len(rows)} rows for {len(work)} runs to {args.output_csv}")


if __name__ == "__main__":
    main()
