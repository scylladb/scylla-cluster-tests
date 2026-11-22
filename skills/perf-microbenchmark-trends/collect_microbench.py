#!/usr/bin/env python3
"""Collect weekly microbenchmark metrics from Argus into one CSV for the Microbenchmark Trends dashboard.

Usage:
    python3 collect_microbench.py --since 2026-09-18 --out microbench.csv

Each output row is one Argus run that produced a result table. Runs with no result
table (test_error, provisioning failures) are skipped, because they carry no value
to plot.
"""

import argparse
import csv
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor

ARGUS_URL = "https://argus.scylladb.com"

# Argus test ids of the weekly microbenchmark jobs in scylla-enterprise/perf-regression.
TESTS = [
    ("simple-query-weekly-microbenchmark_arm64", "a0063c73-efcf-4878-988d-72af779dc59d"),
    ("simple-query-weekly-microbenchmark_arm64-write", "dcc1afa0-2225-468c-9f45-5cfc8486f7f8"),
    ("simple-query-weekly-microbenchmark_x86_64", "03464849-60e8-46c8-91b9-955cdeb07ea6"),
    ("simple-query-weekly-microbenchmark_x86_64-write", "6e745123-cb53-482b-836c-0609bd36a4e6"),
    ("cql-raw-weekly-microbenchmark_arm64", "474ac937-cbc7-45d1-9375-746b8328f51b"),
    ("cql-raw-weekly-microbenchmark_arm64-write", "68b9d59b-bf4e-4aa1-abea-032e0fd7fa8d"),
    ("cql-raw-weekly-microbenchmark_x86_64", "e62a078f-924b-4662-a77b-bd9daa6e2241"),
    ("cql-raw-weekly-microbenchmark_x86_64-write", "006ddbcc-b344-4bb5-ac1b-374588305aa4"),
    ("alternator-weekly-microbenchmark_arm64", "8e5555c2-031a-4d97-8470-3ccb45ec7d6d"),
    ("alternator-weekly-microbenchmark_arm64-write", "4381b28e-d072-4a95-a109-c641eb5b069f"),
    ("alternator-weekly-microbenchmark_x86_64", "156ce642-ae0d-4ab1-ad3f-22cbb5037e7d"),
    ("alternator-weekly-microbenchmark_x86_64-write", "6d7aa7ad-a9c7-46c5-8da8-061e21c121ed"),
]

# test name prefix -> benchmark label used in the CSV, the page and the email
BENCHMARK_OF = {"simple-query": "perf-simple-query", "cql-raw": "perf-cql-raw", "alternator": "perf-alternator"}

# metric name in the Argus table -> (CSV value column, CSV status column)
METRICS = {
    "instructions_per_op": ("instructions_per_op", "ipo_status"),
    "cpu_cycles_per_op": ("cpu_cycles_per_op", "cpu_status"),
    "allocs_per_op": ("allocs_per_op", "allocs_status"),
}

FIELDS = [
    "benchmark",
    "workload",
    "arch",
    "chart",
    "run_start",
    "release",
    "scylla_version",
    "status",
    "started_by",
    "run_id",
    "argus_url",
] + [col for pair in METRICS.values() for col in pair]


def argus(*args: str) -> object:
    """Run one argus CLI command and return its parsed JSON output.

    Args:
        *args: argus subcommand and its arguments, without --url.

    Returns:
        Parsed JSON, or a dict with an "_err" key when the output is not JSON.
    """
    proc = subprocess.run(
        ["timeout", "150", "argus", *args, "--url", ARGUS_URL], capture_output=True, text=True, check=False
    )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"_err": (proc.stdout + proc.stderr)[:300]}


def release_line(scylla_version: str) -> str:
    """Map a scylla_version to its release line: any ~dev build is master, else major.minor."""
    if scylla_version.endswith("~dev"):
        return "master"
    return ".".join(scylla_version.split(".")[:2])


def full_version(scylla_version: str, packages: list) -> str:
    """Build version.date.revision; microbenchmarks often lack date on scylla-server-target."""
    pkgs = {p["name"]: p for p in packages or []}
    pkg = pkgs.get("scylla-server-target") or {}
    if not pkg.get("date"):
        pkg = pkgs.get("scylla-server") or pkg
    return f"{scylla_version.replace('~', '.')}.{pkg.get('date', '')}.{pkg.get('revision_id', '')}"


def collect_run(job: tuple) -> dict | None:
    """Fetch details and results for one run and flatten them into a CSV row.

    Args:
        job: (test name, Argus test id, run object from `argus run list`).

    Returns:
        The CSV row, or None when the run has no result table.
    """
    name, test_id, run = job
    if run["status"] == "test_error":
        return None
    results = argus("run", "results", "--run-id", run["id"], "--test-id", test_id)
    if not isinstance(results, list) or not results:
        return None
    cells = results[0]["rows"][0]["cells"]
    details = argus("run", "details", "--run-id", run["id"])
    details = details.get("run", details)
    scylla_version = details.get("scylla_version") or ""
    benchmark = BENCHMARK_OF[name.split("-weekly-")[0]]
    workload = "write" if name.endswith("-write") else "read"
    arch = "arm64" if "arm64" in name else "x86_64"
    row = {
        "benchmark": benchmark,
        "workload": workload,
        "arch": arch,
        "chart": f"{benchmark} {workload} {arch}",
        "run_start": run["start_time"][:19] + "Z",
        "release": release_line(scylla_version),
        "scylla_version": full_version(scylla_version, details.get("packages")),
        "status": run["status"],
        "started_by": details.get("started_by"),
        "run_id": run["id"],
        "argus_url": f"{ARGUS_URL}/test/{test_id}/runs?additionalRuns[]={run['id']}",
    }
    for metric, (value_col, status_col) in METRICS.items():
        cell = cells.get(metric) or {}
        value = cell.get("value")
        row[value_col] = "" if value in (None, "") else round(float(value), 2)
        row[status_col] = cell.get("status", "") if value not in (None, "") else ""
    return row


def main() -> None:
    """Collect all runs since --since and write the CSV."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="first day to include, YYYY-MM-DD")
    parser.add_argument("--out", default="microbench.csv")
    args = parser.parse_args()

    jobs = []
    for name, test_id in TESTS:
        runs = argus("run", "list", "--test-id", test_id, "--limit", "100")
        if not isinstance(runs, list):
            raise SystemExit(f"argus run list failed for {name}: {runs}")
        jobs += [(name, test_id, r) for r in runs if r["start_time"][:10] >= args.since]

    with ThreadPoolExecutor(8) as pool:
        rows = [r for r in pool.map(collect_run, jobs) if r]
    rows.sort(key=lambda r: (r["chart"], r["run_start"]))

    with open(args.out, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(jobs)} runs listed, {len(rows)} with results -> {args.out}")


if __name__ == "__main__":
    main()
