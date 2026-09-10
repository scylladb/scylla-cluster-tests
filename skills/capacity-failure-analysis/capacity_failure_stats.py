#!/usr/bin/env python3
"""Collect AWS capacity-provisioning failures for SCT test runs from Argus.

Counts runs that died before provisioning with a capacity error signature
(CapacityReservationError and friends), resolves the AWS region each failure
happened in, and aggregates by week, region and job.

Region needs two sources: Argus leaves ``region_name`` empty on these runs
because the test aborts before any instance is allocated, so the region comes
from the Jenkins ``region`` build parameter, with a nearest-neighbour fallback
for builds that have rotated out of Jenkins history. See
``references/region-resolution.md``.

Run from the repository root so ``sdcm`` is importable::

    PYTHONPATH=. .venv/bin/python skills/capacity-failure-analysis/capacity_failure_stats.py \
        --registry tests.tsv --period month --json-out capacity.json

Periods: ``--period all`` (since the first recorded run), ``month`` (30 days),
``week`` (7 days), or ``--weeks N`` for anything else.
"""

import argparse
import json
import logging
import re
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

LOGGER = logging.getLogger("capacity-failure-stats")

# Signatures of "we could not get the machines" failures. Keep CapacityReservationError
# first: it is the only one that aborts the test outright, the others are usually
# retried by the AZ/region fallback in sdcm/sct_provision/aws/layout.py.
SIGNATURES = {
    "CapacityReservationError": re.compile(r"CapacityReservationError", re.IGNORECASE),
    "ProvisioningCapacityExhausted": re.compile(r"ProvisioningCapacityExhausted", re.IGNORECASE),
    "InsufficientInstanceCapacity": re.compile(r"InsufficientInstanceCapacity", re.IGNORECASE),
}

JENKINS_TREE = "?tree=number,timestamp,result,actions[parameters[name,value]]"


class ArgusError(RuntimeError):
    """Raised when the argus CLI returns a non-zero exit status."""


def argus(*args: str) -> object:
    """Run the argus CLI and return its parsed JSON output.

    Args:
        *args: Arguments passed straight to the ``argus`` executable.

    Returns:
        The decoded JSON document, or None when the command printed nothing.

    Raises:
        ArgusError: If the CLI exits non-zero.
    """
    proc = subprocess.run(["argus", *args], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise ArgusError(f"argus {' '.join(args)}: {proc.stderr.strip()[:300]}")
    return json.loads(proc.stdout or "null")


def parse_timestamp(value: str) -> datetime:
    """Parse an Argus ISO-8601 timestamp into an aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso_week(when: datetime) -> str:
    """Return the ISO week label (e.g. ``2026-W36``) for a datetime."""
    year, week, _ = when.isocalendar()
    return f"{year}-W{week:02d}"


def week_monday(week: str) -> str:
    """Return the Monday of an ISO week label as an ISO date string."""
    return str(datetime.fromisocalendar(int(week[:4]), int(week[6:]), 1).date())


def load_registry(path: str) -> list[dict]:
    """Load a ``name<TAB>test_id[<TAB>category]`` registry file."""
    tests = []
    with open(path, encoding="utf-8") as registry:
        for line in registry:
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            tests.append({"name": fields[0], "test_id": fields[1], "category": fields[2] if len(fields) > 2 else ""})
    return tests


def resolve_window(period: str | None, weeks: int | None) -> tuple[float, str]:
    """Turn the requested reporting period into an ``--after`` timestamp and a label.

    The three named periods are the ones the skill offers up front. ``all`` means
    "since the first recorded run", expressed as epoch 0 so Argus applies no lower
    bound at all.

    Args:
        period: One of ``all``, ``month``, ``week``, or None when ``weeks`` is used.
        weeks: Explicit look-back in weeks, used when ``period`` is None.

    Returns:
        A ``(after_timestamp, human_label)`` pair.
    """
    now = datetime.now(timezone.utc)
    if period == "all":
        return 0.0, "all recorded history"
    if period == "month":
        return (now - timedelta(days=30)).timestamp(), "last month (30 days)"
    if period == "week":
        return (now - timedelta(days=7)).timestamp(), "last week (7 days)"
    return (now - timedelta(weeks=weeks)).timestamp(), f"last {weeks} weeks"


def collect_runs(tests: list[dict], after: float, limit: int) -> list[dict]:
    """List every Argus run of the given tests started after a Unix timestamp.

    Warns when a test returns exactly ``limit`` runs, because the window is then
    probably truncated — most likely on ``--period all``, where the whole history
    can exceed the default page size.
    """
    runs = []
    for test in tests:
        try:
            found = argus("run", "list", "--test-id", test["test_id"], "--after", str(after), "--limit", str(limit))
        except ArgusError as exc:
            LOGGER.warning("skipping %s: %s", test["name"], exc)
            continue
        for run in found or []:
            run["_test"] = test
        runs.extend(found or [])
        if len(found or []) >= limit:
            LOGGER.warning(
                "%s returned the full limit of %s runs - raise --limit, results may be truncated", test["name"], limit
            )
        LOGGER.info("%s: %s runs", test["name"], len(found or []))
    return runs


def match_signature(run_id: str) -> tuple[str | None, str]:
    """Classify one run by the capacity signature in its CRITICAL/ERROR events.

    Args:
        run_id: Argus run UUID.

    Returns:
        A ``(signature, message)`` pair, or ``(None, "")`` when the run shows no
        capacity failure. Only SCT runs have per-event records.
    """
    try:
        events = argus("run", "events", "--run-id", run_id, "--limit", "200") or []
    except ArgusError as exc:
        LOGGER.warning("events unavailable for %s: %s", run_id, exc)
        return None, ""
    for name, pattern in SIGNATURES.items():
        for event in events:
            message = event.get("message", "")
            if pattern.search(message):
                return name, re.sub(r"\s+", " ", message).strip()
    return None, ""


def argus_region(run_id: str) -> list[str]:
    """Return the regions Argus recorded for a run (empty for pre-provisioning failures)."""
    try:
        details = argus("run", "details", "--run-id", run_id) or {}
    except ArgusError as exc:
        LOGGER.warning("details unavailable for %s: %s", run_id, exc)
        return []
    regions = list(details.get("region_name") or [])
    for resource in details.get("allocated_resources") or []:
        region = (resource.get("instance_info") or {}).get("region")
        if region and region not in regions:
            regions.append(region)
    return regions


def jenkins_regions(build_urls: list[str], workers: int) -> dict[str, str | None]:
    """Fetch the ``region`` build parameter for each Jenkins build URL.

    Builds that have rotated out of Jenkins history answer 404; those map to None
    so the caller can fall back to inference.
    """
    import requests  # noqa: PLC0415  # optional dependency: only needed with --jenkins
    from sdcm.keystore import KeyStore  # noqa: PLC0415

    credentials = KeyStore().get_json("jenkins.json")
    auth = (credentials["username"], credentials["password"])

    def fetch(url: str) -> tuple[str, str | None]:
        try:
            response = requests.get(url.rstrip("/") + "/api/json" + JENKINS_TREE, auth=auth, timeout=90)
            if response.status_code != 200:
                return url, None
            params = {}
            for action in response.json().get("actions", []):
                for param in action.get("parameters", []) or []:
                    params[param["name"]] = param.get("value")
            return url, params.get("region")
        except Exception as exc:  # noqa: BLE001  # a single unreachable build must not abort the sweep
            LOGGER.warning("jenkins fetch failed for %s: %s", url, exc)
            return url, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(pool.map(fetch, build_urls))


def resolve_regions(runs: list[dict]) -> dict[str, float]:
    """Attach a region to every run, in order of trustworthiness.

    Jenkins build parameter beats the Argus record (they agree wherever both
    exist, and Jenkins covers the failed runs that Argus does not); anything
    still missing is inferred from the temporally nearest run of the same job.

    Args:
        runs: Run dicts, mutated in place with ``region`` and ``region_src``.

    Returns:
        Accuracy stats for the inference, so the caller can print a caveat.
    """
    for run in runs:
        if run.get("jenkins_region"):
            run["region"], run["region_src"] = run["jenkins_region"], "jenkins"
        elif run.get("argus_regions"):
            run["region"], run["region_src"] = run["argus_regions"][0], "argus"
        else:
            run["region"], run["region_src"] = None, None

    by_test: dict[str, list[dict]] = defaultdict(list)
    for run in runs:
        by_test[run["_test"]["name"]].append(run)

    def nearest(target: dict) -> str | None:
        best, best_gap = None, None
        for other in by_test[target["_test"]["name"]]:
            if other["run_id"] == target["run_id"] or not other["region"]:
                continue
            gap = abs((other["_started"] - target["_started"]).total_seconds())
            if best_gap is None or gap < best_gap:
                best, best_gap = other, gap
        return best["region"] if best else None

    hits = misses = 0
    for run in runs:
        if not run["region"]:
            continue
        predicted = nearest(run)
        if predicted:
            hits += predicted == run["region"]
            misses += predicted != run["region"]

    filled = 0
    for run in runs:
        if run["region"]:
            continue
        predicted = nearest(run)
        if predicted:
            run["region"], run["region_src"] = predicted, "inferred"
            filled += 1
        else:
            run["region"], run["region_src"] = "unknown", "unknown"

    checked = hits + misses
    return {
        "inferred": filled,
        "cross_validated": checked,
        "accuracy_pct": round(100.0 * hits / checked, 1) if checked else 0.0,
        "unknown": sum(1 for run in runs if run["region"] == "unknown"),
    }


def aggregate(records: list[dict], key) -> list[dict]:
    """Group records by a key function into run/failure/build counts."""
    buckets: dict[str, dict] = defaultdict(lambda: {"runs": 0, "failures": 0, "builds": set(), "hit_builds": set()})
    for record in records:
        bucket = buckets[key(record)]
        bucket["runs"] += 1
        bucket["builds"].add(record["build"])
        if record["signature"]:
            bucket["failures"] += 1
            bucket["hit_builds"].add(record["build"])
    rows = []
    for name, bucket in buckets.items():
        rows.append(
            {
                "key": name,
                "runs": bucket["runs"],
                "failures": bucket["failures"],
                "pct": round(100.0 * bucket["failures"] / bucket["runs"], 1) if bucket["runs"] else 0.0,
                "builds": len(bucket["builds"]),
                "hit_builds": len(bucket["hit_builds"]),
            }
        )
    return sorted(rows, key=lambda row: (-row["failures"], row["key"]))


def print_table(title: str, rows: list[dict]) -> None:
    """Print one aggregation as a fixed-width table."""
    print(f"\n{title}")
    print(f"{'':<46} {'Runs':>6} {'Fails':>6} {'%':>7} {'Builds':>7} {'Hit':>5}")
    print("-" * 82)
    for row in rows:
        print(
            f"{row['key']:<46} {row['runs']:>6} {row['failures']:>6} {row['pct']:>6.1f}% {row['builds']:>7} {row['hit_builds']:>5}"
        )
    runs = sum(row["runs"] for row in rows)
    failures = sum(row["failures"] for row in rows)
    print("-" * 82)
    print(f"{'TOTAL':<46} {runs:>6} {failures:>6} {100.0 * failures / runs if runs else 0:>6.1f}%")


def main() -> int:
    """Entry point: collect, classify, resolve regions and report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--test-id", action="append", help="Argus test UUID (repeatable)")
    source.add_argument("--registry", help="TSV file: name<TAB>test_id[<TAB>category]")
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--period",
        choices=["all", "month", "week"],
        help="Reporting period: all = since the first recorded run, month = last 30 days, week = last 7 days",
    )
    window.add_argument("--weeks", type=int, help="Explicit look-back window in weeks (default: 12)")
    parser.add_argument("--limit", type=int, default=500, help="Max runs fetched per test (default: 500)")
    parser.add_argument("--workers", type=int, default=10, help="Parallel API calls (default: 10)")
    parser.add_argument(
        "--no-jenkins", action="store_true", help="Skip Jenkins; regions come from Argus and inference only"
    )
    parser.add_argument("--json-out", help="Write the full per-run dataset here")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)

    tests = (
        load_registry(args.registry)
        if args.registry
        else [{"name": test_id, "test_id": test_id, "category": ""} for test_id in args.test_id]
    )
    after, window_label = resolve_window(args.period, args.weeks or 12)

    raw_runs = collect_runs(tests, after, args.limit)
    if not raw_runs:
        LOGGER.error("no runs found in the window")
        return 1

    runs = []
    for raw in raw_runs:
        runs.append(
            {
                "run_id": raw["id"],
                "_test": raw["_test"],
                "_started": parse_timestamp(raw["start_time"]),
                "start": raw["start_time"],
                "week": iso_week(parse_timestamp(raw["start_time"])),
                "status": raw["status"],
                "build": raw.get("build_job_url", ""),
            }
        )

    LOGGER.info("classifying %s runs...", len(runs))
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for run, (signature, message) in zip(runs, pool.map(lambda r: match_signature(r["run_id"]), runs)):
            run["signature"], run["message"] = signature, message

    LOGGER.info("resolving regions...")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for run, regions in zip(runs, pool.map(lambda r: argus_region(r["run_id"]), runs)):
            run["argus_regions"] = regions
    if not args.no_jenkins:
        found = jenkins_regions(sorted({run["build"] for run in runs if run["build"]}), args.workers)
        for run in runs:
            run["jenkins_region"] = found.get(run["build"])
    provenance = resolve_regions(runs)

    for run in runs:
        run["test"] = run["_test"]["name"]
        run["test_id"] = run["_test"]["test_id"]
        run["category"] = run["_test"]["category"]
        del run["_test"], run["_started"]

    failures = [run for run in runs if run["signature"]]
    # On --period all the requested lower bound is epoch 0, so report the first run
    # actually found rather than 1970.
    first_seen = min(run["start"] for run in runs)[:10]
    print(
        f"\nWindow: {window_label} · {first_seen} to today "
        f"· {len(tests)} tests · {len(runs)} runs · {len(failures)} capacity failures"
    )
    print(f"Signatures: {dict((name, sum(1 for r in failures if r['signature'] == name)) for name in SIGNATURES)}")
    print(
        f"Region provenance: {sum(1 for r in runs if r['region_src'] == 'jenkins')} jenkins, "
        f"{sum(1 for r in runs if r['region_src'] == 'argus')} argus, "
        f"{provenance['inferred']} inferred "
        f"(inference cross-validates at {provenance['accuracy_pct']}% on {provenance['cross_validated']} runs)"
    )

    print_table("Per region", aggregate(runs, lambda r: r["region"]))
    print_table("Per week", aggregate(runs, lambda r: f"{r['week']} (w/c {week_monday(r['week'])})"))
    print_table("Per job", aggregate(runs, lambda r: r["test"]))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as out:
            json.dump({"provenance": provenance, "runs": runs}, out, indent=2)
        LOGGER.info("wrote %s", args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
