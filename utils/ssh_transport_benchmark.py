#!/usr/bin/env python3
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Compare SSH transports of the SCT remoter on local Docker sshd containers (SCT-1396).

Starts `--hosts` sshd containers, then runs each transport in its own subprocess, so CPU time and peak RSS
belong to one transport. Every session is one thread with its own remoter, as SCT node threads have.

Scenarios, for each session count:
  connect  - create a remoter and run its first command (handshake + auth + one command)
  short    - `true`, `--short-runs` times per session, latency per command
  large    - one command that prints `--large-mb` MB per session, aggregate throughput

Usage:
  python -m utils.ssh_transport_benchmark --transports libssh2 asyncssh --sessions 60 200
"""

import argparse
import json
import resource
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

from sdcm.remote import RemoteCmdRunnerBase

IMAGE = "ssh_server_none_auth_test:latest"
DOCKER_DIR = Path(__file__).resolve().parent.parent / "unit_tests" / "test_data" / "auth_none_ssh_docker"
# default sshd drops unauthenticated connections above 10 in flight, which would measure sshd, not the client
SSHD_CMD = ["/usr/sbin/sshd", "-D", "-o", "MaxStartups=1000:30:1000", "-o", "MaxSessions=1000"]


def start_hosts(count: int) -> tuple[list[str], list[str]]:
    subprocess.run(["docker", "build", "-q", "-t", IMAGE, str(DOCKER_DIR)], check=True, stdout=subprocess.DEVNULL)
    ids = [
        subprocess.check_output(["docker", "run", "-d", "--rm", IMAGE, *SSHD_CMD], text=True).strip()
        for _ in range(count)
    ]
    ips = [
        subprocess.check_output(
            ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", cid], text=True
        ).strip()
        for cid in ids
    ]
    return ids, ips


def stop_hosts(ids: list[str]) -> None:
    subprocess.run(["docker", "rm", "-f", *ids], check=False, stdout=subprocess.DEVNULL)


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * pct))]


def cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


SCENARIOS = ("connect", "short", "large")


def run_transport(transport: str, ips: list[str], sessions: int, short_runs: int, large_mb: int) -> list[dict]:
    """One thread per session runs every scenario; a remoter's connection belongs to the thread that opened it.

    A barrier between scenarios lines the threads up and records wall time and process CPU at each boundary.
    """
    RemoteCmdRunnerBase.set_default_ssh_transport(transport)
    marks = []
    barrier = threading.Barrier(sessions, action=lambda: marks.append((time.perf_counter(), cpu_seconds())))
    values = {scenario: [] for scenario in SCENARIOS}
    errors = []

    def session(i):
        remoter = None
        try:
            barrier.wait()
            started = time.perf_counter()
            remoter = RemoteCmdRunnerBase.create_remoter(hostname=ips[i % len(ips)], user="test", port=22)
            remoter.run("true", verbose=False)
            values["connect"].append(time.perf_counter() - started)
            barrier.wait()
            for _ in range(short_runs):
                started = time.perf_counter()
                remoter.run("true", verbose=False)
                values["short"].append(time.perf_counter() - started)
            barrier.wait()
            values["large"].append(len(remoter.run(f"head -c {large_mb << 20} /dev/zero", verbose=False).stdout))
            barrier.wait()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
            barrier.abort()
        finally:
            if remoter is not None:
                remoter.stop()

    threads = [threading.Thread(target=session, args=(i,)) for i in range(sessions)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(f"{transport}: {len(errors)} sessions failed, first: {errors[0]!r}") from errors[0]

    rows = []
    for index, scenario in enumerate(SCENARIOS):
        (start, cpu_start), (end, cpu_end) = marks[index], marks[index + 1]
        row = {"transport": transport, "scenario": scenario, "sessions": sessions, "wall_s": end - start,
               "cpu_s": cpu_end - cpu_start}  # fmt: skip
        if scenario == "large":
            row["mb_per_s"] = sum(values["large"]) / (1 << 20) / (end - start)
        else:
            row["p50_ms"] = statistics.median(values[scenario]) * 1000
            row["p99_ms"] = percentile(values[scenario], 0.99) * 1000
        rows.append(row)
    return rows


def child_main(args) -> None:
    rows = []
    for sessions in args.sessions:
        rows += run_transport(args.child, args.ips, sessions, args.short_runs, args.large_mb)
    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    for row in rows:
        row["peak_rss_mb"] = peak_rss_mb
    print(json.dumps(rows))


def print_table(rows: list[dict]) -> None:
    header = f"{'transport':<9} {'scenario':<8} {'sess':>5} {'p50 ms':>8} {'p99 ms':>8} {'MB/s':>8} {'wall s':>7} {'cpu s':>7} {'rss MB':>7}"
    print(header)
    print("-" * len(header))
    for r in sorted(rows, key=lambda r: (r["scenario"], r["sessions"], r["transport"])):
        p50 = f"{r['p50_ms']:.1f}" if "p50_ms" in r else "-"
        p99 = f"{r['p99_ms']:.1f}" if "p99_ms" in r else "-"
        mbs = f"{r['mb_per_s']:.0f}" if "mb_per_s" in r else "-"
        print(f"{r['transport']:<9} {r['scenario']:<8} {r['sessions']:>5} {p50:>8} {p99:>8} {mbs:>8} "
              f"{r['wall_s']:>7.2f} {r['cpu_s']:>7.2f} {r['peak_rss_mb']:>7.0f}")  # fmt: skip


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transports", nargs="+", default=["libssh2", "asyncssh"])
    parser.add_argument("--sessions", nargs="+", type=int, default=[60, 200])
    parser.add_argument("--hosts", type=int, default=10)
    parser.add_argument("--short-runs", type=int, default=20)
    parser.add_argument("--large-mb", type=int, default=5)
    parser.add_argument("--json", help="also write the rows to this file")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--ips", nargs="+", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.child:
        child_main(args)
        return

    ids, ips = start_hosts(args.hosts)
    rows = []
    try:
        time.sleep(2)  # sshd start
        for transport in args.transports:
            cmd = [sys.executable, "-m", "utils.ssh_transport_benchmark", "--child", transport, "--ips", *ips,
                   "--sessions", *map(str, args.sessions), "--short-runs", str(args.short_runs),
                   "--large-mb", str(args.large_mb)]  # fmt: skip
            child = subprocess.run(cmd, text=True, capture_output=True, check=False)
            if child.returncode:
                sys.exit(f"{transport} run failed:\n{child.stderr[-3000:]}")
            rows += json.loads(child.stdout.strip().splitlines()[-1])
    finally:
        stop_hosts(ids)
    print_table(rows)
    if args.json:
        Path(args.json).write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    main()
