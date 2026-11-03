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

"""Model of the SSH load of a large SCT scale test (scale-450-500), to compare SSH transports (SCT-1396).

Each of `--nodes` virtual nodes has one remoter, as `BaseNode.remoter`. Nodes spread over `--hosts` sshd
containers. A remoter keeps one connection per thread, so every thread below that touches a node opens its own
SSH connection, as SCT does. Periods are compressed from the real ones (30s coredump poll, 5 min health check).

  setup     - one thread per node at the same time (`wait_for_init_wrap`): connect, `--setup-commands` short commands
  poll      - one thread per node for `--duration` (`CoredumpThreadBase`): `true` + small command every `--poll-interval`
  stream    - `--streams` nodes run one long command (stress tool / journalctl -f) that prints `--stream-rate` lines/s,
              a watcher measures each line's delivery lag
  fanout    - every `--fanout-interval`, a new `--fanout-workers` pool over all nodes (cluster health check):
              `nodetool status` sized output (100 B per node) + a small command
  burst     - every `--burst-interval`, a new 32 worker pool, one command per node (`run_func_parallel`)

Usage:
  python -m utils.ssh_scale_benchmark --transports libssh2 asyncssh --nodes 500 --hosts 25
"""

import argparse
import json
import os
import random
import resource
import shlex
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from invoke.watchers import StreamWatcher

from sdcm.remote import RemoteCmdRunnerBase
from utils.ssh_transport_benchmark import cpu_seconds, percentile, start_hosts, stop_hosts

COMPONENTS = ("setup-connect", "setup-cmd", "poll", "stream-lag", "fanout", "burst")


class LagWatcher(StreamWatcher):
    """Each streamed line starts with the remote EPOCHREALTIME, the lag is now minus that."""

    def __init__(self, record):
        super().__init__()
        self.record = record

    def submit(self, stream):
        return []

    def submit_line(self, line: str):
        stamp, _, _ = line.partition(" ")
        try:
            self.record(time.time() - float(stamp))
        except ValueError:
            pass


class Recorder:
    def __init__(self):
        self.lock = threading.Lock()
        self.values = defaultdict(list)
        self.errors = defaultdict(int)

    def add(self, component, value):
        with self.lock:
            self.values[component].append(value)

    def timed(self, component, remoter, cmd, **kwargs):
        started = time.perf_counter()
        try:
            remoter.run(cmd, verbose=False, **kwargs)
        except Exception:  # noqa: BLE001
            with self.lock:
                self.errors[component] += 1
            return
        self.add(component, time.perf_counter() - started)


def run_model(args) -> list[dict]:  # noqa: PLR0914
    RemoteCmdRunnerBase.set_default_ssh_transport(args.child)
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))  # one fd per connection, SCT runners raise it too
    rec = Recorder()
    remoters = [RemoteCmdRunnerBase.create_remoter(hostname=args.ips[i % len(args.ips)], user="test", port=22)
                for i in range(args.nodes)]  # fmt: skip
    marks = {"start": (time.perf_counter(), cpu_seconds())}

    # setup storm: every node at once, as wait_for_init_wrap starts one thread per node
    def setup(remoter):
        rec.timed("setup-connect", remoter, "true")
        for i in range(args.setup_commands):
            rec.timed("setup-cmd", remoter, "head -c 4096 /dev/zero" if i % 4 == 0 else f"echo step {i}")

    threads = [threading.Thread(target=setup, args=(r,)) for r in remoters]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    marks["setup"] = (time.perf_counter(), cpu_seconds())

    # steady state
    stop = threading.Event()
    end = time.monotonic() + args.duration

    def poll(remoter):
        stop.wait(random.uniform(0, args.poll_interval))
        while not stop.is_set():
            rec.timed("poll", remoter, "true")
            rec.timed("poll", remoter, "echo '[]'")
            stop.wait(args.poll_interval)

    lines = max(1, int(args.duration * args.stream_rate))
    pad = "x" * 140
    # bash for $EPOCHREALTIME: the login shell may be dash
    stream_cmd = "bash -c " + shlex.quote(f"for i in $(seq {lines}); do printf '%s %s\\n' \"$EPOCHREALTIME\" {pad}; "
                                          f"sleep {1 / args.stream_rate:.4f}; done")  # fmt: skip

    def stream(remoter):
        try:
            remoter.run(stream_cmd, verbose=False, watchers=[LagWatcher(lambda lag: rec.add("stream-lag", lag))],
                        timeout=args.duration * 3)  # fmt: skip
        except Exception:  # noqa: BLE001
            with rec.lock:
                rec.errors["stream-lag"] += 1

    def pool_round(component, workers, cmds):
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for remoter in remoters:
                pool.submit(lambda r=remoter: [rec.timed(component, r, c) for c in cmds])

    def periodic(component, interval, workers, cmds):
        while not stop.wait(interval) and time.monotonic() < end:
            pool_round(component, workers, cmds)

    status_cmd = f"head -c {100 * args.nodes} /dev/zero"
    background = [threading.Thread(target=poll, args=(r,)) for r in remoters]
    background += [threading.Thread(target=stream, args=(r,)) for r in remoters[: args.streams]]
    background += [
        threading.Thread(target=periodic, args=("fanout", args.fanout_interval, args.fanout_workers,
                                                [status_cmd, "echo host_id"])),
        threading.Thread(target=periodic, args=("burst", args.burst_interval, 32, ["echo done"])),
    ]  # fmt: skip
    for t in background:
        t.start()
    time.sleep(args.duration)
    stop.set()
    for t in background:
        t.join()
    marks["steady"] = (time.perf_counter(), cpu_seconds())
    open_fds = len(os.listdir("/proc/self/fd"))
    for remoter in remoters:
        remoter.stop()

    rows = []
    for component in COMPONENTS:
        values = rec.values[component]
        row = {"transport": args.child, "component": component, "count": len(values),
               "errors": rec.errors[component]}  # fmt: skip
        if values:
            row["p50_ms"] = percentile(values, 0.5) * 1000
            row["p99_ms"] = percentile(values, 0.99) * 1000
            row["max_ms"] = max(values) * 1000
        rows.append(row)
    for phase, prev in (("setup", "start"), ("steady", "setup")):
        rows.append({"transport": args.child, "component": f"[{phase}]", "wall_s": marks[phase][0] - marks[prev][0],
                     "cpu_s": marks[phase][1] - marks[prev][1]})  # fmt: skip
    peak_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    rows.append({"transport": args.child, "component": "[process]", "peak_rss_mb": peak_rss_mb, "open_fds": open_fds})
    return rows


def print_table(rows: list[dict]) -> None:
    header = (
        f"{'transport':<9} {'component':<14} {'count':>7} {'err':>5} {'p50 ms':>8} {'p99 ms':>9} {'max ms':>9}  other"
    )
    print(header)
    print("-" * (len(header) + 20))
    for r in sorted(rows, key=lambda r: (r["component"], r["transport"])):
        if "count" in r:
            nums = [f"{r[k]:.1f}" if k in r else "-" for k in ("p50_ms", "p99_ms", "max_ms")]
            print(f"{r['transport']:<9} {r['component']:<14} {r['count']:>7} {r['errors']:>5} "
                  f"{nums[0]:>8} {nums[1]:>9} {nums[2]:>9}")  # fmt: skip
        else:
            other = " ".join(f"{k}={v:.1f}" if isinstance(v, float) else f"{k}={v}" for k, v in r.items()
                             if k not in ("transport", "component"))  # fmt: skip
            print(f"{r['transport']:<9} {r['component']:<14} {'':>7} {'':>5} {'':>8} {'':>9} {'':>9}  {other}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transports", nargs="+", default=["libssh2", "asyncssh"])
    parser.add_argument("--nodes", type=int, default=500)
    parser.add_argument("--hosts", type=int, default=25)
    parser.add_argument("--setup-commands", type=int, default=20)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--poll-interval", type=float, default=5)
    parser.add_argument("--streams", type=int, default=50)
    parser.add_argument("--stream-rate", type=float, default=20)
    parser.add_argument("--fanout-interval", type=float, default=20)
    parser.add_argument("--fanout-workers", type=int, default=5)
    parser.add_argument("--burst-interval", type=float, default=30)
    parser.add_argument("--json", help="also write the rows to this file")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--ips", nargs="+", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.child:
        print(json.dumps(run_model(args)))
        return

    ids, ips = start_hosts(args.hosts)
    rows = []
    try:
        time.sleep(2)  # sshd start
        passthrough = [a for a in sys.argv[1:] if a != "--transports" and a not in args.transports]
        for transport in args.transports:
            cmd = [sys.executable, "-m", "utils.ssh_scale_benchmark", *passthrough, "--child", transport,
                   "--ips", *ips]  # fmt: skip
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
