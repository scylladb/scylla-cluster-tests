#!/usr/bin/env python3
"""Drive the whole SCT-901 bare-metal simulation end to end.

    uv run python scripts/baremetal-simulation/run_simulation.py                # phase A
    uv run python scripts/baremetal-simulation/run_simulation.py --teardown     # and clean up after
    uv run python scripts/baremetal-simulation/run_simulation.py --phase perf   # 3 db + loader + monitor

Runs the numbered step scripts in order, times each one, records progress in
.state/progress.json and prints a summary with the data points SCT-901 asks for.

Defaults chosen so a single command answers the ticket:

  * phase A  -- preflight, launch, node JSON, distro patch, test case, artifact
    test, log collection, then a host reset and a second artifact test -- which is
    SCT-901 criterion 1, two runs of the same build on the same host.
    ``--reset-passes 0`` drops the repeat.
  * ``--dirty-passes N`` adds runs on the *un-reset* host, which is criterion 2.
    That experiment is expected to FAIL; 09_reset_host.sh documents the measured
    reason.  It is off by default, and the run stops when it fails.
  * the hosts are LEFT RUNNING at the end, so a failure can be investigated on
    the box.  They carry keep=<SIM_KEEP_HOURS>/keep_action=terminate, so they
    cannot leak past that window.  ``--teardown`` terminates them (and reverts
    the distro patch) once the run is over.
  * a failing step stops the run; ``--resume`` starts again after the last step
    that succeeded, so a re-run does not re-provision the hosts.

Every step remains runnable on its own -- this only chains them.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import SIM_DIR, STATE_DIR, cfg, log, read_state, write_state  # noqa: E402


@dataclass
class Step:
    key: str
    title: str
    argv: list[str]
    env: dict[str, str] = field(default_factory=dict)
    #: A step that only reports; a failure is surfaced but does not stop the run.
    advisory: bool = False


def python_step(key: str, title: str, script: str, *args: str, **kwargs) -> Step:
    return Step(key=key, title=title, argv=[sys.executable, str(SIM_DIR / script), *args], **kwargs)


def shell_step(key: str, title: str, script: str, *args: str, **kwargs) -> Step:
    return Step(key=key, title=title, argv=["bash", str(SIM_DIR / script), *args], **kwargs)


def build_steps(args: argparse.Namespace) -> list[Step]:
    steps: list[Step] = [
        python_step("preflight", "preflight checks", "00_preflight.py"),
        python_step("launch", "launch the simulated hosts", "01_launch_hosts.py"),
        python_step("nodes", "write the bare-metal node JSON", "02_write_baremetal_config.py"),
        shell_step("peers", "map peer addresses between hosts", "10_map_peer_addresses.sh"),
        python_step("distro", "patch SCT for Fedora", "03_patch_sct_for_fedora.py", "--apply"),
    ]

    if args.phase == "artifact":
        steps += [
            python_step("testcase", "render the test case", "04_render_test_case.py", "--force"),
            shell_step("run", "artifact test (pass 1)", "05_run_artifact_test.sh"),
            shell_step("logs", "collect logs", "06_collect_logs.sh", advisory=True),
        ]
        pass_no = 1
        for _ in range(args.reset_passes):
            pass_no += 1
            steps += [
                shell_step("reset-%d" % pass_no, f"reset the host before pass {pass_no}", "09_reset_host.sh"),
                shell_step(
                    "run-%d" % pass_no, f"artifact test on the reset host (pass {pass_no})", "05_run_artifact_test.sh"
                ),
                shell_step("logs-%d" % pass_no, f"collect logs (pass {pass_no})", "06_collect_logs.sh", advisory=True),
            ]
        for _ in range(args.dirty_passes):
            pass_no += 1
            steps += [
                shell_step(
                    "dirty-%d" % pass_no,
                    f"artifact test on the DIRTY host (pass {pass_no}, expected to fail)",
                    "07_rerun_dirty_host.sh",
                ),
                shell_step("logs-%d" % pass_no, f"collect logs (pass {pass_no})", "06_collect_logs.sh", advisory=True),
            ]
    else:
        steps += [
            shell_step("perf", "performance smoke test", "08_run_perf_test.sh"),
            shell_step("logs", "collect logs", "06_collect_logs.sh", advisory=True),
        ]

    if args.teardown:
        steps += [
            python_step("revert", "revert the Fedora patches", "03_patch_sct_for_fedora.py", "--revert"),
            python_step("teardown", "terminate the hosts", "99_teardown.py", "--yes"),
        ]
    return steps


def run_step(step: Step, dry_run: bool) -> tuple[bool, float]:
    print()
    log(f"=== {step.title} ===")
    log("  " + " ".join(step.argv))
    if dry_run:
        return True, 0.0
    started = time.monotonic()
    env = {**os.environ, **step.env}
    result = subprocess.run(step.argv, env=env, cwd=str(SIM_DIR.parents[1]), check=False)
    elapsed = time.monotonic() - started
    log(f"--- {step.title}: {'ok' if result.returncode == 0 else f'FAILED (rc={result.returncode})'} in {elapsed:.0f}s")
    return result.returncode == 0, elapsed


def phase_environment(args: argparse.Namespace) -> None:
    """Node counts for the chosen phase, unless the caller pinned them already."""
    counts = {"artifact": ("1", "0", "0"), "perf": ("3", "1", "1")}[args.phase]
    for name, value in zip(("SIM_DB_COUNT", "SIM_LOADER_COUNT", "SIM_MONITOR_COUNT"), counts, strict=True):
        os.environ.setdefault(name, value)


def print_summary(results: list[tuple[Step, bool, float]], args: argparse.Namespace) -> None:
    print()
    log("=" * 68)
    log(f"simulation summary ({cfg('SIM_TEST_TAG')}, phase {args.phase})")
    for step, ok, elapsed in results:
        log(f"  {'ok  ' if ok else 'FAIL'}  {step.title:<45} {elapsed:6.0f}s")

    runs_log = STATE_DIR / "runs.log"
    if runs_log.exists():
        log("")
        log("SCT runs (test ids):")
        for line in runs_log.read_text(encoding="utf-8").splitlines():
            log(f"  {line}")

    results_root = Path.home() / "sct-results"
    if results_root.exists():
        recent = sorted(results_root.glob("2*"), key=lambda p: p.stat().st_mtime)[-3:]
        if recent:
            log("")
            log("most recent result directories:")
            for path in recent:
                log(f"  {path}")

    log("")
    if not args.teardown:
        log("hosts are still running -- terminate with:")
        log("  uv run python scripts/baremetal-simulation/99_teardown.py --yes --all")
        log("  uv run python scripts/baremetal-simulation/03_patch_sct_for_fedora.py --revert")
    log("record on SCT-901: distro detected, RPM install outcome, scylla_setup + /var/lib/scylla mount,")
    log("install wall-clock, log collection, and the io.conf diff between passes (.state/host-state-*).")
    log("=" * 68)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("artifact", "perf"), default="artifact", help="which workload to run")
    parser.add_argument(
        "--reset-passes",
        type=int,
        default=1,
        metavar="N",
        help="extra artifact runs, each preceded by a host reset -- run-to-run variance on one host (default: 1)",
    )
    parser.add_argument(
        "--dirty-passes",
        type=int,
        default=0,
        metavar="N",
        help=(
            "extra artifact runs on the un-reset host (default: 0).  The drift experiment, and it is EXPECTED "
            "TO FAIL: a reused host skips RAID setup, never regenerates /etc/scylla.d/io.conf, and scylla then "
            "refuses to start"
        ),
    )
    parser.add_argument("--teardown", action="store_true", help="terminate the hosts and revert the patch at the end")
    parser.add_argument("--resume", action="store_true", help="skip the steps that already succeeded")
    parser.add_argument("--from-step", metavar="KEY", help="start at this step key (see --list)")
    parser.add_argument("--skip", action="append", default=[], metavar="KEY", help="skip a step key, repeatable")
    parser.add_argument("--list", action="store_true", help="print the steps and exit")
    parser.add_argument("--dry-run", action="store_true", help="print each command instead of running it")
    args = parser.parse_args()

    phase_environment(args)
    steps = build_steps(args)

    if args.list:
        for step in steps:
            print(f"{step.key:<12} {step.title}")
        return 0

    done = set(read_state("progress.json").get("completed", [])) if args.resume else set()
    if args.from_step:
        keys = [step.key for step in steps]
        if args.from_step not in keys:
            log(f"unknown step {args.from_step!r}; known: {', '.join(keys)}")
            return 2
        done |= set(keys[: keys.index(args.from_step)])

    log(f"phase={args.phase} region={cfg('SIM_REGION')} tag={cfg('SIM_TEST_TAG')}")
    log(
        f"nodes: db={os.environ['SIM_DB_COUNT']} loader={os.environ['SIM_LOADER_COUNT']} monitor={os.environ['SIM_MONITOR_COUNT']}"
    )
    if not args.teardown and not args.dry_run:
        log(f"hosts will be LEFT RUNNING (keep={cfg('SIM_KEEP_HOURS')}h); pass --teardown to clean up automatically")

    results: list[tuple[Step, bool, float]] = []
    completed: list[str] = sorted(done)
    failed: Step | None = None

    for step in steps:
        if step.key in done or step.key in args.skip:
            log(f"skipping {step.key} ({step.title})")
            continue
        ok, elapsed = run_step(step, args.dry_run)
        results.append((step, ok, elapsed))
        if ok:
            completed.append(step.key)
            if not args.dry_run:
                write_state("progress.json", {"phase": args.phase, "completed": completed})
            continue
        if step.advisory:
            log(f"{step.title} failed but is advisory -- continuing")
            continue
        failed = step
        break

    print_summary(results, args)
    if failed:
        log(f"stopped at '{failed.key}'. Fix it, then resume with:")
        log(f"  uv run python scripts/baremetal-simulation/run_simulation.py --phase {args.phase} --resume")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
