# Generating a Runner Script

An overlay says *what* to run. A runner script says *how*, in the right order, every time.
Generate one per overlay, **in `scripts/`** — not next to the overlay in
`configurations/minicloud/`. Every phase runs repo-root-relative paths (`uv run sct.py`,
`scripts/minicloud-firewalld-zone.sh`, the `-c` list), so the runner has to resolve the repo
root, and a script living one level under `configurations/` does not. The phase ordering below
is not obvious either, and getting it wrong costs either the logs you came for or real cloud
money.

For a flow anyone will repeat, add a flavor to `scripts/run-minicloud-test.sh` instead — it
already handles the config list, the endpoint and the firewalld zone.

---

## The rule that matters most

**Export `SCT_MINICLOUD_ENDPOINT_URL` for every `sct.py` invocation, and assert it before
provisioning.**

Each `sct.py` call is a separate process. `start-minicloud` sets the endpoint *inside its own
environment*; that does not reach `run-test`. Without the variable exported,
`is_minicloud_active()` returns false and boto3 provisions against **real AWS** — real
instances, real money — and the log looks almost identical to a local run.

The tells, if it has already happened:

| Symptom | Local run | Real cloud |
|---|---|---|
| DB node private IP | `10.164.x.x` in `eu-west-1` (region index shifted by 160) | `10.4.x.x` — the real range |
| Public IP | the private IP, echoed back | a routable address |
| Instance type honoured | no — 1 vCPU regardless | yes, the type the yaml asked for |

A runner that does not assert this is a runner that can bill you. Two independent checks, since
either alone can pass while the other fails:

```bash
assert_minicloud_is_the_target() {
    if ! curl -s --max-time 5 -o /dev/null -X POST "$SCT_MINICLOUD_ENDPOINT_URL" \
            -d "Action=DescribeVpcs&Version=2016-11-15"; then
        echo "REFUSING TO RUN: no minicloud at $SCT_MINICLOUD_ENDPOINT_URL." >&2
        exit 1
    fi
    uv run python -c "
import sys
from sdcm.utils.minicloud.endpoint import is_minicloud_active
sys.exit(0 if is_minicloud_active() else 1)
" || { echo 'REFUSING TO RUN: SCT would provision on REAL AWS.' >&2; exit 1; }
}
```

`DescribeVpcs` is the right probe: minicloud implements it locally, so a response proves the
emulator is answering rather than something else on the port.

---

## Phase order, and why each rule exists

| Phase | Rule |
|---|---|
| `check` | Resolve the config and print the budget. Boots nothing, costs nothing, catches most mistakes |
| `start` | `sct.py start-minicloud`, then `scripts/minicloud-firewalld-zone.sh` — the zone binding is runtime-only and `firewall-cmd --reload` clears it, so it is re-applied on every start |
| `test` | Assert the target first, then `sct.py run-test` |
| `collect` | **Before teardown, always.** Removing the container kills every guest with it, and `collect-logs` will not restart a dead one — that would destroy the logs it came for |
| `clean` | Last. `clean-resources` fails closed when the emulator is unreachable rather than cleaning against a fresh, empty one |

**Derive every phase's invocation from the pipeline, not from guesswork.** The Jenkins steps
are the working reference for exactly these commands, on exactly this no-runner branch:
`vars/runSctTest.groovy`, `vars/runCollectLogs.groovy`, `vars/runCleanupResource.groovy`. All
three export `SCT_CONFIG_FILES` before their command — **that**, not `-c`, is how the config
reaches `collect-logs` and `clean-resources`, which have no `-c` option at all.

One flag trap, and it fails *after* the test has passed — at the end of an hour, with the
guests still up and holding memory:

| Command | `--logdir` means |
|---|---|
| `collect-logs` | the **run** directory |
| `clean-resources` | the **parent** results directory — it reads `<logdir>/latest/test_id`, so passing the run dir fails with `test_id not found` |

Pass `--test-id` to both and the asymmetry stops mattering. It is also the unambiguous form:
`collect-logs` falls back to `<logdir>/test_id` when the option is absent, but being explicit
costs nothing and reads better in a script someone else will run.

```bash
phase_collect() {
    local logdir tid
    logdir=$(readlink -f ~/sct-results/latest)
    tid=$(cat "$logdir/test_id")
    export SCT_TEST_ID="$tid"
    uv run sct.py collect-logs --backend aws --test-id "$tid" --logdir "$logdir" || true
}
phase_clean() {
    local tid; tid=$(cat "$(readlink -f ~/sct-results/latest)/test_id")
    export SCT_TEST_ID="$tid"
    uv run sct.py clean-resources --post-behavior --test-id "$tid" -b aws
}
```

The artifacts that matter land on disk either way — `minicloud-inspect.json` and a
`minicloud-serial-<instance>.log` per guest, the only record of a node SCT could never reach.

Check every phase against `sct.py <command> --help` **and** the matching `vars/*.groovy` step
before the first real run. A runner that is wrong only in its last two phases looks correct
until it has already cost you the hour.

Every phase gets the **same config list**: `start-minicloud` sizes the container from the node
counts the test will provision, so a phase that saw a different list would have the memory gate
check a different test than the one that runs.

Make the phases individually runnable. Most iteration is re-running `test` against an emulator
that is already up; forcing a full teardown and restart each time wastes ten minutes a cycle.

---

## Skeleton

Everything in `<angle brackets>` is a placeholder to replace; everything else is working code.
Run the finished script through `bash -n` before its first real use — a runner that is wrong
only in its later phases looks fine until it has already cost you an hour.

```bash
#!/bin/bash
# One line saying which test this runs, which overlay shrinks it, and that it produces
# no performance data.
set -euo pipefail
# Resolve the repo root rather than counting directory levels, so the runner keeps working
# wherever it is placed and whatever depth it sits at.
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"

PHASE="all"
while getopts "p:h" opt; do
    case "$opt" in
        p) PHASE="$OPTARG" ;;
        h) awk '/^#/ {print; next} {exit}' "$0"; exit 0 ;;
        *) exit 5 ;;
    esac
done

CONFIGS=(
    <test-case>.yaml
    <any production overlays from the jenkinsfile>
    configurations/minicloud/<your-overlay>.yaml
    configurations/minicloud.yaml          # last: its mandatory values must win
)
CONFIG_ARGS=(); for c in "${CONFIGS[@]}"; do CONFIG_ARGS+=(-c "$c"); done
TEST="<module.Class.method>"

# The same list as JSON, exported for every phase - this is what reaches collect-logs and
# clean-resources, exactly as the three vars/*.groovy steps do it.
export SCT_CONFIG_FILES=$(printf '%s\n' "${CONFIGS[@]}" | python3 -c 'import json,sys; print(json.dumps([l.strip() for l in sys.stdin if l.strip()]))')

export SCT_MINICLOUD_ENDPOINT_URL="${SCT_MINICLOUD_ENDPOINT_URL:-http://localhost:5000}"
export SCT_AMI_ID_DB_SCYLLA="${SCT_AMI_ID_DB_SCYLLA:-<a cached, released, x86_64 AMI>}"
export SCT_REGION_NAME="${SCT_REGION_NAME:-eu-west-1}"
export SCT_USE_MGMT="${SCT_USE_MGMT:-false}"
export SCT_ENABLE_ARGUS="${SCT_ENABLE_ARGUS:-false}"

# ... assert_minicloud_is_the_target() from above ...

phase_check() {
    # SCT_CONFIG_FILES is already the JSON list the command wants.
    uv run sct.py conf -b aws "$SCT_CONFIG_FILES" >/dev/null
    echo "    config resolves and validates"
    # Every role, not just the db one - an unpinned loader resolves arm64.
    uv run sct.py get-db-arch -b aws "$SCT_CONFIG_FILES" 2>/dev/null | tail -1
    awk '/MemAvailable/ {printf "    MemAvailable: %.1f GiB\n", $2/1048576}' /proc/meminfo
    # Deliberately no budget arithmetic here: start-minicloud's preflight computes it from the
    # resolved config and fails with the exact numbers. Doing it again in bash means parsing
    # minicloud_lightweight_memory by hand, and "3G" / "4096MiB" are both valid values that
    # $(( )) cannot evaluate - while an unset default would abort the whole script under set -u.
}
phase_start()   { uv run sct.py start-minicloud -b aws "${CONFIG_ARGS[@]}"; scripts/minicloud-firewalld-zone.sh; }
phase_test()    { assert_minicloud_is_the_target; uv run sct.py run-test "$TEST" --backend aws "${CONFIG_ARGS[@]}"; }
# NOTE: no -c on these two - see the flag traps above.
phase_collect() {
    local logdir tid
    logdir=$(readlink -f ~/sct-results/latest); tid=$(cat "$logdir/test_id")
    export SCT_TEST_ID="$tid"
    uv run sct.py collect-logs --backend aws --test-id "$tid" --logdir "$logdir" || true
}
phase_clean() {
    local tid; tid=$(cat "$(readlink -f ~/sct-results/latest)/test_id")
    export SCT_TEST_ID="$tid"
    uv run sct.py clean-resources --post-behavior --test-id "$tid" -b aws
}

case "$PHASE" in
    check|start|test|collect|clean) "phase_$PHASE" ;;
    all)
        phase_check; phase_start
        # The test's exit code is the result, but collection and teardown must happen either
        # way - capture it rather than letting `set -e` skip them.
        rc=0; phase_test || rc=$?
        phase_collect; phase_clean
        exit "$rc" ;;
    *) echo "unknown phase: $PHASE" >&2; exit 5 ;;
esac
```

Pin the AMI by id rather than resolving from a version: a version can resolve to an uncached
image and spend half an hour rebuilding its disk from the EBS snapshot. `~/.cache/minicloud/amis`
lists what is already local.

---

## What the check phase should print

Enough to stop a bad run before it costs anything:

- that the config resolves (`sct.py conf` exits 0)
- the guest architecture (`sct.py get-db-arch`) — arm64 cannot boot under KVM on an x86_64 host
- the arithmetic: guests x per-guest memory + 2 GiB, against `MemAvailable`

The preflight gate does the memory arithmetic itself and fails with the numbers. Printing it in
`check` means you see it before spending anything, not after the container is up.
