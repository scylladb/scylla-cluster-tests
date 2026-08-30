#!/bin/bash
#
# Loader CPU/thread diagnostic sampler (SCT-601).
#
# Appends one timestamped block per sample to a log file on the loader host, so that a client-side
# latency spike can be attributed to (or cleared of) loader CPU contention afterwards. The log is
# streamed off the loader by LoaderCpuFileLogger and collected by LoaderLogCollector.
#
# One block per sample, every line tagged with its kind so the file stays greppable:
#
#   === <utc timestamp> sample=<n> uptime=<seconds>
#   cpu       <mpstat -P ALL>: TIME CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle
#   proc      <pidstat -u -h>: TIME UID PID %usr %system %guest %wait %CPU CPU Command
#   thread    <pidstat -tu -h>: TIME UID TGID TID %usr %system %guest %wait %CPU CPU Command
#   ctxt      PID <voluntary_ctxt_switches> <nonvoluntary_ctxt_switches> <threads>
#   pressure  /proc/pressure/cpu (PSI): some|full avg10=.. avg60=.. avg300=.. total=..
#   loadavg   /proc/loadavg: 1m 5m 15m running/total last_pid
#
# Without sysstat the same block is emitted from /proc only, tagged rawcpu/rawproc - cumulative
# counters instead of percentages, so the reader has to diff consecutive samples.
#
# All tools of one block share a single measurement window, so the numbers inside a block line up.

set -u

# Pin the sysstat output format. Under a 12h locale mpstat/pidstat print "03:20:26 PM  all ..." -
# one field more than "15:20:26     all ..." - which shifts every documented column by one and would
# make the per-thread sort pick the wrong metric. S_TIME_FORMAT=ISO overrides the locale.
export S_TIME_FORMAT=ISO
export LC_ALL=C

INTERVAL=1
OUT=/var/tmp/loader-cpu.log
# matched against the command name (`pidstat -C`, `pgrep` without -f), covers SCT's stress tools
PATTERN='java|cassandra-stress|scylla-bench|latte|ycsb|cql-stress'
THREAD_EVERY=0 # per-thread sampling every Nth sample; 0 disables it
TOP_THREADS=20 # keep only the N hottest threads - a c-s run has ~1000 of them
MAX_MB=2048    # stop sampling instead of filling up the loader disk
MAX_PROCS=30   # bound the per-process lines of a block, however wide the pattern matches

usage() {
    echo "usage: $0 [-i interval] [-o out_file] [-p comm_pattern] [-t thread_every_n_samples]" >&2
    echo "          [-T top_threads] [-m max_mb]" >&2
    exit 2
}

while getopts "i:o:p:t:T:m:" opt; do
    case "$opt" in
    i) INTERVAL=$OPTARG ;;
    o) OUT=$OPTARG ;;
    p) PATTERN=$OPTARG ;;
    t) THREAD_EVERY=$OPTARG ;;
    T) TOP_THREADS=$OPTARG ;;
    m) MAX_MB=$OPTARG ;;
    *) usage ;;
    esac
done

if command -v mpstat >/dev/null 2>&1 && command -v pidstat >/dev/null 2>&1; then
    HAVE_SYSSTAT=1
else
    HAVE_SYSSTAT=0
fi

TMPDIR=$(mktemp -d)
on_exit() {
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) sampler stopped" >>"$OUT"
    rm -rf "$TMPDIR"
}
trap on_exit EXIT
# a TERM/INT handler that does not exit would leave the loop running: bash resumes the script once
# the handler returns, and `systemctl stop` would have to SIGKILL the sampler
trap 'exit 0' TERM INT

# processes to sample, matched by name the way `pidstat -C` does it. `pgrep -f` must not be used
# here: the pattern is part of this script's own command line, so it would match the sampler itself
sampled_pids() {
    pgrep "$PATTERN" 2>/dev/null | head -n "$MAX_PROCS"
}

emit_header() {
    cat <<EOF
=== $(date -u +%Y-%m-%dT%H:%M:%SZ) sampler started pid=$$ interval=${INTERVAL}s \
thread_every=${THREAD_EVERY} top_threads=${TOP_THREADS} pattern='${PATTERN}' sysstat=${HAVE_SYSSTAT}
=== cpu: TIME CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle
=== proc/thread: TIME UID [TGID] PID/TID %usr %system %guest %wait %CPU CPU Command
=== ctxt: PID voluntary_ctxt_switches nonvoluntary_ctxt_switches threads
EOF
}

# tagged sysstat output: the banner, the trailing Average block and the column headers are dropped,
# so only data lines reach the log. Headers are recognized by '%usr', which every one of them carries
# and no data line does - unlike a field position, that holds for any locale and sysstat version.
tag_sysstat() {
    local tag=$1 file=$2
    awk -v tag="$tag" '
        NF && $1 != "Average:" && $0 !~ /^Linux/ && $0 !~ /%usr/ {print tag, $0}
    ' "$file"
}

# The %CPU field of `pidstat -tu -h` data lines, as the sort key of an already tagged line. Read from
# the header instead of hardcoded: %wait only exists since sysstat 11.5, and a data line has one
# field less than the header ('# Time' vs a single timestamp), which the prepended tag adds back.
thread_cpu_sort_key() {
    local file=$1 key
    key=$(awk '/%CPU/ {for (i = 1; i <= NF; i++) if ($i == "%CPU") {print i; exit}}' "$file")
    echo "${key:-10}"
}

sample_sysstat() {
    local with_threads=$1
    : >"$TMPDIR/mpstat"
    : >"$TMPDIR/pidstat"
    : >"$TMPDIR/pidstat_t"
    # one shared measurement window for every tool of this block
    mpstat -P ALL "$INTERVAL" 1 >"$TMPDIR/mpstat" 2>/dev/null &
    pidstat -u -h -C "$PATTERN" "$INTERVAL" 1 >"$TMPDIR/pidstat" 2>/dev/null &
    if [[ $with_threads -eq 1 ]]; then
        pidstat -tu -h -C "$PATTERN" "$INTERVAL" 1 >"$TMPDIR/pidstat_t" 2>/dev/null &
    fi
    wait

    tag_sysstat cpu "$TMPDIR/mpstat"
    tag_sysstat proc "$TMPDIR/pidstat"
    if [[ $with_threads -eq 1 ]]; then
        tag_sysstat thread "$TMPDIR/pidstat_t" |
            sort -k"$(thread_cpu_sort_key "$TMPDIR/pidstat_t")" -nr | head -n "$TOP_THREADS"
    fi
}

sample_proc() {
    sed 's/^/rawcpu /' /proc/stat | grep '^rawcpu cpu'
    for pid in $(sampled_pids); do
        # utime stime cutime cstime num_threads, all cumulative - diff two samples to get a rate
        awk '{print "rawproc", $1, $2, $14, $15, $16, $17, $20}' "/proc/$pid/stat" 2>/dev/null
    done
    sleep "$INTERVAL"
}

# context switches and thread count, cheap enough to take on every sample
sample_ctxt() {
    for pid in $(sampled_pids); do
        awk -v pid="$pid" '
            /^voluntary_ctxt_switches/ {vol=$2}
            /^nonvoluntary_ctxt_switches/ {nonvol=$2}
            /^Threads/ {threads=$2}
            END {if (vol != "") print "ctxt", pid, vol, nonvol, threads}
        ' "/proc/$pid/status" 2>/dev/null
    done
}

sample_pressure() {
    if [[ -r /proc/pressure/cpu ]]; then
        sed 's/^/pressure /' /proc/pressure/cpu
    fi
    echo "loadavg $(cat /proc/loadavg)"
}

emit_header >>"$OUT"

sample=0
while :; do
    sample=$((sample + 1))
    with_threads=0
    if [[ $THREAD_EVERY -gt 0 ]] && ((sample % THREAD_EVERY == 0)); then
        with_threads=1
    fi

    {
        echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) sample=$sample uptime=$(cut -d' ' -f1 /proc/uptime)"
        if [[ $HAVE_SYSSTAT -eq 1 ]]; then
            sample_sysstat "$with_threads"
        else
            sample_proc
        fi
        sample_ctxt
        sample_pressure
    } >>"$OUT" 2>/dev/null

    # checked once a minute, the size call is not worth doing every second
    if ((sample % 60 == 0)); then
        size_mb=$(($(stat -c%s "$OUT" 2>/dev/null || echo 0) / 1024 / 1024))
        if ((size_mb >= MAX_MB)); then
            echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) size cap reached (${size_mb}MB >= ${MAX_MB}MB), stopping" >>"$OUT"
            exit 0
        fi
    fi
done
