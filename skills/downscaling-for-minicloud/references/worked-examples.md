# Worked Examples

Two overlays produced by following this skill, shown as **output to recognise, not files to
copy**. The repository ships no ready-made downscales beyond the three below; producing the one
your test needs is the point.

Three real overlays already live in `configurations/minicloud/` and are worth reading in full:
`rolling-upgrade.yaml`, `scale-cluster.yaml`, `generic-rolling-upgrade.yaml`.

---

## A longevity test — volume in the command string

Base: `test-cases/longevity/longevity-100gb-4h.yaml`, the most reused longevity config in the
repo. 6 db + 2 loaders, a 20M-row preload and 4 hours of parallel write/read.

Budget: 3 db + 1 loader + 1 monitor = 5 guests x 4GiB = 20GiB, plus ~2GiB headroom.

```yaml
n_db_nodes: 3       # 3 keeps quorum with one node down; below that a nemesis breaks the cluster
n_loaders: 1        # one loader saturates a 1-vCPU cluster on its own
n_monitor_nodes: 1  # SisyphusMonkey's non-disruptive set reads Prometheus

sizing_db:          # base asks 8 vCPU / 64GiB; lightweight mode ignores it either way
  vcpu: 2
  memory: '>=16'
  arch: x86_64
gce_n_local_ssd_disk_db: 1
# sizing_db resolves the db role into the emulator catalog, but the loader falls through to the
# gce default n4a-highcpu-4, which minicloud does not serve. Pin what the base leaves unpinned.
gce_instance_type_loader: 'n2-standard-2'
gce_instance_type_monitor: 'n2-standard-2'

test_duration: 60

# Production commands with row counts, threads and durations cut. Unchanged: the consistency
# level, replication_factor=3, the -col shape, and the two-command list - two commands means
# two parallel streams, and collapsing them changes the test.
prepare_write_cmd: "cassandra-stress write cl=QUORUM n=200000 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=10 -pop seq=1..200000 -col 'n=FIXED(10) size=FIXED(512)' -log interval=5"
stress_cmd: ["cassandra-stress write cl=QUORUM duration=20m ... -rate threads=10 -pop seq=1..200000 ...",
             "cassandra-stress read  cl=QUORUM duration=20m ... -rate threads=10 -pop seq=1..200000 ..."
             ]

nemesis_selector: "not disruptive"
nemesis_interval: 2
```

`n=` and `-pop seq=1..N` moved together — a `-pop` range wider than `n=` changes the access
pattern rather than the size.

---

## A performance test — volume in config dicts

Base: `test-cases/performance/latte-perf-regression-predefined-throughput-steps-tablets.yaml`,
driven by `PerformanceRegressionPredefinedStepsTest`. 4 loaders, 20M-row read tables, five
30-minute steps per sub-test reaching 1.1M ops/s.

Downscaled for the **read** sub-test only. `stress_cmd` is never read here; the load lives in
the step dicts.

```yaml
n_loaders: 1   # with round_robin, each stress list holds one command per loader - so one each

# minicloud has no arm64 guests: they run under KVM, not emulation. The base pins i8g.4xlarge.
# SCT derives the architecture from the instance type and resolves the image to match, so this
# one key is the whole override - confirm with `sct.py get-db-arch`.
instance_type_db: 'i4i.4xlarge'

test_duration: 60
prepare_stress_duration: 15   # the preload timeout in minutes; base allows 300 for 650M rows

# 50k rows instead of 20M. --start-cycle / --end-cycle / row_count / offset must stay
# consistent with each other, or the read workload addresses rows the preload never wrote.
prepare_write_cmd:
  - >-
    latte run --function write --consistency ALL --duration 50000
    --threads 4 --concurrency 8 --rate 2000
    --start-cycle 1 --end-cycle 50000
    -P row_count=50000 -P offset=0
    -P column_count=1 -P column_size=1024
    data_dir/latte/latte_cs_alike.rn

# Two steps instead of five, so the step transition and the throttle-to-unthrottled switch both
# get exercised. Production asks 300K-1.1M ops/s at 1760 parallelism; 4 threads x 8 concurrency
# is 32, which is what a 1-vCPU loader can drive. This is a dict merge, so the write, mixed and
# read_disk_only steps keep their production values - unused here, left alone deliberately.
perf_gradual_throttle_steps:
  read:
    - {threads: 4, concurrency: 8, rate: '10000'}
    - {threads: 4, concurrency: 8, rate: 'unthrottled'}

perf_gradual_step_duration:
  read: '5m'
```

Two things about this one are not in the yaml:

- **Drop `latency-decorator-error-thresholds-*.yaml` from the config list.** It asserts on
  measured latency, which on a 1-vCPU guest is blown by orders of magnitude. A red run there
  tells you only that the guest is small.
- **Run one `sub_tests` entry at a time.** The jenkinsfile lists four; each is a separate job.

And say it plainly in the overlay header: a run like this exercises the gradual-steps machinery
end to end — schema, preload, warm-up, step transitions, Prometheus queries, result collection
— and produces no performance data.
