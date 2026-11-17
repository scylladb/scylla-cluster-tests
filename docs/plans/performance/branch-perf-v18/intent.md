# branch-perf-v18 and the move from cassandra-stress to latte

**Jira:** SCT-1029

## Problem

The scheduled performance jobs run from `branch-perf-v17`. That branch is
old and drifts further from master each week:

- It is 2,873 commits behind master. Each fix for perf runs must be
  backported to it, and the backports conflict more and more.
- Its integration CI stage cannot pass. The suite runs without
  `pytest-xdist` and hits the 45-minute timeout, so a backport PR gets no
  integration signal.
- 16 scheduled jobs use cassandra-stress (c-s). SCT invests in latte, which
  has rate control per step, threads and concurrency, and rune workloads.
  The latte throughput jobs that exist on v17 have never run in production.

The perf baselines cannot move to latte without a new baseline lineage.
Latte numbers do not compare with c-s numbers, so a test that switches tool
in place mixes two baselines in one Argus history.

## Who it affects

- The perf team that builds on the shared branch: aleksbykov, mikliapko,
  roydahan, juliayakovlev.
- The jobs under `jenkins-pipelines/performance/branch-perf-v17/` and their
  schedule in `configurations/triggers/perf-regression.yaml`.
- The weekly perf status report and the release comparisons in Argus.

## Evidence

Branch distance, measured on 2026-11-17:

```
$ git rev-list --count upstream/branch-perf-v17..upstream/master
2873
$ git rev-list --count upstream/master..upstream/branch-perf-v17
673
```

Integration CI on perf-v17 backport PRs (PR-15680, PR-15682), 2026-08-04:
the `integration tests` stage hits its 45-minute timeout and the build ends
`ABORTED` with `No test report files were found. Configuration error?`.
The suite reached 65 % at the timeout.

Argus has no runs for the production latte throughput tests
`latte-perf-regression-predefined-throughput-steps-tablets`
(`e8678dc0-b294-4589-a790-3a3e63c0ea44`) and
`latte-perf-regression-predefined-throughput-steps-vnodes`
(`d7de54ea-26fd-4cc5-ba41-69fe2cf11b8f`). Their staging copy
`scylla-staging/fruch/i8g_latte/latte-perf-regression-predefined-throughput-steps-tablets-test`
passed three times on 2026-04-13 and 2026-04-14.

## Definition of done

- `branch-perf-v18` exists, is cut from master, and its unit and
  integration CI stages pass.
- Each scheduled c-s job has a latte twin on v18, and the trigger runs the
  twin. A twin runs on the schedule only after it meets the switch gate in
  `spec.md` against its c-s job on the same Scylla build.
- Each latte twin has at least one run on the newest GA release that its
  c-s job covers, so release comparisons have a latte baseline.
- The c-s history in Argus stays readable. No latte run lands in a c-s
  test history.
- The jobs without c-s run from v18 under their current names.
- No scheduled job runs from v17. The weekly job creator no longer builds
  v17 jobs. The perf team has the announcement.

## Out of scope

- A change of workload, data size, instance types, or cadence. Each latte
  twin measures what its c-s job measures.
- The c-s jobs without a schedule (driver jobs, Manager backup, z4d, Azure,
  Scylla Cloud, compression features). Their owners decide to switch,
  move, or drop each one.
- Alternator on latte (SCT-534).
- Changes to latte itself.
