# SC sanity longevity: nemesis coverage

Local mirror of the run statistics for `sanity-longevity-100gb-4h-sc-test`, the job defined by
[`test-cases/longevity/longevity-100gb-4h-cql-stress-sc.yaml`](../test-cases/longevity/longevity-100gb-4h-cql-stress-sc.yaml).
The per-nemesis verdicts and their reasoning live on the wiki page
[Nemesis coverage: SC sanity longevity](https://scylladb.atlassian.net/wiki/spaces/RND/pages/473661450);
this file keeps the numbers next to the config they come from, so a reader of the repository can
see where the sweep stands without leaving it.

The job runs `CategorySweepMonkey`, which executes every nemesis once, in category order, and
stops. Each run resumes where the previous one stopped, through
[`configurations/nemesis/sweep_resume_after_run7.yaml`](../configurations/nemesis/sweep_resume_after_run7.yaml).

## Where the sweep stands

112 nemesis are discovered in total; 13 of them are `topology-changes`, held out of the sweep by
`DISABLED_CATEGORIES` while SCYLLADB-4529 is open, so 99 are in scope.

| Status | Count | Share of all 112 |
|---|---:|---:|
| pass | 19 | 17% |
| failed | 8 | 7% |
| blocked (topology-changes) | 13 | 12% |
| **failed + blocked** | **21** | **19%** |
| skipped | 25 | 22% |
| not executed | 47 | 42% |

## Per-run statistics

Counts are the Argus nemesis statuses for the run - did the disruption itself complete - not the
coverage verdict on the wiki, which asks whether a nemesis exposed a strong-consistency problem.
A nemesis can be "succeeded" here and still be the one that failed the run.

| Run | When (UTC) | Reached | succeeded | failed | skipped | Outcome |
|---|---|---:|---:|---:|---:|---|
| [#7](https://argus.scylladb.com/tests/scylla-cluster-tests/16968b65-d957-4871-9452-03824a05d30c) | 2026-09-17 13:26–20:36 | 22 | 16 | 3 | 3 | aborted - node-1 crash loop on audit restart (SCYLLADB-4625) |
| [#9](https://argus.scylladb.com/tests/scylla-cluster-tests/de61fd2c-b4a2-4edc-8a50-3a553e085996) | 2026-09-18 15:25–15:40 | 6 | 1 | 0 | 5 | failed - SC writes unavailable during decommission (SCYLLADB-4604) |
| [#10](https://argus.scylladb.com/tests/scylla-cluster-tests/64b2f4b9-dcf7-456d-ad1c-6a7498955e35) | 2026-09-21 08:42–09:22 | 6 | 2 | 1 | 3 | failed - write stress died during `CorruptThenRepairMonkey` |
| [#13](https://argus.scylladb.com/tests/scylla-cluster-tests/47fedaef-92b9-49c6-9a62-63b3b0267cdc) | 2026-09-22 09:45–11:06 | 9 | 0 | 1 | 8 | failed after 14 min - `disablebinary` for 1.17 s killed the write stress (SCYLLADB-4671) |
| [#14](https://argus.scylladb.com/tests/scylla-cluster-tests/f57403f7-15ec-4333-9922-e99704c90fe0) | 2026-09-22 13:13–14:15 | 5 | 1 | 1 | 3 | failed - `ReadValidationError` on rows an earlier nemesis prevented prepare from writing |
| [#15](https://argus.scylladb.com/tests/scylla-cluster-tests/3dd4dabf-fc6d-4012-9662-2bb9aaae85a0) | 2026-09-23 14:39–18:53 | 7 | 0 | 3 | 4 | aborted - node-1 crash loop after `HardRebootNodeMonkey` (SCYLLADB-4625) |

Run #13 reached nine nemesis but Argus recorded two: the eight precheck exclusions are submitted
with one shared timestamp, and only the first survives. The SCT log has all of them.

## What the runs changed in the job

| Commit | Change | Why |
|---|---|---|
| `540600481` | `CategorySweepMonkey` | run every nemesis once, in category order, instead of a shuffled infinite cycle |
| `3dfcf31c4` | `DISABLED_CATEGORIES = {topology-changes}` | hold 13 nemesis out while SCYLLADB-4529 is open |
| `fd3f4cbdc` | `-errors ignore` on `stress_cmd` | cql-stress fail-fast ended run #13 on the first nemesis that closed a CQL port |
| `b179e9800` | fail-fast kept on `prepare_write_cmd`, `nemesis_during_prepare: false` | ignoring errors during prepare left ~103k rows unwritten in run #14, which the read phase then reported as validation failures |
| `087be6312` | `EnableDisableTableEncryptionAwsKmsProviderWithRotationMonkey` put back in the sweep | it never got a verdict in run #14 - the rows it was blamed for were missing before it started |
| `e3db976cb` | the seven nemesis run #15 reached excluded | both KMS nemesis write at CL=ALL, which SC tables reject; `HardRebootNodeMonkey` hits SCYLLADB-4625; the four skips are static for this job |

## Open issues found by this job

| Issue | What it is |
|---|---|
| [SCYLLADB-4625](https://scylladb.atlassian.net/browse/SCYLLADB-4625) | node cannot restart after enabling audit - permanent crash loop; run #15 hit it after a hard reboot, without audit |
| [SCYLLADB-4604](https://scylladb.atlassian.net/browse/SCYLLADB-4604) | SC: decommission blocks writes - migrated tablet raft groups never elect a leader |
| [SCYLLADB-4529](https://scylladb.atlassian.net/browse/SCYLLADB-4529) | node bootstrap fails on group0 snapshot - blocks the topology-changes category |
| [SCYLLADB-4671](https://scylladb.atlassian.net/browse/SCYLLADB-4671) | SC: timeouts reported as CL=ONE/SIMPLE, so drivers never retry them |
| [SCYLLADB-4700](https://scylladb.atlassian.net/browse/SCYLLADB-4700) | SC: writes fail without replica failover while the Raft leader's CQL port is down |
