# RCA: SMI-324 - AWS I/O presets counted the disks twice

| | |
|---|---|
| Ticket | [SMI-324](https://scylladb.atlassian.net/browse/SMI-324) |
| Found by | Customer investigation [CUSTOMER-776](https://scylladb.atlassian.net/browse/CUSTOMER-776) |
| Fix | [scylla-machine-image#1033](https://github.com/scylladb/scylla-machine-image/pull/1033) (merged to `next` on 2026-10-12) |
| Related | [SMI-297](https://scylladb.atlassian.net/browse/SMI-297), [SMI-223](https://scylladb.atlassian.net/browse/SMI-223), [SMI-218](https://scylladb.atlassian.net/browse/SMI-218), [QAINFRA-51](https://scylladb.atlassian.net/browse/QAINFRA-51) |
| Status | Draft for review |

## Summary

The multi-disk sizes of i7i, i7ie, i8g and i8ge got I/O limits that are too high.
`AwsIoSetup.generate` multiplies each `aws_io_params.yaml` entry by the local disk count.
The entries for these four families are whole-instance values, so first boot counts the disks twice.
An i8ge.12xlarge node gets 48.8 GB/s read bandwidth. The correct value is 12.2 GB/s.
Seastar does not throttle, and the device queue becomes the only limit.

The SCT AMI artifacts test showed the error in release runs for almost one year.
From 2025-10-23, 350 release runs on affected sizes reported a deviation of 50% to 83% between the installed and the measured values.
The test sends this deviation only as a `WARNING` cell in an Argus result table.
The runs passed, and nobody looked at the tables.

## Impact

- Affected sizes:
  - i7i and i8g: 8xlarge, 12xlarge, 16xlarge, 24xlarge.
  - i7ie and i8ge: 2xlarge, 6xlarge, 12xlarge, 18xlarge, 24xlarge.
- Single-disk sizes are correct. Sizes without a preset (48xlarge, metal) run iotune at first boot and are correct.
- Measurements from the SMI-324 description: on a 33-node i8ge.12xlarge cluster, the scheduler reported 34% to 37% consumption.
  The nodes ran at 137% to 147% of the capacity that the yaml states for the array.
  Six nodes queued 100 to 240 requests per drive at 10 ms per write.
- The installed file stays on a node after an image upgrade. Only a manual change repairs it ([SMI-218](https://scylladb.atlassian.net/browse/SMI-218)).

Releases that ship the wrong entries:

| Branch | First release with the entries | Overstatement | Fix status |
|---|---|---|---|
| 2025.1 | 2025.1.12 (backports [#921](https://github.com/scylladb/scylla-machine-image/pull/921), [#922](https://github.com/scylladb/scylla-machine-image/pull/922), CUSTOMER-242) | disk count squared, see below | no backport |
| 2025.2 | not affected | - | - |
| 2025.3 | 2025.3.3 (2025-10-30) | disk count | no backport |
| 2025.4 | 2025.4.0-rc3 (2025-10-23) | disk count | no backport |
| 2026.1, 2026.2, 2026.3 | 2026.1.0-rc0 and all later | disk count | 2026.3 backport [#1034](https://github.com/scylladb/scylla-machine-image/pull/1034) is on hold for performance tests |

On `branch-2025.1` every multi-disk entry of the four families is the `next` entry times the disk count.
`generate()` on that branch still multiplies by the disk count.
Example: the 2025.1 i8ge.12xlarge entry is 48,761,999,360, so first boot writes 195 GB/s for a 12.2 GB/s array.
The 2025.1 i7ie.2xlarge Argus runs confirm this: the installed value is 3.5 times the measured value.

## Timeline

| Date | Event |
|---|---|
| 2022-01-20 | [#289](https://github.com/scylladb/scylla-machine-image/pull/289) (`4bc0664`) moves the AWS preset code from the scylla repository. Multi-disk presets use `value * nr_disks`. Entries are per disk. |
| 2022 | In [#349](https://github.com/scylladb/scylla-machine-image/pull/349) (i4i), Avi Kivity finds the same double count in review: "you doubled both the constant and nr_disks" ([comment](https://github.com/scylladb/scylla-machine-image/pull/349#discussion_r867455896)). |
| 2024-12-09 | [#562](https://github.com/scylladb/scylla-machine-image/pull/562) (`828c2a2`) moves the presets to `aws_io_params.yaml` and multiplies every entry by `nr_disks`. The yaml has no comment about the unit. |
| 2024-12 to 2025-07 | [#561](https://github.com/scylladb/scylla-machine-image/pull/561) (i7ie), [#676](https://github.com/scylladb/scylla-machine-image/pull/676) (i8g, closed) and [#709](https://github.com/scylladb/scylla-machine-image/pull/709) (i7i) use single-disk measurements. The review asks for RAID0 measurement. [#709](https://github.com/scylladb/scylla-machine-image/pull/709) merges on 2025-07-01 with per-disk values. See [The i7i review discussion](#the-i7i-review-discussion). |
| 2025-01-27 | scylla-pkg `a5872a75` adds i7ie.large to i7ie.3xlarge to the release AMI test list. |
| 2025-08-03 | Staging runs `scylla-staging/roy/artifacts-ami-test-i7i` on all i7i sizes show a bandwidth ratio of 1.0 between installed and measured values: [8xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/1f64eb95-31bf-4ebc-bb46-3cb23cec4816), [12xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/a9178d17-8689-4451-9d23-9243d9418a39), [16xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/21e86125-af99-45b2-aa8c-a8f061c61146), [24xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/ca035f96-36cf-4571-8f0d-762c3794aac4). The per-disk entries are correct. |
| 2025-10-21 | [#776](https://github.com/scylladb/scylla-machine-image/pull/776) (`4007fac` i7i, `82cc0b7` i7ie) merges whole-instance entries. |
| 2025-10-22 | `b8ef6fa` (i8g, i8ge, content of [#796](https://github.com/scylladb/scylla-machine-image/pull/796), SMI-223) is pushed to `next` directly ([comment](https://github.com/scylladb/scylla-machine-image/pull/796#issuecomment-3430430881)). GitHub shows [#796](https://github.com/scylladb/scylla-machine-image/pull/796) as merged with a different commit. Backports to 2025.4 ([#811](https://github.com/scylladb/scylla-machine-image/pull/811), [#813](https://github.com/scylladb/scylla-machine-image/pull/813)) and 2025.3 ([#814](https://github.com/scylladb/scylla-machine-image/pull/814), [#815](https://github.com/scylladb/scylla-machine-image/pull/815), [#819](https://github.com/scylladb/scylla-machine-image/pull/819)) follow. |
| 2025-10-23 | First `WARNING` on an affected size: 2025.4.0~rc3, i7ie.2xlarge, deviation 50% ([run](https://argus.scylladb.com/tests/scylla-cluster-tests/40aa8381-7ed5-4e67-9e06-733823971583)). |
| 2025-10-27, 2025-10-29 | scylla-pkg `040c33d4`, `ac1d2c38` add i8g and i8ge sizes up to 16xlarge and 18xlarge to the release ARM list. |
| 2026-01-25 | 2026.1.0~rc0 ARM runs report `WARNING` on [i8g.8xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/2bbb17a5-2dc6-42da-be08-3224e4d142e8) (50%), [i8g.16xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/4087d5ec-1990-4efc-a6fd-1317d52fcae0) (75%) and [i8ge.2xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/5a85dd4b-0685-46c1-8486-41a9b2b2d1ac) (50%). |
| 2026-01-28 | QAINFRA-51 runs [i8g.48xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/1a937989-2f6e-4ff8-94fb-3c247bc51bf4) and [i8g.metal-24xl](https://argus.scylladb.com/tests/scylla-cluster-tests/792ab23c-ea2e-4abd-a8eb-98ca689e73eb). Both sizes have no preset, and both pass with 0% to 5% deviation. |
| 2026-03-09 | 2026.1.0 run on i8ge.12xlarge shows the production values: installed 48,761,999,360, measured 12,151,646,208, deviation 75% ([run](https://argus.scylladb.com/tests/scylla-cluster-tests/a2e427d2-e3c5-49e0-892c-b3d2ebfdad78)). |
| 2026-03-30 | First 2025.1.12 run on i7ie.2xlarge: ratio 3.5, deviation 71% ([run](https://argus.scylladb.com/tests/scylla-cluster-tests/ae3efa7d-c4db-4bbd-a420-e4243405d2af)). |
| 2026-05-21 | Staging run on i8g.24xlarge: ratio 6.1, deviation 84% ([run](https://argus.scylladb.com/tests/scylla-cluster-tests/5a8ec1c6-9aba-4b8d-9514-4a83890d9fdd)). |
| 2026-06-16 to 2026-09-28 | [SMI-297](https://scylladb.atlassian.net/browse/SMI-297) compares the yaml entries to the AWS whole-instance IOPS and reads them as whole-instance values. See [Near miss: SMI-297](#near-miss-smi-297). |
| 2026-10-08 | SMI-324 is filed from CUSTOMER-776. |
| 2026-10-12 | [#1033](https://github.com/scylladb/scylla-machine-image/pull/1033) merges to `next`. |

## Root cause

### Mechanism

`AwsIoSetup.generate` (`common/scylla_cloud_io_setup`) sets each value to `entry * number of local disks`.
It writes the result to `/etc/scylla.d/io_properties.yaml`. Scylla uses this file as is.
The code assumes per-disk entries. Nothing in the yaml or the code states this.

### How whole-instance entries got into the yaml

[#776](https://github.com/scylladb/scylla-machine-image/pull/776) and [#796](https://github.com/scylladb/scylla-machine-image/pull/796) took their values from `/etc/scylla.d/io_properties.yaml` read back from running Scylla AMIs.
That file describes the whole RAID0 array, so the values are whole-instance totals.
In 2025-10 the measuring tool `tools/io_properties_measurements/get_scylla_io_properties.py` ([#812](https://github.com/scylladb/scylla-machine-image/pull/812)) automated this read-back.
It stores `disks[0]` and does not divide by the disk count.

Intermediate revisions of both PRs show the double count in the data:

| PR revision | Content |
|---|---|
| [#776](https://github.com/scylladb/scylla-machine-image/pull/776) `072a1e4` (2025-08-18) | Each multi-disk i7ie value is `i7ie.ALL * disks` exactly. These are presets read back, not iotune results. |
| [#776](https://github.com/scylladb/scylla-machine-image/pull/776) `b970f98` (2025-10-17) | `072a1e4 * disks` exactly for 60 of 64 values. |
| [#796](https://github.com/scylladb/scylla-machine-image/pull/796) `536be8d` (2025-09-15) | Whole-array RAID0 values. |
| [#796](https://github.com/scylladb/scylla-machine-image/pull/796) `7bcde6a` (2025-10-16) | `536be8d * disks` exactly for all 64 values. This is the "Measured" table in the `b8ef6fa` commit message. |
| Final, both PRs (2025-10-21) | Whole-array values. |

The `7bcde6a` values grow with the square of the disk count.
The measured AMI already had whole-array entries, `generate()` multiplied them, and the tool read the result back.
The arithmetic is exact. The AMI that the tool measured is not confirmed.
The final revisions removed one factor of the disk count. The second factor stayed.

## The i7i review discussion

This section answers the review question about the earlier i7i discussion.

In [#561](https://github.com/scylladb/scylla-machine-image/pull/561), [#676](https://github.com/scylladb/scylla-machine-image/pull/676) and [#709](https://github.com/scylladb/scylla-machine-image/pull/709) the authors measured one SSD without RAID0.
The author of [#561](https://github.com/scylladb/scylla-machine-image/pull/561) wrote: "Measured single local SSD w/o RAID0, since we simulate RAID0 performance on scylla_cloud_io_setup script from single drive performance" ([comment](https://github.com/scylladb/scylla-machine-image/pull/561#issuecomment-2532682535)).
The review asked for RAID0 measurement:

- [#709](https://github.com/scylladb/scylla-machine-image/pull/709), on `i7i.ALL`: "This is incorrect since it's not measuring all disks (raid0) but single disk." ([comment](https://github.com/scylladb/scylla-machine-image/pull/709#discussion_r2123448207))
- [#561](https://github.com/scylladb/scylla-machine-image/pull/561): "Are these single disk numbers and we multiply it somewhere by the number of disks?" ([comment](https://github.com/scylladb/scylla-machine-image/pull/561#discussion_r2123496724)). This question has no reply.
- [#676](https://github.com/scylladb/scylla-machine-image/pull/676): "it makes sense to me to measure the raid0 drive or to measure a single disk and multiply by number of disks but the former should be more accurate" ([comment](https://github.com/scylladb/scylla-machine-image/pull/676#issuecomment-2918851725)).

[#709](https://github.com/scylladb/scylla-machine-image/pull/709) merged on 2025-07-01 with per-disk values "as a workaround" ([comment](https://github.com/scylladb/scylla-machine-image/pull/709#issuecomment-3018878165)), with a follow-up to fix the values.
The follow-ups [#776](https://github.com/scylladb/scylla-machine-image/pull/776) and [#796](https://github.com/scylladb/scylla-machine-image/pull/796) measured the RAID0 array.

What changed: the source of the values changed from one disk to the whole array.
The multiplication in `generate()` stayed. No comment in [#776](https://github.com/scylladb/scylla-machine-image/pull/776) or [#796](https://github.com/scylladb/scylla-machine-image/pull/796) mentions the disk count or the multiplication.

The review saw the symptom.
On [#776](https://github.com/scylladb/scylla-machine-image/pull/776): "in i7i, the 8xlarge is 4 times better than the 4x" ([comment](https://github.com/scylladb/scylla-machine-image/pull/776#issuecomment-3419680224)) and "around 2x more for some reason" ([comment](https://github.com/scylladb/scylla-machine-image/pull/776#issuecomment-3424715277)).
The author fixed the measuring script, which removed one factor ([comment](https://github.com/scylladb/scylla-machine-image/pull/776#issuecomment-3425932014)).
The final check of [#776](https://github.com/scylladb/scylla-machine-image/pull/776) compared the entries to the AWS whole-instance figures ([comment](https://github.com/scylladb/scylla-machine-image/pull/776#issuecomment-3427367058)). The [#796](https://github.com/scylladb/scylla-machine-image/pull/796) review asked for the same comparison ([comment](https://github.com/scylladb/scylla-machine-image/pull/796#issuecomment-3394151978)).
This check passes only when the entries are whole-instance values, so it confirmed the defect.

## Release AMI test coverage

This section answers the review question about the release AMI tests.

### What runs

The release AMI artifacts job takes its instance types from `scripts/jenkins-pipelines/instance_types.yaml` in scylla-pkg.
It starts one SCT `artifacts-ami-test` run for each type.
By default, master and `releng-testing` run only the tier1 and tier2 master types: i4i.2xlarge and i4i.xlarge on x86, im4gn.xlarge and i4g.2xlarge on ARM.

The release tier2 list has these i7 and i8 types:

| Family | Sizes in the release list | Affected sizes in the list | Affected sizes not in the list |
|---|---|---|---|
| i7i | none | none | 8xlarge, 12xlarge, 16xlarge, 24xlarge |
| i7ie (x86) | large, xlarge, 2xlarge, 3xlarge | 2xlarge | 6xlarge, 12xlarge, 18xlarge, 24xlarge |
| i8g (ARM) | large to 16xlarge | 8xlarge, 16xlarge | 12xlarge, 24xlarge |
| i8ge (ARM) | large to 18xlarge | 2xlarge, 6xlarge, 12xlarge, 18xlarge | 24xlarge |

### What the test checks

The `check Scylla IO Params` subtest (`artifacts_test.py`, `sdcm/utils/validators/iotune.py`) does these steps:

1. It reads the installed `/etc/scylla.d/io_properties.yaml`.
2. It runs `scylla_io_setup`, which always runs iotune on the array.
3. It sends the installed and measured values and the deviation to Argus.

A deviation of 15% or more gets `Status.WARNING` in the Argus table (`sdcm/argus_results.py`).
The test raises no event, and the run status does not change.

When an entry is too high by the disk count `d`, the deviation is `(d - 1) / d`.
This gives 50% for 2 disks, 75% for 4 disks and 83% for 6 disks.

### What Argus shows

Data source: all 7,751 runs of the 131 `artifacts-ami*` tests since 2025-07-01.
The result tables come from the 1,126 runs on i7i, i7ie, i8g and i8ge.

Release runs with a `WARNING` on an affected size, before 2026-10-08:

| Branch | Sizes | Runs | First | Last | Installed / measured |
|---|---|---|---|---|---|
| 2025.1 | i7ie.2xlarge | 6 | 2026-03-30 | 2026-09-25 | 3.5 |
| 2025.3 | i7ie.2xlarge | 8 | 2025-10-30 | 2026-02-23 | 2.0 |
| 2025.4 | i7ie.2xlarge | 20 | 2025-10-23 | 2026-06-11 | 2.0 |
| 2026.1 | i7ie.2xlarge, i8g.8xlarge, i8g.16xlarge, i8ge.2xlarge, i8ge.6xlarge, i8ge.12xlarge, i8ge.18xlarge | 155 | 2026-01-25 | 2026-09-25 | 2.0, 2.0, 4.0, 2.0, 2.0, 4.0, 6.0 |
| 2026.2 | same 7 sizes | 98 | 2026-05-06 | 2026-10-05 | same |
| 2026.3 | same 7 sizes | 63 | 2026-07-19 | 2026-10-04 | same |
| Total | | 350 | | | |

The pattern is clear in the same jobs:

- Every run on an affected size after the entries shipped is `WARNING`.
- The single-disk sizes of the same families are `PASS` in 623 of 650 runs, for example [i8ge.3xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/26bd92ec-5815-44b0-af33-2eafc05f4f56) and [i8g.4xlarge](https://argus.scylladb.com/tests/scylla-cluster-tests/8a07faba-43e9-47f4-bed6-11f0d6ed44a3) in 2026.1.0~rc0.
  23 of the other 27 are i7ie.3xlarge `write_iops` warnings on older AMIs ([example](https://argus.scylladb.com/tests/scylla-cluster-tests/b607dc13-dcf4-437b-a636-9e0f7c2af2f6)).
- Before the new entries shipped, i7ie.2xlarge was `PASS` with a ratio of 1.0 in all releases. Last `PASS` runs: [2025.1.11](https://argus.scylladb.com/tests/scylla-cluster-tests/e1a02dc9-6a22-401f-a84d-69764fc6c740), [2025.2.5](https://argus.scylladb.com/tests/scylla-cluster-tests/03de4f26-a627-4252-aa58-13d5580f47d4), [2025.3.2](https://argus.scylladb.com/tests/scylla-cluster-tests/c046ffb4-2fe2-4de7-9f43-c6546e57c866), [2025.4.0~rc2](https://argus.scylladb.com/tests/scylla-cluster-tests/6e5d517b-0325-4b2d-a4a7-dfc61804dbc2).

Nearly all of these runs have status `passed`. The runs checked have investigation status `not_investigated`.

First `WARNING` run per branch and size. The deviation column shows the read bandwidth deviation.

| Size | Deviation | 2025.1 | 2025.3 | 2025.4 | 2026.1 | 2026.2 | 2026.3 |
|---|---|---|---|---|---|---|---|
| i7ie.2xlarge | 50% (2025.1: 71%) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/ae3efa7d-c4db-4bbd-a420-e4243405d2af) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/24800240-2e67-49f2-b8a2-114f68f0fa7d) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/40aa8381-7ed5-4e67-9e06-733823971583) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/3edf954a-46d7-4535-9f56-a16479863784) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/18b5394e-d8c6-4b11-870d-2c84e98beb36) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/328fe743-8350-4e0e-909b-ae7530c79cdb) |
| i8g.8xlarge | 50% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/2bbb17a5-2dc6-42da-be08-3224e4d142e8) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/9eace7c3-b8aa-4fc9-80c5-5494b2dfc22c) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/fe166b6f-fe21-4cca-bf0b-e194622c5363) |
| i8g.16xlarge | 75% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/4087d5ec-1990-4efc-a6fd-1317d52fcae0) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/d821bba9-c562-436a-ae36-c543863d9b4a) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/dac750d1-4ebe-42d5-bb0e-a4c670696832) |
| i8ge.2xlarge | 50% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/5a85dd4b-0685-46c1-8486-41a9b2b2d1ac) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/2da74d41-f2ba-4182-99d6-cfdf80b827f5) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/d7ed4a85-12a8-4c6c-a8b8-60ff41bd93e5) |
| i8ge.6xlarge | 50% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/15f51722-e5ba-4ea0-82e1-1b716f76a279) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/ab135566-7083-4a6c-b151-7428fc9770c4) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/4a5f4a7b-0387-4e08-96c7-024442e94206) |
| i8ge.12xlarge | 75% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/a2e427d2-e3c5-49e0-892c-b3d2ebfdad78) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/668cd168-0f30-48c1-9710-839e78b374c9) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/a9ee5820-9ef4-44f8-bd94-bc90b0166d05) |
| i8ge.18xlarge | 83% | no run | no run | no run | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/0415696a-47af-4074-a2a8-2610480b4a7f) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/926779d6-2f58-4c51-966d-4b4a303f901d) | [run](https://argus.scylladb.com/tests/scylla-cluster-tests/779b1ce4-b434-4673-b9bd-a33d39b81a34) |

Example, 2026.1.0 on i8ge.12xlarge ([run](https://argus.scylladb.com/tests/scylla-cluster-tests/a2e427d2-e3c5-49e0-892c-b3d2ebfdad78)):

| Value | Installed | Measured | Deviation |
|---|---|---|---|
| read_bandwidth | 48,761,999,360 | 12,151,646,208 | 75% |
| read_iops | 3,743,524 | 924,312 | 75% |
| write_bandwidth | 24,098,465,792 | 6,040,161,280 | 74% |
| write_iops | 3,007,176 | 777,692 | 74% |

The installed values are identical to the values on the customer nodes in CUSTOMER-776.

## Near miss: SMI-297

[SMI-297](https://scylladb.atlassian.net/browse/SMI-297) (2026-06-16) compared the `aws_io_params.yaml` entries of i8g and i8ge to the AWS whole-instance IOPS.
It found the multi-disk entries 18% to 32% below the AWS figures and read them as whole-instance values.
Follow-up measurements with iotune and fio on an i8ge.12xlarge agreed with the entries.
Nobody compared the entries with the file installed on the node, which was four times higher.
Three months of investigation stayed one step from the defect.

## Why detection failed

1. **A warning does not fail the run.** The deviation goes to an Argus table cell. The run passes, and release triage focuses on failed runs. 350 warnings stayed unread.
2. **The review used the wrong reference.** [#776](https://github.com/scylladb/scylla-machine-image/pull/776) and [#796](https://github.com/scylladb/scylla-machine-image/pull/796) compared the entries to the AWS whole-instance figures. The values matched, so the review accepted them.
3. **The symptom got a partial fix.** The review saw 4 times steps between sizes. The measuring script fix removed one factor of the disk count.
4. **The yaml unit is implicit.** No comment and no test stated that entries are per disk. The unit tests checked only that a mock i3 value is multiplied by the disk count.
5. **The measuring tool reads back the installed file.** On an AMI with a preset it returns `entry * disks`, not a measurement.
6. **The final i8g and i8ge commit had no PR review.** `b8ef6fa` was pushed to `next` directly. Its commit message table differs from the yaml in the same commit: i8g.24xlarge read IOPS is 16,056K in the table and 2,662,435 in the yaml.
7. **The 2025.1 backports multiplied the entries.** The conflict resolution on `branch-2025.1` stored `next` values times the disk count. The reason is not known.
8. **Coverage has gaps.** No i7i size is in the release list. The largest i7ie, i8g and i8ge sizes with a preset are not in the list.

## Corrective actions

| # | Action | Repository | Owner | Status |
|---|---|---|---|---|
| 1 | Use per-disk entries for i7i, i7ie, i8g, i8ge. Divide by the disk count in the measuring tool. Add a per-drive test. Add a unit header to the yaml. | scylla-machine-image | Valerii Ponomarov | Merged to `next` ([#1033](https://github.com/scylladb/scylla-machine-image/pull/1033)) |
| 2 | Backport [#1033](https://github.com/scylladb/scylla-machine-image/pull/1033) to 2026.3, 2026.2, 2026.1, 2025.4 and 2025.3. | scylla-machine-image | | 2026.3 open ([#1034](https://github.com/scylladb/scylla-machine-image/pull/1034)), waits for performance tests |
| 3 | Fix the squared entries on `branch-2025.1`. | scylla-machine-image | | Needs a ticket |
| 4 | Raise an `ERROR` event in the artifacts test when the installed value is more than 15% above the measured value. Keep `WARNING` for values below the measurement. | scylla-cluster-tests | | Proposed |
| 5 | Show result-table warnings on the run status or the release dashboard in Argus, so passed runs with warnings get triage. | argus | | Proposed |
| 6 | Add every multi-disk size that has a preset to the release AMI list: all i7i sizes, i7ie 6xlarge to 24xlarge, i8g 12xlarge and 24xlarge, i8ge 24xlarge. | scylla-pkg | | Proposed |
| 7 | Add a check that each `aws_io_params.yaml` key has a release AMI test entry. | scylla-machine-image or scylla-pkg | | Proposed |
| 8 | Add a unit test that compares each entry times the disk count to the AWS published instance-store IOPS. | scylla-machine-image | | Proposed |
| 9 | Make the measuring tool fail when it reads a file that a preset wrote. | scylla-machine-image | | Proposed |
| 10 | Write a runbook for nodes with wrong values. Do a capacity check before each change (see the procedure in SMI-324). | SRE, scylla-machine-image | | Proposed |
| 11 | List the customer clusters on affected sizes and releases. | Cloud, support | Israel Fruchter | Planned |

## Open items

- Find the AMI that the [#796](https://github.com/scylladb/scylla-machine-image/pull/796) `7bcde6a` measurement used.
- Find why the 2025.1 backport stored entries times the disk count.
- Find why 23 release runs on i7ie.3xlarge show `write_iops` warnings on older AMIs.
- Confirm that no Argus view or notification shows result-table warnings for passed runs.
- Find which cloud images (Scylla Cloud, marketplace) used the affected releases.
