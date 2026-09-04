# Runbook: Migrate Performance Tests to a New Instance Type

Adding or switching the hardware a perf job runs on.

The worked example behind this runbook is the i8g (Graviton / ARM64) migration —
[../plans/i8g-performance-jobs-migration.md](../plans/i8g-performance-jobs-migration.md).
Read it if your migration also changes architecture.

**Time:** several weeks end to end, because the validation runs are on a weekly cadence.

---

## What makes this expensive

Nothing about a perf test transfers across hardware for free:

| Changes with hardware | Why |
|---|---|
| Throughput steps | They are percentages of a max that just changed |
| Latency thresholds | Absolute ms limits calibrated on the old hardware |
| Thread / concurrency counts | Different core counts and memory bandwidth |
| Region assignment | New instance families are not available everywhere |
| Image resolution | ARM64 needs a different AMI, resolved per (provider, arch) |

Plan for all five. Skipping the recalibration produces jobs that either never fail or always
fail.

---

## Step 1 — Decide how the instance is specified

Two options:

**Constraint-based (preferred for new work).** Express what the test needs and let SCT resolve
the instance per cloud:

```yaml
sizing_db:
  vcpu: 16
  memory: 128
  arch: arm64
  disk: nvme
```

See [../cross-cloud-sizing.md](../cross-cloud-sizing.md) for the constraint reference and
`hydra sizing-preview` to check what resolves. The `migrate-to-sizing` skill automates
converting literal `instance_type_*` params.

**Literal.** `instance_type_db` / `gce_instance_type_db` / `azure_instance_type_db`. Still used
by most existing perf jobs, and correct when the whole point of the job is to characterise one
specific instance type.

For a hardware *comparison* job, use literals — the instance is the variable under test.

## Step 2 — Create baseline jobs

Copy the existing jenkinsfiles for the family, suffixing the hardware:

```
scylla-enterprise-perf-regression-predefined-throughput-steps-tablets.jenkinsfile
  -> scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-tablets.jenkinsfile
```

Naming convention in `branch-perf-v17/scylla-enterprise/perf-regression/` is
`<family>-<hardware>-<topology>`, e.g. `-i8g-tablets`, `-z4d-highlssd-tablets`,
`-azure-vnodes`.

At this stage point them at the **existing** step and threshold configs. They will be wrong,
but the jobs need to run before you can measure anything.

**Do not delete the old jobs.** They are the rollback path, and they remain the only valid
jobs for versions that do not support the new hardware.

## Step 3 — Confirm availability and pick regions

- Check the instance family exists in your candidate regions and that quota is sufficient.
- Pick regions that are free at your job's cadence — concurrent perf jobs in one region
  interfere. Rules and current allocation:
  [../perf-tests-region-scheduling.md](../perf-tests-region-scheduling.md).
- Set `job_throttle_category` to a value scoped to (region, instance family), e.g.
  `SCT-perf-eu-west-2-i8g`.

Insufficient capacity surfaces as `InsufficientInstanceCapacity`, which
`_is_test_error()` maps to `TEST_ERROR` rather than `FAILED` — so a capacity problem will not
look like a regression, but it will cost you a cycle.

## Step 4 — Validation runs on the old config

Run the new jobs against recent builds with the old steps. Goals:

- [ ] Provisioning works — image resolves for the arch, disks come up as expected
- [ ] Stress tools run (for a new architecture, confirm the loader images are multi-arch)
- [ ] The test completes end to end
- [ ] **Capture the achieved `unthrottled` throughput** — this is your first read on the new
      ceiling

Architecture change checklist:

- [ ] Loader Docker images exist for the target arch (`configurations/stress_images/`)
- [ ] `arch: "aarch64"` set on the trigger entry
- [ ] Scylla AMI available for the arch in the chosen region

## Step 5 — Recalibrate

This is the substantive work. Follow
[runbook-recalibrate-steps.md](runbook-recalibrate-steps.md) in full: uncapped run per
(load, config) -> percentage ladder -> new step config -> validation -> new thresholds.

Produces:

- `configurations/performance/<tool>_gradual_load_steps_<hardware>.yaml`
- `configurations/performance/latency-decorator-error-thresholds-steps-<...>-<hardware>-<topology>.yaml`

> **Naming is inconsistent today.** The shipped file is
> `latency-decorator-error-thresholds-steps-ent-i8g-tablets.yaml` (hardware before topology),
> while the i8g plan's Phase 6 specifies
> `latency-decorator-error-thresholds-steps-ent-vnodes-i8g.yaml` (topology before hardware).
> Match the shipped file. Also note only the **tablets** i8g threshold file exists — there is
> no i8g vnodes threshold file yet.

Document the comparison against the previous hardware's thresholds in the new file's comments.
When both instance type and architecture change, expect the difference to be substantial and
non-uniform across steps.

## Step 6 — Wire up the version matrix

Edit `configurations/triggers/perf-regression.yaml` — **not** the generated jenkinsfile, and
not `perfRegressionParallelPipelinebyRegion.groovy`.

Route by version support. Versions that support the new hardware get the new job; older
versions keep the old one:

```yaml
# new hardware, master only, on a schedule label
- job_name: "/scylla-enterprise/perf-regression/...-i8g-vnodes"
  backend: "aws"
  arch: "aarch64"
  include_versions: ["master"]
  labels: ["master-monthly"]
  job_throttle_category: "SCT-perf-eu-west-2-i8g"
  params:
    region: "eu-west-2"
    sub_tests: '["test_read_gradual_increase_load", ...]'

# new hardware, release branches that support it (no label — non-master bypasses label gating)
- job_name: "/scylla-enterprise/perf-regression/...-i8g-vnodes"
  backend: "aws"
  arch: "aarch64"
  exclude_versions: ["2025.2", "2025.1", "2024.2", "2024.1", "master"]
  labels: []
  ...

# old hardware retained for versions that cannot run the new one
- job_name: "/scylla-enterprise/perf-regression/...-vnodes"
  backend: "aws"
  include_versions: ["2025.2", "2025.1", "2024.2", "2024.1"]
  labels: []
  ...
```

Remember: **label gating applies only to `master`.** Release-branch entries carry `labels: []`
and are selected purely by version. Get the version boundaries right — they are prefix
matches, so `2025.3` also matches `2025.3.3`.

Then run the generator (or let pre-commit do it):

```bash
python3 utils/build_system/generate_trigger_jenkinsfiles.py
```

Add comments in the YAML documenting the arch routing — future readers need to know why two
entries exist for what looks like the same job.

## Step 7 — Monitor production runs and keep a rollback

- [ ] Watch the first triggered run of each new job
- [ ] Confirm results land in Argus under the new job name
- [ ] Confirm thresholds fire on genuine regressions and not on noise
- [ ] Old jobs still present and runnable

Rollback is reverting the `job_name` in the trigger YAML back to the old jobs and regenerating.
Document the criteria that would trigger it (e.g. more than N consecutive false-positive
threshold failures) before you deploy, not after.

---

## Checklist

- [ ] Instance specified (constraint or literal), previewed
- [ ] New jenkinsfiles created, old ones retained
- [ ] Instance available and in quota in chosen regions
- [ ] Regions free at this cadence; recorded in the region-scheduling doc
- [ ] `job_throttle_category` scoped to (region, family)
- [ ] Loader images available for the target arch
- [ ] Infrastructure validation runs pass
- [ ] Steps recalibrated per [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md)
- [ ] Thresholds created, keys matching generated step names
- [ ] Trigger YAML updated with version routing, generator run
- [ ] Rollback criteria written down
