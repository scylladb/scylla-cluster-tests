# Workflow: Analyze Capacity Failures

Five phases from "how many runs failed with X" to a published report. Phases are numbered because the
region resolution in Phase 3 depends on the run set from Phase 2, and reporting before provenance is known
produces figures that cannot be defended.

## Phase 1: Establish Scope

**Entry:** A question about capacity or provisioning failures.

1. Fix the **time window**. Default to 12 weeks. Warn the user that Jenkins region coverage degrades with
   window length (roughly 80% at 6 weeks, about half at 12) — a longer window buys more failures but a
   larger inferred fraction.
2. Fix the **test set**. The Argus CLI requires explicit test UUIDs and has no command to enumerate a group,
   so a job name alone is not enough. Resolve UUIDs from, in order:
   - the "Test Registry" table in `skills/perf-weekly-status-report/SKILL.md` (the 20 enterprise perf tests)
   - `argus run get --run-id UUID`, which returns the `test_id` for an example run the user supplied
   - the user, asked directly
3. Write the set to a TSV of `name<TAB>test_id<TAB>category`.
4. If the user named one job as an example, confirm whether they want just that job or the whole suite —
   the two answers differ by an order of magnitude in effort and in conclusion.

**Exit:** A TSV of test UUIDs and an agreed window.

## Phase 2: Collect and Classify

**Entry:** Phase 1 complete.

1. Run the helper script from the repository root:

   ```bash
   PYTHONPATH=. .venv/bin/python skills/capacity-failure-analysis/capacity_failure_stats.py \
       --registry tests.tsv --weeks 12 --json-out capacity.json
   ```

2. Read the signature breakdown it prints. If one signature is the whole population, say so in the report —
   it narrows the remediation. If the placement-group variant appears, split it out; it is a configuration
   bug, not a capacity shortage.
3. Sanity-check the failure count against total `test_error` runs. Capacity failures being the large
   majority of `test_error` is expected; capacity failures *exceeding* `test_error` means the matcher is
   catching recovered attempts and the rule is wrong.

**Exit:** `capacity.json` exists, and the signature mix is understood.

## Phase 3: Establish Region Provenance

**Entry:** `capacity.json` exists.

1. Read the provenance line the script prints: how many regions came from Jenkins, from Argus, and from
   inference, plus the cross-validation accuracy.
2. Apply the accuracy thresholds from [region-resolution.md](../references/region-resolution.md):
   above ~90% infer and label; 70-90% use for shape only; below 70% do not infer at all.
3. If Jenkins coverage is poor and the window is negotiable, offer the user a shorter window with better
   coverage as an alternative.
4. Confirm no region bucket is `unknown`. If any remain, report them as their own row — never drop them, as
   dropping silently changes the denominator.

**Exit:** The measured-versus-inferred split is known and a sentence describing it has been drafted.

## Phase 4: Aggregate and Interpret

**Entry:** Phase 3 complete.

1. Build three groupings: per region, per ISO week, per job. Every row carries runs, failures and rate.
2. Rank remediation by **rate**, not count. Flag regions with fewer than ~10 runs as too small to rank.
3. Cross-check region against instance family. If every affected job shares one family (i8g, i4i), the story
   is an instance shortage rather than a regional one, and the remediation changes accordingly.
4. Look for weeks where every region fails at once — that indicates an external AWS capacity event, and no
   configuration change will help.
5. Pick the remediation from the table in `SKILL.md` and name a concrete target region or instance type,
   quoting the rate that justifies it.

**Exit:** A ranked region table and a specific, justified recommendation.

## Phase 5: Report

**Entry:** Phase 4 complete.

1. Build the HTML following [report-format.md](../references/report-format.md) — section order, the
   validated palette, both the chart and its table view.
2. Include the method section in full: detection rule with source line, runs versus builds, region
   provenance with inference accuracy, and scope limits.
3. Publish as an Artifact. If the user also wants a local file, wrap it in a real
   `<!doctype html><html><head></head><body>` skeleton first.

**Exit:** Report published and the link given to the user.

## Verification

Before delivering, confirm every item:

- [ ] Failure counts reproduce from `capacity.json` — spot-check one region's total against the run list
- [ ] Every count is paired with a run total and a rate
- [ ] Builds hit is reported as well as failed runs
- [ ] Provenance sentence states measured versus inferred, with the accuracy figure
- [ ] Inferred values are visibly marked in the failure log
- [ ] Signature variants are separated, not merged
- [ ] Regions too small to rank are flagged
- [ ] The recommendation names a region or instance type and the rate behind it
- [ ] Scope limits are stated, including that non-registry tests were not swept
