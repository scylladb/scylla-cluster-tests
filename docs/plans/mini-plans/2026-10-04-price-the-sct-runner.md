# Mini-Plan: Include the SCT Runner in the Cost Estimate

**Date:** 2026-10-04
**Estimated LOC:** ~120
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))

## Problem

Every run pays for an SCT runner, and no estimate includes it. The runner also outlives the
test: its window covers startup, the test, teardown, log collection, cleanup and email. For a
four-hour test it is budgeted at closer to seven hours. That is a few percent of a large run but
a much bigger share of a small one, which is where a future cost gate would be most wrong.

The runner is fully predictable. `sdcm/sct_runner.py:SctRunner.instance_type` picks one of two
fixed types per cloud from the test duration, and the runner is never spot. Backend and test
duration are both known when the estimate runs.

The real obstacle is the catalog. It only carries what clusters use, so the runner families
(`m7i-flex.*` on AWS, `Standard_E2s_v3` on Azure) price as unknown today.

## Approach

`config -> runner type from backend + duration -> runner window -> on-demand rate -> runner line in the estimate`

1. Add the runner families to the catalog configuration and regenerate.
2. Show the runner as its own row in the estimate, not folded into a cluster role, so the table
   still explains itself.
3. Price it over the runner's window, not `test_duration`, which would under-count.
4. Always use on-demand, whatever the run's provision type, because the runner never uses spot.
5. A backend with no runner (docker, local k8s) gets no runner row.

Out of scope: credential-gated checks that each price source still answers, and measuring
estimates against the actual cost now reported to Argus.

## Files to Modify

- `data/instance_catalog/sizing_config.yaml` -- add the runner families
- `data/instance_catalog/{aws,gce,azure,oci}.yaml` -- regenerated
- `sdcm/utils/cloud_catalog/cost.py` -- add the runner row
- `unit_tests/unit/test_cost.py` -- runner row present, priced over its window, never spot,
  absent on runner-less backends

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py -v`
- [ ] `hydra estimate-cost -b aws test-cases/longevity/longevity-10gb-3h.yaml` shows a priced
      runner row
- [ ] A test longer than seven hours shows the long-term runner type
- [ ] A spot run still prices the runner on-demand
- [ ] `uv run sct.py pre-commit` passes
