# Mini-Plan: Prevent Orphaned Spot Fleet Instances on Provisioning Timeout (SCT-779)

**Date:** 2026-08-03
**Owner:** roydahan
**Estimated LOC:** ~150
**Related Jira:** [SCT-779](https://scylladb.atlassian.net/browse/SCT-779)
**Reproduction:** https://argus.scylladb.com/tests/scylla-cluster-tests/57239065-71b4-4179-b38b-e8aaf40cc2fd

## Problem
On `scale-180-200-cluster-test`, the Jenkins "Provision Resources" stage (fixed 30-min timeout,
`vars/longevityPipeline.groovy:353`) fired while SCT was mid-relocation to a 3rd fallback AWS
region (`eu-west-3`). The hard-kill of the remote process meant the in-flight Spot Fleet Request
was never cancelled. AWS kept fulfilling it asynchronously after the pipeline gave up, and the
post-actions `clean-resources` step only terminated the instances it could find at that moment —
it had no code path that cancels open Spot Fleet Requests. Result: 66 orphaned `i7i.large`
instances leaked in `eu-west-3`.

## Approach
1. **Disable region fallback for all scale-tests** (separate, self-contained commit): add
   `fallback_to_next_region: false` to `test-cases/scale/scale-cluster.yaml`, the shared base config
   for every `configurations/scale/scale-*.yaml` variant. Relocating a 100+ node cluster across
   regions is what blew the fixed stage-timeout budget in the incident; AZ-level fallback within the
   configured region is unaffected. (This replaces the original "increase the stage timeout" idea.)
2. **Tag the Spot Fleet Request itself at creation, then cancel by tag in clean-resources.**
   Spot Fleet Requests *are* taggable (`ResourceType="spot-fleet-request"` in the request config's
   top-level `TagSpecifications`) — only the fleet's `LaunchSpecifications[].TagSpecifications` are
   restricted to `ResourceType="instance"`. So we tag the request with the same standard SCT tags as
   the instances it launches, and clean-resources discovers and cancels it by tag exactly like every
   other resource type — regardless of *why* it was left behind. This needs no cross-process state,
   no persisted file, and covers every process-kill mode (SIGTERM, SIGKILL, host loss).
   - Tag the request at both creation sites: `sdcm/ec2_client.py:_request_spot_fleet` (legacy) and
     `sdcm/provision/aws/utils.py:create_spot_fleet_instance_request` (current provisioner), deriving
     the `spot-fleet-request` tag spec from the instance tag spec already being built.
   - New `clean_spot_fleet_requests_aws(tags_dict, regions, dry_run)` in
     `sdcm/utils/resources_cleanup.py`: per region, paginate `describe_spot_fleet_requests`, match
     configs whose `Tags` contain all `tags_dict` pairs and whose state is active
     (`submitted`/`active`/`modifying`), then `cancel_spot_fleet_requests(..., TerminateInstances=True)`.
   - Wire into `clean_cloud_resources()` as a cleanup step run *before* `clean_instances_aws` (cancel
     the request first so it stops launching new instances, settle, then the instance pass sweeps
     whatever it launched). `dry_run` is fully side-effect free.

## Files to Modify
- `test-cases/scale/scale-cluster.yaml` -- add `fallback_to_next_region: false`
- `sdcm/ec2_client.py` -- `_spot_fleet_request_tag_specifications()` helper; set top-level
  `TagSpecifications` on the fleet config in `_request_spot_fleet`
- `sdcm/provision/aws/utils.py` -- set top-level `TagSpecifications` on the fleet request config in
  `create_spot_fleet_instance_request`
- `sdcm/utils/resources_cleanup.py` -- new `clean_spot_fleet_requests_aws()`; wire into
  `clean_cloud_resources()` before `clean_instances_aws`
- `unit_tests/unit/test_clean_cloud_resources_func.py` -- top-level `test_*` functions for
  `clean_spot_fleet_requests_aws()` (mocked `describe_spot_fleet_requests` paginator /
  `cancel_spot_fleet_requests`): matching/non-matching tags, terminal-state skip, dry-run,
  per-region error isolation
- `unit_tests/unit/test_config.py` -- regression test asserting `SCTConfiguration` resolves
  `fallback_to_next_region=False` for `test-cases/scale/scale-cluster.yaml`

Note: the earlier `TestConfig` file-handoff + `flock` approach (and its
`cancel_leaked_spot_fleet_requests` cleanup) was removed — tagging makes it unnecessary.

## Verification
- [x] `SCTConfiguration` resolves `fallback_to_next_region: false` for
      `test-cases/scale/scale-cluster.yaml` (covered by `test_36a_scale_tests_disable_region_fallback`)
- [x] Fleet request is tagged with `ResourceType="spot-fleet-request"` at both creation sites
- [x] `clean_spot_fleet_requests_aws` cancels only tag-matching, active requests; skips terminal
      state and non-matching tags; per-region errors don't abort other regions
- [x] `--dry-run` never calls `cancel_spot_fleet_requests` and never sleeps
- [x] Unit tests pass: `test_config.py`, `test_clean_cloud_resources_func.py`, `provisioner/`
- [x] `ruff check` / `ruff format` clean on all modified files
