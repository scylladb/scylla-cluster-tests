# Mini-Plan: Replace Spot Fleet with EC2 Fleet Supporting Multiple Instance Types

**Date:** 2026-08-03
**Estimated LOC:** ~350
**Related PR:** https://github.com/scylladb/scylla-cluster-tests/pull/15640

## Problem

`Spot Fleet` (`request_spot_fleet`) only accepts a **single instance type** per launch
specification list entry practically used by SCT today (`sdcm/provision/aws/utils.py:302-317`
builds `LaunchSpecifications=[instance_parameters]` with exactly one entry). For large scale
tests (e.g. `test-cases/scale/scale-cluster.yaml`, 180-200 nodes, single
`instance_type_db: i7i.large`), this means the whole spot request lives or dies on the capacity
of one instance pool, causing frequent `SPOT_CAPACITY_NOT_AVAILABLE_ERROR` /
`FLEET_LIMIT_EXCEEDED_ERROR` failures. AWS also treats Spot Fleet as legacy and recommends
**EC2 Fleet** (`create_fleet`) for new integrations, which natively supports diversifying a
single request across several instance type overrides (e.g. `i7i.large`, `i7ie.large`,
`i4i.large`), increasing the chance of full capacity fulfillment.

## Approach

Both AWS provisioning paths share one EC2 Fleet helper, `provision_ec2_fleet()` in
`sdcm/provision/aws/utils.py`: the upfront `provision-resources` path (`sdcm/sct_provision/aws/*` ->
`AWSInstanceProvisioner`) and the in-test path that `add_nodes` takes during cluster growth and
nemesis (`sdcm/cluster_aws.py` -> `EC2ClientWrapper.create_spot_fleet`). That removes SCT's last
RequestSpotFleet caller.

- Add a new `create_ec2_fleet_instance_request()` helper in `sdcm/provision/aws/utils.py` that
  calls `ec2_clients[region].create_fleet(...)` with `Type="instant"`,
  `TargetCapacitySpecification={"TotalTargetCapacity": count, "DefaultTargetCapacityType": "spot"}`,
  and a `LaunchTemplateConfigs[0].Overrides` list built from one-or-more instance types (each
  override differs only by `InstanceType`, everything else — AMI, subnet, security groups, user
  data — shared via a single launch template / launch spec base).
- Use the `capacity-optimized-prioritized` Spot allocation strategy and give every override a
  `Priority` in list order (primary type = 0). AWS still optimizes for capacity first, but honors
  the priorities on a best-effort basis; plain `capacity-optimized` has no notion of a preferred type
  and could place a scale test on an older generation while the primary pool still had capacity.
- **No polling helper is needed.** Because the request uses `Type="instant"`, `create_fleet`
  is synchronous: the response already carries `Instances[].InstanceIds` and an `Errors` list
  (non-empty on partial fulfillment). The Spot Fleet describe/poll loop
  (`get_provisioned_fleet_instance_ids()` + `describe_spot_fleet_request_history`) is therefore
  dropped rather than mirrored; error classification reads the `Errors` list directly
  (`is_ec2_fleet_retryable()` / `log_ec2_fleet_errors()`).
- **All-or-nothing, no batches.** Each AZ's nodes go into one request with
  `SpotOptions.MinTargetCapacity` = count and `SingleAvailabilityZone`, so AWS launches every
  instance or none. SCT never pays for a partial set it must throw away, and there are no earlier
  batches to roll back when a later one fails. AWS caps `MinTargetCapacity` at 1000, so a request
  above `EC2_FLEET_MAX_TARGET_CAPACITY` fails loudly before anything is created. If AWS reports
  transient errors (`RequestLimitExceeded`, `InternalError`, `ServiceUnavailable`), the request is
  sent again, up to 3 attempts with 10s/20s backoff; capacity, limit and configuration errors go
  straight to the caller's fallback. Rolling back a partial set stays as a safety net.
- In `sdcm/provision/aws/provisioner.py::AWSInstanceProvisioner`, rename
  `_execute_spot_fleet_instance_request()` -> `_execute_ec2_fleet_instance_request()` and call the
  new EC2 Fleet helper instead of `create_spot_fleet_instance_request()`/`cancel_spot_fleet_requests`,
  using `delete_fleets` for cleanup. Note: an `instant` fleet cannot be deleted while retaining its instances, so on
  success the (inert) fleet record is left in place - AWS deletes it automatically once its
  instances terminate; deletion with termination is used only on the rollback/error paths.
- Keep SCT-779's guarantee that a provisioning process killed mid-request (e.g. a Jenkins stage
  timeout) leaves nothing clean-resources can't find: tag the fleet (`ResourceType=fleet`) and its
  throwaway launch template (`ResourceType=launch-template`) with the instance tags minus the
  per-node `Name`. The `instant` fleet itself can't keep launching after a kill, and its instances
  are tagged at launch, so only the launch template can leak. clean-resources gets an `aws`-backend
  step deleting leaked templates, limited to the `sct-fleet-` name prefix because
  `clean_launch_templates_aws` ignores `keep` tags (the SCT runner's template is tagged
  `RunByUser=QA`).
- Extend `instance_parameters` handling so `AWSInstanceProvisioner.provision()` /
  `_provision_spot_instances()` accept `instance_parameters: AWSInstanceParams |
  List[AWSInstanceParams]` (the abstract base in
  `sdcm/provision/common/provisioner.py:29-35` already types this as
  `InstanceParamsBase | List[InstanceParamsBase]`, so no interface change needed there) — when a
  list is given, build one `Overrides` entry per `InstanceType` in the EC2 Fleet request instead
  of a single-type launch spec.
- Add a dedicated AWS config param `aws_instance_type_db_alternatives` (a `StringOrList`, i.e. an
  actual list of interchangeable DB instance types) consumed **only** by the EC2 Fleet provisioning
  path. `instance_type_db` / `instance_type_loader` stay single-literal and unchanged everywhere else.
  **Decision:** rejected overloading `instance_type_db` with CSV — maintainer feedback (@fruch) and
  the open heterogeneous-cluster proposal (PR #13427) reserve a future CSV/`cluster_topology`
  meaning of "deploy different types per rack" for `instance_type_db`, which would collide with a
  "spot alternatives" meaning. A separate param keeps the two concepts unambiguous and avoids
  auditing every plain-string consumer of `instance_type_db` (AMI/arch lookup, sizing validation,
  AZ selection).
- Validate the alternatives at config time: each must be available in the region and match
  `instance_type_db`'s CPU architecture (the DB AMI is selected for it), vCPU count and memory,
  using the offline instance catalog (`data/instance_catalog`) and falling back to the AWS arch
  lookup for uncatalogued types. Local disk size and CPU generation may differ, so a fleet can
  produce a heterogeneous cluster - acceptable for scale tests, and documented on the option.
- Wire the parsed alternatives list down through `sdcm/sct_provision/aws/cluster.py`
  (`_instance_types = [instance_type_db] + split_instance_types(aws_instance_type_db_alternatives)`,
  deduped) into the list-based `provision()` call. Only DBCluster defines an alternatives param;
  all other clusters (loader, monitor, oracle, zero-token) provision a single instance type.
- When EC2 Fleet is used:
  - `instance_provision: spot_fleet` - always, at any count, on both paths, so every DB node
    (initial or added mid-test) gets the alternatives. spot_fleet means spot or nothing: it never
    falls back to on-demand, whatever `instance_provision_fallback_on_demand` says (AZ/region
    fallback still applies). The scale-cluster Jenkins jobs set `provision_type: 'spot_fleet'`.
  - `instance_provision: spot` - only on the upfront path for more than `SPOT_CNT_LIMIT` (10)
    nodes per AZ, as before; smaller requests and in-test `add_nodes` use plain spot requests with
    `instance_type_db` alone.
- Spot placement scores (`use_spot_placement_scores`) are skipped for spot_fleet runs. They score
  a request for `instance_type_db` alone, while the fleet also offers the alternatives - and AWS
  documents that fewer than three types always score low - so they must not pick or veto AZs.

## Files to Modify

- `sdcm/provision/aws/utils.py` -- add `provision_ec2_fleet()` (the shared all-or-nothing entry
  point), `create_ec2_fleet_instance_request()`,
  `create_launch_template()`/`delete_launch_template()`, `delete_ec2_fleet()`,
  `is_ec2_fleet_retryable()`, `log_ec2_fleet_errors()` and `split_instance_types()`, replacing
  the `SpotFleet`-specific helpers (`create_spot_fleet_instance_request()`,
  `get_provisioned_fleet_instance_ids()`). No `describe_fleets` polling helper is added —
  `instant` fleets return synchronously.
- `sdcm/provision/aws/provisioner.py` -- rename `_execute_spot_fleet_instance_request()` ->
  `_execute_ec2_fleet_instance_request()` (supporting a list of `AWSInstanceParams`), drop the
  `_get_provisioned_fleet_instance_ids()`/`_wait_for_fleet_request_done()` poll helpers, use
  `delete_ec2_fleet()`/direct termination for cleanup instead of `cancel_spot_fleet_requests`, retry
  transient fleet errors, and tag the fleet and its launch template
- `sdcm/provision/aws/constants.py` -- add EC2 Fleet constants (`EC2_FLEET_MAX_TARGET_CAPACITY`,
  `EC2_FLEET_LAUNCH_TEMPLATE_PREFIX`, `EC2_FLEET_TYPE_INSTANT`, `EC2_FLEET_ALLOCATION_STRATEGY`,
  `EC2_FLEET_RETRYABLE_ERROR_CODES`, `EC2_FLEET_MAX_ATTEMPTS`, `EC2_FLEET_RETRY_BACKOFF`)
- `sdcm/sct_config/mixins/aws.py` -- add the dedicated `aws_instance_type_db_alternatives` field
  (fleet-only, AWS-only, `StringOrList`) to `AwsConfigMixin`
- `sdcm/sct_config/config.py` -- in `_instance_type_validation()`, validate every listed type is
  available in the target region and interchangeable with `instance_type_db`
  (`_validate_aws_instance_type_db_alternatives()`); `instance_type_db`/`instance_type_loader` stay
  single-literal
- `sdcm/sct_provision/aws/cluster.py` -- `_instance_types` builds `[instance_type_db] +
  alternatives` (deduped) via `_INSTANCE_TYPE_ALTERNATIVES_PARAM_NAME`; only the fleet path uses
  entries beyond the first
- `sdcm/provision/common/provision_plan_builder.py`, `sdcm/provision/common/provisioner.py` -- a
  `spot_fleet` plan step (`fleet=True`) with no on-demand fallback
- `sdcm/provision/common/fallback.py` -- skip spot placement scores for spot_fleet
- `sdcm/cluster_aws.py`, `sdcm/ec2_client.py` -- spot_fleet `add_nodes` through
  `provision_ec2_fleet()` with the DB alternatives, never on-demand; RequestSpotFleet code removed
- `jenkins-pipelines/oss/{scale,raft}/scale-*-cluster-test.jenkinsfile` -- `provision_type: 'spot_fleet'`
- `sdcm/utils/resources_cleanup.py` -- `name_prefix` filter for `clean_launch_templates_aws()` and
  an `aws`-backend step deleting leaked `sct-fleet-*` launch templates
- `test-cases/scale/scale-cluster.yaml` -- `aws_instance_type_db_alternatives: ['i7ie.large',
  'i4i.large', 'i3en.large']`: the other AWS types matching the test's `sizing_db` (which resolves
  `instance_type_db` to `i7i.large`)
- `unit_tests/unit/test_aws_spot_provisioning.py`, `unit_tests/unit/test_aws_ec2_fleet_provisioner.py`
  -- the `create_fleet` request shape, error classification and retries, rollback, tagging, and
  `split_instance_types()` (replacing the Spot Fleet polling tests)
- `unit_tests/unit/config/test_aws_instance_type_db_alternatives.py` -- alternatives validation
- `unit_tests/unit/test_clean_cloud_resources_func.py` -- the scoped launch template cleanup step
- `unit_tests/unit/provisioner/test_provision_plan_builder.py`, `unit_tests/unit/test_legacy_spot_fleet.py`
  -- spot_fleet plan steps, and spot_fleet `add_nodes`
- Not done, follow-up: `unit_tests/integration/test_aws_services.py` still parametrizes
  `instance_provision` over `["on_demand", "spot", "spot_fleet"]` without a multi-type case

## Verification

- [x] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_aws_spot_provisioning.py
      unit_tests/unit/test_aws_ec2_fleet_provisioner.py unit_tests/unit/test_legacy_spot_fleet.py
      unit_tests/unit/provisioner/ unit_tests/unit/config/ unit_tests/unit/test_clean_cloud_resources_func.py -v`
- [x] The `create_fleet` request with several instance type overrides is built correctly
      (asserted on the API call payload in unit tests, no live AWS call required)
- [x] With `spot`, `SPOT_CNT_LIMIT` gating still routes small counts to single spot instance
      requests; with `spot_fleet`, every count goes through EC2 Fleet
- [ ] A live `scale-180-200-cluster-test` run provisions its DB nodes through EC2 Fleet
- [ ] `uv run sct.py pre-commit` passes
