# Mini-Plan: Replace Spot Fleet with EC2 Fleet Supporting Multiple Instance Types

**Date:** 2026-08-03
**Estimated LOC:** ~350
**Related PR:** TBD

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

Scope this to the actively-tested provisioning path in `sdcm/provision/aws/*` +
`sdcm/sct_provision/aws/*` (exercised by `unit_tests/integration/test_aws_services.py`). The
older `sdcm/cluster_aws.py` / `sdcm/ec2_client.py` spot-fleet path is legacy and out of scope for
this mini-plan — track its removal/migration separately if `tester.py` still depends on it.

- Add a new `create_ec2_fleet_instance_request()` helper in `sdcm/provision/aws/utils.py` that
  calls `ec2_clients[region].create_fleet(...)` with `Type="instant"`,
  `TargetCapacitySpecification={"TotalTargetCapacity": count, "DefaultTargetCapacityType": "spot"}`,
  and a `LaunchTemplateConfigs[0].Overrides` list built from one-or-more instance types (each
  override differs only by `InstanceType`, everything else — AMI, subnet, security groups, user
  data — shared via a single launch template / launch spec base).
- **No polling helper is needed.** Because the request uses `Type="instant"`, `create_fleet`
  is synchronous: the response already carries `Instances[].InstanceIds` and an `Errors` list
  (non-empty on partial fulfillment). The Spot Fleet describe/poll loop
  (`get_provisioned_fleet_instance_ids()` + `describe_spot_fleet_request_history`) is therefore
  dropped rather than mirrored; error classification reads the `Errors` list directly
  (`is_ec2_fleet_unfulfillable()` / `log_ec2_fleet_errors()`).
- In `sdcm/provision/aws/provisioner.py::AWSInstanceProvisioner`, rename
  `_execute_spot_fleet_instance_request()` -> `_execute_ec2_fleet_instance_request()` and call the
  new EC2 Fleet helper instead of `create_spot_fleet_instance_request()`/`cancel_spot_fleet_requests`,
  using `delete_fleets` for cleanup. `_is_provision_type_fleet()` gating (count > `SPOT_CNT_LIMIT`)
  is unchanged. Note: an `instant` fleet cannot be deleted while retaining its instances, so on
  success the (inert) fleet record is left for AWS to reap; deletion with termination is used only
  on the rollback/error paths.
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
- Wire the parsed alternatives list down through `sdcm/sct_provision/aws/cluster.py`
  (`_instance_types = [instance_type_db] + split_instance_types(aws_instance_type_db_alternatives)`,
  deduped) into the list-based `provision()` call. Only DBCluster defines an alternatives param;
  all other clusters (loader, monitor, oracle, zero-token) provision a single instance type.
- Replace the `SPOT_FLEET_LIMIT` constant with `EC2_FLEET_LIMIT` (500) for the per-request fleet
  batch cap; `SPOT_CNT_LIMIT` (10) still gates fleet vs. plain-spot. The batching logic in
  `_provision_spot_instances()` additionally rolls back all instances from earlier batches when a
  later batch under-fulfills, so a multi-batch partial result is never silently returned as success.

## Files to Modify

- `sdcm/provision/aws/utils.py` -- add `create_ec2_fleet_instance_request()`,
  `create_launch_template()`/`delete_launch_template()`, `delete_ec2_fleet()`,
  `is_ec2_fleet_unfulfillable()`, `log_ec2_fleet_errors()` and `split_instance_types()`, replacing
  the `SpotFleet`-specific helpers (`create_spot_fleet_instance_request()`,
  `get_provisioned_fleet_instance_ids()`). No `describe_fleets` polling helper is added —
  `instant` fleets return synchronously.
- `sdcm/provision/aws/provisioner.py` -- rename `_execute_spot_fleet_instance_request()` ->
  `_execute_ec2_fleet_instance_request()` (supporting a list of `AWSInstanceParams`), drop the
  `_get_provisioned_fleet_instance_ids()`/`_wait_for_fleet_request_done()` poll helpers, and use
  `delete_ec2_fleet()`/direct termination for cleanup instead of `cancel_spot_fleet_requests`
- `sdcm/provision/aws/constants.py` -- add EC2 Fleet constants (`EC2_FLEET_LIMIT`,
  `EC2_FLEET_TYPE_INSTANT`, `EC2_FLEET_ALLOCATION_STRATEGY`, `EC2_FLEET_UNFULFILLABLE_ERROR_CODES`)
- `sdcm/sct_config.py` -- add the dedicated `aws_instance_type_db_alternatives` field (fleet-only,
  AWS-only, `StringOrList`) and validate every listed type is available in the target region in
  `_instance_type_validation()`; `instance_type_db`/`instance_type_loader` stay single-literal
- `sdcm/sct_provision/aws/cluster.py` -- `_instance_types` builds `[instance_type_db] +
  alternatives` (deduped) via `_INSTANCE_TYPE_ALTERNATIVES_PARAM_NAME`; only the fleet path uses
  entries beyond the first
- `test-cases/scale/scale-cluster.yaml` -- example config: `instance_type_db: 'i7i.large'` +
  `aws_instance_type_db_alternatives: ['i7ie.large', 'i4i.large', 'i3en.large']` for the scale test
- `unit_tests/unit/test_aws_spot_provisioning.py` -- add tests for the `create_fleet` request
  shape, error classification, and `split_instance_types()` (replacing the Spot Fleet polling tests)
- `unit_tests/integration/test_aws_services.py` -- extend the `instance_provision` parametrize
  list / add a case exercising multiple instance types end-to-end against moto

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_aws_spot_provisioning.py -v`
- [ ] Integration test passes: `uv run python -m pytest unit_tests/integration/test_aws_services.py -k fleet -v`
- [ ] Manually verify a `create_fleet` request with 3 instance type overrides is built correctly
      (assert on the API call payload in a unit test, no live AWS call required)
- [ ] Confirm `_is_provision_type_fleet()` / `SPOT_CNT_LIMIT` gating still routes small counts to
      single spot instance requests, unaffected by this change
- [ ] `uv run sct.py pre-commit` passes
</content>
