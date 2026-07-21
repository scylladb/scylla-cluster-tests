# Mini-Plan: KVM Nested Virtualization for SCT Runner

> **Note:** This mini-plan will be dropped once all items are implemented and key highlights have been merged into [`docs/plans/minicloud-local-testing.md`](../minicloud-local-testing.md). See [PR #14009](https://github.com/scylladb/scylla-cluster-tests/pull/14009) for the consolidated plan.

**Date:** 2026-06-08
**Estimated LOC:** ~80
**Related PR:** #14875

## Problem

Minicloud runs QEMU VMs inside Docker, requiring `/dev/kvm` on the host. Jenkins ASG builders
don't provide KVM. Minicloud tests must run on sct-runner instances with nested virtualization
enabled at the hypervisor level:
- **AWS**: Only `c8i`, `m8i`, `r8i` instance families support `CpuOptions.NestedVirtualization`
- **GCE**: N1, N2, N2D, C2, C2D, C3, C3D, M1, M2, M3 families support it via `AdvancedMachineFeatures`
- **E2/T2D/A2 (GCE)** and all other AWS families: NOT supported

## Approach

1. `AwsSctRunner._create_instance` auto-detects instance family from `instance_type.split(".")[0]`
   and adds `CpuOptions={"NestedVirtualization": "enabled"}` for supported families
2. `GceSctRunner._create_instance` auto-detects from `instance_type.split("-")[0]` and passes
   `enable_nested_virtualization=True` to `create_instance()`
3. `gce_utils.create_instance()` sets `instance.advanced_machine_features.enable_nested_virtualization`
4. `scripts/start-minicloud.sh` loads `kvm_intel`/`kvm_amd` kernel module at runtime before Docker start
5. `minicloud-provision-test.yaml` sets `instance_type_runner: 'm8i.large'` (smallest KVM-capable AWS type)

## Files to Modify

- `sdcm/sct_runner.py` -- `NESTED_VIRTUALIZATION_INSTANCE_FAMILIES` on both `AwsSctRunner` and `GceSctRunner`; auto-enable logic in both `_create_instance` methods
- `sdcm/utils/gce_utils.py` -- add `enable_nested_virtualization` param to `create_instance()`
- `scripts/start-minicloud.sh` -- `modprobe kvm_intel || modprobe kvm_amd` before docker run
- `test-cases/minicloud-provision-test.yaml` -- `instance_type_runner: 'm8i.large'`

## Verification

- `python -m pytest unit_tests/unit/test_sct_runner.py -x` passes (20 tests)
- `python -c "from sdcm.utils.gce_utils import create_instance"` imports without error
- Manual: `sct.py create-runner-instance --cloud-provider aws --instance-type m8i.large` → instance has `/dev/kvm`
- Manual: `sct.py create-runner-instance --cloud-provider gce --instance-type n2-standard-2` → instance has `/dev/kvm`
