# Stress Parameter Split Migration Guide

## Overview

The `stress_before_upgrade` parameter has been split into two separate parameters to clarify their different use cases and prevent misconfiguration.

## Background

Previously, `stress_before_upgrade` was used in two incompatible ways:
1. **Preload/Validation** - Running stress workload before any nodes are upgraded (can use CL=ALL)
2. **During Upgrade** - Running stress workload while nodes are being upgraded (cannot use CL=ALL)

This led to issues when trying to use CL=ALL for validation, as it would fail during rolling upgrades when not all nodes are available.

## New Parameters

### `stress_before_upgrade`
- **Purpose**: Preload and validation workload that runs BEFORE any nodes are upgraded
- **CL Support**: Can use CL=ALL for data validation
- **Usage**: Data preparation, validation, baseline metrics collection
- **Example**:
  ```yaml
  stress_before_upgrade: cassandra-stress write cl=ALL n=10000000 ...
  ```

### `stress_during_upgrade` (NEW)
- **Purpose**: Workload that runs DURING rolling upgrade while nodes are being upgraded
- **CL Support**: CANNOT use CL=ALL (not all nodes may be available)
- **Usage**: Testing cluster behavior under load during upgrades
- **Example**:
  ```yaml
  stress_during_upgrade: scylla-bench -consistency-level=quorum ...
  ```

## Migration Guide

### For Test Configurations Using `test_rolling_upgrade`

If your test configuration uses the `test_rolling_upgrade` method and wants to run a workload during the upgrade:

**Before:**
```yaml
stress_before_upgrade: scylla-bench -workload=sequential -mode=write -consistency-level=quorum ...
```

**After:**
```yaml
stress_before_upgrade: scylla-bench -workload=sequential -mode=write -consistency-level=all ...
stress_during_upgrade: scylla-bench -workload=sequential -mode=write -consistency-level=quorum ...
```

### For Test Configurations Using `test_generic_cluster_upgrade`

No changes required! This test only uses `stress_before_upgrade` for preload, which is the correct usage.

### For Test Configurations Using `test_cluster_upgrade_latency_regression`

No changes required! This test uses `stress_before_upgrade` for baseline metrics before upgrade starts, which is correct.

## Code Changes

### In `upgrade_test.py`

1. **Line ~663**: Schema creation during setup now uses `stress_during_upgrade` if available, with fallback to `stress_before_upgrade`
2. **Line ~763**: During-upgrade workload now explicitly uses `stress_during_upgrade` parameter

### In Test Configuration Files

The main change is in `test-cases/upgrades/rolling-upgrade.yaml`, which now defines both parameters.

## Best Practices

1. **For Validation**: Use `stress_before_upgrade` with CL=ALL to ensure data is correctly written before testing
2. **For Load Testing**: Use `stress_during_upgrade` with CL=QUORUM or CL=ONE for workloads during upgrades
3. **For Baseline Metrics**: Use `stress_before_upgrade` to collect pre-upgrade performance metrics
4. **For Upgrade Testing**: Use `stress_during_upgrade` to test cluster behavior under load

## Backward Compatibility

- Existing configurations that only use `stress_before_upgrade` will continue to work
- The schema creation logic has fallback to use `stress_before_upgrade` if `stress_during_upgrade` is not defined
- No breaking changes for configurations using `test_generic_cluster_upgrade` or `test_cluster_upgrade_latency_regression`

## Examples

### Full Configuration Example

```yaml
# Preload with CL=ALL for validation
stress_before_upgrade: >
  cassandra-stress write no-warmup cl=ALL n=10100200
  -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)'
  -mode cql3 native -rate threads=1000 -pop seq=1..10100200

# Run during upgrade with CL=QUORUM
stress_during_upgrade: >
  scylla-bench -workload=sequential -mode=write -replication-factor=3
  -partition-count=800 -consistency-level=quorum -validate-data

# Verify after upgrade with CL=QUORUM
stress_after_cluster_upgrade: >
  cassandra-stress read no-warmup cl=QUORUM n=10100200
  -mode cql3 native -rate threads=1000 -pop seq=1..10100200
```

## Related Issues

- Original issue: #6452 (attempt to use CL=ALL for validation)
- This PR resolves the issue by clearly separating preload from during-upgrade workloads
