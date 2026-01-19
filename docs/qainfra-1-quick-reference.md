# QAINFRA-1: Architecture Diagrams and Quick Reference

## Current State (AWS Only)

```
┌─────────────────────────────────────────────────────────────┐
│                    SCT Test Execution                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├── AWS Backend ✓
                 │   ├── Capacity Reservation ENABLED
                 │   ├── Auto-fallback to alternative AZs
                 │   ├── Placement group support
                 │   └── Automatic cleanup
                 │
                 ├── GCE Backend ✗
                 │   └── NO capacity reservation
                 │
                 ├── Azure Backend ✗
                 │   └── NO capacity reservation
                 │
                 └── OCI Backend ✗
                     └── NO capacity reservation
```

## Target State (All Backends)

```
┌─────────────────────────────────────────────────────────────┐
│                    SCT Test Execution                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├── AWS Backend ✓
                 │   └── sdcm/provision/aws/capacity_reservation.py
                 │
                 ├── GCE Backend ✓ (Phase 1)
                 │   └── sdcm/provision/gce/capacity_reservation.py
                 │
                 ├── Azure Backend ✓ (Phase 2)
                 │   └── sdcm/provision/azure/capacity_reservation.py
                 │
                 └── OCI Backend ✓ (Phase 3)
                     └── sdcm/provision/oci/capacity_reservation.py
                     
                 Common Interface (Phase 4):
                 └── sdcm/provision/common/capacity_reservation.py
```

## Capacity Reservation Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                 1. Test Configuration                       │
│  cluster_backend: gce                                       │
│  gce_use_capacity_reservation: true                         │
│  n_db_nodes: 3                                              │
│  instance_type_db: n2-highmem-16                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          2. Calculate Required Capacity                     │
│  - 3x n2-highmem-16 (DB nodes)                              │
│  - 1x n2-highcpu-8 (Loader)                                 │
│  - Duration: test_duration + buffer                         │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          3. Create Reservations                             │
│  Try zone: us-east1-b                                       │
│    ├── Reserve 3x n2-highmem-16  → SUCCESS ✓                │
│    └── Reserve 1x n2-highcpu-8   → SUCCESS ✓                │
│  If FAIL → Try next zone (us-east1-c)                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          4. Provision Instances                             │
│  Use reservation IDs:                                       │
│    ├── cr-abc123 for DB nodes                               │
│    └── cr-def456 for loaders                                │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          5. Run Test                                        │
│  - Instances guaranteed available                           │
│  - No capacity-related failures                             │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│          6. Cleanup                                         │
│  - Terminate instances                                      │
│  - Cancel reservations                                      │
│  - Verify no orphaned resources                             │
└─────────────────────────────────────────────────────────────┘
```

## Class Architecture

```
┌─────────────────────────────────────────────────────────────┐
│         CapacityReservationBase (Abstract)                  │
│  + is_enabled(params) → bool                                │
│  + reserve(params) → None                                   │
│  + get_id(zone, type) → str                                 │
│  + cancel(params) → None                                    │
│  + cancel_all_regions(test_id) → None                       │
└────────────────┬────────────────────────────────────────────┘
                 │
         ┌───────┴────────┬────────────┬───────────┐
         │                │            │           │
         ▼                ▼            ▼           ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│AWS Capacity │  │GCE Capacity │  │Azure Cap.   │  │OCI Capacity │
│Reservation  │  │Reservation  │  │Reservation  │  │Reservation  │
│             │  │             │  │             │  │             │
│- Uses boto3 │  │- Uses       │  │- Uses Azure │  │- Uses OCI   │
│- Multi-AZ   │  │  google-    │  │  SDK        │  │  SDK        │
│  fallback   │  │  cloud-     │  │- Capacity   │  │- AD-        │
│- Placement  │  │  compute    │  │  Res Groups │  │  specific   │
│  groups     │  │- Zonal/     │  │- Multi-zone │  │- Shape-     │
│             │  │  Regional   │  │             │  │  based      │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

## Integration Points

```
┌─────────────────────────────────────────────────────────────┐
│                   sct_config.py                             │
│  Defines: {backend}_use_capacity_reservation                │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│            sct_provision/{backend}/layout.py                │
│  Calls: CapacityReservation.reserve(params)                 │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│         provision/{backend}/provisioner.py                  │
│  Uses: reservation_id when creating instances               │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│           cluster_{backend}.py (optional)                   │
│  May check: if params.get('use_capacity_reservation')       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                    tester.py                                │
│  Cleanup: CapacityReservation.cancel(params)                │
└─────────────────────────────────────────────────────────────┘
```

## API Comparison Matrix

| Feature | AWS EC2 | GCE Compute | Azure VM | OCI Compute |
|---------|---------|-------------|----------|-------------|
| **SDK** | boto3 | google-cloud-compute | azure-mgmt-compute | oci |
| **Main Class** | EC2Client | ReservationsClient | ComputeManagementClient | ComputeClient |
| **Create Method** | create_capacity_reservation() | insert() | create_or_update() | create_compute_capacity_reservation() |
| **List Method** | describe_capacity_reservations() | list() | list() | list_compute_capacity_reservations() |
| **Delete Method** | cancel_capacity_reservation() | delete() | begin_delete() | delete_compute_capacity_reservation() |
| **Get Method** | describe_capacity_reservations() | get() | get() | get_compute_capacity_reservation() |
| **Scope** | Availability Zone | Zone/Region | Zone | Availability Domain |
| **Grouping** | None | None | Capacity Res Group | Compartment |
| **Instance Match** | Targeted | Specific/Auto | Automatic | Automatic |

## Configuration Parameters

### AWS (Current)
```yaml
use_capacity_reservation: true      # Global enable/disable
availability_zone: "a"              # AZ selection
use_placement_group: false          # Optional placement group
```

### GCE (Phase 1 - NEW)
```yaml
gce_use_capacity_reservation: true  # Enable for GCE
gce_reservation_type: "zonal"       # zonal or regional
gce_reservation_specific: true      # specific vs shared
availability_zone: "b"              # Zone selection
```

### Azure (Phase 2 - NEW)
```yaml
azure_use_capacity_reservation: true              # Enable for Azure
azure_capacity_reservation_group: "sct-cr-group"  # Reservation group
azure_reservation_zones: ["1", "2"]               # Multi-zone support
availability_zone: "1"                            # Default zone
```

### OCI (Phase 3 - NEW)
```yaml
oci_use_capacity_reservation: true  # Enable for OCI
oci_compartment_id: "ocid1.comp..." # Compartment ID
oci_availability_domain: "AD-1"     # Specific AD
```

## File Structure

```
scylla-cluster-tests/
├── sdcm/
│   ├── provision/
│   │   ├── aws/
│   │   │   ├── capacity_reservation.py      [EXISTS - 283 lines]
│   │   │   ├── provisioner.py                [MODIFY - add CR usage]
│   │   │   └── ...
│   │   ├── gce/
│   │   │   ├── capacity_reservation.py      [NEW - ~250 lines]
│   │   │   ├── provisioner.py                [MODIFY if needed]
│   │   │   └── ...
│   │   ├── azure/
│   │   │   ├── capacity_reservation.py      [NEW - ~300 lines]
│   │   │   ├── provisioner.py                [MODIFY - add CR usage]
│   │   │   └── ...
│   │   ├── oci/
│   │   │   ├── capacity_reservation.py      [NEW - ~250 lines]
│   │   │   ├── provisioner.py                [NEW/MODIFY]
│   │   │   └── ...
│   │   └── common/
│   │       └── capacity_reservation.py      [NEW - Phase 4 - ~150 lines]
│   ├── sct_config.py                         [MODIFY - add parameters]
│   ├── cluster_gce.py                        [MODIFY if needed]
│   ├── cluster_azure.py                      [MODIFY if needed]
│   └── tester.py                             [MODIFY - cleanup logic]
├── defaults/
│   ├── gce_config.yaml                       [MODIFY - add defaults]
│   ├── azure_config.yaml                     [MODIFY - add defaults]
│   └── oci_config.yaml                       [MODIFY - add defaults]
├── unit_tests/
│   ├── test_gce_capacity_reservation.py      [NEW - ~200 lines]
│   ├── test_azure_capacity_reservation.py    [NEW - ~250 lines]
│   └── test_oci_capacity_reservation.py      [NEW - ~200 lines]
└── docs/
    ├── qainfra-1-executive-summary.md        [EXISTS]
    ├── qainfra-1-capacity-reservation-plan.md [EXISTS]
    └── qainfra-1-quick-reference.md          [THIS FILE]
```

## Quick Command Reference

### Check if Capacity Reservation is Enabled
```python
from sdcm.provision.gce.capacity_reservation import GCECapacityReservation

if GCECapacityReservation.is_enabled(params):
    print("Capacity reservation enabled for GCE")
```

### Create Reservations
```python
GCECapacityReservation.reserve(params)
# Creates reservations based on test config
# Tries multiple zones if needed
# Raises exception if no capacity available
```

### Get Reservation ID
```python
cr_id = GCECapacityReservation.get_id(zone="b", instance_type="n2-highmem-16")
# Use this ID when provisioning instances
```

### Cleanup
```python
GCECapacityReservation.cancel(params)
# Cancels all reservations for this test
```

### Emergency Cleanup (All Regions)
```python
GCECapacityReservation.cancel_all_regions(test_id="12345")
# Finds and cancels all reservations with this test_id across all regions
```

## Testing Checklist

### Unit Tests
- [ ] Test `is_enabled()` logic with various configs
- [ ] Test reservation creation with mock API
- [ ] Test multi-zone fallback logic
- [ ] Test cleanup/cancellation
- [ ] Test error handling (API errors, capacity unavailable)
- [ ] Test configuration validation

### Integration Tests
- [ ] Create reservation in real cloud account
- [ ] Verify reservation appears in cloud console
- [ ] Provision instance using reservation
- [ ] Verify instance uses reserved capacity
- [ ] Test cleanup after provisioning
- [ ] Test multi-zone fallback with real API

### Manual Testing
- [ ] Run full test with capacity reservation enabled
- [ ] Check cloud console for reservation details
- [ ] Verify costs match expectations
- [ ] Test failure scenarios (invalid config, no capacity)
- [ ] Verify cleanup on test failure
- [ ] Check for orphaned reservations

## Cost Monitoring

### AWS Cost Tags
```python
tags = {
    'TestId': test_id,
    'CapacityReservation': 'true',
    'Backend': 'aws'
}
```

### GCE Labels
```python
labels = {
    'test_id': test_id,
    'capacity_reservation': 'true',
    'backend': 'gce'
}
```

### Azure Tags
```python
tags = {
    'TestId': test_id,
    'CapacityReservation': 'true',
    'Backend': 'azure'
}
```

### OCI Defined Tags
```python
defined_tags = {
    'SCT': {
        'TestId': test_id,
        'CapacityReservation': 'true',
        'Backend': 'oci'
    }
}
```

## Troubleshooting Guide

### Issue: Reservation Creation Fails
**Symptoms**: Exception during `reserve()` call  
**Causes**:
- Insufficient quota
- Invalid instance type
- Region/zone not supported
- Permissions issue

**Solution**:
1. Check cloud console for quota limits
2. Verify instance type available in zone
3. Check service account permissions
4. Review error message for specific issue

### Issue: Instances Not Using Reservation
**Symptoms**: Reservation shows 0 usage  
**Causes**:
- Reservation ID not passed to provisioner
- Zone mismatch
- Instance type mismatch

**Solution**:
1. Verify `get_id()` returns correct ID
2. Check zone matches between reservation and instance
3. Verify instance type matches exactly

### Issue: Orphaned Reservations
**Symptoms**: Reservations remain after test  
**Causes**:
- Test crashed before cleanup
- Cleanup code not executed
- Permission issue during delete

**Solution**:
1. Run cleanup manually: `cancel_all_regions(test_id)`
2. Check cleanup job logs
3. Verify delete permissions
4. Set up monitoring alerts

### Issue: Capacity Still Unavailable
**Symptoms**: Reservation created but instances fail to launch  
**Causes**:
- Reservation in wrong zone
- Regional capacity exhausted
- Cloud provider issue

**Solution**:
1. Verify reservation is active
2. Try alternative zones
3. Contact cloud provider support
4. Consider regional reservation (GCE)

## Performance Impact

### Reservation Creation Time
- **AWS**: ~30-60 seconds
- **GCE**: ~20-40 seconds
- **Azure**: ~45-90 seconds (includes group creation)
- **OCI**: ~30-60 seconds

### Instance Provisioning Time
- **With Reservation**: No change (same as without)
- **Benefit**: Guaranteed capacity, no retry delays

### Test Duration Impact
- **Additional Time**: +30-90 seconds (reservation creation)
- **Saved Time**: Eliminates capacity-related retries (can save minutes to hours)
- **Net Impact**: Usually positive (faster overall)

## Security Considerations

### Permissions Required

**GCE**:
- `compute.reservations.create`
- `compute.reservations.delete`
- `compute.reservations.list`
- `compute.reservations.get`

**Azure**:
- `Microsoft.Compute/capacityReservationGroups/write`
- `Microsoft.Compute/capacityReservationGroups/delete`
- `Microsoft.Compute/capacityReservations/write`
- `Microsoft.Compute/capacityReservations/delete`

**OCI**:
- `CAPACITY_RESERVATION_CREATE`
- `CAPACITY_RESERVATION_DELETE`
- `CAPACITY_RESERVATION_READ`
- `CAPACITY_RESERVATION_UPDATE`

### Best Practices
1. Use service accounts with minimal required permissions
2. Tag all reservations with test_id for tracking
3. Implement automatic cleanup to prevent cost accumulation
4. Monitor for orphaned reservations
5. Set maximum reservation duration limits
6. Use separate projects/subscriptions/compartments for testing

---

**Quick Links**:
- [Executive Summary](./qainfra-1-executive-summary.md)
- [Detailed Plan](./qainfra-1-capacity-reservation-plan.md)
- [AWS Implementation](../sdcm/provision/aws/capacity_reservation.py)
