# QAINFRA-1: Capacity Reservation Implementation Plan

## Executive Summary

This document outlines the plan to extend capacity reservation functionality from AWS to GCE, Azure, and OCI backends in the ScyllaDB Cluster Tests framework.

## Background

### Current State
- **AWS**: Full capacity reservation support implemented (`sdcm/provision/aws/capacity_reservation.py`)
  - Reserves compute capacity before test execution
  - Ensures instances are available when needed
  - Supports fallback to alternative availability zones
  - Integrates with placement groups
  - Automatic cleanup after test completion

### Problem Statement
GCE, Azure, and OCI backends lack capacity reservation support, which can lead to:
- Test failures due to insufficient capacity during peak usage
- Inability to guarantee resource availability for critical tests
- Increased test flakiness and retry overhead
- Limited confidence in infrastructure provisioning

## Cloud Provider Analysis

### 1. Google Cloud Platform (GCE)

#### API: Compute Engine Reservations
- **Documentation**: https://cloud.google.com/compute/docs/instances/reservations-overview
- **SDK**: `google-cloud-compute` Python library

#### Key Features:
- **Zonal Reservations**: Reserve specific machine types in a single zone
- **Regional Reservations**: Reserve across zones in a region (for flexibility)
- **Committed Use**: Optional discounts for long-term reservations
- **VM Instance Matching**: Specific or shared reservations

#### API Methods:
```python
from google.cloud import compute_v1

# Create reservation
reservations_client.insert(project=project, zone=zone, reservation_resource=reservation)

# List reservations
reservations_client.list(project=project, zone=zone)

# Delete reservation
reservations_client.delete(project=project, zone=zone, reservation=name)

# Get reservation
reservations_client.get(project=project, zone=zone, reservation=name)
```

#### Configuration Parameters Needed:
- `gce_use_capacity_reservation`: Boolean to enable feature
- `gce_reservation_type`: "zonal" or "regional"
- `gce_reservation_vm_count`: Number of VMs to reserve
- `gce_reservation_specific_match`: Boolean for specific vs. shared reservation

#### Limitations:
- Must specify exact machine type and zone/region
- Cannot mix machine types in single reservation
- Different pricing model (pay for reserved capacity)
- Minimum reservation period may apply

### 2. Microsoft Azure

#### API: On-Demand Capacity Reservation
- **Documentation**: https://learn.microsoft.com/en-us/azure/virtual-machines/capacity-reservation-overview
- **SDK**: `azure-mgmt-compute` Python library

#### Key Features:
- **Capacity Reservation Groups**: Logical grouping of reservations
- **Zone-specific**: Reserve in specific availability zones
- **Flexible VM Series**: Support for various VM series
- **No-commit**: Pay-as-you-go, cancel anytime

#### API Methods:
```python
from azure.mgmt.compute import ComputeManagementClient

# Create capacity reservation group
compute_client.capacity_reservation_groups.create_or_update(
    resource_group_name, capacity_reservation_group_name, parameters
)

# Create capacity reservation
compute_client.capacity_reservations.create_or_update(
    resource_group_name, capacity_reservation_group_name, 
    capacity_reservation_name, parameters
)

# Delete capacity reservation
compute_client.capacity_reservations.begin_delete(
    resource_group_name, capacity_reservation_group_name, 
    capacity_reservation_name
)
```

#### Configuration Parameters Needed:
- `azure_use_capacity_reservation`: Boolean to enable feature
- `azure_capacity_reservation_group`: Name of reservation group
- `azure_reserved_vm_count`: Number of VMs to reserve
- `azure_reservation_zones`: List of zones for reservation

#### Limitations:
- Not supported with Availability Sets
- Not compatible with Spot VMs
- Certain VM sizes not supported
- Zone-specific limitations per region
- Requires Capacity Reservation Group before individual reservations

### 3. Oracle Cloud Infrastructure (OCI)

#### API: Compute Capacity Reservations
- **Documentation**: https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/reserve-capacity.htm
- **SDK**: `oci` Python SDK

#### Key Features:
- **AD-specific**: Reserve in specific Availability Domains
- **Shape-based**: Reserve specific instance shapes
- **Compartment-scoped**: Per-compartment reservations
- **In-use/Reserved Counts**: Track active usage vs. total reserved

#### API Methods:
```python
import oci

# Create capacity reservation
compute_client.create_compute_capacity_reservation(
    create_compute_capacity_reservation_details
)

# List capacity reservations
compute_client.list_compute_capacity_reservations(compartment_id)

# Delete capacity reservation
compute_client.delete_compute_capacity_reservation(
    capacity_reservation_id
)

# Get capacity reservation
compute_client.get_compute_capacity_reservation(
    capacity_reservation_id
)
```

#### Configuration Parameters Needed:
- `oci_use_capacity_reservation`: Boolean to enable feature
- `oci_compartment_id`: Compartment for reservations
- `oci_availability_domain`: AD for reservation
- `oci_instance_shape`: Shape to reserve
- `oci_reserved_instance_count`: Number of instances

#### Limitations:
- AD-specific (no regional reservations)
- Must specify exact shape
- Compartment-level permissions required
- Different fault domains within AD
- OCI-specific tagging requirements

## Implementation Strategy

### Phase-by-Phase Approach

#### **Phase 1: GCE Implementation** (Recommended First)
**Rationale**: GCE has simpler API, existing infrastructure in SCT, and is actively used.

**Timeline**: 2-3 weeks
**Effort**: Medium
**Dependencies**: GCE cluster support already exists

**Tasks**:
1. Create `sdcm/provision/gce/capacity_reservation.py` (new file, ~250 lines)
2. Update `sdcm/cluster_gce.py` (minimal changes)
3. Update `sdcm/sct_config.py` (add 4-5 config parameters)
4. Update `defaults/gce_config.yaml` (add defaults)
5. Write unit tests (new file, ~200 lines)
6. Integration testing
7. Documentation

#### **Phase 2: Azure Implementation**
**Rationale**: Second priority, growing usage, more complex API.

**Timeline**: 3-4 weeks
**Effort**: Medium-High
**Dependencies**: Azure provisioner improvements may be needed

**Tasks**:
1. Create `sdcm/provision/azure/capacity_reservation.py` (new file, ~300 lines)
2. Update `sdcm/cluster_azure.py` (minimal changes)
3. Update `sdcm/provision/azure/provisioner.py` (integration)
4. Update `sdcm/sct_config.py` (add 5-6 config parameters)
5. Update `defaults/azure_config.yaml` (add defaults)
6. Write unit tests (new file, ~250 lines)
7. Integration testing
8. Documentation

#### **Phase 3: OCI Implementation** (Optional/Future)
**Rationale**: Lowest priority, limited current usage, infrastructure in development.

**Timeline**: 2-3 weeks
**Effort**: Medium
**Dependencies**: OCI cluster support maturity

**Tasks**:
1. Create `sdcm/provision/oci/` directory
2. Create `sdcm/provision/oci/capacity_reservation.py` (new file, ~250 lines)
3. Enhance OCI cluster support
4. Update `sdcm/sct_config.py` (add 4-5 config parameters)
5. Update `defaults/oci_config.yaml` (add defaults)
6. Write unit tests (new file, ~200 lines)
7. Integration testing
8. Documentation

### Common Interface Design

```python
# Base class for all capacity reservations
class CapacityReservationBase(ABC):
    """Abstract base class for cloud provider capacity reservations."""
    
    @abstractmethod
    def is_enabled(self, params: dict) -> bool:
        """Check if capacity reservation is enabled for this backend."""
        pass
    
    @abstractmethod
    def reserve(self, params: dict) -> None:
        """Create capacity reservations based on test parameters."""
        pass
    
    @abstractmethod
    def get_id(self, zone: str, instance_type: str) -> str:
        """Get reservation ID for specific instance type in zone."""
        pass
    
    @abstractmethod
    def cancel(self, params: dict) -> None:
        """Cancel all active reservations."""
        pass
    
    @abstractmethod
    def cancel_all_regions(self, test_id: str) -> None:
        """Cancel reservations across all regions."""
        pass
```

## Configuration Schema

### GCE Configuration
```yaml
# In defaults/gce_config.yaml or test config
gce_use_capacity_reservation: false  # Enable GCE reservations
gce_reservation_type: "zonal"        # zonal or regional
gce_reservation_specific: true        # specific vs shared reservation
```

### Azure Configuration
```yaml
# In defaults/azure_config.yaml or test config
azure_use_capacity_reservation: false              # Enable Azure reservations
azure_capacity_reservation_group: "sct-test-group" # Reservation group name
azure_reservation_zones: ["1", "2"]                # Zones for reservation
```

### OCI Configuration
```yaml
# In defaults/oci_config.yaml or test config
oci_use_capacity_reservation: false   # Enable OCI reservations
oci_compartment_id: ""                # Compartment for reservation
oci_availability_domain: ""           # Specific AD to use
```

## Testing Strategy

### Unit Tests
- Mock cloud provider APIs
- Test reservation creation logic
- Test error handling and fallbacks
- Test cleanup/cancellation logic
- Test configuration validation

### Integration Tests
- End-to-end test with actual cloud providers
- Test multi-zone fallback
- Test with different instance types
- Test cleanup on test failure
- Test concurrent reservations

### Manual Testing
Each implementation requires manual testing:
1. Enable capacity reservation in test config
2. Run provision test with various instance types
3. Verify reservation created in cloud console
4. Verify instances use reservations
5. Verify cleanup after test completion
6. Test failure scenarios (capacity unavailable)

## Cost Considerations

### GCE
- Pay for reserved capacity whether used or not
- Committed use discounts available
- Regional reservations may cost more

### Azure
- Pay-as-you-go for reservations
- Charged even if VMs not running
- No upfront commitment required

### OCI
- Pay for reserved capacity
- Minimum reservation periods may apply
- Compartment-level billing

### Mitigation Strategies
1. Automatic cleanup after tests
2. Maximum reservation duration limits
3. Monitoring/alerting for orphaned reservations
4. Cost reports per test run
5. Optional auto-cancellation on test failure

## Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| API changes by cloud providers | High | Low | Version pinning, extensive testing |
| Cost overruns from orphaned reservations | High | Medium | Automated cleanup, monitoring |
| Capacity still unavailable despite reservation | Medium | Low | Multi-zone fallback, skip logic |
| Implementation complexity | Medium | Medium | Phased approach, code reuse |
| Testing overhead | Low | High | Automated tests, mock APIs |

## Success Criteria

### Phase 1 (GCE) Complete When:
- [x] GCE capacity reservation code implemented
- [x] Unit tests passing with >80% coverage
- [x] Integration tests successful on real GCE
- [x] Documentation complete
- [x] Code review approved
- [x] No cost overruns during testing

### Phase 2 (Azure) Complete When:
- [x] Azure capacity reservation code implemented
- [x] Unit tests passing with >80% coverage
- [x] Integration tests successful on real Azure
- [x] Documentation complete
- [x] Code review approved
- [x] No cost overruns during testing

### Phase 3 (OCI) Complete When:
- [x] OCI capacity reservation code implemented
- [x] Unit tests passing with >80% coverage
- [x] Integration tests successful on real OCI
- [x] Documentation complete
- [x] Code review approved
- [x] No cost overruns during testing

## Open Questions

1. **Priority**: Which backend should be implemented first?
   - Recommendation: GCE (simpler API, active usage)

2. **Common Interface**: Should we refactor AWS to use common base class?
   - Recommendation: Yes, in Phase 4 after individual implementations

3. **Multi-DC Support**: Should we support multi-DC from the start?
   - Recommendation: No, start with single-DC like AWS, add later

4. **Cost Limits**: Should we enforce maximum reservation costs?
   - Recommendation: Yes, add configurable cost guards

5. **Fallback Behavior**: What should happen if capacity unavailable?
   - Recommendation: Try alternative zones, then skip/fail test with clear message

## Resources Required

### Development
- 1 Senior Engineer for 6-8 weeks (all phases)
- Access to GCE/Azure/OCI test accounts
- Budget for capacity reservation testing

### Testing
- GCE project with quota for reservations
- Azure subscription with capacity reservation permissions
- OCI tenancy with compartment access
- CI/CD pipeline updates

### Documentation
- Technical writer for user documentation (optional)
- Examples and tutorials
- Troubleshooting guides

## Timeline

### Aggressive Timeline (Parallel Work)
- Week 1-2: Phase 1 (GCE) - Implementation
- Week 3: Phase 1 (GCE) - Testing & Documentation
- Week 4-5: Phase 2 (Azure) - Implementation
- Week 6-7: Phase 2 (Azure) - Testing & Documentation
- Week 8-9: Phase 3 (OCI) - Implementation (if approved)
- Week 10: Phase 3 (OCI) - Testing & Documentation

### Conservative Timeline (Sequential Work)
- Week 1-3: Phase 1 (GCE) - Complete
- Week 4-7: Phase 2 (Azure) - Complete
- Week 8-10: Phase 3 (OCI) - Complete (if approved)
- Week 11-12: Common abstraction & refactoring

## Next Steps

1. **Decision Required**: Approve overall approach and phase priority
2. **Resource Allocation**: Assign engineering resources
3. **Account Setup**: Ensure test accounts have necessary permissions
4. **Kickoff**: Begin Phase 1 implementation

## Appendix

### Reference Links
- AWS Capacity Reservation Impl: `sdcm/provision/aws/capacity_reservation.py`
- GCE Reservations Docs: https://cloud.google.com/compute/docs/instances/reservations-overview
- Azure Capacity Reservation Docs: https://learn.microsoft.com/en-us/azure/virtual-machines/capacity-reservation-overview
- OCI Capacity Reservation Docs: https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/reserve-capacity.htm

### Related Issues
- GitHub Issue #8839: Capacity reservation AZ fallback
- GitHub Issue #8514: Capacity reservation instance type mismatch
- GitHub Issue #9317: Multi-DC capacity reservation support

### Code Examples
See AWS implementation for reference:
- Reservation creation: `SCTCapacityReservation.reserve()`
- Reservation usage: `sdcm/provision/aws/provisioner.py`
- Configuration: `sdcm/sct_config.py` (`use_capacity_reservation` parameter)
